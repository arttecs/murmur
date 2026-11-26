import { Channel, type ChannelListeners } from './channel.js'
import { parseEnvelope } from './envelope.js'
import { invokeListener } from './invoke-listener.js'
import { MurmurStream, type StreamOptions } from './stream.js'
import { RetryAfterError, Transport, validateRetryOptions, type RetryOptions } from './transport.js'

/** Configures the built-in authorization request. */
export interface AuthOptions {
  /** Extra headers or a provider called before each token request. */
  headers?: Record<string, string> | (() => Record<string, string> | Promise<Record<string, string>>)

  /** The browser's credential mode for the token request. */
  credentials?: RequestCredentials
}

/** A subscription token and its expiration. */
export interface AuthorizationToken {
  /** The token used to authorize the SSE connection. */
  token: string
  /** The token expiration in Unix seconds. */
  expires_at: number
  /** The requested wire topics this custom token covers. Omit for batch-wide coverage. */
  channels?: string[]
}

/** Issues tokens for all or a subset of the requested private topics. */
export type Authorizer = (channels: string[], signal: AbortSignal) => Promise<AuthorizationToken | AuthorizationToken[]>

/** Configures the Murmur client. */
export interface MurmurOptions extends Partial<RetryOptions> {
  /** The full server-sent events endpoint. */
  url: string

  /** The endpoint that issues tokens for private topics. */
  authEndpoint?: string

  /** Options for the built-in authorization request. */
  auth?: AuthOptions

  /** Replaces the built-in authorization request. */
  authorizer?: Authorizer

  /** The dotted namespace for unqualified event names. */
  namespace?: string

  /** The minimum seconds remaining before a cached token can be reused. */
  tokenRefreshMarginSeconds?: number

  /** The number of consecutive failures that expires an independent stream. */
  streamMaxFailures?: number

  /** The fetch implementation used for authorization requests. */
  fetch?: typeof fetch

  /** The EventSource constructor used for streaming. */
  EventSource?: typeof EventSource

  /** Sends browser credentials on channel and stream connections. */
  withCredentials?: boolean
}

/** A point-in-time snapshot of the client and its shared connection. */
export interface MurmurStats {
  /** Whether the shared connection is currently open. */
  connected: boolean

  /** The number of times the shared connection opened. */
  connections: number

  /** The number of failed shared connection attempts and unexpected drops. */
  disconnections: number

  /** The number of data events passed to the shared channel dispatch. */
  events: number

  /** The number of gap signals received on the shared connection. */
  gaps: number

  /** The number of token authorization attempts, excluding cache hits. */
  tokenRequests: number

  /** The number of token authorization attempts that failed before cancellation. */
  tokenFailures: number

  /** The number of currently joined channels. */
  channels: number

  /** The number of independent streams that have not closed. */
  streams: number

  /** The current replay cursor of the shared connection. */
  lastEventId: string | undefined

  /** The most recently announced ID of the shared SSE connection. */
  connectionId: string | undefined

  /** The Unix time in milliseconds of the last data event passed to dispatch. */
  lastEventAt: number | undefined
}

/** The arguments delivered to listeners for each client event. */
type ClientEventArgs = {
  /** Delivers a replay gap notification. */
  gap: []
  /** Reports a shared connection that opened. */
  connected: []
  /** Reports a failed or dropped shared connection. */
  disconnected: []
  /** Reports a channel denied authorization by the application. */
  unauthorized: [status: number, channelName: string]
  /** Reports a client protocol or configuration error. */
  error: [error: Error]
}

/** The names of events emitted by the client. */
type ClientEvent = keyof ClientEventArgs

/** A joined channel and its registered event callbacks. */
type Subscription = {
  /** The subscription exposed to the application. */
  channel: Channel
  /** The callbacks registered by wire event name. */
  listeners: ChannelListeners
  /** Whether this join is still waiting for its first attach marker. */
  pending: boolean
}

/** The prefix that identifies a private topic on the server. */
const privatePrefix = 'private-'

/** The maximum number of simultaneous built-in authorization requests per attempt. */
const authorizationConcurrency = 4

/** The request target byte budget, reserving 2 KiB of the server limit for headers and the request line. */
const requestTargetBudget = 6 * 1024

/** Validates a token response before using it in a comma-delimited list. */
function isAuthorizationToken(value: unknown): value is AuthorizationToken {
  return value !== null && typeof value === 'object' &&
    'token' in value && typeof value.token === 'string' && value.token !== '' && !value.token.includes(',') &&
    'expires_at' in value && typeof value.expires_at === 'number' && Number.isFinite(value.expires_at) &&
    (!('channels' in value) || value.channels === undefined ||
      (Array.isArray(value.channels) && [...value.channels].every(channel => typeof channel === 'string')))
}

/** Rejects a failure limit that is not a positive integer. */
function validateFailureLimit(value: number, option: 'streamMaxFailures' | 'maxFailures'): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(option + ' must be an integer greater than or equal to 1')
  }
}

/** Carries the HTTP status of an unsuccessful built-in authorization response. */
class AuthorizationResponseError extends Error {
  /** Records the status and retry header of the response. */
  public constructor(public readonly status: number, public readonly retryAfter: string | null) {
    super('Murmur authorization failed')
  }
}

/** Converts Retry-After delay-seconds or an HTTP date to a delay in milliseconds. */
function retryAfterDelay(header: string | null): number {
  const value = header?.trim() ?? ''
  if (/^\d+$/.test(value)) {
    return Number(value) * 1000
  }
  if (/^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:[a-z]+)?,? /i.test(value) && /\d:\d{2}:\d{2}/.test(value)) {
    const time = Date.parse(value)
    if (Number.isFinite(time)) {
      return Math.max(0, time - Date.now())
    }
  }
  return 0
}

/** Multiplexes channel subscriptions and opens independent streams. */
export class Murmur {
  /** The joined channels and their callbacks, indexed by subscription name and kind. */
  protected readonly channels = new Map<string, Subscription>()

  /** The callbacks registered for client events. */
  protected readonly listeners: {
    [Event in ClientEvent]: Set<(...args: ClientEventArgs[Event]) => void>
  } = {
    gap: new Set(),
    connected: new Set(),
    disconnected: new Set(),
    unauthorized: new Set(),
    error: new Set(),
  }

  /** The transport for the shared event stream. */
  protected readonly transport: Transport

  /** The active streams created by this client. */
  protected readonly streams = new Set<MurmurStream>()

  /** The cumulative counters reported by stats(). */
  protected readonly counts = {
    connections: 0,
    disconnections: 0,
    events: 0,
    gaps: 0,
    tokenRequests: 0,
    tokenFailures: 0,
  }

  /** The time the last valid shared data event was received. */
  protected lastEventAt?: number

  /** The ID most recently announced by the shared connection. */
  protected connectionId?: string

  /** Whether a protocol mismatch has already been reported. */
  protected protocolMismatchReported = false

  /** The client's retry settings, also used as the defaults for streams. */
  protected readonly retryOptions: RetryOptions

  /** The time that must remain on a cached token for it to be reused, in seconds. */
  protected readonly tokenRefreshMarginSeconds: number

  /** The default consecutive failure limit for independent streams. */
  protected readonly streamMaxFailures: number

  /** The reusable tokens associated with each authorized private topic. */
  protected readonly cachedTokens = new Map<string, AuthorizationToken[]>()

  /** Whether a token-bearing attempt has already failed without an HTTP status since the last open. */
  protected sawOpaqueFailure = false

  /** Whether a pre-open SSE failure has already caused a credential refresh since the last open. */
  protected refreshedAfterUnopenedError = false

  /** The private topics waiting for explicit authorization or a new join. */
  protected readonly deniedTopics = new Set<string>()

  /** The subscriptions sent on the attempt waiting for its attach marker. */
  protected attemptSubscriptions: Subscription[] = []

  /** Whether a channel change has queued a connection update. */
  protected connectionQueued = false

  /** Whether the client has been permanently closed. */
  protected closed = false

  /** Creates a client for the given streaming endpoint. */
  public constructor(protected readonly options: MurmurOptions) {
    const retryMs = options.retryMs ?? 3000
    this.retryOptions = {
      retryMs,
      maxRetryMs: options.maxRetryMs ?? Math.max(30_000, retryMs),
      retryJitter: options.retryJitter ?? 0.25,
    }
    validateRetryOptions(this.retryOptions)

    this.tokenRefreshMarginSeconds = options.tokenRefreshMarginSeconds ?? 30
    if (!Number.isFinite(this.tokenRefreshMarginSeconds) || this.tokenRefreshMarginSeconds < 0) {
      throw new RangeError('tokenRefreshMarginSeconds must be finite and greater than or equal to zero')
    }

    this.streamMaxFailures = options.streamMaxFailures ?? 3
    validateFailureLimit(this.streamMaxFailures, 'streamMaxFailures')

    this.transport = new Transport(
      options.EventSource,
      options.withCredentials ?? false,
      this.retryOptions,
      (signal, lastEventId) => this.connectionUrl(signal, lastEventId),
      (event, data) => this.receive(event, data),
      event => {
        if (event === 'connected') {
          this.counts.connections++
          this.sawOpaqueFailure = false
          this.refreshedAfterUnopenedError = false
        } else {
          this.attemptSubscriptions = []
          this.counts.disconnections++
        }
        this.emit(event)
      },
      (url, status) => this.refreshAfterUnopenedError(url, status),
    )
  }

  /** Joins a public channel. */
  public channel(name: string): Channel {
    return this.join('public:' + name, name)
  }

  /** Joins a private channel and authorizes its topic before connecting. */
  public private(name: string): Channel {
    return this.join('private:' + name, privatePrefix + name)
  }

  /** Opens an independent stream using a server-issued subscription URL. */
  public stream(url: string, options: StreamOptions = {}): MurmurStream {
    const retryMs = options.retryMs ?? this.retryOptions.retryMs
    const retryOptions = {
      retryMs,
      maxRetryMs: options.maxRetryMs ?? Math.max(this.retryOptions.maxRetryMs, retryMs),
      retryJitter: options.retryJitter ?? this.retryOptions.retryJitter,
    }
    const maxFailures = options.maxFailures ?? this.streamMaxFailures
    validateRetryOptions(retryOptions)
    validateFailureLimit(maxFailures, 'maxFailures')

    const stream = new MurmurStream(
      url,
      this.options.EventSource,
      this.options.withCredentials ?? false,
      retryOptions,
      maxFailures,
      closed => { this.streams.delete(closed) },
    )
    if (this.closed) {
      stream.close()
    } else {
      this.streams.add(stream)
    }
    return stream
  }

  /** Leaves the subscriptions joined with the given name. */
  public leave(name: string): void {
    this.leaveSubscription(name, false)
    this.leaveSubscription(name, true)
  }

  /**
   * Leaves exactly one subscription by its name and kind.
   *
   * @internal
   */
  public leaveSubscription(name: string, isPrivate: boolean): void {
    const key = (isPrivate ? 'private:' : 'public:') + name
    const subscription = this.channels.get(key)
    if (!subscription) {
      return
    }

    subscription.channel.detach()
    this.channels.delete(key)
    const topic = subscription.channel.name
    if (![...this.channels.values()].some(({ channel }) => channel.name === topic)) {
      this.deniedTopics.delete(topic)
      const group = this.cachedTokens.get(topic)
      this.cachedTokens.delete(topic)
      if (group && group.length > 1) {
        for (const [other, tokens] of this.cachedTokens) {
          if (tokens === group) {
            this.cachedTokens.delete(other)
          }
        }
      }
    }
    this.updateEvents()
    this.scheduleConnection()
  }

  /** Retries authorization for one denied private channel or all denied private channels. */
  public reauthorize(name?: string): void {
    if (name !== undefined) {
      if (!this.deniedTopics.delete(privatePrefix + name)) {
        return
      }
    } else if (this.deniedTopics.size > 0) {
      this.deniedTopics.clear()
    } else {
      return
    }
    this.scheduleConnection()
  }

  /** Registers a client event listener and returns a function that removes it. */
  public on<Event extends ClientEvent>(event: Event, listener: (...args: ClientEventArgs[Event]) => void): () => void {
    const listeners = this.listeners[event]
    if (!this.closed) {
      listeners.add(listener)
    }
    return () => {
      listeners.delete(listener)
    }
  }

  /** Returns the server-issued ID used for Laravel sender exclusion. */
  public socketId(): string | undefined {
    return this.connectionId
  }

  /** Returns a fresh snapshot of shared connection activity and active subscriptions. */
  public stats(): MurmurStats {
    return {
      ...this.counts,
      connected: this.transport.connected,
      channels: this.channels.size,
      streams: this.streams.size,
      lastEventId: this.transport.replayCursor,
      connectionId: this.connectionId,
      lastEventAt: this.lastEventAt,
    }
  }

  /** Closes channels and streams and cancels outstanding connection attempts. */
  public close(): void {
    this.closed = true
    this.transport.close()
    for (const stream of this.streams) {
      stream.close()
    }
    for (const { channel } of this.channels.values()) {
      channel.detach()
    }
    this.channels.clear()
    this.attemptSubscriptions = []
    for (const listeners of Object.values(this.listeners)) {
      listeners.clear()
    }
    this.cachedTokens.clear()
    this.deniedTopics.clear()
    this.updateEvents()
  }

  /** Creates a subscription or returns the existing channel for the same name and kind. */
  protected join(key: string, topic: string): Channel {
    const existing = this.channels.get(key)
    if (existing) {
      return existing.channel
    }

    const listeners: ChannelListeners = new Map()
    const channel = new Channel(topic, this.options.namespace ?? 'App.Events', listeners, () => this.updateEvents())
    if (!this.closed) {
      this.channels.set(key, { channel, listeners, pending: true })
      this.scheduleConnection()
    }
    return channel
  }

  /** Applies channel changes together when the current tick ends. */
  protected scheduleConnection(): void {
    this.transport.cancelPending()
    if (this.channels.size === 0) {
      this.attemptSubscriptions = []
      this.transport.close()
      return
    }
    if (this.connectionQueued) {
      return
    }

    this.connectionQueued = true
    queueMicrotask(() => {
      this.connectionQueued = false
      if (!this.closed && this.channels.size > 0) {
        this.transport.connect()
      }
    })
  }

  /** Synchronizes the stream's named events with the active channel listeners. */
  protected updateEvents(): void {
    const events = new Set<string>()
    for (const { listeners } of this.channels.values()) {
      for (const event of listeners.keys()) {
        events.add(event)
      }
    }
    this.transport.setEvents(events)
  }

  /** Builds the stream URL for the current subscriptions. */
  protected async connectionUrl(signal: AbortSignal, lastEventId: string | undefined): Promise<string | undefined> {
    const requested = [...new Set(Array.from(this.channels.values(), ({ channel }) => channel.name))]
    const privateTopics = requested.filter(topic => topic.startsWith(privatePrefix))
    await this.authorize(privateTopics.filter(topic => !this.deniedTopics.has(topic)), signal)
    signal.throwIfAborted()

    const topics = requested.filter(topic => !this.deniedTopics.has(topic))
    if (topics.length === 0) {
      return
    }

    const url = new URL(this.options.url, globalThis.location?.href)
    url.searchParams.set('topics', topics.join(','))
    url.searchParams.delete('token')
    const tokens = new Set<string>()
    for (const topic of topics) {
      for (const { token } of this.cachedTokens.get(topic) ?? []) {
        tokens.add(token)
      }
    }
    if (tokens.size > 0) {
      url.searchParams.set('token', [...tokens].join(','))
    }
    url.searchParams.delete('lastEventId')
    if (lastEventId !== undefined) {
      url.searchParams.set('lastEventId', lastEventId)
    }
    url.searchParams.delete('connection')
    if (this.connectionId !== undefined) {
      url.searchParams.set('connection', this.connectionId)
    }
    const subscriptions = [...this.channels.values()].filter(({ channel }) => !this.deniedTopics.has(channel.name))
    const attached = new Set(subscriptions.filter(({ pending }) => !pending).map(({ channel }) => channel.name))
    const fresh = topics.filter(topic => !attached.has(topic))
    url.searchParams.delete('fresh')
    if (fresh.length > 0) {
      url.searchParams.set('fresh', fresh.join(','))
    }
    if (new TextEncoder().encode(url.pathname + url.search).length > requestTargetBudget) {
      this.emit('error', new Error(
        `Murmur subscription URL exceeds the request budget with ${privateTopics.length} private channels requested. Use a multi-topic token for large private subscriptions or reduce the topic list.`,
      ))
      return
    }
    this.attemptSubscriptions = subscriptions
    return url.href
  }

  /** Reuses valid credentials and authorizes missing private topics. */
  protected async authorize(topics: string[], signal: AbortSignal): Promise<void> {
    const minimumExpiry = Date.now() / 1000 + this.tokenRefreshMarginSeconds
    const missing = topics.filter(topic =>
      !this.cachedTokens.get(topic)?.every(({ expires_at }) => expires_at > minimumExpiry),
    )
    if (missing.length === 0) {
      return
    }

    const { authorizer } = this.options
    if (authorizer) {
      await this.authorizeTopics(topics, signal, () => authorizer([...topics], signal), true)
      return
    }

    const remaining = missing.values()
    await Promise.all(Array.from({ length: Math.min(authorizationConcurrency, missing.length) }, async () => {
      for (const topic of remaining) {
        signal.throwIfAborted()
        await this.authorizeTopics([topic], signal, () => this.requestToken(topic, signal), false)
      }
    }))
  }

  /** Records valid tokens or marks only the requested private topics denied. */
  protected async authorizeTopics(
    topics: string[],
    signal: AbortSignal,
    request: () => Promise<unknown>,
    acceptsArray: boolean,
  ): Promise<void> {
    this.counts.tokenRequests++
    const startedAt = Date.now() / 1000
    let tokens: AuthorizationToken[]
    try {
      const body = await request()
      signal.throwIfAborted()
      const result: unknown[] = acceptsArray && Array.isArray(body) ? [...body] : [body]
      if (!result.every(isAuthorizationToken)) {
        throw new Error('Invalid Murmur authorization response')
      }
      tokens = result
    } catch (error) {
      signal.throwIfAborted()
      this.counts.tokenFailures++
      if ((!acceptsArray && !(error instanceof AuthorizationResponseError)) ||
        error === null || typeof error !== 'object' || !('status' in error) ||
        typeof error.status !== 'number') {
        throw error
      }

      if (!(error.status >= 400 && error.status < 500) || [408, 425, 429].includes(error.status)) {
        const header = error instanceof AuthorizationResponseError ? error.retryAfter
          : error instanceof Response ? error.headers.get('Retry-After')
            : null
        const delay = retryAfterDelay(header)
        throw delay > 0 ? new RetryAfterError(delay, error) : error
      }

      const minimumExpiry = Math.max(startedAt, Date.now() / 1000)
      const denied = acceptsArray ? topics.filter(topic =>
        !this.cachedTokens.get(topic)?.every(({ expires_at }) => expires_at > minimumExpiry),
      ) : topics
      this.deny(denied, error.status)
      return
    }

    const explicitCoverage = acceptsArray && tokens.some(({ channels }) => channels !== undefined)
    const uncovered: string[] = []
    for (const topic of topics) {
      const covering = explicitCoverage
        ? tokens.filter(({ channels }) => channels === undefined || channels.includes(topic))
        : tokens
      if (covering.length > 0) {
        this.cachedTokens.set(topic, covering)
      } else {
        uncovered.push(topic)
      }
    }
    this.deny(uncovered, 403)
  }

  /** Removes uncovered topics from the connection and notifies their listeners. */
  protected deny(topics: string[], status: number): void {
    const denied = new Set(topics)
    for (const topic of denied) {
      this.deniedTopics.add(topic)
      this.cachedTokens.delete(topic)
    }
    const subscriptions = [...this.channels.values()].filter(({ channel }) => denied.has(channel.name))
    for (const { channel } of subscriptions) {
      channel.authorizationDenied(status)
    }
    for (const topic of denied) {
      this.emit('unauthorized', status, topic.slice(privatePrefix.length))
    }
  }

  /** Refreshes credentials at most once per outage when a pre-open failure suggests rejection. */
  protected refreshAfterUnopenedError(url: string, status: number | undefined): void {
    const sent = new URL(url).searchParams.get('token')
    if (this.refreshedAfterUnopenedError || sent === null) {
      return
    }
    if (status === undefined && !this.sawOpaqueFailure) {
      this.sawOpaqueFailure = true
      return
    }
    if (status !== undefined && status !== 401 && status !== 403) {
      return
    }

    this.refreshedAfterUnopenedError = true
    const rejected = new Set(sent.split(','))
    for (const [topic, tokens] of this.cachedTokens) {
      if (tokens.some(({ token }) => rejected.has(token))) {
        this.cachedTokens.delete(topic)
      }
    }
  }

  /** Requests an authorization response from the built-in endpoint. */
  protected async requestToken(topic: string, signal: AbortSignal): Promise<unknown> {
    const { auth, authEndpoint } = this.options
    const supplied = typeof auth?.headers === 'function' ? await auth.headers() : auth?.headers
    signal.throwIfAborted()
    const headers = supplied === undefined ? undefined : new Headers(supplied)
    const defaults: Record<string, string> = {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    }
    if (auth?.credentials !== 'omit' && !headers?.has('X-XSRF-TOKEN') && typeof document !== 'undefined') {
      const xsrf = document.cookie.split(';')
        .map(cookie => cookie.trim())
        .find(cookie => cookie.startsWith('XSRF-TOKEN='))
      if (xsrf !== undefined) {
        defaults['X-XSRF-TOKEN'] = decodeURIComponent(xsrf.slice('XSRF-TOKEN='.length))
      }
    }
    if (headers) {
      for (const [name, value] of Object.entries(defaults)) {
        if (!headers.has(name)) {
          headers.set(name, value)
        }
      }
    }

    const fetch = this.options.fetch ?? globalThis.fetch.bind(globalThis)
    const response = await fetch(authEndpoint ?? '/broadcasting/auth', {
      method: 'POST',
      credentials: auth?.credentials ?? 'include',
      headers: headers ?? defaults,
      body: JSON.stringify({ channel_name: topic, socket_id: this.connectionId }),
      signal,
    })
    if (!response.ok) {
      throw new AuthorizationResponseError(response.status, response.headers.get('Retry-After'))
    }

    return response.json()
  }

  /** Dispatches a stream event to its channel or emits a gap notification. */
  protected receive(event: string, data: string): void {
    if (event === 'murmur.connected') {
      this.receiveConnection(data)
      return
    }
    if (event === 'murmur.attached') {
      this.receiveAttachment()
      return
    }
    if (event === 'murmur.gap') {
      this.counts.gaps++
      this.emit('gap')
      return
    }

    const envelope = parseEnvelope(data)
    if (envelope === undefined) {
      return
    }

    this.counts.events++
    this.lastEventAt = Date.now()
    const { topic, data: payload } = envelope
    const subscriptions = [this.channels.get('public:' + topic)]
    if (topic.startsWith(privatePrefix)) {
      subscriptions.push(this.channels.get('private:' + topic.slice(privatePrefix.length)))
    }
    for (const subscription of subscriptions) {
      for (const callback of subscription?.listeners.get(event) ?? []) {
        invokeListener(callback, payload)
      }
    }
  }

  /** Acknowledges only the joins included on the connection that attached. */
  protected receiveAttachment(): void {
    const attached = this.attemptSubscriptions
    this.attemptSubscriptions = []
    for (const subscription of attached) {
      subscription.pending = false
    }
    for (const { channel } of attached) {
      channel.attachmentConfirmed()
    }
  }

  /** Stores a valid server-issued connection ID and reports an unexpected protocol. */
  protected receiveConnection(data: string): void {
    let announcement: unknown
    try {
      announcement = JSON.parse(data)
    } catch {
      return
    }
    if (announcement === null || typeof announcement !== 'object') {
      return
    }

    if ('connection_id' in announcement && typeof announcement.connection_id === 'string' &&
      announcement.connection_id.length === 32 && !/[^a-f0-9]/.test(announcement.connection_id)) {
      this.connectionId = announcement.connection_id
    }
    if ('protocol' in announcement && announcement.protocol !== 1 && !this.protocolMismatchReported) {
      this.protocolMismatchReported = true
      this.emit('error', new Error(`Murmur server announced protocol ${String(announcement.protocol)}; expected 1.`))
    }
  }

  /** Notifies listeners of a client event. */
  protected emit<Event extends ClientEvent>(event: Event, ...args: ClientEventArgs[Event]): void {
    for (const listener of this.listeners[event]) {
      invokeListener(listener, ...args)
    }
  }
}
