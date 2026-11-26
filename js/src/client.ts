import { Channel, type ChannelListeners } from './channel.js'
import { parseEnvelope } from './envelope.js'
import { invokeListener } from './invoke-listener.js'
import { MurmurStream, type StreamOptions } from './stream.js'
import { Transport, validateRetryOptions, type RetryOptions } from './transport.js'

/** Configures the built-in authorization request. */
export interface AuthOptions {
  /** Extra headers or a provider called before each token request. */
  headers?: Record<string, string> | (() => Record<string, string> | Promise<Record<string, string>>)

  /** The browser's credential mode for the token request. */
  credentials?: RequestCredentials
}

/** Issues a token for private topics in place of the built-in request. */
export type Authorizer = (channels: string[], signal: AbortSignal) => Promise<{
  /** The token used to authorize the SSE connection. */
  token: string
  /** The token expiration in Unix seconds. */
  expires_at: number
}>

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

/** The arguments delivered to listeners for each client event. */
type ClientEventArgs = {
  gap: []
  connected: []
  disconnected: []
  unauthorized: [status: number]
}

/** The names of events emitted by the client. */
type ClientEvent = keyof ClientEventArgs

/** A joined channel and its registered event callbacks. */
type Subscription = {
  /** The subscription exposed to the application. */
  channel: Channel
  /** The callbacks registered by wire event name. */
  listeners: ChannelListeners
}

/** A reusable authorization token for a specific list of private topics. */
type CachedToken = {
  /** The sorted topics serialized as a cache key. */
  topics: string
  /** The token used to authorize a connection. */
  token: string
  /** The token expiration in Unix seconds. */
  expiresAt: number
}

/** The prefix that identifies a private topic on the server. */
const privatePrefix = 'private-'

/** Rejects a failure limit that is not a positive integer. */
function validateFailureLimit(value: number, option: 'streamMaxFailures' | 'maxFailures'): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(option + ' must be an integer greater than or equal to 1')
  }
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
  }

  /** The transport for the shared event stream. */
  protected readonly transport: Transport

  /** The active streams created by this client. */
  protected readonly streams = new Set<MurmurStream>()

  /** The client's retry settings, also used as the defaults for streams. */
  protected readonly retryOptions: RetryOptions

  /** The time that must remain on a cached token for it to be reused, in seconds. */
  protected readonly tokenRefreshMarginSeconds: number

  /** The default consecutive failure limit for independent streams. */
  protected readonly streamMaxFailures: number

  /** The most recent token and the private topics it authorizes. */
  protected cachedToken?: CachedToken

  /** Whether a channel change has queued a connection update. */
  protected connectionQueued = false

  /** Whether the client has been permanently closed. */
  protected closed = false

  /** Creates a client for the given streaming endpoint. */
  public constructor(protected readonly options: MurmurOptions) {
    this.retryOptions = {
      retryMs: options.retryMs ?? 3000,
      maxRetryMs: options.maxRetryMs ?? 30_000,
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
      event => this.emit(event),
      url => {
        if (this.cachedToken && new URL(url).searchParams.get('token') === this.cachedToken.token) {
          this.cachedToken = undefined
        }
      },
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
    const retryOptions = {
      retryMs: options.retryMs ?? this.retryOptions.retryMs,
      maxRetryMs: options.maxRetryMs ?? this.retryOptions.maxRetryMs,
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
    const leftPublic = this.channels.delete('public:' + name)
    const leftPrivate = this.channels.delete('private:' + name)
    if (leftPublic || leftPrivate) {
      this.updateEvents()
      this.scheduleConnection()
    }
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

  /** Closes channels and streams and cancels outstanding connection attempts. */
  public close(): void {
    this.closed = true
    this.transport.close()
    for (const stream of this.streams) {
      stream.close()
    }
    for (const { listeners } of this.channels.values()) {
      listeners.clear()
    }
    this.channels.clear()
    for (const listeners of Object.values(this.listeners)) {
      listeners.clear()
    }
    this.cachedToken = undefined
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
      this.channels.set(key, { channel, listeners })
      this.scheduleConnection()
    }
    return channel
  }

  /** Applies channel changes together when the current tick ends. */
  protected scheduleConnection(): void {
    this.transport.cancelPending()
    if (this.channels.size === 0) {
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
  protected async connectionUrl(signal: AbortSignal, lastEventId: string | undefined): Promise<string> {
    const topics = [...new Set(Array.from(this.channels.values(), ({ channel }) => channel.name))]
    const privateTopics = topics.filter(topic => topic.startsWith(privatePrefix))
    const url = new URL(this.options.url, globalThis.location?.href)
    url.searchParams.set('topics', topics.join(','))
    url.searchParams.delete('token')
    if (privateTopics.length > 0) {
      url.searchParams.set('token', await this.tokenFor(privateTopics, signal))
    }
    url.searchParams.delete('lastEventId')
    if (lastEventId !== undefined) {
      url.searchParams.set('lastEventId', lastEventId)
    }
    return url.href
  }

  /** Reuses a token for the same private topics or requests a fresh one. */
  protected async tokenFor(channels: string[], signal: AbortSignal): Promise<string> {
    const topics = JSON.stringify([...channels].sort())
    const cached = this.cachedToken
    if (cached?.topics === topics && cached.expiresAt - Date.now() / 1000 > this.tokenRefreshMarginSeconds) {
      return cached.token
    }

    const body = await this.requestToken(channels, signal)
    if (
      body === null || typeof body !== 'object' ||
      !('token' in body) || typeof body.token !== 'string' ||
      !('expires_at' in body) || typeof body.expires_at !== 'number' || !Number.isFinite(body.expires_at)
    ) {
      throw new Error('Invalid Murmur authorization response')
    }

    if (!signal.aborted) {
      this.cachedToken = { topics, token: body.token, expiresAt: body.expires_at }
    }
    return body.token
  }

  /** Requests an authorization response from the provider or the built-in endpoint. */
  protected async requestToken(channels: string[], signal: AbortSignal): Promise<unknown> {
    const { authorizer } = this.options
    if (authorizer) {
      try {
        return await authorizer(channels, signal)
      } catch (error) {
        if (!signal.aborted && error !== null && typeof error === 'object' &&
          'status' in error && typeof error.status === 'number' && error.status >= 400 && error.status < 500) {
          this.emit('unauthorized', error.status)
        }
        throw error
      }
    }

    const { auth, authEndpoint } = this.options
    const supplied = typeof auth?.headers === 'function' ? await auth.headers() : auth?.headers
    signal.throwIfAborted()
    const headers = supplied === undefined ? undefined : new Headers(supplied)
    const defaults: Record<string, string> = {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    }
    if (!headers?.has('X-XSRF-TOKEN') && typeof document !== 'undefined') {
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
    const response = await fetch(authEndpoint ?? '/murmur/auth', {
      method: 'POST',
      credentials: auth?.credentials ?? 'include',
      headers: headers ?? defaults,
      body: JSON.stringify({ channels }),
      signal,
    })
    if (!response.ok) {
      if (!signal.aborted && response.status >= 400 && response.status < 500) {
        this.emit('unauthorized', response.status)
      }
      throw new Error('Murmur authorization failed')
    }

    return response.json()
  }

  /** Dispatches a stream event to its channel or emits a gap notification. */
  protected receive(event: string, data: string): void {
    if (event === 'murmur.gap') {
      this.emit('gap')
      return
    }

    const envelope = parseEnvelope(data)
    if (envelope === undefined) {
      return
    }

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

  /** Notifies listeners of a client event. */
  protected emit<Event extends ClientEvent>(event: Event, ...args: ClientEventArgs[Event]): void {
    for (const listener of this.listeners[event]) {
      invokeListener(listener, ...args)
    }
  }
}
