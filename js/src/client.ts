import { Channel, type ChannelListeners } from './channel.js'
import { parseEnvelope } from './envelope.js'
import { invokeListener } from './invoke-listener.js'
import { MurmurStream } from './stream.js'
import { Transport } from './transport.js'

/** The reconnection delay used when none is configured. */
const defaultRetryMs = 3000

/** Configures the Murmur client. */
export interface MurmurOptions {
  /** The full server-sent events endpoint. */
  url: string

  /** The endpoint that issues tokens for private topics. */
  authEndpoint?: string

  /** The dotted namespace for unqualified event names. */
  namespace?: string

  /** The initial delay before retrying a failed connection, in milliseconds. */
  retryMs?: number

  /** The fetch implementation used for authorization requests. */
  fetch?: typeof fetch

  /** The EventSource constructor used for streaming. */
  EventSource?: typeof EventSource
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

type Subscription = { channel: Channel; listeners: ChannelListeners }
type CachedToken = { topics: string; token: string; expiresAt: number }

const privatePrefix = 'private-'

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

  /** The most recent token and the private topics it authorizes. */
  protected cachedToken?: CachedToken

  /** Whether a channel change has queued a connection update. */
  protected connectionQueued = false

  /** Whether the client has been permanently closed. */
  protected closed = false

  /** Creates a client for the given streaming endpoint. */
  public constructor(protected readonly options: MurmurOptions) {
    this.transport = new Transport(
      options.EventSource,
      options.retryMs ?? defaultRetryMs,
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
  public stream(url: string): MurmurStream {
    const stream = new MurmurStream(url, this.options.EventSource, this.options.retryMs ?? defaultRetryMs, closed => {
      this.streams.delete(closed)
    })
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
    if (cached?.topics === topics && cached.expiresAt - Date.now() / 1000 > 30) {
      return cached.token
    }

    const headers: Record<string, string> = {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    }
    const xsrf = typeof document === 'undefined' ? undefined : document.cookie
      .split(';')
      .map(cookie => cookie.trim())
      .find(cookie => cookie.startsWith('XSRF-TOKEN='))
    if (xsrf !== undefined) {
      headers['X-XSRF-TOKEN'] = decodeURIComponent(xsrf.slice('XSRF-TOKEN='.length))
    }

    const fetch = this.options.fetch ?? globalThis.fetch.bind(globalThis)
    const response = await fetch(this.options.authEndpoint ?? '/murmur/auth', {
      method: 'POST',
      credentials: 'include',
      headers,
      body: JSON.stringify({ channels }),
      signal,
    })
    if (!response.ok) {
      if (!signal.aborted && response.status >= 400 && response.status < 500) {
        this.emit('unauthorized', response.status)
      }
      throw new Error('Murmur authorization failed')
    }

    const body: unknown = await response.json()
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
