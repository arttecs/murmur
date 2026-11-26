import type { Channel } from './channel.js'
import { Murmur, type AuthOptions, type MurmurOptions } from './client.js'
import { invokeListener } from './invoke-listener.js'

/** Configures Murmur as a custom Laravel Echo broadcaster. */
export interface MurmurConnectorOptions extends Omit<MurmurOptions, 'namespace'> {
  /** Echo's dotted event namespace, or false to disable its prefix. */
  namespace?: string | false

  /** The CSRF token to use when window.Laravel does not supply one. */
  csrfToken?: string | null

  /** The token sent as an Authorization header on token requests. */
  bearerToken?: string | null
}

/** The connection states Laravel Echo reports for a connector. */
export type MurmurConnectionStatus = 'connected' | 'disconnected' | 'connecting' | 'reconnecting' | 'failed'

/** Formats event names the way Laravel Echo does, without importing Echo. */
class EventFormatter {
  /** Creates a formatter for a dotted namespace, or none when it is false or empty. */
  public constructor(protected readonly namespace: string | false) {}

  /** Resolves an Echo event name to the wire name Laravel broadcasts. */
  public format(event: string): string {
    if (event.startsWith('.') || event.startsWith('\\')) {
      return event.substring(1)
    }

    return (this.namespace ? `${this.namespace}.${event}` : event).replace(/\./g, '\\')
  }
}

/** The prefix Echo and Murmur use for private channels. */
const privatePrefix = 'private-'

/** Resolves a CSRF token using the same precedence as Echo's built-in connectors. */
function resolveCsrfToken(options: MurmurConnectorOptions): string | null | undefined {
  const browser: (Window & { Laravel?: { csrfToken?: string } }) | undefined =
    typeof window === 'undefined' ? undefined : window
  return browser?.Laravel?.csrfToken || options.csrfToken ||
    (typeof document !== 'undefined' && typeof document.querySelector === 'function'
      ? document.querySelector('meta[name="csrf-token"]')?.getAttribute('content')
      : null)
}

/** Combines the caller's authorization headers with Echo's CSRF and bearer tokens. */
function authorization(options: MurmurConnectorOptions): AuthOptions {
  const { auth, bearerToken } = options

  return {
    ...auth,
    headers: async () => {
      const supplied = typeof auth?.headers === 'function' ? await auth.headers() : auth?.headers
      const headers = new Headers(supplied)
      const csrfToken = resolveCsrfToken(options)
      if (csrfToken) {
        headers.set('X-CSRF-TOKEN', csrfToken)
      }
      if (bearerToken) {
        headers.set('Authorization', `Bearer ${bearerToken}`)
      }
      return Object.fromEntries(headers)
    },
  }
}

/** Adapts a Murmur subscription to the Echo channel API. */
class MurmurEchoChannel {
  /** The literal Laravel notification event. */
  public readonly notificationCreatedEvent = '.Illuminate\\Notifications\\Events\\BroadcastNotificationCreated'

  /** Echo callbacks keyed by their wire event name. */
  protected readonly listeners = new Map<string, Set<CallableFunction>>()

  /** The callbacks notified when the channel attaches or recovers from a failure. */
  protected readonly subscriptions = new Set<CallableFunction>()

  /** The callbacks notified of a denied private authorization request. */
  protected readonly errors = new Set<CallableFunction>()

  /** Whether attachment has already been reported since this channel joined or the connection failed. */
  protected subscribedNotified = false

  /** Whether the channel remains joined. */
  protected active = true

  /** Wraps a topic subscription and formats event names as Echo does. */
  public constructor(
    protected readonly channel: Channel,
    protected readonly formatter: EventFormatter,
    protected readonly onWhisper: () => void = () => {},
  ) {
    this.channel.onAttached(() => this.attached())
    this.channel.error(status => this.unauthorized(status))
  }

  /** Registers an Echo callback for a formatted event. */
  public listen(event: string, callback: CallableFunction): this {
    if (!this.active) {
      return this
    }

    const name = this.formatter.format(event)
    let callbacks = this.listeners.get(name)
    if (!callbacks) {
      const registered = new Set<CallableFunction>()
      this.listeners.set(name, registered)
      this.channel.listen('.' + name, data => {
        for (const handler of registered) {
          invokeListener(() => handler(data))
        }
      })
      callbacks = registered
    }
    callbacks.add(callback)
    return this
  }

  /** Removes all callbacks for an event or the specified callback only. */
  public stopListening(event: string, callback?: CallableFunction): this {
    const name = this.formatter.format(event)
    const callbacks = this.listeners.get(name)
    if (!callbacks) {
      return this
    }

    if (callback) {
      callbacks.delete(callback)
    } else {
      callbacks.clear()
    }
    if (callbacks.size === 0) {
      this.listeners.delete(name)
      this.channel.stopListening('.' + name)
    }
    return this
  }

  /** Registers a callback for future server acknowledgments of this channel. */
  public subscribed(callback: CallableFunction): this {
    if (this.active) {
      this.subscriptions.add(callback)
    }
    return this
  }

  /** Registers a callback for future private authorization failures. */
  public error(callback: CallableFunction): this {
    if (this.active) {
      this.errors.add(callback)
    }
    return this
  }

  /** Registers a callback for Laravel broadcast notifications. */
  public notification(callback: CallableFunction): this {
    return this.listen(this.notificationCreatedEvent, callback)
  }

  /** Removes the given Laravel notification callback. */
  public stopListeningForNotification(callback: CallableFunction): this {
    return this.stopListening(this.notificationCreatedEvent, callback)
  }

  /** Preserves fluent setup for unsupported client events and warns once. */
  public listenForWhisper(_event: string, _callback: CallableFunction): this {
    this.onWhisper()
    return this
  }

  /** Preserves fluent cleanup for unsupported client events. */
  public stopListeningForWhisper(_event: string, _callback?: CallableFunction): this {
    return this
  }

  /** Notifies subscription callbacks the first time this channel attaches after a join or failure. */
  protected attached(): void {
    if (this.subscribedNotified) {
      return
    }
    this.subscribedNotified = true
    for (const callback of [...this.subscriptions]) {
      if (this.active) {
        invokeListener(() => callback())
      }
    }
  }

  /** Notifies this channel's error callbacks of its denied authorization status. */
  protected unauthorized(status: number): void {
    for (const callback of [...this.errors]) {
      if (this.active) {
        invokeListener(() => callback(status))
      }
    }
  }

  /**
   * Allows the next successful attachment to notify listeners after a connection failure.
   *
   * @internal
   */
  public connectionLost(): void {
    this.subscribedNotified = false
  }

  /**
   * Clears callbacks and disables listener registration on a departed channel.
   *
   * @internal
   */
  public detach(): void {
    this.active = false
    for (const callbacks of this.listeners.values()) {
      callbacks.clear()
    }
    this.listeners.clear()
    this.subscriptions.clear()
    this.errors.clear()
  }
}

/** Adapts an authorized Murmur subscription to Echo's private channel API. */
class MurmurEchoPrivateChannel extends MurmurEchoChannel {
  /** Preserves method chaining without sending an unsupported client event and warns once. */
  public whisper(_event: string, _data: Record<string, unknown>): this {
    this.onWhisper()
    return this
  }
}

/** Connects Laravel Echo to a shared Murmur SSE connection. */
export class MurmurConnector {
  /** The channel registry used by Echo to leave every channel. */
  public readonly channels: Partial<Record<string, MurmurEchoChannel>> = Object.create(null)

  /** The formatter shared by the connector's channels. */
  protected readonly formatter: EventFormatter

  /** The client backing the current connector lifecycle. */
  protected client!: Murmur

  /** Whether the connector has an active client. */
  protected active = false

  /** The last reported connection state. */
  protected status: MurmurConnectionStatus = 'disconnected'

  /** Whether the unsupported client events warning was already printed. */
  protected whisperWarned = false

  /** The observers of Echo connection status changes. */
  protected readonly statusListeners = new Set<(status: MurmurConnectionStatus) => void>()

  /** Creates a connector from the options passed through Echo. */
  public constructor(protected readonly options: MurmurConnectorOptions) {
    if (typeof options.url !== 'string' || options.url.trim() === '') {
      throw new Error('Murmur requires a non-empty url option for its SSE endpoint.')
    }

    this.formatter = new EventFormatter(options.namespace ?? 'App.Events')
    this.connect()
  }

  /** Returns the client for streams, statistics, and other advanced operations. */
  public get murmur(): Murmur {
    return this.client
  }

  /** Creates a fresh client if disconnected and opens SSE when a channel joins. */
  public connect(): void {
    if (this.active) {
      return
    }

    this.client = new Murmur({
      ...this.options,
      namespace: this.options.namespace === false ? '' : this.options.namespace,
      auth: authorization(this.options),
    })
    this.active = true

    this.client.on('connected', () => this.setStatus('connected'))
    this.client.on('disconnected', () => {
      for (const channel of Object.values(this.channels)) {
        channel?.connectionLost()
      }
      this.setStatus('reconnecting')
    })
  }

  /** Returns or joins the public channel with the given name. */
  public channel(name: string): MurmurEchoChannel {
    if (name.startsWith(privatePrefix)) {
      return this.privateChannel(name.slice(privatePrefix.length))
    }

    const existing = this.channels[name]
    if (existing) {
      return existing
    }
    const channel = new MurmurEchoChannel(this.client.channel(name), this.formatter, () => this.warnAboutWhispers())
    this.track(name, channel)
    return channel
  }

  /** Returns or joins the private channel with the given name. */
  public privateChannel(name: string): MurmurEchoPrivateChannel {
    const topic = privatePrefix + name
    const existing = this.channels[topic]
    if (existing instanceof MurmurEchoPrivateChannel) {
      return existing
    }
    const channel = new MurmurEchoPrivateChannel(this.client.private(name), this.formatter, () => this.warnAboutWhispers())
    this.track(topic, channel)
    return channel
  }

  /** Rejects encrypted private channels because Murmur does not support them. */
  public encryptedPrivateChannel(_name: string): never {
    throw new Error('Murmur does not support encrypted private channels.')
  }

  /** Rejects presence channels because Murmur does not support them. */
  public presenceChannel(_name: string): never {
    throw new Error('Murmur does not support presence channels.')
  }

  /** Listens to an event on the given public channel. */
  public listen(name: string, event: string, callback: CallableFunction): MurmurEchoChannel {
    return this.channel(name).listen(event, callback)
  }

  /** Leaves the public and private variants of the given channel. */
  public leave(name: string): void {
    this.leaveChannel(name)
    this.leaveChannel(privatePrefix + name)
  }

  /** Leaves exactly the channel registered under the supplied wire name. */
  public leaveChannel(name: string): void {
    const channel = this.channels[name]
    if (!channel) {
      return
    }

    channel.detach()
    delete this.channels[name]
    const isPrivate = channel instanceof MurmurEchoPrivateChannel
    this.client.leaveSubscription(isPrivate ? name.slice(privatePrefix.length) : name, isPrivate)
    this.setStatus(Object.keys(this.channels).length === 0 ? 'disconnected' : 'connecting')
  }

  /** Returns the server-issued ID or an empty value for Echo's request interceptors. */
  public socketId(): string {
    return this.active ? this.client.socketId() ?? '' : ''
  }

  /** Returns the current Echo connection status. */
  public connectionStatus(): MurmurConnectionStatus {
    return this.status
  }

  /** Subscribes to status changes and returns a cleanup function. */
  public onConnectionChange(callback: (status: MurmurConnectionStatus) => void): () => void {
    this.statusListeners.add(callback)
    return () => { this.statusListeners.delete(callback) }
  }

  /** Closes the client, including streams and retries, and detaches all channels. */
  public disconnect(): void {
    this.active = false
    this.client.close()
    for (const name of Object.keys(this.channels)) {
      this.channels[name]?.detach()
      delete this.channels[name]
    }
    this.setStatus('disconnected')
  }

  /** Records a channel and reports the pending subscription change. */
  protected track(name: string, channel: MurmurEchoChannel): void {
    if (this.active) {
      this.channels[name] = channel
      this.setStatus('connecting')
    } else {
      channel.detach()
    }
  }

  /** Notifies observers when the connector changes state. */
  protected setStatus(status: MurmurConnectionStatus): void {
    if (status !== this.status) {
      this.status = status
      for (const callback of this.statusListeners) {
        invokeListener(callback, status)
      }
    }
  }

  /** Warns once per connector that client events are not delivered. */
  protected warnAboutWhispers(): void {
    if (!this.whisperWarned) {
      this.whisperWarned = true
      console.warn('Murmur does not support client events: whisper and listenForWhisper do nothing.')
    }
  }
}
