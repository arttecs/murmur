import { EventFormatter, type ConnectionStatus } from 'laravel-echo'
import type { Channel } from './channel.js'
import { Murmur, type AuthOptions, type MurmurOptions } from './client.js'
import { invokeListener } from './invoke-listener.js'

/** Configures Murmur as a custom Laravel Echo broadcaster. */
export interface MurmurConnectorOptions extends Omit<MurmurOptions, 'namespace'> {
  /** Echo's dotted event namespace, or false to disable its prefix. */
  namespace?: string | false

  /** The token sent as an Authorization header on token requests. */
  bearerToken?: string | null
}

/** The prefix Echo and Murmur use for private channels. */
const privatePrefix = 'private-'

/** Combines the caller's authorization headers with Echo's bearer token. */
function authorization(options: MurmurConnectorOptions): AuthOptions | undefined {
  const { auth, bearerToken } = options
  if (!bearerToken) {
    return auth
  }

  return {
    ...auth,
    headers: async () => {
      const supplied = typeof auth?.headers === 'function' ? await auth.headers() : auth?.headers
      const headers = new Headers(supplied)
      headers.set('Authorization', `Bearer ${bearerToken}`)
      return Object.fromEntries(headers)
    },
  }
}

/** Adds exact channel removal without changing the standalone client's leave behavior. */
class EchoMurmur extends Murmur {
  /** Leaves a subscription by name and kind and refreshes the shared connection. */
  public leaveSubscription(name: string, isPrivate: boolean): void {
    const key = (isPrivate ? 'private:' : 'public:') + name
    const subscription = this.channels.get(key)
    if (subscription) {
      subscription.listeners.clear()
      this.channels.delete(key)
      this.updateEvents()
      this.scheduleConnection()
    }
  }
}

/** Adapts a Murmur subscription to the Echo channel API. */
class MurmurEchoChannel {
  /** The literal Laravel notification event. */
  public readonly notificationCreatedEvent = '.Illuminate\\Notifications\\Events\\BroadcastNotificationCreated'

  /** Echo callbacks keyed by their wire event name. */
  protected readonly listeners = new Map<string, Set<CallableFunction>>()

  /** The callbacks notified each time the shared connection opens. */
  protected readonly subscriptions = new Set<CallableFunction>()

  /** The callbacks notified of a denied private authorization request. */
  protected readonly errors = new Set<CallableFunction>()

  /** Whether the channel remains joined. */
  protected active = true

  /** Wraps a topic subscription and uses Echo's event formatter. */
  public constructor(
    protected readonly channel: Channel,
    protected readonly formatter: EventFormatter,
  ) {}

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

  /** Registers a callback for future successful opens with this channel joined. */
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

  /** Preserves fluent setup for unsupported client events. */
  public listenForWhisper(_event: string, _callback: CallableFunction): this {
    return this
  }

  /** Preserves fluent cleanup for unsupported client events. */
  public stopListeningForWhisper(_event: string, _callback?: CallableFunction): this {
    return this
  }

  /**
   * Notifies subscription callbacks that the shared connection has opened.
   *
   * @internal
   */
  public opened(): void {
    for (const callback of [...this.subscriptions]) {
      if (this.active) {
        invokeListener(() => callback())
      }
    }
  }

  /**
   * Notifies error callbacks of the authorization status.
   *
   * @internal
   */
  public unauthorized(status: number): void {
    for (const callback of [...this.errors]) {
      if (this.active) {
        invokeListener(() => callback(status))
      }
    }
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
  /** Preserves method chaining without sending an unsupported client event. */
  public whisper(_event: string, _data: Record<string, unknown>): this {
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
  protected client!: EchoMurmur

  /** Whether the connector has an active client. */
  protected active = false

  /** The last reported connection state. */
  protected status: ConnectionStatus = 'disconnected'

  /** The observers of Echo connection status changes. */
  protected readonly statusListeners = new Set<(status: ConnectionStatus) => void>()

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

    this.client = new EchoMurmur({
      ...this.options,
      namespace: this.options.namespace === false ? '' : this.options.namespace,
      auth: authorization(this.options),
    })
    this.active = true

    this.client.on('connected', () => {
      const joined = Object.values(this.channels)
      this.setStatus('connected')
      for (const channel of joined) {
        channel?.opened()
      }
    })
    this.client.on('disconnected', () => this.setStatus('reconnecting'))
    this.client.on('unauthorized', status => {
      for (const channel of Object.values(this.channels)) {
        if (channel instanceof MurmurEchoPrivateChannel) {
          channel.unauthorized(status)
        }
      }
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
    const channel = new MurmurEchoChannel(this.client.channel(name), this.formatter)
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
    const channel = new MurmurEchoPrivateChannel(this.client.private(name), this.formatter)
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

  /** Returns undefined because Murmur has no connection identifier. */
  public socketId(): undefined {
    return undefined
  }

  /** Returns the current Echo connection status. */
  public connectionStatus(): ConnectionStatus {
    return this.status
  }

  /** Subscribes to status changes and returns a cleanup function. */
  public onConnectionChange(callback: (status: ConnectionStatus) => void): () => void {
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
  protected setStatus(status: ConnectionStatus): void {
    if (status !== this.status) {
      this.status = status
      for (const callback of this.statusListeners) {
        invokeListener(callback, status)
      }
    }
  }
}
