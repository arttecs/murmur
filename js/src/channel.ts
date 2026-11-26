import { invokeListener } from './invoke-listener.js'

/** The callbacks registered under each wire event name. */
export type ChannelListeners = Map<string, Set<(data: unknown) => void>>

/** A subscription to one Murmur topic. */
export class Channel {
  /** Callbacks registered for this channel's authorization failures. */
  protected readonly errors = new Set<(status: number) => void>()

  /** Callbacks registered for the server's attachment acknowledgment. */
  protected readonly attachments = new Set<() => void>()

  /** The subscribed topic, including its prefix for private channels. */
  public readonly name: string

  /**
   * Creates a channel that reports changes to its event names.
   *
   * @internal
   */
  public constructor(
    name: string,
    protected readonly namespace: string,
    protected readonly listeners: ChannelListeners,
    protected readonly listenersChanged: () => void,
  ) {
    this.name = name
  }

  /** Registers a callback for an event on this channel. */
  public listen(event: string, callback: (data: unknown) => void): this {
    const name = this.eventName(event)
    const callbacks = this.listeners.get(name)
    if (callbacks) {
      callbacks.add(callback)
    } else {
      this.listeners.set(name, new Set([callback]))
      this.listenersChanged()
    }
    return this
  }

  /** Removes the callbacks for an event on this channel. */
  public stopListening(event: string): this {
    if (this.listeners.delete(this.eventName(event))) {
      this.listenersChanged()
    }
    return this
  }

  /** Registers a callback for a denied authorization request on this channel. */
  public error(callback: (status: number) => void): this {
    this.errors.add(callback)
    return this
  }

  /**
   * Delivers a denied authorization status to this channel's listeners.
   *
   * @internal
   */
  public authorizationDenied(status: number): void {
    for (const callback of this.errors) {
      invokeListener(callback, status)
    }
  }

  /**
   * Registers a callback for attachment on a connection that included this channel.
   *
   * @internal
   */
  public onAttached(callback: () => void): void {
    this.attachments.add(callback)
  }

  /**
   * Notifies listeners that the server finished attaching this subscription.
   *
   * @internal
   */
  public attachmentConfirmed(): void {
    for (const callback of this.attachments) {
      invokeListener(callback)
    }
  }

  /**
   * Clears callbacks when the owning client leaves this subscription.
   *
   * @internal
   */
  public detach(): void {
    this.listeners.clear()
    this.errors.clear()
    this.attachments.clear()
  }

  /** Resolves an event to the name used in the stream. */
  protected eventName(event: string): string {
    if (event.startsWith('.')) {
      return event.slice(1)
    }
    return this.namespace ? this.namespace.replaceAll('.', '\\') + '\\' + event : event
  }
}
