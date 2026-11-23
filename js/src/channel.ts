/** The callbacks registered under each wire event name. */
export type ChannelListeners = Map<string, Set<(data: unknown) => void>>

/** A subscription to one Murmur topic. */
export class Channel {
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

  /** Resolves an event to the name used in the stream. */
  protected eventName(event: string): string {
    if (event.startsWith('.')) {
      return event.slice(1)
    }
    return this.namespace ? this.namespace.replaceAll('.', '\\') + '\\' + event : event
  }
}
