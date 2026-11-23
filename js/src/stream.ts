import { parseEnvelope } from './envelope.js'
import { invokeListener } from './invoke-listener.js'
import { Transport } from './transport.js'

/** Consumes an independent Murmur stream from a server-issued URL. */
export class MurmurStream {
  /** The transport that opens and retries the subscription. */
  protected readonly transport: Transport

  /** The callbacks registered for data and control events. */
  protected readonly listeners = new Map<string, Set<(data?: unknown) => void>>()

  /** Whether the stream has stopped permanently. */
  protected closed = false

  /** Releases the owning client's reference to this stream. */
  protected onClose?: (stream: MurmurStream) => void

  /**
   * Opens a stream and notifies its owning client when it closes.
   *
   * @internal
   */
  public constructor(
    protected readonly url: string,
    eventSource: typeof EventSource | undefined,
    retryMs: number,
    onClose: (stream: MurmurStream) => void,
  ) {
    this.onClose = onClose
    this.transport = new Transport(
      eventSource,
      retryMs,
      (_signal, lastEventId) => this.connectionUrl(lastEventId),
      (event, data) => this.receive(event, data),
      (status, failures) => {
        if (status === 'disconnected' && failures === 3) {
          this.finish('expired')
        }
      },
    )
    this.transport.setEvents(new Set(['murmur.end']))
    this.transport.connect()
  }

  /** Registers a callback for a literal stream event. */
  public on(event: string, callback: (data: unknown) => void): this
  /** Registers a callback for a stream control event. */
  public on(event: 'end' | 'gap' | 'expired', callback: () => void): this
  public on(event: string, callback: (data?: unknown) => void): this {
    if (!this.closed) {
      const callbacks = this.listeners.get(event)
      if (callbacks) {
        callbacks.add(callback)
      } else {
        this.listeners.set(event, new Set([callback]))
        const events = new Set(this.listeners.keys())
        events.delete('end')
        events.delete('gap')
        events.delete('expired')
        events.add('murmur.end')
        this.transport.setEvents(events)
      }
    }
    return this
  }

  /** Stops the subscription, cancels retries, and releases its callbacks. */
  public close(): void {
    if (this.closed) {
      return
    }
    this.closed = true
    this.transport.close()
    for (const callbacks of this.listeners.values()) {
      callbacks.clear()
    }
    this.listeners.clear()
    const onClose = this.onClose
    this.onClose = undefined
    onClose?.(this)
  }

  /** Keeps the supplied URL until an event ID is received, then changes only the replay cursor. */
  protected connectionUrl(lastEventId: string | undefined): string {
    if (lastEventId === undefined) {
      return this.url
    }

    const fragmentIndex = this.url.indexOf('#')
    const address = fragmentIndex === -1 ? this.url : this.url.slice(0, fragmentIndex)
    const fragment = fragmentIndex === -1 ? '' : this.url.slice(fragmentIndex)
    const queryIndex = address.indexOf('?')
    const path = queryIndex === -1 ? address : address.slice(0, queryIndex)
    const query = queryIndex === -1 ? '' : address.slice(queryIndex + 1)
    const parameters = query === '' ? [] : query.split('&')
    const cursor = ['lastEventId=' + encodeURIComponent(lastEventId)]
    let found = false
    const updated = parameters.flatMap(parameter => {
      if (!new URLSearchParams('?' + parameter).has('lastEventId')) {
        return [parameter]
      }
      const replacement = found ? [] : cursor
      found = true
      return replacement
    })
    if (!found) {
      updated.push(...cursor)
    }
    return path + (updated.length ? '?' + updated.join('&') : '') + fragment
  }

  /** Resets the failure count and delivers control events or unwrapped data. */
  protected receive(event: string, data: string): void {
    this.transport.resetFailures()
    if (event === 'murmur.end') {
      this.finish('end')
      return
    }
    if (event === 'murmur.gap') {
      this.emit('gap')
      return
    }

    const envelope = parseEnvelope(data)
    if (envelope === undefined) {
      return
    }
    this.emit(event, envelope.data)
  }

  /** Delivers a data payload or gap notification to its listeners. */
  protected emit(event: string, ...args: [data?: unknown]): void {
    for (const listener of this.listeners.get(event) ?? []) {
      invokeListener(listener, ...args)
    }
  }

  /** Closes the connection before notifying listeners of terminal events. */
  protected finish(event: 'end' | 'expired'): void {
    const listeners = [...this.listeners.get(event) ?? []]
    this.close()
    for (const listener of listeners) {
      invokeListener(listener)
    }
  }
}
