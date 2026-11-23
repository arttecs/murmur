/** Records connections and delivers events through the EventTarget interface. */
export class FakeEventSource extends EventTarget implements EventSource {
  /** The EventSource connecting state. */
  public static readonly CONNECTING = 0

  /** The EventSource open state. */
  public static readonly OPEN = 1

  /** The EventSource closed state. */
  public static readonly CLOSED = 2

  /** The connections created during the current test. */
  public static readonly instances: FakeEventSource[] = []

  public readonly CONNECTING = FakeEventSource.CONNECTING
  public readonly OPEN = FakeEventSource.OPEN
  public readonly CLOSED = FakeEventSource.CLOSED
  public readonly url: string
  public readonly withCredentials: boolean
  public readyState: number = this.CONNECTING
  public onopen: EventSource['onopen'] = null
  public onmessage: EventSource['onmessage'] = null
  public onerror: EventSource['onerror'] = null
  public closed = false

  /** Records the URL and credentials of a new connection. */
  public constructor(url: string | URL, options?: EventSourceInit) {
    super()
    this.url = String(url)
    this.withCredentials = options?.withCredentials ?? false
    FakeEventSource.instances.push(this)
  }

  /** Registers a listener for a lifecycle or named message event. */
  public override addEventListener<E extends Event>(
    type: string,
    listener: ((event: E) => void) | EventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void {
    super.addEventListener(type, listener as EventListenerOrEventListenerObject | null, options)
  }

  /** Removes a listener for a lifecycle or named message event. */
  public override removeEventListener<E extends Event>(
    type: string,
    listener: ((event: E) => void) | EventListenerObject | null,
    options?: boolean | EventListenerOptions,
  ): void {
    super.removeEventListener(type, listener as EventListenerOrEventListenerObject | null, options)
  }

  /** Delivers a named event with a JSON payload and cursor. */
  public emit(type: string, data: unknown, lastEventId: string): void {
    if (!this.closed) {
      this.dispatchEvent(new MessageEvent(type, { data: JSON.stringify(data), lastEventId }))
    }
  }

  /** Delivers a connection error. */
  public fail(): void {
    if (!this.closed) {
      this.dispatchEvent(new Event('error'))
    }
  }

  /** Marks this connection as closed. */
  public close(): void {
    this.closed = true
    this.readyState = this.CLOSED
  }
}
