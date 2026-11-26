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

  /** The instance constant for a connection attempt. */
  public readonly CONNECTING = FakeEventSource.CONNECTING
  /** The instance constant for an open connection. */
  public readonly OPEN = FakeEventSource.OPEN
  /** The instance constant for a closed connection. */
  public readonly CLOSED = FakeEventSource.CLOSED
  /** The URL supplied to the constructor. */
  public readonly url: string
  /** Whether the connection was configured to send credentials. */
  public readonly withCredentials: boolean
  /** The observable EventSource state. */
  public readyState: number = this.CONNECTING
  /** The EventSource handler property for open events. */
  public onopen: EventSource['onopen'] = null
  /** The EventSource handler property for unnamed messages. */
  public onmessage: EventSource['onmessage'] = null
  /** The EventSource handler property for connection errors. */
  public onerror: EventSource['onerror'] = null
  /** Whether the source has been closed. */
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
