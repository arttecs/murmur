/** The current source and the listeners registered on it. */
type Connection = {
  source: EventSource
  open: (event: Event) => void
  error: (event: Event) => void
  message: (event: Event) => void
}

/** Checks whether an event carries a stream payload. */
function isMessage(event: Event): event is Event & { data: string; lastEventId?: unknown } {
  return typeof event === 'object' && event !== null && 'data' in event && typeof event.data === 'string'
}

/** Owns the EventSource lifecycle, retry timer and replay cursor. */
export class Transport {
  /** The current connection and its registered listeners. */
  protected connection?: Connection

  /** The controller for URL preparation on the current attempt. */
  protected pending?: AbortController

  /** The time scheduled for the next attempt. */
  protected retryTimer?: ReturnType<typeof setTimeout>

  /** The number of failures since the last successful connection. */
  protected failures = 0

  /** The ID of the last received data event. */
  protected lastEventId?: string

  /** The wire events that the connection should listen for. */
  protected events = new Set(['murmur.gap'])

  /** Creates a transport that prepares a fresh URL for every attempt. */
  public constructor(
    protected readonly eventSource: typeof EventSource | undefined,
    protected readonly retryMs: number,
    protected readonly connectionUrl: (signal: AbortSignal, lastEventId: string | undefined) => string | Promise<string>,
    protected readonly onMessage: (event: string, data: string) => void,
    protected readonly onStatus: (event: 'connected' | 'disconnected', failures: number) => void,
    protected readonly onUnopenedError?: (url: string) => void,
  ) {}

  /** Replaces the active connection with a new attempt. */
  public connect(): void {
    this.close()
    void this.open()
  }

  /** Restarts the retry sequence after a successful connection or stream message. */
  public resetFailures(): void {
    this.failures = 0
  }

  /** Updates named listeners on the active connection and future attempts. */
  public setEvents(events: Set<string>): void {
    events.add('murmur.gap')
    const connection = this.connection
    if (connection) {
      const { source, message } = connection
      for (const event of this.events) {
        if (!events.has(event)) {
          source.removeEventListener(event, message)
        }
      }
      for (const event of events) {
        if (!this.events.has(event)) {
          source.addEventListener(event, message)
        }
      }
    }
    this.events = events
  }

  /** Cancels work that could open a connection for obsolete subscriptions. */
  public cancelPending(): void {
    this.pending?.abort()
    this.pending = undefined
    if (this.retryTimer !== undefined) {
      clearTimeout(this.retryTimer)
      this.retryTimer = undefined
    }
  }

  /** Stops the active connection and removes its listeners. */
  public close(): void {
    this.cancelPending()
    const connection = this.connection
    if (!connection) {
      return
    }

    this.connection = undefined
    const { source, open, error, message } = connection
    source.removeEventListener('open', open)
    source.removeEventListener('error', error)
    for (const event of this.events) {
      source.removeEventListener(event, message)
    }
    source.close()
  }

  /** Prepares the connection and binds its named events. */
  protected async open(): Promise<void> {
    const attempt = new AbortController()
    this.pending = attempt
    try {
      const url = await this.connectionUrl(attempt.signal, this.lastEventId)
      if (attempt.signal.aborted) {
        return
      }

      const EventSource = this.eventSource ?? globalThis.EventSource
      this.attach(new EventSource(url), url)
    } catch {
      if (!attempt.signal.aborted) {
        this.handleFailure()
      }
    } finally {
      if (this.pending === attempt) {
        this.pending = undefined
      }
    }
  }

  /** Binds listeners to the active source, including polyfills without currentTarget. */
  protected attach(source: EventSource, url: string): void {
    let opened = false
    const connection: Connection = {
      source,
      open: event => {
        if (this.connection === connection && !isMessage(event)) {
          opened = true
          this.resetFailures()
          this.onStatus('connected', 0)
        }
      },
      error: event => {
        if (this.connection === connection && !isMessage(event)) {
          if (!opened) {
            this.onUnopenedError?.(url)
          }
          this.handleFailure()
        }
      },
      message: event => {
        if (this.connection === connection && isMessage(event)) {
          this.receive(event.type, event.data, event.lastEventId)
        }
      },
    }
    this.connection = connection
    source.addEventListener('open', connection.open)
    source.addEventListener('error', connection.error)
    for (const event of this.events) {
      source.addEventListener(event, connection.message)
    }
  }

  /** Schedules a replacement connection with capped exponential backoff and jitter. */
  protected retry(): void {
    let delay = this.retryMs
    if (this.failures > 1) {
      delay = Math.min(this.retryMs * 2 ** (this.failures - 1), 30_000)
      delay += delay * 0.25 * Math.random()
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined
      void this.open()
    }, delay)
  }

  /** Closes the failed connection, schedules a retry, and reports the failure. */
  protected handleFailure(): void {
    this.close()
    this.failures++
    this.retry()
    this.onStatus('disconnected', this.failures)
  }

  /** Captures the resume cursor and dispatches named stream data. */
  protected receive(event: string, data: string, lastEventId: unknown): void {
    if (event !== 'murmur.gap' && typeof lastEventId === 'string' && lastEventId !== '') {
      this.lastEventId = lastEventId
    }
    this.onMessage(event, data)
  }
}
