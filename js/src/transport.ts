/** Owns the EventSource lifecycle, retry timer and replay cursor. */
export class Transport {
  /** The current native connection. */
  protected source?: EventSource

  /** The controller for URL preparation on the current attempt. */
  protected pending?: AbortController

  /** The time scheduled for the next attempt. */
  protected retryTimer?: ReturnType<typeof setTimeout>

  /** The ID of the last received data event. */
  protected lastEventId?: string

  /** The wire events that the connection should listen for. */
  protected events = new Set(['murmur.gap'])

  /** Creates a transport that prepares a fresh URL for every attempt. */
  public constructor(
    protected readonly eventSource: typeof EventSource | undefined,
    protected readonly retryMs: number,
    protected readonly connectionUrl: (signal: AbortSignal) => Promise<URL>,
    protected readonly onMessage: (event: string, data: string) => void,
    protected readonly onStatus: (event: 'connected' | 'disconnected') => void,
  ) {}

  /** Replaces the active connection with a new attempt. */
  public connect(): void {
    this.close()
    void this.open()
  }

  /** Updates named listeners on the active connection and future attempts. */
  public setEvents(events: Set<string>): void {
    events.add('murmur.gap')
    const source = this.source
    if (source) {
      for (const event of this.events) {
        if (!events.has(event)) {
          source.removeEventListener(event, this.handleMessage)
        }
      }
      for (const event of events) {
        if (!this.events.has(event)) {
          source.addEventListener(event, this.handleMessage)
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
    const source = this.source
    if (!source) {
      return
    }

    this.source = undefined
    source.removeEventListener('open', this.handleOpen)
    source.removeEventListener('error', this.handleError)
    for (const event of this.events) {
      source.removeEventListener(event, this.handleMessage)
    }
    source.close()
  }

  /** Prepares the connection and binds its named events. */
  protected async open(): Promise<void> {
    const attempt = new AbortController()
    this.pending = attempt
    try {
      const url = await this.connectionUrl(attempt.signal)
      if (attempt.signal.aborted) {
        return
      }

      url.searchParams.delete('lastEventId')
      if (this.lastEventId !== undefined) {
        url.searchParams.set('lastEventId', this.lastEventId)
      }
      const EventSource = this.eventSource ?? globalThis.EventSource
      const source = new EventSource(url.href)
      this.source = source
      source.addEventListener('open', this.handleOpen)
      source.addEventListener('error', this.handleError)
      for (const event of this.events) {
        source.addEventListener(event, this.handleMessage)
      }
    } catch {
      if (!attempt.signal.aborted) {
        this.close()
        this.retry()
      }
    } finally {
      if (this.pending === attempt) {
        this.pending = undefined
      }
    }
  }

  /** Schedules a replacement connection after the retry delay. */
  protected retry(): void {
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined
      void this.open()
    }, this.retryMs)
  }

  /** Reports that the native connection opened. */
  protected readonly handleOpen = (event: Event): void => {
    if (event.currentTarget === this.source && !(event instanceof MessageEvent)) {
      this.onStatus('connected')
    }
  }

  /** Closes the failed connection and schedules a client-managed retry. */
  protected readonly handleError = (event: Event): void => {
    if (event.currentTarget === this.source && !(event instanceof MessageEvent)) {
      this.close()
      this.retry()
      this.onStatus('disconnected')
    }
  }

  /** Captures the resume cursor and dispatches named stream data. */
  protected readonly handleMessage = (event: Event): void => {
    if (event.currentTarget !== this.source || !(event instanceof MessageEvent)) {
      return
    }

    if (event.type !== 'murmur.gap' && event.lastEventId) {
      this.lastEventId = event.lastEventId
    }
    const data: unknown = event.data
    if (typeof data === 'string') {
      this.onMessage(event.type, data)
    }
  }
}
