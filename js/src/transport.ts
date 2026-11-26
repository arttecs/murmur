/** The current source and the listeners registered on it. */
type Connection = {
  /** The active event source. */
  source: EventSource
  /** Whether the source has opened. */
  opened: boolean
  /** Whether the server has acknowledged attachment. */
  attached: boolean
  /** The valid cursor sent in the request, if present. */
  requestedCursor?: string
  /** Handles a successful connection. */
  open: (event: Event) => void
  /** Handles a failed connection. */
  error: (event: Event) => void
  /** Handles a named stream message. */
  message: (event: Event) => void
}

/** Configures the delay between connection attempts. */
export interface RetryOptions {
  /** The delay before the first retry, in milliseconds. */
  retryMs: number

  /** The maximum base delay before jitter, in milliseconds. */
  maxRetryMs: number

  /** The maximum fraction of the base delay added as jitter after the first retry. */
  retryJitter: number
}

/** Rejects invalid values or combinations of retry options. */
export function validateRetryOptions({ retryMs, maxRetryMs, retryJitter }: RetryOptions): void {
  if (!Number.isFinite(retryMs) || retryMs <= 0) {
    throw new RangeError('retryMs must be finite and greater than zero')
  }
  if (!Number.isFinite(maxRetryMs) || maxRetryMs < retryMs) {
    throw new RangeError('maxRetryMs must be finite and greater than or equal to retryMs')
  }
  if (!Number.isFinite(retryJitter) || retryJitter < 0 || retryJitter > 1) {
    throw new RangeError('retryJitter must be finite and between 0 and 1')
  }
}

/** Carries a minimum delay requested by the endpoint for a failed attempt. */
export class RetryAfterError extends Error {
  /** Records the retry delay and the original failure. */
  public constructor(public readonly delayMs: number, cause: unknown) {
    super('Murmur connection attempt must wait before retrying', { cause })
  }
}

/** Checks whether an event carries a stream payload. */
function isMessage(event: Event): event is Event & { data: string; lastEventId?: unknown } {
  return typeof event === 'object' && event !== null && 'data' in event && typeof event.data === 'string'
}

/** Matches the protocol's complete event ID format. */
const eventIdPattern = /^[0-9]{1,18}-[0-9]{1,18}(?![\s\S])/

/** Reads a valid request cursor from an absolute or relative URL. */
function readRequestedCursor(url: string): string | undefined {
  const fragment = url.indexOf('#')
  const address = fragment === -1 ? url : url.slice(0, fragment)
  const query = address.indexOf('?')
  const cursor = query === -1 ? null : new URLSearchParams(address.slice(query + 1)).get('lastEventId')
  return cursor !== null && eventIdPattern.test(cursor) ? cursor : undefined
}

/** Compares a candidate ID with the cursor without losing decimal precision. */
function isLaterEventId(candidate: string, cursor: string): boolean {
  const candidateSeparator = candidate.indexOf('-')
  const cursorSeparator = cursor.indexOf('-')
  const candidateTime = BigInt(candidate.slice(0, candidateSeparator))
  const cursorTime = BigInt(cursor.slice(0, cursorSeparator))
  if (candidateTime !== cursorTime) {
    return candidateTime > cursorTime
  }
  return BigInt(candidate.slice(candidateSeparator + 1)) > BigInt(cursor.slice(cursorSeparator + 1))
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

  /** The latest replay position acknowledged by the server or an application event. */
  protected lastEventId?: string

  /** The wire events that the connection should listen for. */
  protected events = new Set(['murmur.gap', 'murmur.connected', 'murmur.attached', 'murmur.heartbeat'])

  /** Creates a transport that prepares a fresh URL for every attempt. */
  public constructor(
    protected readonly eventSource: typeof EventSource | undefined,
    protected readonly withCredentials: boolean,
    protected readonly retryOptions: RetryOptions,
    protected readonly connectionUrl: (signal: AbortSignal, lastEventId: string | undefined) => string | undefined | Promise<string | undefined>,
    protected readonly onMessage: (event: string, data: string) => void,
    protected readonly onStatus: (event: 'connected' | 'disconnected', failures: number) => void,
    protected readonly onUnopenedError?: (url: string, status: number | undefined) => void,
  ) {}

  /** Reports whether the active source has opened. */
  public get connected(): boolean {
    return this.connection?.opened ?? false
  }

  /** Returns the cursor used when opening the next source. */
  public get replayCursor(): string | undefined {
    return this.lastEventId
  }

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
    events.add('murmur.connected')
    events.add('murmur.attached')
    events.add('murmur.heartbeat')
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
      if (attempt.signal.aborted || url === undefined) {
        return
      }

      const EventSource = this.eventSource ?? globalThis.EventSource
      const source = this.withCredentials ? new EventSource(url, { withCredentials: true }) : new EventSource(url)
      this.attach(source, url)
    } catch (error) {
      if (!attempt.signal.aborted) {
        this.handleFailure(error instanceof RetryAfterError ? error.delayMs : undefined)
      }
    } finally {
      if (this.pending === attempt) {
        this.pending = undefined
      }
    }
  }

  /** Binds listeners to the active source, including polyfills without currentTarget. */
  protected attach(source: EventSource, url: string): void {
    const connection: Connection = {
      source,
      opened: false,
      attached: false,
      requestedCursor: this.lastEventId ?? readRequestedCursor(url),
      open: event => {
        if (this.connection === connection && !isMessage(event)) {
          connection.opened = true
          this.resetFailures()
          this.onStatus('connected', 0)
        }
      },
      error: event => {
        if (this.connection === connection && !isMessage(event)) {
          if (!connection.opened) {
            const status = 'status' in event && typeof event.status === 'number' ? event.status
              : 'code' in event && typeof event.code === 'number' ? event.code
                : undefined
            this.onUnopenedError?.(url, status)
          }
          this.handleFailure()
        }
      },
      message: event => {
        if (this.connection === connection && isMessage(event)) {
          this.receive(connection, event.type, event.data, event.lastEventId)
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
  protected retry(minimumDelay = 0): void {
    const { retryMs, maxRetryMs, retryJitter } = this.retryOptions
    let delay = retryMs
    if (this.failures > 1) {
      delay = Math.min(retryMs * 2 ** (this.failures - 1), maxRetryMs)
      if (retryJitter !== 0) {
        delay += delay * retryJitter * Math.random()
      }
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined
      void this.open()
    }, Math.max(delay, Math.min(minimumDelay, maxRetryMs)))
  }

  /** Closes the failed connection, schedules a retry, and reports the failure. */
  protected handleFailure(minimumDelay?: number): void {
    this.close()
    this.failures++
    this.retry(minimumDelay)
    this.onStatus('disconnected', this.failures)
  }

  /** Captures the resume cursor and dispatches named stream data. */
  protected receive(connection: Connection, event: string, data: string, lastEventId: unknown): void {
    const id = typeof lastEventId === 'string' && eventIdPattern.test(lastEventId) ? lastEventId : undefined
    if (event === 'murmur.heartbeat') {
      const cursor = this.lastEventId ?? connection.requestedCursor
      if (connection.attached && id !== undefined && (cursor === undefined || isLaterEventId(id, cursor))) {
        this.lastEventId = id
      }
      return
    }
    if (event === 'murmur.attached') {
      connection.attached = true
    }
    if (id !== undefined && event !== 'murmur.gap' && event !== 'murmur.connected') {
      this.lastEventId = id
    }
    this.onMessage(event, data)
  }
}
