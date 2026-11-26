import { createServer, type Server as HttpServer } from 'node:http'
import { EventSource } from 'eventsource'
import Echo from 'laravel-echo'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { MurmurConnector } from '../../src/echo'
import { Murmur, type MurmurOptions } from '../../src/index'
import { close, listen, publish, startMurmur, tokenFor } from './server'

/** A request made by the real EventSource and the server's response status. */
type Attempt = {
  /** The address the client requested. */
  url: URL
  /** The status returned by the server. */
  status?: number
  /** The cursor sent by the server's attach marker. */
  attached?: string
  /** Aborts the HTTP response without affecting other SSE requests. */
  interrupt: () => void
}

/** The server shared by the integration cases. */
let murmur: Awaited<ReturnType<typeof startMurmur>>

/** The auth endpoint used by the current case. */
let authServer: HttpServer | undefined

/** Clients to close after the current case. */
const clients: Murmur[] = []

/** The deadline and polling interval for client callbacks. */
const waitOptions = { timeout: 3500, interval: 20 }

/** The base retry delay for cases that count terminal stream attempts. */
const streamRetryMs = 25

/** Starts a local authorization endpoint and records the client's JSON requests. */
async function startAuth(
  authorize: (channel: string, attempt: number) => ReturnType<typeof tokenFor> | 403 | Promise<ReturnType<typeof tokenFor> | 403>,
): Promise<{ endpoint: string; requests: unknown[] }> {
  const requests: unknown[] = []
  authServer = createServer(async (request, response) => {
    if (request.method !== 'POST' || request.url !== '/broadcasting/auth') {
      response.writeHead(404).end()
      return
    }

    try {
      let raw = ''
      for await (const chunk of request.setEncoding('utf8')) {
        raw += chunk
      }
      const body: unknown = JSON.parse(raw)
      requests.push(body)
      if (body === null || typeof body !== 'object' || !('channel_name' in body) ||
        typeof body.channel_name !== 'string') {
        response.writeHead(400).end()
        return
      }
      const grant = await authorize(body.channel_name, requests.length)
      if (grant === 403) {
        response.writeHead(403).end()
        return
      }
      response.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(grant))
    } catch {
      response.writeHead(500).end()
    }
  })
  const port = await listen(authServer)
  return { endpoint: `http://127.0.0.1:${port}/broadcasting/auth`, requests }
}

/** Creates a client that records real SSE responses and can gate reconnect requests. */
function client(
  options: Partial<MurmurOptions> = {},
  attempts: Attempt[] = [],
  beforeReconnect?: () => Promise<void>,
): Murmur {
  /** Observes SSE requests without changing the wire data or EventSource behavior. */
  class ObservedEventSource extends EventSource {
    /** Opens the real EventSource and records its HTTP requests. */
    public constructor(url: string | URL, init?: EventSourceInit) {
      let current: Attempt | undefined
      super(url, {
        ...init,
        fetch: async (url, request) => {
          if (beforeReconnect && attempts.length > 0) {
            await beforeReconnect()
          }
          const controller = new AbortController()
          const attempt: Attempt = { url: new URL(url), interrupt: () => controller.abort(new Error('SSE connection interrupted')) }
          current = attempt
          attempts.push(attempt)
          const signal = request?.signal
            ? AbortSignal.any([request.signal, controller.signal])
            : controller.signal
          const response = await fetch(url, { ...request, signal })
          attempt.status = response.status
          return response
        },
      })
      this.addEventListener('murmur.attached', (event: MessageEvent) => {
        if (current) {
          current.attached = event.lastEventId
        }
      })
    }
  }

  const m = new Murmur({ url: `${murmur.address}/sse`, EventSource: ObservedEventSource, ...options })
  clients.push(m)
  return m
}

/** Builds the URL the Laravel stream bridge issues for a private topic. */
function streamUrl(topic: string, cursor: string, ttl: number): string {
  const { token } = tokenFor([topic], ttl)
  return `${murmur.address}/sse?topics=${encodeURIComponent(topic)}&lastEventId=${encodeURIComponent(cursor)}&token=${encodeURIComponent(token)}`
}

/** Checks that no further request starts across a full retry delay including jitter. */
async function expectNoRetry(attempts: Attempt[], count: number, retryDelay: number): Promise<void> {
  const deadline = performance.now() + retryDelay
  await vi.waitUntil(() => attempts.length > count || performance.now() >= deadline, {
    timeout: retryDelay + 1000,
    interval: 10,
  })
  expect(attempts).toHaveLength(count)
}

describe('Murmur client and PHP server', () => {
  beforeAll(async () => {
    murmur = await startMurmur()
  }, 10_000)

  afterEach(async () => {
    for (const m of clients.splice(0)) {
      m.close()
    }
    if (authServer) {
      await close(authServer)
      authServer = undefined
    }
  })

  afterAll(async () => {
    await murmur?.stop()
  })

  it('delivers the unwrapped payload to a public channel', async () => {
    const connected = vi.fn()
    const updated = vi.fn<(data: unknown) => void>()
    const m = client()
    m.on('connected', connected)
    m.channel('news').listen('.updated', updated)
    await vi.waitFor(() => expect(connected).toHaveBeenCalledExactlyOnceWith(), waitOptions)

    const payload = { headline: 'Meet me at the café', tags: ['local'] }
    await publish(murmur.address, 'news', 'updated', payload)
    await vi.waitFor(() => expect(updated).toHaveBeenCalledExactlyOnceWith(payload), waitOptions)
  })

  it('authorizes a private channel and refuses a topic outside the token grant', async () => {
    const allowed = 'private-orders.42'
    const { endpoint, requests } = await startAuth(() => tokenFor([allowed], 60))
    const connected = vi.fn()
    const shipped = vi.fn<(data: unknown) => void>()
    const authorized = client({ authEndpoint: endpoint })
    authorized.on('connected', connected)
    authorized.private('orders.42').listen('.shipped', shipped)
    await vi.waitFor(() => expect(connected).toHaveBeenCalledExactlyOnceWith(), waitOptions)

    const payload = { order: 42 }
    await publish(murmur.address, allowed, 'shipped', payload)
    await vi.waitFor(() => expect(shipped).toHaveBeenCalledExactlyOnceWith(payload), waitOptions)

    const attempts: Attempt[] = []
    const refused = client({ authEndpoint: endpoint }, attempts)
    const opened = vi.fn()
    const disconnected = vi.fn()
    const received = vi.fn<(data: unknown) => void>()
    refused.on('connected', opened)
    refused.on('disconnected', disconnected)
    refused.private('orders.43').listen('.shipped', received)
    await vi.waitFor(() => expect(disconnected).toHaveBeenCalledExactlyOnceWith(), waitOptions)

    await publish(murmur.address, 'private-orders.43', 'shipped', { order: 43 })
    expect(requests).toEqual([{ channel_name: allowed }, { channel_name: 'private-orders.43' }])
    expect(attempts.map(attempt => attempt.status)).toEqual([403])
    expect(opened).not.toHaveBeenCalled()
    expect(received).not.toHaveBeenCalled()
  })

  it('delivers on multiple private channels with separate tokens when one channel is denied', async () => {
    const { endpoint, requests } = await startAuth(channel =>
      channel === 'private-denied' ? 403 : tokenFor([channel], 60),
    )
    const attempts: Attempt[] = []
    const m = client({ authEndpoint: endpoint }, attempts)
    const connected = vi.fn()
    const unauthorized = vi.fn()
    const denied = vi.fn()
    const first = vi.fn()
    const second = vi.fn()
    m.on('connected', connected)
    m.on('unauthorized', unauthorized)
    m.private('first').listen('.updated', first)
    m.private('denied').listen('.updated', denied)
    m.private('second').listen('.updated', second)
    await vi.waitFor(() => expect(connected).toHaveBeenCalledExactlyOnceWith(), waitOptions)

    await publish(murmur.address, 'private-denied', 'updated', { value: 0 })
    await publish(murmur.address, 'private-first', 'updated', { value: 1 })
    await publish(murmur.address, 'private-second', 'updated', { value: 2 })
    await vi.waitFor(() => expect(second).toHaveBeenCalledExactlyOnceWith({ value: 2 }), waitOptions)

    expect(first).toHaveBeenCalledExactlyOnceWith({ value: 1 })
    expect(denied).not.toHaveBeenCalled()
    expect(unauthorized).toHaveBeenCalledExactlyOnceWith(403, 'denied')
    expect(requests).toHaveLength(3)
    expect(requests).toEqual(expect.arrayContaining([
      { channel_name: 'private-first' }, { channel_name: 'private-denied' }, { channel_name: 'private-second' },
    ]))
    expect(attempts).toHaveLength(1)
    expect(attempts[0]?.url.searchParams.get('topics')).toBe('private-first,private-second')
    expect(attempts[0]?.url.searchParams.get('token')?.split(',')).toHaveLength(2)
  })

  it('resolves namespaced and literal channel listeners from real event names', async () => {
    const connected = vi.fn()
    const namespaced = vi.fn<(data: unknown) => void>()
    const literal = vi.fn<(data: unknown) => void>()
    const m = client()
    m.on('connected', connected)
    m.channel('orders').listen('OrderShipped', namespaced).listen('.OrderShipped', literal)
    await vi.waitFor(() => expect(connected).toHaveBeenCalledExactlyOnceWith(), waitOptions)

    await publish(murmur.address, 'orders', 'App\\Events\\OrderShipped', { order: 1 })
    await publish(murmur.address, 'orders', 'OrderShipped', { order: 2 })
    await vi.waitFor(() => expect(literal).toHaveBeenCalledExactlyOnceWith({ order: 2 }), waitOptions)
    expect(namespaced).toHaveBeenCalledExactlyOnceWith({ order: 1 })
  })

  it('renews an expired channel token and replays the missed event exactly once', async () => {
    const topic = 'private-renewal'
    let resume: (() => void) | undefined
    const missedPublished = new Promise<void>(resolve => { resume = resolve })
    const { endpoint, requests } = await startAuth(async (channel, attempt) => {
      if (attempt === 1) {
        return tokenFor([channel], 2)
      }
      await missedPublished
      return tokenFor([channel], 60)
    })
    const attempts: Attempt[] = []
    const connected = vi.fn()
    const disconnected = vi.fn()
    const received = vi.fn<(data: unknown) => void>()
    const m = client({ authEndpoint: endpoint, retryMs: 20 }, attempts)
    m.on('connected', connected)
    m.on('disconnected', disconnected)
    m.private('renewal').listen('.updated', received)

    let firstId: string
    try {
      await vi.waitFor(() => expect(connected).toHaveBeenCalledExactlyOnceWith(), waitOptions)
      firstId = await publish(murmur.address, topic, 'updated', { phase: 'initial' })
      await vi.waitFor(() => expect(received).toHaveBeenCalledExactlyOnceWith({ phase: 'initial' }), waitOptions)
      await vi.waitFor(() => expect(disconnected).toHaveBeenCalledExactlyOnceWith(), waitOptions)

      await publish(murmur.address, topic, 'updated', { phase: 'missed' })
      expect(connected).toHaveBeenCalledTimes(1)
      expect(received).toHaveBeenCalledExactlyOnceWith({ phase: 'initial' })
    } finally {
      resume?.()
    }

    await vi.waitFor(() => expect(connected).toHaveBeenCalledTimes(2), waitOptions)
    const lastId = await publish(murmur.address, topic, 'updated', { phase: 'live again' })
    await vi.waitFor(() => expect(received).toHaveBeenLastCalledWith({ phase: 'live again' }), waitOptions)

    expect(received.mock.calls).toEqual([[{ phase: 'initial' }], [{ phase: 'missed' }], [{ phase: 'live again' }]])
    expect(disconnected).toHaveBeenCalledExactlyOnceWith()
    expect(requests).toEqual([{ channel_name: topic }, { channel_name: topic, socket_id: m.socketId() }])
    expect(attempts.map(attempt => attempt.status)).toEqual([200, 200])
    expect(attempts[1]?.url.searchParams.get('token')).not.toBe(attempts[0]?.url.searchParams.get('token'))
    expect(attempts[1]?.url.searchParams.get('lastEventId')).toBe(firstId)
    expect(m.stats()).toMatchObject({ connections: 2, events: 3, lastEventId: lastId })
  })

  it('skips pre-join history and replays outage events after a new channel attaches', async () => {
    const attempts: Attempt[] = []
    const m = client({ retryMs: 250 }, attempts)
    const first = vi.fn()
    const second = vi.fn()
    const disconnected = vi.fn()
    m.on('disconnected', disconnected)
    m.channel('first-join').listen('.updated', first)
    await vi.waitFor(() => expect(m.socketId()).toBeDefined(), waitOptions)
    await publish(murmur.address, 'first-join', 'updated', 'initial')
    await vi.waitFor(() => expect(first).toHaveBeenCalledExactlyOnceWith('initial'), waitOptions)

    const beforeJoin = await publish(murmur.address, 'second-join', 'updated', 'before join')
    m.channel('second-join').listen('.updated', second)
    await vi.waitFor(() => expect(m.stats().lastEventId).toBe(beforeJoin), waitOptions)
    expect(attempts[1]?.url.searchParams.get('fresh')).toBe('second-join')
    expect(second).not.toHaveBeenCalled()

    attempts[1]?.interrupt()
    await vi.waitFor(() => expect(disconnected).toHaveBeenCalledExactlyOnceWith(), waitOptions)
    const duringOutage = await publish(murmur.address, 'second-join', 'updated', 'during outage')
    expect(attempts).toHaveLength(2)

    await vi.waitFor(() => expect(second).toHaveBeenCalledExactlyOnceWith('during outage'), waitOptions)
    expect(attempts).toHaveLength(3)
    expect(attempts[2]?.url.searchParams.has('fresh')).toBe(false)
    expect(attempts[2]?.url.searchParams.get('lastEventId')).toBe(beforeJoin)
    expect(m.stats().lastEventId).toBe(duringOutage)

    await publish(murmur.address, 'second-join', 'updated', 'live again')
    await vi.waitFor(() => expect(second).toHaveBeenLastCalledWith('live again'), waitOptions)
    expect(second.mock.calls).toEqual([['during outage'], ['live again']])
  })

  it('delivers private broadcasts through Echo after the SSE request is interrupted', async () => {
    const topic = 'private-echo-orders.42'
    const { endpoint, requests } = await startAuth(channel => tokenFor([channel], 60))
    const connections: AbortController[] = []

    /** Uses real HTTP with a signal that can interrupt the current response. */
    class InterruptibleEventSource extends EventSource {
      /** Opens an SSE connection that the test can interrupt. */
      public constructor(url: string | URL, init?: EventSourceInit) {
        const interrupt = new AbortController()
        connections.push(interrupt)
        super(url, {
          ...init,
          fetch: (url, request) => {
            const signal = request?.signal
              ? AbortSignal.any([request.signal, interrupt.signal])
              : interrupt.signal
            return fetch(url, { ...request, signal })
          },
        })
      }
    }

    const echo = new Echo({
      broadcaster: MurmurConnector,
      url: `${murmur.address}/sse`,
      authEndpoint: endpoint,
      EventSource: InterruptibleEventSource,
      retryMs: 20,
    })
    const subscribed = vi.fn()
    const received = vi.fn<(payload: unknown) => void>()
    try {
      echo.private('echo-orders.42').listen('OrderShipped', received).subscribed(subscribed)
      await vi.waitFor(() => expect(subscribed).toHaveBeenCalledExactlyOnceWith(), waitOptions)
      await publish(murmur.address, topic, 'App\\Events\\OrderShipped', { order: 42, phase: 'first' })
      await vi.waitFor(() => expect(received).toHaveBeenCalledExactlyOnceWith({ order: 42, phase: 'first' }), waitOptions)

      expect(connections).toHaveLength(1)
      connections[0]?.abort(new Error('SSE connection interrupted'))
      await vi.waitFor(() => expect(subscribed).toHaveBeenCalledTimes(2), waitOptions)
      await publish(murmur.address, topic, 'App\\Events\\OrderShipped', { order: 42, phase: 'second' })
      await vi.waitFor(() => expect(received.mock.calls).toEqual([
        [{ order: 42, phase: 'first' }],
        [{ order: 42, phase: 'second' }],
      ]), waitOptions)

      expect(connections).toHaveLength(2)
      expect(requests).toEqual([{ channel_name: topic }])
    } finally {
      echo.disconnect()
    }
  })

  it('excludes the sender by its announced ID during live delivery and replay', async () => {
    const attempts: Attempt[] = []
    let resumeReconnect: (() => void) | undefined
    const excludedPublished = new Promise<void>(resolve => { resumeReconnect = resolve })
    const beforeReconnect = vi.fn(() => excludedPublished)
    const sender = client({ retryMs: 1 }, attempts, beforeReconnect)
    const recipient = client()
    const disconnected = vi.fn()
    const senderUpdates = vi.fn()
    const recipientUpdates = vi.fn()
    sender.on('disconnected', disconnected)
    sender.channel('exclusion').listen('.updated', senderUpdates)
    recipient.channel('exclusion').listen('.updated', recipientUpdates)
    await vi.waitFor(() => {
      expect(sender.socketId()).toMatch(/^[a-f0-9]{32}$/)
      expect(recipient.socketId()).toMatch(/^[a-f0-9]{32}$/)
    }, waitOptions)

    const id = sender.socketId()
    expect(id).not.toBe(recipient.socketId())
    await publish(murmur.address, 'exclusion', 'updated', 'excluded live', id)
    const firstId = await publish(murmur.address, 'exclusion', 'updated', 'first')
    await vi.waitFor(() => expect(senderUpdates).toHaveBeenCalledExactlyOnceWith('first'), waitOptions)

    attempts[0]?.interrupt()
    try {
      await vi.waitFor(() => expect(beforeReconnect).toHaveBeenCalledExactlyOnceWith(), waitOptions)
      expect(disconnected).toHaveBeenCalledExactlyOnceWith()
      await publish(murmur.address, 'exclusion', 'updated', 'excluded during outage', id)
      expect(attempts).toHaveLength(1)
    } finally {
      resumeReconnect?.()
    }
    await publish(murmur.address, 'exclusion', 'updated', 'second')
    await vi.waitFor(() => {
      expect(senderUpdates).toHaveBeenLastCalledWith('second')
      expect(recipientUpdates).toHaveBeenLastCalledWith('second')
    }, waitOptions)

    expect(attempts).toHaveLength(2)
    expect(attempts[1]?.url.searchParams.get('connection')).toBe(id)
    expect(attempts[1]?.url.searchParams.get('lastEventId')).toBe(firstId)
    expect(senderUpdates.mock.calls).toEqual([['first'], ['second']])
    expect(recipientUpdates.mock.calls).toEqual([
      ['excluded live'], ['first'], ['excluded during outage'], ['second'],
    ])
  })

  it('resumes a stream at the numbered marker when its requested cursor was ahead of the log', async () => {
    const topic = 'private-cursor-correction'
    const tail = await publish(murmur.address, topic, 'entry', 'stored before the join')
    const ahead = '999999999999999999-0'
    const received = vi.fn()
    const attempts: Attempt[] = []
    client({ retryMs: 25 }, attempts).stream(streamUrl(topic, ahead, 60)).on('entry', received)
    await vi.waitFor(() => expect(attempts[0]?.attached).toBe(tail), waitOptions)
    expect(attempts[0]?.url.searchParams.get('lastEventId')).toBe(ahead)
    expect(received).not.toHaveBeenCalled()

    attempts[0]?.interrupt()
    await vi.waitFor(() => expect(attempts[1]?.attached).toBe(tail), waitOptions)
    expect(attempts[1]?.url.searchParams.get('lastEventId')).toBe(tail)
    expect(received).not.toHaveBeenCalled()
  })

  it('reports a gap when the stream cursor predates the log', async () => {
    const topic = 'private-gap'
    const gap = vi.fn()
    const received = vi.fn<(data: unknown) => void>()
    const payload = { entry: 'retained' }
    await publish(murmur.address, topic, 'entry', payload)

    client().stream(streamUrl(topic, '0-0', 60)).on('gap', gap).on('entry', received)
    await vi.waitFor(() => expect(received).toHaveBeenCalledExactlyOnceWith(payload), waitOptions)
    expect(gap).toHaveBeenCalledExactlyOnceWith()
  })

  it('replays a Laravel stream and stops permanently after its end marker', async () => {
    const topic = 'private-stream'
    const cursor = `${Date.now() - 1000}-0`
    const url = streamUrl(topic, cursor, 60)
    await publish(murmur.address, topic, 'entry', { part: 1 })
    await publish(murmur.address, topic, 'entry', { part: 2 })
    await publish(murmur.address, topic, 'murmur.end', null)

    const attempts: Attempt[] = []
    const delivered: unknown[] = []
    const ended = vi.fn(() => delivered.push('end'))
    const expired = vi.fn()
    client({ retryMs: streamRetryMs }, attempts).stream(url)
      .on('entry', data => delivered.push(data))
      .on('end', ended)
      .on('expired', expired)
    await vi.waitFor(() => expect(ended).toHaveBeenCalledExactlyOnceWith(), waitOptions)
    await vi.waitFor(async () => {
      const health = await fetch(`${murmur.address}/health`, { signal: AbortSignal.timeout(1000) })
      expect(await health.json()).toEqual({ status: 'ok', connections: 0 })
    }, waitOptions)
    await expectNoRetry(attempts, 1, streamRetryMs * 2)

    expect(delivered).toEqual([{ part: 1 }, { part: 2 }, 'end'])
    expect(expired).not.toHaveBeenCalled()
    expect(attempts[0]?.status).toBe(200)
    expect(attempts[0]?.url.href).toBe(url)
  })

  it('reports an expired stream after exactly three refused requests', async () => {
    const attempts: Attempt[] = []
    const url = streamUrl('private-expired', `${Date.now()}-0`, -1)
    const expired = vi.fn()
    const ended = vi.fn()
    const received = vi.fn<(data: unknown) => void>()
    client({ retryMs: streamRetryMs }, attempts).stream(url, { maxFailures: 3 })
      .on('expired', expired)
      .on('end', ended)
      .on('entry', received)
    await vi.waitFor(() => expect(expired).toHaveBeenCalledExactlyOnceWith(), waitOptions)
    await expectNoRetry(attempts, 3, streamRetryMs * 6)

    expect(attempts.map(attempt => [attempt.url.href, attempt.status])).toEqual([[url, 401], [url, 401], [url, 401]])
    expect(ended).not.toHaveBeenCalled()
    expect(received).not.toHaveBeenCalled()
  })
})
