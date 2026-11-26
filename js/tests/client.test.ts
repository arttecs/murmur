import { Buffer } from 'node:buffer'
import { afterEach, assert, beforeEach, expect, it, vi } from 'vitest'
import type { Authorizer } from '../src/index'
import { FakeEventSource } from './fake-event-source'
import { authFetch, client, failingThenOk, flush, nowSeconds, reset, topicsOf, wait } from './helpers'

/** Returns one default-shaped JWT per topic for request size tests. */
const sizedTokens: Authorizer = async topics => topics.map(topic => {
  const expires_at = 1_893_456_900
  const claims = Buffer.from(JSON.stringify({ topics: [topic], exp: expires_at })).toString('base64url')
  return { token: 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.' + claims + '.' + 'A'.repeat(43), expires_at }
})

/** The longest replay cursor accepted by the protocol. */
const maxCursor = '999999999999999999-999999999999999999'

/** A valid connection identifier for reconnect tests. */
const connectionId = '0123456789abcdef0123456789abcdef'

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
})

afterEach(() => {
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
  reset()
  vi.useRealTimers()
})

it('opens one connection for all channels joined in the same tick', async () => {
  const m = client()
  m.channel('news')
  m.channel('alerts')
  await flush()
  expect(FakeEventSource.instances).toHaveLength(1)
  assert.isDefined(FakeEventSource.instances[0])
  expect(topicsOf(FakeEventSource.instances[0])).toEqual(['news', 'alerts'])
})

it('prefixes private channels and fetches a token first', async () => {
  const m = client({ fetch: authFetch({ token: 'jwt-1', expires_at: nowSeconds() + 900 }) })
  m.private('orders.1')
  await flush()
  assert.isDefined(authFetch.calls[0])
  expect(authFetch.calls[0].body).toEqual({ channel_name: 'private-orders.1' })
  assert.isDefined(FakeEventSource.instances[0])
  const url = new URL(FakeEventSource.instances[0].url)
  expect(url.searchParams.get('topics')).toBe('private-orders.1')
  expect(url.searchParams.get('token')).toBe('jwt-1')
})

it.each([
  { nameAt: (index: number) => 'orders.' + String(index).padStart(4, '0'), capacity: 30 },
  { nameAt: (index: number) => 'orders.00000000-0000-4000-8000-' + String(index).padStart(12, '0'), capacity: 19 },
  { nameAt: (index: number) => 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'.charAt(index), capacity: 37 },
])('opens a subscription at capacity $capacity and rejects one more topic before opening', async ({ nameAt, capacity }) => {
  const accepted = client({ authorizer: sizedTokens })
  const rejected = client({ authorizer: sizedTokens })
  const error = vi.fn()
  rejected.on('error', error)
  for (let index = 0; index < capacity; index++) {
    accepted.private(nameAt(index))
    rejected.private(nameAt(index))
  }
  rejected.private(nameAt(capacity))
  await flush()

  expect(FakeEventSource.instances).toHaveLength(1)
  const [source] = FakeEventSource.instances
  assert.isDefined(source)
  expect(topicsOf(source)).toHaveLength(capacity)
  expect(error).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ message: expect.stringContaining('request budget') }))
})

it.each([false, true])('retries at capacity with attachment before the drop set to %s', async attached => {
  const m = client({ authorizer: sizedTokens, retryMs: 10 })
  const error = vi.fn()
  const disconnected = vi.fn()
  m.on('error', error)
  m.on('disconnected', disconnected)
  for (let index = 0; index < 30; index++) {
    m.private('orders.' + String(index).padStart(4, '0')).listen('.updated', () => {})
  }
  await flush()

  const [first] = FakeEventSource.instances
  assert.isDefined(first)
  first.dispatchEvent(new Event('open'))
  first.emit('murmur.connected', { connection_id: connectionId, protocol: 1 }, '')
  if (attached) {
    first.emit('murmur.attached', {}, maxCursor)
  } else {
    first.emit('updated', { topic: 'private-orders.0000', data: null }, maxCursor)
  }
  first.fail()
  expect(disconnected).toHaveBeenCalledExactlyOnceWith()
  await wait(10)

  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  const query = new URL(retry.url).searchParams
  expect(query.get('connection')).toBe(connectionId)
  expect(query.get('lastEventId')).toBe(maxCursor)
  expect(query.get('fresh')).toBe(attached ? null : topicsOf(retry).join(','))
  retry.dispatchEvent(new Event('open'))
  expect(m.stats().connected).toBe(true)
  expect(error).not.toHaveBeenCalled()
})

it.each([
  { announce: false, cursor: false },
  { announce: true, cursor: false },
  { announce: false, cursor: true },
  { announce: true, cursor: true },
])('reserves reconnect parameters with ID known=$announce and cursor known=$cursor', async ({ announce, cursor }) => {
  const m = client({ fetch: authFetch({ token: 'x'.repeat(6006), expires_at: nowSeconds() + 900 }) })
  const error = vi.fn()
  m.on('error', error)
  m.private('a')
  await flush()
  const [first] = FakeEventSource.instances
  assert.isDefined(first)
  if (announce) {
    first.emit('murmur.connected', { connection_id: connectionId, protocol: 1 }, '')
  }
  if (cursor) {
    first.emit('murmur.attached', {}, maxCursor)
  }
  m.channel('b')
  await flush()

  expect(FakeEventSource.instances).toHaveLength(1)
  expect(error).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ message: expect.stringContaining('request budget') }))
})

it('resolves namespaced and literal event names', async () => {
  const m = client()
  const seen: unknown[] = []
  m.channel('news').listen('OrderShipped', d => seen.push(d)).listen('.updated', d => seen.push(d))
  await flush()
  const es = FakeEventSource.instances[0]
  assert.isDefined(es)
  es.emit('App\\Events\\OrderShipped', { topic: 'news', data: { id: 1 } }, '5-0')
  es.emit('updated', { topic: 'news', data: { id: 2 } }, '6-0')
  expect(seen).toEqual([{ id: 1 }, { id: 2 }])
})

it('routes an event only to the channel of its topic', async () => {
  const m = client()
  const a: unknown[] = []
  const b: unknown[] = []
  m.channel('news').listen('.x', d => a.push(d))
  m.channel('alerts').listen('.x', d => b.push(d))
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  FakeEventSource.instances[0].emit('x', { topic: 'alerts', data: 1 }, '1-0')
  expect(a).toEqual([])
  expect(b).toEqual([1])
})

it('reconnects with the last received id', async () => {
  const m = client({ retryMs: 10 })
  m.channel('news').listen('.x', () => {})
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  FakeEventSource.instances[0].emit('x', { topic: 'news', data: 1 }, '7-3')
  FakeEventSource.instances[0].fail()
  await wait(20)
  assert.isDefined(FakeEventSource.instances[1])
  expect(new URL(FakeEventSource.instances[1].url).searchParams.get('lastEventId')).toBe('7-3')
  expect(FakeEventSource.instances[0].closed).toBe(true)
})

it('fetches a fresh token when the cached one is about to expire', async () => {
  const fetch = authFetch({ token: 'jwt-1', expires_at: nowSeconds() + 10 })
  const m = client({ fetch, retryMs: 10 })
  m.private('a')
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  FakeEventSource.instances[0].dispatchEvent(new Event('open'))
  FakeEventSource.instances[0].fail()
  await wait(20)
  expect(fetch.calls).toHaveLength(2)
})

it('reuses a token that is still valid', async () => {
  const fetch = authFetch({ token: 'jwt-1', expires_at: nowSeconds() + 900 })
  const m = client({ fetch, retryMs: 10 })
  m.private('a')
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  FakeEventSource.instances[0].dispatchEvent(new Event('open'))
  FakeEventSource.instances[0].fail()
  await wait(20)
  expect(fetch.calls).toHaveLength(1)
})

it.each([401, 403])('fetches a fresh token after a pre-open SSE rejection with status %s', async status => {
  const fetch = vi.fn<typeof globalThis.fetch>()
    .mockResolvedValueOnce(Response.json({ token: 'rejected', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(Response.json({ token: 'fresh', expires_at: nowSeconds() + 900 }))
  const m = client({ fetch, retryMs: 10 })
  m.private('orders')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(new URL(first.url).searchParams.get('token')).toBe('rejected')

  first.dispatchEvent(Object.assign(new Event('error'), { code: status }))
  await wait(10)

  expect(fetch).toHaveBeenCalledTimes(2)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('token')).toBe('fresh')
})

it('keeps retrying when the authorization endpoint fails', async () => {
  const fetch = failingThenOk()
  const m = client({ fetch, retryMs: 10 })
  m.private('a')
  await wait(30)
  expect(FakeEventSource.instances).toHaveLength(1)
})

it.each([408, 425, 429])('retries a built-in authorization response with status %s', async status => {
  const fetch = vi.fn(authFetch({ token: 'allowed', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(new Response(null, { status }))
  const m = client({ fetch, retryMs: 10 })
  const unauthorized = vi.fn()
  m.on('unauthorized', unauthorized)
  m.private('orders')
  await flush()

  expect(FakeEventSource.instances).toHaveLength(0)
  expect(unauthorized).not.toHaveBeenCalled()
  await wait(9)
  expect(fetch).toHaveBeenCalledTimes(1)
  await wait(1)
  expect(fetch).toHaveBeenCalledTimes(2)
  expect(FakeEventSource.instances).toHaveLength(1)
})

it.each([
  { header: '2', delay: 2000 },
  { header: new Date(3000).toUTCString(), delay: 3000 },
  { header: '30', delay: 5000 },
  { header: 'invalid', delay: 10 },
  { header: '1.5', delay: 10 },
  { header: new Date(0).toUTCString(), delay: 10 },
])('uses Retry-After $header to set a retry floor of $delay ms', async ({ header, delay }) => {
  vi.setSystemTime(0)
  const fetch = vi.fn(authFetch({ token: 'allowed', expires_at: 900 }))
    .mockResolvedValueOnce(new Response(null, { status: 429, headers: { 'Retry-After': header } }))
  const m = client({ fetch, retryMs: 10, maxRetryMs: 5000 })
  m.private('orders')
  await flush()

  await wait(delay - 1)
  expect(fetch).toHaveBeenCalledTimes(1)
  await wait(1)
  expect(fetch).toHaveBeenCalledTimes(2)
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('backs off consecutive authorization failures with jitter and caps the base delay', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
  let attempts = 0
  const m = client({
    fetch: async () => {
      attempts++
      return new Response(null, { status: 500 })
    },
  })
  m.private('orders')
  await flush()
  expect(attempts).toBe(1)

  const delays = [3000, 6750, 13500, 27000, 33750, 33750]
  for (const [index, delay] of delays.entries()) {
    await wait(delay - 1)
    expect(attempts).toBe(index + 1)
    await wait(1)
    expect(attempts).toBe(index + 2)
  }
})

it('resets channel retry backoff when the connection attaches', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
  client({ retryMs: 100 }).channel('news')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.fail()

  await wait(100)
  const second = FakeEventSource.instances[1]
  assert.isDefined(second)
  second.fail()

  await wait(224)
  expect(FakeEventSource.instances).toHaveLength(2)
  await wait(1)
  const third = FakeEventSource.instances[2]
  assert.isDefined(third)
  third.dispatchEvent(new Event('open'))
  third.emit('murmur.attached', {}, '')
  third.fail()

  await wait(99)
  expect(FakeEventSource.instances).toHaveLength(3)
  await wait(1)
  expect(FakeEventSource.instances).toHaveLength(4)
})

it.each([400, 401, 403, 404, 419, 422, 499])('isolates a %s denial to the affected channel until reauthorized', async status => {
  const fetch = vi.fn(authFetch({ token: 'allowed', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(new Response(null, { status }))
  const m = client({ fetch, retryMs: 10 })
  const unauthorized = vi.fn()
  const denied = vi.fn()
  const allowed = vi.fn()
  m.on('unauthorized', unauthorized)
  m.private('orders').error(denied)
  m.private('alerts').error(allowed)
  m.channel('news')
  await wait(100)

  expect(unauthorized).toHaveBeenCalledExactlyOnceWith(status, 'orders')
  expect(denied).toHaveBeenCalledExactlyOnceWith(status)
  expect(allowed).not.toHaveBeenCalled()
  expect(fetch).toHaveBeenCalledTimes(2)
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(topicsOf(first)).toEqual(['private-alerts', 'news'])

  m.reauthorize('missing')
  await flush()
  expect(FakeEventSource.instances).toHaveLength(1)
  m.reauthorize('orders')
  await flush()
  const next = FakeEventSource.instances[1]
  assert.isDefined(next)
  expect(topicsOf(next)).toEqual(['private-orders', 'private-alerts', 'news'])
  expect(fetch).toHaveBeenCalledTimes(3)
})

it('reauthorizes a denied channel when it is left and rejoined', async () => {
  const fetch = vi.fn(authFetch({ token: 'allowed', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(new Response(null, { status: 403 }))
  const m = client({ fetch })
  m.private('orders')
  await flush()
  expect(FakeEventSource.instances).toHaveLength(0)

  m.leave('orders')
  m.private('orders')
  await flush()

  expect(fetch).toHaveBeenCalledTimes(2)
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('reauthorizes all denied channels when no name is supplied', async () => {
  const fetch = vi.fn(authFetch({ token: 'allowed', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(new Response(null, { status: 403 }))
    .mockResolvedValueOnce(new Response(null, { status: 403 }))
  const m = client({ fetch })
  m.private('orders')
  m.private('alerts')
  await flush()
  expect(FakeEventSource.instances).toHaveLength(0)

  m.reauthorize()
  await flush()

  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  expect(topicsOf(source)).toEqual(['private-orders', 'private-alerts'])
  expect(fetch).toHaveBeenCalledTimes(4)
})

it('reports an oversized private subscription and waits for the channel set to change', async () => {
  const fetch = vi.fn(authFetch({ token: 'small', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(Response.json({ token: 'x'.repeat(7000), expires_at: nowSeconds() + 900 }))
  const m = client({ fetch, retryMs: 10 })
  const error = vi.fn()
  m.on('error', error)
  m.private('big')
  m.private('small')
  await wait(100)

  expect(error).toHaveBeenCalledExactlyOnceWith(expect.any(Error))
  expect(error.mock.calls[0]?.[0]?.message).toMatch(/2 private channels.*multi-topic token/i)
  expect(FakeEventSource.instances).toHaveLength(0)
  expect(vi.getTimerCount()).toBe(0)

  m.leave('big')
  await flush()
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('emits gap', async () => {
  const m = client()
  let gaps = 0
  m.on('gap', () => gaps++)
  m.channel('news')
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  FakeEventSource.instances[0].emit('murmur.gap', {}, '')
  expect(gaps).toBe(1)
})

it('emits a gap before the attach marker on a reconnect with an ahead cursor', async () => {
  const m = client({ retryMs: 10 })
  const received: string[] = []
  m.on('gap', () => received.push('gap'))
  m.on('attached', () => received.push('attached'))
  m.channel('news')
  await flush()
  const [first] = FakeEventSource.instances
  assert.isDefined(first)
  first.emit('murmur.attached', {}, maxCursor)
  first.fail()
  await wait(10)

  const reconnect = FakeEventSource.instances[1]
  assert.isDefined(reconnect)
  expect(new URL(reconnect.url).searchParams.get('lastEventId')).toBe(maxCursor)
  reconnect.dispatchEvent(new Event('open'))
  reconnect.emit('murmur.gap', {}, '')
  expect(received).toEqual(['attached', 'gap'])
  expect(m.stats()).toMatchObject({ gaps: 1, lastEventId: maxCursor })

  reconnect.emit('murmur.attached', {}, '3-0')
  expect(received).toEqual(['attached', 'gap', 'attached'])
  expect(m.stats().lastEventId).toBe('3-0')
})

it('stops everything on close', async () => {
  const m = client({ retryMs: 10 })
  m.channel('news')
  await flush()
  m.close()
  await wait(20)
  assert.isDefined(FakeEventSource.instances[0])
  expect(FakeEventSource.instances[0].closed).toBe(true)
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('delivers to the remaining channel listeners when one throws', async () => {
  const error = new Error('Channel listener failed')
  const listener = vi.fn()
  client().channel('news').listen('.x', () => { throw error }).listen('.x', listener)
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  const enqueue = vi.spyOn(globalThis, 'queueMicrotask').mockImplementation(() => {})

  source.emit('x', { topic: 'news', data: 1 }, '1-0')

  expect(listener).toHaveBeenCalledExactlyOnceWith(1)
  expect(enqueue).toHaveBeenCalledTimes(1)
  const rethrow = enqueue.mock.calls[0]?.[0]
  assert.isDefined(rethrow)
  expect(rethrow).toThrow(error)
})

it('delivers to the remaining gap listeners when one throws', async () => {
  const error = new Error('Gap listener failed')
  const listener = vi.fn()
  const m = client()
  m.on('gap', () => { throw error })
  m.on('gap', listener)
  m.channel('news')
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  const enqueue = vi.spyOn(globalThis, 'queueMicrotask').mockImplementation(() => {})

  source.emit('murmur.gap', {}, '')

  expect(listener).toHaveBeenCalledExactlyOnceWith()
  expect(enqueue).toHaveBeenCalledTimes(1)
  const rethrow = enqueue.mock.calls[0]?.[0]
  assert.isDefined(rethrow)
  expect(rethrow).toThrow(error)
})

it('does not dispatch messages or errors from a closed fake connection', () => {
  const source = new FakeEventSource('https://app.test/sse')
  const message = vi.fn()
  const error = vi.fn()
  source.addEventListener('x', message)
  source.addEventListener('error', error)
  source.close()

  source.emit('x', { topic: 'news', data: 1 }, '1-0')
  source.fail()

  expect(message).not.toHaveBeenCalled()
  expect(error).not.toHaveBeenCalled()
})

it('emits disconnected once per failed authorization attempt', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>().mockRejectedValue(new Error('Authorization failed'))
  const disconnected = vi.fn()
  const m = client({ fetch, retryMs: 10 })
  m.on('disconnected', disconnected)
  m.private('orders')

  await flush()
  expect(disconnected).toHaveBeenCalledTimes(1)

  await wait(10)
  expect(fetch).toHaveBeenCalledTimes(2)
  expect(disconnected).toHaveBeenCalledTimes(2)
  expect(FakeEventSource.instances).toHaveLength(0)
})

it('receives polyfill messages and distinguishes data events from native open and error events', async () => {
  const connected = vi.fn()
  const disconnected = vi.fn()
  const listener = vi.fn()
  const m = client({ retryMs: 10 })
  m.on('connected', connected)
  m.on('disconnected', disconnected)
  m.channel('news').listen('.open', listener).listen('.error', listener)
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)

  source.dispatchEvent(new Event('open'))
  source.dispatchEvent(Object.assign(new Event('open'), {
    data: '{"topic":"news","data":1}',
    lastEventId: '7-1',
  }))
  const message = Object.assign(new Event('error'), {
    data: '{"topic":"news","data":2}',
    lastEventId: '7-2',
  })
  Object.defineProperty(message, 'currentTarget', { value: null })
  source.dispatchEvent(message)

  expect(connected).toHaveBeenCalledTimes(1)
  expect(disconnected).not.toHaveBeenCalled()
  expect(listener.mock.calls).toEqual([[1], [2]])
  expect(source.closed).toBe(false)

  source.fail()
  await wait(10)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('lastEventId')).toBe('7-2')
  expect(disconnected).toHaveBeenCalledTimes(1)
})

it('bounds concurrent requests and aborts every outstanding request on supersede', async () => {
  const { fetch, requests } = pendingAuthFetch()
  const m = client({ fetch })
  for (const name of ['a', 'b', 'c', 'd', 'e', 'f']) {
    m.private(name)
  }
  await flush()
  expect(requests).toHaveLength(4)

  requests[0]?.resolve(Response.json({ token: 'a', expires_at: nowSeconds() + 900 }))
  await flush()
  expect(requests).toHaveLength(5)

  m.leave('a')
  await flush()
  expect(requests).toHaveLength(9)
  expect(requests.slice(1, 5).every(request => request.signal?.aborted)).toBe(true)
  for (const request of requests.slice(1, 5)) {
    request.resolve(Response.json({ token: 'obsolete', expires_at: nowSeconds() + 900 }))
  }
  await flush()
  expect(requests).toHaveLength(9)
  expect(FakeEventSource.instances).toHaveLength(0)

  for (const [index, request] of requests.slice(5).entries()) {
    request.resolve(Response.json({ token: 'current-' + index, expires_at: nowSeconds() + 900 }))
  }
  await flush()
  requests[9]?.resolve(Response.json({ token: 'current-4', expires_at: nowSeconds() + 900 }))
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  expect(topicsOf(source)).toEqual(['private-b', 'private-c', 'private-d', 'private-e', 'private-f'])
  expect(new URL(source.url).searchParams.get('token')).toBe('current-0,current-1,current-2,current-3,current-4')
})

it('aborts sibling authorization requests when one channel returns a 5xx', async () => {
  const { fetch, requests } = pendingAuthFetch()
  const m = client({ fetch, retryMs: 10, retryJitter: 0 })
  for (const name of ['a', 'b', 'c', 'd', 'e']) {
    m.private(name)
  }
  await flush()
  expect(requests).toHaveLength(4)

  requests[1]?.resolve(new Response(null, { status: 503 }))
  await flush()
  expect(requests).toHaveLength(4)
  expect(requests.every(request => request.signal?.aborted)).toBe(true)
  expect(m.stats()).toMatchObject({ tokenRequests: 4, tokenFailures: 1, disconnections: 1 })

  requests[0]?.resolve(Response.json({ token: 'obsolete', expires_at: nowSeconds() + 900 }))
  await wait(10)
  expect(requests).toHaveLength(8)
  expect(FakeEventSource.instances).toHaveLength(0)
})

it('replaces the pending retry when a channel is joined', async () => {
  const m = client({ retryMs: 10 })
  m.channel('news')
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.fail()

  m.channel('alerts')
  await flush()
  const replacement = FakeEventSource.instances[1]
  assert.isDefined(replacement)
  expect(topicsOf(replacement)).toEqual(['news', 'alerts'])

  await wait(20)
  expect(FakeEventSource.instances).toHaveLength(2)
  expect(replacement.closed).toBe(false)
  expect(vi.getTimerCount()).toBe(0)
})

it('adds and removes a listener on an open connection without reconnecting', async () => {
  const listener = vi.fn()
  const channel = client().channel('news')
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.dispatchEvent(new Event('open'))

  channel.listen('.x', listener)
  source.emit('x', { topic: 'news', data: 1 }, '1-0')
  channel.stopListening('.x')
  source.emit('x', { topic: 'news', data: 2 }, '2-0')
  await flush()

  expect(listener).toHaveBeenCalledExactlyOnceWith(1)
  expect(FakeEventSource.instances).toHaveLength(1)
  expect(source.closed).toBe(false)
})

it('reconnects for the remaining topic and closes when leaving the last channel', async () => {
  const m = client({ retryMs: 10 })
  m.channel('news')
  m.channel('alerts')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)

  m.leave('news')
  await flush()
  const second = FakeEventSource.instances[1]
  assert.isDefined(second)
  expect(first.closed).toBe(true)
  expect(topicsOf(second)).toEqual(['alerts'])

  m.leave('alerts')
  await wait(20)
  expect(second.closed).toBe(true)
  expect(FakeEventSource.instances).toHaveLength(2)
})

it('reports connected and disconnected and supports unsubscribing', async () => {
  const connected = vi.fn()
  const disconnected = vi.fn()
  const m = client({ retryMs: 10 })
  const unsubscribeConnected = m.on('connected', connected)
  const unsubscribeDisconnected = m.on('disconnected', disconnected)
  m.channel('news')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)

  first.dispatchEvent(new Event('open'))
  first.fail()
  expect(connected).toHaveBeenCalledExactlyOnceWith()
  expect(disconnected).toHaveBeenCalledExactlyOnceWith()
  unsubscribeConnected()
  unsubscribeDisconnected()

  await wait(10)
  const second = FakeEventSource.instances[1]
  assert.isDefined(second)
  second.dispatchEvent(new Event('open'))
  second.fail()
  expect(connected).toHaveBeenCalledTimes(1)
  expect(disconnected).toHaveBeenCalledTimes(1)
})

it('fetches only the new channel and removes its cached token on leave', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>()
    .mockResolvedValueOnce(Response.json({ token: 'first', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(Response.json({ token: 'second', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(Response.json({ token: 'third', expires_at: nowSeconds() + 900 }))
  const m = client({ fetch })
  m.private('a')
  await flush()

  m.private('b')
  await flush()

  expect(fetch).toHaveBeenCalledTimes(2)
  expect(fetch.mock.calls[1]?.[1]?.body).toBe('{"channel_name":"private-b"}')
  const source = FakeEventSource.instances[1]
  assert.isDefined(source)
  expect(new URL(source.url).searchParams.get('token')).toBe('first,second')

  m.leave('b')
  await flush()
  const remaining = FakeEventSource.instances[2]
  assert.isDefined(remaining)
  expect(new URL(remaining.url).searchParams.get('token')).toBe('first')

  m.private('b')
  await flush()
  const rejoined = FakeEventSource.instances[3]
  assert.isDefined(rejoined)
  expect(new URL(rejoined.url).searchParams.get('token')).toBe('first,third')
  expect(fetch).toHaveBeenCalledTimes(3)
})

it('sends credentials, JSON acceptance, and the decoded XSRF token for authorization', async () => {
  vi.stubGlobal('document', { cookie: 'other=value; XSRF-TOKEN=a%2Bb%3D' })
  const fetch = vi.fn(authFetch({ token: 'jwt-1', expires_at: nowSeconds() + 900 }))
  client({ fetch }).private('a')
  await flush()

  expect(fetch).toHaveBeenCalledWith('/broadcasting/auth', expect.objectContaining({
    credentials: 'include',
    headers: expect.objectContaining({
      Accept: 'application/json',
      'X-XSRF-TOKEN': 'a+b=',
    }),
  }))
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('retains the replay cursor after a gap', async () => {
  const m = client({ retryMs: 10 })
  m.channel('news').listen('.x', () => {})
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.emit('x', { topic: 'news', data: 1 }, '7-3')

  source.emit('murmur.gap', {}, '')
  source.fail()
  await wait(10)

  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('lastEventId')).toBe('7-3')
})

it('ignores malformed JSON and envelopes without a topic and keeps delivering events', async () => {
  const listener = vi.fn()
  client().channel('news').listen('.x', listener)
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)

  source.dispatchEvent(new MessageEvent('x', { data: '{', lastEventId: '1-0' }))
  source.emit('x', { data: 2 }, '2-0')
  source.emit('x', { topic: 'news', data: 3 }, '3-0')

  expect(listener).toHaveBeenCalledExactlyOnceWith(3)
})

it('cancels a pending retry on close', async () => {
  const m = client({ retryMs: 10 })
  m.channel('news')
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.fail()

  m.close()
  await wait(20)

  expect(FakeEventSource.instances).toHaveLength(1)
  expect(source.closed).toBe(true)
  expect(vi.getTimerCount()).toBe(0)
})

it('aborts pending authorization on close and ignores its late response', async () => {
  const { fetch, requests } = pendingAuthFetch()
  const m = client({ fetch, retryMs: 10 })
  m.private('a')
  await flush()
  const request = requests[0]
  assert.isDefined(request)

  m.close()
  expect(request.signal?.aborted).toBe(true)
  request.resolve(Response.json({ token: 'late', expires_at: nowSeconds() + 900 }))
  await wait(20)

  expect(FakeEventSource.instances).toHaveLength(0)
  expect(vi.getTimerCount()).toBe(0)
})

/** Records pending authorization requests so tests can resolve them later. */
function pendingAuthFetch() {
  const requests: { resolve: (response: Response) => void; signal?: AbortSignal | null }[] = []
  const fetch: typeof globalThis.fetch = (_input, init) => new Promise(resolve => {
    requests.push({ resolve, signal: init?.signal })
  })
  return { fetch, requests }
}

it.each([
  ['retryMs', 0],
  ['retryMs', -1],
  ['retryMs', Infinity],
  ['retryMs', NaN],
  ['maxRetryMs', 0],
  ['maxRetryMs', -1],
  ['maxRetryMs', Infinity],
  ['maxRetryMs', NaN],
  ['maxRetryMs', 2999],
  ['retryJitter', -0.01],
  ['retryJitter', 1.01],
  ['retryJitter', Infinity],
  ['retryJitter', NaN],
  ['tokenRefreshMarginSeconds', -1],
  ['tokenRefreshMarginSeconds', Infinity],
  ['tokenRefreshMarginSeconds', NaN],
  ['streamMaxFailures', 0],
  ['streamMaxFailures', -1],
  ['streamMaxFailures', 1.5],
  ['streamMaxFailures', Infinity],
  ['streamMaxFailures', NaN],
])('rejects invalid %s (%s) on the client', (option, value) => {
  const create = () => client({ [option]: value })
  expect(create).toThrow(RangeError)
  expect(create).toThrow(option)
})

it.each([
  { retryJitter: 0.5, delays: [100, 200, 200] },
  { retryJitter: 0, delays: [100, 160, 160] },
])('caps the base delay with retryJitter $retryJitter', async ({ retryJitter, delays }) => {
  const random = vi.spyOn(Math, 'random').mockReturnValue(0.5)
  client({ retryMs: 100, maxRetryMs: 160, retryJitter }).channel('news')
  await flush()

  for (const [index, delay] of delays.entries()) {
    const source = FakeEventSource.instances[index]
    assert.isDefined(source)
    source.fail()
    await wait(delay - 1)
    expect(FakeEventSource.instances).toHaveLength(index + 1)
    await wait(1)
    expect(FakeEventSource.instances).toHaveLength(index + 2)
  }
  expect(random).toHaveBeenCalledTimes(retryJitter === 0 ? 0 : 2)
})

it.each([
  { margin: 5, remaining: 6, requests: 1 },
  { margin: 60, remaining: 60, requests: 2 },
  { margin: 60, remaining: 59, requests: 2 },
  { margin: 0, remaining: 1, requests: 1 },
])('uses a $margin-second margin with $remaining seconds remaining', async ({ margin, remaining, requests }) => {
  vi.setSystemTime(0)
  const fetch = authFetch({ token: 'jwt-1', expires_at: remaining + 1 })
  const m = client({ fetch, retryMs: 1000, tokenRefreshMarginSeconds: margin })
  m.private('orders')
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.dispatchEvent(new Event('open'))
  source.fail()

  await wait(1000)

  expect(FakeEventSource.instances).toHaveLength(2)
  expect(fetch.calls).toHaveLength(requests)
})

it('merges static auth headers case-insensitively and sends the configured credentials', async () => {
  vi.stubGlobal('document', { cookie: 'XSRF-TOKEN=from-cookie' })
  const fetch = vi.fn(authFetch({ token: 'jwt', expires_at: nowSeconds() + 900 }))
  client({
    fetch,
    authEndpoint: '/api/tokens',
    auth: {
      credentials: 'omit',
      headers: { Authorization: 'Bearer abc', 'x-xsrf-token': 'from-header', accept: 'application/vnd.api+json' },
    },
  }).private('orders')
  await flush()

  expect(fetch).toHaveBeenCalledExactlyOnceWith('/api/tokens', expect.objectContaining({
    credentials: 'omit',
    method: 'POST',
    body: '{"channel_name":"private-orders"}',
  }))
  const headers = new Headers(fetch.mock.calls[0]?.[1]?.headers)
  expect(headers.get('Authorization')).toBe('Bearer abc')
  expect(headers.get('X-XSRF-TOKEN')).toBe('from-header')
  expect(headers.get('Accept')).toBe('application/vnd.api+json')
  expect(headers.get('Content-Type')).toBe('application/json')
})

it('reads auth headers on each token request', async () => {
  let bearer = 'first'
  const fetch = vi.fn(authFetch({ token: 'jwt', expires_at: nowSeconds() + 900 }))
  const m = client({ fetch, auth: { headers: () => ({ Authorization: 'Bearer ' + bearer }) } })
  m.private('orders')
  await flush()
  bearer = 'second'
  m.private('alerts')
  await flush()

  expect(fetch.mock.calls.map(([, init]) => new Headers(init?.headers).get('Authorization')))
    .toEqual(['Bearer first', 'Bearer second'])
})

it('waits for async auth headers before making the token request', async () => {
  let resolveHeaders: ((headers: Record<string, string>) => void) | undefined
  const headers = new Promise<Record<string, string>>(resolve => { resolveHeaders = resolve })
  const fetch = vi.fn(authFetch({ token: 'jwt', expires_at: nowSeconds() + 900 }))
  client({ fetch, auth: { headers: () => headers } }).private('orders')
  await flush()
  expect(fetch).not.toHaveBeenCalled()

  resolveHeaders?.({ Authorization: 'Bearer async' })
  await flush()
  expect(fetch).toHaveBeenCalledTimes(1)
  expect(new Headers(fetch.mock.calls[0]?.[1]?.headers).get('Authorization')).toBe('Bearer async')
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('does not fetch an obsolete token when async auth headers finish after close', async () => {
  let resolveHeaders: ((headers: Record<string, string>) => void) | undefined
  const headers = new Promise<Record<string, string>>(resolve => { resolveHeaders = resolve })
  const fetch = vi.fn(authFetch({ token: 'jwt', expires_at: nowSeconds() + 900 }))
  const m = client({ fetch, auth: { headers: () => headers } })
  m.private('orders')
  await flush()

  m.close()
  resolveHeaders?.({ Authorization: 'Bearer late' })
  await flush()

  expect(fetch).not.toHaveBeenCalled()
  expect(FakeEventSource.instances).toHaveLength(0)
})

it('uses the custom authorizer in place of the built-in request and reuses its token', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>()
  const headers = vi.fn(() => ({ Authorization: 'Bearer unused' }))
  const authorizer = vi.fn(async (_channels: string[], _signal: AbortSignal) => ({
    token: 'custom', expires_at: nowSeconds() + 900,
  }))
  const m = client({ fetch, authEndpoint: '/unused', auth: { headers }, authorizer, retryMs: 10 })
  m.private('orders')
  await flush()

  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(authorizer).toHaveBeenCalledExactlyOnceWith(['private-orders'], expect.any(AbortSignal))
  expect(new URL(first.url).searchParams.get('token')).toBe('custom')
  first.dispatchEvent(new Event('open'))
  first.fail()
  await wait(10)

  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('token')).toBe('custom')
  expect(authorizer).toHaveBeenCalledTimes(1)
  expect(fetch).not.toHaveBeenCalled()
  expect(headers).not.toHaveBeenCalled()
})

it.each([null, { token: 123, expires_at: 1 }, { token: 'bad' }, { token: 'bad', expires_at: Infinity }])(
  'rejects an invalid custom authorization result (%j) and retries',
  async invalid => {
    const authorizer = vi.fn<Authorizer>()
      .mockResolvedValueOnce(invalid as Awaited<ReturnType<Authorizer>>)
      .mockResolvedValue({ token: 'valid', expires_at: nowSeconds() + 900 })
    const m = client({ authorizer, retryMs: 10 })
    const disconnected = vi.fn()
    m.on('disconnected', disconnected)
    m.private('orders')
    await flush()

    expect(disconnected).toHaveBeenCalledExactlyOnceWith()
    expect(FakeEventSource.instances).toHaveLength(0)
    await wait(10)
    expect(authorizer).toHaveBeenCalledTimes(2)
    const retry = FakeEventSource.instances[0]
    assert.isDefined(retry)
    expect(new URL(retry.url).searchParams.get('token')).toBe('valid')
  },
)

it.each([
  { error: { status: 400 }, statuses: [[400, 'orders'], [400, 'alerts']] },
  { error: { status: 499 }, statuses: [[499, 'orders'], [499, 'alerts']] },
  { error: { status: 408 }, statuses: [] },
  { error: { status: 425 }, statuses: [] },
  { error: { status: 429 }, statuses: [] },
  { error: { status: 500 }, statuses: [] },
  { error: { status: '403' }, statuses: [] },
  { error: new Error('Authorization unavailable'), statuses: [] },
  { error: null, statuses: [] },
])('pauses denied batch authorizations and retries other rejected authorizations ($error)', async ({
  error, statuses,
}) => {
  const authorizer = vi.fn()
    .mockRejectedValueOnce(error)
    .mockResolvedValue({ token: 'valid', expires_at: nowSeconds() + 900 })
  const unauthorized = vi.fn()
  const m = client({ authorizer, retryMs: 10 })
  m.on('unauthorized', unauthorized)
  m.private('orders')
  m.private('alerts')
  await flush()

  expect(unauthorized.mock.calls).toEqual(statuses)
  expect(FakeEventSource.instances).toHaveLength(0)
  await wait(9)
  expect(authorizer).toHaveBeenCalledTimes(1)
  await wait(1)
  if (statuses.length > 0) {
    expect(authorizer).toHaveBeenCalledTimes(1)
    m.reauthorize()
    await flush()
  }
  expect(authorizer).toHaveBeenCalledTimes(2)
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('aborts the custom authorizer on close and ignores a late unauthorized rejection', async () => {
  let rejectToken: ((error: unknown) => void) | undefined
  const authorizer = vi.fn((_channels: string[], _signal: AbortSignal) =>
    new Promise<{ token: string; expires_at: number }>((_resolve, reject) => { rejectToken = reject }),
  )
  const unauthorized = vi.fn()
  const m = client({ authorizer })
  m.on('unauthorized', unauthorized)
  m.private('orders')
  await flush()
  const signal = authorizer.mock.calls[0]?.[1]
  assert.isDefined(signal)

  m.close()
  expect(signal.aborted).toBe(true)
  rejectToken?.({ status: 401 })
  await flush()

  expect(unauthorized).not.toHaveBeenCalled()
  expect(FakeEventSource.instances).toHaveLength(0)
  expect(vi.getTimerCount()).toBe(0)
})

it.each([
  { withCredentials: true, expected: [{ withCredentials: true }] },
  { withCredentials: false, expected: [] },
  { withCredentials: undefined, expected: [] },
])('passes EventSource credentials for channels and streams when set to $withCredentials', async ({
  withCredentials, expected,
}) => {
  const constructorArgs = vi.fn()

  /** Records the exact arguments supplied to the transport's EventSource. */
  class ObservedEventSource extends FakeEventSource {
    /** Records optional arguments before initializing the source. */
    public constructor(url: string | URL, ...options: [EventSourceInit?]) {
      super(url, ...options)
      constructorArgs(String(url), ...options)
    }
  }

  const m = client({ EventSource: ObservedEventSource, withCredentials })
  m.channel('news')
  await flush()
  m.stream('https://app.test/sse?topics=alerts')
  await flush()

  expect(constructorArgs.mock.calls).toEqual([
    ['https://app.test/sse?topics=news', ...expected],
    ['https://app.test/sse?topics=alerts', ...expected],
  ])
})

it('returns a fresh plain statistics snapshot with empty initial values', () => {
  const m = client()
  const initial = {
    connected: false, connections: 0, disconnections: 0, events: 0, gaps: 0,
    tokenRequests: 0, tokenFailures: 0, channels: 0, streams: 0,
    lastEventId: undefined, lastEventAt: undefined, connectionId: undefined,
  }
  const snapshot = m.stats()

  expect(Object.getPrototypeOf(snapshot)).toBe(Object.prototype)
  expect(snapshot).toStrictEqual(initial)
  snapshot.events = 100
  expect(m.stats()).not.toBe(snapshot)
  expect(m.stats()).toStrictEqual(initial)
})

it('tracks opens and failures across retries and deliberate channel reconnects', async () => {
  const m = client({ retryMs: 10, retryJitter: 0 })
  m.channel('news')
  m.channel('news')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(m.stats()).toMatchObject({ connected: false, channels: 1, connections: 0, disconnections: 0 })

  first.fail()
  expect(m.stats()).toMatchObject({ connected: false, channels: 1, connections: 0, disconnections: 1 })
  await wait(10)
  const second = FakeEventSource.instances[1]
  assert.isDefined(second)
  second.dispatchEvent(new Event('open'))
  expect(m.stats()).toMatchObject({ connected: true, channels: 1, connections: 1, disconnections: 1 })

  m.channel('alerts')
  await flush()
  expect(m.stats()).toMatchObject({ connected: false, channels: 2, connections: 1, disconnections: 1 })
  const third = FakeEventSource.instances[2]
  assert.isDefined(third)
  third.dispatchEvent(new Event('open'))
  expect(m.stats()).toMatchObject({ connected: true, channels: 2, connections: 2, disconnections: 1 })

  m.leave('alerts')
  await flush()
  const fourth = FakeEventSource.instances[3]
  assert.isDefined(fourth)
  fourth.dispatchEvent(new Event('open'))
  expect(m.stats()).toMatchObject({ connected: true, channels: 1, connections: 3, disconnections: 1 })

  fourth.fail()
  expect(m.stats()).toMatchObject({ connected: false, connections: 3, disconnections: 2 })
  await wait(20)
  const last = FakeEventSource.instances[4]
  assert.isDefined(last)
  last.dispatchEvent(new Event('open'))
  expect(m.stats()).toMatchObject({ connected: true, connections: 4, disconnections: 2 })

  m.leave('news')
  expect(m.stats()).toMatchObject({ connected: false, channels: 0, connections: 4, disconnections: 2 })
})

it('counts shared data once per dispatch and records gaps, cursor and receive time separately', async () => {
  vi.setSystemTime(1_000)
  const m = client()
  const listener = vi.fn()
  const otherListener = vi.fn()
  m.channel('news').listen('.updated', listener).listen('.updated', otherListener)
  m.channel('alerts')
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.dispatchEvent(new Event('open'))
  source.emit('ignored', { topic: 'news', data: 0 }, '1-0')
  source.emit('updated', { data: 0 }, '2-0')
  expect(m.stats()).toMatchObject({ events: 0, gaps: 0, lastEventAt: undefined, lastEventId: '2-0' })

  source.emit('updated', { topic: 'news', data: 1 }, '3-0')
  expect(listener).toHaveBeenCalledExactlyOnceWith(1)
  expect(otherListener).toHaveBeenCalledExactlyOnceWith(1)
  expect(m.stats()).toMatchObject({ events: 1, gaps: 0, lastEventAt: 1_000, lastEventId: '3-0' })

  vi.setSystemTime(2_000)
  source.emit('murmur.gap', {}, '4-0')
  expect(m.stats()).toMatchObject({ events: 1, gaps: 1, lastEventAt: 1_000, lastEventId: '3-0' })

  source.emit('updated', { topic: 'alerts', data: 2 }, '')
  expect(listener).toHaveBeenCalledTimes(1)
  expect(otherListener).toHaveBeenCalledTimes(1)
  expect(m.stats()).toMatchObject({ events: 2, gaps: 1, lastEventAt: 2_000, lastEventId: '3-0' })
})

it('counts active streams without including their traffic or failures in shared statistics', async () => {
  const m = client({ streamMaxFailures: 1 })
  const firstStream = m.stream('https://app.test/sse?topics=news').on('updated', () => {})
  m.stream('https://app.test/sse?topics=alerts')
  const explicitlyClosed = m.stream('https://app.test/sse?topics=other')
  expect(m.stats().streams).toBe(3)
  await flush()
  const first = FakeEventSource.instances[0]
  const second = FakeEventSource.instances[1]
  assert.isDefined(first)
  assert.isDefined(second)

  first.dispatchEvent(new Event('open'))
  first.emit('updated', { topic: 'news', data: 1 }, '1-0')
  first.emit('murmur.gap', {}, '2-0')
  first.emit('murmur.end', {}, '3-0')
  expect(m.stats().streams).toBe(2)
  firstStream.close()
  expect(m.stats().streams).toBe(2)

  second.fail()
  expect(m.stats().streams).toBe(1)
  explicitlyClosed.close()
  expect(m.stats()).toStrictEqual({
    connected: false, connections: 0, disconnections: 0, events: 0, gaps: 0,
    tokenRequests: 0, tokenFailures: 0, channels: 0, streams: 0,
    lastEventId: undefined, lastEventAt: undefined, connectionId: undefined,
  })
})

it('counts built-in token failures and successful attempts while skipping cache hits', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>()
    .mockResolvedValueOnce(new Response(null, { status: 403 }))
    .mockResolvedValueOnce(Response.json({ token: 'jwt', expires_at: nowSeconds() + 900 }))
  const m = client({ fetch, retryMs: 10 })
  const unauthorized = vi.fn(() => m.stats())
  m.on('unauthorized', unauthorized)
  m.private('orders')
  await flush()

  expect(unauthorized.mock.results[0]?.value).toMatchObject({ tokenRequests: 1, tokenFailures: 1 })
  expect(m.stats()).toMatchObject({ tokenRequests: 1, tokenFailures: 1, disconnections: 0 })
  m.reauthorize()
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.dispatchEvent(new Event('open'))
  expect(m.stats()).toMatchObject({ tokenRequests: 2, tokenFailures: 1, disconnections: 0 })

  m.channel('news')
  await flush()
  expect(FakeEventSource.instances).toHaveLength(2)
  expect(m.stats()).toMatchObject({ tokenRequests: 2, tokenFailures: 1, disconnections: 0 })
})

it('counts rejected and invalid authorizer results before a successful retry', async () => {
  const authorizer = vi.fn<Authorizer>()
    .mockRejectedValueOnce(new Error('Unavailable'))
    .mockResolvedValueOnce({ token: 'bad', expires_at: NaN })
    .mockResolvedValueOnce({ token: 'jwt', expires_at: nowSeconds() + 900 })
  const m = client({ authorizer, retryMs: 10, retryJitter: 0 })
  m.private('orders')
  await flush()
  expect(m.stats()).toMatchObject({ tokenRequests: 1, tokenFailures: 1, disconnections: 1 })

  await wait(10)
  expect(m.stats()).toMatchObject({ tokenRequests: 2, tokenFailures: 2, disconnections: 2 })
  await wait(20)
  expect(m.stats()).toMatchObject({ tokenRequests: 3, tokenFailures: 2, disconnections: 2 })
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('keeps the final statistics after close while clearing live state', async () => {
  let rejectToken: ((error: unknown) => void) | undefined
  const authorizer: Authorizer = () => new Promise((_resolve, reject) => { rejectToken = reject })
  const m = client({ authorizer })
  m.channel('news').listen('.updated', () => {})
  m.stream('https://app.test/sse?topics=alerts')
  await flush()
  const shared = FakeEventSource.instances[1]
  assert.isDefined(shared)
  shared.dispatchEvent(new Event('open'))
  shared.emit('updated', { topic: 'news', data: 1 }, '1-0')
  const lastEventAt = m.stats().lastEventAt

  m.private('orders')
  await flush()
  expect(m.stats()).toMatchObject({ channels: 2, streams: 1, tokenRequests: 1, tokenFailures: 0 })
  m.close()
  const final = m.stats()
  expect(final).toStrictEqual({
    connected: false, connections: 1, disconnections: 0, events: 1, gaps: 0,
    tokenRequests: 1, tokenFailures: 0, channels: 0, streams: 0,
    lastEventId: '1-0', lastEventAt, connectionId: undefined,
  })

  rejectToken?.({ status: 403 })
  shared.dispatchEvent(new Event('open'))
  shared.dispatchEvent(new Event('error'))
  shared.dispatchEvent(new MessageEvent('updated', { data: '{"topic":"news","data":2}', lastEventId: '2-0' }))
  m.channel('late')
  m.stream('https://app.test/sse?topics=late')
  m.close()
  await wait(3000)
  expect(m.stats()).toStrictEqual(final)
})

it('raises the default retry cap to a larger retryMs', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0)
  client({ retryMs: 60_000 }).channel('news')
  await flush()

  for (const index of [0, 1]) {
    const source = FakeEventSource.instances[index]
    assert.isDefined(source)
    source.fail()
    await wait(59_999)
    expect(FakeEventSource.instances).toHaveLength(index + 1)
    await wait(1)
    expect(FakeEventSource.instances).toHaveLength(index + 2)
  }
})

it('accepts a stream retryMs above the client retry cap', () => {
  const m = client({ retryMs: 100, maxRetryMs: 200 })

  expect(() => m.stream('http://murmur.test/sse?topics=news', { retryMs: 500 })).not.toThrow()
  expect(() => m.stream('http://murmur.test/sse?topics=news', { retryMs: 500, maxRetryMs: 499 })).toThrow(RangeError)
})

it('does not read the XSRF cookie when credentials are omitted', async () => {
  vi.stubGlobal('document', { cookie: 'XSRF-TOKEN=from-cookie' })
  const fetch = vi.fn(authFetch({ token: 'jwt', expires_at: nowSeconds() + 900 }))
  client({ fetch, auth: { credentials: 'omit', headers: { Authorization: 'Bearer abc' } } }).private('orders')
  await flush()

  const headers = new Headers(fetch.mock.calls[0]?.[1]?.headers)
  expect(headers.get('Authorization')).toBe('Bearer abc')
  expect(headers.has('X-XSRF-TOKEN')).toBe(false)
})

it('uses a batch token for every private channel and consolidates later joins', async () => {
  const authorizer = vi.fn<Authorizer>()
    .mockResolvedValueOnce({ token: 'pair', expires_at: nowSeconds() + 900 })
    .mockResolvedValueOnce({ token: 'trio', expires_at: nowSeconds() + 900 })
  const m = client({ authorizer })
  m.private('a')
  m.private('b')
  await flush()
  expect(authorizer).toHaveBeenCalledExactlyOnceWith(['private-a', 'private-b'], expect.any(AbortSignal))

  m.private('c')
  await flush()
  expect(authorizer).toHaveBeenLastCalledWith(['private-a', 'private-b', 'private-c'], expect.any(AbortSignal))
  m.leave('a')
  await flush()

  const source = FakeEventSource.instances[2]
  assert.isDefined(source)
  expect(topicsOf(source)).toEqual(['private-b', 'private-c'])
  expect(new URL(source.url).searchParams.get('token')).toBe('trio')
  expect(authorizer).toHaveBeenCalledTimes(2)
})

it('keeps an attached private channel authorized when a new join makes the batch fail', async () => {
  const authorizer = vi.fn<Authorizer>()
    .mockResolvedValueOnce({ token: 'orders', expires_at: nowSeconds() + 900 })
    .mockRejectedValueOnce({ status: 403 })
  const m = client({ authorizer, retryMs: 10 })
  const orders = vi.fn()
  const ordersDenied = vi.fn()
  const forbiddenDenied = vi.fn()
  const unauthorized = vi.fn()
  m.on('unauthorized', unauthorized)
  m.private('orders').listen('.shipped', orders).error(ordersDenied)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.dispatchEvent(new Event('open'))
  first.emit('murmur.attached', {}, '4-0')

  m.private('forbidden').error(forbiddenDenied)
  await wait(100)
  expect(authorizer).toHaveBeenCalledTimes(2)
  expect(authorizer).toHaveBeenLastCalledWith(['private-orders', 'private-forbidden'], expect.any(AbortSignal))
  expect(unauthorized).toHaveBeenCalledExactlyOnceWith(403, 'forbidden')
  expect(forbiddenDenied).toHaveBeenCalledExactlyOnceWith(403)
  expect(ordersDenied).not.toHaveBeenCalled()
  const next = FakeEventSource.instances[1]
  assert.isDefined(next)
  expect(topicsOf(next)).toEqual(['private-orders'])
  expect(new URL(next.url).searchParams.get('token')).toBe('orders')
  next.emit('shipped', { topic: 'private-orders', data: 42 }, '5-0')
  expect(orders).toHaveBeenCalledExactlyOnceWith(42)
})

it('uses only the reported token coverage and denies uncovered private topics', async () => {
  const expires_at = nowSeconds() + 900
  const authorizer = vi.fn<Authorizer>().mockResolvedValue([
    { token: 'pair', expires_at, channels: ['private-a', 'private-b'] },
    { token: 'individual', expires_at, channels: ['private-c'] },
    { token: 'unused', expires_at, channels: ['private-unrequested'] },
  ])
  const m = client({ authorizer, retryMs: 10 })
  const unauthorized = vi.fn()
  m.on('unauthorized', unauthorized)
  for (const name of ['a', 'b', 'c', 'denied']) {
    m.private(name)
  }
  await wait(100)

  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(topicsOf(first)).toEqual(['private-a', 'private-b', 'private-c'])
  expect(new URL(first.url).searchParams.get('token')).toBe('pair,individual')
  expect(unauthorized).toHaveBeenCalledExactlyOnceWith(403, 'denied')

  m.leave('c')
  await flush()
  const remaining = FakeEventSource.instances[1]
  assert.isDefined(remaining)
  expect(new URL(remaining.url).searchParams.get('token')).toBe('pair')
  expect(authorizer).toHaveBeenCalledTimes(1)
})

it('preserves a batch credential inside the refresh margin until it expires', async () => {
  vi.setSystemTime(0)
  const authorizer = vi.fn<Authorizer>()
    .mockResolvedValueOnce({ token: 'orders', expires_at: 10 })
    .mockRejectedValue({ status: 403 })
  const m = client({ authorizer, retryMs: 10 })
  const unauthorized = vi.fn()
  m.on('unauthorized', unauthorized)
  m.private('orders')
  await flush()

  m.private('forbidden')
  await flush()
  const next = FakeEventSource.instances[1]
  assert.isDefined(next)
  expect(topicsOf(next)).toEqual(['private-orders'])
  expect(unauthorized).toHaveBeenCalledExactlyOnceWith(403, 'forbidden')

  vi.setSystemTime(10_000)
  next.dispatchEvent(new Event('open'))
  next.fail()
  await wait(10)
  expect(unauthorized.mock.calls).toEqual([[403, 'forbidden'], [403, 'orders']])
  expect(FakeEventSource.instances).toHaveLength(2)
})

it('allows an empty batch result to deny every requested channel', async () => {
  const authorizer = vi.fn<Authorizer>().mockResolvedValue([])
  const m = client({ authorizer, retryMs: 10 })
  const unauthorized = vi.fn()
  m.on('unauthorized', unauthorized)
  m.private('orders')
  await wait(100)

  expect(unauthorized).toHaveBeenCalledExactlyOnceWith(403, 'orders')
  expect(authorizer).toHaveBeenCalledTimes(1)
  expect(FakeEventSource.instances).toHaveLength(0)
})

it('sends grouped token results together and replaces the group after a channel leaves', async () => {
  const authorizer = vi.fn<Authorizer>()
    .mockResolvedValueOnce([
      { token: 'group', expires_at: nowSeconds() + 900 },
      { token: 'individual', expires_at: nowSeconds() + 900 },
    ])
    .mockResolvedValueOnce({ token: 'remaining', expires_at: nowSeconds() + 900 })
  const m = client({ authorizer })
  m.private('a')
  m.private('b')
  m.private('c')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(new URL(first.url).searchParams.get('token')).toBe('group,individual')

  m.leave('c')
  await flush()
  const next = FakeEventSource.instances[1]
  assert.isDefined(next)
  expect(authorizer).toHaveBeenLastCalledWith(['private-a', 'private-b'], expect.any(AbortSignal))
  expect(new URL(next.url).searchParams.get('token')).toBe('remaining')
})

it.each([
  { kind: 'opaque pre-open', open: false, status: undefined, refreshAfter: [2, 4, 8, 16] },
  { kind: 'opaque early end', open: true, status: undefined, refreshAfter: [2, 4, 8, 16] },
  { kind: 'known rejection', open: false, status: 401, refreshAfter: [1, 4, 8, 16] },
])('spaces credential refreshes over ten minutes of $kind failures', async ({ open, status, refreshAfter }) => {
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
  vi.setSystemTime(0)
  let request = 0
  const fetch: typeof globalThis.fetch = async () =>
    Response.json({ token: String(++request), expires_at: nowSeconds() + 900 })
  const m = client({ fetch })
  for (const topic of ['a', 'b', 'c']) {
    m.private(topic)
  }
  await flush()

  const delays = [3000, 6750, 13500, 27000, ...Array<number>(16).fill(33750)]
  let requestsForAttempt = 3
  for (const [index, delay] of [...delays, 9750].entries()) {
    const source = FakeEventSource.instances[index]
    assert.isDefined(source)
    expect(request).toBe(requestsForAttempt)
    expect(new URL(source.url).searchParams.get('token')).toBe([
      request - 2, request - 1, request,
    ].join(','))
    if (open) {
      source.dispatchEvent(new Event('open'))
      source.emit('murmur.connected', { connection_id: connectionId, protocol: 1 }, '')
    }
    source.dispatchEvent(Object.assign(new Event('error'), { status }))
    await wait(delay)
    if (refreshAfter.includes(index + 1)) {
      requestsForAttempt += 3
    }
  }
  expect(Date.now()).toBe(600_000)
  expect(FakeEventSource.instances).toHaveLength(21)
  expect(m.stats().tokenRequests).toBe(15)
})

it('starts the credential refresh schedule over after attachment', async () => {
  let request = 0
  const fetch: typeof globalThis.fetch = async () =>
    Response.json({ token: String(++request), expires_at: nowSeconds() + 900 })
  const m = client({ fetch, retryMs: 10, retryJitter: 0 })
  m.private('a')
  await flush()
  for (const [index, delay] of [10, 20, 40].entries()) {
    const source = FakeEventSource.instances[index]
    assert.isDefined(source)
    source.fail()
    await wait(delay)
  }
  expect(request).toBe(2)

  const recovered = FakeEventSource.instances[3]
  assert.isDefined(recovered)
  recovered.dispatchEvent(new Event('open'))
  recovered.emit('murmur.attached', {}, '')
  recovered.fail()
  await wait(10)
  const rejected = FakeEventSource.instances[4]
  assert.isDefined(rejected)
  rejected.dispatchEvent(Object.assign(new Event('error'), { status: 401 }))
  await wait(20)
  expect(request).toBe(3)
})

it('retains cached tokens across known non-credential SSE failures', async () => {
  const fetch = vi.fn(async () => Response.json({ token: 'valid', expires_at: nowSeconds() + 900 }))
  const m = client({ fetch, retryMs: 10, retryJitter: 0 })
  m.private('a')
  await flush()

  for (const [index, status] of [503, 500, 431].entries()) {
    const source = FakeEventSource.instances[index]
    assert.isDefined(source)
    source.dispatchEvent(Object.assign(new Event('error'), { code: status }))
    await wait(10 * 2 ** index)
  }
  expect(FakeEventSource.instances).toHaveLength(4)
  expect(fetch).toHaveBeenCalledTimes(1)
})

it('stores the announced connection ID and reuses it for SSE and Laravel authorization', async () => {
  const fetch = vi.fn(authFetch({ token: 'jwt', expires_at: nowSeconds() + 10 }))
  const m = client({ fetch, url: 'https://app.test/sse?connection=unissued', retryMs: 10 })
  const control = vi.fn()
  const id = '0123456789abcdef0123456789abcdef'
  expect(m.socketId()).toBeUndefined()
  expect(m.stats().connectionId).toBeUndefined()

  m.channel('news').listen('.murmur.connected', control).listen('.updated', () => {})
  m.private('orders')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(new URL(first.url).searchParams.has('connection')).toBe(false)
  expect(authFetch.calls).toEqual([{ body: { channel_name: 'private-orders' } }])

  first.dispatchEvent(new Event('open'))
  first.emit('updated', { topic: 'news', data: 1 }, '10-0')
  first.emit('murmur.connected', { connection_id: id, protocol: 1, topic: 'news', data: 2 }, '20-0')
  expect(m.socketId()).toBe(id)
  expect(m.stats()).toMatchObject({ connectionId: id, lastEventId: '10-0', events: 1 })
  expect(control).not.toHaveBeenCalled()

  first.fail()
  await wait(10)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('connection')).toBe(id)
  expect(authFetch.calls[1]).toEqual({ body: { channel_name: 'private-orders', socket_id: id } })

  m.stream('https://app.test/sse?topics=stream')
  await flush()
  const independent = FakeEventSource.instances[2]
  assert.isDefined(independent)
  expect(new URL(independent.url).searchParams.has('connection')).toBe(false)
})

it.each([
  '{',
  'null',
  '[]',
  '42',
  '{"connection_id":42,"protocol":2}',
  '{"connection_id":"0123456789abcdef0123456789abcdef","protocol":true}',
  '{"connection_id":"0123456789abcdef0123456789abcdef","protocol":null}',
  '{"connection_id":"0123456789abcdef0123456789abcdef"}',
  '{"connection_id":"ABCDEF0123456789ABCDEF0123456789","protocol":2}',
  '{"connection_id":"0123456789abcdef0123456789abcdef0","protocol":2}',
  '{"connection_id":"0123456789abcdef/../../aaaaaaaa","protocol":2}',
])('ignores an invalid connection announcement (%s)', async data => {
  const m = client()
  const error = vi.fn()
  m.on('error', error)
  m.channel('news')
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.dispatchEvent(new Event('open'))
  const before = m.stats()

  source.dispatchEvent(new MessageEvent('murmur.connected', { data, lastEventId: '9-0' }))
  expect(m.stats()).toEqual(before)
  expect(m.socketId()).toBeUndefined()
  expect(error).not.toHaveBeenCalled()

  const issued = 'fedcba9876543210fedcba9876543210'
  source.emit('murmur.connected', { connection_id: issued, protocol: '1' }, '')
  expect(m.socketId()).toBe(issued)
  expect(error.mock.calls[0]?.[0]?.message).toBe('Murmur server announced protocol "1"; expected 1.')
})

it('reports a protocol mismatch once and continues delivering on repeated connections', async () => {
  const m = client({ retryMs: 10 })
  const error = vi.fn()
  const updated = vi.fn()
  const id = 'abcdef0123456789abcdef0123456789'
  m.on('error', error)
  m.channel('news').listen('.updated', updated)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)

  first.emit('murmur.connected', { connection_id: id, protocol: 2 }, '')
  first.emit('updated', { topic: 'news', data: 1 }, '1-0')
  expect(first.closed).toBe(false)
  first.fail()
  await wait(10)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  retry.emit('murmur.connected', { connection_id: id, protocol: 2 }, '')
  retry.emit('updated', { topic: 'news', data: 2 }, '2-0')

  expect(error).toHaveBeenCalledExactlyOnceWith(expect.any(Error))
  expect(error.mock.calls[0]?.[0]?.message).toMatch(/protocol 2.*expected 1/i)
  expect(updated.mock.calls).toEqual([[1], [2]])
  expect(retry.closed).toBe(false)
  expect(m.socketId()).toBe(id)
})

it('omits fresh without a cursor but keeps a first join pending until attachment', async () => {
  const m = client({ retryMs: 10, retryJitter: 0, url: 'https://app.test/sse?fresh=obsolete' })
  m.channel('news').listen('.updated', () => {})
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(new URL(first.url).searchParams.has('fresh')).toBe(false)
  first.fail()

  await wait(10)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.has('fresh')).toBe(false)
  expect(m.stats().lastEventId).toBeUndefined()
  retry.dispatchEvent(new Event('open'))
  retry.emit('updated', { topic: 'news', data: 1 }, '3-0')
  retry.fail()

  await wait(20)
  const pending = FakeEventSource.instances[2]
  assert.isDefined(pending)
  expect(new URL(pending.url).searchParams.get('fresh')).toBe('news')
  pending.emit('murmur.attached', {}, '')
  expect(m.stats().lastEventId).toBe('3-0')
  m.channel('alerts')
  await flush()
  const next = FakeEventSource.instances[3]
  assert.isDefined(next)
  expect(new URL(next.url).searchParams.get('fresh')).toBe('alerts')
})

it('closes a failed source and rebuilds fresh, token and cursor after replay or attachment', async () => {
  let authorization = 0
  const fetch: typeof globalThis.fetch = async () =>
    Response.json({ token: 'token-' + ++authorization, expires_at: nowSeconds() + 900 })
  const m = client({ fetch, retryMs: 10 })
  m.channel('news').listen('.updated', () => {})
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.emit('murmur.attached', {}, '10-0')

  m.private('orders')
  await flush()
  const joining = FakeEventSource.instances[1]
  assert.isDefined(joining)
  expect(new URL(joining.url).searchParams.get('fresh')).toBe('private-orders')
  expect(new URL(joining.url).searchParams.get('token')).toBe('token-1')
  joining.dispatchEvent(new Event('open'))
  joining.emit('updated', { topic: 'news', data: 1 }, '15-0')
  joining.fail()
  expect(joining.closed).toBe(true)
  expect(FakeEventSource.instances).toHaveLength(2)

  await wait(10)
  const retry = FakeEventSource.instances[2]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('fresh')).toBe('private-orders')
  expect(new URL(retry.url).searchParams.get('lastEventId')).toBe('15-0')
  expect(new URL(retry.url).searchParams.get('token')).toBe('token-1')
  retry.dispatchEvent(new Event('open'))
  retry.emit('murmur.attached', {}, '15-0')
  retry.emit('updated', { topic: 'news', data: 2 }, '16-0')
  retry.fail()

  expect(retry.closed).toBe(true)
  await wait(10)
  const attached = FakeEventSource.instances[3]
  assert.isDefined(attached)
  expect(new URL(attached.url).searchParams.has('fresh')).toBe(false)
  expect(new URL(attached.url).searchParams.get('lastEventId')).toBe('16-0')
  expect(new URL(attached.url).searchParams.get('token')).toBe('token-1')
})

it('does not let an old attach marker acknowledge a replacement join', async () => {
  const m = client()
  m.channel('news')
  m.channel('alerts')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)

  m.leave('alerts')
  m.channel('alerts')
  first.emit('murmur.attached', {}, '10-0')
  await flush()

  const next = FakeEventSource.instances[1]
  assert.isDefined(next)
  expect(new URL(next.url).searchParams.get('fresh')).toBe('alerts')
  expect(new URL(next.url).searchParams.get('lastEventId')).toBe('10-0')
})

it('accepts a lower numbered attach cursor without dispatching the marker as data', async () => {
  const m = client({ retryMs: 10 })
  const controls = vi.fn()
  const updated = vi.fn()
  m.channel('news').listen('.murmur.attached', controls).listen('.updated', updated)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.emit('updated', { topic: 'news', data: 'before correction' }, '999999999999999999-8')
  first.fail()

  await wait(10)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  retry.emit('murmur.attached', { topic: 'news', data: 'control' }, '999999999999999998-9')
  expect(m.stats()).toMatchObject({ lastEventId: '999999999999999998-9', events: 1 })
  expect(controls).not.toHaveBeenCalled()

  retry.emit('updated', { topic: 'news', data: 'new' }, '999999999999999998-10')
  expect(updated.mock.calls).toEqual([['before correction'], ['new']])
  expect(m.stats()).toMatchObject({ lastEventId: '999999999999999998-10', events: 2 })
})

it('delivers lower IDs after reconnecting with a cursor ahead of an empty log', async () => {
  const m = client({ retryMs: 10 })
  const updated = vi.fn()
  m.channel('news').listen('.updated', updated)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.emit('murmur.attached', {}, '2000-5')
  first.fail()

  await wait(10)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('lastEventId')).toBe('2000-5')
  retry.emit('murmur.attached', {}, '')
  expect(m.stats().lastEventId).toBe('2000-5')
  retry.emit('updated', { topic: 'news', data: 1 }, '1999-0')
  retry.emit('updated', { topic: 'news', data: 2 }, '1999-1')
  expect(updated.mock.calls).toEqual([[1], [2]])
  expect(m.stats()).toMatchObject({ lastEventId: '1999-1', events: 2 })
})

it('keeps a wire topic attached while any alias remains joined', async () => {
  const m = client({ retryMs: 10, fetch: authFetch({ token: 'jwt', expires_at: nowSeconds() + 900 }) })
  m.private('x')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.emit('murmur.attached', {}, '10-0')

  m.channel('private-x')
  await flush()
  const alias = FakeEventSource.instances[1]
  assert.isDefined(alias)
  expect(new URL(alias.url).searchParams.has('fresh')).toBe(false)
  alias.fail()
  m.leave('x')
  await flush()

  const remaining = FakeEventSource.instances[2]
  assert.isDefined(remaining)
  expect(topicsOf(remaining)).toEqual(['private-x'])
  expect(new URL(remaining.url).searchParams.has('fresh')).toBe(false)
})

it('emits attached after opening and continues to report each attach marker', async () => {
  const m = client({ retryMs: 10 })
  const connected = vi.fn()
  const attached = vi.fn(() => m.stats().lastEventId)
  m.on('connected', connected)
  const unsubscribe = m.on('attached', attached)
  m.channel('news')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.dispatchEvent(new Event('open'))
  first.emit('murmur.connected', { connection_id: '0123456789abcdef0123456789abcdef', protocol: 1 }, '')
  expect(connected).toHaveBeenCalledExactlyOnceWith()
  expect(attached).not.toHaveBeenCalled()
  first.emit('murmur.attached', {}, '5-0')
  expect(attached).toHaveBeenCalledExactlyOnceWith()
  expect(attached).toHaveReturnedWith('5-0')

  m.channel('alerts')
  await flush()
  const replacement = FakeEventSource.instances[1]
  assert.isDefined(replacement)
  replacement.dispatchEvent(new Event('open'))
  replacement.emit('murmur.attached', {}, '6-0')
  expect(attached).toHaveBeenCalledTimes(2)
  unsubscribe()
  replacement.fail()
  await wait(10)
  FakeEventSource.instances[2]?.emit('murmur.attached', {}, '7-0')
  expect(attached).toHaveBeenCalledTimes(2)
})

it('replaces a stale shared cursor with a lower heartbeat after an unnumbered marker', async () => {
  const m = client({ retryMs: 10 })
  const heartbeat = vi.fn()
  const attached = vi.fn()
  m.on('attached', attached)
  m.channel('news').listen('.murmur.heartbeat', heartbeat)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.dispatchEvent(new Event('open'))
  first.emit('murmur.attached', {}, maxCursor)
  first.fail()
  await wait(10)

  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('lastEventId')).toBe(maxCursor)
  retry.dispatchEvent(new Event('open'))
  retry.emit('murmur.heartbeat', { topic: 'news', data: 'control' }, '9-0')
  expect(m.stats()).toMatchObject({ lastEventId: maxCursor, events: 0 })
  expect(attached).toHaveBeenCalledExactlyOnceWith()

  retry.emit('murmur.attached', {}, '')
  expect(m.stats().lastEventId).toBe(maxCursor)
  retry.emit('murmur.heartbeat', { topic: 'news', data: 'control' }, '10-0')
  expect(m.stats().lastEventId).toBe('10-0')
  retry.emit('murmur.heartbeat', {}, '9999999999999999999-10')
  retry.emit('murmur.heartbeat', {}, '11-0\n')
  retry.emit('murmur.heartbeat', {}, '')
  expect(m.stats()).toMatchObject({ lastEventId: '10-0', events: 0, lastEventAt: undefined })
  expect(heartbeat).not.toHaveBeenCalled()
  expect(attached).toHaveBeenCalledTimes(2)

  retry.fail()
  await wait(10)
  const next = FakeEventSource.instances[2]
  assert.isDefined(next)
  expect(new URL(next.url).searchParams.get('lastEventId')).toBe('10-0')
})

it('retries early ends with growing, capped backoff and keeps the unacknowledged topics fresh', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.4)
  const m = client({ retryMs: 10, maxRetryMs: 25, retryJitter: 0.5 })
  const attached = vi.fn()
  m.on('attached', attached)
  m.channel('news')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.dispatchEvent(new Event('open'))
  first.emit('murmur.attached', {}, '10-0')
  m.channel('alerts')
  await flush()

  for (const [offset, delay] of [10, 24, 30, 30].entries()) {
    const index = offset + 1
    const source = FakeEventSource.instances[index]
    assert.isDefined(source)
    source.dispatchEvent(new Event('open'))
    expect(m.stats().connected).toBe(true)
    source.emit('murmur.connected', { connection_id: '0123456789abcdef0123456789abcdef', protocol: 1 }, '')
    source.emit('murmur.heartbeat', {}, '11-0')
    expect(new URL(source.url).searchParams.get('fresh')).toBe('alerts')
    source.fail()
    expect(m.stats().connected).toBe(false)
    await wait(delay - 1)
    expect(FakeEventSource.instances).toHaveLength(index + 1)
    await wait(1)
  }

  const recovered = FakeEventSource.instances[5]
  assert.isDefined(recovered)
  expect(new URL(recovered.url).searchParams.has('fresh')).toBe(true)
  expect(new URL(recovered.url).searchParams.get('lastEventId')).toBe('10-0')
  expect(attached).toHaveBeenCalledTimes(1)
})
