import { afterEach, assert, beforeEach, expect, it, vi } from 'vitest'
import type { Authorizer } from '../src/index'
import { FakeEventSource } from './fake-event-source'
import { authFetch, client, failingThenOk, flush, nowSeconds, reset, topicsOf, wait } from './helpers'

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
  expect(authFetch.calls[0].body).toEqual({ channels: ['private-orders.1'] })
  assert.isDefined(FakeEventSource.instances[0])
  const url = new URL(FakeEventSource.instances[0].url)
  expect(url.searchParams.get('topics')).toBe('private-orders.1')
  expect(url.searchParams.get('token')).toBe('jwt-1')
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

it('fetches a fresh token after a connection fails before opening', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>()
    .mockResolvedValueOnce(Response.json({ token: 'rejected', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(Response.json({ token: 'fresh', expires_at: nowSeconds() + 900 }))
  const m = client({ fetch, retryMs: 10 })
  m.private('orders')
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(new URL(first.url).searchParams.get('token')).toBe('rejected')

  first.fail()
  await wait(10)

  expect(fetch).toHaveBeenCalledTimes(2)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(new URL(retry.url).searchParams.get('token')).toBe('fresh')
})

it('keeps retrying when authorization fails', async () => {
  const fetch = failingThenOk()
  const m = client({ fetch, retryMs: 10 })
  m.private('a')
  await wait(30)
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('backs off consecutive authorization failures with jitter and caps the base delay', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
  let attempts = 0
  const m = client({
    fetch: async () => {
      attempts++
      return new Response(null, { status: 403 })
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

it('resets channel retry backoff when the connection opens', async () => {
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
  third.fail()

  await wait(99)
  expect(FakeEventSource.instances).toHaveLength(3)
  await wait(1)
  expect(FakeEventSource.instances).toHaveLength(4)
})

it('emits unauthorized with each 4xx authorization status and skips server errors', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0)
  const responses = [401, 403, 419, 422, 500]
  const fetch: typeof globalThis.fetch = async () => new Response(null, { status: responses.shift() ?? 500 })
  const statuses: number[] = []
  const m = client({ fetch, retryMs: 10 })
  m.on('unauthorized', status => statuses.push(status))
  m.private('orders')

  await flush()
  await wait(150)
  expect(statuses).toEqual([401, 403, 419, 422])
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

it('discards an authorization response after the channel set changes', async () => {
  const { fetch, requests } = pendingAuthFetch()
  const m = client({ fetch })
  m.private('a')
  await flush()
  const first = requests[0]
  assert.isDefined(first)

  m.private('b')
  await flush()
  const second = requests[1]
  assert.isDefined(second)
  expect(requests).toHaveLength(2)
  expect(first.signal?.aborted).toBe(true)
  first.resolve(Response.json({ token: 'obsolete', expires_at: nowSeconds() + 900 }))
  await flush()
  expect(FakeEventSource.instances).toHaveLength(0)

  second.resolve(Response.json({ token: 'current', expires_at: nowSeconds() + 900 }))
  await flush()
  expect(FakeEventSource.instances).toHaveLength(1)
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  expect(topicsOf(source)).toEqual(['private-a', 'private-b'])
  expect(new URL(source.url).searchParams.get('token')).toBe('current')
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

it('fetches a new token when another private channel is joined', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>()
    .mockResolvedValueOnce(Response.json({ token: 'first', expires_at: nowSeconds() + 900 }))
    .mockResolvedValueOnce(Response.json({ token: 'second', expires_at: nowSeconds() + 900 }))
  const m = client({ fetch })
  m.private('a')
  await flush()

  m.private('b')
  await flush()

  expect(fetch).toHaveBeenCalledTimes(2)
  expect(fetch.mock.calls[1]?.[1]?.body).toBe('{"channels":["private-a","private-b"]}')
  const source = FakeEventSource.instances[1]
  assert.isDefined(source)
  expect(new URL(source.url).searchParams.get('token')).toBe('second')
})

it('sends credentials, JSON acceptance, and the decoded XSRF token for authorization', async () => {
  vi.stubGlobal('document', { cookie: 'other=value; XSRF-TOKEN=a%2Bb%3D' })
  const fetch = vi.fn(authFetch({ token: 'jwt-1', expires_at: nowSeconds() + 900 }))
  client({ fetch }).private('a')
  await flush()

  expect(fetch).toHaveBeenCalledWith('/murmur/auth', expect.objectContaining({
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
    body: '{"channels":["private-orders"]}',
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
  { error: { status: 400 }, statuses: [400] },
  { error: { status: 499 }, statuses: [499] },
  { error: { status: 500 }, statuses: [] },
  { error: { status: '403' }, statuses: [] },
  { error: new Error('Authorization unavailable'), statuses: [] },
  { error: null, statuses: [] },
])('retries a rejected authorizer and emits unauthorized only for numeric 4xx statuses ($error)', async ({
  error, statuses,
}) => {
  const authorizer = vi.fn()
    .mockRejectedValueOnce(error)
    .mockResolvedValue({ token: 'valid', expires_at: nowSeconds() + 900 })
  const unauthorized = vi.fn()
  const m = client({ authorizer, retryMs: 10 })
  m.on('unauthorized', unauthorized)
  m.private('orders')
  await flush()

  expect(unauthorized.mock.calls.flat()).toEqual(statuses)
  expect(FakeEventSource.instances).toHaveLength(0)
  await wait(9)
  expect(authorizer).toHaveBeenCalledTimes(1)
  await wait(1)
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
    lastEventId: undefined, lastEventAt: undefined,
  }
  const snapshot = m.stats()

  expect(Object.getPrototypeOf(snapshot)).toBe(Object.prototype)
  expect(snapshot).toStrictEqual(initial)
  snapshot.events = 100
  expect(m.stats()).not.toBe(snapshot)
  expect(m.stats()).toStrictEqual(initial)
})

it('tracks opens and failures across retries and deliberate channel reconnects', async () => {
  const m = client({ retryMs: 10 })
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
  await wait(10)
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
    lastEventId: undefined, lastEventAt: undefined,
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
  expect(m.stats()).toMatchObject({ tokenRequests: 1, tokenFailures: 1, disconnections: 1 })
  await wait(10)
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.dispatchEvent(new Event('open'))
  expect(m.stats()).toMatchObject({ tokenRequests: 2, tokenFailures: 1, disconnections: 1 })

  m.channel('news')
  await flush()
  expect(FakeEventSource.instances).toHaveLength(2)
  expect(m.stats()).toMatchObject({ tokenRequests: 2, tokenFailures: 1, disconnections: 1 })
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
    lastEventId: '1-0', lastEventAt,
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
