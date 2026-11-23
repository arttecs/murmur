import { afterEach, assert, beforeEach, expect, it, vi } from 'vitest'
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
  FakeEventSource.instances[0].fail()
  await wait(20)
  expect(fetch.calls).toHaveLength(1)
})

it('keeps retrying when authorization fails', async () => {
  const fetch = failingThenOk()
  const m = client({ fetch, retryMs: 10 })
  m.private('a')
  await wait(30)
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

function pendingAuthFetch() {
  const requests: { resolve: (response: Response) => void; signal?: AbortSignal | null }[] = []
  const fetch: typeof globalThis.fetch = (_input, init) => new Promise(resolve => {
    requests.push({ resolve, signal: init?.signal })
  })
  return { fetch, requests }
}
