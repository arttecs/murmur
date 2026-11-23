import { afterEach, assert, beforeEach, expect, it, vi } from 'vitest'
import { FakeEventSource } from './fake-event-source'
import { authFetch, client, failingThenOk, flush, nowSeconds, reset, topicsOf, wait } from './helpers'

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
})

afterEach(() => {
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
