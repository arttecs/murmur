import { afterEach, assert, beforeEach, expect, it, vi } from 'vitest'
import { FakeEventSource } from './fake-event-source'
import { client, flush, reset, wait } from './helpers'

const URL_1 = 'https://m.test/sse?topics=private-c&token=t&lastEventId=100-0'

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
})

afterEach(() => {
  vi.restoreAllMocks()
  reset()
  vi.useRealTimers()
})

it('opens the given url untouched', async () => {
  client().stream('https://m.test/sse?topics=private-c&token=t&lastEventId=100-0')
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  expect(FakeEventSource.instances[0].url).toBe('https://m.test/sse?topics=private-c&token=t&lastEventId=100-0')
})

it('delivers literal event names with the envelope removed', async () => {
  const seen: unknown[] = []
  client().stream(URL_1).on('token', d => seen.push(d))
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  FakeEventSource.instances[0].emit('token', { topic: 'private-c', data: { text: 'Olá' } }, '101-0')
  expect(seen).toEqual([{ text: 'Olá' }])
})

it('resumes from the last received id', async () => {
  client({ retryMs: 10 }).stream(URL_1).on('token', () => {})
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  FakeEventSource.instances[0].emit('token', { topic: 'private-c', data: 1 }, '105-2')
  FakeEventSource.instances[0].fail()
  await wait(20)
  assert.isDefined(FakeEventSource.instances[1])
  const url = new URL(FakeEventSource.instances[1].url)
  expect(url.searchParams.get('lastEventId')).toBe('105-2')
  expect(url.searchParams.get('token')).toBe('t')
})

it('closes on end and does not reconnect', async () => {
  let ended = 0
  client({ retryMs: 10 }).stream(URL_1).on('end', () => ended++)
  await flush()
  assert.isDefined(FakeEventSource.instances[0])
  FakeEventSource.instances[0].emit('murmur.end', { topic: 'private-c', data: null }, '106-0')
  await wait(20)
  expect(ended).toBe(1)
  expect(FakeEventSource.instances).toHaveLength(1)
  expect(FakeEventSource.instances[0].closed).toBe(true)
})

it('gives up after three consecutive failures', async () => {
  let expired = 0
  client({ retryMs: 5 }).stream(URL_1).on('expired', () => expired++)
  await flush()
  for (let i = 0; i < 3; i++) {
    const source = FakeEventSource.instances[i]
    assert.isDefined(source)
    source.fail()
    await wait(10)
  }
  expect(expired).toBe(1)
  expect(FakeEventSource.instances).toHaveLength(3)
})

it('changes only the replay cursor on reconnect', async () => {
  const url = '/sse?topics=private%2dc&lastEventId=100-0&token=t%20%7e&filter=a+b&filter=a%20b#fragment'
  const stream = client({ retryMs: 5 }).stream(url).on('token', () => {})
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  expect(first.url).toBe(url)
  first.fail()

  await wait(5)
  const second = FakeEventSource.instances[1]
  assert.isDefined(second)
  expect(second.url).toBe(url)
  second.emit('token', { topic: 'private-c', data: 1 }, '107-0')
  second.fail()

  await wait(5)
  const third = FakeEventSource.instances[2]
  assert.isDefined(third)
  expect(third.url).toBe('/sse?topics=private%2dc&lastEventId=107-0&token=t%20%7e&filter=a+b&filter=a%20b#fragment')
  stream.close()
})

it('appends a replay cursor when the stream URL has none', async () => {
  client({ retryMs: 5 }).stream('/sse?topics=news#fragment').on('token', () => {})
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.emit('token', { topic: 'news', data: 1 }, '107-0')
  first.fail()

  await wait(5)
  const second = FakeEventSource.instances[1]
  assert.isDefined(second)
  expect(second.url).toBe('/sse?topics=news&lastEventId=107-0#fragment')
})

it('emits gap and resets the failure count after receiving it', async () => {
  const gap = vi.fn()
  const expired = vi.fn()
  client({ retryMs: 5 }).stream(URL_1).on('gap', gap).on('expired', expired)
  await flush()

  for (let i = 0; i < 2; i++) {
    const source = FakeEventSource.instances[i]
    assert.isDefined(source)
    source.fail()
    await wait(5)
  }

  const third = FakeEventSource.instances[2]
  assert.isDefined(third)
  third.emit('murmur.gap', {}, '')
  expect(gap).toHaveBeenCalledExactlyOnceWith()
  third.fail()
  await wait(5)

  expect(expired).not.toHaveBeenCalled()
  expect(FakeEventSource.instances).toHaveLength(4)
})

it('resets the failure count when the server accepts a connection', async () => {
  const expired = vi.fn()
  const stream = client({ retryMs: 5 }).stream(URL_1).on('expired', expired)
  await flush()

  for (let i = 0; i < 3; i++) {
    const source = FakeEventSource.instances[i]
    assert.isDefined(source)
    source.dispatchEvent(new Event('open'))
    source.fail()
    await wait(5)
  }

  expect(expired).not.toHaveBeenCalled()
  expect(FakeEventSource.instances).toHaveLength(4)
  stream.close()
  expect(vi.getTimerCount()).toBe(0)
})

it('resets the failure count when an event is received', async () => {
  const expired = vi.fn()
  const stream = client({ retryMs: 5 }).stream(URL_1).on('token', () => {}).on('expired', expired)
  await flush()

  for (let i = 0; i < 2; i++) {
    const source = FakeEventSource.instances[i]
    assert.isDefined(source)
    source.fail()
    await wait(5)
  }
  const third = FakeEventSource.instances[2]
  assert.isDefined(third)
  third.emit('token', { topic: 'private-c', data: 1 }, '110-0')
  third.fail()
  await wait(5)

  expect(expired).not.toHaveBeenCalled()
  expect(FakeEventSource.instances).toHaveLength(4)
  stream.close()
})

it('delivers an event whose listener was added after the stream opened', async () => {
  const listener = vi.fn()
  const stream = client().stream(URL_1)
  await flush()
  stream.on('late', listener)
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.emit('late', { topic: 'private-c', data: 2 }, '111-0')

  expect(listener).toHaveBeenCalledExactlyOnceWith(2)
  expect(FakeEventSource.instances).toHaveLength(1)
  stream.close()
})

it('delivers to the remaining stream listeners when one throws', async () => {
  const error = new Error('Stream listener failed')
  const listener = vi.fn()
  client().stream(URL_1).on('token', () => { throw error }).on('token', listener)
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  const enqueue = vi.spyOn(globalThis, 'queueMicrotask').mockImplementation(() => {})

  source.emit('token', { topic: 'private-c', data: 1 }, '101-0')

  expect(listener).toHaveBeenCalledExactlyOnceWith(1)
  expect(enqueue).toHaveBeenCalledTimes(1)
  const rethrow = enqueue.mock.calls[0]?.[0]
  assert.isDefined(rethrow)
  expect(rethrow).toThrow(error)
})

it('delivers to the remaining end listeners when one throws', async () => {
  const error = new Error('End listener failed')
  const listener = vi.fn()
  client().stream(URL_1).on('end', () => { throw error }).on('end', listener)
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  const enqueue = vi.spyOn(globalThis, 'queueMicrotask').mockImplementation(() => {})

  source.emit('murmur.end', { topic: 'private-c', data: null }, '106-0')

  expect(source.closed).toBe(true)
  expect(listener).toHaveBeenCalledExactlyOnceWith()
  expect(enqueue).toHaveBeenCalledTimes(1)
  const rethrow = enqueue.mock.calls[0]?.[0]
  assert.isDefined(rethrow)
  expect(rethrow).toThrow(error)
})

it('cancels pending retries on repeated close', async () => {
  const stream = client({ retryMs: 5 }).stream(URL_1)
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)
  source.fail()

  stream.close()
  stream.close()
  await wait(10)

  expect(source.closed).toBe(true)
  expect(FakeEventSource.instances).toHaveLength(1)
  expect(vi.getTimerCount()).toBe(0)
})

it('closes every stream when its client closes', async () => {
  const m = client({ retryMs: 5 })
  const listener = vi.fn()
  m.stream(URL_1).on('token', listener)
  m.stream(URL_1).on('token', listener)
  await flush()
  const first = FakeEventSource.instances[0]
  const second = FakeEventSource.instances[1]
  assert.isDefined(first)
  assert.isDefined(second)
  first.fail()

  m.close()
  second.dispatchEvent(new MessageEvent('token', {
    data: '{"topic":"private-c","data":1}',
    lastEventId: '101-0',
  }))
  second.dispatchEvent(new Event('error'))
  await wait(10)

  expect(first.closed).toBe(true)
  expect(second.closed).toBe(true)
  expect(listener).not.toHaveBeenCalled()
  expect(FakeEventSource.instances).toHaveLength(2)
  expect(vi.getTimerCount()).toBe(0)
})

it('does not open streams stopped before the first connection', async () => {
  const m = client()
  m.stream(URL_1).close()
  m.stream(URL_1)
  m.close()
  m.stream(URL_1)

  await flush()
  expect(FakeEventSource.instances).toHaveLength(0)
})
