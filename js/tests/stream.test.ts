import { afterEach, assert, beforeEach, expect, it, vi } from 'vitest'
import { FakeEventSource } from './fake-event-source'
import { client, flush, reset, wait } from './helpers'

/** The initial URL for tests of an authorized stream. */
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
  FakeEventSource.instances[0].emit('token', { topic: 'private-c', data: { text: 'Meet me at the café' } }, '101-0')
  expect(seen).toEqual([{ text: 'Meet me at the café' }])
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
    await wait(13)
  }
  expect(expired).toBe(1)
  expect(FakeEventSource.instances).toHaveLength(3)
})

it('backs off stream retries with jitter and stops on the third failure', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
  const expired = vi.fn()
  client({ retryMs: 100 }).stream(URL_1).on('expired', expired)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.fail()

  await wait(99)
  expect(FakeEventSource.instances).toHaveLength(1)
  await wait(1)
  const second = FakeEventSource.instances[1]
  assert.isDefined(second)
  second.fail()

  await wait(224)
  expect(FakeEventSource.instances).toHaveLength(2)
  await wait(1)
  const third = FakeEventSource.instances[2]
  assert.isDefined(third)
  expect(expired).not.toHaveBeenCalled()
  third.fail()

  expect(expired).toHaveBeenCalledExactlyOnceWith()
  expect(vi.getTimerCount()).toBe(0)
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
    await wait(13)
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
    await wait(13)
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

it('expires streams at the client-wide failure limit', async () => {
  const expired = vi.fn()
  client({ streamMaxFailures: 1 }).stream(URL_1).on('expired', expired)
  await flush()
  const source = FakeEventSource.instances[0]
  assert.isDefined(source)

  source.fail()

  expect(expired).toHaveBeenCalledExactlyOnceWith()
  expect(vi.getTimerCount()).toBe(0)
})

it('overrides client retry settings and the failure limit for one stream', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
  const m = client({ retryMs: 100, maxRetryMs: 400, retryJitter: 0, streamMaxFailures: 1 })
  const expired = vi.fn()
  m.stream(URL_1, { retryMs: 20, maxRetryMs: 30, retryJitter: 1, maxFailures: 3 }).on('expired', expired)
  await flush()

  for (const [index, delay] of [20, 45].entries()) {
    const source = FakeEventSource.instances[index]
    assert.isDefined(source)
    source.fail()
    expect(expired).not.toHaveBeenCalled()
    await wait(delay - 1)
    expect(FakeEventSource.instances).toHaveLength(index + 1)
    await wait(1)
    expect(FakeEventSource.instances).toHaveLength(index + 2)
  }

  const last = FakeEventSource.instances[2]
  assert.isDefined(last)
  last.fail()
  expect(expired).toHaveBeenCalledExactlyOnceWith()
  expect(vi.getTimerCount()).toBe(0)
})

it.each([
  ['retryMs', 0],
  ['retryMs', -1],
  ['retryMs', Infinity],
  ['retryMs', NaN],
  ['maxRetryMs', 0],
  ['maxRetryMs', -1],
  ['maxRetryMs', Infinity],
  ['maxRetryMs', NaN],
  ['maxRetryMs', 19],
  ['retryJitter', -0.01],
  ['retryJitter', 1.01],
  ['retryJitter', Infinity],
  ['retryJitter', NaN],
  ['maxFailures', 0],
  ['maxFailures', -1],
  ['maxFailures', 1.5],
  ['maxFailures', Infinity],
  ['maxFailures', NaN],
])('rejects invalid %s (%s) on a stream', (option, value) => {
  const m = client({ retryMs: 20 })
  const open = () => m.stream(URL_1, { [option]: value })
  expect(open).toThrow(RangeError)
  expect(open).toThrow(option)
})

it('resumes from the attach marker without dispatching it or adding fresh', async () => {
  const marker = vi.fn()
  client({ retryMs: 10 }).stream(URL_1).on('murmur.attached', marker)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.dispatchEvent(new Event('open'))
  first.emit('murmur.attached', { topic: 'private-c', data: 'control' }, '108-0')
  first.fail()

  await wait(10)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(retry.url).toBe('https://m.test/sse?topics=private-c&token=t&lastEventId=108-0')
  expect(marker).not.toHaveBeenCalled()
  expect(first.closed).toBe(true)
})

it('updates a stream cursor only from newer numbered heartbeats after the marker', async () => {
  const heartbeat = vi.fn()
  client({ retryMs: 10 }).stream(URL_1).on('murmur.heartbeat', heartbeat)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.dispatchEvent(new Event('open'))
  first.emit('murmur.heartbeat', { topic: 'private-c', data: 'control' }, '')
  first.emit('murmur.heartbeat', { topic: 'private-c', data: 'control' }, '200-0')
  first.emit('murmur.attached', {}, '')
  first.emit('murmur.heartbeat', {}, '99-0')
  first.emit('murmur.heartbeat', {}, '')
  first.fail()

  await wait(10)
  const retry = FakeEventSource.instances[1]
  assert.isDefined(retry)
  expect(retry.url).toBe(URL_1)
  retry.dispatchEvent(new Event('open'))
  retry.emit('murmur.attached', {}, '90-0')
  retry.emit('murmur.heartbeat', {}, '91-0')
  retry.emit('murmur.heartbeat', {}, '90-1')
  retry.emit('murmur.heartbeat', {}, '091-0')
  retry.emit('murmur.heartbeat', {}, '')
  retry.fail()

  await wait(10)
  const next = FakeEventSource.instances[2]
  assert.isDefined(next)
  expect(new URL(next.url).searchParams.get('lastEventId')).toBe('91-0')
  expect(heartbeat).not.toHaveBeenCalled()
})

it('listens for numbered heartbeats on a stream without an application heartbeat listener', async () => {
  client({ retryMs: 10 }).stream(URL_1)
  await flush()
  const first = FakeEventSource.instances[0]
  assert.isDefined(first)
  first.dispatchEvent(new Event('open'))
  first.emit('murmur.attached', {}, '101-0')
  first.emit('murmur.heartbeat', {}, '102-0')
  first.fail()

  await wait(10)
  const next = FakeEventSource.instances[1]
  assert.isDefined(next)
  expect(new URL(next.url).searchParams.get('lastEventId')).toBe('102-0')
})
