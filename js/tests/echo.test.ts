import Echo, { type EchoOptions } from 'laravel-echo'
import { afterEach, assert, beforeEach, expect, it, vi } from 'vitest'
import { MurmurConnector, type MurmurConnectorOptions } from '../src/echo'
import { Murmur } from '../src/index'
import { FakeEventSource } from './fake-event-source'
import { authFetch, flush, nowSeconds, reset, topicsOf, wait } from './helpers'

/** The Echo instances to disconnect after the current test. */
const instances: Echo<'function'>[] = []

/** Creates a real Echo instance, passing through auth providers supported at runtime. */
function echo(options: Partial<MurmurConnectorOptions> = {}): Echo<'function'> {
  const { auth, ...overrides } = options
  const instance = new Echo<'function'>({
    broadcaster: MurmurConnector,
    url: 'https://app.test/sse',
    EventSource: FakeEventSource,
    fetch: authFetch({ token: 'jwt-echo', expires_at: nowSeconds() + 900 }),
    retryMs: 10,
    ...overrides,
    auth: auth as EchoOptions<'function'>['auth'],
  })
  instances.push(instance)
  return instance
}

/** Narrows the connector that Echo's declarations omit for custom drivers. */
function connectorOf(instance: Echo<'function'>): MurmurConnector {
  const connector: unknown = instance.connector
  assert.instanceOf(connector, MurmurConnector)
  return connector
}

/** Gets a specific connection after pending subscription work has run. */
async function connection(index = 0): Promise<FakeEventSource> {
  await flush()
  const source = FakeEventSource.instances[index]
  assert.isDefined(source)
  return source
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
})

afterEach(() => {
  for (const instance of instances.splice(0)) {
    instance.disconnect()
  }
  reset()
  vi.useRealTimers()
})

it('delivers public and authorized private events through Echo', async () => {
  const instance = echo()
  const publicly = vi.fn()
  const privately = vi.fn()
  instance.listen('orders', 'Shipped', publicly)
  instance.private('orders').listen('Shipped', privately)
  const source = await connection()

  source.emit('App\\Events\\Shipped', { topic: 'orders', data: 1 }, '1-0')
  source.emit('App\\Events\\Shipped', { topic: 'private-orders', data: 2 }, '2-0')

  expect(topicsOf(source)).toEqual(['orders', 'private-orders'])
  expect(new URL(source.url).searchParams.get('token')).toBe('jwt-echo')
  expect(authFetch.calls).toEqual([{ body: { channel_name: 'private-orders' } }])
  expect(publicly).toHaveBeenCalledExactlyOnceWith(1)
  expect(privately).toHaveBeenCalledExactlyOnceWith(2)
})

it.each([
  { event: 'Sub.Shipped', wire: 'Shop\\Events\\Sub\\Shipped' },
  { event: 'Sub\\Shipped', wire: 'Shop\\Events\\Sub\\Shipped' },
  { event: '.order.shipped', wire: 'order.shipped' },
  { event: '\\Other\\Events\\Shipped', wire: 'Other\\Events\\Shipped' },
  { event: '..order.shipped', wire: '.order.shipped' },
])('formats $event as $wire with the configured namespace', async ({ event, wire }) => {
  const callback = vi.fn()
  echo({ namespace: 'Shop.Events' }).channel('orders').listen(event, callback)
  const source = await connection()

  source.emit(wire, { topic: 'orders', data: 1 }, '1-0')

  expect(callback).toHaveBeenCalledExactlyOnceWith(1)
})

it('converts dots to backslashes even when the namespace is disabled', async () => {
  const callback = vi.fn()
  echo({ namespace: false }).channel('orders').listen('Sub.Shipped', callback)
  const source = await connection()

  source.emit('Sub\\Shipped', { topic: 'orders', data: 1 }, '1-0')

  expect(callback).toHaveBeenCalledExactlyOnceWith(1)
})

it.each(['public', 'private'] as const)('removes one or all callbacks on a %s channel', async kind => {
  const instance = echo()
  const name = 'orders'
  const topic = kind === 'private' ? 'private-orders' : name
  const channel = kind === 'private' ? instance.private(name) : instance.channel(name)
  const first = vi.fn()
  const second = vi.fn()
  channel.listen('Sub.Shipped', first).listen('Sub.Shipped', second)
  const source = await connection()

  channel.stopListening('Sub\\Shipped', first)
  source.emit('App\\Events\\Sub\\Shipped', { topic, data: 1 }, '1-0')
  channel.stopListening('Sub.Shipped')
  source.emit('App\\Events\\Sub\\Shipped', { topic, data: 2 }, '2-0')

  expect(first).not.toHaveBeenCalled()
  expect(second).toHaveBeenCalledExactlyOnceWith(1)
})

it('delivers notifications and removes only the requested notification callback', async () => {
  const first = vi.fn()
  const second = vi.fn()
  const channel = echo().private('users.1').notification(first).notification(second)
  const source = await connection()
  const event = 'Illuminate\\Notifications\\Events\\BroadcastNotificationCreated'

  source.emit(event, { topic: 'private-users.1', data: { id: 'first' } }, '1-0')
  channel.stopListeningForNotification(first)
  source.emit(event, { topic: 'private-users.1', data: { id: 'second' } }, '2-0')

  expect(first).toHaveBeenCalledExactlyOnceWith({ id: 'first' })
  expect(second.mock.calls).toEqual([[{ id: 'first' }], [{ id: 'second' }]])
})

it('leaves the exact wire channel and leaves both variants by their base name', async () => {
  const instance = echo()
  const publicly = vi.fn()
  const privately = vi.fn()
  const publicChannel = instance.channel('orders').listen('.shipped', publicly)
  const privateChannel = instance.private('orders').listen('.shipped', privately)
  const first = await connection()

  instance.leaveChannel('private-orders')
  first.emit('shipped', { topic: 'private-orders', data: 1 }, '1-0')
  first.emit('shipped', { topic: 'orders', data: 2 }, '2-0')
  const second = await connection(1)
  expect(topicsOf(second)).toEqual(['orders'])
  expect(instance.channel('orders')).toBe(publicChannel)
  expect(instance.private('orders')).not.toBe(privateChannel)
  const third = await connection(2)

  instance.leave('orders')
  third.emit('shipped', { topic: 'orders', data: 3 }, '3-0')
  third.emit('shipped', { topic: 'private-orders', data: 4 }, '4-0')
  await flush()

  expect(privately).not.toHaveBeenCalled()
  expect(publicly).toHaveBeenCalledExactlyOnceWith(2)
  expect(third.closed).toBe(true)
  expect(connectorOf(instance).murmur.stats().channels).toBe(0)
})

it('leaves all channels through the registry Echo iterates', async () => {
  const instance = echo()
  instance.channel('__proto__')
  instance.private('orders')
  const source = await connection()

  instance.leaveAllChannels()

  expect(source.closed).toBe(true)
  expect(connectorOf(instance).murmur.stats().channels).toBe(0)
})

it('keeps one authorized subscription for a private wire name', async () => {
  const instance = echo()
  const channel = instance.channel('private-orders')
  expect(instance.private('orders')).toBe(channel)
  const source = await connection()
  expect(topicsOf(source)).toEqual(['private-orders'])

  instance.leaveChannel('private-orders')

  expect(source.closed).toBe(true)
  expect(connectorOf(instance).murmur.stats().channels).toBe(0)
})

it('throws an unsupported-channel error for presence and encrypted channels', () => {
  const instance = echo()

  expect(() => instance.join('orders')).toThrow(/Murmur does not support presence channels/)
  expect(() => instance.encryptedPrivate('orders')).toThrow(/does not support encrypted private channels\.$/)
  expect(() => connectorOf(instance).encryptedPrivateChannel('orders')).toThrow(/Murmur does not support encrypted private channels/)
})

it('keeps whisper methods chainable without registering or sending client events', async () => {
  const channel = echo().private('orders')
  const whisper = vi.fn()
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  const broadcast = vi.fn()
  expect(channel.listenForWhisper('typing', whisper).whisper('typing', { id: 1 })).toBe(channel)
  channel.listen('.client-typing', broadcast).stopListeningForWhisper('typing')
  const source = await connection()

  source.emit('client-typing', { topic: 'private-orders', data: { id: 2 } }, '1-0')

  expect(whisper).not.toHaveBeenCalled()
  expect(warn).toHaveBeenCalledExactlyOnceWith('Murmur does not support client events: whisper and listenForWhisper do nothing.')
  expect(broadcast).toHaveBeenCalledExactlyOnceWith({ id: 2 })
  expect(authFetch.calls).toHaveLength(1)
})

it('reports subscriptions on open and after reconnect and drops departed callbacks', async () => {
  const instance = echo()
  const firstSubscribed = vi.fn()
  const secondSubscribed = vi.fn()
  instance.channel('orders').subscribed(firstSubscribed)
  const first = await connection()
  expect(firstSubscribed).not.toHaveBeenCalled()
  first.dispatchEvent(new Event('open'))

  instance.channel('news').subscribed(secondSubscribed)
  expect(secondSubscribed).not.toHaveBeenCalled()
  const second = await connection(1)
  second.dispatchEvent(new Event('open'))
  expect(firstSubscribed).toHaveBeenCalledTimes(2)
  expect(secondSubscribed).toHaveBeenCalledExactlyOnceWith()

  second.fail()
  await wait(10)
  const retry = await connection(2)
  retry.dispatchEvent(new Event('open'))
  instance.leaveChannel('orders')
  const remaining = await connection(3)
  remaining.dispatchEvent(new Event('open'))

  expect(firstSubscribed).toHaveBeenCalledTimes(3)
  expect(secondSubscribed).toHaveBeenCalledTimes(3)
})

it('waits for the next open when a subscription callback joins a channel', async () => {
  const instance = echo()
  const subscribed = vi.fn()
  instance.channel('orders').subscribed(() => instance.channel('news').subscribed(subscribed))
  const first = await connection()
  first.dispatchEvent(new Event('open'))

  expect(subscribed).not.toHaveBeenCalled()
  const next = await connection(1)
  next.dispatchEvent(new Event('open'))
  expect(subscribed).toHaveBeenCalledExactlyOnceWith()
})

it('reports a denied token request to every private channel and to no public channel', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>()
    .mockResolvedValueOnce(new Response(null, { status: 403 }))
    .mockResolvedValueOnce(new Response(null, { status: 500 }))
  const instance = echo({ fetch })
  const privateError = vi.fn()
  const publicError = vi.fn()
  instance.private('orders').error(privateError)
  instance.channel('news').error(publicError)

  await flush()
  await wait(10)

  expect(privateError).toHaveBeenCalledExactlyOnceWith(403)
  expect(publicError).not.toHaveBeenCalled()
  expect(FakeEventSource.instances).toHaveLength(1)
})

it('leaves the socket ID undefined and closes the client and pending retries on disconnect', async () => {
  const instance = echo()
  expect(instance.socketId()).toBeUndefined()
  const { murmur } = connectorOf(instance)
  expect(murmur).toBeInstanceOf(Murmur)
  instance.channel('news')
  const source = await connection()
  source.dispatchEvent(new Event('open'))
  const stream = murmur.stream('https://app.test/sse?topics=stream')
  const streamSource = await connection(1)
  source.fail()

  instance.disconnect()
  await wait(50)

  expect(source.closed).toBe(true)
  expect(streamSource.closed).toBe(true)
  expect(FakeEventSource.instances).toHaveLength(2)
  expect(murmur.stats().channels).toBe(0)
  stream.close()
})

it('can connect again after disconnecting', async () => {
  const instance = echo()
  instance.channel('news')
  const first = await connection()

  instance.disconnect()
  connectorOf(instance).connect()
  instance.private('orders')
  const second = await connection(1)
  expect(first.closed).toBe(true)
  expect(topicsOf(second)).toEqual(['private-orders'])
})

it('reports connection transitions using Echo status names and unsubscribes', async () => {
  const instance = echo()
  const status = vi.fn()
  const unsubscribe = connectorOf(instance).onConnectionChange(status)
  expect(instance.connectionStatus()).toBe('disconnected')
  expect(status).not.toHaveBeenCalled()
  expect(FakeEventSource.instances).toHaveLength(0)

  instance.channel('news')
  expect(instance.connectionStatus()).toBe('connecting')
  const first = await connection()
  first.dispatchEvent(new Event('open'))
  expect(instance.connectionStatus()).toBe('connected')
  first.fail()
  expect(instance.connectionStatus()).toBe('reconnecting')
  await wait(10)
  const second = await connection(1)
  second.dispatchEvent(new Event('open'))
  instance.leaveChannel('news')
  expect(instance.connectionStatus()).toBe('disconnected')
  expect(status.mock.calls).toEqual([
    ['connecting'], ['connected'], ['reconnecting'], ['connected'], ['disconnected'],
  ])

  unsubscribe()
  instance.channel('news')
  const third = await connection(2)
  third.dispatchEvent(new Event('open'))
  instance.disconnect()
  expect(instance.connectionStatus()).toBe('disconnected')
  expect(status).toHaveBeenCalledTimes(5)
})

it('requires a non-empty SSE URL', () => {
  expect(() => new Echo({ broadcaster: MurmurConnector })).toThrow(/Murmur.*url/)
  expect(() => echo({ url: '' })).toThrow(/Murmur.*url/)
})

it('passes the auth request options and sends the bearer token without mutating headers', async () => {
  const headers = { authorization: 'old', 'X-Test': 'kept' }
  const fetch = vi.fn(authFetch({ token: 'credentials', expires_at: nowSeconds() + 900 }))
  const instance = echo({
    url: 'https://sse.test/events',
    authEndpoint: 'https://api.test/tokens',
    auth: { headers, credentials: 'omit' },
    bearerToken: 'secret',
    withCredentials: true,
    fetch,
  })
  instance.private('orders')
  const source = await connection()
  const [url, init] = fetch.mock.calls[0] ?? []

  expect(url).toBe('https://api.test/tokens')
  expect(init?.credentials).toBe('omit')
  expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer secret')
  expect(new Headers(init?.headers).get('X-Test')).toBe('kept')
  expect(headers.authorization).toBe('old')
  expect(new URL(source.url).origin + new URL(source.url).pathname).toBe('https://sse.test/events')
  expect(source.withCredentials).toBe(true)
})

it('evaluates asynchronous headers at each token request when a bearer token is supplied', async () => {
  const fetch = vi.fn(authFetch({ token: 'dynamic', expires_at: nowSeconds() + 900 }))
  const headers = vi.fn()
    .mockResolvedValueOnce({ 'X-Rotating': 'first' })
    .mockResolvedValueOnce({ 'X-Rotating': 'second' })
  const instance = echo({ auth: { headers }, bearerToken: 'secret', fetch })
  instance.private('orders')
  await connection()
  instance.private('news')
  await connection(1)

  expect(fetch.mock.calls.map(([, init]) => {
    const requestHeaders = new Headers(init?.headers)
    return [requestHeaders.get('Authorization'), requestHeaders.get('X-Rotating')]
  })).toEqual([['Bearer secret', 'first'], ['Bearer secret', 'second']])
})

it('uses a custom authorizer and forwards the retry and token refresh settings', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>()
  const authorizer = vi.fn<NonNullable<MurmurConnectorOptions['authorizer']>>()
    .mockResolvedValue({ token: 'custom', expires_at: nowSeconds() + 20 })
  const instance = echo({ fetch, authorizer, retryMs: 12, maxRetryMs: 12, retryJitter: 0, tokenRefreshMarginSeconds: 25 })
  instance.private('orders')
  const first = await connection()
  first.dispatchEvent(new Event('open'))
  first.fail()
  await wait(11)
  expect(FakeEventSource.instances).toHaveLength(1)
  await wait(1)
  const second = await connection(1)
  second.fail()
  await wait(12)

  expect(FakeEventSource.instances).toHaveLength(3)
  expect(authorizer).toHaveBeenCalledTimes(3)
  expect(authorizer).toHaveBeenLastCalledWith(['private-orders'], expect.any(AbortSignal))
  expect(fetch).not.toHaveBeenCalled()
  expect(new URL(second.url).searchParams.get('token')).toBe('custom')
})

it('uses stream options on the exposed Murmur client', async () => {
  const connector = connectorOf(echo({ streamMaxFailures: 1 }))
  const expired = vi.fn()
  connector.murmur.stream('https://app.test/sse?topics=stream').on('expired', expired)
  const source = await connection()

  source.fail()

  expect(expired).toHaveBeenCalledExactlyOnceWith()
})

it('isolates channels, listeners, connections, and bearer headers across Echo instances', async () => {
  const fetch = vi.fn(authFetch({ token: 'jwt-echo', expires_at: nowSeconds() + 900 }))
  const first = echo({ bearerToken: 'first', url: 'https://first.test/sse', fetch })
  const second = echo({ url: 'https://second.test/sse', fetch })
  const onlyFirst = vi.fn()
  const onlySecond = vi.fn()
  first.private('orders').listen('.shipped', onlyFirst)
  second.private('orders').listen('.shipped', onlySecond)
  await flush()
  const firstSource = FakeEventSource.instances.find(source => source.url.startsWith('https://first.test/'))
  const secondSource = FakeEventSource.instances.find(source => source.url.startsWith('https://second.test/'))
  assert.isDefined(firstSource)
  assert.isDefined(secondSource)

  firstSource.emit('shipped', { topic: 'private-orders', data: 1 }, '1-0')
  first.disconnect()
  secondSource.emit('shipped', { topic: 'private-orders', data: 2 }, '1-0')

  expect(onlyFirst).toHaveBeenCalledExactlyOnceWith(1)
  expect(onlySecond).toHaveBeenCalledExactlyOnceWith(2)
  expect(firstSource.closed).toBe(true)
  expect(secondSource.closed).toBe(false)
  expect(fetch.mock.calls.map(([, init]) => new Headers(init?.headers).get('Authorization')))
    .toEqual(expect.arrayContaining(['Bearer first', null]))
})
