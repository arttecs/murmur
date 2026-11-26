# Murmur JavaScript client

Install the ESM-only package with npm.

```sh
npm install @arttecs/murmur
```

## Channels

Create a client with your Murmur server's SSE endpoint. Public and private channels share one connection.

```js
import { Murmur } from '@arttecs/murmur'

const murmur = new Murmur({ url: 'https://events.example.com/sse' })

const news = murmur.channel('news')
news.listen('.updated', data => console.log(data))

murmur.private('orders.42')
  .listen('OrderShipped', data => console.log(data))
```

`channel('news')` subscribes to `news` and `private('orders.42')` subscribes to `private-orders.42`. Each callback receives the event payload for its channel.

Joining or leaving a channel rebuilds the shared connection with the current topics and replay cursor. A new channel starts at the server's attach point, skipping earlier events for that topic. The client keeps sending the topic as `fresh` until the server acknowledges it with `murmur.attached`. After attachment, the channel participates in ordinary replay and can receive retained events from an outage. Each retry closes the old EventSource and opens a new one with the latest cursor, credentials, and pending joins.

The default namespace is `App.Events`. The client resolves `OrderShipped` to `App\Events\OrderShipped`, converting namespace dots to backslashes. Set `namespace` to use a different namespace. Prefix an event name with a dot to use the literal name. For example, `.updated` listens for `updated`.

Laravel Echo converts dots in relative class names such as `Sub.Event` to namespace separators. Murmur applies the configured namespace prefix and preserves those dots. To listen for a class in the `Sub` sub-namespace, pass `'Sub\\Event'` in JavaScript.

Use `murmur.on(event, callback)` to listen for these client events. It returns a function that removes the listener.

- `connected` fires when the channel connection opens.
- `disconnected` fires when a transport or authorization attempt fails and is eligible for retry, or when an open connection drops.
- `unauthorized` fires as `(status, channelName)` for each denied private channel. The name is the one passed to `murmur.private()`. Register `murmur.private(name).error(callback)` to receive the numeric status for just that channel.
- `error` receives an `Error` if the shared subscription URL exceeds the request budget or the server announces an unexpected protocol. The first protocol mismatch is reported once and the client continues.
- `gap` fires when the server reports a gap in replay. Refresh any state that depends on the missing events.

The first retry uses `retryMs`. For consecutive failures after the first, the base delay doubles up to `maxRetryMs`, then random jitter of up to `retryJitter` times the base delay is added. A successful connection resets the retry sequence.

`news.stopListening('.updated')` removes the callbacks for that event. `murmur.leave('orders.42')` leaves channels joined under that name. `murmur.close()` closes the channel connection and all streams created by the client, and cancels their retries.

## Sender exclusion

`murmur.socketId()` returns the shared connection ID announced by Murmur. It returns `undefined` until that first announcement, which follows the EventSource open event. The client reuses the ID across reconnects.

Send the ID as `X-Socket-ID` on Laravel requests that broadcast with `toOthers()`. Once the ID is available, use either of these forms.

```js
fetch('/orders/42/ship', { method: 'POST', headers: { 'X-Socket-ID': murmur.socketId() ?? '' } })
axios.post('/orders/42/ship', {}, { headers: { 'X-Socket-ID': murmur.socketId() ?? '' } })
```

## Options

Pass these options to `new Murmur(options)`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url` | `string` | Required | SSE endpoint for the shared channel connection. |
| `authEndpoint` | `string` | `'/broadcasting/auth'` | Endpoint used by the built-in authorization request. |
| `auth` | `AuthOptions` | `undefined` | Headers and credentials for the built-in authorization request. |
| `authorizer` | `Authorizer` | `undefined` | Function that issues one or more tokens for private topics in place of the built-in request. |
| `namespace` | `string` | `'App.Events'` | Dotted namespace for unqualified channel event names. |
| `retryMs` | `number` | `3000` | Delay before the first retry, in milliseconds. Must be finite and greater than zero. |
| `maxRetryMs` | `number` | `30000`, or `retryMs` when that is larger | Cap on the base retry delay before jitter, in milliseconds. Must be finite and at least `retryMs`. |
| `retryJitter` | `number` | `0.25` | Maximum fraction of the base delay added as random jitter from the second consecutive failure onward. Must be finite and between 0 and 1, inclusive. Set to `0` to disable jitter. |
| `tokenRefreshMarginSeconds` | `number` | `30` | Remaining lifetime a cached token must exceed to be reused, in seconds. Must be finite and non-negative. |
| `streamMaxFailures` | `number` | `3` | Number of consecutive failures that expires an independent stream. Must be an integer of at least 1. |
| `fetch` | `typeof fetch` | `globalThis.fetch` | Fetch implementation for the built-in authorization request. |
| `EventSource` | `typeof EventSource` | `globalThis.EventSource` | Constructor used for the shared connection and independent streams. |
| `withCredentials` | `boolean` | `false` | Set to `true` to pass `{ withCredentials: true }` to `EventSource` for the shared connection and streams. |

## Authorization

For each private channel that needs a token, the client sends a JSON `POST` to `authEndpoint` with a body such as `{ "channel_name": "private-orders.42" }`. Once the shared connection ID is known, the body also includes `socket_id`. Up to four requests run concurrently per connection attempt. A change that replaces a pending attempt or closes the client aborts its outstanding requests. By default the client uses `credentials: 'include'`, the `Accept: application/json` and `Content-Type: application/json` headers, and the URL-decoded `XSRF-TOKEN` cookie as `X-XSRF-TOKEN` when the cookie is readable. For Laravel session authentication, make the session and CSRF cookies available and configure CORS if the endpoint is cross-origin.

`auth.headers` accepts a `Record<string, string>` or a function that returns one synchronously or asynchronously. The client calls the function for each token request and merges the headers over the defaults case-insensitively. A supplied `X-XSRF-TOKEN` takes precedence over the cookie. `auth.credentials` is a Fetch `RequestCredentials` value and defaults to `'include'`. Use `withCredentials` to configure credentials on the SSE connections.

For an API that uses bearer tokens, read the current access token when authorization runs.

```js
const murmur = new Murmur({
  url: 'https://events.example.com/sse',
  authEndpoint: 'https://api.example.com/realtime/token',
  auth: {
    credentials: 'omit',
    headers: () => {
      const token = sessionStorage.getItem('access_token')
      if (!token) {
        throw new Error('Sign in to authorize private channels')
      }
      return { Authorization: `Bearer ${token}` }
    },
  },
})
```

A successful request returns `{ token, expires_at }`, with expiry in Unix seconds. The client caches credentials by private channel while their remaining lifetime exceeds `tokenRefreshMarginSeconds`. It releases a channel's cached credentials on leave and drops every token sent on a connection that fails before opening.

A built-in 4xx response leaves only that channel out of the shared connection. Call `murmur.reauthorize('orders.42')` to retry a denied channel, call `murmur.reauthorize()` to retry all denied channels, or leave and rejoin the channel. Network errors, 5xx responses, and invalid token responses fail the attempt and retry with backoff.

To control how tokens are requested, pass an `authorizer(channels, signal)` function. Whenever an eligible private topic needs credentials, the function receives all eligible private wire topic names, including those with cached credentials. A rejection with a numeric 4xx `status` denies only the topics without valid cached credentials. Other rejections retry with backoff. The custom authorizer takes over requests from `authEndpoint`, `auth`, and the client's `fetch` option.

Resolve with one token result or an array of results. Use `{ token, expires_at, channels }` to declare the full wire topic names covered by each token. When `channels` is omitted, the client treats the token as covering the whole batch. Multiple tokens with omitted coverage can together authorize the batch and are cached as a group. To grant only some topics, resolve with tokens whose `channels` list only the topics they cover. Every uncovered topic receives a 403 denial and is left out of the SSE request. An empty result array denies the entire batch.

```js
const murmur = new Murmur({
  url: 'https://events.example.com/sse',
  authorizer: async (channels, signal) => {
    const response = await fetch('/api/realtime/authorize', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topics: channels }),
      signal,
    })
    if (!response.ok) {
      throw Object.assign(new Error('Authorization failed'), { status: response.status })
    }
    return response.json()
  },
})
```

The server defaults to 50 topics per connection and has an 8 KiB limit for the request line and headers. With one default JWT per topic, the server accepted about 46 four-digit order channels or 32 UUID order channels with lean headers. With a 1 KiB cookie, those counts were about 40 and 28. The client enforces a 6 KiB request-target budget to reserve room for headers, so its effective counts can be lower. If the URL exceeds that budget, the client emits `error` and waits for a subscription change. For pages with many private channels, configure a custom `authorizer` that returns one multi-topic token. [Protocol request budget](../docs/protocol.md#request-head-budget)

## Streams

Pass the subscription URL returned by Laravel's `$stream->url()` to `murmur.stream(url)`. Each stream opens its own connection.

```js
const stream = murmur.stream(subscriptionUrl)
  .on('token', data => console.log(data))
  .on('end', () => console.log('Stream finished'))
  .on('gap', () => console.log('Some events are missing'))
  .on('expired', () => console.log('Stream stopped after repeated failures'))
```

Stream event names are literal, and callbacks receive the event payload. The names `end`, `gap`, and `expired` are reserved for the control callbacks, so a server event with one of those names cannot be consumed through a stream. The `end`, `gap`, and `expired` callbacks take no arguments.

- `end` fires when the server sends `murmur.end`. The stream closes permanently.
- `gap` fires when the server sends `murmur.gap` to signal missing events.
- `expired` fires and the stream closes when it reaches its consecutive failure limit. A successful connection, an attach marker, a gap, or an application event on a listened-to name resets the count. A private stream URL carries a fixed token, so retries keep using that token.

Pass `StreamOptions` as the optional second argument to `murmur.stream(url, options)`. Each field is an optional number and follows the validation rules of its client default.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `retryMs` | `number` | Client's `retryMs` | Delay before the first retry in milliseconds. |
| `maxRetryMs` | `number` | Client's `maxRetryMs`, or this stream's `retryMs` when that is larger | Cap on the base retry delay before jitter, in milliseconds. Must be at least this stream's `retryMs`. |
| `retryJitter` | `number` | Client's `retryJitter` | Maximum jitter fraction from the second consecutive failure onward. |
| `maxFailures` | `number` | Client's `streamMaxFailures` | Consecutive failure limit for this stream. |

The first connection uses the URL as supplied. On reconnect, the client updates only `lastEventId` from the latest numbered attach marker or listened-to event. Until it receives an ID, it reconnects with the original URL and its initial cursor.

Call `stream.close()` to stop the stream and cancel any pending retry.

## Statistics

`murmur.stats()` returns a fresh `MurmurStats` snapshot. Cumulative counters survive reconnects. Closing the client clears the live state and preserves the final counters, connection ID, replay cursor, and event timestamp.

| Field | Type | Meaning |
| --- | --- | --- |
| `connected` | `boolean` | Whether the shared connection is open. |
| `connections` | `number` | How many times the shared connection opened. |
| `disconnections` | `number` | Shared connection failures that enter backoff and unexpected drops. |
| `events` | `number` | Valid data envelopes received on a listened-to shared event name and passed to channel dispatch, once per received event. |
| `gaps` | `number` | `murmur.gap` signals received on the shared connection. |
| `tokenRequests` | `number` | Authorization attempts, including custom authorizer calls and attempts cancelled before completing. Reusing a cached token skips the request. |
| `tokenFailures` | `number` | Authorization attempts that failed before cancellation, including invalid token responses. |
| `channels` | `number` | Currently joined channels. |
| `streams` | `number` | Independent streams that have started and have yet to close, including streams waiting to retry. |
| `lastEventId` | `string \| undefined` | Shared replay cursor. It tracks IDs from attach markers and listened-to data events. |
| `connectionId` | `string \| undefined` | The most recently announced ID of the shared connection. |
| `lastEventAt` | `number \| undefined` | `Date.now()` when the last valid shared data event was received. |

```js
murmur.on('disconnected', () => {
  const { disconnections, lastEventAt } = murmur.stats()
  console.log({ disconnections, lastEventAt })
})
```

## Laravel Echo

Install Murmur and Laravel Echo 2.6.3 or a later 2.x release.

```sh
npm install @arttecs/murmur laravel-echo@^2.6.3
```

Pass `MurmurConnector` as the Echo broadcaster.

```js
import Echo from 'laravel-echo'
import { MurmurConnector } from '@arttecs/murmur/echo'

const echo = new Echo({
  broadcaster: MurmurConnector,
  url: 'https://events.example.com/sse',
  authEndpoint: '/broadcasting/auth',
})

echo.private('orders.1')
  .listen('OrderShipped', event => console.log(event))
```

| Echo option | Murmur behavior |
| --- | --- |
| `broadcaster` | Set to `MurmurConnector` to use SSE. |
| `url` | Required SSE endpoint. |
| `authEndpoint` | Private token endpoint. Defaults to `/broadcasting/auth`. |
| `auth.headers`, `auth.credentials` | Headers and Fetch credential mode for the token request. |
| `csrfToken` | CSRF token to use when `window.Laravel.csrfToken` is empty or absent. |
| `bearerToken` | A non-empty token sets `Authorization: Bearer <token>`, replacing any supplied authorization header. |
| `authorizer` | Replaces the built-in token request. Accepts private wire topic names and an abort signal. |
| `withCredentials` | Sends browser credentials with SSE requests. |
| `namespace` | Defaults to `App.Events`. Set to `false` to omit the prefix. |
| `retryMs`, `maxRetryMs`, `retryJitter` | Control the retry delay with the same defaults as the standalone client. |
| `tokenRefreshMarginSeconds` | Controls when a cached private token needs refreshing. |
| `streamMaxFailures` | Sets the failure limit for independent streams opened on the underlying client. |
| `fetch`, `EventSource` | Override the Fetch implementation for token requests and the EventSource constructor for SSE. |

The built-in token request uses Murmur's JSON and CSRF cookie conventions. Echo also resolves `X-CSRF-TOKEN` from the first non-empty value among `window.Laravel.csrfToken`, the `csrfToken` option, and the `content` attribute of `<meta name="csrf-token">`. That value overrides a supplied `X-CSRF-TOKEN` header. A custom `authorizer` handles its own headers. Echo 2.6.3's TypeScript declarations specify static authorization headers. To pass a header provider or set `auth.credentials`, build a `MurmurConnectorOptions` value and cast the Echo options. [Authorization options](#authorization)

```ts
import Echo, { type EchoOptions } from 'laravel-echo'
import { MurmurConnector, type MurmurConnectorOptions } from '@arttecs/murmur/echo'

const options: MurmurConnectorOptions = {
  url: 'https://events.example.com/sse',
  auth: { credentials: 'omit', headers: () => ({ Authorization: `Bearer ${currentToken()}` }) },
}

const echo = new Echo<'function'>({ broadcaster: MurmurConnector, ...options } as EchoOptions<'function'>)
```

Laravel Echo 2.6.3's axios, Vue, jQuery, and Turbo interceptors send the announced ID as `X-Socket-ID` once it is available. Before the first announcement, `echo.socketId()` returns an empty string. The first three interceptors skip the header in that case, while Turbo sends an empty header value.

Public channels, private channels, and Laravel broadcast notifications share one SSE connection. Event names follow Echo's formatter. For example, `Sub.OrderShipped` resolves to `App\Events\Sub\OrderShipped`, while `.order.shipped` listens for the literal `order.shipped`. An initial backslash also marks a literal event name.

- `stopListening(event, callback)` removes one callback. Omit the callback to remove all listeners for the event. `stopListeningForNotification(callback)` removes one notification callback.
- Register `subscribed(callback)` before attachment to receive the initial notification. It fires on the first `murmur.attached` that includes the channel and on successful attachment after a connection failure. Joining or leaving another channel preserves the existing channels' subscription state. The marker is the closest equivalent to Pusher's per-channel `subscription_succeeded` event because it confirms server-side readiness after replay.
- On a private channel, `error(callback)` receives that channel's numeric authorization denial status. The other authorized and public channels continue to receive events. Retry a denied channel through `echo.connector.murmur.reauthorize(name)` or by leaving and rejoining it.
- `leave('orders.1')` leaves both public and private variants. `leaveChannel('private-orders.1')` leaves just the private one. The `private-` prefix identifies an authorized topic.
- `connectionStatus()` reports `disconnected`, `connecting`, `connected`, or `reconnecting`. `onConnectionChange(callback)` on the connector returns an unsubscribe function.
- `echo.connector.murmur` exposes the underlying client for statistics, streams, and replay gaps. In TypeScript, read `echo.connector` into an `unknown` variable and narrow it with `instanceof MurmurConnector` to access the typed client.
- `echo.disconnect()` closes the shared connection, cancels retries, and closes independent streams. Call `echo.connect()` to create a fresh connector, then rejoin the channels you need.

Murmur's Echo connector has these limitations.

- Presence and encrypted private channels throw an unsupported-channel error.
- Client events are unavailable. `whisper`, `listenForWhisper`, and `stopListeningForWhisper` are chainable no-ops; the first `whisper` or `listenForWhisper` call on a connector prints one console warning.
- Wildcard event listeners are unavailable. Register named events with `listen`.

`@laravel/echo-react` and `@laravel/echo-vue` 2.6.3 were checked once by hand with the built connector in jsdom using a fake EventSource; that check is not part of this repository's test suite. In both packages, mounted consumers received public events, private events, and notifications. The connection-status hook reached `connected`, and unmounting released the channels and closed the connection.

## Development

- The build and test tooling requires Node.js `^22.18.0 || ^24.11.0 || >=26.0.0`.
- `npm test` runs the unit tests.
- `npm run test:integration` runs the client against the real Murmur server. It requires PHP on `PATH` and `composer install` at the repository root.
- `npm run build` builds the ESM package and its TypeScript declarations.
