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

Joining or leaving a channel reconnects with the full topic list and the shared replay cursor. When you join a channel on a live client, Murmur can replay earlier events on that channel from the shared cursor onward. Join the channels a page needs before it starts relying on events, or ignore events older than the page's current state.

The default namespace is `App.Events`. The client resolves `OrderShipped` to `App\Events\OrderShipped`, converting namespace dots to backslashes. Set `namespace` to use a different namespace. Prefix an event name with a dot to use the literal name. For example, `.updated` listens for `updated`.

Laravel Echo converts dots in relative class names such as `Sub.Event` to namespace separators. Murmur applies the configured namespace prefix and preserves those dots. To listen for a class in the `Sub` sub-namespace, pass `'Sub\\Event'` in JavaScript.

Use `murmur.on(event, callback)` to listen for these client events. It returns a function that removes the listener.

- `connected` fires when the channel connection opens.
- `disconnected` fires when a connection attempt fails or an open connection reports an error.
- `unauthorized` fires with the numeric status when the built-in authorization request gets a 4xx response or the custom authorizer rejects with an object whose numeric `status` is at least 400 and below 500. The client keeps retrying.
- `gap` fires when the server reports a gap in replay. Refresh any state that depends on the missing events.

The first retry uses `retryMs`. For consecutive failures after the first, the base delay doubles up to `maxRetryMs`, then random jitter of up to `retryJitter` times the base delay is added. A successful connection resets the retry sequence.

`news.stopListening('.updated')` removes the callbacks for that event. `murmur.leave('orders.42')` leaves channels joined under that name. `murmur.close()` closes the channel connection and all streams created by the client, and cancels their retries.

## Options

Pass these options to `new Murmur(options)`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url` | `string` | Required | SSE endpoint for the shared channel connection. |
| `authEndpoint` | `string` | `'/murmur/auth'` | Endpoint used by the built-in authorization request. |
| `auth` | `AuthOptions` | `undefined` | Headers and credentials for the built-in authorization request. |
| `authorizer` | `Authorizer` | `undefined` | Function that issues a token for private topics in place of the built-in request. |
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

For private channels, the built-in authorizer sends a JSON `POST` to `authEndpoint` with the wire topic names in `{ channels }`. By default it uses `credentials: 'include'`, the `Accept: application/json` and `Content-Type: application/json` headers, and the URL-decoded `XSRF-TOKEN` cookie as `X-XSRF-TOKEN` when the cookie is readable. For Laravel session authentication, make the session and CSRF cookies available and configure CORS if the endpoint is cross-origin.

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

To control how tokens are requested, pass an `authorizer` function. It receives the private wire topic names and an abort signal. It must resolve to `{ token: string, expires_at: number }`, with a finite expiration timestamp in Unix seconds. The function handles authorization in place of `authEndpoint`, `auth`, and the client's `fetch` option. Rejections enter the retry sequence.

An error thrown by `auth.headers` or a rejection from `authorizer` is not reported to the application: the attempt counts as a failure, `disconnected` fires and the client retries with backoff. Only a rejection carrying a numeric `status` from 400 to 499 also fires `unauthorized`. With `credentials: 'omit'` the `XSRF-TOKEN` cookie is not read, so no `X-XSRF-TOKEN` header is sent unless you supply one.

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

Both authorization paths use the same response format and token cache. The client reuses its cached token for the same set of private topics while the remaining lifetime exceeds `tokenRefreshMarginSeconds`. If the token was used on a connection that fails before opening, the client discards it.

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
- `expired` fires and the stream closes when it reaches its consecutive failure limit. A successful connection or a received event on a name the stream listens to resets the count. A private stream URL carries a fixed token, so retries keep using that token.

Pass `StreamOptions` as the optional second argument to `murmur.stream(url, options)`. Each field is an optional number and follows the validation rules of its client default.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `retryMs` | `number` | Client's `retryMs` | Delay before the first retry in milliseconds. |
| `maxRetryMs` | `number` | Client's `maxRetryMs`, or this stream's `retryMs` when that is larger | Cap on the base retry delay before jitter, in milliseconds. Must be at least this stream's `retryMs`. |
| `retryJitter` | `number` | Client's `retryJitter` | Maximum jitter fraction from the second consecutive failure onward. |
| `maxFailures` | `number` | Client's `streamMaxFailures` | Consecutive failure limit for this stream. |

The first connection uses the URL as supplied. On reconnect, the client updates only `lastEventId` to the last ID it received. Until it has received an ID, it reconnects with the URL unchanged, so the initial cursor still applies. For both channels and streams, the reconnect cursor only advances on event names the client listens to.

Call `stream.close()` to stop the stream and cancel any pending retry.

## Statistics

`murmur.stats()` returns a fresh `MurmurStats` snapshot. Cumulative counters survive reconnects. Closing the client clears the live state and preserves the final counters, replay cursor, and event timestamp.

| Field | Type | Meaning |
| --- | --- | --- |
| `connected` | `boolean` | Whether the shared connection is open. |
| `connections` | `number` | How many times the shared connection opened. |
| `disconnections` | `number` | Shared connection failures and unexpected drops, including authorization failures. Deliberate channel reconnects and `close()` are excluded. |
| `events` | `number` | Valid data envelopes received on a listened-to shared event name and passed to channel dispatch, once per received event. |
| `gaps` | `number` | `murmur.gap` signals received on the shared connection. |
| `tokenRequests` | `number` | Authorization attempts, including custom authorizer calls and attempts cancelled before completing. Reusing a cached token skips the request. |
| `tokenFailures` | `number` | Authorization attempts that failed before cancellation, including invalid token responses. |
| `channels` | `number` | Currently joined channels. |
| `streams` | `number` | Independent streams that have started and have yet to close, including streams waiting to retry. |
| `lastEventId` | `string \| undefined` | Shared replay cursor. It tracks the most recent non-empty ID received on a listened-to data event name. |
| `lastEventAt` | `number \| undefined` | `Date.now()` when the last valid shared data event was received. |

```js
murmur.on('disconnected', () => {
  const { disconnections, lastEventAt } = murmur.stats()
  console.log({ disconnections, lastEventAt })
})
```

## Development

- The build and test tooling requires Node.js `^22.18.0 || ^24.11.0 || >=26.0.0`.
- `npm test` runs the unit tests.
- `npm run test:integration` runs the client against the real Murmur server. It requires PHP on `PATH` and `composer install` at the repository root.
- `npm run build` builds the ESM package and its TypeScript declarations.
