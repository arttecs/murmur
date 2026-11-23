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

Private channels get a token from `authEndpoint`, which defaults to `/murmur/auth`. The Laravel endpoint needs the session cookie and a CSRF token. The client sends the request with credentials and copies the URL-decoded `XSRF-TOKEN` cookie into the `X-XSRF-TOKEN` header when the cookie is readable by the page. Use a same-origin endpoint or configure CORS for credentialed requests and make both cookies available where needed.

Set `fetch` to supply the Fetch API implementation used for authorization requests. Set `EventSource` to supply the constructor used for channel and stream connections, such as a polyfill. Both options default to the corresponding globals.

Use `murmur.on(event, callback)` to listen for these client events. It returns a function that removes the listener.

- `connected` fires when the channel connection opens.
- `disconnected` fires when a connection attempt fails or an open connection reports an error.
- `unauthorized` fires with the numeric HTTP status when the authorization endpoint returns a 4xx response. The client keeps retrying so it can recover if authorization becomes available again.
- `gap` fires when the server reports a gap in replay. Refresh any state that depends on the missing events.

For both channels and streams, the first retry waits exactly `retryMs`, which defaults to 3000 milliseconds. Further consecutive failures double the base delay up to 30 seconds and add up to 25% jitter. A successful connection resets the retry sequence. Streams stop retrying after three consecutive failures.

`news.stopListening('.updated')` removes the callbacks for that event. `murmur.leave('orders.42')` leaves channels joined under that name. `murmur.close()` closes the channel connection and all streams created by the client, and cancels their retries.

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
- `expired` fires and the stream closes after three consecutive connection failures. A connection the server accepts, and any event the stream listens to, resets the count. A private stream URL carries a fixed token, so an expired token is refused three times in a row.

The first connection uses the URL as supplied. On reconnect, the client updates only `lastEventId` to the last ID it received. Until it has received an ID, it reconnects with the URL unchanged, so the initial cursor still applies. For both channels and streams, the reconnect cursor only advances on event names the client listens to.

Call `stream.close()` to stop the stream and cancel any pending retry.
