# Murmur wire protocol 1

Murmur serves HTTP and Server-Sent Events (SSE). The SSE server and Laravel application expose the routes below. The Laravel authorization endpoint can have a different base URL. The SSE server includes `X-Murmur-Protocol: 1` in the responses it generates for `GET /sse`, `GET /health`, and `GET /ready`, including route-level errors. The PHP constant is `Server::PROTOCOL`. The header is meant for non-browser clients and probes: the server does not list it in `Access-Control-Expose-Headers`, so a cross-origin page cannot read it. The request line and headers of any request must fit in 8 KiB, which bounds the combined length of the subscription query parameters; a larger request is answered with 431.

| Method and path | Purpose |
| --- | --- |
| `GET /sse` | Subscribe to topics, optionally replaying retained events. |
| `POST /publish` | Publish to a server with HTTP publishing enabled and an in-memory event log. |
| `GET /health` | Check that the HTTP server is alive. |
| `GET /ready` | Check that the server is ready to deliver live events. |
| `POST /broadcasting/auth` | Exchange access to one private channel for a subscription token through Laravel. |

Unrecognized method and path combinations on the SSE server return 404. It serves one request per TCP connection. The default cap is 1000 accepted sockets, including open SSE streams. Additional sockets are closed without a response. Accepted sockets have 10 seconds by default to complete a request before the server closes them. This deadline does not limit the lifetime of a successful SSE response. Invalid or oversized HTTP headers can be rejected by the HTTP parser before a Murmur route runs.

## Subscribe

Open a GET request to `/sse` with these URL query parameters.

| Parameter | Required | Value |
| --- | --- | --- |
| `topics` | Yes | A comma-separated list of topic names, URL-encoded as a single query parameter. |
| `token` | Depends on the authorizer | A comma-separated list of tokens that together grant access to the requested private topics. The default verifier requires tokens for private topics. |
| `fresh` | No | A comma-separated subset of `topics` that starts strictly after the sampled log tail. |
| `connection` | No | A previously announced connection ID to reuse across reconnects. |
| `lastEventId` | No | A fallback replay cursor when `Last-Event-ID` is absent or malformed. |

A topic is 1–200 bytes of valid UTF-8 and cannot contain a comma or any byte in `\x00–\x20` or `\x7F` (ASCII whitespace, C0 controls, and DEL). Topic names are case-sensitive. A request may contain at most `server.max_topics_per_connection` entries, 50 by default. Commas delimit entries after URL decoding, and empty entries are invalid.

| Status | Meaning |
| --- | --- |
| 200 | The server accepted the request and opened an SSE response. `murmur.attached` confirms that the initial tail sample and any replay have finished. |
| 400 | The topic list is missing, malformed, or over its entry limit, the connection ID is malformed, the token list is invalid, or `fresh` contains an invalid or unsubscribed topic. |
| 401 | The authorizer rejected a credential, for example because a required token is missing or a supplied token is malformed, expired, or has an invalid signature. |
| 403 | The `Origin` header is disallowed, or the granted topics fail to cover at least one requested private topic. |
| 431 | The request line and headers exceed the 8 KiB request-head limit. |
| 500 | The server could not complete the subscription, for example because the authorizer failed unexpectedly. |
| 503 | The server has reached its SSE connection limit (900 by default) or is shutting down. |

If authorization has not settled by the request deadline, the server closes the socket without a response. A successful response includes `Content-Type: text/event-stream`, `Cache-Control: no-cache`, and `X-Accel-Buffering: no`.

### Frames

Each frame uses LF line endings and ends in a blank line. A published event has one `data:` field containing a JSON object with the wire `topic` and the original JSON value in `data`. An omitted publish payload becomes JSON `null`. The event name is 1–200 bytes without CR or LF. The built-in logs assign every stored event an ID.

```text
id: 1710000000000-2
event: updated
data: {"topic":"news","data":{"title":"New release"}}

```

The server first sends `retry: 3000\n\n`, using `server.retry_ms` in milliseconds. This is an SSE reconnection hint. Immediately afterward, every connection receives an announcement before any replay or live delivery.

```text
event: murmur.connected
data: {"connection_id":"0123456789abcdef0123456789abcdef","protocol":1}

```

The announcement has no `id:` field and does not advance the replay cursor. A connection ID is exactly 32 lowercase hexadecimal characters. When the client omits `connection`, the server generates the ID from 128 random bits and announces it only to that client. A supplied well-formed ID is used as is and several streams may share an ID. Clients should reuse only server-issued IDs. Put the ID in the client's own stream URL as `connection` when reconnecting and send it as `X-Socket-ID` on Laravel requests that can trigger a `toOthers()` broadcast.

The server accepts any well-formed connection ID without authenticating it. An authenticated user who can call an application endpoint that broadcasts with `toOthers()` can pass another connection's ID in `X-Socket-ID` and hide that one event from the matching connection, live and in replay. Proxy and access logs that capture the stream URL or the Laravel request header can expose the ID. This is the same class of risk as a Pusher or Reverb socket ID. [Nginx logging configuration](../README.md#nginx).

The Laravel lifecycle events describe hub membership. On a reconnect with the same ID the new stream can join the hub before the server detects that the old stream has closed. See [lifecycle events](../README.md#lifecycle-events) for how to handle overlapping streams and missing notifications.

The attach marker and numbered heartbeats reveal the newest sampled or dispatched identifier across all topics to every subscriber. This exposes the timing and rough volume of publishing even on topics the subscriber cannot read.

The server sends a heartbeat every 20 seconds by default. For a live subscription, it can carry the ID of the newest event the server has dispatched across all topics.

```text
id: 1710000000000-2
event: murmur.heartbeat
data: {}

```

Treat `murmur.heartbeat` as a control frame. A numbered heartbeat unconditionally replaces the stored replay cursor, like a numbered `murmur.attached`. Process it before ID de-duplication and keep it out of application event callbacks. An idle subscription can receive the same heartbeat ID more than once. The server uses the latest dispatched ID even when the Redis reader is behind the stream tail. A numbered heartbeat is always at or after the newest numbered frame written on that connection. The server uses a `: ping\n\n` comment with no cursor update while the subscription is held before the marker, when it has yet to dispatch a numbered event, or when the newest ID written on the connection is ahead of the latest dispatched ID. A held subscription queues these comments until release. Heartbeat bytes count toward `murmur_bytes_sent_total`. The heartbeat itself adds nothing to `murmur_events_sent_total`.

IDs use `<milliseconds>-<sequence>`. A received cursor must contain 1–18 decimal digits per component. Compare the two components numerically, milliseconds first. IDs increase strictly within one event log across all topics. Redis nodes serving the same stream share its IDs and order. In-memory logs have their own order and lose their history on restart.

### Replay

For a reconnect, send the latest ID received as the `Last-Event-ID` HTTP header or the `lastEventId` query parameter. When both are valid, the header wins. Invalid cursors are ignored.

Without a valid cursor, the server registers the subscription for live delivery before sending `murmur.connected`. It can send events dispatched during the tail sample before `murmur.attached`. Delivery is guaranteed only for matching events published after the client has received `murmur.attached`, while the subscription stays active and subject to sender exclusion and any signalled loss of live history. On Redis, an event published after the server registers the subscription can still be dropped if its ID is at or below the sampled tail and the reader dispatches it after the marker. Wait for `murmur.attached` before fetching initial state or invoking subscribed callbacks.

Replay sends retained matching events with IDs strictly greater than the cursor and at or before the sampled tail, in log order. When `fresh` is present, every entry must also appear in `topics`. Fresh topics deliver only events strictly after the sampled tail. With a cursor, the other topics replay from it. Every connection samples the tail, including one without a cursor or with only fresh topics. Redis uses one tail read within the existing query deadline, with no history lookup when there are no topics to replay. A failed tail sample closes the stream through the same path as a failed replay. The initial `murmur.connected` may arrive while the sample is pending. When replay fails before the attach marker, lifecycle delivery can still report `ConnectionOpened` followed by `ConnectionClosed` with reason `error`.

The stream starts with `retry:`, then `murmur.connected`. Before the single attach marker, a subscription with a cursor can receive an initial `murmur.gap` and replayed events. A subscription without a cursor receives held numbered events at or before the sampled tail on its non-fresh topics before the marker, in log order. Remaining held frames and live delivery follow the marker.

```text
id: 1710000000000-2
event: murmur.attached
data: {}

```

The marker's ID is the sampled tail or the highest ID already written on this connection, whichever is greater. This also applies after a gap and when the requested cursor was ahead of the tail. Treat it as a control frame and process it before applying any ID de-duplication. A numbered marker resets the client's newest ID even if it equals the last replayed ID or is lower than the cursor sent on the request. The server withholds subsequent numbered events at or before the marker's ID, including entries arriving late from the Redis reader. The marker's bytes count toward `murmur_bytes_sent_total`, and the event and replay counters count only published events.

The marker omits `id:` when the server has neither position, for example on an empty log. An unnumbered marker acknowledges attachment without changing the client's cursor. A client that connects without a cursor on an empty log has no resume point until it receives a numbered frame.

Include a newly added topic in `fresh` on the attempt that adds it. Once `murmur.attached` arrives on that connection, omit that topic from `fresh` on reconnect and send the latest cursor, if available. When a cursor is available, ordinary replay then starts at the acknowledged join or later. If the stream drops before the marker arrives, send `fresh` again for the unacknowledged topic. Numbered events received before the marker do not acknowledge the join. The server evaluates `fresh` per request, so clients that use it must build a new URL per attempt instead of letting native `EventSource` retry the same URL.

Live events arriving during replay are held for release around the attach marker. The server drops duplicate or older event IDs once it has written a frame with the same or a newer ID. The connection closes before the attach marker if the tail sample or replay query fails, replay cannot be encoded, or the held live data exceeds `server.max_buffer_bytes`. If the stream ends before the marker, a client that rebuilds its subscription URL should wait for the `retry:` delay it received or use its own backoff. The server also closes a stream whose output exceeds the byte limit under backpressure. The default byte limit is 1,048,576.

The server can send an unnumbered gap frame before resuming delivery.

```text
event: murmur.gap
data: {}

```

- With an in-memory log, a cursor older than log creation or the last evicted event signals a gap. With Redis, a cursor older than the first retained entry signals a gap if Redis reports trimmed history. An unparseable first retained Redis ID also signals a gap.
- A supplied cursor newer than the sampled tail is not treated as lost history, because a cursor can legitimately lie ahead of the log: Laravel's stream URLs carry a time-based cursor, and event IDs are time-based. The server sends no `murmur.gap` for it; a numbered marker corrects the cursor to the tail. After a restart, while the in-memory log is empty, a request that asks to replay from a cursor below the new log's floor receives `murmur.gap` followed by an unnumbered attach marker, and reconnecting with that same cursor repeats the gap until a numbered marker or heartbeat supplies a replacement, which can come from an event on any topic. Two cases are not detected: a cursor issued by a Redis stream that was later deleted or recreated, and a cursor that is ahead of the log because clocks differ. A cursor kept from before an in-memory server restarted is detected, because it falls below the new log's floor.
- A trim gap may be followed by matching retained events. A tail-only sample does not check for trimmed history. A gap indicates that the log cannot guarantee complete history. The log may report a trim gap even if the trimmed entries belonged only to fresh topics. Clients that need a complete view should refresh it from the application.
- If replay would exceed `server.max_replay_events` events eligible for delivery (1000 by default) or the encoded replay frames for this recipient would exceed half of `server.max_buffer_bytes`, the server sends the gap and discards the replay batch. It sends the attach marker before continuing with held and live events.
- If the Redis reader discovers history was trimmed past its cursor, the server sends a gap to live subscribers before delivering retained events or advancing heartbeats past the loss. The reader reads at most 1,000 entries at a time and checks on recovery and before dispatching a batch that reaches the configured log size or 1,000 entries, whichever is smaller. This covers a reader that falls behind while still connected, **provided every publisher trims the stream with the same `log_size` this server is configured with**. Losses are not detected when a publisher trims to a smaller size, when the stream is trimmed by other means (`XTRIM`, `MINID`, `XDEL`), or when the key is deleted or recreated. A failed check causes the reader to retry from its previous cursor.

The `murmur.` event namespace is reserved. `murmur.end` is published by Laravel's `Stream::close()` with a regular event ID and an envelope containing the stream topic and `"data":null`. A stream client can stop consuming that stream upon receiving the marker. The SSE server delivers the marker as an event and leaves the HTTP connection open. The Laravel broadcast and direct publish APIs reject application event names starting with `murmur.`. The server enforces the reservation on both publishing paths: `POST /publish` answers 422 for any `murmur.` name other than `murmur.end`, and the Redis reader skips such stream entries, in live delivery and in replay.

### Private topics

Topic names beginning with `private-` require authorization. The default authorizer accepts an HS256 JWT signed with the same shared secret as the server, at least 32 bytes long. Each token has these claims.

| Claim | Value |
| --- | --- |
| `topics` | JSON array of valid full wire topic names, including the `private-` prefix where applicable. Each private topic must match an entry exactly. |
| `exp` | Positive integer Unix expiration time in seconds. Laravel issues tokens for `token_ttl` seconds, 900 by default. |

The server verifies the JWT signature and expiration with no clock leeway. It checks both claims' types. Laravel issues tokens with these two claims only. If another issuer adds `nbf` or `iat`, the server also rejects a token whose `nbf`, or whose `iat` when `nbf` is absent, is later than its own clock, again with no leeway; omit both claims or keep the clocks synchronized. With the default authorizer, a public-only request can omit `token`. When supplied, the comma-separated list must contain only nonempty items and at most `server.max_topics_per_connection` tokens. The server verifies every distinct token, including those unused by the subscription. One expired token fails the entire request with 401, even if the other tokens cover all the topics. Drop a channel's token when you leave that channel and refresh before the earliest expiry of any token you still send.

The server checks the union of the grants against the requested private topics. When grants overlap, each private topic stays authorized through the latest expiry of a token that covers it. The server closes the stream at the first of those per-topic expiries. Public topics do not impose an expiry. If the calculated expiry is more than ten years away or exceeds the platform's timer range, the server keeps the stream open without scheduling an expiry.

A deployment may replace token verification. In that case the token format and handling of an omitted token are deployment-defined. Tokens still cannot contain commas, which delimit list entries after URL decoding. The status codes and grant coverage and expiry rules are the same.

### Laravel authorization

Use `POST /broadcasting/auth` on the Laravel application. The application registers the route through Laravel's broadcasting setup. Send an authenticated JSON request with `Content-Type: application/json` and a body such as `{"channel_name":"private-orders.42"}`. The optional `socket_id` is accepted and ignored. The token grants access to a topic and the connection ID grants no access. Include the credentials required by the application's route middleware.

Laravel strips the `private-` prefix and checks the registered channel callback with its configured guards. An authorized request receives 200 with `{"token":"<JWT>","expires_at":1710000900}`. The token covers exactly the requested topic and `expires_at` is the Unix time in seconds from the token. Missing or invalid channel names, public and presence channels, unauthenticated requests, and denied channel callbacks receive 403. Application middleware may reject the request earlier.

For several private topics, make one authorization request per topic and send the resulting tokens together on the SSE URL. An application can also authorize several topics in its own endpoint and return one token minted with `Murmur::token(['private-orders.42', 'private-orders.43'])`.

### Request head budget

The topic and token lists each have a default cap of 50 entries, and the 8 KiB limit applies to the entire request line and headers. The byte limit often takes effect before the count limit when sending one JWT per channel. These measurements use one default HS256 token per channel, URL-encoded lists, a 32-character `connection` and an 18-character `lastEventId` in the URL, with no `fresh` parameter or `Last-Event-ID` header. Each count is the most channels the server accepted over HTTP/1.1 for that setup with the default count limit.

| Example wire topic | Topic bytes | JWT bytes | Lean headers | Chrome-style headers | Lean headers with a 1 KiB cookie value |
| --- | ---: | ---: | ---: | ---: | ---: |
| `private-a` | 9 | 136 | 50 | 49 | 46 |
| `private-orders.0042` | 19 | 149 | 45 | 43 | 40 |
| `private-orders.00000000-0000-4000-8000-000000000042` | 51 | 192 | 32 | 30 | 27 |
| 200-byte ASCII topic | 200 | 391 | 13 | 12 | 11 |

Lean requests used `Host: events.example.test`, `Accept: text/event-stream` and `Origin: https://app.example.test`. The Chrome-style requests modeled a cross-origin, same-site EventSource on macOS. They added `Connection`, `Cache-Control`, `User-Agent`, `Sec-CH-UA`, `Sec-CH-UA-Mobile`, `Sec-CH-UA-Platform`, `Sec-Fetch-Site`, `Sec-Fetch-Mode`, `Sec-Fetch-Dest`, `Referer`, `Accept-Encoding` and `Accept-Language`, for 580 bytes of header lines in total, with CRLF endings and no cookie.

Allow room for the browser's actual headers and for longer encoded topic names. A single multi-topic token reduces the request size by sharing the JWT header, signature, and expiry across the authorized topics.

### CORS

The default `server.allowed_origins` list is empty. The subscription handler serves requests without an `Origin` header and omits `Access-Control-Allow-Origin`. A request with an `Origin` header receives 403 unless its origin is allowed. Configure complete origins (`scheme://host[:port]`) without paths, trailing slashes, queries, or fragments. Write internationalised hosts in punycode. Matching is case-insensitive for the scheme and host, compares ports numerically, and accepts equivalent spellings of bracketed IPv6 literals. Explicit HTTP port 80 and HTTPS port 443 are equivalent to an omitted port. The handler echoes an allowed request's `Origin` value. A list containing `*` allows every origin and returns `Access-Control-Allow-Origin: *`. The SSE server adds `Vary: Origin` to routed responses whenever the list excludes `*`.

Murmur's SSE server does not emit `Access-Control-Allow-Credentials` or implement OPTIONS preflights. A reverse proxy can provide those headers or handle OPTIONS for cross-origin clients that require them. CORS on the Laravel authorization route is controlled by the Laravel application.

## Publish

Send `POST /publish` to the SSE server for the HTTP driver. Use a JSON object with a valid `topic` string, a valid `event` name string, and optionally a `data` value of any JSON type. Send `Content-Type: application/json` for JSON. The handler validates the raw body as JSON regardless of its content type. An optional `except` field must be a connection ID string of exactly 32 lowercase hexadecimal characters. Matching connections skip the event in live delivery and replay. The SSE envelope contains no exclusion field.

```json
{"topic":"news","event":"updated","data":{"title":"New release"}}
```

Set `X-Murmur-Timestamp` to the current Unix time in seconds as decimal digits. Set `X-Murmur-Signature` to the lowercase hexadecimal HMAC-SHA256 of the exact bytes below, using the shared secret as the key.

```text
<timestamp>.<raw request body>
```

Use the timestamp's canonical decimal form. The server normalizes it to an integer before signature verification. The dot is a literal ASCII period. Include all body bytes, including any whitespace or final newline, exactly as sent. The absolute difference between the timestamp and the server clock must be at most `server.signature_tolerance` seconds, 60 by default. A signature can be replayed in that window, so restrict publishing to a trusted network.

The raw body may be at most `server.max_payload_bytes` bytes, 65536 by default, including the JSON object and all its syntax. The limit applies to fixed-length and chunked requests.

| Status | Meaning |
| --- | --- |
| 202 | Accepted. The JSON response is `{"id":"<assigned ID>"}`. |
| 401 | Missing or invalid signing headers, timestamp outside the tolerance, or incorrect signature. |
| 413 | Body above the byte limit. |
| 422 | A malformed body, including invalid JSON, topic, event name, or excluded connection ID, or a reserved `murmur.` event name other than `murmur.end`. |
| 404 | HTTP publishing is unavailable on this server, including with the Redis driver. |

A route failure can return 500. Publish error responses have empty bodies at the Murmur route level. The HTTP parser and intermediaries may produce their own responses.

### Publish through Redis

For the Redis driver, write to the same stream key and Redis database as the readers. The stream key is Laravel's configured Redis key prefix followed by `murmur.redis.stream` (default suffix `murmur:events`). The publisher runs this Redis command with each field value as a separate argument.

```text
XADD <stream-key> MAXLEN ~ <log-size> * topic <topic> name <event-name> data <json-value> [except <connection-id>]
```

The default target length is 10000 entries, shared across topics. Redis assigns the ID. Use the same topic and event name rules as HTTP. The `data` field is the JSON encoding of the payload alone and is limited to `server.max_payload_bytes` bytes. Readers skip entries with missing or invalid fields, unparseable IDs, oversized data, or data that cannot be decoded and encoded again as JSON. The optional `except` field follows the HTTP publish rules. Redis publishing uses Redis access controls.

## Probes and graceful shutdown

`GET /health` returns HTTP 200 with `{"status":"ok","connections":0}`, where `connections` is the number of attached SSE subscriptions. Use it for liveness.

`GET /ready` returns HTTP 200 with `{"status":"ready"}` when the server is listening, shutdown has not started, and its event log can deliver live events. It returns HTTP 503 with `{"status":"unavailable"}` while the live log is unavailable or shutdown is in progress. Both responses have `Cache-Control: no-store`. A Redis-backed server first binds its listener after establishing the starting cursor and remains live during later Redis outages.

On graceful shutdown the listener closes immediately. Requests reaching `/ready` or `/sse` on already accepted sockets during shutdown receive 503. Existing streams end at evenly spaced intervals across `server.shutdown_spread` seconds (2 by default). A lone stream ends immediately. Setting the spread to zero ends all streams together. Streams continue to receive live events until they end. Before a scheduled stream ends, the server sends another SSE `retry:` hint chosen uniformly from the configured interval through twice that interval, in milliseconds within PHP's integer range. Clients reconnect using their latest cursor. The server closes any sockets still open at the `server.shutdown_grace` deadline, 5 seconds by default.
