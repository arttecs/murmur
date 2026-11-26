# Murmur wire protocol 1

Murmur serves HTTP and Server-Sent Events (SSE). Its SSE server handles the routes below. The authorization route runs in the Laravel application and can have a different base URL. The SSE server includes `X-Murmur-Protocol: 1` in the responses it generates for `GET /sse`, `GET /health`, and `GET /ready`, including route-level errors. The PHP constant is `Server::PROTOCOL`. The header is meant for non-browser clients and probes: the server does not list it in `Access-Control-Expose-Headers`, so a cross-origin page cannot read it. The request line and headers of any request must fit in 8 KiB, which bounds the combined length of the `topics`, `token`, and `lastEventId` parameters; a larger request is answered with 431.

| Method and path | Purpose |
| --- | --- |
| `GET /sse` | Subscribe to topics, optionally replaying retained events. |
| `POST /publish` | Publish to a server with HTTP publishing enabled and an in-memory event log. |
| `GET /health` | Check that the HTTP server is alive. |
| `GET /ready` | Check that the server is ready to deliver live events. |
| `POST /murmur/auth` | Exchange private channel access for a subscription token through Laravel. The path is configurable. |

Unrecognized method and path combinations on the SSE server return 404. It serves one request per TCP connection. The default cap is 1000 accepted sockets, including open SSE streams. Additional sockets are closed without a response. Accepted sockets have 10 seconds by default to complete a request before the server closes them. This deadline does not limit the lifetime of a successful SSE response. Invalid or oversized HTTP headers can be rejected by the HTTP parser before a Murmur route runs.

## Subscribe

Open a GET request to `/sse` with these URL query parameters.

| Parameter | Required | Value |
| --- | --- | --- |
| `topics` | Yes | A comma-separated list of topic names, URL-encoded as a single query parameter. |
| `token` | For private topics | An HS256 JWT granting access to every requested private topic. |
| `lastEventId` | No | A fallback replay cursor when `Last-Event-ID` is absent or malformed. |

A topic is 1–200 bytes and cannot contain a comma or ASCII whitespace. Topic names are case-sensitive. A request may contain at most `server.max_topics_per_connection` entries, 50 by default. Commas delimit entries after URL decoding, and empty entries are invalid.

| Status | Meaning |
| --- | --- |
| 200 | The subscription succeeded. The response remains open for SSE frames. |
| 400 | The topic list is missing, malformed, or over its entry limit. |
| 401 | The request includes a private topic and has no valid token. |
| 403 | The `Origin` header is disallowed, or a valid token fails to grant at least one requested private topic. |
| 503 | The server has reached its SSE connection limit (900 by default) or is shutting down. |

A successful response includes `Content-Type: text/event-stream`, `Cache-Control: no-cache`, and `X-Accel-Buffering: no`.

### Frames

Each frame uses LF line endings and ends in a blank line. An event has one `data:` field containing a JSON object with the wire `topic` and the original JSON value in `data`. An omitted publish payload becomes JSON `null`. The event name is 1–200 bytes without CR or LF. The built-in logs assign every stored event an ID.

```text
id: 1710000000000-2
event: updated
data: {"topic":"news","data":{"title":"New release"}}

```

The server first sends `retry: 3000\n\n`, using `server.retry_ms` in milliseconds. This is an SSE reconnection hint. It sends `: ping\n\n` comment frames periodically, every 20 seconds by default. These frames carry no event ID and do not advance the replay cursor.

IDs use `<milliseconds>-<sequence>`. A received cursor must contain 1–18 decimal digits per component. Compare the two components numerically, milliseconds first. IDs increase strictly within one event log across all topics. Redis nodes serving the same stream share its IDs and order. In-memory logs have their own order and lose their history on restart.

### Replay

For a reconnect, send the latest ID received as the `Last-Event-ID` HTTP header or the `lastEventId` query parameter. When both are valid, the header wins. Invalid cursors are ignored. Without a valid cursor, the connection starts with live delivery.

Replay sends retained matching events with IDs strictly greater than the cursor, in log order. Live events arriving during replay are held until replay finishes. The server drops duplicate or older IDs once it has written an event with a newer ID. When a cursor is ahead of the sampled log tail, the server uses that tail as the starting point for deduplicating live events. The connection closes if the replay query fails or the held live data exceeds `server.max_buffer_bytes`. The server also closes a stream whose output exceeds that limit under backpressure. The default byte limit is 1,048,576.

The server can send an unnumbered gap frame before resuming delivery.

```text
event: murmur.gap
data: {}

```

- With an in-memory log, a cursor older than log creation or the last evicted event signals a gap. With Redis, a cursor older than the first retained entry signals a gap if Redis reports trimmed history. An unparseable first retained Redis ID also signals a gap.
- A trim gap may be followed by matching retained events. A gap indicates that the log cannot guarantee complete history. Clients that need a complete view should refresh it from the application.
- If replay would exceed `server.max_replay_events` matching events (1000 by default) or the encoded replay frames would exceed half of `server.max_buffer_bytes`, the server sends the gap and discards the replay batch. It continues with held and live events.
- If the Redis reader loses its connection and discovers history was trimmed past its cursor, the server sends a gap to existing subscribers before resuming live delivery.

The `murmur.` event namespace is reserved. `murmur.end` is published by Laravel's `Stream::close()` with a regular event ID and an envelope containing the stream topic and `"data":null`. A stream client can stop consuming that stream upon receiving the marker. The SSE server delivers the marker as an event and leaves the HTTP connection open. The Laravel broadcast and direct publish APIs reject application event names starting with `murmur.`. The server enforces the reservation on both publishing paths: `POST /publish` answers 422 for any `murmur.` name other than `murmur.end`, and the Redis reader skips such stream entries, in live delivery and in replay.

### Private topics

Topic names beginning with `private-` require a token. Sign an HS256 JWT using the same shared secret as the server, at least 32 bytes long. Use these claims.

| Claim | Value |
| --- | --- |
| `topics` | JSON array of strings containing full wire topic names, including `private-`. Each private topic must match an entry exactly. |
| `exp` | Integer Unix expiration time in seconds. Laravel issues tokens for `token_ttl` seconds, 900 by default. |

The server verifies the JWT signature and expiration with no clock leeway. It checks both claims' types. Laravel issues tokens with these two claims only. If another issuer adds `nbf` or `iat`, the server also rejects a token whose `nbf`, or whose `iat` when `nbf` is absent, is later than its own clock, again with no leeway; omit both claims or keep the clocks synchronized. A request containing only public topics may omit the token, and an invalid token is ignored for that request. Every subscription using a valid token closes when the token expires. Obtain a fresh token before reconnecting to private topics.

### Laravel authorization

The default token endpoint is `POST /murmur/auth`. Send an authenticated request with `Content-Type: application/json` and a JSON object such as `{"channels":["private-orders.42"]}`. Provide any credentials and CSRF token required by the Laravel middleware. The package uses the `web` middleware by default. The route path and middleware can be configured, and the route can be disabled.

Laravel expects a nonempty list of valid private topic names with the same entry limit as `/sse` and a combined size of at most 2048 bytes. For each name, Laravel strips the `private-` prefix to check the channel callback and requires an authenticated user using its configured guard. The route returns 200 with `{"token":"<JWT>","expires_at":1710000900}` when all channels are authorized. It returns 422 with `message` and `errors` fields for validation errors and 403 if a channel is denied or lacks an authenticated user. Application middleware may reject the request before it reaches the route.

### CORS

The default `server.allowed_origins` list is empty. The subscription handler serves requests without an `Origin` header and omits `Access-Control-Allow-Origin`. A request with an `Origin` header receives 403 unless its origin is allowed. Configure complete origins (`scheme://host[:port]`) without paths, trailing slashes, queries, or fragments. Matching is case-insensitive for the scheme and host and exact for the port. The handler echoes an allowed request's `Origin` value. A list containing `*` allows every origin and returns `Access-Control-Allow-Origin: *`. The SSE server adds `Vary: Origin` to routed responses whenever the list excludes `*`.

Murmur's SSE server does not emit `Access-Control-Allow-Credentials` or implement OPTIONS preflights. A reverse proxy can provide those headers or handle OPTIONS for cross-origin clients that require them. CORS on the Laravel authorization route is controlled by the Laravel application.

## Publish

Send `POST /publish` to the SSE server for the HTTP driver. Use a JSON object with a valid `topic` string, a valid `event` name string, and optionally a `data` value of any JSON type. Send `Content-Type: application/json` for JSON. The handler validates the raw body as JSON regardless of its content type.

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
| 422 | A malformed body, including invalid JSON, topic, or event name, or a reserved `murmur.` event name other than `murmur.end`. |
| 404 | HTTP publishing is unavailable on this server, including with the Redis driver. |

A route failure can return 500. Publish error responses have empty bodies at the Murmur route level. The HTTP parser and intermediaries may produce their own responses.

### Publish through Redis

For the Redis driver, write to the same stream key and Redis database as the readers. The stream key is Laravel's configured Redis key prefix followed by `murmur.redis.stream` (default suffix `murmur:events`). The publisher runs this Redis command with each field value as a separate argument.

```text
XADD <stream-key> MAXLEN ~ <log-size> * topic <topic> name <event-name> data <json-value>
```

The default target length is 10000 entries, shared across topics. Redis assigns the ID. Use the same topic and event name rules as HTTP. The `data` field is the JSON encoding of the payload alone and is limited to `server.max_payload_bytes` bytes. Readers skip entries with missing or invalid fields, unparseable IDs, oversized data, or invalid JSON. Redis publishing uses Redis access controls.

## Probes and graceful shutdown

`GET /health` returns HTTP 200 with `{"status":"ok","connections":0}`, where `connections` is the number of attached SSE subscriptions. Use it for liveness.

`GET /ready` returns HTTP 200 with `{"status":"ready"}` when the server is listening, shutdown has not started, and its event log can deliver live events. It returns HTTP 503 with `{"status":"unavailable"}` while the live log is unavailable or shutdown is in progress. Both responses have `Cache-Control: no-store`. A Redis-backed server first binds its listener after establishing the starting cursor and remains live during later Redis outages.

On graceful shutdown the listener closes immediately. Requests reaching `/ready` or `/sse` on already accepted sockets during shutdown receive 503. Existing streams end at evenly spaced intervals across `server.shutdown_spread` seconds (2 by default). A lone stream ends immediately. Setting the spread to zero ends all streams together. Streams continue to receive live events until they end. Before a scheduled stream ends, the server sends another SSE `retry:` hint chosen uniformly from the configured interval through twice that interval, in milliseconds within PHP's integer range. Clients reconnect using their latest cursor. The server closes any sockets still open at the `server.shutdown_grace` deadline, 5 seconds by default.
