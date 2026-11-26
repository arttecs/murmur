# Murmur

Murmur is a ReactPHP Server-Sent Events (SSE) server with a Laravel bridge. An SSE subscription can stay open for minutes or hours. When Laravel serves the stream directly through PHP-FPM, that connection holds a worker for its lifetime. Murmur keeps subscriptions in a separate, long-running process. Laravel publishes events over HTTP or Redis and continues serving requests with its workers available.

## Installation

Murmur requires PHP 8.3 or later and Laravel 12 or 13.

```sh
composer require arttecs/murmur
php artisan murmur:install
```

The installer publishes `config/murmur.php`, fills in a missing or blank `MURMUR_SECRET` in `.env`, and sets `BROADCAST_CONNECTION=murmur`. If another broadcaster is configured, the installer asks before changing it. In non-interactive mode it keeps the existing broadcaster unless you pass `--force`. That flag also allows replacing the published configuration. For private channels, the installer points you to Laravel's broadcasting setup if the auth route is missing.

Laravel and every Murmur server must share the same signing secret of at least 32 bytes. The installer generates a 64-character hexadecimal secret and stores it in `.env`. It adds blank placeholders for the secret and origin allowlist to an existing `.env.example` when needed. If `MURMUR_ALLOWED_ORIGINS` is missing or blank, the installer adds and prints the origin of a valid HTTP or HTTPS `APP_URL` from `.env`. For a loopback origin, it reminds you to add a development page served from another port, such as a Vite dev server.

Start the server from your Laravel application directory.

```sh
php artisan murmur:serve
```

The default address is `http://127.0.0.1:8080`. To override the bind address for a single invocation, use `--host` and `--port`. Update `MURMUR_PUBLIC_URL` when the public address changes. Update `MURMUR_PUBLISH_URL` when the address Laravel uses for HTTP publishing changes.

```sh
php artisan murmur:serve --host=127.0.0.1 --port=8081
```

A public subscription receives a reconnect interval, a connection ID, and an attach marker. The server sends a heartbeat every 20 seconds by default. A numbered `murmur.heartbeat` control frame keeps a live subscription's replay cursor current even when its topics are quiet. The server sends a comment heartbeat when it has no position that can safely advance the cursor.

```sh
curl -N 'http://127.0.0.1:8080/sse?topics=news'
```

## Laravel broadcasting

Murmur registers the `murmur` broadcast connection when the application has no entry for it. You can set an application-specific entry in `config/broadcasting.php`.

Make Murmur your application's default broadcast connection in `.env`. Laravel registers the callbacks from `routes/channels.php` on the default connection and uses that connection to authorize private channels.

```dotenv
BROADCAST_CONNECTION=murmur
```

Events implementing `ShouldBroadcast` publish to their Laravel channel names as SSE topics. A `Channel('news')` publishes to `news`, and a `PrivateChannel('orders.42')` publishes to `private-orders.42`. Run your Laravel queue worker if your broadcasts are queued.

Register Laravel's broadcasting routes with `php artisan install:broadcasting`, `withBroadcasting()`, or `Broadcast::routes()`. Define private channel callbacks with `Broadcast::channel()` using patterns such as `orders.{orderId}`.

Send an authenticated JSON request to `POST /broadcasting/auth` with `{"channel_name":"private-orders.42"}`. The app's middleware controls credentials. Laravel uses the channel's configured guard and returns `{"token":"<JWT>","expires_at":1710000900}` for an authorized channel or 403 for a denied, missing, invalid, public, or presence channel. Each token covers one channel. An optional `socket_id` is ignored because the token authorizes the channel and the connection ID grants no access.

Request a token for each private topic and send the tokens as one comma-separated, URL-encoded `token` query parameter on `/sse`. One expired token anywhere in the list fails the whole request with 401, even if other tokens grant all requested topics. Drop a channel's token when you leave it and refresh before the earliest expiry of the tokens you still send.

The 8 KiB request-head limit often takes effect before the default `server.max_topics_per_connection` limit of 50. With one JWT per channel and Chrome-style request headers, the server accepted 49 short 9-byte topics, 43 four-digit order topics, 30 UUID order topics, or 12 topics at the 200-byte limit. Longer headers and cookies reduce these counts. To fit more topics, authorize them in your own Laravel endpoint and return one token minted with `Murmur::token(['private-orders.42', 'private-orders.43'])`. See the [wire protocol measurements](docs/protocol.md#request-head-budget).

Use Laravel's `broadcast($event)->toOthers()` to skip the sending subscription. Send the ID from the `murmur.connected` event in the `X-Socket-ID` header on the Laravel request that triggers the broadcast. Pass the same ID as `connection` on reconnect to keep the exclusion in replay. For a direct publication, pass the ID as the fourth argument to `Murmur::publish()`.

A connection ID grants no access. An authenticated user who can call an application endpoint that broadcasts with `toOthers()` can send another connection's ID in `X-Socket-ID` and hide that one event from that connection in live delivery and replay. Murmur generates IDs from 128 random bits and sends each generated ID only to its owner in `murmur.connected`. The owner then sends it in its own stream URL and Laravel request headers. Proxy and access logs can expose the ID, the same class of risk as a Pusher or Reverb socket ID. Clients should reuse only server-issued IDs.

## Stream API

Use a stream to send named events to a single topic. `url()` returns the subscription URL with a replay cursor and, for a `private-` topic, a topic-scoped token. Check access in your application before passing a private stream URL to a user.

```php
use Arttecs\Murmur\Facades\Murmur;

$stream = Murmur::stream('private-chat.42');
$url = $stream->url();

$stream->emit('token', ['text' => 'Hello']);
$stream->close();
```

Pass `$url` to the subscriber. `emit()` publishes an event, and `close()` publishes a `murmur.end` event that the subscriber can use to close its connection. You can also publish directly with `Murmur::publish('news', 'updated', $data)`.

Use a distinct topic for each stream run. Reusing a topic can replay the previous run's end marker to the next subscriber. Event names starting with `murmur.` are reserved for the protocol.

## JavaScript client

Use `@arttecs/murmur` to subscribe to channels and consume streams in the browser. See the [JavaScript client README](js/README.md) for installation and usage.

The SSE server sends `retry:` and `murmur.connected` before any events. Replay and some live events on a cursorless connection can arrive before `murmur.attached`. The attach marker has `data: {}` and typically carries the sampled log tail as its ID. A numbered marker updates the cursor, even when it corrects a cursor ahead of the log. Wait for the marker before fetching initial state or invoking subscribed callbacks.

When adding topics, include them as a comma-separated `fresh` query value on the request that adds them. Fresh topics start after the sampled tail and existing topics replay from the cursor. Once you receive `murmur.attached` on that connection, reconnect with the latest cursor if available and omit the acknowledged topics from `fresh`. If the stream drops before the marker arrives, send the unacknowledged topics in `fresh` again. The server evaluates `fresh` per request, so a client that uses it must build its URL for each attempt and take over reconnection from native `EventSource`. See the [replay protocol](docs/protocol.md#replay).

## Migrating from Laravel Reverb

Keep your Laravel broadcast events, `routes/channels.php` authorization callbacks, and `Channel` and `PrivateChannel` classes. Set `BROADCAST_CONNECTION=murmur` and start `php artisan murmur:serve`. The app and server use one `MURMUR_SECRET` in place of the Reverb app ID, key, and secret.

Replace the Pusher-compatible browser transport with `@arttecs/murmur`. If your frontend uses Laravel Echo, use the [Murmur Echo connector](js/README.md#laravel-echo). Keep the same Laravel `/broadcasting/auth` route and guards you used with Reverb. Murmur returns a subscription token for the authorized channel.

| Setting | Reverb | Murmur |
| --- | --- | --- |
| Default broadcast connection | `BROADCAST_CONNECTION=reverb` | `BROADCAST_CONNECTION=murmur` |
| Server bind address and port | `REVERB_SERVER_HOST`, `REVERB_SERVER_PORT` | `server.host` and `server.port` via `MURMUR_HOST` and `MURMUR_PORT` |
| URL Laravel uses to publish | `REVERB_SCHEME`, `REVERB_HOST`, `REVERB_PORT` | `publish_url` via `MURMUR_PUBLISH_URL` in HTTP mode |
| Address the browser connects to | `VITE_REVERB_SCHEME`, `VITE_REVERB_HOST`, `VITE_REVERB_PORT` in the usual Echo setup | `public_url` via `MURMUR_PUBLIC_URL` for stream URLs. Use the same public base URL in the browser configuration. |
| Browser origin allowlist | App-level `allowed_origins` in `config/reverb.php` | `server.allowed_origins` via `MURMUR_ALLOWED_ORIGINS`. The default denies requests carrying an `Origin` header. Enter complete origins, including the scheme and any non-default port. |
| Open client connection cap | `REVERB_APP_MAX_CONNECTIONS` | `server.max_connections` via `MURMUR_MAX_CONNECTIONS` |
| Multi-node event delivery | `REVERB_SCALING_ENABLED=true` and Redis connection settings | `MURMUR_DRIVER=redis`, `MURMUR_REDIS_URL`, and `MURMUR_REDIS_CONNECTION` |

See the [Reverb configuration reference](https://github.com/laravel/docs/blob/13.x/reverb.md) and [default Reverb settings](https://github.com/laravel/reverb/blob/main/config/reverb.php).

Reverb dispatches its own connection events. In Murmur, enable [lifecycle events](#lifecycle-events) and register Laravel listeners for `ConnectionOpened` and `ConnectionClosed`. Murmur dispatches them from a separate worker as best-effort notifications for authorized SSE connections.

Presence channels, client events (whispers), multiple apps, and a Pusher-compatible HTTP API are unsupported in this version.

### Client events (whispers)

Murmur does not support whispers. Send the action to your Laravel application over HTTP, authorize it there, and broadcast the resulting event to the appropriate topic.

## Testing

Call `Murmur::fake()` before exercising Laravel code that publishes events. The fake records direct publishes, streams, and broadcasts through the `murmur` connection in memory. The server can stay stopped.

```php
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\Event;

$fake = Murmur::fake();

$connectionId = '0123456789abcdef0123456789abcdef';
Murmur::publish('news', 'updated', ['id' => 42], except: $connectionId);

$fake->assertPublished('news', 'updated', fn (Event $event): bool => $event->data['id'] === 42 && $event->except === $connectionId);
$fake->assertPublishedTimes('news', 'updated', 1);
$fake->assertNotPublished('news', 'removed');
```

Use `assertNothingPublished()` when no events should be sent. `published()` returns the recorded `Event` objects in publication order. If you omit the event name from `assertPublished()` or `assertNotPublished()`, the assertion considers every event on the topic. The fake can issue private subscription tokens using a fixed test secret when the application has no configured secret.

## Configuration

Edit `config/murmur.php` after publishing it. The environment variables shown below map to the corresponding configuration keys.

| Key | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `driver` | `MURMUR_DRIVER` | `http` | Transport used by Laravel and the server. Use http for one server, redis to share history across servers, or a registered custom driver. |
| `secret` | `MURMUR_SECRET` | `null` | Shared signing secret for subscription tokens and HTTP publish requests. Set the same secret of at least 32 bytes in Laravel and on every server. |
| `server.host` | `MURMUR_HOST` | `127.0.0.1` | Address to bind. Change this to accept traffic from another host. |
| `server.port` | `MURMUR_PORT` | `8080` | TCP port to listen on. Change this if the default port is in use. |
| `server.heartbeat` | `MURMUR_HEARTBEAT` | `20.0` | Seconds between SSE heartbeats. Keep this below the proxy idle timeout. |
| `server.retry_ms` | `MURMUR_RETRY_MS` | `3000` | Milliseconds clients should wait before reconnecting. Increase this to ease reconnect spikes. |
| `server.log_size` | `MURMUR_LOG_SIZE` | `10000` | Retained event count across all topics. Increase this to keep more replay history. |
| `server.max_connections` | `MURMUR_MAX_CONNECTIONS` | `900` | Maximum open SSE connections. Size this for your event loop and host capacity. |
| `server.max_sockets` | `MURMUR_MAX_SOCKETS` | `1000` | Maximum open TCP sockets, including SSE. Keep room above the SSE limit for HTTP requests. |
| `server.request_timeout` | `MURMUR_REQUEST_TIMEOUT` | `10.0` | Seconds allowed to complete an HTTP request. Increase this for slower uploads. |
| `server.max_replay_events` | `MURMUR_MAX_REPLAY_EVENTS` | `1000` | Maximum events per replay. Increase this if clients must recover longer interruptions. |
| `server.max_topics_per_connection` | `MURMUR_MAX_TOPICS_PER_CONNECTION` | `50` | Maximum topics in one subscription. Adjust this for clients that combine topics. |
| `server.max_payload_bytes` | `MURMUR_MAX_PAYLOAD_BYTES` | `65536` | Maximum publish payload size in bytes. Increase this if events need more space. |
| `server.max_buffer_bytes` | `MURMUR_MAX_BUFFER_BYTES` | `1048576` | Maximum buffered bytes per SSE connection. Lower this to shed slow subscribers earlier. |
| `server.allowed_origins` | `MURMUR_ALLOWED_ORIGINS` | `[]` | Comma-separated browser origins allowed for SSE. Use `*` to allow every origin. |
| `server.signature_tolerance` | `MURMUR_SIGNATURE_TOLERANCE` | `60` | Allowed difference in seconds between the server clock and an HTTP publish timestamp. |
| `server.loop_lag_threshold_ms` | `MURMUR_LOOP_LAG_THRESHOLD_MS` | `100` | Warn when the event loop timer fires more than this many milliseconds late. Must be positive. |
| `server.shutdown_grace` | `MURMUR_SHUTDOWN_GRACE` | `5.0` | Seconds to drain connections on shutdown before closing the remaining sockets. |
| `server.shutdown_spread` | `MURMUR_SHUTDOWN_SPREAD` | `2.0` | Seconds over which to end open streams on shutdown. Set to zero to end them together. Must be less than the shutdown grace period. |
| `server.log_channel` | `MURMUR_LOG_CHANNEL` | `null` | Laravel channel for server errors and lifecycle messages. The server writes from inside the event loop. Choose a fast local channel such as stderr, a file, or syslog that makes no network requests. |
| `public_url` | `MURMUR_PUBLIC_URL` | `http://127.0.0.1:8080` | Base URL for browser subscriptions. Set it to the public proxy or load balancer URL when browsers reach the server through an intermediary. |
| `publish_url` | `MURMUR_PUBLISH_URL` | `http://127.0.0.1:8080` | Base URL Laravel uses for HTTP publishing. Set it to the address reachable from the Laravel application. |
| `token_ttl` | `MURMUR_TOKEN_TTL` | `900` | Subscription token lifetime in seconds. Adjust how long a client can subscribe before it needs a fresh token. |
| `stream_cursor_margin_ms` | `MURMUR_STREAM_CURSOR_MARGIN_MS` | `1000` | Milliseconds to subtract from a new stream cursor. Increase this if clocks on the application and event hosts can differ. |
| `http.timeout` | `MURMUR_HTTP_TIMEOUT` | `2.0` | Total request deadline in seconds. |
| `http.connect_timeout` | `MURMUR_HTTP_CONNECT_TIMEOUT` | `1.0` | Connection deadline in seconds. |
| `redis.connection` | `MURMUR_REDIS_CONNECTION` | `default` | Name of the Laravel Redis connection used to append events. |
| `redis.url` | `MURMUR_REDIS_URL` | `redis://127.0.0.1:6379` | Redis URL used by the server for live reading and replay. |
| `redis.stream` | `MURMUR_REDIS_STREAM` | `murmur:events` | Stream name before Laravel's Redis key prefix. Set this to isolate applications in one database. |
| `redis.query_timeout` | `MURMUR_REDIS_QUERY_TIMEOUT` | `5.0` | Seconds allowed for a Redis query outside the blocking live read. Increase this for slower Redis round trips. |
| `redis.block_ms` | `MURMUR_REDIS_BLOCK_MS` | `5000` | Milliseconds Redis waits for new events during a live read. A stalled reader is replaced 5 seconds after this wait. |
| `metrics.store` | `MURMUR_METRICS_STORE` | `null` | Snapshot store. Set to `database`, `redis`, `cache`, or a registered custom store. |
| `metrics.interval` | `MURMUR_METRICS_INTERVAL` | `15.0` | Seconds between recording attempts. With lifecycle events enabled, busy ticks coalesce into a single due snapshot. |
| `metrics.ttl` | `MURMUR_METRICS_TTL` | `60` | Age in seconds at which a snapshot becomes stale. Must exceed the interval. |
| `metrics.timeout` | `MURMUR_METRICS_TIMEOUT` | `10.0` | Seconds to wait for an acknowledgement before reporting a timeout. The server keeps waiting for the reply. |
| `metrics.node` | `MURMUR_NODE` | `null` | Server identifier for metric snapshots and lifecycle events. Defaults to the hostname and listening port. |
| `metrics.only` | `MURMUR_METRICS_ONLY` | `[]` | Comma-separated metric names to record. An empty list records all server metrics. |
| `metrics.database.connection` | `MURMUR_METRICS_DB_CONNECTION` | `null` | Laravel database connection. Uses the application default when unset. |
| `metrics.database.table` | `MURMUR_METRICS_DB_TABLE` | `murmur_metrics` | Table where the database store keeps snapshots. |
| `metrics.redis.connection` | `MURMUR_METRICS_REDIS_CONNECTION` | `default` | Laravel Redis connection for snapshots. |
| `metrics.redis.key` | `MURMUR_METRICS_REDIS_KEY` | `murmur:metrics` | Redis hash key before Laravel applies its prefix. |
| `metrics.cache.store` | `MURMUR_METRICS_CACHE_STORE` | `null` | Laravel cache store. Uses the application default when unset. |
| `metrics.cache.prefix` | `MURMUR_METRICS_CACHE_PREFIX` | `murmur:metrics:` | Prefix applied to the cache entries and their index. |
| `lifecycle.enabled` | `MURMUR_LIFECYCLE_EVENTS` | `false` | Dispatch connection lifecycle events through the Artisan worker. |
| `lifecycle.buffer` | `MURMUR_LIFECYCLE_BUFFER` | `1000` | Positive maximum number of waiting records. Overflow evicts the oldest records. |
| `lifecycle.batch` | `MURMUR_LIFECYCLE_BATCH` | `100` | Positive maximum record count per batch. Must be at most `lifecycle.buffer`. |
| `lifecycle.interval` | `MURMUR_LIFECYCLE_INTERVAL` | `1.0` | Positive, finite number of seconds between batch attempts. |

Write `MURMUR_ALLOWED_ORIGINS` as a comma-separated list. Murmur trims whitespace and drops empty entries. Keep `server.max_sockets` at least as high as `server.max_connections`.

A subscriber receives `murmur.gap` in place of a replay if the requested history exceeds `server.max_replay_events` matching events or if the encoded replay frames exceed half of `server.max_buffer_bytes`. The attach marker still carries the sampled tail, and the connection continues delivering live events.

## Writing a driver

Implement `Contracts\Publisher` and `Contracts\EventLog` for an event transport. Implement `Contracts\MetricsStore` to store per-node snapshots, or `Contracts\SubscriptionAuthorizer` to verify application-issued subscription credentials. Register the implementations in a Laravel service provider's `boot()` method.

```php
use App\Murmur\ExampleEventLog;
use App\Murmur\ExampleMetricsStore;
use App\Murmur\ExamplePublisher;
use App\Murmur\ExampleSubscriptionAuthorizer;
use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Arttecs\Murmur\Facades\Murmur;
use Illuminate\Contracts\Foundation\Application;

Murmur::extend(
    'example',
    fn (Application $app): Publisher => $app->make(ExamplePublisher::class),
    fn (Application $app): EventLog => $app->make(ExampleEventLog::class),
);

Murmur::extendMetrics('reports', fn (Application $app): MetricsStore => $app->make(ExampleMetricsStore::class));

Murmur::authorizeSubscriptionsUsing(
    fn (Application $app): SubscriptionAuthorizer => $app->make(ExampleSubscriptionAuthorizer::class),
);
```

Set `MURMUR_DRIVER=example` and, independently, `MURMUR_METRICS_STORE=reports` to use those implementations. Names may contain lowercase ASCII letters, digits, underscores, and hyphens. The built-in event driver names are `http` and `redis`. The built-in metrics store names are `database`, `redis`, and `cache`.

Laravel calls the `Publisher` to send events. `murmur:serve` uses the `EventLog` for live delivery and replay. Event log methods and callbacks run in the server's event loop and must never block. Return ReactPHP promises from `since()` and `ready()` and keep `healthy()` a cheap synchronous check. Metrics store writes run in the separate Artisan worker. A store's `put()` replaces one node's snapshot, `all()` returns snapshots keyed by node, and `forget()` removes a node.

The subscription authorizer factory builds an object when `murmur:serve` creates the server. Its `authorize(?string $token, array $topics)` method runs in that server process's event loop and must be non-blocking. It receives the requested topics and one distinct token at a time, or `null` when the request contains no token. Cache or bound authorization I/O for up to `server.max_topics_per_connection` calls per request. Resolve a ReactPHP promise with an immutable `Server\Auth\Grant` containing a list of valid topic names and a positive Unix expiry in seconds. Reject with `Exceptions\InvalidToken` for invalid credentials. The server checks whether the grants cover each requested private topic. Use a token format without commas. With no custom factory, the server verifies Murmur's HS256 JWTs. Laravel's broadcasting endpoint, `Murmur::token()`, and private stream URLs still issue Murmur JWTs, so configure clients and the authorizer to use compatible credentials.

Run the packaged PHPUnit suites against your implementations. Extend `Testing\EventLogConformanceTestCase` and provide a new empty log with the supplied loop and a writer that appends into that log's backend. The fixture must trim to the requested capacity and return the assigned `EventId` directly or through a promise.

Event logs sample the tail before querying replay history and return it even when the topic list is empty. An empty topic list returns no events, and an empty log returns a null tail. Return events through the sampled tail and apply the optional `since()` event filter before counting results against the limit.

Deliver events to listeners in strictly increasing identifier order across all topics, including when listeners subscribe to the same log at different times. Each tail sample must be at or after every event already delivered to listeners when the sample is taken. The server uses that order and tail to assign attachment and heartbeat cursors. An idle subscription can silently skip events if the driver violates these guarantees.

```php
use App\Tests\Support\EventLogFixture;
use Arttecs\Murmur\Testing\EventLogConformanceTestCase;
use React\EventLoop\LoopInterface;

class ExampleEventLogTest extends EventLogConformanceTestCase
{
    protected function createEventLog(int $capacity, LoopInterface $loop): array
    {
        $fixture = new EventLogFixture($capacity, $loop);

        return [$fixture->log(), $fixture->append(...)];
    }
}
```

For a metrics store, extend `Testing\MetricsStoreConformanceTestCase` and implement `createMetricsStore(): MetricsStore` to return an empty, isolated store for each test. For a token-based authorizer, extend `Testing\SubscriptionAuthorizerConformanceTestCase` and supply the authorizer and its valid, malformed, and expired token fixtures. Use the fixture loop for asynchronous authorization and test your missing-token policy in your own suite. All three suites use plain PHPUnit.

## Deployment

Run Murmur behind a reverse proxy. The default empty origin allowlist serves requests that omit the `Origin` header, including typical same-origin EventSource requests. To serve a browser application from another origin, set `MURMUR_ALLOWED_ORIGINS` to its scheme, host, and non-default port, with no trailing slash. Write internationalised hosts in punycode. The server canonicalizes numeric ports and bracketed IPv6 literals, so `[::1]` and `[0:0:0:0:0:0:0:1]` match. Requests with an unlisted origin receive 403. Use `*` only when every origin should be allowed.

Keep `POST /publish` on a trusted network. A signed request is replayable within the default 60-second timestamp tolerance.

Without `ext-uv`, `ext-ev`, or `ext-event`, ReactPHP uses `stream_select()` and is limited to about 1000 sockets. `murmur:serve` prints a startup notice for this backend and refuses to start when `server.max_sockets` exceeds 1000. The defaults cap SSE connections at 900 and accepted sockets at 1000. To scale further, install one of the extensions and raise `ulimit -n` before increasing the connection and socket limits.

Use `GET /health` for liveness. It returns HTTP 200 with `"status":"ok"` and the current number of SSE subscriptions in `connections`. Use `GET /ready` to decide where to route new subscriptions. It returns HTTP 200 with `{"status":"ready"}` while the listener and live log are ready, and 503 with `{"status":"unavailable"}` when the live log is unavailable or the server is draining. A Redis-backed server starts listening after it can resolve its starting cursor.

For Kubernetes, bind to an address reachable from the kubelet, such as `MURMUR_HOST=0.0.0.0`, and configure a readiness probe on the Murmur container.

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8080
```

With `ext-pcntl` enabled in CLI PHP, `SIGTERM` and `SIGINT` stop new TCP connections and start graceful shutdown. Existing SSE streams finish at evenly spaced intervals across `MURMUR_SHUTDOWN_SPREAD`, 2 seconds by default. Each receives a final SSE `retry:` hint between `MURMUR_RETRY_MS` and twice that value. Set the spread to zero to end streams together. Keep it below `MURMUR_SHUTDOWN_GRACE`, which defaults to 5 seconds and bounds how long remaining sockets can stay open.

### Nginx

Proxy `/sse` to Murmur and disable buffering so events reach subscribers as they arrive. Set `MURMUR_PUBLIC_URL` to the external base URL. With the `http` driver, point `MURMUR_PUBLISH_URL` at the internal server address. The `token` and `connection` parameters travel in the `/sse` query string. Exclude that query string from proxy and load balancer logs and traces. Avoid logging `X-Socket-ID` on Laravel requests as well.

```nginx
location = /sse {
    access_log off;
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_read_timeout 1h;
}
```

### Supervisor

Run `murmur:serve` under Supervisor to start it at boot and restart it after an exit. Adjust the paths and application user for your deployment.

```ini
[program:murmur]
directory=/var/www/app
command=/usr/bin/php artisan murmur:serve
user=www-data
autostart=true
autorestart=true
stopsignal=TERM
redirect_stderr=true
stdout_logfile=/var/log/murmur.log
```

## Scaling with Redis

Use Redis 7.0 or later to share live events and replay history across Murmur nodes. The Laravel app needs a Redis client, either the phpredis extension or `predis/predis`. CI tests both clients. Murmur uses raw Redis commands and supports non-cluster connections. Set `MURMUR_DRIVER=redis` and the same `MURMUR_SECRET` in the Laravel application and on every Murmur node. Point each node at the shared Redis instance and database with `MURMUR_REDIS_URL`.

```dotenv
MURMUR_DRIVER=redis
MURMUR_REDIS_URL=redis://redis.internal:6379/0
MURMUR_PUBLIC_URL=https://events.example.com
```

Configure the Laravel connection named by `MURMUR_REDIS_CONNECTION` to reach the same Redis instance and database. Laravel appends events to the stream using that connection. Both sides use `database.redis.options.prefix` followed by `murmur.redis.stream` as the stream key. Keep these settings and `murmur.server.log_size` identical in the app and on every node, and enter the stream name without the prefix in `murmur.redis.stream`. The app uses the log size to trim the stream with `MAXLEN ~`. Each Murmur node uses Redis stream metadata to detect whether a reconnecting subscriber's cursor predates the retained history.

Run `php artisan murmur:serve` on each node and route `/sse` through a load balancer with response buffering disabled. With the Redis driver, a node starts listening only after it can reach Redis and resolve its starting cursor. It logs one message while waiting for Redis. A node listens on `127.0.0.1` by default, so set `MURMUR_HOST` to an address the load balancer can reach. Set `MURMUR_PUBLIC_URL` to the load balancer's public base URL. Each node reads the same stream, so a reconnect can resume on any node while its cursor is retained. The Redis driver disables the server's `POST /publish` endpoint.

During a Redis outage, each server retries its stream read one second after a failure. Murmur logs when the reader loses its connection and when it recovers. Each node reads at most 1,000 entries at a time, and checks for trimming on recovery and before delivering a batch that reaches the configured log size or 1,000 entries, whichever is smaller. If Redis has trimmed past the reader's cursor, Murmur sends `murmur.gap` to connected subscribers before delivering the retained events. This detection relies on `murmur.server.log_size` being identical in the application and on every node: if the application trims to a smaller size than a node expects, for example during a rolling deploy that changes `MURMUR_LOG_SIZE`, that node can skip trimmed events without signalling a gap. Trimming the stream by other means (`XTRIM`, `MINID`, `XDEL`) and deleting or recreating the key are not detected either. The check can signal a gap when nothing a subscriber needed was lost: when the only trimmed entries were ones it had already received, and when a reader is so far behind that trimming races the check. Size `log_size` well above the backlog a node can accumulate between reads; a gap reaches every connected subscriber at once. If the check fails, the reader retries from its previous cursor.

Every subscription samples the tail. Tail samples and replay queries share one Redis connection per Murmur process. Each command has a 5 second deadline by default. A command that exceeds its deadline closes that shared connection and fails all samples and replay queries queued on it. Each failed subscription ends before `murmur.attached` and increments `murmur_replay_failures_total`. Clients that rebuild their subscription URL should honor the stream's `retry:` delay or apply their own backoff when a stream ends before the marker.

Synchronize clocks across the Laravel application, Murmur nodes, and Redis. `stream_cursor_margin_ms` sets how much skew to allow in stream start cursors.

### Replay storage trade-off

Murmur uses one global Redis stream for all topics. A subscriber can resume with one cursor, event IDs have a total order, and each Murmur node needs one live reader regardless of how many topics it serves. A reconnect with an old cursor can scan the whole retained range after that cursor, including entries on unrelated topics. Redis trims the stream approximately to `murmur.server.log_size` entries, so a noisy topic shortens the history available to every topic. Replay stops scanning if matching events exceed `server.max_replay_events`, and the server discards replay frames if their encoded size exceeds half of `server.max_buffer_bytes`. Clients that track numbered heartbeats can resume quiet subscriptions from a recent cursor, reducing replay work and the chance of a gap after unrelated traffic has trimmed the stream. Account for aggregate publish volume when choosing a log size, especially when clients reconnect after long interruptions.

### Redis Sentinel and Cluster

The server's `clue/redis-react` client connects to one endpoint. It supports `redis://` over TCP, `rediss://` over TLS, and `redis+unix://` for a Unix socket. It supports password-only `AUTH` and database selection through the URI. Redis ACL usernames, Sentinel discovery, and Cluster slot routing and redirects are unsupported by this client. For failover, give Murmur a stable endpoint or proxy that routes to the writable primary and supports blocking stream reads. Configure Laravel's publishing connection to reach the same Redis database. [Client documentation](https://github.com/clue/reactphp-redis/blob/v2.8.0/README.md), [connection source](https://github.com/clue/reactphp-redis/blob/v2.8.0/src/Factory.php), [Redis reply handling](https://github.com/clue/reactphp-redis/blob/v2.8.0/src/StreamingClient.php).

## Metrics

Each server process collects metrics in memory. Storing snapshots is off by default. Murmur has no HTTP metrics route.

### Read metrics in the server process

`Server::metrics()` returns the process's `Arttecs\Murmur\Server\Metrics` registry.

- `snapshot()` returns a list of samples, each with `name`, `type`, `help`, `labels`, and `value`.
- `value($name, $labels = [])` returns one series, or `null` when it has no sample yet.
- `toPrometheus()` returns the snapshot in the Prometheus text exposition format (0.0.4).
- `Metrics::names()` lists the metric names supplied by the server.

Register a callback with `Murmur::serving()` in a service provider's `boot()` method to receive the server and its event loop before it starts listening.

```php
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\Server;
use Illuminate\Support\Facades\Log;
use React\EventLoop\LoopInterface;

Murmur::serving(function (Server $server, LoopInterface $loop): void {
    $loop->addPeriodicTimer(15.0, function () use ($server): void {
        Log::channel('stderr')->info('murmur', [
            'connections' => $server->metrics()->value('murmur_connections'),
            'events_sent' => $server->metrics()->value('murmur_events_sent_total'),
        ]);
    });
});
```

The callback runs in the server process. Keep timer callbacks fast and local, such as writing to stderr, and keep blocking store or network calls out of the event loop. The metrics with a `status` label gain a sample on the first response with that status. For example, a request to `POST /publish` on a server using the Redis transport counts under `status="404"`.

### Store snapshots for Laravel

Set `MURMUR_METRICS_STORE` in the Laravel app and on each Murmur server, and point them to the same store. The metrics store is independent of the Murmur event transport.

- **Database.** Set `MURMUR_METRICS_STORE=database`. Set the database connection and table before publishing and running the migration. The migration uses the configured connection and table.
- **Redis.** Set `MURMUR_METRICS_STORE=redis`. Use the same Laravel Redis connection, prefix, and metrics hash key on each host. Both PhpRedis and Predis work.
- **Cache.** Set `MURMUR_METRICS_STORE=cache` and choose a Laravel cache store shared by the worker processes and the app, such as Redis or the database cache. Use the same metrics key prefix on each host. Snapshots and their index expire in the cache. Concurrent index updates use an atomic lock when the cache store supports it and are last-writer-wins otherwise.

For the database store, publish and run the migration once during setup.

```bash
php artisan vendor:publish --tag=murmur-migrations
php artisan migrate
```

Each store keeps the latest snapshot for each node. A node identifies one `murmur:serve` process and defaults to `hostname:port` using the listening port, including a `--port` override. Set a distinct `MURMUR_NODE` for each process if the defaults collide. Node names can use up to 128 ASCII letters, digits, dots, underscores, colons, or hyphens. Set `MURMUR_METRICS_ONLY` to a comma-separated selection from the metric names below, or leave it blank to record all of them.

Murmur starts one Artisan worker before the server opens sockets when either metrics persistence or lifecycle events are enabled. The server sends the first snapshot as soon as it starts listening and checks for a new snapshot on each metrics interval. The worker performs store I/O while the server continues serving SSE. With metrics persistence alone, the server skips ticks while a snapshot is pending. When both features are enabled, the server remembers at most one due snapshot and alternates between the two when both are due. Murmur reports a timeout once if a reply exceeds `metrics.timeout` and waits for the acknowledgement before sending again.

When a store write fails, the worker reports the exception through Laravel's exception handler and sends an error acknowledgement. The worker shares the server's standard error, so a logging channel such as `stderr` puts the cause alongside the server's output. The worker reserves standard output for acknowledgements. Keep direct writes to STDOUT out of custom stores and lifecycle listeners. If the worker exits during serving, Murmur reports its exit code or signal once and stops both features until the server restarts. The server keeps accepting connections.

On graceful shutdown, the server stops scheduling snapshots and lets the worker finish a pending write. The server sends SIGTERM after one second if the worker is still running and escalates to SIGKILL 200 ms later. Readers ignore snapshots when their age reaches the TTL. Use a TTL greater than the interval and keep the server and application clocks synchronized.

The worker requires Linux or macOS, `proc_open()`, a POSIX `sh`, an Artisan entry point that resolves to an absolute path, and PHP built without `--enable-sigchild`. It runs the server's PHP binary with its default configuration. `murmur:serve` exits with a one-line error when startup preflight fails. With `pcntl` and `posix`, the worker checks for parent death while processing messages when PHP can dispatch signals. Without both extensions, an orphaned worker exits on end-of-input once its current operation returns.

```php
use Arttecs\Murmur\Facades\Murmur;

$nodes = Murmur::metrics();
$totals = Murmur::metricTotals();
```

`Murmur::metrics()` returns fresh snapshots keyed and sorted by node name. Each contains the Unix timestamp `recorded_at` and a `samples` list. The values belong to one server process and are as old as its timestamp. `Murmur::metricTotals()` sums fresh counters and gauges across nodes except `murmur_uptime_seconds`. Keys use Prometheus-style series names, such as `murmur_publish_requests_total{status="202"}`, or the bare metric name for an unlabeled series. Both methods throw `Arttecs\Murmur\Exceptions\MetricsStoreNotConfigured` when storing is disabled.

```bash
php artisan murmur:status
php artisan murmur:status --json --node=edge-1:8080
php artisan murmur:forget-metrics edge-1:8080
php artisan murmur:forget-metrics
```

`murmur:status` shows the snapshot age, open connections, topics, events sent, errors, and memory per node, followed by a totals row. Use `--json` for the snapshots and totals as JSON, and `--node` to limit both to one node. The command exits with status 1 if the store is disabled or no fresh snapshots match. `murmur:forget-metrics` removes the named node or clears all stale snapshots when no name is supplied. Use it to remove stale database rows or Redis hash fields that remain after their snapshots have aged out.

### Metric names

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `murmur_connections_opened_total` | counter | — | SSE connections accepted. |
| `murmur_connections_closed_total` | counter | — | SSE connections that ended. |
| `murmur_connections_rejected_total` | counter | `status` | SSE requests rejected by HTTP status. |
| `murmur_slow_consumers_total` | counter | — | Connections closed after exceeding the output buffer limit. |
| `murmur_events_received_total` | counter | — | Events delivered by the event log to the hub. |
| `murmur_events_sent_total` | counter | — | Live and replayed event frames written to connections. |
| `murmur_bytes_sent_total` | counter | — | Bytes of SSE output written to connections. |
| `murmur_replays_total` | counter | — | Connections that requested replay with a cursor. |
| `murmur_replay_failures_total` | counter | — | Subscriptions whose tail sample, replay query, or initial release failed. |
| `murmur_replayed_events_total` | counter | — | Event frames written from replay. |
| `murmur_gaps_total` | counter | — | Murmur gap frames written to connections. |
| `murmur_publish_requests_total` | counter | `status` | HTTP publish responses by status. |
| `murmur_event_loop_lag_exceeded_total` | counter | — | Lag measurements, taken every 100 ms, above `server.loop_lag_threshold_ms`. |
| `murmur_lifecycle_dropped_total` | counter | — | Lifecycle records discarded before handoff to the worker, including on overflow or worker loss. |
| `murmur_lifecycle_sent_total` | counter | — | Lifecycle records handed to the worker. |
| `murmur_errors_total` | counter | — | Errors reported, including rate-limited reports. |
| `murmur_connections` | gauge | — | Open SSE connections. |
| `murmur_sockets` | gauge | — | Accepted TCP sockets currently open. |
| `murmur_topics` | gauge | — | Topics with at least one subscriber. |
| `murmur_uptime_seconds` | gauge | — | Seconds since the server started listening. |
| `murmur_memory_bytes` | gauge | — | Bytes of memory allocated by PHP. |
| `murmur_memory_peak_bytes` | gauge | — | Peak bytes of memory allocated by PHP. |
| `murmur_event_loop_lag_seconds` | gauge | — | Largest event loop timer delay in seconds over the last ten measurements, taken every 100 ms, or zero before the first ten. |

## Lifecycle events

Set `MURMUR_LIFECYCLE_EVENTS=true` on each Murmur server and restart it. Laravel dispatches `Arttecs\Murmur\Events\ConnectionOpened` when an authorized SSE connection attaches and `Arttecs\Murmur\Events\ConnectionClosed` when that connection closes. Rejected subscriptions emit no lifecycle events. These events describe the connection to the hub. The browser's `murmur.attached` frame also waits for its initial replay.

Both events expose the 32-character hex `connectionId`, the `node` (`MURMUR_NODE` or `hostname:port`), the subscribed wire `topics` including any `private-` prefixes, and the server's `occurredAt` Unix timestamp in seconds. The JavaScript client's connection ID identifies a browser tab and is the value sent as `X-Socket-ID`. The events contain only these connection fields and, on close, `durationSeconds` and `reason`.

| Close reason | Meaning |
| --- | --- |
| `client` | The peer disconnected. |
| `expired` | The subscription grant expired. |
| `slow_consumer` | The connection exceeded a backpressure limit. |
| `shutdown` | The server ended the stream during shutdown or closed the connection directly. |
| `error` | Replay or another server-side stream operation failed. |

Use queued Laravel listeners with an asynchronous queue connection and a running queue worker. For example, put this listener in `app/Listeners` when using Laravel's event discovery. If discovery is disabled, register the listener in your application's service provider with `Event::listen(ConnectionOpened::class, RecordConnectionOpened::class)`.

```php
namespace App\Listeners;

use Arttecs\Murmur\Events\ConnectionOpened;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Support\Facades\Log;

class RecordConnectionOpened implements ShouldQueue
{
    public function handle(ConnectionOpened $event): void
    {
        Log::info('Murmur connection opened.', [
            'connection_id' => $event->connectionId,
            'node' => $event->node,
        ]);
    }
}
```

Murmur attempts delivery in the server's recorded order and hands each batch to the worker at most once. Batches are capped by both `lifecycle.batch` and the worker's 1 MiB input limit. If a listener throws, the worker reports the exception through Laravel's exception handler, stops dispatching that batch, and accepts the next batch. The listener's queue applies its own execution order and retry policy.

Use the events for best-effort notifications and a durable audit log for auditing. Buffer overflow drops the oldest records, and worker death and shutdown can also lose events. On shutdown the server sends a final batch if the worker can accept it within the existing shutdown budget.

Lifecycle events use the same [worker and platform requirements as metrics persistence](#store-snapshots-for-laravel) and work with or without a metrics store. The worker accepts one batch or snapshot at a time. A slow store delays lifecycle batches, which accumulate until the buffer fills. A slow listener delays both later batches and metrics snapshots. The SSE server continues serving while the worker is busy.

## Logging

Set `MURMUR_LOG_CHANNEL` to a Laravel log channel to receive server delivery errors and informational messages when the server starts listening and when graceful shutdown begins and ends. Choose a fast local channel such as stderr, a file, or syslog because the server writes inside its event loop. Listeners of Laravel's `MessageLogged` event run inside the loop as well, so they must not perform network requests either. `murmur:serve` exits with an error when the channel is not defined in `logging.channels`.

Delivery errors go to PHP's error log by default and fall back to it if the configured logger throws. Murmur rate-limits these messages to one per second and includes the number of suppressed errors in the next message. Redis reader connection transitions use PHP's error log.

When the event loop timer runs late by more than `MURMUR_LOOP_LAG_THRESHOLD_MS`, Murmur warns through the configured log channel or through PHP's error log. Warnings show the delay in milliseconds and are limited to one every ten seconds per server. The breach counter records every measurement above the threshold.

## Version 1 limits

- Murmur supports SSE subscriptions. WebSockets and Laravel presence channels are unsupported.
- The `http` driver runs on a single Murmur node and keeps its bounded history in memory. Restarting that node clears its history. The `redis` driver supports multiple Murmur nodes with a shared history in Redis.
- With the `http` driver and the default cursor margin, a stream created during the server's first second may receive a spurious `murmur.gap` event because its initial cursor predates the in-memory history.

## Compatibility

Semantic versioning covers the `Murmur` facade and the public methods of `MurmurManager`, the `MurmurServiceProvider` class name, the types in `Contracts\` and `Exceptions\`, configuration keys and environment variables, and public Artisan command names and options.

It also covers `Events\ConnectionOpened`, `Events\ConnectionClosed`, `Server\Server`, `Server\ServerConfig`, `Server\Auth\Grant`, `Server\Metrics` (`snapshot()`, `value()`, `toPrometheus()`, and `names()`), `Server\Event`, `Server\EventId`, `Server\Replay`, `Stream`, the classes in `Testing\`, and the [wire protocol](docs/protocol.md).

The lifecycle recorder, worker, worker configuration, and hidden `murmur:worker` command are internal.

## License

Murmur is released under the [MIT License](LICENSE).
