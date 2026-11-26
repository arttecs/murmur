# Murmur

Murmur is a ReactPHP Server-Sent Events (SSE) server with a Laravel bridge. An SSE subscription can stay open for minutes or hours. When Laravel serves the stream directly through PHP-FPM, that connection holds a worker for its lifetime. Murmur keeps subscriptions in a separate, long-running process. Laravel publishes events over HTTP or Redis and continues serving requests with its workers available.

## Installation

Murmur requires PHP 8.3 or later and Laravel 12 or 13.

```sh
composer require arttecs/murmur
php artisan vendor:publish --tag=murmur-config
```

Generate a shared secret of at least 32 bytes and set `MURMUR_SECRET` in your Laravel `.env` file. Laravel and the SSE server use the same secret to sign and verify subscription tokens and HTTP publish requests.

```sh
php -r "echo bin2hex(random_bytes(32));"
```

Start the server from your Laravel application directory.

```sh
php artisan murmur:serve
```

The default address is `http://127.0.0.1:8080`. To override the bind address for a single invocation, use `--host` and `--port`. Update `MURMUR_PUBLIC_URL` when the public address changes. Update `MURMUR_PUBLISH_URL` when the address Laravel uses for HTTP publishing changes.

```sh
php artisan murmur:serve --host=127.0.0.1 --port=8081
```

A public subscription receives a reconnect interval immediately and a heartbeat every 20 seconds by default.

```sh
curl -N 'http://127.0.0.1:8080/sse?topics=news'
```

## Laravel broadcasting

If your application does not have a `config/broadcasting.php` file yet, publish it with `php artisan config:publish broadcasting`. Then add a `murmur` entry to its `connections` array.

```php
'murmur' => [
    'driver' => 'murmur',
],
```

Make Murmur your application's default broadcast connection in `.env`. This is required: Laravel registers the callbacks from `routes/channels.php` on the default connection, so `POST /murmur/auth` can only authorize channels when that connection is `murmur`.

```dotenv
BROADCAST_CONNECTION=murmur
```

Events implementing `ShouldBroadcast` publish to their Laravel channel names as SSE topics. A `Channel('news')` publishes to `news`, and a `PrivateChannel('orders.42')` publishes to `private-orders.42`. Run your Laravel queue worker if your broadcasts are queued.

Register private channel callbacks with Laravel's `Broadcast::channel()` using channel patterns such as `orders.{orderId}`. An authenticated client can send `{"channels":["private-orders.42"]}` to `POST /murmur/auth`. Laravel checks access against each channel's configured guard and returns `token` and `expires_at` on success. Channel names in one request may total up to 2048 bytes. Names with non-ASCII characters grow when encoded in the subscription URL, and the server rejects a request whose headers exceed 8 KiB, so keep channel names short and ASCII. Use the token in the `token` query parameter on `/sse` along with the requested `topics`. The auth route uses the `web` middleware by default, so include the session credentials and CSRF token required by your application.

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
| `server.allowed_origins` | `MURMUR_ALLOWED_ORIGINS` | `["*"]` | Comma-separated browser origins allowed for SSE. Replace * to restrict cross-origin reads. |
| `server.signature_tolerance` | `MURMUR_SIGNATURE_TOLERANCE` | `60` | Allowed difference in seconds between the server clock and an HTTP publish timestamp. |
| `server.shutdown_grace` | `MURMUR_SHUTDOWN_GRACE` | `5.0` | Seconds to drain connections on shutdown before closing the remaining sockets. |
| `server.log_channel` | `MURMUR_LOG_CHANNEL` | `null` | Laravel channel for server errors and lifecycle messages. The server writes from inside the event loop. Choose a fast local channel such as stderr, a file, or syslog that makes no network requests. |
| `public_url` | `MURMUR_PUBLIC_URL` | `http://127.0.0.1:8080` | Base URL for browser subscriptions. Set it to the public proxy or load balancer URL when browsers reach the server through an intermediary. |
| `publish_url` | `MURMUR_PUBLISH_URL` | `http://127.0.0.1:8080` | Base URL Laravel uses for HTTP publishing. Set it to the address reachable from the Laravel application. |
| `token_ttl` | `MURMUR_TOKEN_TTL` | `900` | Subscription token lifetime in seconds. Adjust how long a client can subscribe before it needs a fresh token. |
| `stream_cursor_margin_ms` | `MURMUR_STREAM_CURSOR_MARGIN_MS` | `1000` | Milliseconds to subtract from a new stream cursor. Increase this if clocks on the application and event hosts can differ. |
| `http.timeout` | `MURMUR_HTTP_TIMEOUT` | `2.0` | Total request deadline in seconds. |
| `http.connect_timeout` | `MURMUR_HTTP_CONNECT_TIMEOUT` | `1.0` | Connection deadline in seconds. |
| `auth.enabled` | `MURMUR_AUTH_ENABLED` | `true` | Whether to register the murmur.auth route. |
| `auth.path` | `MURMUR_AUTH_PATH` | `murmur/auth` | URL path for the authorization endpoint. Change this if the path is already in use. |
| `auth.middleware` | `MURMUR_AUTH_MIDDLEWARE` | `["web"]` | Comma-separated Laravel middleware for the authorization route. Publish this file to use middleware whose parameters contain commas. |
| `redis.connection` | `MURMUR_REDIS_CONNECTION` | `default` | Name of the Laravel Redis connection used to append events. |
| `redis.url` | `MURMUR_REDIS_URL` | `redis://127.0.0.1:6379` | Redis URL used by the server for live reading and replay. |
| `redis.stream` | `MURMUR_REDIS_STREAM` | `murmur:events` | Stream name before Laravel's Redis key prefix. Set this to isolate applications in one database. |
| `redis.query_timeout` | `MURMUR_REDIS_QUERY_TIMEOUT` | `5.0` | Seconds allowed for a Redis query outside the blocking live read. Increase this for slower Redis round trips. |
| `redis.block_ms` | `MURMUR_REDIS_BLOCK_MS` | `5000` | Milliseconds Redis waits for new events during a live read. A stalled reader is replaced 5 seconds after this wait. |
| `metrics.store` | `MURMUR_METRICS_STORE` | `null` | Snapshot store. Set to `database`, `redis`, `cache`, or a registered custom store. |
| `metrics.interval` | `MURMUR_METRICS_INTERVAL` | `15.0` | Seconds between recording attempts. Ticks are skipped while a write is pending. |
| `metrics.ttl` | `MURMUR_METRICS_TTL` | `60` | Age in seconds at which a snapshot becomes stale. Must exceed the interval. |
| `metrics.timeout` | `MURMUR_METRICS_TIMEOUT` | `10.0` | Seconds to wait for an acknowledgement before reporting a timeout. The server keeps waiting for the reply. |
| `metrics.node` | `MURMUR_NODE` | `null` | Identifier of this server. Defaults to its hostname and listening port. |
| `metrics.only` | `MURMUR_METRICS_ONLY` | `[]` | Comma-separated metric names to record. An empty list records all server metrics. |
| `metrics.database.connection` | `MURMUR_METRICS_DB_CONNECTION` | `null` | Laravel database connection. Uses the application default when unset. |
| `metrics.database.table` | `MURMUR_METRICS_DB_TABLE` | `murmur_metrics` | Table where the database store keeps snapshots. |
| `metrics.redis.connection` | `MURMUR_METRICS_REDIS_CONNECTION` | `default` | Laravel Redis connection for snapshots. |
| `metrics.redis.key` | `MURMUR_METRICS_REDIS_KEY` | `murmur:metrics` | Redis hash key before Laravel applies its prefix. |
| `metrics.cache.store` | `MURMUR_METRICS_CACHE_STORE` | `null` | Laravel cache store. Uses the application default when unset. |
| `metrics.cache.prefix` | `MURMUR_METRICS_CACHE_PREFIX` | `murmur:metrics:` | Prefix applied to the cache entries and their index. |

Write `MURMUR_ALLOWED_ORIGINS` and `MURMUR_AUTH_MIDDLEWARE` as comma-separated lists. Murmur trims whitespace and drops empty entries. Keep `server.max_sockets` at least as high as `server.max_connections`. Set `MURMUR_AUTH_ENABLED=false` to omit the Laravel authorization route. Its name is `murmur.auth` whenever it is registered. Boolean variables accept `true`, `false`, `1`, `0`, `on`, `off`, `yes`, and `no`; any other value counts as false. Middleware with comma-separated parameters, such as `throttle:60,1`, cannot be written in `MURMUR_AUTH_MIDDLEWARE`; publish the config file and set `auth.middleware` there.

A subscriber receives `murmur.gap` in place of a replay if the requested history exceeds `server.max_replay_events` matching events or if the encoded replay frames exceed half of `server.max_buffer_bytes`. The connection continues delivering live events.

## Custom drivers

Register a publisher factory and an event log factory with `Murmur::extend()` in a Laravel service provider's `boot()` method. This example uses the HTTP publisher and a 5,000-event in-memory log under the name `local`. Set `MURMUR_DRIVER=local` to use it.

```php
use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Publishers\HttpPublisher;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Http\Client\Factory;

Murmur::extend(
    'local',
    fn (Application $app): Publisher => new HttpPublisher(
        $app->make(Factory::class),
        config('murmur.publish_url'),
        config('murmur.secret'),
        config('murmur.http.timeout'),
        config('murmur.http.connect_timeout'),
    ),
    fn (Application $app): EventLog => new MemoryEventLog(5000),
);
```

Each factory receives the Laravel application. Laravel calls the publisher factory to publish events, and `murmur:serve` calls the event log factory to serve live events and replay. Provide your own implementations of `Contracts\Publisher` and `Contracts\EventLog` to use a different transport. The server exposes `POST /publish` for custom drivers whose log is a `MemoryEventLog`. Driver names may contain lowercase ASCII letters, digits, underscores, and hyphens. Murmur reserves `http` and `redis`.

## Deployment

Run Murmur behind a reverse proxy. Keep `POST /publish` on a trusted network. A signed request is replayable within the default 60-second timestamp tolerance.

ReactPHP's default event loop uses `stream_select()` and supports about 1024 file descriptors. The defaults cap SSE connections at 900 and accepted sockets at 1000 to leave room for other descriptors. To scale beyond the default loop's limit, use `ext-uv`, `ext-ev`, or `ext-event` and raise `ulimit -n` before increasing the connection and socket limits.

`GET /health` returns HTTP 200 with a JSON object containing `"status":"ok"` and `connections`, the current number of SSE subscriptions.

### Nginx

Proxy `/sse` to Murmur and disable buffering so events reach subscribers as they arrive. Set `MURMUR_PUBLIC_URL` to the external base URL. With the `http` driver, point `MURMUR_PUBLISH_URL` at the internal server address. Subscription tokens travel in the `/sse` query string. Exclude that query string from proxy and load balancer logs and from traces.

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

Run `murmur:serve` under Supervisor to start it at boot and restart it after an exit. Adjust the paths and application user for your deployment. With `ext-pcntl` enabled in CLI PHP, Murmur stops accepting connections on `SIGTERM` or `SIGINT` and allows up to 5 seconds by default for open streams to flush and in-flight requests to finish. Set `MURMUR_SHUTDOWN_GRACE` to change the deadline. It closes any remaining sockets before stopping the event loop.

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

Configure the Laravel connection named by `MURMUR_REDIS_CONNECTION` to reach the same Redis instance and database. Laravel appends events to the stream using that connection. Both sides use `database.redis.options.prefix` followed by `murmur.redis.stream` as the stream key. Keep both settings identical in the app and on every node, and enter the stream name without the prefix in `murmur.redis.stream`. The app uses `murmur.server.log_size` to trim the stream. Each Murmur node uses Redis stream metadata to detect whether a reconnecting subscriber's cursor predates the retained history.

Run `php artisan murmur:serve` on each node and route `/sse` through a load balancer with response buffering disabled. With the Redis driver, a node starts listening only after it can reach Redis and resolve its starting cursor. It logs one message while waiting for Redis. A node listens on `127.0.0.1` by default, so set `MURMUR_HOST` to an address the load balancer can reach. Set `MURMUR_PUBLIC_URL` to the load balancer's public base URL. Each node reads the same stream, so a reconnect can resume on any node while its cursor is retained. The Redis driver disables the server's `POST /publish` endpoint.

Replay history is one global log shared by all topics. Redis trims it approximately to `murmur.server.log_size` entries. During a Redis outage, each server retries its stream read one second after a failure. Murmur logs when the reader loses its connection and when it recovers. If Redis trimmed events during the outage, Murmur sends a `murmur.gap` event to connected subscribers before it resumes delivering retained events. The check errs towards signalling: a subscriber may receive `murmur.gap` when the only trimmed entries were ones it had already received. Replay queries share one Redis connection and each has a 5 second deadline by default, so a Redis slower than that fails the replays in flight and those subscribers reconnect.

Synchronize clocks across the Laravel application, Murmur nodes, and Redis. `stream_cursor_margin_ms` sets how much skew to allow in stream start cursors.

## Metrics

Each server process collects metrics in memory. Storing snapshots is off by default. Murmur has no HTTP metrics route.

### Read metrics in the server process

`Server::metrics()` returns the process's `Arttecs\Murmur\Server\Metrics` registry.

- `snapshot()` returns a list of samples, each with `name`, `type`, `help`, `labels`, and `value`.
- `value($name, $labels = [])` returns one series, or `null` when it has no sample yet.
- `toPrometheus()` returns the snapshot in the Prometheus text exposition format (0.0.4).

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
- **Cache.** Set `MURMUR_METRICS_STORE=cache` and choose a Laravel cache store shared by the writer processes and the app, such as Redis or the database cache. Use the same metrics key prefix on each host. Snapshots and their index expire in the cache. Concurrent index updates use an atomic lock when the cache store supports it and are last-writer-wins otherwise.

For the database store, publish and run the migration once during setup.

```bash
php artisan vendor:publish --tag=murmur-migrations
php artisan migrate
```

Each store keeps the latest snapshot for each node. A node identifies one `murmur:serve` process and defaults to `hostname:port` using the listening port, including a `--port` override. Set a distinct `MURMUR_NODE` for each process if the defaults collide. Node names can use up to 128 ASCII letters, digits, dots, underscores, colons, or hyphens. Set `MURMUR_METRICS_ONLY` to a comma-separated selection from the metric names below, or leave it blank to record all of them.

Murmur starts one Artisan writer before the server starts listening and reuses it for the server's lifetime. The server sends the first snapshot as soon as it starts listening, then attempts a new snapshot on each interval tick. Store I/O runs in the writer, so the server keeps delivering events while a write is pending. The server skips ticks until the writer acknowledges the current snapshot. If `metrics.timeout` elapses, Murmur reports the timeout once for that write and keeps waiting. A late acknowledgement allows recording to resume on the next tick.

When a store write fails, the writer reports the underlying exception through Laravel's exception handler and sends an error acknowledgement. The writer shares the server's standard error, so with a log channel that writes there, such as `stderr`, the cause appears next to the server's own output. The writer's standard output carries the acknowledgements and is otherwise discarded: a custom store must not write to it, because a line that is not an acknowledgement counts as a failed write. The next interval can write another snapshot. If the writer exits during serving, Murmur reports its exit code or signal and stops recording until the server restarts. The server keeps accepting connections.

On graceful shutdown, the server stops scheduling snapshots and closes the writer's input. The writer can finish its current write. If it is still running after one second, the server sends SIGTERM and escalates to SIGKILL shortly after. Readers ignore snapshots when their age reaches the TTL. Use a TTL greater than the interval and keep the server and application clocks synchronized.

The recorder is supported on Linux and macOS and requires `proc_open()`, a POSIX `sh`, and PHP built without `--enable-sigchild`. The writer runs the server's PHP binary without the server's `-d` or `-c` options. Windows is unsupported. `murmur:serve` checks these requirements at startup and exits with a one-line error if a check fails. With `pcntl` and `posix`, the writer checks for parent death during store calls when PHP can dispatch signals. Without both extensions, a writer orphaned in the middle of a store call exits on end-of-input once that call returns, so the store's own timeouts bound how long it lives.

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

### Register a custom store

Call `Murmur::extendMetrics()` from a Laravel service provider's `boot()` method. The factory receives the application and returns an implementation of `Arttecs\Murmur\Contracts\MetricsStore`. The manager resolves it once per process on demand. This example registers a store backed by Laravel's `redis` cache with its own prefix. Set `MURMUR_METRICS_STORE=reports` to use it.

```php
use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Metrics\CacheMetricsStore;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Foundation\Application;

Murmur::extendMetrics('reports', fn (Application $app): MetricsStore => new CacheMetricsStore(
    $app->make(CacheFactory::class)->store('redis'),
    'reports:murmur:',
    (int) ceil((float) config('murmur.metrics.ttl')),
));
```

To use a different backend, implement `put()` to replace one node's snapshot, `all()` to return the snapshots keyed by node, and `forget()` to remove a node. Custom names use lowercase ASCII letters, digits, underscores, or hyphens. Murmur reserves `database`, `redis`, and `cache`.

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
| `murmur_replayed_events_total` | counter | — | Event frames written from replay. |
| `murmur_gaps_total` | counter | — | Murmur gap frames written to connections. |
| `murmur_publish_requests_total` | counter | `status` | HTTP publish responses by status. |
| `murmur_errors_total` | counter | — | Errors reported, including rate-limited reports. |
| `murmur_connections` | gauge | — | Open SSE connections. |
| `murmur_sockets` | gauge | — | Accepted TCP sockets currently open. |
| `murmur_topics` | gauge | — | Topics with at least one subscriber. |
| `murmur_uptime_seconds` | gauge | — | Seconds since the server started listening. |
| `murmur_memory_bytes` | gauge | — | Bytes of memory allocated by PHP. |
| `murmur_memory_peak_bytes` | gauge | — | Peak bytes of memory allocated by PHP. |

## Logging

Set `MURMUR_LOG_CHANNEL` to a Laravel log channel to receive server delivery errors and informational messages when the server starts listening and when graceful shutdown begins and ends. Choose a fast local channel such as stderr, a file, or syslog because the server writes inside its event loop. Listeners of Laravel's `MessageLogged` event run inside the loop as well, so they must not perform network requests either. `murmur:serve` exits with an error when the channel is not defined in `logging.channels`.

Delivery errors go to PHP's error log by default and fall back to it if the configured logger throws. Murmur rate-limits these messages to one per second and includes the number of suppressed errors in the next message. Redis reader connection transitions use PHP's error log.

## Version 1 limits

- Murmur supports SSE subscriptions. WebSockets and Laravel presence channels are unsupported.
- The `http` driver runs on a single Murmur node and keeps its bounded history in memory. Restarting that node clears its history. The `redis` driver supports multiple Murmur nodes with a shared history in Redis.
- With the `http` driver and the default cursor margin, a stream created during the server's first second may receive a spurious `murmur.gap` event because its initial cursor predates the in-memory history.

## Compatibility

Semantic versioning covers the `Murmur` facade and the public methods of `MurmurManager`, the types in `Contracts\` and `Exceptions\`, configuration keys and environment variables, and public Artisan command names and options.

It also covers `Server\Server`, `Server\ServerConfig`, `Server\Metrics` (`snapshot()`, `value()`, `toPrometheus()`, and `names()`), `Server\Event`, `Server\EventId`, `Server\Replay`, `Stream`, the classes in `Testing\`, and the [wire protocol](docs/protocol.md).

## License

Murmur is released under the [MIT License](LICENSE).
