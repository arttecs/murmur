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

The default address is `http://127.0.0.1:8080`. To override the bind address for a single invocation, use `--host` and `--port`. Update `MURMUR_PUBLIC_URL` when the public address changes. With the `http` driver, also update `MURMUR_PUBLISH_URL` when the publish address changes.

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

Register private channel callbacks with Laravel's `Broadcast::channel()` using channel patterns such as `orders.{orderId}`. An authenticated client can send `{"channels":["private-orders.42"]}` to `POST /murmur/auth`. Laravel checks access to each requested channel and returns `token` and `expires_at` on success. Use the token in the `token` query parameter on `/sse` along with the requested `topics`. The auth route uses the `web` middleware by default, so include the session credentials and CSRF token required by your application.

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

## JavaScript client

Use `@arttecs/murmur` to subscribe to channels and consume streams in the browser. See the [JavaScript client README](js/README.md) for installation and usage.

## Configuration

Edit `config/murmur.php` after publishing it. The environment variables shown below map to the corresponding configuration keys.

| Key | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `driver` | `MURMUR_DRIVER` | `http` | `http` for HTTP publishing and in-memory history, or `redis` for a shared Redis stream |
| `secret` | `MURMUR_SECRET` | Required | Shared signing secret of at least 32 bytes |
| `server.host` | `MURMUR_HOST` | `127.0.0.1` | Address on which the server listens |
| `server.port` | `MURMUR_PORT` | `8080` | Port on which the server listens |
| `server.heartbeat` | | `20` | Seconds between SSE heartbeats |
| `server.retry_ms` | | `3000` | Reconnect interval sent to subscribers, in milliseconds |
| `server.log_size` | | `10000` | Maximum in-memory history for `http`, or approximate stream length for `redis` |
| `server.max_connections` | | `10000` | Maximum number of concurrent SSE connections |
| `server.max_topics_per_connection` | | `50` | Maximum number of topics on a subscription |
| `server.max_payload_bytes` | | `65536` | Maximum size of the HTTP publish request body in bytes |
| `server.max_buffer_bytes` | | `1048576` | Maximum queued output per subscriber in bytes |
| `server.allowed_origins` | | `['*']` | Allowed origins for cross-origin SSE requests |
| `public_url` | `MURMUR_PUBLIC_URL` | `http://127.0.0.1:8080` | Base URL subscribers use to reach the SSE server |
| `publish_url` | `MURMUR_PUBLISH_URL` | `http://127.0.0.1:8080` | Base URL Laravel uses with the `http` driver |
| `token_ttl` | | `900` | Subscription token lifetime in seconds |
| `stream_cursor_margin_ms` | | `1000` | Milliseconds subtracted from stream creation time for the initial cursor |
| `http.timeout` | | `2.0` | Publish request timeout in seconds |
| `http.connect_timeout` | | `1.0` | Publish connection timeout in seconds |
| `redis.connection` | `MURMUR_REDIS_CONNECTION` | `default` | Laravel Redis connection used to append events |
| `redis.url` | `MURMUR_REDIS_URL` | `redis://127.0.0.1:6379` | Redis URL used by the Murmur server |
| `redis.stream` | | `murmur:events` | Stream key before Laravel's Redis prefix is applied |
| `auth.path` | | `murmur/auth` | Laravel route path for private channel authorization |
| `auth.middleware` | | `['web']` | Middleware for the authorization route |

## Deployment

### Nginx

Proxy `/sse` to Murmur and disable buffering so events reach subscribers as they arrive. Set `MURMUR_PUBLIC_URL` to the external base URL. With the `http` driver, point `MURMUR_PUBLISH_URL` at the internal server address.

```nginx
location = /sse {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_read_timeout 1h;
}
```

### Supervisor

Run `murmur:serve` under Supervisor to start it at boot and restart it after an exit. Adjust the paths and application user for your deployment. With `ext-pcntl` enabled in CLI PHP, Murmur closes its connections and stops the event loop on `SIGTERM` or `SIGINT`.

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

Use Redis 6.2 or later to share live events and replay history across Murmur nodes. Set `MURMUR_DRIVER=redis` and the same `MURMUR_SECRET` in the Laravel application and on every Murmur node. Point each node at the shared Redis instance and database with `MURMUR_REDIS_URL`.

```dotenv
MURMUR_DRIVER=redis
MURMUR_REDIS_URL=redis://redis.internal:6379/0
MURMUR_PUBLIC_URL=https://events.example.com
```

Configure the Laravel connection named by `MURMUR_REDIS_CONNECTION` to reach the same Redis instance and database. Laravel appends events to the stream using that connection. Both sides use `database.redis.options.prefix` followed by `murmur.redis.stream` as the stream key. Keep both settings identical in the app and on every node, and enter the stream name without the prefix in `murmur.redis.stream`.

Run `php artisan murmur:serve` on each node and route `/sse` through a load balancer with response buffering disabled. Set `MURMUR_PUBLIC_URL` to the load balancer's public base URL. Each node reads the same stream, so a reconnect can resume on any node while its cursor is retained. The Redis driver disables the server's `POST /publish` endpoint and leaves `MURMUR_PUBLISH_URL` unused.

Replay history is one global log shared by all topics. Redis trims it approximately to `murmur.server.log_size` entries. During a Redis outage, each server retries its stream read one second after a failure. PHP's error log is the only source of any underlying PHP error messages during these retries.

## Version 1 limits

- Murmur supports SSE subscriptions. WebSockets and Laravel presence channels are unsupported.
- The `http` driver runs on a single Murmur node and keeps its bounded history in memory. Restarting that node clears its history. The `redis` driver supports multiple Murmur nodes with a shared history in Redis.
- With the `http` driver and the default cursor margin, a stream created during the server's first second may receive a spurious `murmur.gap` event because its initial cursor predates the in-memory history.
