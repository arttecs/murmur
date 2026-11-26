# Changelog

## Unreleased

### Added

- ReactPHP SSE server with live subscriptions, heartbeats, bounded event replay, and graceful shutdown.
- Laravel bridge with broadcasting, private channel authorization, and a stream API.
- JavaScript client for subscribing to channels and consuming streams in the browser.
- Redis Streams driver for publishing from Laravel and sharing event history across Murmur nodes.
- Environment variables for all package settings, including Redis timing, shutdown grace, and an option to disable the Laravel authorization route.
- Custom publisher and event log factories registered through `Murmur::extend()`.
- Per-process server metrics read through `Server::metrics()`, with `snapshot()`, `value()`, and `toPrometheus()`. No metrics route is served.
- Opt-in persistence for per-node metrics snapshots in database, Redis, cache, or custom stores registered with `Murmur::extendMetrics()`.
- One long-lived Artisan writer per metrics-enabled server, with configurable intervals, acknowledgement timeouts, node names, and metric filters.
- `Murmur::metrics()`, `Murmur::metricTotals()`, and the `murmur:status` and `murmur:forget-metrics` commands for reading and managing stored snapshots.
- `Murmur::serving()` to receive the server and its event loop when `murmur:serve` starts.
- PSR-3 logging through an optional Laravel log channel for delivery errors and server lifecycle messages.
- JavaScript client options for retry delays, jitter, token refresh margin, and stream failure limits, with per-stream overrides.
- JavaScript client authorization headers, credentials mode, custom authorizers, and credentialed SSE connections.
- JavaScript client statistics through `stats()`.
- Event loop lag metrics and rate-limited warnings, plus a startup notice and socket-limit check for `stream_select`.
- `GET /ready` for live event log readiness and staggered graceful shutdown with SSE reconnect hints.
- `murmur:install` to publish configuration and set the application's environment values for Murmur.
- Protocol 1 documentation and the `X-Murmur-Protocol` response header on subscriptions and probes.
- `Murmur::fake()` and PHPUnit conformance suites for event logs and metrics stores.
- Guides for migrating from Reverb, writing drivers, testing applications, and operating Murmur.

### Changed

- `Contracts\EventLog` gained `onGap()`, `ready()`, and `healthy()` during pre-release development. Custom event logs must implement all three methods.
- Murmur supplies the Laravel broadcast connection by default and marks classes outside the supported PHP surface as internal.
