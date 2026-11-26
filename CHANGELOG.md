# Changelog

## Unreleased

### Added

- ReactPHP SSE server with live subscriptions, heartbeats, bounded event replay, and graceful shutdown.
- Laravel bridge with broadcasting, private channel authorization, and a stream API.
- JavaScript client for subscribing to channels and consuming streams in the browser.
- Redis Streams driver for publishing from Laravel and sharing event history across Murmur nodes.
- Environment variables for all package settings, including Redis timing, shutdown grace.
- Custom publisher and event log factories registered through `Murmur::extend()`.
- Non-blocking custom subscription authorizers registered through `Murmur::authorizeSubscriptionsUsing()`, with a PHPUnit conformance suite.
- Per-process server metrics read through `Server::metrics()`, with `snapshot()`, `value()`, and `toPrometheus()`. No metrics route is served.
- Opt-in persistence for per-node metrics snapshots in database, Redis, cache, or custom stores registered with `Murmur::extendMetrics()`.
- One long-lived Artisan writer per metrics-enabled server, with configurable intervals, acknowledgement timeouts, node names, and metric filters.
- `Murmur::metrics()`, `Murmur::metricTotals()`, and the `murmur:status` and `murmur:forget-metrics` commands for reading and managing stored snapshots.
- `Murmur::serving()` to receive the server and its event loop when `murmur:serve` starts.
- PSR-3 logging through an optional Laravel log channel for delivery errors and server lifecycle messages.
- JavaScript client options for retry delays, jitter, token refresh margin, and stream failure limits, with per-stream overrides.
- JavaScript client authorization headers, credentials mode, custom authorizers, and credentialed SSE connections.
- JavaScript client statistics through `stats()`.
- Laravel Echo connector at `@arttecs/murmur/echo` for public and private channels, notifications, and connection status.
- Event loop lag metrics and rate-limited warnings, plus a startup notice and socket-limit check for `stream_select`.
- `GET /ready` for live event log readiness and staggered graceful shutdown with SSE reconnect hints.
- `murmur:install` to publish configuration and set the application's environment values for Murmur.
- SSE connection IDs and sender exclusion through Laravel's `toOthers()` and `Murmur::publish()`.
- A `murmur.attached` control frame that advances the cursor to the sampled tail after replay and acknowledges newly joined topics.
- Protocol 1 documentation and the `X-Murmur-Protocol` response header on subscriptions and probes.
- `Murmur::fake()` and PHPUnit conformance suites for event logs and metrics stores.
- Guides for migrating from Reverb, writing drivers, testing applications, and operating Murmur.

### Changed

- Private channel authorization uses Laravel's broadcasting route and issues one token per channel.
- SSE subscriptions can combine several tokens and skip replay for newly joined topics.
- `Contracts\EventLog` gained `onGap()`, `ready()`, and `healthy()` during pre-release development. Custom event logs must implement all three methods. The `since()` method accepts a filter and returns a replay bounded by the sampled tail.
- Event log drivers must deliver events in strictly increasing identifier order across all topics and sample tails that cover events already delivered to listeners. The conformance suite checks both guarantees for numbered heartbeats.
- Murmur supplies the Laravel broadcast connection by default and marks classes outside the supported PHP surface as internal.

### Fixed

- Cursorless connections sample the log tail so quiet subscribers can replay from their attach point when it has an ID.
- Custom authorizer grants are validated, duplicate tokens are verified once per request, and distant expiries avoid scheduling timers outside the supported range.
- Conformance suites share a bounded ReactPHP loop waiter and exercise replay snapshots during appends.
- Origin matching normalizes explicit HTTP and HTTPS default ports. The installer derives origins from HTTP and HTTPS application URLs and explains how to add a loopback development port.
