# Changelog

All notable changes to this project are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Publish one event to multiple topics with `Murmur::publishToTopics()` or a signed HTTP request containing `topics`. Configure the HTTP batch limit with `MURMUR_MAX_TOPICS_PER_PUBLISH` (default 100).
- Schedule PHP cycle collection with `MURMUR_GC_INTERVAL` and report collected cycles in `murmur_gc_collected_cycles_total`.
- Configure the listen backlog with `MURMUR_BACKLOG` (default 4096) to handle bursts of connections.

### Changed

- Batch Laravel broadcasts across distinct channels in one HTTP request or Redis command. A channel listed twice in one broadcast is published once. HTTP publishers split a batch that exceeds the server's topic or payload limit and fall back to individual requests when an older server rejects the batch. Redis publishers need permission for `EVAL` in addition to `XADD`.
- Spread SSE heartbeat writes across the configured heartbeat interval to reduce event-loop stalls.
- In-memory replay uses a topic index to find matching events after a reconnect cursor.

## 0.1.3 - 2026-11-27

### Changed

- The documentation has moved to the [Murmur website](https://murmur.arttecs.com/docs/) and is maintained in [arttecs/murmur-docs](https://github.com/arttecs/murmur-docs).

## 0.1.2 - 2026-11-27

### Fixed

- On `ext-uv`, a timer cancelled after it was already due could still run. Murmur now ignores such timers: the worker no longer crashes the server after it is reaped, closed servers stop sending heartbeats and sampling lag, a finished request's deadline no longer closes its SSE stream, and a closed Redis log no longer retries or disconnects.

## 0.1.1 - 2026-11-27

### Changed

- The readmes of the package and of the JavaScript client show the Murmur logo.

## 0.1.0 - 2026-11-27

### Added

- ReactPHP SSE server with live subscriptions, heartbeats, bounded event replay, and graceful shutdown.
- Laravel bridge with broadcasting, private channel authorization, and a stream API.
- JavaScript client for subscribing to channels and consuming streams in the browser.
- Redis Streams driver for publishing from Laravel and sharing event history across Murmur nodes.
- Environment variables for all package settings, including Redis timing, shutdown grace, and lifecycle acknowledgement timeout.
- Custom publisher and event log factories registered through `Murmur::extend()`.
- Non-blocking custom subscription authorizers registered through `Murmur::authorizeSubscriptionsUsing()`, with a PHPUnit conformance suite.
- Per-process server metrics read through `Server::metrics()`, with `snapshot()`, `value()`, and `toPrometheus()`. No metrics route is served.
- Opt-in persistence for per-node metrics snapshots in database, Redis, cache, or custom stores registered with `Murmur::extendMetrics()`.
- One long-lived Artisan worker per server with metrics persistence or lifecycle events enabled.
- Opt-in Laravel `ConnectionOpened` and `ConnectionClosed` events, including close reasons, with bounded best-effort delivery outside the SSE event loop.
- `murmur_lifecycle_sent_total` and `murmur_lifecycle_dropped_total` counters.
- `Murmur::metrics()`, `Murmur::metricTotals()`, and the `murmur:status` and `murmur:forget-metrics` commands for reading and managing stored snapshots.
- `Murmur::serving()` to receive the server and its event loop when `murmur:serve` starts.
- PSR-3 logging through an optional Laravel log channel for delivery errors and server lifecycle messages.
- JavaScript client options for retry delays, jitter, token refresh margin, and stream failure limits, with per-stream overrides.
- JavaScript client authorization headers, credentials mode, custom authorizers, and credentialed SSE connections.
- JavaScript client statistics through `stats()`.
- Laravel Echo connector at `@arttecs/murmur/echo` for public and private channels, notifications, and connection status.
- JavaScript connection IDs through `socketId()` and `stats().connectionId`, used by the Echo connector for `X-Socket-ID` and `toOthers()`.
- JavaScript `attached` and `error` client events, `reauthorize()`, per-channel authorization errors, and a read-only `stream.lastEventId` for resuming independent streams.
- Event loop lag metrics and rate-limited warnings, plus a startup notice and socket-limit check for `stream_select`.
- `GET /ready` for live event log readiness and staggered graceful shutdown with SSE reconnect hints.
- `murmur:install` to publish configuration and set the application's environment values for Murmur.
- SSE connection IDs and sender exclusion through Laravel's `toOthers()` and `Murmur::publish()`. Clients can reuse an ID across reconnects, and several streams can share it.
- A `murmur.attached` control frame that advances the cursor to the sampled tail after replay and acknowledges newly joined topics.
- Protocol 1 documentation and the `X-Murmur-Protocol` response header on subscriptions and probes.
- `Murmur::fake()` and PHPUnit conformance suites for event logs and metrics stores.
- Guides for migrating from Reverb, writing drivers, testing applications, and operating Murmur.

### Changed

- Topic names are validated independently of the process locale. They must be 1–200 bytes of valid UTF-8 without commas, ASCII whitespace, C0 control characters, or DEL.
- Private channel authorization uses Laravel's broadcasting route and issues one token per channel.
- The JavaScript client authorizes each private channel through `/broadcasting/auth`, isolates denied channels, retries throttled authorization with `Retry-After`, and budgets the stream URL for its largest reconnect.
- The JavaScript client marks newly joined channels as fresh until the server attaches them, follows numbered heartbeats, backs off with jitter until a stream attaches, and expires independent streams after eight attempts without progress.
- SSE subscriptions can combine several tokens and skip replay for newly joined topics.
- `Contracts\EventLog` gained `onGap()`, `ready()`, and `healthy()` during pre-release development. Custom event logs must implement all three methods. The `since()` method accepts a filter and returns a replay bounded by the sampled tail.
- Event log drivers must deliver events in strictly increasing identifier order across all topics and sample tails that cover events already delivered to listeners. The conformance suite checks both guarantees for numbered heartbeats.
- Murmur supplies the Laravel broadcast connection by default and marks classes outside the supported PHP surface as internal.

### Fixed

- Lifecycle listener failures no longer stop delivery of later events in the same batch.
- Worker acknowledgements use a dedicated pipe so application output reaches the server log.
- Lifecycle delivery flushes queued batches through the shutdown grace period and counts unacknowledged records when the worker stops.
- The worker drops malformed lifecycle records individually, reports their count, and dispatches the remaining records in order.
- Full lifecycle batches flush on the next event-loop tick when the worker is idle.
- Murmur detects worker exit even when a descendant inherits the acknowledgement pipe, closes its pipe watchers before reaping, and logs lifecycle drops at shutdown.
- The worker resets database query duration tracking and peak memory usage between messages.
- Cursorless connections sample the log tail so quiet subscribers can replay from their attach point when it has an ID.
- Custom authorizer grants are validated, duplicate tokens are verified once per request, and distant expiries avoid scheduling timers outside the supported range.
- Conformance suites share a bounded ReactPHP loop waiter and exercise replay snapshots during appends.
- Origin matching normalizes explicit HTTP and HTTPS default ports. The installer derives origins from HTTP and HTTPS application URLs and explains how to add a loopback development port.
