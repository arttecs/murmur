# Changelog

## Unreleased

### Added

- ReactPHP SSE server with live subscriptions, heartbeats, bounded event replay, and graceful shutdown.
- Laravel bridge with broadcasting, private channel authorization, and a stream API.
- JavaScript client for subscribing to channels and consuming streams in the browser.
- Redis Streams driver for publishing from Laravel and sharing event history across Murmur nodes.
- Environment variables for all package settings, including Redis timing, shutdown grace, and an option to disable the Laravel authorization route.
- Custom publisher and event log factories registered through `Murmur::extend()`.
- Per-process Prometheus and JSON metrics, served only with a bearer token configured, plus `Murmur::metrics()` and `murmur:status`.
- PSR-3 logging through an optional Laravel log channel for delivery errors and server lifecycle messages.
- JavaScript client options for retry delays, jitter, token refresh margin, and stream failure limits, with per-stream overrides.
- JavaScript client authorization headers, credentials mode, custom authorizers, and credentialed SSE connections.
- JavaScript client statistics through `stats()`.

### Changed

- `Contracts\EventLog` gained `onGap()` and `ready()` during pre-release development. Custom event logs must implement both methods.
