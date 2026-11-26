# Changelog

## Unreleased

### Added

- ReactPHP SSE server with live subscriptions, heartbeats, bounded event replay, and graceful shutdown.
- Laravel bridge with broadcasting, private channel authorization, and a stream API.
- JavaScript client for subscribing to channels and consuming streams in the browser.
- Redis Streams driver for publishing from Laravel and sharing event history across Murmur nodes.
- Environment variables for all package settings, including Redis timing, shutdown grace, and an option to disable the Laravel authorization route.
- Custom publisher and event log factories registered through `Murmur::extend()`.
- Per-process Prometheus and JSON metrics with optional bearer authentication, plus `Murmur::metrics()` and `murmur:status`.
- PSR-3 logging through an optional Laravel log channel for delivery errors and server lifecycle messages.

### Changed

- `Contracts\EventLog` gained `onGap()` and `ready()` during pre-release development. Custom event logs must implement both methods.

### Tests

- Cover the once-per-outage Redis gap notification across repeated recovery failures.
