<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/arttecs/murmur/main/art/logo-dark.svg">
    <img src="https://raw.githubusercontent.com/arttecs/murmur/main/art/logo-light.svg" alt="Murmur" width="300">
  </picture>
</p>

[![Tests](https://github.com/arttecs/murmur/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/arttecs/murmur/actions/workflows/tests.yml)
[![Packagist Version](https://img.shields.io/packagist/v/arttecs/murmur)](https://packagist.org/packages/arttecs/murmur)
[![npm Version](https://img.shields.io/npm/v/%40arttecs%2Fmurmur)](https://www.npmjs.com/package/@arttecs/murmur)
[![PHP Version](https://img.shields.io/packagist/dependency-v/arttecs/murmur/php)](https://packagist.org/packages/arttecs/murmur)
[![Documentation](https://img.shields.io/badge/docs-Murmur-blue)](https://murmur.arttecs.com/docs/)
[![License](https://img.shields.io/github/license/arttecs/murmur)](LICENSE)

Murmur is a ReactPHP Server-Sent Events (SSE) server with a Laravel bridge. An SSE subscription can stay open for minutes or hours. When Laravel serves the stream directly through PHP-FPM, that connection holds a worker for its lifetime. Murmur keeps subscriptions in a separate, long-running process. Laravel publishes events over HTTP or Redis and continues serving requests with its workers available.

- **[No held workers.](https://murmur.arttecs.com/docs/)** Subscriptions live in one event-loop process; PHP-FPM and Octane workers stay free.
- **[Laravel broadcasting.](https://murmur.arttecs.com/docs/broadcasting/)** A `murmur` broadcaster, private channels authorized by Laravel's own `/broadcasting/auth`, `toOthers()`, and a [Laravel Echo connector](https://murmur.arttecs.com/docs/laravel-echo/).
- **[Replay on reconnect.](https://murmur.arttecs.com/docs/wire-protocol/#replay)** Clients resume from their last event ID, and are told when history was lost.
- **[Scales out.](https://murmur.arttecs.com/docs/redis/)** Redis Streams share events and history across any number of Murmur nodes.
- **[Closed by default.](https://murmur.arttecs.com/docs/security/)** Signed publishing, an empty origin allowlist, no metrics route, and optional features off until enabled.
- **[Observable.](https://murmur.arttecs.com/docs/metrics/)** Health and readiness endpoints, Prometheus-format metrics, optional persisted snapshots, and connection lifecycle events.

## Requirements

Murmur requires PHP 8.3 or later and Laravel 12 or 13.
The server runs on Linux and macOS; Windows is not supported.

## Installation

```sh
composer require arttecs/murmur
php artisan murmur:install
```

The installer publishes `config/murmur.php` and sets up your application's Murmur environment.

## Quick start

Start the server from your Laravel application directory.

```sh
php artisan murmur:serve
```

Open a public subscription in a separate terminal.

```sh
curl -N 'http://127.0.0.1:8080/sse?topics=news'
```

To subscribe from your browser application, install the client with `npm install @arttecs/murmur` and listen on the `news` channel. If the browser application uses a different origin from the SSE server, add its origin to `MURMUR_ALLOWED_ORIGINS`.

```js
import { Murmur } from '@arttecs/murmur'

const murmur = new Murmur({ url: 'http://127.0.0.1:8080/sse' })

const news = murmur.channel('news')
news.listen('.updated', data => console.log(data))
```

Once the subscription has attached, publish an event from your Laravel application.

```php
use Arttecs\Murmur\Facades\Murmur;

Murmur::publish('news', 'updated', ['title' => 'New release']);
```

## Documentation

Read the [Murmur documentation](https://murmur.arttecs.com/docs/).

- [Installation](https://murmur.arttecs.com/docs/installation/)
- [Quick start](https://murmur.arttecs.com/docs/quick-start/)
- [Configuration](https://murmur.arttecs.com/docs/configuration/)
- [Production](https://murmur.arttecs.com/docs/production/)
- [Security](https://murmur.arttecs.com/docs/security/)
- [Wire protocol](https://murmur.arttecs.com/docs/wire-protocol/)
- [Migrating from Laravel Reverb](https://murmur.arttecs.com/docs/migrate-from-reverb/)
- [Compatibility and public API](https://murmur.arttecs.com/docs/compatibility/)

## Changelog

See the [changelog](CHANGELOG.md).

## Contributing

Send code contributions to this repository and documentation changes to [arttecs/murmur-docs](https://github.com/arttecs/murmur-docs). See the [contributing guide](CONTRIBUTING.md) and [code of conduct](CODE_OF_CONDUCT.md).

## Security

Report vulnerabilities privately following the [security policy](SECURITY.md).

## License

Murmur is released under the [MIT License](LICENSE).
