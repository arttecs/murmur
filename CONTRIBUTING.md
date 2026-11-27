# Contributing to Murmur

Thank you for considering a contribution. This document explains how to report problems, propose changes, and get a pull request merged.

## Reporting bugs and requesting features

- Search the [existing issues](https://github.com/arttecs/murmur/issues) first.
- Open a bug report with the versions involved (Murmur, PHP, Laravel, Redis, event loop extension), the relevant configuration without secrets, and the smallest reproduction you can produce.
- Open a feature request by describing the problem you are trying to solve before the solution you have in mind.
- **Do not open a public issue for a security vulnerability.** Follow the [security policy](SECURITY.md).

## Development setup

The repository holds two packages: the PHP package at the root and the browser client in `js/`.

Requirements: PHP 8.3 or later with `pcntl` and `posix`, Composer, a Redis 7 server on `127.0.0.1:6379`, Node.js (current LTS), and Docker for the container smoke test. Linux and macOS are supported; Windows is not.

```sh
git clone https://github.com/arttecs/murmur.git
cd murmur
composer install

cd js
npm ci
```

## Running the checks

Every pull request must pass the same checks that CI runs.

```sh
composer test
composer lint -- --test
sh tests/Docker/smoke.sh
```

```sh
cd js
npm run typecheck
npm test
npm run test:integration
npm run build
```

`composer lint` without `--test` fixes the style in place. The JavaScript integration tests start a real `murmur:serve` process, so run `composer install` at the root first.

CI runs the PHP suite on PHP 8.3, 8.4, and 8.5 with Laravel 12 and 13, with both Redis clients, and on the `ext-uv`, `ext-ev`, and `ext-event` loops. A change that touches timers, shutdown, or socket handling should be considered against all of them: extension loops can fire a timer slightly early. `tests/Support/EarlyTimerLoop.php` reproduces that on any loop. On `ext-uv`, a timer cancelled after it became due can still run: repeat work with `Arttecs\Murmur\Server\PeriodicTimer` instead of `addPeriodicTimer()`, and make a one-shot callback check that it is still the current timer before acting. `tests/Support/ReadyTimerLoop.php` reproduces that on any loop.

## Coding standards

- **Style.** PHP follows the Laravel preset of [Pint](https://laravel.com/framework/docs/pint). TypeScript is strict and ESM only.
- **Documentation in code.** Every class, method, property, and constant in `src/` has a docblock; every exported or module-level TypeScript symbol has TSDoc. Inline comments are not used anywhere: if code needs a comment to be understood, rename or restructure it, or say it in the docblock.
- **Visibility.** Use `protected`, not `private`, so applications can extend the package. Mark what is not public API with `@internal`.
- **Framework boundaries.** Nothing under `src/Server/`, `src/Contracts/`, `src/Exceptions/`, or `src/Testing/` imports `Illuminate\*`. The server must stay usable without Laravel.
- **The event loop.** No blocking I/O and no application code runs inside the server's event loop. Work that may block goes through the worker process.
- **Secure by default.** New endpoints, events, and options are closed or disabled by default and configurable through `config/murmur.php` and environment variables.
- **Tests.** Write the test first. Each behaviour needs a test that fails without the change. Avoid wall-clock upper bounds and fixed sleeps; wait for the condition.
- **Public API.** Classify new public types in `tests/Unit/PublicSurfaceTest.php` and update the [compatibility guide](https://murmur.arttecs.com/docs/compatibility/) when the public API changes.

## Documentation

User-facing changes to behaviour, configuration, or public API need a matching pull request in [arttecs/murmur-docs](https://github.com/arttecs/murmur-docs) against the branch for the affected version. Wire protocol changes must update the [wire protocol](https://murmur.arttecs.com/docs/wire-protocol/) page in that pull request. Keep release notes in this repository's [changelog](CHANGELOG.md).

## Commits and pull requests

- Use [Conventional Commits](https://www.conventionalcommits.org/) in English: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`, `ci:`, with a `(js)` scope for the browser client and `!` for a breaking change.
- Keep one logical change per commit and per pull request.
- Add a line to the `Unreleased` section of [`CHANGELOG.md`](CHANGELOG.md) for anything a user of the package can notice.
- Link the matching documentation pull request when behaviour, configuration, or public API changes.
- Open the pull request against `main` and fill in the template. A maintainer reviews it once CI is green.

## Releasing

Maintainers release from `main`:

1. Set the same version in `js/package.json` and `js/package-lock.json`, and rename the `Unreleased` section of [`CHANGELOG.md`](CHANGELOG.md) to `<version> - <date>`, leaving an empty `Unreleased` section above it.
2. Merge that change, then tag the merge commit as `v<version>` and push the tag.

The `Release` workflow publishes `@arttecs/murmur` to npm and creates the GitHub release from the changelog entry. Packagist picks the tag up on its own.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
