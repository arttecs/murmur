# Security Policy

## Supported Versions

Murmur has not reached 1.0. Security fixes are released for the latest minor version only.

## Reporting a Vulnerability

Please do not report security vulnerabilities through public GitHub issues, discussions, or pull requests.

Report them privately through GitHub: open the [Security tab](https://github.com/arttecs/murmur/security/advisories/new) of this repository and choose "Report a vulnerability".

If you cannot use GitHub's private reporting, email [security@arttecs.com](mailto:security@arttecs.com?subject=%5BMURMUR%5D). The subject must start with `[MURMUR]` so the report is routed correctly.

Include as much of the following as you can:

- the affected version of `arttecs/murmur` or `@arttecs/murmur`, and the PHP, Laravel, and Redis versions in use;
- the relevant configuration (driver, allowed origins, whether metrics persistence or lifecycle events are enabled), without secrets;
- the steps or a proof of concept that reproduces the problem;
- the impact you expect an attacker to achieve.

You should receive an acknowledgement within five working days. Once the report is confirmed, a fix is prepared privately, released, and credited to you in the advisory unless you prefer otherwise.

## Scope

Reports of particular interest:

- bypassing subscription authorization, or reading events from a topic a token does not grant;
- forging or replaying signed `POST /publish` requests;
- cross-origin access that ignores `allowed_origins`;
- denial of service against the server process from an unauthenticated client beyond the documented connection, topic, and buffer limits;
- code execution or information disclosure through the worker process.

Misconfiguration that the documentation warns against (for example exposing the signing secret) is not treated as a vulnerability in the package.
