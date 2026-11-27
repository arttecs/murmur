## What and why

<!-- What this changes and the problem it solves. Link the issue if there is one. -->

## How it was verified

<!-- Tests added or changed, and anything you checked by hand. -->

## Checklist

- [ ] `composer test` and `composer lint -- --test` pass.
- [ ] For changes under `js/`: `npm run typecheck`, `npm test`, and `npm run test:integration` pass.
- [ ] New behaviour has a test that fails without the change.
- [ ] `CHANGELOG.md` has an entry under `Unreleased` for user-visible changes.
- [ ] A matching [murmur-docs](https://github.com/arttecs/murmur-docs) pull request is linked for changes to user-facing behaviour, configuration, public API, or the wire protocol.
- [ ] Commits follow Conventional Commits.
