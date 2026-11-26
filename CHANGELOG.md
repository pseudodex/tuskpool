# Changelog

All notable changes to the public API (`__all__`). Format: [Keep a Changelog](https://keepachangelog.com), versions follow semver.

## Unreleased

### Changed
- `TaskPool.map` and `TaskPool.as_completed` are annotated as `AsyncGenerator`, so `contextlib.aclosing()` type-checks. Runtime behaviour is unchanged.
