# Architecture decisions

Append a new entry when you make an architectural decision (new dependency, new layer or package, change to a port, switch to migrations, ...). Newest last. Keep entries to a few lines: what, why, consequence.

## Layered architecture with enforced dependency direction
pool -> services -> domain | errors, checked by `lint-imports`. Business rules live in `domain/` and `services/` so they can be tested without any framework.

## Project kind: library
Published for other code to import. The public API is `__all__` in `__init__.py`; no I/O, environment reads or logging configuration in library code; versioned with semver and a CHANGELOG.

## Concurrency: async
I/O-bound work with concurrent operations. Ports, services and adapters that do I/O are `async def`; pure domain code stays synchronous. Switching back to sync touches every port, service and test.

## Fixed project policies
- 100% line and branch coverage, enforced in `pyproject.toml`.
- Ruff (format and lint), pyright strict, import-linter contracts, pre-commit.
- Standard-library first; new dependencies need an entry here.

## Deviation: Python 3.11 is still supported
The template targets 3.12+. Released versions support 3.11 and dropping it would break users, so ruff and pyright target 3.11 (no PEP 695 syntax) and CI tests 3.11 to 3.13.

## Deviation: no `adapters/` layer
The pool does no external I/O, so there are no ports or adapters. `errors` sits beside `domain` below `services`. Add `adapters/` with the first external dependency.

## Deviation: compatibility with released names
- `tuskpool.pool` re-exports the public classes so old imports keep working.
- `services/task_pool.py` logs to `tuskpool.pool`, not `__name__`, because the README documents that logger.
- `JobTimeout` subclasses `TimeoutError` only: there is one error, so no `AppError` base, and the name keeps no `Error` suffix. `max_workers` and `timeout` still raise `ValueError`.
- `shutdown(wait=...)` stays a positional boolean.
- `TaskPool` keeps its docstrings (it is the public documentation) and stays one class over the usual size limit of ~300 lines per module.

## Extra tooling
`pytest-timeout` (30 s) and `filterwarnings = error` are kept so a deadlock or a leaked warning fails the run. The `bench` extra stays an optional dependency because the README documents installing it. Benchmarks may `print`.
