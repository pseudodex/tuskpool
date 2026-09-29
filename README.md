# tuskpool

An asyncio task pool with capped concurrency and bounded memory.

## Contents

- [Why](#why)
- [Install](#install)
- [Quick start](#quick-start)
- [API](#api)
- [How memory stays bounded](#how-memory-stays-bounded)
- [Behavior worth knowing](#behavior-worth-knowing)
  - [Errors](#errors)
  - [Stopping early](#stopping-early)
  - [Timeouts](#timeouts)
  - [Counters](#counters)
  - [Don't nest](#dont-nest)
- [Compared with other libraries](#compared-with-other-libraries)
  - [Measured behavior](#measured-behavior)
  - [Features](#features)
  - [Choosing between tuskpool and aiostream](#choosing-between-tuskpool-and-aiostream)
- [Development](#development)
- [License](#license)

## Why

Plain `asyncio.gather` over a big list creates every task up front. A `TaskGroup` with a `Semaphore` still creates every task before the semaphore lets it run. This pool takes a slot *before* it pulls the next item, so a huge (or endless) input never sits in memory.

- Zero dependencies, standard library only
- Python 3.11 or newer
- Fully typed (`py.typed`); result types are inferred from the function you pass
- Sync and async iterables both work as input

## Install

With pip:

```bash
pip install tuskpool
```

or with [uv](https://docs.astral.sh/uv/):

```bash
uv add tuskpool
```

To work on the library itself, see [Development](#development).

## Quick start

```python
import asyncio
from tuskpool import TaskPool

async def fetch(url: str) -> bytes: ...

async def main() -> None:
    async with TaskPool(max_workers=10, timeout=5) as pool:
        # Results as they finish
        async for page in pool.as_completed(fetch, urls):
            save(page)

        # Results in input order
        async for page in pool.map(fetch, urls):
            save(page)

        # A single job
        task = await pool.create_task(fetch, "https://example.com")
        page = await task

asyncio.run(main())
```

`urls` can be a list, a generator, or an async generator. It is read lazily.

## API

| Member | What it does |
|---|---|
| `TaskPool(max_workers, timeout=None)` | Create a pool. `timeout` is per job, in seconds. See [Timeouts](#timeouts). |
| `await pool.create_task(fn, *args, **kwargs)` | Start one job once a slot is free. Returns the `asyncio.Task`. |
| `pool.map(fn, iterable)` | Async iterator of results, in input order. See [Errors](#errors). |
| `pool.as_completed(fn, iterable)` | Async iterator of results, in the order jobs finish. |
| `await pool.join()` | Wait until no job is running. The pool stays open. |
| `await pool.shutdown(wait=True, cancel_futures=False)` | Refuse new jobs. Waits for running jobs unless `wait=False`; cancels them if `cancel_futures=True`. |
| `async with pool:` | On exit, waits for running jobs. If the block raised, cancels them instead. |
| `pool.submitted`, `.completed`, `.failed`, `.timed_out` | Counters. See [Counters](#counters). |
| `pool.active` | Jobs currently alive. |
| `JobTimeout` | Raised for a job that exceeded `timeout`. Subclass of `TimeoutError`. |

## How memory stays bounded

- A slot is taken before the next item is pulled from your iterable.
- `map` and `as_completed` never run more than `max_workers` items ahead of the code consuming them. A slow consumer slows the pool down; results do not pile up.
- The pool never keeps results. `create_task` returns a `Task`, and a finished task holds its result, so don't keep millions of them.

## Behavior worth knowing

### Errors

In `map` and `as_completed` the first failure is raised to the consumer and the remaining jobs are cancelled. `map` raises it only once every earlier item has been yielded, so a stuck earlier item delays the error. Every failed job is also logged at `ERROR` on the `tuskpool.pool` logger.

### Stopping early

`break` does not close an async generator immediately; the remaining jobs are cancelled a tick later. To cancel right away, wrap the stream:

```python
from contextlib import aclosing

async with aclosing(pool.map(fetch, urls)) as results:
    async for page in results:
        if done(page):
            break
```

### Timeouts

`timeout` limits each job, not the whole run, and does not count time spent waiting for a slot. A job's own `TimeoutError` is a normal failure, not a `JobTimeout`.

### Counters

They are updated one event-loop iteration after a job ends. `join()` and `shutdown()` guarantee they are current when they return.

### Don't nest

Don't use a pool from inside one of its own jobs. Every slot can end up held by a job that is waiting for a slot.

## Compared with other libraries

### Measured behavior

The same job (1 ms of `asyncio.sleep`) was run 2,000 times with `max_workers` / `max_at_once` / `task_limit` set to 10, and a consumer that takes 3 ms per result, so it is slower than the jobs. The last column is the most jobs that had started but whose results had not yet been consumed. A memory-bounded library keeps it near the limit.

| Call | Accepts a generator | Peak concurrency | Jobs started ahead of the consumer |
|---|---|---|---|
| tuskpool `TaskPool.map` | yes | 10 | 10 |
| tuskpool `TaskPool.as_completed` | yes | 10 | 10 |
| aiostream `stream.map` (ordered) | yes | 10 | 10 |
| aiostream `stream.map` (unordered) | yes | 10 | 10 |
| aiometer `amap` | no, needs a list | 10 | 1900 |
| aiometer `run_on_each` | yes | 10 | n/a (no results) |

- All of them respect the concurrency limit.
- `aiometer.amap` takes a `Sequence` (it raised `TypeError` on a generator), so the whole input must be in memory. With a list it ran almost the whole input ahead of the slow consumer, and the finished results waited in memory.
- `aiometer.run_on_each` read a generator lazily here, but its signature says `Sequence`, so that is not guaranteed.
- The numbers were measured with aiometer 1.0.0 and aiostream 0.8.1 on Python 3.13. To reproduce them:

  ```bash
  pip install -e ".[bench]"   # or: uv sync --extra bench
  python benchmarks/compare.py
  ```

### Features

| | tuskpool | aiostream | aiometer |
|---|---|---|---|
| Memory-bounded with a slow consumer | yes | yes | no (`amap`) |
| Per-job timeout | `timeout=` | a separate `timeout` operator | no |
| Rate limit | no | a separate `spaceout` operator | `max_per_second` |
| Job counters (`completed`, `failed`, ...) | yes | no | no |
| Runs on | asyncio | asyncio | asyncio and trio (via anyio) |
| License | MIT | GPL-3.0-or-later | MIT |
| Dependencies | none | `typing-extensions` | `anyio` (and `exceptiongroup` before Python 3.11) |
| Python | 3.11+ | 3.10+ | 3.8+ |

### Choosing between tuskpool and aiostream

aiostream is the closest alternative. Both keep memory bounded the same way, and on failure both cancel the running jobs and close the input, so those are not reasons to pick one over the other. What differed in testing:

| | tuskpool | aiostream |
|---|---|---|
| Two streams running at once, limit 10 | at most 10 jobs run: the limit belongs to the pool | 20 jobs run: `task_limit` belongs to each stream |
| One stuck job, 0.5 s timeout | stopped after 0.5 s: the timeout is per job | stopped after 1.5 s: the `timeout` operator waits between results, so it fired 0.5 s after the last other job finished |

Pick **tuskpool** when:

- You want one limit for a whole program, such as a rate-limited API or a database with few connections. Create one pool and use it from every call site. One-off jobs (`create_task`) and streams (`map`, `as_completed`) all count against the same limit.
- A stuck job must be stopped on its own clock, and you want to tell timeouts (`JobTimeout`) from other failures.
- You want a pool you can manage: `join()`, `shutdown(cancel_futures=True)`, `async with`, and counters.
- You want no dependencies and a small API: one class.

Pick **aiostream** when:

- You need more than a bounded map. It has many stream operators (merge, zip, chunk, filter and more) that you can chain into pipelines.
- You need Python 3.10, or you want a library that has been around longer. aiostream has had about 30 releases; tuskpool is new.
- A limit per stream is what you want.

The licenses also differ: tuskpool is MIT, so it can be used in closed-source projects. aiostream is GPL-3.0-or-later, which is copyleft, so check that it fits how you distribute your code.

To reproduce the two rows above:

```bash
python benchmarks/shared_limit_and_timeout.py   # needs the bench extra
```

## Development

```bash
git clone https://github.com/pseudodex/tuskpool.git
cd tuskpool
pip install -e ".[dev]"   # or: uv sync --extra dev
pytest                    # 124 tests
pytest --cov=tuskpool --cov-branch
mypy                      # strict
python benchmarks/compare.py                    # needs the bench extra
python benchmarks/shared_limit_and_timeout.py  # needs the bench extra
```

The suite is run on Python 3.11, 3.12 and 3.13. Behavior of `asyncio.gather` changed in 3.12, so run it on more than one version if you change the waiting code.

## License

[MIT](https://github.com/pseudodex/tuskpool/blob/main/LICENSE)
