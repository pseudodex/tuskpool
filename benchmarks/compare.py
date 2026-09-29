import asyncio
import time
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

import aiometer
from aiostream import stream

from tuskpool import TaskPool

ITEMS = 2000
LIMIT = 10
CONSUMER_DELAY = 0.003


class Probe:
    def __init__(self) -> None:
        self.pulled = self.started = self.consumed = self.running = 0
        self.peak = self.jobs_ahead = self.items_ahead = 0

    def generator(self) -> Iterator[int]:
        for i in range(ITEMS):
            self.pulled += 1
            self.items_ahead = max(self.items_ahead, self.pulled - self.consumed)
            yield i

    async def job(self, x: int) -> int:
        self.started += 1
        self.running += 1
        self.peak = max(self.peak, self.running)
        self.jobs_ahead = max(self.jobs_ahead, self.started - self.consumed)
        await asyncio.sleep(0.001)
        self.running -= 1
        return x

    async def consume_slowly(self, results: Any) -> None:
        async for _ in results:
            self.consumed += 1
            await asyncio.sleep(CONSUMER_DELAY)


async def tuskpool_map(probe: Probe, source: Any) -> None:
    async with TaskPool(LIMIT) as pool:
        await probe.consume_slowly(pool.map(probe.job, source))


async def tuskpool_as_completed(probe: Probe, source: Any) -> None:
    async with TaskPool(LIMIT) as pool:
        await probe.consume_slowly(pool.as_completed(probe.job, source))


def aiostream_map(ordered: bool) -> Callable[[Probe, Any], Awaitable[None]]:
    async def run(probe: Probe, source: Any) -> None:
        mapped = stream.map(stream.iterate(source), probe.job, task_limit=LIMIT, ordered=ordered)
        async with mapped.stream() as results:
            await probe.consume_slowly(results)

    return run


async def aiometer_amap(probe: Probe, source: Any) -> None:
    async with aiometer.amap(probe.job, source, max_at_once=LIMIT) as results:
        await probe.consume_slowly(results)


async def aiometer_run_on_each(probe: Probe, source: Any) -> None:
    async def job(x: int) -> None:
        await probe.job(x)
        probe.consumed += 1

    await aiometer.run_on_each(job, source, max_at_once=LIMIT)


CASES: list[tuple[str, Callable[[Probe, Any], Awaitable[None]], bool]] = [
    ("tuskpool `TaskPool.map`", tuskpool_map, True),
    ("tuskpool `TaskPool.as_completed`", tuskpool_as_completed, True),
    ("aiostream `stream.map` (ordered)", aiostream_map(True), True),
    ("aiostream `stream.map` (unordered)", aiostream_map(False), True),
    ("aiometer `amap`", aiometer_amap, True),
    ("aiometer `run_on_each`", aiometer_run_on_each, False),
]


async def run_case(run: Callable[[Probe, Any], Awaitable[None]], make_source: Callable[[Probe], Any]) -> Probe:
    probe = Probe()
    await run(probe, make_source(probe))
    return probe


async def main() -> None:
    print(f"{ITEMS} items, limit {LIMIT}, consumer {CONSUMER_DELAY * 1000:.0f} ms per result\n")
    print("| Call | Accepts a generator | Peak concurrency | Jobs started ahead of the consumer |")
    print("|---|---|---|---|")
    for name, run, has_results in CASES:
        accepts_generator = True
        try:
            probe = await run_case(run, lambda p: p.generator())
        except TypeError:
            accepts_generator = False
            probe = await run_case(run, lambda p: list(range(ITEMS)))
        ahead = str(probe.jobs_ahead) if has_results else "n/a (no results)"
        print(f"| {name} | {'yes' if accepts_generator else 'no, needs a list'} | {probe.peak} | {ahead} |")


asyncio.run(main())
