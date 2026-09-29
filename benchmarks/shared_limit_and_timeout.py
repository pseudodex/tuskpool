import asyncio
import time
from collections.abc import AsyncIterator

from aiostream import stream

from tuskpool import JobTimeout, TaskPool

LIMIT = 10


async def peak_concurrency(run_two_streams) -> int:
    running = peak = 0

    async def job(x: int) -> int:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.005)
        running -= 1
        return x

    await run_two_streams(job)
    return peak


async def two_aiostream_streams(job) -> None:
    async def drain(results: AsyncIterator[int]) -> None:
        async for _ in results:
            pass

    async def run() -> None:
        first = stream.map(stream.iterate(range(200)), job, task_limit=LIMIT).stream()
        second = stream.map(stream.iterate(range(200)), job, task_limit=LIMIT).stream()
        async with first as a, second as b:
            await asyncio.gather(drain(a), drain(b))

    await run()


async def two_tuskpool_streams(job) -> None:
    async def drain(pool: TaskPool) -> None:
        async for _ in pool.as_completed(job, range(200)):
            pass

    async with TaskPool(LIMIT) as pool:
        await asyncio.gather(drain(pool), drain(pool))


async def stuck_job(x: int) -> int:
    await asyncio.sleep(30 if x == 0 else 0.2)
    return x


async def seconds_until_stuck_job_stops_aiostream() -> float:
    started = time.perf_counter()
    mapped = stream.map(stream.iterate(range(20)), stuck_job, task_limit=5, ordered=False)
    try:
        async with stream.timeout(mapped, 0.5).stream() as results:
            async for _ in results:
                pass
    except TimeoutError:
        pass
    return time.perf_counter() - started


async def seconds_until_stuck_job_stops_tuskpool() -> float:
    started = time.perf_counter()
    try:
        async with TaskPool(5, timeout=0.5) as pool:
            async for _ in pool.as_completed(stuck_job, range(20)):
                pass
    except JobTimeout:
        pass
    return time.perf_counter() - started


async def main() -> None:
    print(f"Two concurrent streams, limit {LIMIT}: peak concurrency")
    print(f"  aiostream (task_limit={LIMIT} per stream): {await peak_concurrency(two_aiostream_streams)}")
    print(f"  tuskpool (one pool of {LIMIT}):            {await peak_concurrency(two_tuskpool_streams)}")
    print("One stuck job among fast ones, timeout 0.5 s: seconds until it is stopped")
    print(f"  aiostream `timeout` operator: {await seconds_until_stuck_job_stops_aiostream():.1f}")
    print(f"  tuskpool `timeout=0.5`:       {await seconds_until_stuck_job_stops_tuskpool():.1f}")


asyncio.run(main())
