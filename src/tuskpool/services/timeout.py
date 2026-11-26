import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from tuskpool.errors import JobTimeout


R = TypeVar("R")


# Takes a callable, not an awaitable: the job must be called inside the time limit.
async def run_with_timeout(job: Callable[[], Awaitable[R]], limit: float | None) -> R:
    if limit is None:
        return await job()

    timer = asyncio.timeout(limit)

    try:
        async with timer:
            return await job()

    except TimeoutError:
        if timer.expired():
            msg = f"job exceeded {limit}s"
            raise JobTimeout(msg) from None

        raise  # the job's own TimeoutError
