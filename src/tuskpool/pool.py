import asyncio
import logging
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from types import TracebackType
from typing import Any, ParamSpec, Self, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")


class JobTimeout(TimeoutError):
    """A job ran longer than the pool's `timeout`."""


async def _as_async_iter(items: Iterable[T] | AsyncIterable[T]) -> AsyncGenerator[T, None]:
    """Adapt a sync or async iterable to an async generator, lazily.

    Closing it also closes the wrapped async iterable.
    """
    if not isinstance(items, AsyncIterable):
        for item in items:
            yield item
        return

    iterator = aiter(items)
    try:
        async for item in iterator:
            yield item
    finally:
        if aclose := getattr(iterator, "aclose", None):
            await aclose()


class TaskPool:
    """Run async jobs with a hard cap on how many are alive at once.

    A slot is taken before the next item is pulled from the input, and the pool
    never stores results, so memory stays bounded however large the input is.

    Use it as an async context manager to wait for (or, on error, cancel) the
    remaining jobs on exit.

    Example:
        >>> async with TaskPool(max_workers=10, timeout=5) as pool:
        ...     async for page in pool.as_completed(fetch, urls):
        ...         save(page)

    Attributes:
        submitted: Jobs started so far.
        completed: Jobs that finished without raising.
        failed: Jobs that raised (not counting timeouts or cancellations).
        timed_out: Jobs that exceeded `timeout`.
        active: Jobs currently alive (running or scheduled).

    Note:
        Don't use the pool from inside one of its own jobs: every slot can end
        up held by a job that is waiting for a slot.
    """

    def __init__(self, max_workers: int, timeout: float | None = None) -> None:
        """Create a pool.

        Args:
            max_workers: Maximum number of jobs alive at once. Must be >= 1.
            timeout: Per-job limit in seconds, or None for no limit. Must be > 0.

        Raises:
            ValueError: If `max_workers` or `timeout` is out of range.
        """
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0, or None to disable")

        self._max_workers = max_workers
        self._timeout = timeout
        self._slots = asyncio.BoundedSemaphore(max_workers)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closed = False

        self.submitted = 0
        self.completed = 0
        self.failed = 0  # raised an exception (not a timeout, not cancelled)
        self.timed_out = 0

    @property
    def active(self) -> int:
        """Number of jobs currently alive (running or scheduled)."""
        return len(self._tasks)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.shutdown(wait=True, cancel_futures=exc_type is not None)

    async def create_task(
        self, fn: Callable[P, Awaitable[R]], /, *args: P.args, **kwargs: P.kwargs
    ) -> asyncio.Task[R]:
        """Start `fn(*args, **kwargs)` once a slot is free.

        Args:
            fn: Async function to run. Pass the function, not a coroutine.
            *args: Positional arguments for `fn`.
            **kwargs: Keyword arguments for `fn`.

        Returns:
            The running task. Don't keep the tasks if you create millions: a
            finished task holds on to its result.

        Raises:
            RuntimeError: If the pool is closed.
        """
        await self._acquire()
        return self._spawn(fn, args, kwargs)

    async def map(
        self,
        fn: Callable[[T], Awaitable[R]],
        iterable: Iterable[T] | AsyncIterable[T],
    ) -> AsyncIterator[R]:
        """Run `fn` on every item and yield the results in input order.

        At most `max_workers` items are started ahead of the consumer, so a slow
        consumer slows the pool down instead of results piling up.

        If you may stop early, wrap the stream in `contextlib.aclosing()` so the
        remaining jobs are cancelled immediately rather than on garbage collection.

        Args:
            fn: Async function taking one item.
            iterable: Items to process. Sync or async, and read lazily.

        Yields:
            `fn(item)` for each item, in the order the items were given.

        Raises:
            Exception: A failure, raised once every earlier item has been
                yielded, so a stuck earlier item delays it. The remaining jobs
                are cancelled.
            RuntimeError: If the pool is closed.
        """
        items = _as_async_iter(iterable)
        pending: deque[asyncio.Task[R]] = deque()
        exhausted = False
        try:
            while True:
                if not exhausted:
                    room = self._max_workers - len(pending)
                    exhausted = await self._fill(fn, items, room, pending.append)
                if not pending:
                    return
                yield await pending.popleft()
        finally:
            await self._close_stream(items, pending)

    async def as_completed(
        self,
        fn: Callable[[T], Awaitable[R]],
        iterable: Iterable[T] | AsyncIterable[T],
    ) -> AsyncIterator[R]:
        """Run `fn` on every item and yield the results as they finish.

        Same bounds and error behaviour as `map`, but not in input order.

        Args:
            fn: Async function taking one item.
            iterable: Items to process. Sync or async, and read lazily.

        Yields:
            `fn(item)` for each item, in the order the jobs finish.

        Raises:
            Exception: The first failure, raised to the consumer. The remaining
                jobs are cancelled.
            RuntimeError: If the pool is closed.
        """
        items = _as_async_iter(iterable)
        finished: asyncio.Queue[asyncio.Task[R]] = asyncio.Queue()
        running: set[asyncio.Task[R]] = set()  # started, not yet yielded

        def register(task: asyncio.Task[R]) -> None:
            running.add(task)
            task.add_done_callback(finished.put_nowait)

        exhausted = False
        try:
            while True:
                if not exhausted:
                    room = self._max_workers - len(running)
                    exhausted = await self._fill(fn, items, room, register)
                if not running:
                    return
                task = await finished.get()
                running.discard(task)
                yield task.result()
        finally:
            await self._close_stream(items, running)

    async def join(self) -> None:
        """Wait until no job is running. The pool stays open for new jobs."""
        while self._tasks:
            await asyncio.wait(tuple(self._tasks))

    async def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Refuse new jobs and, by default, wait for the running ones.

        Args:
            wait: Wait for the running jobs to finish before returning.
            cancel_futures: Cancel the running jobs instead of letting them finish.
        """
        self._closed = True
        tasks = tuple(self._tasks)
        try:
            if cancel_futures:
                self._cancel_all(tasks)
            if wait:
                await self._wait_all(tasks)
        except BaseException:  # cancelled while waiting
            self._cancel_all(tasks)
            raise

    async def _acquire(self) -> None:
        if self._closed:
            raise RuntimeError("Cannot add tasks to a closed pool.")
        await self._slots.acquire()
        if self._closed:  # closed while waiting for a slot
            self._slots.release()
            raise RuntimeError("Pool was closed while waiting for a slot.")

    async def _fill(
        self,
        fn: Callable[..., Awaitable[Any]],
        items: AsyncIterator[Any],
        room: int,
        register: Callable[[asyncio.Task[Any]], None],
    ) -> bool:
        """Start up to `room` jobs, handing each to `register`. True if items ran out."""
        for _ in range(room):
            task = await self._start_next(fn, items)
            if task is None:
                return True
            register(task)
        return False

    async def _start_next(
        self, fn: Callable[..., Awaitable[Any]], items: AsyncIterator[Any]
    ) -> asyncio.Task[Any] | None:
        """Take a slot, then pull the next item and start it. None if exhausted."""
        await self._acquire()
        try:
            item = await anext(items)
        except StopAsyncIteration:
            self._slots.release()
            return None
        except BaseException:
            self._slots.release()
            raise
        return self._spawn(fn, (item,), {})

    def _spawn(self, fn: Callable[..., Awaitable[Any]], args: tuple[Any, ...], kwargs: dict[str, Any]) -> asyncio.Task[Any]:
        """Create the task. The caller must already hold a slot."""
        try:
            task = asyncio.create_task(self._run(fn, args, kwargs))
        except BaseException:
            self._slots.release()
            raise
        self._tasks.add(task)
        self.submitted += 1
        # A callback, not the task body, so it also runs if the task is cancelled before starting.
        task.add_done_callback(self._on_done)
        return task

    async def _run(self, fn: Callable[..., Awaitable[Any]], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        if self._timeout is None:
            return await fn(*args, **kwargs)

        timer = asyncio.timeout(self._timeout)
        try:
            async with timer:
                return await fn(*args, **kwargs)
        except TimeoutError:
            if timer.expired():
                raise JobTimeout(f"job exceeded {self._timeout}s") from None
            raise  # the job's own TimeoutError

    def _on_done(self, task: asyncio.Task[Any]) -> None:
        self._release(task)
        self._count_outcome(task)
        self._log_failure(task)

    def _release(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        self._slots.release()

    def _count_outcome(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            self.completed += 1
        elif isinstance(exc, JobTimeout):
            self.timed_out += 1
        else:
            self.failed += 1

    @staticmethod
    def _log_failure(task: asyncio.Task[Any]) -> None:
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("Pool job failed: %r", exc)

    async def _close_stream(self, items: AsyncGenerator[Any, None], tasks: Iterable[asyncio.Task[Any]]) -> None:
        """Stop a `map`/`as_completed`: cancel its jobs, then close its source."""
        tasks = tuple(tasks)
        self._cancel_all(tasks)
        await self._wait_all(tasks)
        await items.aclose()

    @staticmethod
    def _cancel_all(tasks: Iterable[asyncio.Task[Any]]) -> None:
        for task in tasks:
            task.cancel()

    @staticmethod
    async def _wait_all(tasks: tuple[asyncio.Task[Any], ...]) -> None:
        if tasks:
            await asyncio.wait(tasks)
