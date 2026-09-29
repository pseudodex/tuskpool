import asyncio
import gc
import logging
import weakref
from collections.abc import AsyncIterator
from contextlib import aclosing
from typing import Any

import pytest

from tuskpool import JobTimeout, TaskPool
from tuskpool.pool import _as_async_iter


@pytest.fixture(autouse=True)
async def no_unhandled_loop_errors():
    """Fail any test that leaves an unretrieved task exception or loop error."""
    loop = asyncio.get_running_loop()
    seen: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: seen.append(context))
    yield
    gc.collect()
    await asyncio.sleep(0)
    assert not seen, seen


def assert_idle(pool: TaskPool) -> None:
    """No job alive and every slot handed back."""
    assert pool.active == 0
    assert pool._slots._value == pool._max_workers


class Probe:
    """Records concurrency, and how far the input was read ahead."""

    def __init__(self, pool: TaskPool | None = None, delay: float = 0.001) -> None:
        self.pool = pool
        self.delay = delay
        self.running = 0
        self.peak = 0
        self.peak_active = 0
        self.pulled = 0
        self.source_closed = False

    async def job(self, x: Any) -> Any:
        self.running += 1
        self.peak = max(self.peak, self.running)
        if self.pool is not None:
            self.peak_active = max(self.peak_active, self.pool.active)
        try:
            await asyncio.sleep(self.delay)
            return x
        finally:
            self.running -= 1

    async def source(self, n: int) -> AsyncIterator[int]:
        try:
            for i in range(n):
                self.pulled += 1
                yield i
        finally:
            self.source_closed = True

    def sync_source(self, n: int):
        for i in range(n):
            self.pulled += 1
            yield i


async def collect(stream: AsyncIterator[Any]) -> list[Any]:
    return [x async for x in stream]


STREAMS = ["map", "as_completed"]


def stream_of(pool: TaskPool, name: str, fn: Any, source: Any) -> AsyncIterator[Any]:
    return getattr(pool, name)(fn, source)


class TestConstruction:
    @pytest.mark.parametrize("bad", [0, -1, -100])
    def test_max_workers_must_be_positive(self, bad: int) -> None:
        with pytest.raises(ValueError, match="max_workers"):
            TaskPool(bad)

    @pytest.mark.parametrize("bad", [0, -0.5, -10])
    def test_timeout_must_be_positive(self, bad: float) -> None:
        with pytest.raises(ValueError, match="timeout"):
            TaskPool(1, timeout=bad)

    async def test_defaults_and_initial_stats(self) -> None:
        pool = TaskPool(3)
        assert (pool.submitted, pool.completed, pool.failed, pool.timed_out, pool.active) == (0, 0, 0, 0, 0)
        assert_idle(pool)

    async def test_timeout_none_is_allowed(self) -> None:
        TaskPool(1, timeout=None)


class TestCreateTask:
    async def test_runs_with_args_and_kwargs(self) -> None:
        async def add(a: int, b: int = 0) -> int:
            return a + b

        async with TaskPool(2) as pool:
            task = await pool.create_task(add, 1, b=2)
            assert await task == 3
        assert_idle(pool)

    async def test_returns_a_task(self) -> None:
        async def nop() -> None: ...

        async with TaskPool(1) as pool:
            assert isinstance(await pool.create_task(nop), asyncio.Task)

    async def test_blocks_while_pool_is_full(self) -> None:
        gate = asyncio.Event()
        async with TaskPool(2) as pool:
            await pool.create_task(gate.wait)
            await pool.create_task(gate.wait)
            third = asyncio.create_task(pool.create_task(gate.wait))
            await asyncio.sleep(0.02)
            assert not third.done(), "third job must wait for a free slot"
            assert pool.active == 2
            gate.set()
            task = await asyncio.wait_for(third, 1)
            assert await task is True
        assert_idle(pool)

    async def test_never_exceeds_max_workers(self) -> None:
        async with TaskPool(3) as pool:
            probe = Probe(pool, delay=0.005)
            for i in range(30):
                await pool.create_task(probe.job, i)
        assert probe.peak == 3
        assert probe.peak_active <= 3

    async def test_closed_pool_refuses(self) -> None:
        pool = TaskPool(1)
        await pool.shutdown()

        async def nop() -> None: ...

        with pytest.raises(RuntimeError, match="closed"):
            await pool.create_task(nop)
        assert_idle(pool)

    async def test_closed_while_waiting_for_slot(self) -> None:
        gate = asyncio.Event()
        pool = TaskPool(1)
        await pool.create_task(gate.wait)
        waiter = asyncio.create_task(pool.create_task(gate.wait))
        await asyncio.sleep(0.01)
        await pool.shutdown(wait=False)
        gate.set()
        with pytest.raises(RuntimeError, match="closed"):
            await waiter
        await pool.join()
        assert_idle(pool)

    async def test_failed_job_is_counted_logged_and_frees_its_slot(self, caplog: pytest.LogCaptureFixture) -> None:
        async def boom() -> None:
            raise ValueError("boom")

        caplog.set_level(logging.ERROR, logger="tuskpool")
        async with TaskPool(1) as pool:
            task = await pool.create_task(boom)
            await pool.join()
            with pytest.raises(ValueError, match="boom"):
                await task
        assert (pool.failed, pool.completed, pool.timed_out) == (1, 0, 0)
        assert any("boom" in record.getMessage() for record in caplog.records)
        assert_idle(pool)

    async def test_cancelled_job_is_not_counted_and_frees_its_slot(self) -> None:
        gate = asyncio.Event()
        async with TaskPool(1) as pool:
            task = await pool.create_task(gate.wait)
            await asyncio.sleep(0)
            task.cancel()
            await pool.join()
            assert task.cancelled()
        assert (pool.failed, pool.completed, pool.timed_out) == (0, 0, 0)
        assert pool.submitted == 1
        assert_idle(pool)

    async def test_cancelled_before_it_starts_frees_its_slot(self) -> None:
        async def nop() -> None: ...

        async with TaskPool(1) as pool:
            task = await pool.create_task(nop)
            task.cancel()  # never got a chance to run
            await pool.join()
        assert_idle(pool)

    async def test_non_awaitable_function_counts_as_failure(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.ERROR, logger="tuskpool")
        async with TaskPool(1) as pool:
            task = await pool.create_task(lambda: 1)  # type: ignore[arg-type]
            await pool.join()
            with pytest.raises(TypeError):
                await task
        assert pool.failed == 1
        assert_idle(pool)

    async def test_falsy_results_are_returned(self) -> None:
        async def zero() -> int:
            return 0

        async with TaskPool(1) as pool:
            assert await (await pool.create_task(zero)) == 0


@pytest.mark.parametrize("name", STREAMS)
class TestStreams:
    async def test_empty_input(self, name: str) -> None:
        async with TaskPool(3) as pool:
            assert await collect(stream_of(pool, name, Probe().job, [])) == []
        assert pool.submitted == 0
        assert_idle(pool)

    async def test_single_item(self, name: str) -> None:
        async with TaskPool(3) as pool:
            assert await collect(stream_of(pool, name, Probe().job, [42])) == [42]
        assert_idle(pool)

    async def test_all_results_arrive(self, name: str) -> None:
        async with TaskPool(4) as pool:
            out = await collect(stream_of(pool, name, Probe().job, range(200)))
        assert sorted(out) == list(range(200))
        assert (pool.submitted, pool.completed, pool.failed) == (200, 200, 0)
        assert_idle(pool)

    @pytest.mark.parametrize("workers", [1, 2, 5, 50])
    async def test_concurrency_never_exceeds_max_workers(self, name: str, workers: int) -> None:
        async with TaskPool(workers) as pool:
            probe = Probe(pool, delay=0.002)
            await collect(stream_of(pool, name, probe.job, range(150)))
        assert probe.peak <= workers
        assert probe.peak_active <= workers
        assert probe.peak == min(workers, 150)
        assert_idle(pool)

    async def test_max_workers_one_is_sequential(self, name: str) -> None:
        async with TaskPool(1) as pool:
            probe = Probe(pool)
            out = await collect(stream_of(pool, name, probe.job, range(20)))
        assert probe.peak == 1
        assert out == list(range(20))

    @pytest.mark.parametrize(
        "make_source",
        [
            pytest.param(lambda p, n: list(range(n)), id="list"),
            pytest.param(lambda p, n: tuple(range(n)), id="tuple"),
            pytest.param(lambda p, n: range(n), id="range"),
            pytest.param(lambda p, n: (i for i in range(n)), id="generator"),
            pytest.param(lambda p, n: p.sync_source(n), id="sync-generator"),
            pytest.param(lambda p, n: p.source(n), id="async-generator"),
        ],
    )
    async def test_accepts_sync_and_async_iterables(self, name: str, make_source: Any) -> None:
        probe = Probe()
        async with TaskPool(3) as pool:
            out = await collect(stream_of(pool, name, probe.job, make_source(probe, 25)))
        assert sorted(out) == list(range(25))

    async def test_accepts_async_iterator_without_aclose(self, name: str) -> None:
        class Countdown:
            def __init__(self, n: int) -> None:
                self.n = n

            def __aiter__(self) -> "Countdown":
                return self

            async def __anext__(self) -> int:
                if self.n == 0:
                    raise StopAsyncIteration
                self.n -= 1
                return self.n

        async with TaskPool(2) as pool:
            out = await collect(stream_of(pool, name, Probe().job, Countdown(10)))
        assert sorted(out) == list(range(10))

    async def test_falsy_results_are_not_mistaken_for_the_end(self, name: str) -> None:
        async def falsy(x: int) -> Any:
            return [None, 0, "", False, ()][x]

        async with TaskPool(2) as pool:
            out = await collect(stream_of(pool, name, falsy, range(5)))
        assert len(out) == 5

    async def test_input_is_read_lazily_under_backpressure(self, name: str) -> None:
        workers, total = 3, 60
        probe = Probe(delay=0.0005)
        worst = 0
        consumed = 0
        async with TaskPool(workers) as pool:
            async for _ in stream_of(pool, name, probe.job, probe.source(total)):
                consumed += 1
                worst = max(worst, probe.pulled - consumed)
                await asyncio.sleep(0.003)  # consumer much slower than the jobs
        assert consumed == total
        assert worst <= workers, f"read {worst} items ahead of the consumer"

    async def test_sync_input_is_read_lazily(self, name: str) -> None:
        workers, total = 4, 40
        probe = Probe(delay=0.0005)
        worst = consumed = 0
        async with TaskPool(workers) as pool:
            async for _ in stream_of(pool, name, probe.job, probe.sync_source(total)):
                consumed += 1
                worst = max(worst, probe.pulled - consumed)
                await asyncio.sleep(0.002)
        assert worst <= workers

    async def test_endless_input_can_be_abandoned(self, name: str) -> None:
        async def endless() -> AsyncIterator[int]:
            i = 0
            while True:
                yield i
                i += 1

        async with TaskPool(4) as pool:
            seen = 0
            async for _ in stream_of(pool, name, Probe().job, endless()):
                seen += 1
                if seen == 50:
                    break
        assert_idle(pool)

    async def test_results_are_not_retained(self, name: str) -> None:
        class Blob: ...

        refs: list[weakref.ReferenceType[Blob]] = []

        async def make(_: int) -> Blob:
            blob = Blob()
            refs.append(weakref.ref(blob))
            return blob

        async with TaskPool(3) as pool:
            async for blob in stream_of(pool, name, make, range(100)):
                pass
            del blob
            await pool.join()
            await asyncio.sleep(0)  # let the loop drop its references to finished tasks
            gc.collect()
            assert len(refs) == 100
            assert all(ref() is None for ref in refs), "the pool kept results alive"
            assert not pool._tasks

    async def test_job_failure_reaches_the_consumer_and_cleans_up(self, name: str) -> None:
        gate = asyncio.Event()  # never set: blocked jobs can only end by being cancelled
        finished: list[int] = []

        async def job(x: int) -> int:
            if x == 2:
                raise ValueError("bad item")
            if x >= 3 or name == "as_completed":
                await gate.wait()
            finished.append(x)
            return x

        probe = Probe()
        async with TaskPool(4) as pool:
            with pytest.raises(ValueError, match="bad item"):
                await collect(stream_of(pool, name, job, probe.source(100)))
            assert probe.source_closed
            assert pool.failed == 1
            assert pool.active == 0
            # map reaches item 2 only after items 0 and 1 are done; nothing else may finish
            assert finished == ([0, 1] if name == "map" else [])
        assert_idle(pool)

    async def test_early_break_with_aclosing_cancels_jobs_and_closes_source(self, name: str) -> None:
        probe = Probe(delay=0.05)
        async with TaskPool(4) as pool:
            async with aclosing(stream_of(pool, name, probe.job, probe.source(10_000))) as stream:
                async for _ in stream:
                    break
            assert probe.source_closed
            assert pool.active == 0
            assert pool.submitted < 10
        assert_idle(pool)

    async def test_plain_break_is_cleaned_up_by_the_event_loop(self, name: str) -> None:
        probe = Probe(delay=0.05)
        async with TaskPool(4) as pool:
            async for _ in stream_of(pool, name, probe.job, probe.source(10_000)):
                break
            for _ in range(100):  # the generator is finalised by the loop, a tick or two later
                if probe.source_closed and pool.active == 0:
                    break
                await asyncio.sleep(0.005)
            assert probe.source_closed
            assert pool.active == 0
        assert_idle(pool)

    async def test_source_error_reaches_the_consumer_and_cleans_up(self, name: str) -> None:
        gate = asyncio.Event()  # never set
        finished: list[int] = []

        async def job(x: int) -> int:
            await gate.wait()
            finished.append(x)
            return x

        async def bad_source() -> AsyncIterator[int]:
            yield 1
            yield 2
            raise RuntimeError("source blew up")

        async with TaskPool(4) as pool:
            with pytest.raises(RuntimeError, match="source blew up"):
                await collect(stream_of(pool, name, job, bad_source()))
            assert finished == []
            assert pool.submitted == 2
            assert pool.active == 0
        assert_idle(pool)

    async def test_sync_source_error_reaches_the_consumer(self, name: str) -> None:
        def bad_source():
            yield 1
            raise RuntimeError("sync source blew up")

        async with TaskPool(2) as pool:
            with pytest.raises(RuntimeError, match="sync source blew up"):
                await collect(stream_of(pool, name, Probe().job, bad_source()))
        assert_idle(pool)

    async def test_cancelling_the_consumer_cleans_up(self, name: str) -> None:
        probe = Probe(delay=1)
        async with TaskPool(3) as pool:

            async def consume() -> None:
                async for _ in stream_of(pool, name, probe.job, probe.source(100)):
                    pass

            consumer = asyncio.create_task(consume())
            await asyncio.sleep(0.02)
            assert pool.active == 3
            consumer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consumer
            assert probe.source_closed
            assert pool.active == 0
        assert_idle(pool)

    async def test_pool_is_reusable_after_a_stream(self, name: str) -> None:
        async with TaskPool(3) as pool:
            first = await collect(stream_of(pool, name, Probe().job, range(10)))
            second = await collect(stream_of(pool, name, Probe().job, range(10, 20)))
            task = await pool.create_task(Probe().job, 99)
            assert await task == 99
        assert sorted(first + second) == list(range(20))

    async def test_two_streams_share_the_worker_cap(self, name: str) -> None:
        async with TaskPool(4) as pool:
            probe = Probe(pool, delay=0.003)
            a, b = await asyncio.gather(
                collect(stream_of(pool, name, probe.job, range(100))),
                collect(stream_of(pool, name, probe.job, range(100, 200))),
            )
        assert sorted(a) == list(range(100))
        assert sorted(b) == list(range(100, 200))
        assert probe.peak <= 4
        assert probe.peak_active <= 4
        assert_idle(pool)

    async def test_stream_on_closed_pool_raises(self, name: str) -> None:
        pool = TaskPool(2)
        await pool.shutdown()
        with pytest.raises(RuntimeError, match="closed"):
            await collect(stream_of(pool, name, Probe().job, [1, 2, 3]))
        assert_idle(pool)

    async def test_pool_closed_mid_stream(self, name: str) -> None:
        async with TaskPool(2) as pool:
            seen = 0
            with pytest.raises(RuntimeError, match="closed"):
                async for _ in stream_of(pool, name, Probe().job, range(100)):
                    seen += 1
                    if seen == 3:
                        await pool.shutdown(wait=False)
            assert 3 <= seen < 100
        assert_idle(pool)


class TestMapOrdering:
    async def test_input_order_when_jobs_finish_in_reverse(self) -> None:
        events = [asyncio.Event() for _ in range(4)]

        async def job(i: int) -> int:
            await events[i].wait()
            return i

        async with TaskPool(4) as pool:
            consumer = asyncio.create_task(collect(pool.map(job, range(4))))
            for i in reversed(range(4)):
                events[i].set()
                await asyncio.sleep(0.005)
            assert await consumer == [0, 1, 2, 3]

    async def test_input_order_with_random_durations(self) -> None:
        async def job(i: int) -> int:
            await asyncio.sleep(((i * 7919) % 13) / 2000)
            return i

        async with TaskPool(8) as pool:
            assert await collect(pool.map(job, range(300))) == list(range(300))

    async def test_a_slow_first_item_holds_back_results_but_not_memory(self) -> None:
        workers = 4
        gate = asyncio.Event()
        started: list[int] = []

        async def job(i: int) -> int:
            started.append(i)
            if i == 0:
                await gate.wait()
            return i

        async with TaskPool(workers) as pool:
            consumer = asyncio.create_task(collect(pool.map(job, range(100))))
            await asyncio.sleep(0.05)
            assert len(started) == workers, "must not run further ahead than the window"
            assert not consumer.done()
            gate.set()
            assert await consumer == list(range(100))

    async def test_failure_is_raised_when_its_turn_comes(self) -> None:
        async def job(i: int) -> int:
            if i == 2:
                raise ValueError("two")
            return i

        got: list[int] = []
        async with TaskPool(5) as pool:
            with pytest.raises(ValueError, match="two"):
                async for value in pool.map(job, range(5)):
                    got.append(value)
        assert got == [0, 1]


class TestAsCompletedOrdering:
    async def test_results_arrive_as_jobs_finish(self) -> None:
        events = [asyncio.Event() for _ in range(4)]

        async def job(i: int) -> int:
            await events[i].wait()
            return i

        order: list[int] = []
        async with TaskPool(4) as pool:

            async def consume() -> None:
                async for value in pool.as_completed(job, range(4)):
                    order.append(value)

            consumer = asyncio.create_task(consume())
            for i in (2, 0, 3, 1):
                events[i].set()
                await asyncio.sleep(0.005)
            await consumer
        assert order == [2, 0, 3, 1]

    async def test_fast_jobs_are_not_held_back_by_a_slow_one(self) -> None:
        gate = asyncio.Event()

        async def job(i: int) -> int:
            if i == 0:
                await gate.wait()
            return i

        got: list[int] = []
        async with TaskPool(3) as pool:

            async def consume() -> None:
                async for value in pool.as_completed(job, range(10)):
                    got.append(value)

            consumer = asyncio.create_task(consume())
            await asyncio.sleep(0.05)
            assert sorted(got) == list(range(1, 10)), "item 0 is stuck but must not block the others"
            assert not consumer.done()
            gate.set()
            await consumer
        assert sorted(got) == list(range(10))


class TestTimeout:
    async def test_slow_job_raises_job_timeout(self) -> None:
        async def slow() -> None:
            await asyncio.sleep(5)

        async with TaskPool(1, timeout=0.03) as pool:
            task = await pool.create_task(slow)
            await pool.join()
            with pytest.raises(JobTimeout):
                await task
        assert (pool.timed_out, pool.failed, pool.completed) == (1, 0, 0)
        assert_idle(pool)

    async def test_job_timeout_is_a_timeout_error(self) -> None:
        assert issubclass(JobTimeout, TimeoutError)

    async def test_fast_job_is_unaffected(self) -> None:
        async with TaskPool(1, timeout=1) as pool:
            assert await (await pool.create_task(Probe().job, 7)) == 7
        assert pool.timed_out == 0

    async def test_jobs_own_timeout_error_is_a_plain_failure(self) -> None:
        async def own() -> None:
            raise TimeoutError("mine")

        async with TaskPool(1, timeout=1) as pool:
            task = await pool.create_task(own)
            await pool.join()
            with pytest.raises(TimeoutError, match="mine") as info:
                await task
        assert not isinstance(info.value, JobTimeout)
        assert (pool.failed, pool.timed_out) == (1, 0)

    async def test_jobs_own_inner_asyncio_timeout_is_a_plain_failure(self) -> None:
        async def inner() -> None:
            async with asyncio.timeout(0.01):
                await asyncio.sleep(5)

        async with TaskPool(1, timeout=2) as pool:
            task = await pool.create_task(inner)
            await pool.join()
            with pytest.raises(TimeoutError) as info:
                await task
        assert not isinstance(info.value, JobTimeout)
        assert (pool.failed, pool.timed_out) == (1, 0)

    async def test_no_timeout_lets_the_job_own_timeout_through(self) -> None:
        async def own() -> None:
            raise TimeoutError("mine")

        async with TaskPool(1) as pool:
            task = await pool.create_task(own)
            await pool.join()
            with pytest.raises(TimeoutError):
                await task
        assert pool.failed == 1

    async def test_timeout_is_per_job_not_total(self) -> None:
        async def job(i: int) -> int:
            await asyncio.sleep(0.02)
            return i

        async with TaskPool(1, timeout=0.15) as pool:
            out = await collect(pool.map(job, range(20)))  # 0.4s total, each job 0.02s
        assert out == list(range(20))
        assert pool.timed_out == 0

    async def test_timeout_does_not_count_time_spent_waiting_for_a_slot(self) -> None:
        gate = asyncio.Event()

        async def blocker() -> None:
            await gate.wait()

        async def quick() -> str:
            return "ok"

        async with TaskPool(1, timeout=0.1) as pool:
            await pool.create_task(blocker)
            waiting = asyncio.create_task(pool.create_task(quick))
            await asyncio.sleep(0.2)  # longer than the timeout
            gate.set()
            task = await waiting
            assert await task == "ok"

    @pytest.mark.parametrize("name", STREAMS)
    async def test_stream_raises_job_timeout_and_frees_slots(self, name: str) -> None:
        async def job(i: int) -> int:
            if i == 3:
                await asyncio.sleep(5)
            return i

        async with TaskPool(2, timeout=0.03) as pool:
            with pytest.raises(JobTimeout):
                await collect(stream_of(pool, name, job, range(10)))
        assert pool.timed_out == 1
        assert_idle(pool)

    async def test_timed_out_job_is_cancelled(self) -> None:
        cancelled = asyncio.Event()

        async def slow() -> None:
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async with TaskPool(1, timeout=0.02) as pool:
            await pool.create_task(slow)
            await asyncio.wait_for(cancelled.wait(), 1)

    async def test_timeout_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.ERROR, logger="tuskpool")

        async def slow() -> None:
            await asyncio.sleep(5)

        async with TaskPool(1, timeout=0.02) as pool:
            await pool.create_task(slow)
            await pool.join()
        assert any("JobTimeout" in record.getMessage() for record in caplog.records)


class TestJoin:
    async def test_join_on_idle_pool_returns(self) -> None:
        await asyncio.wait_for(TaskPool(2).join(), 1)

    async def test_join_waits_for_all_jobs_and_keeps_pool_open(self) -> None:
        done: list[int] = []

        async def job(i: int) -> None:
            await asyncio.sleep(0.01 * (i + 1))
            done.append(i)

        pool = TaskPool(5)
        for i in range(5):
            await pool.create_task(job, i)
        await pool.join()
        assert sorted(done) == list(range(5))
        assert_idle(pool)
        assert await (await pool.create_task(Probe().job, 1)) == 1
        await pool.shutdown()

    async def test_join_does_not_raise_for_failed_jobs(self) -> None:
        async def boom() -> None:
            raise ValueError

        pool = TaskPool(2)
        await pool.create_task(boom)
        await pool.join()
        assert pool.failed == 1
        await pool.shutdown()


class TestShutdown:
    async def test_waits_for_running_jobs_by_default(self) -> None:
        finished = asyncio.Event()

        async def job() -> None:
            await asyncio.sleep(0.05)
            finished.set()

        pool = TaskPool(1)
        await pool.create_task(job)
        await pool.shutdown()
        assert finished.is_set()
        assert pool.completed == 1
        assert_idle(pool)

    async def test_wait_false_returns_immediately_and_lets_jobs_finish(self) -> None:
        finished = asyncio.Event()

        async def job() -> None:
            await asyncio.sleep(0.05)
            finished.set()

        pool = TaskPool(1)
        await pool.create_task(job)
        await pool.shutdown(wait=False)
        assert not finished.is_set()
        assert pool.closed if hasattr(pool, "closed") else pool._closed
        await asyncio.wait_for(finished.wait(), 1)
        await pool.join()
        assert_idle(pool)

    async def test_cancel_futures_cancels_running_jobs(self) -> None:
        pool = TaskPool(3)
        tasks = [await pool.create_task(asyncio.sleep, 10) for _ in range(3)]
        await pool.shutdown(cancel_futures=True)
        assert all(task.cancelled() for task in tasks)
        assert pool.failed == 0
        assert_idle(pool)

    async def test_cancel_futures_without_waiting(self) -> None:
        pool = TaskPool(2)
        tasks = [await pool.create_task(asyncio.sleep, 10) for _ in range(2)]
        await pool.shutdown(wait=False, cancel_futures=True)
        await pool.join()
        assert all(task.cancelled() for task in tasks)
        assert_idle(pool)

    async def test_is_idempotent(self) -> None:
        pool = TaskPool(2)
        await pool.create_task(asyncio.sleep, 0.01)
        await pool.shutdown()
        await pool.shutdown()
        await pool.shutdown(cancel_futures=True)
        assert_idle(pool)

    async def test_refuses_new_jobs_afterwards(self) -> None:
        pool = TaskPool(2)
        await pool.shutdown()
        with pytest.raises(RuntimeError):
            await pool.create_task(asyncio.sleep, 0)

    async def test_cancelling_shutdown_cancels_the_jobs(self) -> None:
        pool = TaskPool(2)
        task = await pool.create_task(asyncio.sleep, 10)
        closing = asyncio.create_task(pool.shutdown())
        await asyncio.sleep(0.01)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        await pool.join()
        assert task.cancelled()
        assert_idle(pool)


class TestContextManager:
    async def test_returns_the_pool(self) -> None:
        async with TaskPool(1) as pool:
            assert isinstance(pool, TaskPool)

    async def test_exit_waits_for_running_jobs(self) -> None:
        finished = asyncio.Event()

        async def job() -> None:
            await asyncio.sleep(0.05)
            finished.set()

        async with TaskPool(1) as pool:
            await pool.create_task(job)
        assert finished.is_set()
        assert_idle(pool)

    async def test_exit_on_error_cancels_running_jobs_and_propagates(self) -> None:
        task: asyncio.Task[Any] | None = None
        with pytest.raises(KeyError):
            async with TaskPool(2) as pool:
                task = await pool.create_task(asyncio.sleep, 10)
                raise KeyError("body failed")
        assert task is not None and task.cancelled()
        assert_idle(pool)

    async def test_exit_closes_the_pool(self) -> None:
        async with TaskPool(1) as pool:
            pass
        with pytest.raises(RuntimeError):
            await pool.create_task(asyncio.sleep, 0)


class TestStats:
    async def test_counters_add_up_for_a_mixed_workload(self) -> None:
        gate = asyncio.Event()

        async def ok() -> str:
            return "ok"

        async def boom() -> None:
            raise ValueError

        async def slow() -> None:
            await asyncio.sleep(5)

        async def cancelme() -> None:
            await gate.wait()

        pool = TaskPool(20, timeout=0.05)
        for _ in range(5):
            await pool.create_task(ok)
        for _ in range(3):
            await pool.create_task(boom)
        for _ in range(2):
            await pool.create_task(slow)
        doomed = [await pool.create_task(cancelme) for _ in range(2)]
        await asyncio.sleep(0)
        for task in doomed:
            task.cancel()
        await pool.join()
        assert pool.submitted == 12
        assert pool.completed == 5
        assert pool.failed == 3
        assert pool.timed_out == 2
        assert pool.completed + pool.failed + pool.timed_out + len(doomed) == pool.submitted
        assert_idle(pool)
        await pool.shutdown()

    async def test_active_tracks_running_jobs(self) -> None:
        gate = asyncio.Event()
        pool = TaskPool(5)
        for _ in range(3):
            await pool.create_task(gate.wait)
        assert pool.active == 3
        gate.set()
        await pool.join()
        assert pool.active == 0
        await pool.shutdown()

    async def test_stream_counts(self) -> None:
        async def job(i: int) -> int:
            if i % 10 == 0:
                raise ValueError(i)
            return i

        async with TaskPool(5) as pool:
            got = 0
            with pytest.raises(ValueError):
                async for _ in pool.map(job, range(1, 100)):
                    got += 1
        assert pool.failed >= 1
        assert pool.submitted >= got


class TestSlotAccounting:
    async def test_no_slot_leak_over_many_mixed_outcomes(self) -> None:
        async def job(i: int) -> int:
            await asyncio.sleep(0.0005 * (i % 3))
            if i % 5 == 0:
                raise ValueError(i)
            if i % 11 == 0:
                await asyncio.sleep(1)
            return i

        pool = TaskPool(7, timeout=0.05)
        tasks: list[asyncio.Task[int]] = []
        for i in range(400):
            tasks.append(await pool.create_task(job, i))
        await pool.join()
        assert_idle(pool)
        assert pool.submitted == 400
        assert pool.completed + pool.failed + pool.timed_out == 400
        await pool.shutdown()

    async def test_no_slot_leak_after_repeated_aborted_streams(self) -> None:
        async with TaskPool(3) as pool:
            for name in STREAMS * 5:
                probe = Probe(delay=0.002)
                async with aclosing(stream_of(pool, name, probe.job, probe.source(1000))) as stream:
                    async for _ in stream:
                        break
                assert probe.source_closed
                assert_idle(pool)

    async def test_no_slot_leak_when_every_job_fails_in_streams(self) -> None:
        async def boom(i: int) -> None:
            raise ValueError(i)

        async with TaskPool(3) as pool:
            for name in STREAMS:
                with pytest.raises(ValueError):
                    await collect(stream_of(pool, name, boom, range(50)))
                assert_idle(pool)


class TestSpawnFailure:
    async def test_slot_is_returned_when_the_task_cannot_be_created(self) -> None:
        loop = asyncio.get_running_loop()
        original = loop.get_task_factory()

        def broken_factory(_loop: Any, coro: Any, **_kwargs: Any) -> Any:
            coro.close()  # avoid a "never awaited" warning
            raise RuntimeError("cannot create task")

        pool = TaskPool(1)
        loop.set_task_factory(broken_factory)
        try:
            with pytest.raises(RuntimeError, match="cannot create task"):
                await pool.create_task(asyncio.sleep, 0)
        finally:
            loop.set_task_factory(original)
        assert pool.submitted == 0
        assert_idle(pool)
        assert await (await pool.create_task(Probe().job, 5)) == 5
        await pool.shutdown()


class TestAsAsyncIter:
    async def test_sync_iterable_is_lazy(self) -> None:
        pulled = 0

        def gen():
            nonlocal pulled
            for i in range(10):
                pulled += 1
                yield i

        it = _as_async_iter(gen())
        assert pulled == 0
        assert await anext(it) == 0
        assert pulled == 1
        await it.aclose()

    async def test_async_iterable(self) -> None:
        async def gen() -> AsyncIterator[int]:
            for i in range(3):
                yield i

        assert [x async for x in _as_async_iter(gen())] == [0, 1, 2]

    async def test_closing_closes_the_wrapped_async_generator(self) -> None:
        probe = Probe()
        it = _as_async_iter(probe.source(10))
        await anext(it)
        assert not probe.source_closed
        await it.aclose()
        assert probe.source_closed

    async def test_wrapped_iterator_without_aclose(self) -> None:
        class Plain:
            def __aiter__(self) -> "Plain":
                return self

            async def __anext__(self) -> int:
                return 1

        it = _as_async_iter(Plain())
        assert await anext(it) == 1
        await it.aclose()

    async def test_exhausted_source_stays_exhausted(self) -> None:
        it = _as_async_iter([1])
        assert await anext(it) == 1
        for _ in range(3):
            with pytest.raises(StopAsyncIteration):
                await anext(it)

    async def test_empty(self) -> None:
        assert [x async for x in _as_async_iter([])] == []
