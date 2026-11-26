from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from typing import TypeVar


T = TypeVar("T")


async def as_async_iter(items: Iterable[T] | AsyncIterable[T]) -> AsyncGenerator[T, None]:
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
