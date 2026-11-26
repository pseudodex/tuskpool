class JobTimeout(TimeoutError):  # noqa: N818 - public name since 0.1.0
    """A job ran longer than the pool's `timeout`."""
