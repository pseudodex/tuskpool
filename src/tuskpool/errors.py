class JobTimeout(TimeoutError):
    """A job ran longer than the pool's `timeout`."""
