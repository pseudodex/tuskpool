def check_limits(max_workers: int, timeout: float | None) -> None:
    if max_workers < 1:
        raise ValueError("max_workers must be >= 1")

    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be > 0, or None to disable")
