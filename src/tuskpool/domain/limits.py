def check_limits(max_workers: int, timeout: float | None) -> None:
    if max_workers < 1:
        msg = "max_workers must be >= 1"
        raise ValueError(msg)

    if timeout is not None and timeout <= 0:
        msg = "timeout must be > 0, or None to disable"
        raise ValueError(msg)
