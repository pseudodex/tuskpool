import pytest

from tuskpool.domain import check_limits


@pytest.mark.parametrize(("max_workers", "timeout"), [(1, None), (10, 0.1)])
def test_check_limits_valid_values_pass(max_workers: int, timeout: float | None) -> None:
    check_limits(max_workers, timeout)


@pytest.mark.parametrize(
    ("max_workers", "timeout", "message"),
    [(0, None, "max_workers"), (1, 0, "timeout"), (1, -1.0, "timeout")],
)
def test_check_limits_out_of_range_raises(
    max_workers: int,
    timeout: float | None,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        check_limits(max_workers, timeout)
