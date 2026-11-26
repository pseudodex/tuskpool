import tuskpool
from tuskpool import pool


def test_every_name_in_all_is_importable() -> None:
    for name in tuskpool.__all__:
        assert hasattr(tuskpool, name)


def test_old_pool_module_path_exports_the_public_objects() -> None:
    assert pool.TaskPool is tuskpool.TaskPool
    assert pool.JobTimeout is tuskpool.JobTimeout
