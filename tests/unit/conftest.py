import pytest

from deche.core import cache
from deche.test_utils import memory_cache, mem_fs, path as cache_path, exc_func
from deche.types import FrozenDict


@pytest.fixture(scope="function", autouse=True)
def cleanup(c: cache, path):
    files = list(c.fs.glob(f"{path}/**/*"))
    for f in files:
        if c.fs.exists(f):
            c.fs.rm(path=f)


@pytest.fixture(scope="function", autouse=True)
def test_cleanup():
    yield
    for f in mem_fs.glob(path=f"/**/*"):
        if mem_fs.exists(f):
            mem_fs.rm(f)


@pytest.fixture()
def c():
    return memory_cache


@pytest.fixture()
def path():
    return cache_path


@pytest.fixture(scope="function")
def inputs():
    return FrozenDict({"a": "1", "b": 2, "c": b"3"})


@pytest.fixture(scope="function")
def inputs_key():
    return "100795f3d6d26a8f8f808cb5589412f5d5f67e8fd8188a42b18f935c04348940"


@pytest.fixture(scope="function")
def output():
    return b"some data"


@pytest.fixture(scope="function")
def cached_exception():
    try:
        exc_func()
    except ZeroDivisionError:
        pass
