import os
import time

import pytest
from frozendict import frozendict

from deche.core import Cache
from deche.test_utils import exc_func
from deche.test_utils import func_ttl_expiry_append
from deche.test_utils import mem_fs
from deche.test_utils import memory_cache
from deche.test_utils import path as cache_path


@pytest.fixture(scope="function", autouse=True)
def cleanup(c: Cache, path):
    files = list(c.fs.glob(f"{path}/**/*"))
    for f in files:
        if c.fs.exists(f):
            c.fs.rm(path=f)


@pytest.fixture(scope="function", autouse=True)
def test_cleanup():
    yield
    for f in mem_fs.glob(path="/**/*"):
        if mem_fs.exists(f):
            mem_fs.rm(f)


@pytest.fixture(scope="function", autouse=True)
def env_cleanup():
    for k in tuple(os.environ):
        if k.startswith("DECHE_"):
            del os.environ[k]


@pytest.fixture()
def c():
    return memory_cache


@pytest.fixture()
def path():
    return cache_path


@pytest.fixture(scope="function")
def inputs():
    return frozendict({"a": "1", "b": 2, "c": b"3"})


@pytest.fixture(scope="function")
def inputs_key():
    return "73936aabe2480e82fe9ef81f944b70432d6c132c8fc7e2b520284c788dba15fb"


@pytest.fixture(scope="function")
def output():
    return b"some data"


@pytest.fixture(scope="function")
def cached_exception():
    try:
        exc_func()
    except ZeroDivisionError:
        pass


@pytest.fixture(scope="function")
def cached_ttl_data():
    func_ttl_expiry_append(1, 2)
    time.sleep(0.11)
    func_ttl_expiry_append(1, 2)
    time.sleep(0.11)
    func_ttl_expiry_append(1, 2)
