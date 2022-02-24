import os
import time
from unittest.mock import patch

import pytest

from deche.test_utils import exc_func
from deche.test_utils import func_ttl_expiry_append
from deche.test_utils import mem_fs
from deche.test_utils import memory_cache
from deche.test_utils import path as cache_path
from deche.util import frozendict


@pytest.fixture(scope="function", autouse=True)
def file_cleanup():
    yield
    for f in mem_fs.glob(path="/**/*"):
        if mem_fs.exists(f) and mem_fs.isfile(f):
            mem_fs.rm(f)


@pytest.fixture(scope="function", autouse=True)
def env_cleanup():
    for k in tuple(os.environ):
        if k.startswith("DECHE_"):
            del os.environ[k]


@pytest.fixture(scope="function", autouse=True)
def patch_config():
    with patch("deche.core.config.paths", return_value=[]):
        yield


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
    return "4a404f9c59a7e417729bacf5d9aae323074ec5929f08b0049975ba22284ccb5b"


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
