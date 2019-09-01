import shutil

import pytest

from deche.cache import Cache
from deche.test_utils import TEST_FOLDER


@pytest.fixture(scope='function', autouse=True)
def cleanup(path):
    if TEST_FOLDER.exists():
        shutil.rmtree(TEST_FOLDER)
    yield
    TEST_FOLDER.mkdir(exist_ok=True)


@pytest.fixture()
def c():
    return Cache()


@pytest.fixture(scope='function')
def path():
    return str(TEST_FOLDER)


@pytest.fixture(scope='function')
def inputs():
    return {'a': '1', 'b': 2, 'c': b'3'}


@pytest.fixture(scope='function')
def output():
    return b'some data'
