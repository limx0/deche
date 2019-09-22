import shutil

import pytest

from deche.core import Cache
from deche.test_utils import TEST_FOLDER
from deche.types import FrozenDict


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
    return FrozenDict({'a': '1', 'b': 2, 'c': b'3'})


@pytest.fixture(scope='function')
def inputs_key():
    return '100795f3d6d26a8f8f808cb5589412f5d5f67e8fd8188a42b18f935c04348940'


@pytest.fixture(scope='function')
def output():
    return b'some data'
