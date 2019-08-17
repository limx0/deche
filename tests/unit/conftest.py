from pathlib import Path
import os

import pytest

from deche.cache import Cache

TEST_FOLDER = Path.cwd().joinpath('resources')


@pytest.fixture(scope='function', autouse=True)
def cleanup(path):
    for f in TEST_FOLDER.glob('**/*'):
        os.remove(str(f))
    if TEST_FOLDER.exists():
        TEST_FOLDER.rmdir()
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
