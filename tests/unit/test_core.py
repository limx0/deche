import time
from unittest import mock

import pytest

from deche.core import cache, tokenize
from deche.test_utils import func, identity, func_ttl_expiry, func_ttl_expiry_append, tmp_fs, exc_func
from deche.types import FrozenDict


def test_key_deterministic(inputs, inputs_key):
    assert tokenize(inputs)[0] == inputs_key


def test_input_serialization(inputs, inputs_key):
    key, value = tokenize(inputs)
    assert key == inputs_key
    assert value == b'\x80\x04\x95\x9e\x00\x00\x00\x00\x00\x00\x00\x8c\x0bdeche.types\x94\x8c\nFrozenDict\x94\x93\x94)\x81\x94}\x94(\x8c\x05_dict\x94\x8c\x0bcollections\x94\x8c\x0bOrderedDict\x94\x93\x94)R\x94(\x8c\x01a\x94\x8c\x011\x94\x8c\x01b\x94K\x02\x8c\x01c\x94C\x013\x94u}\x94\x8c\x0e__orig_class__\x94\x8c\x06typing\x94\x8c\x0bOrderedDict\x94\x93\x94sb\x8c\x05_hash\x94Nub.'


def test_output_serialization(c: cache, output):
    key, value = tokenize(output)
    assert key == 'fbe752b7ad43eab170053c3f374f7bcb6ccc00bb9c0de57a324aeca3e45171bb'
    assert value == b'\x80\x04\x95\r\x00\x00\x00\x00\x00\x00\x00C\tsome data\x94.'


def test_write(c, path, inputs, output):
    key, _ = tokenize(obj=inputs)
    c.write_input(path=f'{path}/{key}', inputs=inputs)
    c.write_output(path=f'{path}/{key}', output=output, output_serializer=identity)
    assert c.valid(path=f'{path}/{key}')


def test_func_wrapper(c):
    func(1, 2, x=5)
    key = '53f8bed42c814532ae65a8cf727fbebacfcdb46d24b0e770990f23471fba4f60'
    full_path = f'/{func.__module__}.{func.__name__}/{key}'
    assert func.tokenize(1, 2, x=5) == key
    assert c.valid(path=full_path)


def test_func_tokenize(inputs, inputs_key):
    key = func.tokenize(**inputs)
    assert key == inputs_key


def test_func_is_cached():
    func(3, 4, zzz=10)
    assert func.is_cached(3, 4, zzz=10)
    assert func.is_cached(b=4, a=3, zzz=10)


def test_load_cached_data():
    expected = func(3, 4, zzz=10)
    assert func.is_cached(3, 4, zzz=10)
    result = func.load_cached_data(3, 4, zzz=10)
    assert result == expected


def test_load_cached_parameters():
    func(3, 4, zzz=10)
    assert func.is_cached(3, 4, zzz=10)
    result = func.list_cached_parameters()
    assert result == [FrozenDict([('a', 3), ('b', 4), ('zzz', 10)])]


def test_cache_ttl():
    func_ttl_expiry(1, 2)
    assert func_ttl_expiry.is_cached(1, 2)
    time.sleep(0.11)
    assert not func_ttl_expiry.is_cached(1, 2)


def test_cache_append(path):
    func_ttl_expiry_append(1, 2)
    assert func_ttl_expiry_append.is_cached(1, 2)
    time.sleep(0.11)
    func_ttl_expiry_append(1, 2)
    time.sleep(0.11)
    func_ttl_expiry_append(1, 2)
    key = func_ttl_expiry_append.tokenize(1, 2)
    full_path = f'{path}/{func_ttl_expiry_append.__module__}.{func_ttl_expiry_append.__name__}'

    c = cache(fs=tmp_fs)
    assert c.fs.exists(path=f'{full_path}/{key}')
    assert c.fs.exists(path=f'{full_path}/{key}-1')
    assert c.fs.exists(path=f'{full_path}/{key}-2')


def test_cache_path(c: cache, path):
    func(1, 2)
    assert func.path == '/deche.test_utils.func'


def test_cache_exception(c: cache, path):
    try:
        exc_func()
    except ZeroDivisionError as e:
        exc = exc_func.load_cached_exception()
        assert exc == e


@mock.patch('deche.cache.write_output')
def test_exception_no_run(mock_write_output, cached_exception):
    exc_func()
    assert not mock_write_output.called


def test_failing_validator():

    def failing_validator(fs, path):
        raise Exception("Validator Failed")

    @cache(fs='mem', cache_validators=(failing_validator,))
    def func():
        return 1

    with pytest.raises(Exception) as e:
        assert e.value == 'Validator Failed'
