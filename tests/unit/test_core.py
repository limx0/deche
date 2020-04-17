import os
import time
from unittest import mock

import pytest
from fsspec.implementations.memory import MemoryFileSystem
from s3fs import S3FileSystem

from deche.core import cache, tokenize
from deche.test_utils import func, identity, func_ttl_expiry, func_ttl_expiry_append, exc_func, memory_cache


def test_init():
    c = cache(fs_protocol="memory")
    assert isinstance(c.fs, MemoryFileSystem)


def test_lazy_init():
    c = cache()
    assert c.fs is None
    assert c.prefix is None
    os.environ.update(
        {
            "DECHE_FS__PROTOCOL": "s3",
            "DECHE_FS__STORAGE_OPTIONS__KEY": "key",
            "DECHE_FS__STORAGE_OPTIONS__SECRET": "secret",
            "DECHE_FS__PREFIX": "/test",
        }
    )
    assert isinstance(c.fs, S3FileSystem)
    assert c.fs.key == "key"
    assert c.fs.secret == "secret"
    assert c.fs_protocol == "s3"
    assert c.fs_storage_options == {"key": "key", "secret": "secret"}
    assert c.prefix == "/test"


def test_key_deterministic(inputs, inputs_key):
    assert tokenize(inputs)[0] == inputs_key


def test_input_serialization(inputs, inputs_key):
    key, value = tokenize(inputs)
    assert key == inputs_key
    assert (
        value
        == b"\x80\x04\x95\x9e\x00\x00\x00\x00\x00\x00\x00\x8c\x0bdeche.types\x94\x8c\nFrozenDict\x94\x93\x94)\x81\x94}\x94(\x8c\x05_dict\x94\x8c\x0bcollections\x94\x8c\x0bOrderedDict\x94\x93\x94)R\x94(\x8c\x01a\x94\x8c\x011\x94\x8c\x01b\x94K\x02\x8c\x01c\x94C\x013\x94u}\x94\x8c\x0e__orig_class__\x94\x8c\x06typing\x94\x8c\x0bOrderedDict\x94\x93\x94sb\x8c\x05_hash\x94Nub."
    )


def test_output_serialization(c: cache, output):
    key, value = tokenize(output)
    assert key == "fbe752b7ad43eab170053c3f374f7bcb6ccc00bb9c0de57a324aeca3e45171bb"
    assert value == b"\x80\x04\x95\r\x00\x00\x00\x00\x00\x00\x00C\tsome data\x94."


def test_write(c, path, inputs, output):
    key, _ = tokenize(obj=inputs)
    c.write_input(path=f"{path}/{key}", inputs=inputs)
    c.write_output(path=f"{path}/{key}", output=output, output_serializer=identity)
    assert c.valid(path=f"{path}/{key}")


def test_func_wrapper(c):
    func(1, 2, x=5)
    key = "53f8bed42c814532ae65a8cf727fbebacfcdb46d24b0e770990f23471fba4f60"
    full_path = f"/{func.__module__}.{func.__name__}/{key}"
    assert func.tokenize(1, 2, x=5) == key
    assert c.valid(path=full_path)


def test_func_tokenize(inputs, inputs_key):
    key = func.tokenize(**inputs)
    assert key == inputs_key


def test_func_is_cached():
    func(3, 4, zzz=10)
    assert func.is_cached(3, 4, zzz=10)
    assert func.is_cached(b=4, a=3, zzz=10)


def test_list_cached_inputs():
    func(3, 4, zzz=10)

    result = func.list_cached_inputs()
    assert result == ["745c3cd4d7f1e96bbc62406e2e0b65749c546ceea0629a37e25fdad123eee86e"]

    result = func.list_cached_inputs(key_only=False)
    assert result == ["/deche.test_utils.func/745c3cd4d7f1e96bbc62406e2e0b65749c546ceea0629a37e25fdad123eee86e.inputs"]


def test_list_cached_data():
    func(3, 4, zzz=10)
    assert func.is_cached(3, 4, zzz=10)
    result = func.list_cached_data()
    assert result == ["745c3cd4d7f1e96bbc62406e2e0b65749c546ceea0629a37e25fdad123eee86e"]

    result = func.list_cached_data(key_only=False)
    assert result == ["/deche.test_utils.func/745c3cd4d7f1e96bbc62406e2e0b65749c546ceea0629a37e25fdad123eee86e"]


def test_list_cached_exceptions():
    try:
        exc_func()
    except Exception as e:
        pass
    result = exc_func.list_cached_exceptions()
    assert result == ["be51217c13e7165157585330ecb37a638ef58d32dd8ff4c5b1aadc0a59298f19"]

    result = exc_func.list_cached_exceptions(key_only=False)
    assert result == ["/deche.test_utils.exc_func/be51217c13e7165157585330ecb37a638ef58d32dd8ff4c5b1aadc0a59298f19.exc"]


def test_load_cached_inputs():
    expected = dict(a=3, b=4, zzz=10)
    func(**expected)
    result = func.load_cached_inputs(kwargs=expected)
    assert result == expected

    key = func.tokenize(a=3, b=4, zzz=10)
    result = func.load_cached_inputs(key=key)
    assert result == expected


def test_load_cached_data():
    expected = func(3, 4, zzz=10)
    assert func.is_cached(3, 4, zzz=10)
    result = func.load_cached_data(kwargs={"a": 3, "b": 4, "zzz": 10})
    assert expected == result == func(3, 4, zzz=10)

    result = func.load_cached_data(kwargs=dict(a=3, b=4, zzz=10))
    assert result == expected

    key = func.tokenize(a=3, b=4, zzz=10)
    result = func.load_cached_data(key=key)
    assert result == expected


def test_load_cached_exception():
    try:
        exc_func()
    except ZeroDivisionError as expected:
        result = exc_func.load_cached_exception(kwargs={})
        assert isinstance(result, type(expected))
        assert type(expected) == type(result)

        key = exc_func.tokenize()
        result = exc_func.load_cached_exception(key=key)
        assert isinstance(result, type(expected))


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
    full_path = f"{path}/{func_ttl_expiry_append.__module__}.{func_ttl_expiry_append.__name__}"

    c = cache(fs_protocol="file", fs_storage_options={"auto_mkdir": True})
    assert c.fs.exists(path=f"{full_path}/{key}")
    assert c.fs.exists(path=f"{full_path}/{key}-1")
    assert c.fs.exists(path=f"{full_path}/{key}-2")


def test_cache_path(c: cache, path):
    func(1, 2)
    assert func.path == "/deche.test_utils.func"


def test_cache_exception(c: cache, path):
    try:
        exc_func()
    except ZeroDivisionError as e:
        exc = exc_func.load_cached_exception(kwargs={})
        assert type(exc) == type(e)


def test_cached_exception_raises(cached_exception):
    with pytest.raises(ZeroDivisionError):
        exc_func()


@mock.patch("deche.cache.write_output")
def test_exception_no_run(mock_write_output, cached_exception):
    try:
        exc_func()
    except ZeroDivisionError:
        pass
    assert not mock_write_output.called


def test_failing_validator():
    def failing_validator(fs, path):
        raise Exception("Validator Failed")

    @memory_cache.replace(cache_validators=(failing_validator,))
    def func():
        return 1

    with pytest.raises(Exception) as e:
        assert e.value == "Validator Failed"
