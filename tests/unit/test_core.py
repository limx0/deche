import datetime
import time
from collections.abc import Iterable
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from deche import CacheExpiryMode
from deche.core import Cache
from deche.core import tokenize
from deche.test_utils import Class
from deche.test_utils import async_func
from deche.test_utils import exc_func
from deche.test_utils import func
from deche.test_utils import func_ttl_expiry
from deche.test_utils import func_ttl_expiry_append
from deche.test_utils import identity
from deche.test_utils import memory_cache
from deche.util import ValidationError
from deche.util import frozendict


def test_init():
    c = Cache(fs_protocol="memory", prefix="/")
    assert isinstance(c.fs, MemoryFileSystem)


#
#
# def test_lazy_init():
#     c = Cache()
#     assert c.fs is None
#     assert c.prefix is None
#     os.environ.update(
#         {
#             "DECHE_FS__PROTOCOL": "memory",
#             "DECHE_FS__STORAGE_OPTIONS__KEY": "key",
#             "DECHE_FS__STORAGE_OPTIONS__SECRET": "secret",
#             "DECHE_FS__PREFIX": "/test",
#         }
#     )
#     assert isinstance(c.fs, MemoryFileSystem)
#     assert c.fs_protocol == "memory"
#     assert c.fs_storage_options == {"key": "key", "secret": "secret"}
#     assert c.prefix == "/test"
#
#
# def test_lazy_init_prefix():
#     c = Cache()
#     assert c.prefix is None
#     assert c._path(func) == "deche.test_utils.func"
#     os.environ.update(
#         {
#             "DECHE_FS__PROTOCOL": "memory",
#             "DECHE_FS__PREFIX": "/test",
#         }
#     )
#     assert c._path(func) == "/test/deche.test_utils.func"
#
#
# def test_lazy_init_fs():
#     c = Cache()
#     assert c.fs is None
#     os.environ.update({"DECHE_FS__PROTOCOL": "memory"})
#     assert isinstance(c.fs, MemoryFileSystem)


@pytest.mark.parametrize(
    "prefix",
    [
        "/test",
        "/test/",
    ],
)
def test_prefix(prefix):
    c = Cache(fs_protocol="file", prefix=prefix)
    assert c.prefix == "/test"


def test_key_deterministic(inputs, inputs_key):
    assert tokenize(inputs)[0] == inputs_key


def test_input_serialization(inputs, inputs_key):
    key, value = tokenize(inputs)
    assert key == inputs_key
    expected = (
        b"\x80\x04\x958\x00\x00\x00\x00\x00\x00\x00\x8c\ndeche.util\x94\x8c\nfrozendict\x94\x93\x94)\x81\x94"
        b"(\x8c\x01a\x94\x8c\x011\x94\x8c\x01b\x94K\x02\x8c\x01c\x94C\x013\x94u."
    )
    assert value == expected


def test_output_serialization(c: Cache, output):
    key, value = tokenize(output)
    assert key == "fbe752b7ad43eab170053c3f374f7bcb6ccc00bb9c0de57a324aeca3e45171bb"
    assert value == b"\x80\x04\x95\r\x00\x00\x00\x00\x00\x00\x00C\tsome data\x94."


def test_read_input(c: Cache):
    func(1, 2, x=5)
    key = "66037f0c905c6acfe47e15d35dfc6f0bf9721ebc3c92defd001192f4b88f409b"
    assert func.load_cached_inputs(key=key) == frozendict({"a": 1, "b": 2, "x": 5})


def test_write(c, path, inputs, output):
    key, _ = tokenize(obj=inputs)
    c.write_input(path=f"{path}/{key}", inputs=inputs)
    c.write_output(path=f"{path}/{key}", output=output, output_serializer=identity)
    assert c.valid(path=f"{path}/{key}")


def test_func_wrapper(c):
    func(1, 2, x=5)
    key = "66037f0c905c6acfe47e15d35dfc6f0bf9721ebc3c92defd001192f4b88f409b"
    full_path = f"/{func.__module__}.{func.__name__}/{key}"
    assert func.tokenize(1, 2, x=5) == key
    assert c.valid(path=full_path)


def test_func_tokenize(inputs, inputs_key):
    key = func.tokenize(**inputs)
    assert key == inputs_key


def test_func_is_valid():
    func(3, 4, zzz=10)
    assert func.is_valid(3, 4, zzz=10)
    assert func.is_valid(b=4, a=3, zzz=10)


def test_list_cached_inputs():
    func(3, 4, zzz=10)

    result = func.list_cached_inputs()
    assert result == ["f4f46c47d91eea40eba825cf941ff22bdc87ce849400ed3fd85be092e43031d4"]

    result = func.list_cached_inputs(key_only=False)
    assert result == ["/deche.test_utils.func/f4f46c47d91eea40eba825cf941ff22bdc87ce849400ed3fd85be092e43031d4.inputs"]


def test_list_cached_data():
    func(3, 4, zzz=10)
    assert func.is_valid(3, 4, zzz=10)
    result = func.list_cached_data()
    assert result == ["f4f46c47d91eea40eba825cf941ff22bdc87ce849400ed3fd85be092e43031d4"]

    result = func.list_cached_data(key_only=False)
    assert result == ["/deche.test_utils.func/f4f46c47d91eea40eba825cf941ff22bdc87ce849400ed3fd85be092e43031d4"]


# def test_list_cached_exceptions():
#     with pytest.raises(ZeroDivisionError):
#         exc_func()
#     result = exc_func.list_cached_exceptions()
#     assert result == ["6c8d328939ceaaf60d6cbe813bf07a48656647184baa590fe9b6632bfc3d7936"]

#     result = exc_func.list_cached_exceptions(key_only=False)
#     assert result == ["/deche.test_utils.exc_func/6c8d328939ceaaf60d6cbe813bf07a48656647184baa590fe9b6632bfc3d7936.exc"]


def test_iter():
    func(3, 4, zzz=10)
    result = func.iter_cached_inputs()
    assert isinstance(result, Iterable)
    assert next(result) == "f4f46c47d91eea40eba825cf941ff22bdc87ce849400ed3fd85be092e43031d4"


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
    assert func.is_valid(3, 4, zzz=10)
    result = func.load_cached_data(kwargs={"a": 3, "b": 4, "zzz": 10})
    assert expected == result == func(3, 4, zzz=10)

    result = func.load_cached_data(kwargs=dict(a=3, b=4, zzz=10))
    assert result == expected

    key = func.tokenize(a=3, b=4, zzz=10)
    result = func.load_cached_data(key=key)
    assert result == expected


# def test_load_cached_exception():
#     try:
#         exc_func()
#     except ZeroDivisionError as expected:
#         result = exc_func.load_cached_exception(kwargs={})
#         assert isinstance(result, type(expected))
#         assert type(expected) == type(result)

#         key = exc_func.tokenize()
#         result = exc_func.load_cached_exception(key=key)
#         assert isinstance(result, type(expected))


# def test_remove_all_exceptions():
#     try:
#         exc_func(1)
#     except ZeroDivisionError:
#         pass
#     try:
#         exc_func(2)
#     except ZeroDivisionError:
#         pass
#     assert len(exc_func.list_cached_exceptions()) == 2
#     exc_func.remove_all_cached_exceptions()
#     assert len(exc_func.list_cached_exceptions()) == 0


def test_exists():
    func(1, 2)
    assert func.has_data(kwargs=dict(a=1, b=2))
    assert func.has_data(key=func.tokenize(1, 2))


def test_cache_ttl():
    func_ttl_expiry(1, 2)
    assert func_ttl_expiry.is_valid(1, 2)
    time.sleep(0.11)
    assert not func_ttl_expiry.is_valid(1, 2)


def test_cache_append(path, cached_ttl_data):
    key = func_ttl_expiry_append.tokenize(1, 2)
    full_path = f"{path}/{func_ttl_expiry_append.__module__}.{func_ttl_expiry_append.__name__}"
    c = Cache(fs_protocol="file", prefix=full_path, fs_storage_options={"auto_mkdir": True})
    files = c.fs.ls(path=f"{full_path}/")
    assert c.fs.exists(path=f"{full_path}/{key}")
    assert len(files) == 5
    assert func_ttl_expiry_append.load_cached_inputs(key=key) == frozendict({"a": 1, "b": 2})


def test_append_iter_files(cached_ttl_data):
    keys = func_ttl_expiry_append.list_cached_data()
    assert keys == ["dba23a98657fd0cf15150510055fb3ffd0ad72fa3b74e9538b6260f0123c1758"]


def test_cache_append_path(c: Cache):
    @c.replace(cache_ttl=datetime.timedelta(seconds=1), cache_expiry_mode=CacheExpiryMode.APPEND)
    def append_func(a=1):
        return a

    append_func()
    files = append_func.list_cached_data()
    assert len(files) == 1

    time.sleep(1.1)

    append_func()
    non_append_files = append_func.list_cached_data(key_only=False)
    assert len(non_append_files) == 1
    all_files = c.fs.glob(f"{c._path(append_func)}/*")
    assert len(all_files) == 4


def test_cache_path(c: Cache):
    func(1, 2)
    assert func.path() == "/deche.test_utils.func"


# def test_cache_exception(c: Cache):
#     try:
#         exc_func()
#     except ZeroDivisionError as e:
#         exc = exc_func.load_cached_exception(kwargs={})
#         assert type(exc) == type(e)


def test_cached_exception_raises(cached_exception):
    with pytest.raises(ZeroDivisionError):
        exc_func()


@mock.patch("deche.Cache.write_output")
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

    assert not func.is_valid()
    assert func() == 1


def test_cache_replace():
    c1 = Cache(fs_protocol="memory", prefix="/", cache_ttl=10)
    assert c1.cache_ttl == 10
    c2 = c1.replace(cache_ttl=20)
    assert c2.cache_ttl == 20


def test_varargs_assertion(c: Cache):
    @c
    def add(a, *b):
        return a + sum(b)

    with pytest.raises(AssertionError):
        assert add(a=1, b=1)


def test_no_varargs_okay(c: Cache):
    @c
    def add(*_, a, b):
        return a + b

    assert add(a=1, b=1)


def test_no_hashable_params(c: Cache):
    @c.replace(non_hashable_kwargs=["b"])
    def add(*_, a, b):
        return a + b

    assert add.tokenize(a=1, b=1) == add.tokenize(a=1, b=5)


def test_list_data_ignores_exception_file(c: Cache):
    try:
        exc_func()
    except ZeroDivisionError:
        pass
    result = func.list_cached_data()
    assert result == []


@pytest.mark.asyncio
async def test_async(c: Cache):
    result1 = await async_func(1, 2)
    assert result1 == 3
    result2 = async_func.list_cached_data()
    expected = ["be135fb1728e46edf48724c4aea2bcbbf523da81e1236862222c04e4571afe72"]
    assert result2 == expected


def test_class_attributes_cache_data(c: Cache):
    # Arrange
    cls = Class(a=1, b=2)

    cached = cls.func_a.list_cached_data()
    assert not cached

    cls.func_a()
    cached = cls.func_a.list_cached_data()
    assert cached == ["9fc4ea24177b15bb7c3d7509a056b7107e37de3ff815d5e0563c4971382f0fda"]


def test_class_attributes_correct_token(c: Cache):
    # Arrange
    cls1 = Class(a=1, b=2)
    cls2 = Class(a=1, b=3)
    assert not cls1.func_a.is_valid(cls1)

    cls1.func_a()
    assert cls1.func_a.is_valid(cls1)
    assert cls2.func_a.is_valid(cls2)


def test_custom_serializer(c: Cache):
    from io import BytesIO

    def serialize(df: pd.DataFrame) -> bytes:
        buff = BytesIO()
        df.to_parquet(buff)
        return buff.getvalue()

    def deserialize(raw: bytes) -> pd.DataFrame:
        return pd.read_parquet(BytesIO(raw))

    @c.replace(output_serializer=serialize, output_deserializer=deserialize)
    def make_dataframe():
        return pd.DataFrame({"a": np.random.randn(10), "b": np.random.randn(10)})

    df = make_dataframe()
    assert df.shape == (10, 2)
    assert make_dataframe.is_valid()
    df = make_dataframe()
    assert df.shape == (10, 2)


def test_path_callable(c: Cache):
    @c.replace(path_callable=lambda func, kwargs: "a/custom/path")
    def func():
        return 1

    _ = func()
    assert c.fs.exists("a/custom/path")


def test_result_validator(c: Cache):
    @c.replace(result_validator=lambda x: x > 5)
    def func(n: int):
        return n

    assert func(n=6) == 6

    with pytest.raises(ValidationError):
        func(n=1)
