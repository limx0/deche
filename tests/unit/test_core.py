from deche.core import Cache, tokenize
from deche.test_utils import func
from deche.types import FrozenDict


def test_key_deterministic(inputs, inputs_key):
    assert tokenize(inputs)[0] == inputs_key


def test_input_serialization(inputs, inputs_key):
    key, value = tokenize(inputs)
    assert key == inputs_key
    assert value == b'\x80\x04\x95\x9e\x00\x00\x00\x00\x00\x00\x00\x8c\x0bdeche.types\x94\x8c\nFrozenDict\x94\x93\x94)\x81\x94}\x94(\x8c\x05_dict\x94\x8c\x0bcollections\x94\x8c\x0bOrderedDict\x94\x93\x94)R\x94(\x8c\x01a\x94\x8c\x011\x94\x8c\x01b\x94K\x02\x8c\x01c\x94C\x013\x94u}\x94\x8c\x0e__orig_class__\x94\x8c\x06typing\x94\x8c\x0bOrderedDict\x94\x93\x94sb\x8c\x05_hash\x94Nub.'


def test_output_serialization(c: Cache, output):
    key, value = tokenize(output)
    assert key == 'fbe752b7ad43eab170053c3f374f7bcb6ccc00bb9c0de57a324aeca3e45171bb'
    assert value == b'\x80\x04\x95\r\x00\x00\x00\x00\x00\x00\x00C\tsome data\x94.'


def test_write(c, path, inputs, output):
    c.write(path=path, inputs=inputs, output=output)
    key, _ = tokenize(obj=inputs)
    assert c.exists(path=path, key=key)


def test_func_wrapper(c, path):
    func(1, 2, x=5)
    full_path = f'{path}/{func.__module__}.{func.__name__}'
    key = '53f8bed42c814532ae65a8cf727fbebacfcdb46d24b0e770990f23471fba4f60'
    assert func.tokenize(1, 2, x=5) == key
    assert c.exists(full_path, key=key)


def test_func_tokenize(inputs, inputs_key):
    key = func.tokenize(**inputs)
    assert key == inputs_key


def test_func_is_cached():
    func(3, 4, zzz=10)
    assert func.is_cached(3, 4, zzz=10)


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