from deche.cache import Cache
from deche.test_utils import func


def test_input_serialization(c: Cache, inputs):
    key, value = c.tokenize(inputs)
    assert key == '0c2e66b4abf85ca2cbc98252ed99ebc3dfadd4af9bd131554c76216ac89161c9'
    assert value == b'\x80\x04\x95\x1b\x00\x00\x00\x00\x00\x00\x00}\x94(\x8c\x01a\x94\x8c\x011\x94\x8c\x01b\x94K\x02\x8c\x01c\x94C\x013\x94u.'


def test_output_serialization(c: Cache, output):
    key, value = c.tokenize(output)
    assert key == 'fbe752b7ad43eab170053c3f374f7bcb6ccc00bb9c0de57a324aeca3e45171bb'
    assert value == b'\x80\x04\x95\r\x00\x00\x00\x00\x00\x00\x00C\tsome data\x94.'


def test_write(c, path, inputs, output):
    c.write(path=path, inputs=inputs, output=output)
    key, _ = c.tokenize(obj=inputs)
    assert c.exists(path=path, key=key)


def test_func_wrapper(c, path):
    func(1, 2, x=5)
    full_path = f'{path}/{func.__module__}.{func.__name__}'
    key = '1dc3a322d79dc1f1819059a3380387a51320ca2245ff3b58910551555cc36ef7'
    assert c.exists(full_path, key=key)
