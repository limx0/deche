from deche.cache import Cache


def test_input_serialization(c: Cache, inputs):
    key, value = c.object_to_value_key(inputs)
    assert key == '0c2e66b4abf85ca2cbc98252ed99ebc3dfadd4af9bd131554c76216ac89161c9'
    assert value == b'\x80\x04\x95\x1b\x00\x00\x00\x00\x00\x00\x00}\x94(\x8c\x01a\x94\x8c\x011\x94\x8c\x01b\x94K\x02\x8c\x01c\x94C\x013\x94u.'


def test_output_serialization(c: Cache, output):
    key, value = c.object_to_value_key(output)
    assert key == 'fbe752b7ad43eab170053c3f374f7bcb6ccc00bb9c0de57a324aeca3e45171bb'
    assert value == b'\x80\x04\x95\r\x00\x00\x00\x00\x00\x00\x00C\tsome data\x94.'


def test_write(c, path, inputs, output):
    c.write(path=path, inputs=inputs, output=output)
    assert c.exists(path, inputs)

# def test_func_wrapper(c, inputs):
#     func(1, 2, x=5)
#     path = f'demo/{func.__module__}.{func.__name__}'
#     assert c.exists(path, inputs=inputs)


# def test_class_cache(c):
#     cls = Class(a=1, b=2)
#     cls.c()
#     assert c.exists(path, parameters={'a': 1, 'b': 2, 'x': 5})
