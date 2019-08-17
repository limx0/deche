import functools
import hashlib
from dataclasses import dataclass
from typing import Callable

from cloudpickle import cloudpickle
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from deche.inspection import args_kwargs_to_kwargs
from deche.util import ensure_path


# TODO - Use fsspec to build a more generic file interface
# TODO - Add optional expiry time, now - created
# TODO - Tests for class methods, watch out for self cache invalidation, maybe ast the variables used in func?


@dataclass
class Cache:
    input_serializer: Callable = cloudpickle.dumps
    input_deserializer: Callable = cloudpickle.loads
    output_serializer: Callable = cloudpickle.dumps
    output_deserializer: Callable = cloudpickle.dumps

    fs: AbstractFileSystem = LocalFileSystem()
    write_func: Callable = None
    read_func: Callable = None

    def __post_init__(self):
        # Check for environment variables
        pass

    @staticmethod
    def bytes_to_key(value: bytes):
        return hashlib.sha256(value).hexdigest()

    def object_to_value_key(self, obj: object, serializer: Callable = cloudpickle.dumps) -> (bytes, str, str):
        value = serializer(obj)
        key = self.bytes_to_key(value=value)
        return key, value

    def exists(self, path, inputs, serializer=None):
        key, _ = self.object_to_value_key(obj=inputs, serializer=serializer or self.input_serializer)
        return self.fs.exists(f'{ensure_path(path)}/{key}')

    def _read(self, path: str, key: str, read_func=None) -> bytes:
        read_func = read_func or self.read_func
        return read_func(path=f'{path}/{key}', mode='rb')

    # TODO Add content hash to inputs
    def _write(self, path: str, key: str, value: bytes, write_func=None):
        write_func = write_func or self.write_func
        if write_func is not None:
            return write_func(path=f'{ensure_path(path)}/{key}', mode='wb', value=value)
        else:
            with self.fs.open(path=f'{ensure_path(path)}/{key}', mode='wb') as f:
                f.write(value)

    def read_inputs(self, path: str, key: str, deserializer=None, read_func=None):
        deserializer = deserializer or self.input_serializer
        raw = self._read(path=path, key=key, read_func=read_func)
        return deserializer(raw)

    def read_output(self, path: str, key: str, deserializer=None, read_func=None):
        deserializer = deserializer or self.output_deserializer
        raw = self._read(path=path, key=key, read_func=read_func)
        return deserializer(raw)

    def write(self, path: str, inputs: object, output: object, input_serializer=None, output_serializer=None):
        key, input_value = self.object_to_value_key(obj=inputs, serializer=input_serializer or self.input_serializer)
        content_hash, output_value = self.object_to_value_key(obj=output, serializer=output_serializer or self.output_serializer)
        self._write(path=path, key=f'{key}.parameters', value=input_value)
        self._write(path=path, key=f'{key}', value=output_value)


def cache(prefix, **cache_kwargs):
    prefix = ensure_path(prefix)
    c = Cache(**cache_kwargs)

    def deco(func):
        path = f'{prefix}/{func.__module__}.{func.__name__}'

        @functools.wraps(func)
        def inner(*args, **kwargs):
            parameters = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs)
            if c.exists(path=path, inputs=parameters):
                return c.read_output(path=path, parameters=parameters)
            result = func(*args, **kwargs)
            c.write(path=path, parameters=parameters, data=result)

        return inner

    return deco
