import functools
import hashlib
from dataclasses import dataclass

from cloudpickle import cloudpickle
from deche.inspection import args_kwargs_to_kwargs
from deche.util import ensure_path
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from typing import Callable

# Remove read func, write bytes to BytesIO then write to fs
# TODO - Use fsspec to build a more generic file interface
# TODO - Add optional expiry time, now - created. With option to overwrite/append (cache every 5 days)
# TODO - Tests for class methods, watch out for self cache invalidation, maybe ast the variables used in func?


@dataclass
class Cache:
    content_hash: bool = False

    input_serializer: Callable = cloudpickle.dumps
    input_deserializer: Callable = cloudpickle.loads
    output_serializer: Callable = cloudpickle.dumps
    output_deserializer: Callable = cloudpickle.loads

    fs: AbstractFileSystem = LocalFileSystem()
    prefix: str = '/'

    def __post_init__(self):
        # Check for environment variables
        pass

    @staticmethod
    def bytes_to_key(value: bytes):
        return hashlib.sha256(value).hexdigest()

    def tokenize(self, obj: object, serializer: Callable = cloudpickle.dumps) -> (str, bytes):
        value = serializer(obj)
        key = self.bytes_to_key(value=value)
        return key, value

    def exists(self, path, key):
        return self.fs.exists(f"{ensure_path(path)}/{key}")

    def _read(self, path: str, key: str) -> bytes:
        with self.fs.open(path=f"{ensure_path(path)}/{key}", mode="rb") as f:
            return f.read()

    def _write(self, path: str, key: str, value: bytes):
        with self.fs.open(path=f"{ensure_path(path)}/{key}", mode="wb") as f:
            f.write(value)

    def read_inputs(self, path: str, key: str, deserializer=None):
        deserializer = deserializer or self.input_serializer
        raw = self._read(path=path, key=key)
        return deserializer(raw)

    def read_output(self, path: str, key: str, deserializer=None):
        deserializer = deserializer or self.output_deserializer
        raw = self._read(path=path, key=key)
        return deserializer(raw)

    def write(
            self,
            path: str,
            inputs: object,
            output: object,
            input_serializer=None,
            output_serializer=None,
    ):
        content_hash, output_value = self.tokenize(
            obj=output, serializer=output_serializer or self.output_serializer
        )
        key, input_value = self.tokenize(
            obj=inputs, serializer=input_serializer or self.input_serializer
        )
        self._write(path=path, key=f"{key}.inputs", value=input_value)
        self._write(path=path, key=f"{key}", value=output_value)


def cache(prefix, **cache_kwargs):
    prefix = ensure_path(prefix)
    c = Cache(**cache_kwargs)

    def deco(func):
        path = f"{prefix}/{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def inner(*args, **kwargs):
            inputs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs)
            key, _ = c.tokenize(obj=inputs)
            if c.exists(path=path, key=key):
                return c.read_output(path=path, key=key)
            output = func(*args, **kwargs)
            c.write(path=path, inputs=inputs, output=output)

        return inner

    return deco
