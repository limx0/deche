import functools
import hashlib
import inspect
import pathlib
from dataclasses import dataclass
from typing import Callable

from cloudpickle import cloudpickle
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from deche.inspection import args_kwargs_to_kwargs
from deche.types import singleton
from deche.util import ensure_path


def tokenize(obj: object, serializer: Callable = cloudpickle.dumps) -> (str, bytes):
    value = serializer(obj)
    key = hashlib.sha256(value).hexdigest()
    return key, value


def hash_clean_source(func, length=7):
    src = inspect.getsource(func).split('\n')
    lines = [l.strip() for l in src if not l.startswith('@')]
    clean_src = '\n'.join(lines)
    return hashlib.sha256(clean_src)[:length]


def func_qualname(func):
    if func.__module__ == '__main__':
        # TODO add tests
        return f'{func.__module__}/{func.__name__}-{hash_clean_source(func)}'
    else:
        return f'{func.__module__}/{func.__name__}'


@singleton
@dataclass
class Cache:
    content_hash: bool = False

    input_serializer: Callable = cloudpickle.dumps
    input_deserializer: Callable = cloudpickle.loads
    output_serializer: Callable = cloudpickle.dumps
    output_deserializer: Callable = cloudpickle.loads

    fs: AbstractFileSystem = LocalFileSystem()
    prefix: str = "/"

    def __post_init__(self):
        # Check for environment variables
        pass

    def exists(self, path, key):
        return self.fs.exists(f"{ensure_path(path)}/{key}")

    def _read(self, path: str, key: str) -> bytes:
        with self.fs.open(path=f"{ensure_path(path)}/{key}", mode="rb") as f:
            return f.read()

    def _write(self, path: str, key: str, value: bytes):
        with self.fs.open(path=f"{ensure_path(path)}/{key}", mode="wb") as f:
            f.write(value)

    def read_inputs(self, path: str, key: str, deserializer=None):
        deserializer = deserializer or self.input_deserializer
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
        content_hash, output_value = tokenize(
            obj=output, serializer=output_serializer or self.output_serializer
        )
        key, input_value = tokenize(
            obj=inputs, serializer=input_serializer or self.input_serializer
        )
        self._write(path=path, key=f"{key}.inputs", value=input_value)
        self._write(path=path, key=f"{key}", value=output_value)


def tokenize_func(func):
    def inner(*args, **kwargs):
        full_kwargs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs)
        key, value = tokenize(obj=full_kwargs)
        return key

    return inner


def is_cached(cache, path, func):
    def inner(*args, **kwargs):
        key = func.tokenize(*args, **kwargs)
        return cache.exists(path=path, key=key)

    return inner


def list_cached_parameters(cache, path):
    def inner():
        return [
            cache.read_inputs(path=path, key=pathlib.Path(f).name)
            for f in cache.fs.glob(f'{path}/*.inputs')
        ]

    return inner


def load_cached_data(cache, func, path):
    def inner(*args, **kwargs):
        key = func.tokenize(*args, **kwargs)
        return cache.read_output(path=path, key=key)

    return inner


def cache(prefix, **cache_kwargs):
    prefix = ensure_path(prefix)
    c = Cache(**cache_kwargs)

    def deco(func):
        path = f"{prefix}/{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def inner(*args, **kwargs):
            inputs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs)
            key, _ = tokenize(obj=inputs)
            if c.exists(path=path, key=key):
                return c.read_output(path=path, key=key)
            output = func(*args, **kwargs)
            c.write(path=path, inputs=inputs, output=output)
            return output

        inner.tokenize = tokenize_func(func=func)
        inner.is_cached = is_cached(cache=c, path=path, func=inner)
        inner.load_cached_data = load_cached_data(cache=c, path=path, func=inner)
        inner.list_cached_parameters = list_cached_parameters(cache=c, path=path)
        return inner

    return deco
