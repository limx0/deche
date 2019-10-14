import datetime
import functools
import hashlib
import inspect
import pathlib
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Union

from cloudpickle import cloudpickle
from fsspec import AbstractFileSystem, filesystem

from deche import config
from deche.inspection import args_kwargs_to_kwargs
from deche.util import modified_name


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


def tokenize_func(func):
    def inner(*args, **kwargs):
        full_kwargs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs)
        key, value = tokenize(obj=full_kwargs)
        return key

    return inner


class CacheExpiryMode(Enum):
    REMOVE = 1
    APPEND = 2


def is_input_filename(key):
    return key.endswith('.inputs')


@dataclass
class _Cache:
    fs: AbstractFileSystem = None
    prefix: str = ''
    input_serializer: Callable = cloudpickle.dumps
    input_deserializer: Callable = cloudpickle.loads
    output_serializer: Callable = cloudpickle.dumps
    output_deserializer: Callable = cloudpickle.loads
    cache_ttl: Union[datetime.timedelta, int] = None
    cache_expiry_mode: CacheExpiryMode = CacheExpiryMode.REMOVE

    def __post_init__(self):
        if self.fs is None:
            self.fs = filesystem(protocol=config.get('fs.protocol'), **config.get("fs.storage_options", {}))
        if isinstance(self.cache_ttl, datetime.timedelta):
            self.cache_ttl = self.cache_ttl.total_seconds()

    def _has_passed_cache_ttl(self, path):
        info = self.fs.info(path=path)
        modified = info[modified_name(self.fs)]
        age = time.time() - modified
        return age > self.cache_ttl

    def valid(self, path):
        exists = self.fs.exists(path)
        if not exists:
            return False
        elif not self.cache_ttl:
            return exists
        else:
            return not self._has_passed_cache_ttl(path=path)

    def read(self, path):
        with self.fs.open(path, mode='rb') as f:
            return f.read()

    def read_input(self, path, deserializer=None):
        deserializer = deserializer or self.input_deserializer
        data = self.read(path=path)
        return deserializer(data)

    def read_output(self, path, deserializer=None):
        deserializer = deserializer or self.output_deserializer
        data = self.read(path=path)
        return deserializer(data)

    def write(self, path: str, data: bytes):
        if self.cache_ttl and self.cache_expiry_mode == CacheExpiryMode.APPEND and not is_input_filename(path):
            # move any existing files
            key = pathlib.Path(path).name
            for f in sorted(self.fs.glob(f'{path}*'), reverse=True):
                if is_input_filename(f):
                    continue
                if pathlib.Path(f).name == key:
                    num = 0
                    suffix = ''
                else:
                    num = int(f.replace(f'{path}-', ""))
                    f = f[:-2]
                    suffix = f'-{num}'
                self.fs.mv(f'{f}{suffix}', f'{f}-{num+1}')
        with self.fs.open(path, mode='wb') as f:
            return f.write(data)

    def write_input(self, path, inputs, input_serializer=None):
        key, input_value = tokenize(obj=inputs, serializer=input_serializer or self.input_serializer)
        self.write(path=f"{path}.inputs", data=input_value)

    def write_output(self, path, output, output_serializer=None):
        content_hash, output_value = tokenize(
            obj=output, serializer=output_serializer or self.output_serializer
        )
        self.write(path=path, data=output_value)

    def is_cached(self, path, func):
        def inner(*args, **kwargs):
            key = func.tokenize(*args, **kwargs)
            return self.valid(path=f'{path}/{key}')

        return inner

    def list_cached_parameters(self, path, deserializer=None):
        deserializer = deserializer or self.input_deserializer

        def inner():
            input_files = list(self.fs.glob(f'{path}/*.inputs'))
            return [self.read_input(f, deserializer=deserializer) for f in input_files]

        return inner

    def load_cached_data(self, func, path, deserializer=None):
        def inner(*args, **kwargs):
            key = func.tokenize(*args, **kwargs)
            return self.read_output(path=f'{path}/{key}', deserializer=deserializer)

        return inner

    def __call__(self, func):
        path = f"{self.prefix}/{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def inner(*args, **kwargs):
            inputs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs)
            key, _ = tokenize(obj=inputs)
            if self.valid(path=f'{path}/{key}'):
                return self.read(path=f'{path}/{key}')
            output = func(*args, **kwargs)
            self.write_input(path=f'{path}/{key}', inputs=inputs)
            self.write_output(path=f'{path}/{key}', output=output)
            return output

        inner.tokenize = tokenize_func(func=func)
        inner.is_cached = self.is_cached(path=path, func=inner)
        inner.load_cached_data = self.load_cached_data(path=path, func=inner)
        inner.list_cached_parameters = self.list_cached_parameters(path=path)
        return inner

    def replace(self, **kwargs):
        attrs = {k: getattr(self, k) for k in self.__dataclass_fields__}
        return self.__class__(**{**attrs, **kwargs})


# noinspection PyPep8Naming
class cache(_Cache):
    pass
