import datetime
import functools
import hashlib
import pathlib
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Callable, Union, Tuple, Optional

from cloudpickle import cloudpickle
from fsspec import filesystem
from loguru import logger

from deche import config
from deche.enums import CacheVersion
from deche.inspection import args_kwargs_to_kwargs
from deche.util import is_input_filename, identity, ensure_path, not_cache_append_file
from deche.validators import exists, has_passed_cache_ttl


def tokenize(obj: object, serializer: Callable = cloudpickle.dumps) -> (str, bytes):
    value = serializer(obj)
    key = hashlib.sha256(value).hexdigest()
    return key, value


def tokenize_func(func):
    def inner(*args, **kwargs):
        full_kwargs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs)
        key, value = tokenize(obj=full_kwargs)
        return key

    return inner


DEFAULT_VALIDATORS = (exists,)


class Extensions:
    inputs = ".inputs"
    exception = ".exc"


class CacheExpiryMode(Enum):
    REMOVE = 1
    APPEND = 2


# TODO implement pickle serialisation/deserialisation for class w/ config refresh
# TODO Clean up config load - classmethod?


def data_filter(f):
    return not f.endswith(Extensions.inputs) or f.endswith(Extensions.exception)


@dataclass
class _Cache:
    fs_protocol: Optional[str] = None
    fs_storage_options: Optional[dict] = None
    prefix: Optional[str] = None
    input_serializer: Callable = cloudpickle.dumps
    input_deserializer: Callable = cloudpickle.loads
    output_serializer: Callable = cloudpickle.dumps
    output_deserializer: Callable = cloudpickle.loads
    cache_ttl: Optional[Union[datetime.timedelta, datetime.datetime, int]] = None
    cache_expiry_mode: CacheExpiryMode = CacheExpiryMode.REMOVE
    cache_validators: Tuple[Callable] = None

    def __post_init__(self):
        self._fs = None
        if self.cache_validators is None:
            self.cache_validators = DEFAULT_VALIDATORS
        if isinstance(self.cache_ttl, datetime.timedelta):
            self.cache_ttl = self.cache_ttl.total_seconds()
        if self.cache_ttl is not None:
            self.cache_validators += (partial(has_passed_cache_ttl, cache_ttl=self.cache_ttl),)
        if self.fs_protocol == "file":
            assert (
                "auto_mkdir" in self.fs_storage_options
            ), "Set auto_mkdir=True when using LocalFileSystem so directories can be created"
        if self.fs_protocol:
            self._fs = filesystem(protocol=self.fs_protocol, **(self.fs_storage_options or {}))
        if self.prefix is not None:
            self.prefix = ensure_path(self.prefix)

    @property
    def fs(self):
        if self._fs is None:
            if self.fs_protocol is not None:
                self._fs = filesystem(protocol=self.fs_protocol, **(self.fs_storage_options or {}))
            # Try and load from config
            else:
                self._load_from_config()
        return self._fs

    def _load_from_config(self):
        config.refresh()
        if config.get("fs.protocol", None) is not None:
            logger.debug("Initialising deche from config")
            self.fs_protocol = config["fs.protocol"]
            logger.debug(f"fs_protocol: {self.fs_protocol}")
            self.fs_storage_options = config.get("fs.storage_options", None)
            logger.debug(f"fs_storage_options: {self.fs_storage_options}")
            self.prefix = ensure_path(config.get("fs.prefix", None))
            logger.debug(f"prefix: {self.prefix}")
            self.__post_init__()

    def _path(self, func):
        if not self._fs:
            self.fs
        path = f"{func.__module__}.{func.__name__}"
        return f"{self.prefix}/{path}" if self.prefix is not None else path

    def valid(self, path):
        return all((validator(fs=self.fs, path=path) for validator in self.cache_validators))

    def read(self, path):
        with self.fs.open(path, mode="rb") as f:
            logger.debug(f"{self.fs_protocol}://{path}")
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
            for f in sorted(self.fs.glob(f"{path}*"), reverse=True):
                if is_input_filename(f):
                    continue
                if pathlib.Path(f).name == key:
                    num = 0
                    suffix = ""
                else:
                    num = int(f.replace(f"{path}-", ""))
                    f = f[:-2]
                    suffix = f"-{num}"
                self.fs.mv(f"{f}{suffix}", f"{f}-{num + 1}")
        with self.fs.open(path, mode="wb") as f:
            logger.debug(f"{self.fs_protocol}://{path}")
            return f.write(data)

    def write_input(self, path, inputs, input_serializer=None):
        key, input_value = tokenize(obj=inputs, serializer=input_serializer or self.input_serializer)
        self.write(path=f"{path}{Extensions.inputs}", data=input_value)

    def write_output(self, path, output, output_serializer=None):
        content_hash, output_value = tokenize(obj=output, serializer=output_serializer or self.output_serializer)
        self.write(path=path, data=output_value)

    def is_cached(self, func):
        def inner(*args, **kwargs):
            path = self._path(func)
            key = func.tokenize(*args, **kwargs)
            return self.valid(path=f"{path}/{key}")

        return inner

    # TODO This needs to be cleaned up
    def is_exception(self, func):
        def inner(*args, **kwargs):
            key = tokenize_func(func)(*args, **kwargs)
            return self.fs.exists(path=f"{self._path(func)}/{key}{Extensions.exception}")

        return inner

    def _iter(self, func, ext=None, filter_=identity):
        def inner(key_only=True):
            path = self._path(func)
            glob = self.fs.glob(f"{path}/*{ext or ''}")
            iterator = filter(not_cache_append_file, glob)
            iterator = filter(filter_, iterator)
            if key_only:
                iterator = map(lambda f: pathlib.Path(f).stem, iterator)
            yield from iterator

        return inner

    def _list(self, func, ext=None, filter_=identity):
        iter_inner = self._iter(func=func, ext=ext, filter_=filter_)

        def inner(key_only=True):
            return list(iter_inner(key_only=key_only))

        return inner

    def _load(self, func, deserializer=None, ext=None, version=CacheVersion.LATEST):
        def inner(*, key=None, kwargs=None):
            assert key is not None or kwargs is not None, "Must pass key or kwargs"
            path = self._path(func)
            if key is None:
                key = func.tokenize(**kwargs)
            return self.read_output(path=f"{path}/{key}{ext or ''}", deserializer=deserializer)

        return inner

    def _remove(self, func, ext=None):
        def inner(*, key=None, kwargs=None):
            assert key is not None or kwargs is not None, "Must pass key or kwargs"
            path = self._path(func)
            if key is None:
                key = func.tokenize(**kwargs)
            if not self.fs.exists(path=f"{path}/{key}{ext or ''}"):
                return
            return self.fs.rm(path=f"{path}/{key}{ext or ''}")

        return inner

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            path = self._path(func=func)
            inputs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs)
            key, _ = tokenize(obj=inputs)
            if self.valid(path=f"{path}/{key}"):
                return self._load(func=func)(key=key)
            elif self.is_exception(func=func)(*args, **kwargs):
                raise self._load(func=func, ext=Extensions.exception)(key=key)
            try:
                self.write_input(path=f"{path}/{key}", inputs=inputs)
                output = func(*args, **kwargs)
                self.write_output(path=f"{path}/{key}", output=output)
            except Exception as e:
                self.write_output(path=f"{path}/{key}{Extensions.exception}", output=e)
                raise e

            return output

        wrapper.tokenize = tokenize_func(func=func)
        wrapper.func = func
        wrapper.is_cached = self.is_cached(func=wrapper)
        wrapper.is_exception = self.is_exception(func=wrapper)
        wrapper.list_cached_inputs = self._list(func=wrapper, ext=Extensions.inputs)
        wrapper.list_cached_data = self._list(func=wrapper, filter_=data_filter)
        wrapper.list_cached_exceptions = self._list(func=wrapper, ext=Extensions.exception)
        wrapper.iter_cached_inputs = self._iter(func=wrapper, ext=Extensions.inputs)
        wrapper.iter_cached_data = self._iter(func=wrapper, filter_=data_filter)
        wrapper.iter_cached_exception = self._iter(func=wrapper, ext=Extensions.exception)
        wrapper.load_cached_inputs = self._load(func=wrapper, ext=Extensions.inputs)
        wrapper.load_cached_data = self._load(func=wrapper)
        wrapper.load_cached_exception = self._load(func=wrapper, ext=Extensions.exception)
        wrapper.remove_cached_inputs = self._remove(func=wrapper, ext=Extensions.inputs)
        wrapper.remove_cached_data = self._remove(func=wrapper)
        wrapper.remove_cached_exceptions = self._remove(func=wrapper, ext=Extensions.exception)
        wrapper.path = functools.partial(self._path, func=func)
        return wrapper

    def replace(self, **kwargs):
        attrs = {k: getattr(self, k) for k in self.__dataclass_fields__}
        return self.__class__(**{**attrs, **kwargs})


# noinspection PyPep8Naming
class cache(_Cache):
    pass
