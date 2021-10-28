import datetime
import functools
import hashlib
import inspect
import logging
import pathlib
import pickle
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Callable, Optional, Tuple, Union

import cloudpickle
from fsspec import filesystem

from deche import config
from deche.enums import CacheVersion
from deche.inspection import args_kwargs_to_kwargs
from deche.util import ensure_path
from deche.util import identity
from deche.util import is_input_filename
from deche.util import not_cache_append_file
from deche.util import wrapped_partial
from deche.validators import exists
from deche.validators import has_passed_cache_ttl


logger = logging.getLogger(__name__)

DEFAULT_SERIALIZER = partial(cloudpickle.dumps, protocol=pickle.DEFAULT_PROTOCOL)
DEFAULT_DESERIALIZER = partial(cloudpickle.loads)


def tokenize(obj: object, serializer: Callable = DEFAULT_SERIALIZER) -> Tuple[str, bytes]:
    value = serializer(obj)
    key = hashlib.sha256(value).hexdigest()
    return key, value


def tokenize_func(func, ignore=None):
    def inner(*args, **kwargs):
        full_kwargs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs, ignore=ignore)
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
    return not (f.endswith(Extensions.inputs) or f.endswith(Extensions.exception))


@dataclass
class Cache:
    fs_protocol: Optional[str] = None
    fs_storage_options: Optional[dict] = None
    prefix: Optional[str] = None
    input_serializer: Callable = DEFAULT_SERIALIZER
    input_deserializer: Callable = DEFAULT_DESERIALIZER
    output_serializer: Callable = DEFAULT_SERIALIZER
    output_deserializer: Callable = DEFAULT_DESERIALIZER
    cache_ttl: Optional[Union[datetime.timedelta, datetime.datetime, int]] = None
    cache_expiry_mode: CacheExpiryMode = CacheExpiryMode.REMOVE
    cache_validators: Tuple[Callable] = None
    non_hashable_kwargs: Optional[Tuple[str]] = None

    def __post_init__(self):
        self._fs = None
        if self.cache_validators is None:
            self.cache_validators = DEFAULT_VALIDATORS
        if isinstance(self.cache_ttl, datetime.timedelta):
            self.cache_ttl: float = self.cache_ttl.total_seconds()
        if self.cache_ttl is not None:
            self.cache_validators += (wrapped_partial(has_passed_cache_ttl, cache_ttl=self.cache_ttl),)
        if self.fs_protocol:
            self._fs = filesystem(protocol=self.fs_protocol, **(self.fs_storage_options or {}))
        if self.prefix is not None:
            self.prefix = ensure_path(self.prefix)
        for validator in self.cache_validators:
            err = (
                f"Validator: {validator} must have __name__ attr, if using `functools.partial, "
                f"consider using `deche.util.wrapper_partial`"
            )
            assert hasattr(validator, "__name__"), err
        self._parents = set()

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
        validator = None
        try:
            for validator in self.cache_validators:
                assert validator(fs=self.fs, path=path), "Validation not True"
            return True
        except Exception as e:
            logger.debug(f"{path} Validator:{validator.__name__} failed with Exception: {e}")
            return False

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

    def _write_ttl_append(self, path: str):
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

    def write(self, path: str, data: bytes):
        if self.cache_ttl and self.cache_expiry_mode == CacheExpiryMode.APPEND and not is_input_filename(path):
            self._write_ttl_append(path=path)

        # Ensure parent exists
        parent = str(pathlib.Path(path).parent)
        if parent not in self._parents:
            if not self.fs.exists(parent):
                self.fs.mkdir(parent)
            self._parents.add(parent)

        with self.fs.open(path, mode="wb") as f:
            logger.debug(f"{self.fs_protocol}://{path}")
            return f.write(data)

    def write_input(self, path, inputs, input_serializer=None):
        key, input_value = tokenize(obj=inputs, serializer=input_serializer or self.input_serializer)
        self.write(path=f"{path}{Extensions.inputs}", data=input_value)

    def write_output(self, path, output, output_serializer=None):
        content_hash, output_value = tokenize(obj=output, serializer=output_serializer or self.output_serializer)
        self.write(path=path, data=output_value)

    def is_valid(self, func):
        def inner(*args, **kwargs):
            path = self._path(func)
            key = func.tokenize(*args, **kwargs)
            return self.valid(path=f"{path}/{key}")

        return inner

    def _exists(self, func, ext=None):
        def inner(*, key=None, kwargs=None):
            assert key is not None or kwargs is not None, "Must pass key or kwargs"
            path = self._path(func)
            if key is None:
                key = func.tokenize(**kwargs)
            return self.fs.exists(path=f"{path}/{key}{ext or ''}")

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

    def _remove_all(self, func, ext=None):
        def inner():
            list_inner = self._list(func=func, ext=ext)
            for key in list_inner():
                self._remove(func, ext=ext)(key=key)

        return inner

    def __call__(self, func):  # noqa: C901
        # TODO - very lazy async support. Refactor
        # TODO - fsspec also has async support - could make exists/load calls async

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                path = self._path(func=func)
                inputs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs, ignore=self.non_hashable_kwargs)
                key, _ = tokenize(obj=inputs)
                if self.valid(path=f"{path}/{key}"):
                    return self._load(func=func)(key=key)
                elif self._exists(func=func, ext=Extensions.exception)(key=key):
                    raise self._load(func=func, ext=Extensions.exception)(key=key)
                try:
                    self.write_input(path=f"{path}/{key}", inputs=inputs)
                    logger.debug(f"Calling {func}")
                    output = await func(*args, **kwargs)
                    logger.debug(f"Function {func} ran successfully")
                    self.write_output(path=f"{path}/{key}", output=output)
                except Exception as e:
                    logger.debug(f"Function {func} raised {e}")
                    self.write_output(path=f"{path}/{key}{Extensions.exception}", output=e)
                    raise e

                return output

        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                path = self._path(func=func)
                inputs = args_kwargs_to_kwargs(func=func, args=args, kwargs=kwargs, ignore=self.non_hashable_kwargs)
                key, _ = tokenize(obj=inputs)
                if self.valid(path=f"{path}/{key}"):
                    return self._load(func=func)(key=key)
                elif self._exists(func=func, ext=Extensions.exception)(key=key):
                    raise self._load(func=func, ext=Extensions.exception)(key=key)
                try:
                    self.write_input(path=f"{path}/{key}", inputs=inputs)
                    logger.debug(f"Calling {func}")
                    output = func(*args, **kwargs)
                    logger.debug(f"Function {func} ran successfully")
                    self.write_output(path=f"{path}/{key}", output=output)
                except Exception as e:
                    logger.debug(f"Function {func} raised {e}")
                    self.write_output(path=f"{path}/{key}{Extensions.exception}", output=e)
                    raise e

                return output

        wrapper.tokenize = tokenize_func(func=func, ignore=self.non_hashable_kwargs)
        wrapper.func = func
        wrapper.fs = self.fs
        wrapper.is_valid = self.is_valid(func=wrapper)
        wrapper.has_inputs = self._exists(func=wrapper, ext=Extensions.inputs)
        wrapper.has_data = self._exists(func=wrapper)
        wrapper.has_exception = self._exists(func=wrapper, ext=Extensions.exception)
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
        wrapper.remove_cached_exception = self._remove(func=wrapper, ext=Extensions.exception)
        wrapper.remove_all_cached_exceptions = self._remove_all(func=wrapper, ext=Extensions.exception)
        wrapper.path = functools.partial(self._path, func=func)
        wrapper.deche = self
        return wrapper

    def replace(self, **kwargs):
        attrs = {k: getattr(self, k) for k in self.__dataclass_fields__}
        return self.__class__(**{**attrs, **kwargs})
