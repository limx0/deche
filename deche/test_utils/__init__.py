import tempfile

from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from deche.core import cache, CacheExpiryMode

mem_fs = MemoryFileSystem()
path = ''


class Class:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    @cache(fs=mem_fs)
    def c(self):
        return self.a + self.b


@cache(fs=mem_fs, prefix=path)
def func(a, b, **kwargs):
    return a + b


@cache(fs=mem_fs, prefix=path)
def exc_func():
    return 1/0


tmp_fs = LocalFileSystem(auto_mkdir=True)
path = str(tempfile.mkdtemp())


@cache(fs=tmp_fs, prefix=path, cache_ttl=0.1, cache_expiry_mode=CacheExpiryMode.REMOVE)
def func_ttl_expiry(a, b, **kwargs):
    return a + b


@cache(fs=tmp_fs, prefix=path, cache_ttl=0.1, cache_expiry_mode=CacheExpiryMode.APPEND)
def func_ttl_expiry_append(a, b, **kwargs):
    return a + b


def identity(x):
    return x
