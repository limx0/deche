import tempfile

from fsspec.implementations.local import LocalFileSystem

from deche.core import cache, CacheExpiryMode

tmp_fs = LocalFileSystem()
path = str(tempfile.mkdtemp())


class Class:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    @cache(fs=tmp_fs)
    def c(self):
        return self.a + self.b


@cache(fs=tmp_fs, prefix=path)
def func(a, b, **kwargs):
    return a + b


@cache(fs=tmp_fs, prefix=path, cache_ttl=0.1, cache_expiry_mode=CacheExpiryMode.REMOVE)
def func_ttl_expiry(a, b, **kwargs):
    return a + b


@cache(fs=tmp_fs, prefix=path, cache_ttl=0.1, cache_expiry_mode=CacheExpiryMode.APPEND)
def func_ttl_expiry_append(a, b, **kwargs):
    return a + b


def identity(x):
    return x
