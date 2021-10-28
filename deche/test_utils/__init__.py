import tempfile

from deche.core import Cache
from deche.core import CacheExpiryMode


memory_cache = Cache(fs_protocol="memory")
path = ""


class Class:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    @memory_cache
    def c(self):
        return self.a + self.b


@memory_cache.replace(prefix=path)
def func(a, b, **kwargs):
    return a + b


@memory_cache.replace(prefix=path)
def exc_func(x=1):
    return x / 0


fs_cache = Cache(fs_protocol="file", fs_storage_options=dict(auto_mkdir=True))
path = str(tempfile.mkdtemp())


@fs_cache.replace(prefix=path, cache_ttl=0.1, cache_expiry_mode=CacheExpiryMode.REMOVE)
def func_ttl_expiry(a, b, **kwargs):
    return a + b


@fs_cache.replace(prefix=path, cache_ttl=0.1, cache_expiry_mode=CacheExpiryMode.APPEND)
def func_ttl_expiry_append(a, b, **kwargs):
    return a + b


def identity(x):
    return x


@memory_cache
async def async_func(x, y=1):
    return x + y


mem_fs = memory_cache.fs
tmp_fs = fs_cache.fs


# @memory_cache
# async def async_func(a):
#     return a
