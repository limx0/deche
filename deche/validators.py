import datetime
from typing import Union

from fsspec import AbstractFileSystem


def exists(fs: AbstractFileSystem, path) -> bool:
    return fs.exists(path)


def has_passed_cache_ttl(fs: AbstractFileSystem, path: str, cache_ttl: Union[datetime.datetime, int]) -> bool:
    modified = fs.modified(path=path)
    # Cache until
    if isinstance(cache_ttl, datetime.datetime):
        return datetime.datetime.now() > cache_ttl
    elif isinstance(cache_ttl, (int, float)):
        age = (datetime.datetime.utcnow() - modified).total_seconds()
        return age < cache_ttl
    else:
        raise NotImplementedError
