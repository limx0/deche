import datetime
from typing import Union

from fsspec import AbstractFileSystem


def exists(fs: AbstractFileSystem, inputs_path, content_path) -> bool:
    return fs.exists(content_path)


def has_passed_cache_ttl(fs: AbstractFileSystem, path: str, cache_ttl: Union[datetime.datetime, int]) -> bool:
    modified = datetime.datetime.utcfromtimestamp(fs.stat(path=path)["mtime"])
    # Cache until
    if isinstance(cache_ttl, datetime.datetime):
        return datetime.datetime.now() > cache_ttl
    elif isinstance(cache_ttl, (int, float)):
        age = (datetime.datetime.utcnow() - modified).total_seconds()
        return age < cache_ttl
    else:
        raise NotImplementedError
