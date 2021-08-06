import hashlib
import inspect
import pathlib
import re
from functools import partial
from functools import update_wrapper


def is_input_filename(key):
    return key.endswith(".inputs")


def ensure_path(path):
    """
    Ensure a path is valid
    :param path:
    :return:

    >>> ensure_path('hello')
    'hello'
    >>> ensure_path('hello/')
    'hello'
    """
    defaults = {
        None: None,
        "/": "",
        "": "",
    }
    if path in defaults:
        return defaults[path]
    p = pathlib.Path(path)
    return str(p)


def hash_clean_source(func, length=7):
    src = inspect.getsource(func).split("\n")
    lines = [line.strip() for line in src if not line.startswith("@")]
    clean_src = "\n".join(lines)
    return hashlib.sha256(clean_src)[:length]


def func_qualname(func):
    if func.__module__ == "__main__":
        # TODO add tests
        return f"{func.__module__}/{func.__name__}-{hash_clean_source(func)}"
    else:
        return f"{func.__module__}/{func.__name__}"


def identity(x):
    """Identity function"""
    return x


pat = re.compile(r".*-\d+")


def not_cache_append_file(f):
    """
    Boolean filter for if `f` if a Cache.APPEND file
    >>> not_cache_append_file(f='07f65922d8e59a3da8e06d702d1d243dc7e186fea9783d94dec2cc8ddc0b9618')
    True
    >>> not_cache_append_file(f='07f65922d8e59a3da8e06d702d1d243dc7e186fea9783d94dec2cc8ddc0b9618-1')
    False
    >>> not_cache_append_file(f='07f65922d8e59a3da8e06d702d1d243dc7e186fea9783d94dec2cc8ddc0b9618-135')
    False
    """
    return not bool(re.search(pattern=pat, string=f))


def wrapped_partial(func, *args, **kwargs):
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func
