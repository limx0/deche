import pathlib
import re
from functools import partial
from functools import update_wrapper
from typing import Dict


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


def is_class_instance(kwargs: Dict) -> bool:
    if "self" in kwargs:
        try:
            kwargs["self"].__class__
            return True
        except AttributeError:
            pass
    return False
