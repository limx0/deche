import hashlib
import inspect
import pathlib


def is_input_filename(key):
    return key.endswith('.inputs')


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
    p = pathlib.Path(path)
    return str(p)


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
