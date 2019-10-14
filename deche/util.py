import pathlib


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
