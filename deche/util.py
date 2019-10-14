import pathlib

import fsspec


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


def modified_name(fs):
    if isinstance(fs, fsspec.get_filesystem_class('file')):
        return 'created'
    elif isinstance(fs, fsspec.get_filesystem_class('s3')):
        return 'LastModified'
    else:
        raise NotImplementedError
