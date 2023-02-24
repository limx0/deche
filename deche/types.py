from collections.abc import Mapping
from typing import OrderedDict


class FrozenDict(Mapping):
    """
    An immutable wrapper around dictionaries that implements the complete :py:class:`collections.Mapping`
    interface. It can be used as a drop-in replacement for dictionaries where immutability is desired.
    """

    dict_cls = OrderedDict

    def __init__(self, *args, **kwargs):
        self._dict = self.dict_cls(*args, **kwargs)
        self._hash = None

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def copy(self, **add_or_replace):
        return self.__class__(self, **add_or_replace)

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return f"<{self.__class__.__name__}, {self._dict}>"

    def __hash__(self):
        if self._hash is None:
            h = 0
            for key, value in self._dict.items():
                h ^= hash((key, value))
            self._hash = h
        return self._hash


def singleton(cls):
    """decorator for a class to make a singleton out of it"""
    instances = {}

    def get_instance(*args, **kwargs):
        """creating or just return the one and only class instance.
        The singleton depends on the parameters used in __init__"""

        from deche.inspection import args_kwargs_to_kwargs

        full_kwargs = args_kwargs_to_kwargs(cls, args, kwargs)
        key = (cls, full_kwargs)
        if key not in instances:
            instances[key] = cls(*args, **kwargs)
        return instances[key]

    return get_instance
