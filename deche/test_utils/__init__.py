from pathlib import Path

from deche.core import cache

TEST_FOLDER = Path.absolute(Path(__file__)).parent.joinpath("resources")


class Class:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    @cache(TEST_FOLDER)
    def c(self):
        return self.a + self.b


@cache(prefix=str(TEST_FOLDER))
def func(a, b, **kwargs):
    return a + b


def identity(x):
    return x
