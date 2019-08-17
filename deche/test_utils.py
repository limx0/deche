from deche.cache import cache


class Class:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    @cache('test')
    def c(self):
        return self.a + self.b


@cache('test')
def func(a, b, **kwargs):
    return a + b


def identity(x):
    return x
