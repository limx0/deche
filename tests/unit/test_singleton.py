from deche.types import singleton


@singleton
class A:
    """test class"""

    def __init__(self, key=None, subkey=None):
        self.key = key
        self.subkey = subkey


def test_singleton():
    """some basic tests"""
    testCases = [(None, None), (10, 20), (30, None), (None, 30)]
    instances = set()

    for key, subkey in testCases:
        if key is None:
            if subkey is None:
                instance1, instance2 = A(), A()
            else:
                instance1, instance2 = A(subkey=subkey), A(subkey=subkey)
        else:
            if subkey is None:
                instance1, instance2 = A(key), A(key)
            else:
                instance1, instance2 = A(key, subkey=subkey), A(key, subkey=subkey)

        assert instance1 == instance2
        assert instance1.key == key and instance1.subkey == subkey
        instances.add(instance1)

    assert len(instances) == len(testCases)
