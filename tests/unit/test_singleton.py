from deche.types import singleton


@singleton
class A:
    """ test class """

    def __init__(self, key=None, subkey=None):
        self.key = key
        self.subkey = subkey

    def __repr__(self):
        return "A(id=%d, %s,%s)" % (id(self), self.key, self.subkey)


def test_singleton():
    """ some basic tests """
    testCases = [(None, None), (10, 20), (30, None), (None, 30)]
    instances = set()
    instance1 = None
    instance2 = None

    for key, subkey in testCases:
        if key == None:
            if subkey == None:
                instance1, instance2 = A(), A()
            else:
                instance1, instance2 = A(subkey=subkey), A(subkey=subkey)
        else:
            if subkey == None:
                instance1, instance2 = A(key), A(key)
            else:
                instance1, instance2 = A(key, subkey=subkey), A(key, subkey=subkey)

        print("instance1: %-25s" % instance1, " instance2: %-25s" % instance2)
        assert instance1 == instance2
        assert instance1.key == key and instance1.subkey == subkey
        instances.add(instance1)

    assert len(instances) == len(testCases)
