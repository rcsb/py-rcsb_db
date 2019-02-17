##
# File: HashableDict.py
#
##
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping


class HashableDict(Mapping):

    def __init__(self, *args, **kwargs):
        self._d = dict(*args, **kwargs)

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __getitem__(self, key):
        return self._d[key]

    def __hash__(self):
        return hash(tuple(sorted(self._d.items())))

    def __repr__(self):
        return self._d.__repr__()


if __name__ == '__main__':
    hd = HashableDict()
    print(hd)

    td = HashableDict({'a': 1, 'b': 2})
    print(td)
    print(type(td))

    rd = HashableDict({'a': 1, 'b': 2, 'c': 3})
    l1 = [td, td, td]
    t1 = tuple(l1)
    l2 = [rd, rd, rd, rd]
    t2 = tuple(l2)
    ok = t1 == t2

    q = set(l1) == set(l2)
    print("l1-l2 %r" % q)

    q = set(l1) == set(l1)
    print("l1-l1 %r" % q)
    #
    kwD = HashableDict({'dictMapPath': 'pth string'})
    hd = HashableDict({'locator': 1, 'format': 'vrpt-xml-to-cif', 'kwargs': kwD})
    w1 = [hd, hd, hd]
    q = list(set(w1))
    print(q)
    #
