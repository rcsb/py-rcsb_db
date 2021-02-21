##
# File:    CaseNormalizedDict.py
# Author:  J. Westbrook
# Date:    4-Sep-2018
# Version: 0.001
#
# Updates:
##
"""
Dictionary container with case insensitive key comparison preserving the
case of the input keys.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import collections
import logging

logger = logging.getLogger(__name__)


class CaseNormalizedDict(dict):

    """Dictionary container with case insensitive key comparison preserving the
    case of the input keys.

    """

    # This example subclasses dict directly following the example from the requests module.
    # proxy = {}

    def __init__(self, data):
        super(CaseNormalizedDict, self).__init__(data)
        self.__local = dict((k.lower(), k) for k in data)
        # for k in data:
        #    self[k] = data[k]

    def __contains__(self, k):
        return k.lower() in self.__local

    def __delitem__(self, k):
        key = self.__local[k.lower()]
        super(CaseNormalizedDict, self).__delitem__(key)
        del self.__local[k.lower()]

    def __getitem__(self, k):
        key = self.__local[k.lower()]
        return super(CaseNormalizedDict, self).__getitem__(key)

    def get(self, k, default=None):
        return self[k] if k in self else default

    def __setitem__(self, k, v):
        super(CaseNormalizedDict, self).__setitem__(k, v)
        self.__local[k.lower()] = k


class CaseNormalizedDict2(collections.MutableMapping):

    """Dictionary container with case insensitive key comparison preserving the
    case of the input keys.

    """

    # This example subclasses the abstract base class MutableMapping.

    def __init__(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)
        self.__local = dict((k.lower(), k) for k in self.__dict__)

    def __contains__(self, k):
        return k.lower() in self.__local

    def __len__(self):
        return len(self.__local)

    def __iter__(self):
        return iter(self.__local)

    def __getitem__(self, k):
        return self.__dict__[self.__local[k.lower()]]

    def __setitem__(self, k, val):
        self.__dict__[k] = val
        self.__local[k.lower()] = k

    def __delitem__(self, k):
        del self.__dict__[k]
        del self.__local[k.lower()]

    def pop(self, k):
        kp = self.__local.pop(k.lower())
        return self.__dict__.pop(kp)

    def getKey(self, k):
        return self.__local.get(k.lower())

    #
    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return "{}, ({})".format(super(CaseNormalizedDict2, self).__repr__(), self.__dict__)
