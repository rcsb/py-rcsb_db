##
# File:    DictMethodResourceProvider.py
# Author:  J. Westbrook
# Date:    3-Jun-2019
# Version: 0.001 Initial version
#
#
# Updates:
#
##
"""
Skeleton Singleton metaclass.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


class _SingletonClass(type):
    __classInstances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__classInstances:
            cls.__classInstances[cls] = super(_SingletonClass, cls).__call__(*args, **kwargs)
        return cls.__classInstances[cls]

    def clear(cls):
        try:
            del _SingletonClass.__classInstances[cls]
        except KeyError:
            pass


class SingletonClass(_SingletonClass("MySingletonClass", (object,), {})):
    pass
