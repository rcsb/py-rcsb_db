##
# File: CacheUtils.py
# Date: 18-Jul-2019 jdw
#
##

import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)


class CacheUtils:
    """LRU style cache.
    """

    def __init__(self, size=10, label="cache"):
        """LRU style cache

        Args:
            size (int, optional): maximum number of elements in the cache. Defaults to 10.
            label (str, optional): A label an instance of the cache.  Defaults to "cache".
        """
        self.__size = size
        self.__cache = OrderedDict()
        self.__label = label

    def get(self, key):
        """Return the cached value associated with the input key.  The key:value
        combination are moved to top of the cache.

        Args:
            key (hashable): identifier for cached object

        Returns:
            (any): Cached object associated with the input key or None
        """
        try:
            value = self.__cache.pop(key)
            self.__cache[key] = value
            return value
        except KeyError:
            return None

    def set(self, key, value):
        """Store the input value in the cache. The cache contents of the cache
        are adjusted if the size limit is exceeded.

        Args:
            key (hashable): identifier for cached object
            value (any): value to be cached

        Returns:
            bool: True for success or false otherwise
        """
        try:
            try:
                self.__cache.pop(key)
            except KeyError:
                if len(self.__cache) >= self.__size:
                    self.__cache.popitem(last=False)
            self.__cache[key] = value
            return True
        except Exception as e:
            logger.exception("Failing for %s with %s", self.__label, str(e))
        return False
