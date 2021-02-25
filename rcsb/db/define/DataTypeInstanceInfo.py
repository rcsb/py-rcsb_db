##
# File:    DataTypeInstanceInfo.py
# Author:  J. Westbrook
# Date:    7-Jun-2018
# Version: 0.001 Initial version
#
# Updates:
#      15-Jun-2018 jdw cleanup exception handling
#      18-Jun-2018 jdw turn off distracting warning messages by default
#       6-Oct-2018 jdw make the methods in this module an innocuous pass-thru
#                      if no instance data is available.
#
##
"""
Manage data type details extracted by scanning example data sets.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class DataTypeInstanceInfo(object):
    def __init__(self, filePath, **kwargs):
        self.__filePath = filePath
        # Turn off warnings for missing values
        self.__verbose = kwargs.get("verbose", False)
        self.__tD = {}
        self.__mU = MarshalUtil()
        self.__byPassMode = not self.__setup(self.__filePath)

    def __setup(self, filePath):
        """
        Read the output serialized by ScanRepoUtil() -
        tD[category] -> d[atName]->{minWidth: , maxWidth:, minPrec:, maxPrec: , count}
        """
        try:
            if not filePath:
                return False

            self.__tD = self.__mU.doImport(filePath, fmt="json")
            return len(self.__tD) > 0
        except Exception:
            return False

    def testCache(self):
        logger.debug("Data length %d", len(self.__tD))
        logger.debug("Bypass mode %r", self.__byPassMode)
        return True if (self.__byPassMode or (not self.__byPassMode and self.__tD)) else False

    def exists(self, catName, atName=None):
        if self.__byPassMode:
            return True
        try:
            if atName:
                return atName in self.__tD[catName]
            else:
                return catName in self.__tD
            return True
        except Exception:
            pass
        return False

    def getAttributeTypeInfo(self, catName, atName):
        try:
            return self.__tD[catName][atName]
        except Exception as e:
            if self.__verbose:
                logger.warning("Missing instance type info for category %r attribute %r %s", catName, atName, str(e))
        return {}

    def getCategoryTypeInfo(self, catName):
        try:
            return self.__tD[catName]
        except Exception as e:
            if self.__verbose:
                logger.warning("Missing instance type info for category %r  %s", catName, str(e))
        return {}

    def getMinWidth(self, catName, atName):
        try:
            return self.__tD[catName][atName]["minWidth"]
        except Exception as e:
            if self.__verbose:
                logger.warning("Missing instance type info for category %r attribute %r %s", catName, atName, str(e))
        return 0

    def getMaxWidth(self, catName, atName):
        try:
            return self.__tD[catName][atName]["maxWidth"]
        except Exception as e:
            if self.__verbose:
                logger.warning("Missing instance type info for category %r attribute %r %s", catName, atName, str(e))
        return 0

    def getMinPrecision(self, catName, atName):
        try:
            return self.__tD[catName][atName]["minPrec"]
        except Exception as e:
            if self.__verbose:
                logger.warning("Missing instance type info for category %r attribute %r %s", catName, atName, str(e))
        return 0

    def getMaxPrecision(self, catName, atName):
        try:
            return self.__tD[catName][atName]["maxPrec"]
        except Exception as e:
            if self.__verbose:
                logger.warning("Missing instance type info for category %r attribute %r %s", catName, atName, str(e))
        return 0

    def getCount(self, catName, atName):
        try:
            return self.__tD[catName][atName]["count"]
        except Exception as e:
            if self.__verbose:
                logger.warning("Missing instance type info for category %r attribute %r %s", catName, atName, str(e))
        return 0
