##
# File:    ProvenanceProvider.py
# Author:  J. Westbrook
# Date:    25-Aug-2019
# Version: 0.001
#
# Updates:
#
##
"""
Utilities to access and update provenance details.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os

from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.SingletonClass import SingletonClass

logger = logging.getLogger(__name__)


class ProvenanceProvider(SingletonClass):
    """Utilities to access and update provenance details."""

    def __init__(self, cfgOb, cachePath, useCache=True, **kwargs):
        """Utilities to access and update provenance details.

        Args:
            cfgOb ([type]): ConfigInfo() instance
            cachePath ([type]): path to directory containing schema
            useCache (bool, optional): use cached schema. Defaults to True.
        """

        self.__cfgOb = cfgOb
        self.__configName = self.__cfgOb.getDefaultSectionName()
        self.__cachePath = cachePath
        self.__useCache = useCache
        #
        self.__workPath = os.path.join(self.__cachePath, "work")
        self.__provenanceCachePath = os.path.join(self.__cachePath, self.__cfgOb.get("PROVENANCE_INFO_CACHE_DIR", sectionName=self.__configName))
        self.__provenanceLocator = self.__cfgOb.getPath("PROVENANCE_INFO_LOCATOR", sectionName=self.__configName)
        #
        self.__fileU = FileUtil(workPath=self.__workPath)
        self.__fileU.mkdir(self.__provenanceCachePath)
        self.__kwargs = kwargs
        #

    def __reload(self, locator, dirPath, useCache=True):
        #
        fn = self.__fileU.getFileName(locator)
        filePath = os.path.join(dirPath, fn)
        logger.debug("Using cache path %s", dirPath)
        self.__fileU.mkdir(dirPath)
        if not useCache:
            try:
                os.remove(filePath)
            except Exception:
                pass
        #
        if useCache and self.__fileU.exists(filePath):
            ok = True
        else:
            logger.debug("Fetch data from source %s", locator)
            ok = self.__fileU.get(locator, filePath)

        return filePath if ok else None

    def fetch(self):
        try:
            provenanceFileCachePath = self.__reload(self.__provenanceLocator, self.__provenanceCachePath, useCache=self.__useCache)
            mU = MarshalUtil(workPath=self.__workPath)
            return mU.doImport(provenanceFileCachePath, fmt="json")
        except Exception as e:
            logger.exception("Failed retreiving provenance with %s", str(e))
        return {}

    def update(self, provD):
        ok = False
        try:
            provenanceFileCachePath = self.__reload(self.__provenanceLocator, self.__provenanceCachePath, useCache=self.__useCache)
            mU = MarshalUtil(workPath=self.__workPath)
            tD = mU.doImport(provenanceFileCachePath, fmt="json")
            tD.update(provD)
            ok = mU.doExport(provenanceFileCachePath, tD, fmt="json")
        except Exception as e:
            logger.exception("Failed updating provenance with %s", str(e))
        return ok

    def store(self, provD):
        ok = False
        try:
            provenanceFileCachePath = self.__reload(self.__provenanceLocator, self.__provenanceCachePath, useCache=self.__useCache)
            mU = MarshalUtil(workPath=self.__workPath)
            ok = mU.doExport(provenanceFileCachePath, provD, fmt="json")
        except Exception as e:
            logger.exception("Failed storing provenance with %s", str(e))
        return ok
