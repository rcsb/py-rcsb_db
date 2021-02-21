##
# File:   DataTypeApiProvider.py
# Author:  J. Westbrook
# Date:   18-Aug-2019
# Version: 0.001 Initial version
#
# Updates:
#
#
##
"""
Data type application and instance information provider.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.db.define.DataTypeApplicationInfo import DataTypeApplicationInfo
from rcsb.db.define.DataTypeInstanceInfo import DataTypeInstanceInfo
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.SingletonClass import SingletonClass

logger = logging.getLogger(__name__)


class DataTypeApiProvider(SingletonClass):
    """Data type application and instance information provider."""

    def __init__(self, cfgOb, cachePath, useCache=True, **kwargs):
        """Data type application and instance information provider.

        Args:
            cfgOb (object):  ConfigInfo() object instance
            cachePath (str): path to hold the cache directory
            useCache (bool, optional): flag to use cached files. Defaults to True.

        """
        self.__cfgOb = cfgOb
        self.__configName = self.__cfgOb.getDefaultSectionName()
        self.__useCache = useCache
        self.__cachePath = cachePath
        # self.__contentInfoConfigName = "content_info_helper_configuration"
        self.__fileU = FileUtil()
        self.__contentDefHelper = self.__cfgOb.getHelper("CONTENT_DEF_HELPER_MODULE", sectionName=self.__configName, cfgOb=self.__cfgOb)
        self.__dirPath = os.path.join(cachePath, self.__cfgOb.get("DATA_TYPE_INFO_CACHE_DIR", sectionName=self.__configName))
        self.__kwargs = kwargs
        #
        logger.debug("Leaving constructor")

    def getDataTypeInstanceApi(self, databaseName, **kwargs):
        """Return instance of DataTypeInstanceInfo().

        Args:
            databaseName (str): database name

        Returns:
            (object): Instance of DataTypeInstanceInfo()
        """
        _ = kwargs
        dataTypeInstanceLocatorPath = self.__cfgOb.getPath("INSTANCE_DATA_TYPE_INFO_LOCATOR_PATH", sectionName=self.__configName)
        dataTypeInstanceFile = self.__contentDefHelper.getDataTypeInstanceFile(databaseName) if self.__contentDefHelper else None
        if dataTypeInstanceLocatorPath and dataTypeInstanceFile:
            loc = os.path.join(dataTypeInstanceLocatorPath, dataTypeInstanceFile)
            filePath = self.__reload(loc, self.__dirPath, useCache=self.__useCache)
            dtApi = DataTypeInstanceInfo(filePath)
        else:
            # DataTypeInstanceInfo() provides an internal by-pass mode where no coverage data is available.
            dtApi = DataTypeInstanceInfo(None)
            logger.debug("No data coverage available for database %s", databaseName)
        return dtApi

    def getDataTypeApplicationApi(self, appName, **kwargs):
        """Return instance of DataTypeApplicationInfo.

        Args:
            appName (str): application name (e.g., SQL, ANY)

        Returns:
            (object): Instance of DataTypeApplicationInfo()
        """
        _ = kwargs
        dataTypeApplicationLocator = self.__cfgOb.getPath("APP_DATA_TYPE_INFO_LOCATOR", sectionName=self.__configName)
        filePath = self.__reload(dataTypeApplicationLocator, self.__dirPath, useCache=self.__useCache)
        dtApi = DataTypeApplicationInfo(filePath, dataTyping=appName, workPath=self.__dirPath) if filePath else None
        return dtApi

    def __reload(self, urlTarget, dirPath, useCache=True):
        #
        fn = self.__fileU.getFileName(urlTarget)
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
            logger.debug("Fetch data from source %s", urlTarget)
            ok = self.__fileU.get(urlTarget, os.path.join(dirPath, fn))

        return filePath if ok else None
