##
# File:    DictionaryApiProvider.py
# Author:  J. Westbrook
# Date:    3-Jun-2019
# Version: 0.001 Initial version
#
# Updates:
#  14-Aug-2019 jdw adding remote dictionary fetch and caching logic.
##
"""
Resource provider for dictionary API.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from mmcif.api.DictionaryApi import DictionaryApi
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.SingletonClass import SingletonClass

logger = logging.getLogger(__name__)


class DictionaryApiProvider(SingletonClass):
    """ Resource provider for dictionary APIs.
    """

    def __init__(self, dirPath, useCache=True):
        """Resource provider for dictionary APIs.

        Args:
            dirPath (str): path to the directory containing cache files
            useCache (bool, optional): flag to use cached files. Defaults to True.

        """
        self.__apiMap = {}
        self.__dirPath = dirPath
        self.__useCache = useCache
        #
        self.__fileU = FileUtil(workPath=self.__dirPath)
        logger.debug("Leaving constructor")

    def __reload(self, dictLocators, dirPath, useCache=True):
        """Reload local cache of dictionary resources and return a dictionary API instance.

        Args:
            dictLocators (list, str): list of locators for dictionary resource files
            dirPath (str): path to the directory containing cache files
            useCache (bool, optional): flag to use cached files. Defaults to True.

        Returns:
            (object): instance of dictionary API
        """
        #
        # verify the exitence of the cache directory ...
        self.__fileU.mkdir(dirPath)
        if not useCache:
            for dictLocator in dictLocators:
                try:
                    fn = self.__fileU.getFileName(dictLocator)
                    os.remove(os.path.join(dirPath, fn))
                except Exception:
                    pass
        #
        ret = True
        for dictLocator in dictLocators:
            cacheFilePath = os.path.join(dirPath, self.__fileU.getFileName(dictLocator))
            if useCache and self.__fileU.exists(cacheFilePath):
                # nothing to do
                continue
            logger.debug("Fetching url %s caching in %s", dictLocator, cacheFilePath)
            ok = self.__fileU.get(dictLocator, cacheFilePath)
            ret = ret and ok
        return ret

    def getApi(self, dictLocators, **kwargs):
        """Return a dictionary API object of the input dictioaries.

        Arguments:
            dictLocators {list str} -- list of dictionary locator paths

        Returns:
            [object] -- returns DictionaryApi() object for input dictionaries
        """
        dictFileNames = [self.__fileU.getFileName(dictLocator) for dictLocator in dictLocators]
        dictTup = tuple(dictFileNames)
        dApi = self.__apiMap[dictTup] if dictTup in self.__apiMap else self.__getApi(dictLocators, **kwargs)
        self.__apiMap[dictTup] = dApi
        return dApi

    def __getApi(self, dictLocators, **kwargs):
        """ Return an instance of a dictionary API instance for the input dictionary locator list.
        """
        consolidate = kwargs.get("consolidate", True)
        replaceDefinition = kwargs.get("replaceDefinitions", True)
        verbose = kwargs.get("verbose", True)
        #
        ok = self.__reload(dictLocators, self.__dirPath, useCache=self.__useCache)
        #
        dApi = None
        if ok:
            mU = MarshalUtil()
            containerList = []
            for dictLocator in dictLocators:
                cacheFilePath = os.path.join(self.__dirPath, self.__fileU.getFileName(dictLocator))
                containerList.extend(mU.doImport(cacheFilePath, fmt="mmcif-dict"))
            #
            dApi = DictionaryApi(containerList=containerList, consolidate=consolidate, replaceDefinition=replaceDefinition, verbose=verbose)
        return dApi
