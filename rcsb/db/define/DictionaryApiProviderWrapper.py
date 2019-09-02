##
# File:    DictionaryApiProviderWrapper.py
# Author:  J. Westbrook
# Date:   18-Aug-2019
# Version: 0.001 Initial version
#
# Updates:
#
##
"""
Wrapper for dictionary API provider.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os.path

from rcsb.db.define.DictionaryApiProvider import DictionaryApiProvider
from rcsb.utils.io.SingletonClass import SingletonClass

logger = logging.getLogger(__name__)


class DictionaryApiProviderWrapper(SingletonClass):
    """ Wrapper for dictionary API provider.
    """

    def __init__(self, cfgOb, cachePath, useCache=True, **kwargs):
        """Wrapper for dictionary API provider.

        Args:
            cfgOb (object):  ConfigInfo() object instance
            cachePath (str): top path to contain the dictionary cache directory
            useCache (bool, optional): flag to use cached files. Defaults to True.

        """
        self.__cfgOb = cfgOb
        self.__configName = self.__cfgOb.getDefaultSectionName()
        self.__contentInfoConfigName = "content_info_helper_configuration"
        self.__dictLocatorMap = self.__cfgOb.get("DICT_LOCATOR_CONFIG_MAP", sectionName=self.__contentInfoConfigName)
        dirPath = os.path.join(cachePath, self.__cfgOb.get("DICTIONARY_CACHE_DIR", sectionName=self.__configName))
        self.__dP = DictionaryApiProvider(dirPath, useCache=useCache, **kwargs)
        logger.debug("Leaving constructor")

    def getApiByLocators(self, dictLocators, **kwargs):
        """Return a dictionary API object for the input dictionary locator list.

        Args:
            dictLocators (list str): list of dictionary locators

        Returns:
            (object): Instance of DictionaryApi()
        """
        return self.__dP.getApi(dictLocators, **kwargs)

    def getApiByName(self, databaseName, **kwargs):
        """Return a dictionary API object for the input schema name.

        Args:
            databaseName (str): database schema name

        Returns:
            (object): Instance of DictionaryApi()
        """
        if databaseName not in self.__dictLocatorMap:
            logger.error("Missing dictionary locator configuration for database schema %s", databaseName)
            dictLocators = []
        else:
            dictLocators = [self.__cfgOb.getPath(configLocator, sectionName=self.__contentInfoConfigName) for configLocator in self.__dictLocatorMap[databaseName]]
        #
        return self.__dP.getApi(dictLocators, **kwargs)
