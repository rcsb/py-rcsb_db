##
# File:    DictionaryProvider.py
# Author:  J. Westbrook
# Date:    3-Jun-2019
# Version: 0.001 Initial version
#
#
# Updates:
#
##
"""
Resource provider for dictionary API.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.utils.SingletonClass import SingletonClass
from rcsb.utils.io.MarshalUtil import MarshalUtil

from mmcif.api.DictionaryApi import DictionaryApi

logger = logging.getLogger(__name__)


class DictionaryProvider(SingletonClass):
    """ Resource provider for dictionary API.
    """

    def __init__(self):
        """Resource provider for dictionary method runner.

        Arguments:
            dictLocators {list str} -- list of dictionary locator paths
        """
        self.__apiMap = {}
        logger.info("Leaving constructor")

    def getApi(self, dictLocators, **kwargs):
        """Return a dictionary API object of the input dictioaries.

        Arguments:
            dictLocators {list str} -- list of dictionary locator paths

        Returns:
            [object] -- returns DictionaryApi() object for input dictionaries
        """
        dictTup = tuple(dictLocators)
        dApi = self.__apiMap[dictTup] if dictTup in self.__apiMap else self.__getApi(dictLocators, **kwargs)
        self.__apiMap[dictTup] = dApi
        return dApi

    def __getApi(self, dictLocators, **kwargs):
        """ Return an instance of a dictionary API instance for the input dictionary locator list.
        """
        consolidate = kwargs.get("consolidate", True)
        replaceDefinition = kwargs.get("replaceDefinitions", True)
        verbose = kwargs.get("verbose", True)
        mU = MarshalUtil()
        containerList = []
        for dictLocator in dictLocators:
            containerList.extend(mU.doImport(dictLocator, fmt="mmcif-dict"))
        #
        dApi = DictionaryApi(containerList=containerList, consolidate=consolidate, replaceDefinition=replaceDefinition, verbose=verbose)
        return dApi
