##
# File:    testDictionaryApiProvider.py
# Author:  J. Westbrook
# Date:    15-Aug-2019
# Version: 0.001
#
# Update:

##
"""
Tests for dictionary API provider and cache.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.db.define.DictionaryApiProvider import DictionaryApiProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DictionaryProviderTests(unittest.TestCase):
    def setUp(self):
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__dirPath = os.path.join(self.__cachePath, "dictionaries")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__configName = configName
        self.__contentInfoConfigName = "content_info_helper_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        dictLocatorMap = self.__cfgOb.get("DICT_LOCATOR_CONFIG_MAP", sectionName=self.__contentInfoConfigName)
        schemaName = "pdbx_core"
        self.__dictLocators = [self.__cfgOb.getPath(configLocator, sectionName=self.__contentInfoConfigName) for configLocator in dictLocatorMap[schemaName]]
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testResourceCache(self):
        """Test case - generate and check dictonary artifact and api caches
        """
        try:
            logger.debug("Dictionary locators %r", self.__dictLocators)
            dp = DictionaryApiProvider(dirPath=self.__dirPath, useCache=False)
            dApi = dp.getApi(self.__dictLocators)
            ok = dApi.testCache()
            self.assertTrue(ok)
            title = dApi.getDictionaryTitle()
            logger.debug("Title %r", title)
            self.assertEqual(title, "mmcif_pdbx.dic,rcsb_mmcif_ext.dic,vrpt_mmcif_ext.dic")
            # revL = dApi.getDictionaryHistory()
            numRev = dApi.getDictionaryRevisionCount()
            logger.debug("Number of dictionary revisions (numRev) %r", numRev)
            self.assertGreater(numRev, 220)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def dictionaryProviderSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictionaryProviderTests("testResourceCache"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = dictionaryProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
