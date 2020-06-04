##
# File:    testDictionaryApiProviderWrapper.py
# Author:  J. Westbrook
# Date:    15-Aug-2019
# Version: 0.001
#
# Update:

##
"""
Tests for dictionary API provider wrapper.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.db.define.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
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

        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__configName = configName
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        #
        self.__contentInfoConfigName = "content_info_helper_configuration"
        dictLocatorMap = self.__cfgOb.get("DICT_LOCATOR_CONFIG_MAP", sectionName=self.__contentInfoConfigName)
        self.__databaseName = "pdbx_core"
        self.__dictLocators = [self.__cfgOb.getPath(configLocator, sectionName=self.__configName) for configLocator in dictLocatorMap[self.__databaseName]]
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testWrapperByName(self):
        """Test case - get dictionary API by schema name
        """
        try:
            dp = DictionaryApiProviderWrapper(self.__cfgOb, self.__cachePath, useCache=False)
            dApi = dp.getApiByName(self.__databaseName)
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

    def testWrapperByLocators(self):
        """Test case - get dictionary API by locator list
        """
        try:
            dp = DictionaryApiProviderWrapper(self.__cfgOb, self.__cachePath, useCache=False)
            dApi = dp.getApiByLocators(self.__dictLocators)
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
    suiteSelect.addTest(DictionaryProviderTests("testWrapperByName"))
    suiteSelect.addTest(DictionaryProviderTests("testWrapperByLocators"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = dictionaryProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
