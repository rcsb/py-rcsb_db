# File:    ContentDefinitionTests.py
# Author:  J. Westbrook
# Date:    22-May-2013
# Version: 0.001
#
# Update:
#  23-May-2018  jdw add preliminary default and helper tests
#   5-Jun-2018  jdw update prototypes for IoUtil() methods
#  13-Jun-2018  jdw add content classes
#   6-Feb-2019  jdw replace IoUtil() with MarshalUtil()
#
#
#
##
"""
Tests for extraction, supplementing and packaging dictionary metadata for schema construction.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.db.define.ContentDefinition import ContentDefinition
from rcsb.utils.dictionary.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
from rcsb.db.helpers.ContentDefinitionHelper import ContentDefinitionHelper
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ContentDefinitionTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        self.__configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, defaultSectionName=self.__configName, mockTopPath=self.__mockTopPath)
        #
        #
        self.__pathPdbxDictionaryFile = self.__cfgOb.getPath("PDBX_DICT_LOCATOR", sectionName=self.__configName)
        self.__pathRcsbDictionaryFile = self.__cfgOb.getPath("RCSB_DICT_LOCATOR", sectionName=self.__configName)
        self.__pathVrptDictionaryFile = self.__cfgOb.getPath("VRPT_DICT_LOCATOR", sectionName=self.__configName)

        self.__mU = MarshalUtil()
        #
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__dP = DictionaryApiProviderWrapper(self.__cachePath, cfgOb=self.__cfgOb, configName=self.__configName, useCache=True)
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testDefaults(self):
        """Test the default case of using only dictionary content."""
        try:
            dictApi = self.__dP.getApiByLocators(dictLocators=[self.__pathPdbxDictionaryFile])
            ok = dictApi.testCache()
            self.assertTrue(ok)
            sdi = ContentDefinition(dictApi)
            nS = sdi.getSchemaNames()
            logger.debug("schema name length %d", len(nS))
            self.assertGreaterEqual(len(nS), 600)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testHelper(self):
        """Test the dictionary content supplemented by helper function"""
        try:
            cH = ContentDefinitionHelper(cfgOb=self.__cfgOb)
            dictApi = self.__dP.getApiByLocators(dictLocators=[self.__pathPdbxDictionaryFile])
            sdi = ContentDefinition(dictApi, collectionGroupName="chem_comp", contentDefHelper=cH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)

            #
            logger.debug("Dictionary category name length %d", len(catNameL))
            self.assertGreaterEqual(len(catNameL), 600)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtensionWithHelper(self):
        """Test the dictionary content supplemented by helper function"""
        try:
            cH = ContentDefinitionHelper(cfgOb=self.__cfgOb)
            dictApi = self.__dP.getApiByLocators(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile])
            sdi = ContentDefinition(dictApi, collectionGroupName="pdbx_core", contentDefHelper=cH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)

            #
            logger.debug("Dictionary category name length %d", len(catNameL))
            self.assertGreaterEqual(len(catNameL), 650)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testRepoWithHelper(self):
        """Test the dictionary content supplemented by helper function for auxiliary schema"""
        try:
            cH = ContentDefinitionHelper(cfgOb=self.__cfgOb)
            dictApi = self.__dP.getApiByLocators(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile, self.__pathVrptDictionaryFile])
            sdi = ContentDefinition(dictApi, collectionGroupName="repository_holdings", contentDefHelper=cH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)

            #
            logger.debug("Dictionary category name length %d", len(catNameL))
            self.assertGreaterEqual(len(catNameL), 680)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def contentInfoDefaultSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ContentDefinitionTests("testDefaults"))
    return suiteSelect


def contentInfoHelperSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ContentDefinitionTests("testHelper"))
    return suiteSelect


def contentInfoExtensionSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ContentDefinitionTests("testExtensionWithHelper"))
    return suiteSelect


def contentInfoRepoSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ContentDefinitionTests("testRepoWithHelper"))
    return suiteSelect


if __name__ == "__main__":
    #

    mySuite = contentInfoDefaultSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = contentInfoHelperSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = contentInfoExtensionSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = contentInfoRepoSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

#
