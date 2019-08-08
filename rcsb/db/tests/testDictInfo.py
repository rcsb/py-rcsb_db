# File:    DictInfoTests.py
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

from rcsb.db.define.DictInfo import DictInfo
from rcsb.db.define.DictionaryProvider import DictionaryProvider
from rcsb.db.helpers.DictInfoHelper import DictInfoHelper
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DictInfoTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__pathConfig = os.path.join(self.__mockTopPath, "config", "dbload-setup-example.yml")
        configName = "site_info"
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        #
        self.__pathPdbxDictionaryFile = self.__cfgOb.getPath("PDBX_DICT_LOCATOR", sectionName=configName)
        self.__pathRcsbDictionaryFile = self.__cfgOb.getPath("RCSB_DICT_LOCATOR", sectionName=configName)
        self.__pathVrptDictionaryFile = self.__cfgOb.getPath("VRPT_DICT_LOCATOR", sectionName=configName)

        self.__mU = MarshalUtil()
        #
        self.__pathSaveDictInfoDefaultJson = os.path.join(HERE, "test-output", "dict_info_default.json")
        self.__pathSaveDictInfoJson = os.path.join(HERE, "test-output", "dict_info.json")
        self.__pathSaveDictInfoExtJson = os.path.join(HERE, "test-output", "dict_info_with_ext.json")
        self.__pathSaveDictInfoRepoJson = os.path.join(HERE, "test-output", "dict_info_with_repo.json")
        self.__pathSaveDefText = os.path.join(HERE, "test-output", "dict_info.txt")
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testDefaults(self):
        """ Test the default case of using only dictionary content.
        """
        try:
            dP = DictionaryProvider()
            dictApi = dP.getApi(dictLocators=[self.__pathPdbxDictionaryFile])
            sdi = DictInfo(dictApi)
            nS = sdi.getSchemaNames()
            #
            logger.debug("Dictionary category name length %d", len(nS))
            ok = self.__mU.doExport(self.__pathSaveDictInfoDefaultJson, nS, fmt="json", indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testHelper(self):
        """ Test the dictionary content supplemented by helper function

        """
        try:
            dH = DictInfoHelper(cfgOb=self.__cfgOb)
            dP = DictionaryProvider()
            dictApi = dP.getApi(dictLocators=[self.__pathPdbxDictionaryFile])
            sdi = DictInfo(dictApi, dictSubset="chem_comp", dictHelper=dH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)

            #
            logger.debug("Dictionary category name length %d", len(catNameL))
            ok = self.__mU.doExport(self.__pathSaveDictInfoJson, afD, fmt="json", indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtensionWithHelper(self):
        """ Test the dictionary content supplemented by helper function

        """
        try:
            dH = DictInfoHelper(cfgOb=self.__cfgOb)
            dP = DictionaryProvider()
            dictApi = dP.getApi(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile])
            sdi = DictInfo(dictApi, dictSubset="pdbx_core", dictHelper=dH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)

            #
            logger.debug("Dictionary category name length %d", len(catNameL))
            ok = self.__mU.doExport(self.__pathSaveDictInfoExtJson, afD, fmt="json", indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testRepoWithHelper(self):
        """ Test the dictionary content supplemented by helper function for auxiliary schema

        """
        try:
            dH = DictInfoHelper(cfgOb=self.__cfgOb)
            dP = DictionaryProvider()
            dictApi = dP.getApi(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile, self.__pathVrptDictionaryFile])
            sdi = DictInfo(dictApi, dictSubset="repository_holdings", dictHelper=dH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)

            #
            logger.debug("Dictionary category name length %d", len(catNameL))
            ok = self.__mU.doExport(self.__pathSaveDictInfoRepoJson, afD, fmt="json", indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def dictInfoDefaultSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictInfoTests("testDefaults"))
    return suiteSelect


def dictInfoHelperSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictInfoTests("testHelper"))
    return suiteSelect


def dictInfoExtensionSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictInfoTests("testExtensionWithHelper"))
    return suiteSelect


def dictInfoRepoSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictInfoTests("testRepoWithHelper"))
    return suiteSelect


if __name__ == "__main__":
    #

    mySuite = dictInfoDefaultSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = dictInfoHelperSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = dictInfoExtensionSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = dictInfoRepoSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

#
