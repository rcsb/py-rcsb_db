##
# File:    SchemaProviderTests.py
# Author:  J. Westbrook
# Date:    9-Dec-2019
# Version: 0.001
#
# Update:
##
"""
Tests for essential access features of SchemaProvider() module

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
from rcsb.db.define.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
from rcsb.db.helpers.ContentDefinitionHelper import ContentDefinitionHelper
from rcsb.db.helpers.DocumentDefinitionHelper import DocumentDefinitionHelper
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaSearchContextsTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__docHelper = DocumentDefinitionHelper(cfgOb=self.__cfgOb)
        #
        self.__pathPdbxDictionaryFile = self.__cfgOb.getPath("PDBX_DICT_LOCATOR", sectionName=configName)
        self.__pathRcsbDictionaryFile = self.__cfgOb.getPath("RCSB_DICT_LOCATOR", sectionName=configName)
        self.__pathVrptDictionaryFile = self.__cfgOb.getPath("VRPT_DICT_LOCATOR", sectionName=configName)

        # self.__mU = MarshalUtil()
        #
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__dP = DictionaryApiProviderWrapper(self.__cfgOb, self.__cachePath, useCache=True)

        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSearchGroups(self):
        ok = self.__docHelper.checkSearchGroups()
        self.assertTrue(ok)

    def testExpandSearchGroups(self):
        """Expand search groups and metadata content as these would be display in RCSB search menu.
        """
        # cfD, afD = self.__getContentFeatures()
        _, afD = self.__getContentFeatures()

        groupNameList = self.__docHelper.getSearchGroups()
        logger.info("Search groups (%d)", len(groupNameList))
        for groupName in groupNameList:
            # get attributes in group
            attributeTupList = self.__docHelper.getSearchGroupAttributes(groupName)
            logger.info("Search Group (%2d): %s", len(attributeTupList), groupName)
            # get search context and brief descriptions -
            for catName, atName in attributeTupList:
                searchContextTupL = self.__docHelper.getSearchContexts(catName, atName)
                if not searchContextTupL:
                    logger.warning("Missing search context for %s.%s", catName, atName)
                descriptionText = self.__docHelper.getAttributeDescription(catName, atName, contextType="brief")
                if not descriptionText:
                    logger.warning("Missing brief description %s.%s", catName, atName)
                #
                fD = afD[catName] if catName in afD else {}
                units = fD["UNITS"] if "UNITS" in fD else None
                enumD = fD["ENUMS_ANNOTATED"] if "ENUMS_ANNOTATED" in fD else None
                logger.debug("units %r and enumD %r", units, enumD)

                nestedContextDL = self.__docHelper.getNestedContexts(catName)
                for nestedContextD in nestedContextDL:
                    contextName = nestedContextD["CONTEXT_NAME"]
                    contextPath = nestedContextD["CONTEXT_PATH"] if "CONTEXT_PATH" in nestedContextD else None
                    if contextPath:
                        cpCatName = contextPath.split(".")[0]
                        cpAtName = contextPath.split(".")[1]
                        nestedPathSearchContext = self.__docHelper.getSearchContexts(cpCatName, cpAtName)
                        if not nestedPathSearchContext:
                            logger.warning("Missing nested (%r) search context for %r %r", contextName, cpCatName, cpAtName)
                #
                logger.debug("  %r %r -> %r (%s)", catName, atName, descriptionText, ",".join([tup[0] for tup in searchContextTupL]))
        return True

    def __getContentFeatures(self):
        """ Get category and attribute features
        """
        try:
            cH = ContentDefinitionHelper(cfgOb=self.__cfgOb)
            dictApi = self.__dP.getApiByLocators(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile])
            sdi = ContentDefinition(dictApi, databaseName="pdbx_core", contentDefHelper=cH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)
            #
            return cfD, afD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None, None


def schemaSearchGroupSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaSearchContextsTests("testSearchGroups"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = schemaSearchGroupSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
