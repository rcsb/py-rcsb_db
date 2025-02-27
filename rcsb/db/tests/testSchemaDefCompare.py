##
# File:    SchemaDefCompareTests.py
# Author:  J. Westbrook
# Date:    9-Feb-2020
# Version: 0.001
#
# Update:
##
"""
Tests for comparisons of computed and cached local and json schema defintions from
dictionary metadata and user preference data.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest


from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefCompareTests(unittest.TestCase):
    skipFlag = True

    def setUp(self):
        self.__verbose = True
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=True)
        #
        self.__validationLevels = self.__cfgOb.getList("VALIDATION_LEVELS_TEST", sectionName="database_catalog_configuration")
        self.__encodingTypes = self.__cfgOb.getList("ENCODING_TYPES_TEST", sectionName="database_catalog_configuration")
        #
        buildAll = True
        if buildAll:
            self.__databaseNameList = self.__cfgOb.getList("DATABASE_NAMES_DEPLOYED", sectionName="database_catalog_configuration")
            self.__dataTypingList = self.__cfgOb.getList("DATATYPING_DEPLOYED", sectionName="database_catalog_configuration")
            #
        else:
            self.__databaseNameList = self.__cfgOb.getList("DATABASE_NAMES_TEST", sectionName="database_catalog_configuration")
            # self.__databaseNameList = ["repository_holdings"]
            self.__dataTypingList = self.__cfgOb.getList("DATATYPING_TEST", sectionName="database_catalog_configuration")
        #
        self.__startTime = time.monotonic()
        logger.debug("Starting %s now", self.id())

    def tearDown(self):
        logger.debug("Completed %s in %.3f s", self.id(), time.monotonic() - self.__startTime)

    @unittest.skipIf(skipFlag, "Troubleshooting test")
    def testCompareSchemaDefs(self):
        try:
            difPathList = []
            for databaseName in self.__databaseNameList:
                for dataTyping in self.__dataTypingList:
                    logger.debug("Building schema %s with types %s", databaseName, dataTyping)
                    pth = self.__schP.schemaDefCompare(databaseName, dataTyping)
                    if pth:
                        difPathList.append(pth)
            if difPathList:
                logger.info("Schema definition difference path list %r", [os.path.split(pth)[1] for pth in difPathList])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFlag, "Troubleshooting test")
    def testCompareCollectionSchema(self):
        try:
            difPathList = []
            for databaseName in self.__databaseNameList:
                dD = self.__schP.makeSchemaDef(databaseName, dataTyping="ANY", saveSchema=False)
                sD = SchemaDefAccess(dD)
                for cd in sD.getCollectionInfo():
                    collectionName = cd["NAME"]
                    for encodingType in self.__encodingTypes:
                        if encodingType.lower() != "json":
                            continue
                        for level in self.__validationLevels:
                            pth = self.__schP.jsonSchemaCompare(databaseName, collectionName, encodingType, level)
                            if pth:
                                difPathList.append(pth)
            if difPathList:
                logger.info("JSON schema difference path list %r", [os.path.split(pth)[1] for pth in difPathList])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def schemaCompareSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefCompareTests("testCompareSchemaDefs"))
    suiteSelect.addTest(SchemaDefCompareTests("testCompareCollectionSchema"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = schemaCompareSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
