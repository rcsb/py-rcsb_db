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

from rcsb.db.helpers.DocumentDefinitionHelper import DocumentDefinitionHelper
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaProviderTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=False)
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
            self.__dataTypingList = self.__cfgOb.getList("DATATYPING_TEST", sectionName="database_catalog_configuration")
        #
        self.__docHelper = DocumentDefinitionHelper(cfgOb=self.__cfgOb)
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSchemaAccessDefault(self):
        for databaseName in self.__databaseNameList:
            cDL = self.__docHelper.getCollectionInfo(databaseName)
            for cD in cDL:
                collectionName = cD["NAME"]
                for encodingType in self.__encodingTypes:
                    if encodingType.lower() == "rcsb":
                        continue
                    for level in self.__validationLevels:
                        logger.debug("Loading ->%s %s %s %s", databaseName, collectionName, encodingType, level)
                        sD = self.__schP.getJsonSchema(databaseName, collectionName, encodingType=encodingType, level=level)
                        self.assertTrue(sD is not None)


def schemaProviderSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaProviderTests("testSchemaAccessDefault"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = schemaProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
