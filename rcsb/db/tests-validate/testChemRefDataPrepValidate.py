# File:    ChemRefDataPrepValidateTests.py
# Author:  J. Westbrook
# Date:    7-Jan-2019
# Version: 0.001
#
# Update:
#
##
"""
Tests for processing and validating integrated chemical reference data.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from jsonschema import Draft4Validator
from jsonschema import FormatChecker

# from rcsb.db.mongo.ChemRefExtractor import ChemRefExtractor
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.chemref.DrugBankProvider import DrugBankProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ChemRefDataPrepValidateTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        #
        self.__configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, defaultSectionName=self.__configName, mockTopPath=self.__mockTopPath)
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=True)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testValidateFull(self):
        self.__validateChemRef("DrugBank", schemaLevel="full")

    def __validateChemRef(self, extResource, schemaLevel="full"):
        eCount = 0
        if extResource == "DrugBank":
            schemaName = "drugbank_core"
            collectionNames = ["drugbank_core"]
            user = self.__cfgOb.get("_DRUGBANK_AUTH_USERNAME", sectionName=self.__configName)
            pw = self.__cfgOb.get("_DRUGBANK_AUTH_PASSWORD", sectionName=self.__configName)
            cacheDir = self.__cfgOb.get("DRUGBANK_CACHE_DIR", sectionName=self.__configName)
            dbP = DrugBankProvider(dirPath=os.path.join(self.__cachePath, cacheDir), useCache=True, username=user, password=pw)
            # idD = dbP.getMapping()
            # crExt = ChemRefExtractor(self.__cfgOb)
            # idD = crExt.getChemCompAccesionMapping(extResource)
            dList = dbP.getDocuments()
            logger.info("Validating %d Drugbank documents", len(dList))
            eCount = self.__validate(schemaName, collectionNames, dList, schemaLevel=schemaLevel)

        return eCount

    def __validate(self, databaseName, collectionNames, dList, schemaLevel="full"):

        eCount = 0
        for collectionName in collectionNames:
            _ = self.__schP.makeSchemaDef(databaseName, dataTyping="ANY", saveSchema=True)
            cD = self.__schP.makeSchema(databaseName, collectionName, encodingType="JSON", level=schemaLevel, saveSchema=True)
            # Raises exceptions for schema compliance.
            Draft4Validator.check_schema(cD)
            #
            valInfo = Draft4Validator(cD, format_checker=FormatChecker())
            for ii, dD in enumerate(dList):
                logger.debug("Database %s collection %s document %d", databaseName, collectionName, ii)
                try:
                    cCount = 0
                    for error in sorted(valInfo.iter_errors(dD), key=str):
                        logger.info("database %s collection %s path %s error: %s", databaseName, collectionName, error.path, error.message)
                        logger.info(">>> failing object is %r", dD)
                        eCount += 1
                        cCount += 1
                    #
                    logger.debug("database %s collection %s count %d", databaseName, collectionName, cCount)
                except Exception as e:
                    logger.exception("Validation error %s", str(e))

        return eCount


def chemRefValidateSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemRefDataPrepValidateTests("testValidateFull"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = chemRefValidateSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
