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

from rcsb.db.mongo.ChemRefExtractor import ChemRefExtractor
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.chemref.ChemRefDataPrep import ChemRefDataPrep
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
        self.__pathConfig = os.path.join(self.__mockTopPath, "config", "dbload-setup-example.yml")
        self.__workPath = os.path.join(HERE, "test-output")
        #
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, defaultSectionName="site_info", mockTopPath=self.__mockTopPath)
        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb)
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
            crdp = ChemRefDataPrep(self.__cfgOb)
            crExt = ChemRefExtractor(self.__cfgOb)
            idD = crExt.getChemCompAccesionMapping(extResource)
            dList = crdp.getDocuments(extResource, idD)
            eCount = self.__validate(schemaName, collectionNames, dList, schemaLevel=schemaLevel)

        return eCount

    def __validate(self, schemaName, collectionNames, dList, schemaLevel="full"):

        eCount = 0
        for collectionName in collectionNames:
            _ = self.__schU.makeSchemaDef(schemaName, dataTyping="ANY", saveSchema=True, altDirPath=self.__workPath)
            cD = self.__schU.makeSchema(schemaName, collectionName, schemaType="JSON", level=schemaLevel, saveSchema=True, altDirPath=self.__workPath)
            # Raises exceptions for schema compliance.
            Draft4Validator.check_schema(cD)
            #
            valInfo = Draft4Validator(cD, format_checker=FormatChecker())
            for ii, dD in enumerate(dList):
                logger.debug("Schema %s collection %s document %d", schemaName, collectionName, ii)
                try:
                    cCount = 0
                    for error in sorted(valInfo.iter_errors(dD), key=str):
                        logger.info("schema %s collection %s path %s error: %s", schemaName, collectionName, error.path, error.message)
                        logger.info(">>> failing object is %r", dD)
                        eCount += 1
                        cCount += 1
                    #
                    logger.debug("schema %s collection %s count %d", schemaName, collectionName, cCount)
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
