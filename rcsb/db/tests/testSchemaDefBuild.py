##
# File:    SchemaDefBuildTests.py
# Author:  J. Westbrook
# Date:    9-Jun-2018
# Version: 0.001
#
# Update:
#      7-Sep-2018 jdw Update JSON/BSON schema generation tests
#      7-Oct-2018 jdw update with repository_holdings and  sequence_cluster tests
#     29-Nov-2018 jdw add selected build tests
#     31-Mar-2019 jdw add test to generate schema with $ref to represent parent/child relationsip
#     24-Aug-2019 jdw change over to SchemaProvider()
##
"""
Tests for utilities employed to construct local and json schema defintions from
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
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefBuildTests(unittest.TestCase):
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
        self.__saveSchema = True
        self.__compareSchema = False
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testBuildSchemaDefs(self):
        try:
            for databaseName in self.__databaseNameList:
                for dataTyping in self.__dataTypingList:
                    logger.info("Building schema %s with types %s", databaseName, dataTyping)
                    self.__schP.makeSchemaDef(databaseName, dataTyping=dataTyping, saveSchema=self.__saveSchema)
                    if self.__compareSchema:
                        self.__schP.schemaDefCompare(databaseName, dataTyping)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBuildCollectionSchema(self):
        for databaseName in self.__databaseNameList:
            dD = self.__schP.makeSchemaDef(databaseName, dataTyping="ANY", saveSchema=False)
            sD = SchemaDefAccess(dD)
            for cd in sD.getCollectionInfo():
                collectionName = cd["NAME"]
                for encodingType in self.__encodingTypes:
                    if encodingType.lower() == "rcsb":
                        continue
                    for level in self.__validationLevels:
                        self.__schP.makeSchema(databaseName, collectionName, encodingType=encodingType, level=level, saveSchema=self.__saveSchema)
                        if self.__compareSchema and encodingType.lower() == "json":
                            self.__schP.jsonSchemaCompare(databaseName, collectionName, encodingType, level)

    def testCompareSchema(self):
        databaseName = "pdbx_core"
        collectionName = "pdbx_core_entry"
        encodingType = "json"
        level = "full"
        #
        oldPath = os.path.join(HERE, "test-saved-output", "json-full-db-pdbx_core-col-pdbx_core_entry.json")
        mU = MarshalUtil(workPath=os.path.join(HERE, "test-output"))
        sOld = mU.doImport(oldPath, fmt="json")
        sNew = self.__schP.makeSchema(databaseName, collectionName, encodingType=encodingType, level=level)
        numDif, difD = self.__schP.schemaCompare(sOld, sNew)
        logger.debug("numDiffs %d", numDif)
        self.assertGreaterEqual(numDif, 141)
        self.assertGreaterEqual(len(difD["changed"]), 160)
        logger.debug("difD %r", difD)

    @unittest.skip("Deprecated test")
    def testCompareSchemaCategories(self):
        """ Compare common categories across schema definitions.
        """
        try:
            sdCc = SchemaDefAccess(self.__schP.makeSchemaDef("chem_comp_core", dataTyping="ANY", saveSchema=False))
            sdBcc = SchemaDefAccess(self.__schP.makeSchemaDef("bird_chem_comp_core", dataTyping="ANY", saveSchema=False))
            #
            logger.info("")
            for schemaId in ["CHEM_COMP", "PDBX_CHEM_COMP_AUDIT"]:
                atCcL = sdCc.getAttributeIdList(schemaId)
                atBCcL = sdBcc.getAttributeIdList(schemaId)

                logger.debug("%s attributes (%d) %r", schemaId, len(atCcL), atCcL)
                logger.debug("%s attributes (%d) %r", schemaId, len(atBCcL), atBCcL)

                sDif = set(atCcL) - set(atBCcL)
                if sDif:
                    logger.info("For %s attribute differences %r", schemaId, sDif)
                self.assertEqual(len(sDif), 0)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBuildColSchemaWithRefs(self):
        for databaseName in ["ihm_dev_full"]:
            dD = self.__schP.makeSchemaDef(databaseName, dataTyping="ANY", saveSchema=False)
            sD = SchemaDefAccess(dD)
            for cd in sD.getCollectionInfo():
                collectionName = cd["NAME"]
                for schemaType in self.__encodingTypes:
                    if schemaType.lower() == "rcsb":
                        continue
                    for level in self.__validationLevels:
                        self.__schP.makeSchema(
                            databaseName, collectionName, encodingType=schemaType, level=level, saveSchema=self.__saveSchema, extraOpts="addParentRefs|addPrimaryKey"
                        )


def schemaBuildSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefBuildTests("testBuildSchemaDefs"))
    suiteSelect.addTest(SchemaDefBuildTests("testBuildCollectionSchema"))
    suiteSelect.addTest(SchemaDefBuildTests("testCompareSchemaCategories"))
    return suiteSelect


def schemaBuildRefSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefBuildTests("testBuildColSchemaWithRefs"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = schemaBuildSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = schemaBuildRefSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
