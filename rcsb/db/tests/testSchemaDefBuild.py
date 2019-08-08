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
import pprint
import time
import unittest

from jsondiff import diff

from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
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
        pathConfig = os.path.join(mockTopPath, "config", "dbload-setup-example.yml")
        configName = "site_info"
        self.__workPath = os.path.join(HERE, "test-output")
        self.__savePath = os.path.join(HERE, "test-saved-output")
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb)
        self.__mU = MarshalUtil(workPath=self.__workPath)
        #
        self.__schemaLevels = self.__cfgOb.getList("SCHEMA_LEVELS_TEST", sectionName="schema_catalog_info")
        self.__schemaTypes = self.__cfgOb.getList("SCHEMA_TYPES_TEST", sectionName="schema_catalog_info")
        #
        self.__schemaNameList = self.__cfgOb.getList("SCHEMA_NAMES_TEST", sectionName="schema_catalog_info")
        self.__dataTypingList = self.__cfgOb.getList("DATATYPING_TEST", sectionName="schema_catalog_info")
        self.__saveSchema = True
        self.__diffFlag = False
        self.__rewriteFlag = True
        self.__emptyPatchS = "[]"
        self.maxDiff = None
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testBuildSchemaDefs(self):
        try:
            for schemaName in self.__schemaNameList:
                for dataTyping in self.__dataTypingList:
                    self.__schU.makeSchemaDef(schemaName, dataTyping=dataTyping, saveSchema=True, altDirPath=self.__workPath)
                    self.__schemaCompare(schemaName, dataTyping, self.__diffFlag, self.__rewriteFlag)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBuildCollectionSchema(self):
        for schemaName in self.__schemaNameList:
            dD = self.__schU.makeSchemaDef(schemaName, dataTyping="ANY", saveSchema=False, altDirPath=None)
            sD = SchemaDefAccess(dD)
            for cd in sD.getCollectionInfo():
                collectionName = cd["NAME"]
                for schemaType in self.__schemaTypes:
                    if schemaType.lower() == "rcsb":
                        continue
                    for level in self.__schemaLevels:
                        self.__schU.makeSchema(schemaName, collectionName, schemaType=schemaType, level=level, saveSchema=self.__saveSchema, altDirPath=self.__workPath)
                        self.__jsonSchemaCompare(collectionName, schemaType, level, self.__diffFlag, self.__rewriteFlag)

    def __getVersion(self, jsonSchema):
        try:
            comment = jsonSchema["$comment"] if "$comment" in jsonSchema else ""
            ff = comment.split(":")
            version = ff[1].strip()
            return version
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ""

    def __jsonSchemaCompare(self, collectionName, schemaType, level, diffFlag, rewriteFlag):
        #
        if rewriteFlag:
            fPath1 = self.__schU.getJsonSchemaLocator(collectionName, schemaType, level, altDirPath=self.__workPath)
            sD = self.__mU.doImport(fPath1, fmt="json")
            sDRef = self.__mU.doExport(fPath1, sD, fmt="json", indent=3)
        if diffFlag:
            fPath1 = self.__schU.getJsonSchemaLocator(collectionName, schemaType, level, altDirPath=self.__workPath)
            sD = self.__mU.doImport(fPath1, fmt="json")
            v2 = self.__getVersion(sD)
            fPath2 = self.__schU.getJsonSchemaLocator(collectionName, schemaType, level, altDirPath=self.__savePath)
            sDRef = self.__mU.doImport(fPath2, fmt="json")
            v1 = self.__getVersion(sDRef)
            self.assertEqual(len(sDRef), len(sD))
            #
            jD = diff(sDRef, sD, marshal=True, syntax="explicit")
            if jD:
                logger.debug("jsondiff for %s %s %s = \n%s", collectionName, schemaType, level, pprint.pformat(jD, indent=3, width=100))
                _, fn = os.path.split(fPath2)
                bn, _ = os.path.splitext(fn)
                fPath = os.path.join(self.__workPath, bn + v1 + "-" + v2 + "-diff.json")
                self.__mU.doExport(fPath, jD, fmt="json", indent=3)
            self.assertEqual(len(jD), 0)

    def __schemaCompare(self, schemaName, dataTyping, diffFlag, rewriteFlag):
        #
        if rewriteFlag:
            fPath1 = self.__schU.getSchemaDefLocator(schemaName, dataTyping=dataTyping, altDirPath=self.__workPath)
            sD = self.__mU.doImport(fPath1, fmt="json")
            sDRef = self.__mU.doExport(fPath1, sD, fmt="json", indent=3)
        if diffFlag:
            fPath1 = self.__schU.getSchemaDefLocator(schemaName, dataTyping=dataTyping, altDirPath=self.__workPath)
            sD = self.__mU.doImport(fPath1, fmt="json")
            v2 = sD["DATABASE_VERSION"]
            fPath2 = self.__schU.getSchemaDefLocator(schemaName, dataTyping=dataTyping, altDirPath=self.__savePath)
            sDRef = self.__mU.doImport(fPath2, fmt="json")
            v1 = sDRef["DATABASE_VERSION"]
            self.assertEqual(len(sDRef), len(sD))
            #
            jD = diff(sDRef, sD, syntax="explicit", marshal=True)
            if jD:
                _, fn = os.path.split(fPath2)
                bn, _ = os.path.splitext(fn)
                fPath = os.path.join(self.__workPath, bn + v1 + "-" + v2 + "-diff.json")
                logger.debug("jsondiff for %s %s = \n%s", schemaName, dataTyping, pprint.pformat(jD, indent=3, width=100))
                self.__mU.doExport(fPath, jD, fmt="json", indent=3)
            self.assertEqual(len(jD), 0)

    def testCompareSchema(self):
        """ Compare common categories across schema definitions.
        """
        try:
            sdCc, _, _, _ = self.__schU.getSchemaInfo("chem_comp_core", altDirPath=self.__workPath)
            sdBcc, _, _, _ = self.__schU.getSchemaInfo("bird_chem_comp_core", altDirPath=self.__workPath)
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
        for schemaName in ["ihm_dev_full"]:
            dD = self.__schU.makeSchemaDef(schemaName, dataTyping="ANY", saveSchema=False, altDirPath=self.__workPath)
            sD = SchemaDefAccess(dD)
            for cd in sD.getCollectionInfo():
                collectionName = cd["NAME"]
                for schemaType in self.__schemaTypes:
                    if schemaType.lower() == "rcsb":
                        continue
                    for level in self.__schemaLevels:
                        self.__schU.makeSchema(
                            schemaName,
                            collectionName,
                            schemaType=schemaType,
                            level=level,
                            saveSchema=self.__saveSchema,
                            altDirPath=self.__workPath,
                            extraOpts="addParentRefs|addPrimaryKey",
                        )


def schemaBuildSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefBuildTests("testBuildSchemaDefs"))
    suiteSelect.addTest(SchemaDefBuildTests("testBuildCollectionSchema"))
    suiteSelect.addTest(SchemaDefBuildTests("testCompareSchema"))
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
