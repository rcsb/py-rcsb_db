##
# File:    SchemaDefAccessTests.py
# Author:  J. Westbrook
# Date:    15-Jun-2018
# Version: 0.001
#
# Update:
#
##
"""
Tests the accessor methods for schema meta data.

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


class SchemaDefAccessTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=True, clearPath=False)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testAccess(self):
        collectionGroupNames = ["pdbx_core", "core_chem_comp"]
        dataTypingList = ["ANY", "SQL"]
        for collectionGroupName in collectionGroupNames:
            for dataTyping in dataTypingList:
                self.__testAccess(collectionGroupName, dataTyping)

    def __testAccess(self, collectionGroupName, dataTyping):
        try:
            sD = self.__schP.makeSchemaDef(collectionGroupName, dataTyping=dataTyping, saveSchema=False)
            ok = self.__testAccessors(sD)
            self.assertTrue(ok)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()
        return {}

    def __testAccessors(self, schemaDef):
        """Verify data and accessor mapping -"""

        sd = SchemaDefAccess(schemaDef)
        logger.debug("Schema name %s", sd.getName())
        logger.debug("App name %s", sd.getAppName())

        logger.debug("Database name %s", sd.getDatabaseName())
        logger.debug("Versioned database name %s", sd.getVersionedDatabaseName())

        logger.debug("Collection info %r", sd.getCollectionInfo())

        for dS in sd.getDataSelectorNames():
            logger.debug("Selector %s %r", dS, sd.getDataSelectors(dS))

        collectionInfoL = sd.getCollectionInfo()
        for dD in collectionInfoL:
            collectionName = dD["NAME"]

            logger.debug("Collection excluded %r", sd.getCollectionExcluded(collectionName))
            logger.debug("Collection included %r", sd.getCollectionSelected(collectionName))
            logger.debug("Collection document key attribute names %r", sd.getDocumentKeyAttributeNames(collectionName))

        schemaIdList = sd.getSchemaIdList()
        for schemaId in schemaIdList:
            #
            aIdL = sd.getAttributeIdList(schemaId)
            tObj = sd.getSchemaObject(schemaId)
            attributeIdList = tObj.getAttributeIdList()
            self.assertEqual(len(aIdL), len(attributeIdList))
            attributeNameList = tObj.getAttributeNameList()
            logger.debug("Ordered attribute Id   list %s", str(attributeIdList))
            logger.debug("Ordered attribute name list %s", str(attributeNameList))
            #
            mAL = tObj.getMapAttributeNameList()
            logger.debug("Ordered mapped attribute name list %s", str(mAL))

            mAL = tObj.getMapAttributeIdList()
            logger.debug("Ordered mapped attribute id   list %s", str(mAL))

            cL = tObj.getMapInstanceCategoryList()
            logger.debug("Mapped category list %s", str(cL))

            for cV in cL:
                aL = tObj.getMapInstanceAttributeList(cV)
                logger.debug("Mapped attribute list in %s :  %s", cV, str(aL))
        return True


def schemaAccessSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefAccessTests("testAccess"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = schemaAccessSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
