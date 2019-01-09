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
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefAccessTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        pathConfig = os.path.join(mockTopPath, 'config', 'dbload-setup-example.yml')
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__sdu = SchemaDefUtil(cfgOb=self.__cfgOb)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testAccess(self):
        schemaNames = ['pdbx_core', 'chem_comp_core', 'bird_chem_comp_core']
        dataTypingList = ['ANY', 'SQL']
        for schemaName in schemaNames:
            for dataTyping in dataTypingList:
                self.__testAccess(schemaName, dataTyping)

    def __testAccess(self, schemaName, dataTyping):
        try:
            sD = self.__sdu.makeSchemaDef(schemaName, dataTyping=dataTyping, saveSchema=False)
            ok = self.__testAccessors(sD)
            self.assertTrue(ok)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()
        return {}

    def __testAccessors(self, schemaDef):
        """  Verify data and accessor mapping -

        """

        sd = SchemaDefAccess(schemaDef)
        logger.debug("Schema name %s" % sd.getName())
        logger.debug("Schema name %s" % sd.getAppName())

        logger.debug("Database name %s" % sd.getDatabaseName())
        logger.debug("Versioned database name %s" % sd.getVersionedDatabaseName())

        logger.debug("Collections %r" % sd.getContentTypeCollections(sd.getName()))

        for dS in sd.getDataSelectorNames():
            logger.debug("Selector %s %r" % (dS, sd.getDataSelectors(dS)))

        collectionNames = sd.getContentTypeCollections(sd.getName())
        for collectionName in collectionNames:

            logger.debug("Collection excluded %r" % sd.getCollectionExcluded(collectionName))
            logger.debug("Collection included %r" % sd.getCollectionSelected(collectionName))
            logger.debug("Collection document key attribute names %r" % sd.getDocumentKeyAttributeNames(collectionName))

        schemaIdList = sd.getSchemaIdList()
        for schemaId in schemaIdList:
            #
            aIdL = sd.getAttributeIdList(schemaId)
            tObj = sd.getSchemaObject(schemaId)
            attributeIdList = tObj.getAttributeIdList()
            self.assertEqual(len(aIdL), len(attributeIdList))
            attributeNameList = tObj.getAttributeNameList()
            logger.debug("Ordered attribute Id   list %s" % (str(attributeIdList)))
            logger.debug("Ordered attribute name list %s" % (str(attributeNameList)))
            #
            mAL = tObj.getMapAttributeNameList()
            logger.debug("Ordered mapped attribute name list %s" % (str(mAL)))

            mAL = tObj.getMapAttributeIdList()
            logger.debug("Ordered mapped attribute id   list %s" % (str(mAL)))

            cL = tObj.getMapInstanceCategoryList()
            logger.debug("Mapped category list %s" % (str(cL)))

            for c in cL:
                aL = tObj.getMapInstanceAttributeList(c)
                logger.debug("Mapped attribute list in %s :  %s" % (c, str(aL)))
        return True


def schemaAccessSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefAccessTests("testAccess"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = schemaAccessSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
