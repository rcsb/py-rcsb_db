##
# File:    SchemaDefAccessConfigTests.py
# Author:  J. Westbrook
# Date:    15-Jun-2018
# Version: 0.001
#
# Update:
#   20-Aug-2018 jdw Replace local getHelper() method with method in configuration class.
##
"""
Tests the accessor methods for schema meta data using externally store configuration details.

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
from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.IoUtil import IoUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefAccessConfigTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        #
        self.__mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        self.__pathConfig = os.path.join(self.__mockTopPath, 'config', 'dbload-setup-example.yml')
        #
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, mockTopPath=self.__mockTopPath)
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
        schemaNames = ['pdbx', 'pdbx_core', 'chem_comp', 'bird', 'bird_family', 'bird_chem_comp', 'repository_holdings', 'entity_sequence_clusters']
        applicationNames = ['ANY', 'SQL']
        for schemaName in schemaNames:
            for applicationName in applicationNames:
                self.__testAccess(schemaName, applicationName)

    def __testBuild(self, schemaName, applicationName):
        try:
            #
            optName = 'SCHEMA_DEF_LOCATOR_%s' % applicationName.upper()
            pathSchemaDefJson = self.__cfgOb.getPath(optName, sectionName=schemaName)
            #
            smb = SchemaDefBuild(schemaName, self.__pathConfig, mockTopPath=self.__mockTopPath)
            sD = smb.build(applicationName=applicationName)
            #
            logger.debug("Schema %s dictionary category length %d" % (schemaName, len(sD['SCHEMA_DICT'])))
            self.assertGreaterEqual(len(sD['SCHEMA_DICT']), 5)
            #
            ioU = IoUtil()
            ioU.serialize(pathSchemaDefJson, sD, format='json', indent=3)
            return sD

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()
        return {}

    def __testAccess(self, schemaName, applicationName):
        try:
            sD = self.__testBuild(schemaName, applicationName)
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
    suiteSelect.addTest(SchemaDefAccessConfigTests("testAccess"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = schemaAccessSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
