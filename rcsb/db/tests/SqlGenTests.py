##
# File:    SqlGenTests.py
# Author:  J. Westbrook
# Date:    31-Jan-2012
# Version: 0.001
#
# Updates:  20-Dec-2017 jdw py2/py3 working in compat23 branch
#           12-Mar-2018 jdw refactor for Python Packaging -
#            6-Jul-2018 jdw Update for new schema def prototypes
##
"""
Test cases for SQL command generation  --   no data connections required for these tests --

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import sys
import time
import unittest

from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.db.sql.SqlGen import SqlGenAdmin, SqlGenCondition, SqlGenQuery
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.IoUtil import IoUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SqlGenTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        #
        self.__mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        self.__pathConfig = os.path.join(self.__mockTopPath, 'config', 'dbload-setup-example.cfg')
        #
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, mockTopPath=self.__mockTopPath)
        #

        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def testSQLMethods(self):
        schemaNames = ['pdbx', 'chem_comp', 'bird', 'bird_family', 'bird_chem_comp']
        applicationName = 'SQL'
        for schemaName in schemaNames:
            d = self.__testBuild(schemaName, applicationName)
            sD = SchemaDefAccess(d)
            self.__testSchemaCreate(sD)
            self.__testImportExport(sD)
            self.__testSelectionAndConditions(sD)
    #

    def __getHelper(self, modulePath, **kwargs):
        aMod = __import__(modulePath, globals(), locals(), [''])
        sys.modules[modulePath] = aMod
        #
        # Strip off any leading path to the module before we instaniate the object.
        mpL = modulePath.split('.')
        moduleName = mpL[-1]
        #
        aObj = getattr(aMod, moduleName)(**kwargs)
        return aObj

    def __testBuild(self, schemaName, applicationName):
        try:
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

    def __testSchemaCreate(self, sD):
        """Test case -  create table schema using input schema definition as an example
        """

        try:
            tableIdList = sD.getSchemaIdList()
            myAd = SqlGenAdmin(self.__verbose)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = sD.getSchemaObject(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=sD.getDatabaseName(), tableDefObj=tableDefObj))
                logger.debug("\n\n+SqlGenTests table creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            self.assertGreaterEqual(len(sqlL), 10)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __testImportExport(self, sD):
        """Test case -  import and export commands --
        """

        try:
            databaseName = sD.getDatabaseName()
            tableIdList = sD.getSchemaIdList()
            myAd = SqlGenAdmin(self.__verbose)
            for tableId in tableIdList:
                tableDefObj = sD.getSchemaObject(tableId)
                exportPath = os.path.join(HERE, "test-output", tableDefObj.getName() + ".tdd")
                sqlExport = myAd.exportTable(databaseName, tableDefObj, exportPath=exportPath)
                logger.debug("\n\n+SqlGenTests table export SQL string\n %s\n\n" % sqlExport)
                sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=exportPath)
                logger.debug("\n\n+SqlGenTests table import SQL string\n %s\n\n" % sqlImport)
                self.assertGreaterEqual(len(sqlImport), 100)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __testSelectionAndConditions(self, sD):
        """Test case -  selection everything for a simple condition-
        """
        try:
            # get delete attribute -
            #
            tableIdList = sD.getSchemaIdList()
            sqlGen = SqlGenQuery(schemaDefObj=sD, verbose=self.__verbose)

            for tableId in tableIdList:
                tableDefObj = sD.getSchemaObject(tableId)
                dAtId = tableDefObj.getDeleteAttributeId()
                sqlCondition = SqlGenCondition(schemaDefObj=sD, verbose=self.__verbose)
                sqlCondition.addValueCondition((tableId, dAtId), 'EQ', ('D000001', 'CHAR'))
                aIdList = sD.getAttributeIdList(tableId)
                for aId in aIdList:
                    sqlGen.addSelectAttributeId(attributeTuple=(tableId, aId))
                sqlGen.setCondition(sqlCondition)
                sqlGen.addOrderByAttributeId(attributeTuple=(tableId, dAtId))
                sqlS = sqlGen.getSql()
                logger.debug("\n\n+SqlGenTests table creation SQL string\n %s\n\n" % sqlS)
                self.assertGreaterEqual(len(sqlS), 50)
                sqlGen.clear()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suiteSQLMethods():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SqlGenTests("testSQLMethods"))
    return suiteSelect


if __name__ == '__main__':
    # Run all tests --
    # unittest.main()
    #
    mySuite = suiteSQLMethods()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
