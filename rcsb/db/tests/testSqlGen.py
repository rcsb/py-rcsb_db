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
from rcsb.db.sql.SqlGen import SqlGenAdmin, SqlGenCondition, SqlGenQuery
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SqlGenTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__sdu = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=True)
        #

        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def testSQLMethods(self):
        schemaNames = ["pdbx_core"]
        dataTyping = "SQL"
        for schemaName in schemaNames:
            dD = self.__sdu.makeSchemaDef(schemaName, dataTyping=dataTyping, saveSchema=False)
            sD = SchemaDefAccess(dD)
            self.__testSchemaCreate(sD)
            self.__testImportExport(sD)
            self.__testSelectionAndConditions(sD)

    #

    def __getHelper(self, modulePath, **kwargs):
        aMod = __import__(modulePath, globals(), locals(), [""])
        sys.modules[modulePath] = aMod
        #
        # Strip off any leading path to the module before we instaniate the object.
        mpL = modulePath.split(".")
        moduleName = mpL[-1]
        #
        aObj = getattr(aMod, moduleName)(**kwargs)
        return aObj

    def __testSchemaCreate(self, sD):
        """Test case -  create table schema using input schema definition as an example"""

        try:
            tableIdList = sD.getSchemaIdList()
            myAd = SqlGenAdmin(self.__verbose)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = sD.getSchemaObject(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=sD.getDatabaseName(), tableDefObj=tableDefObj))
                logger.debug("\n\n+SqlGenTests table creation SQL string\n %s\n\n", "\n".join(sqlL))
            self.assertGreaterEqual(len(sqlL), 10)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def __testImportExport(self, sD):
        """Test case -  import and export commands --"""

        try:
            databaseName = sD.getDatabaseName()
            tableIdList = sD.getSchemaIdList()
            myAd = SqlGenAdmin(self.__verbose)
            for tableId in tableIdList:
                tableDefObj = sD.getSchemaObject(tableId)
                exportPath = os.path.join(HERE, "test-output", tableDefObj.getName() + ".tdd")
                sqlExport = myAd.exportTable(databaseName, tableDefObj, exportPath=exportPath)
                logger.debug("\n\n+SqlGenTests table export SQL string\n %s\n\n", sqlExport)
                sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=exportPath)
                logger.debug("\n\n+SqlGenTests table import SQL string\n %s\n\n", sqlImport)
                self.assertGreaterEqual(len(sqlImport), 100)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def __testSelectionAndConditions(self, sD):
        """Test case -  selection everything for a simple condition-"""
        try:
            # get delete attribute -
            #
            tableIdList = sD.getSchemaIdList()
            logger.debug("TableIdList %r", tableIdList)
            sqlGen = SqlGenQuery(schemaDefObj=sD, verbose=self.__verbose)

            for tableId in tableIdList:
                tableDefObj = sD.getSchemaObject(tableId)
                dAtId = tableDefObj.getDeleteAttributeId()

                if dAtId:
                    sqlCondition = SqlGenCondition(schemaDefObj=sD, verbose=self.__verbose)
                    sqlCondition.addValueCondition((tableId, dAtId), "EQ", ("D000001", "CHAR"))
                    aIdList = sD.getAttributeIdList(tableId)
                    for aId in aIdList:
                        sqlGen.addSelectAttributeId(attributeTuple=(tableId, aId))
                    sqlGen.setCondition(sqlCondition)
                    sqlGen.addOrderByAttributeId(attributeTuple=(tableId, dAtId))
                    sqlS = sqlGen.getSql()
                    logger.debug("\n\n+SqlGenTests table creation SQL string\n %s\n\n", sqlS)
                    self.assertGreaterEqual(len(sqlS), 50)
                    sqlGen.clear()
                else:
                    logger.debug("Missing delete atttribe for table %r", tableId)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def suiteSQLMethods():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SqlGenTests("testSQLMethods"))
    return suiteSelect


if __name__ == "__main__":
    # Run all tests --
    # unittest.main()
    #
    mySuite = suiteSQLMethods()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
