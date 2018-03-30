##
# File:    BirdLoaderTests.py
# Author:  J. Westbrook
# Date:    9-Jan-2013
# Version: 0.001
#
# Updates:
#  11-Jan-2013 jdw revise treatment of null values in inserts.
#   2-Oct-2017  jdw escape null string '\N'
#
#
##
"""
Tests for creating and loading BIRD rdbms database using PDBx/mmCIF data files
and external schema definition.

These test database connections deferring to authentication details defined
in the environment.   See class MyDbConnect() for the environment requirements.


"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import os
import time
import unittest

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.sql.MyDbSqlGen import MyDbAdminSqlGen
from rcsb_db.loaders.SchemaDefLoader import SchemaDefLoader
from rcsb_db.loaders.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.mysql.Connection import Connection
from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef

from rcsb_db.utils.RepoPathUtil import RepoPathUtil
from rcsb_db.utils.ConfigUtil import ConfigUtil

class BirdLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(BirdLoaderTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__tddFileList = []
        self.__verbose = True

    def setUp(self):
        self.__verbose = True
        self.__databaseName = 'bird_v5'
        self.__topCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        self.__fileLimit = 100
        self.__birdMockLen = 4

        self.__mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data")
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "MYSQL_DB"
        connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName)
        #
        self.__cObj = self.__open(connectD)
        self.__dbCon = self.__getClientConnection(self.__cObj)
        #
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        self.__close(self.__cObj)
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def __assignResource(self, cfgOb, resourceName="MYSQL_DB"):
        cn = Connection(cfgOb=cfgOb)
        return cn.assignResource(resourceName=resourceName)

    def __open(self, connectD):
        cObj = Connection()
        cObj.setPreferences(connectD)
        ok = cObj.openConnection()
        if ok:
            return cObj
        else:
            return None

    def __close(self, cObj):
        if cObj is not None:
            cObj.closeConnection()
            self.__dbCon = None
            return True
        else:
            return False

    def __getClientConnection(self, cObj):
        return cObj.getClientConnection()

    def testBirdPathList(self):
        """Test case -  get the path list of PRD definitions in the CVS repository.
        """
        try:
            rpU = RepoPathUtil(self.__cfgOb, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
            self.__loadPathList = rpU.getBirdPathList()

            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            self.assertGreaterEqual(len(self.__loadPathList), self.__birdMockLen)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testBirdSchemaCreate(self):
        """Test case -  create table schema using BIRD schema definition
        """
        try:
            msd = BirdSchemaDef(verbose=self.__verbose)
            dbName = msd.getDatabaseName()
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose)
            sqlL = myAd.createDatabaseSQL(dbName)
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=msd.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("+BIRD table creation SQL string\n %s\n\n" % '\n'.join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myQ.setWarning('ignore')
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            logger.debug("\n\n+INFO mysql server returns %r\n" % ret)
            self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testMakeLoadPrdFiles(self):
        """Test case - for loading BIRD definition data files
        """
        try:
            self.testBirdPathList()
            bsd = BirdSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=bsd, verbose=self.__verbose, workPath=os.path.join(HERE, "test-output"))
            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            containerNameList, self.__tddFileList = sml.makeLoadFiles(self.__loadPathList)
            for tId, tPath in self.__tddFileList:
                logger.debug("\nCreate loadable file %s %s\n" % (tId, tPath))
            self.assertGreaterEqual(len(self.__tddFileList), 6)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testBirdBatchImport(self):
        """Test case -  import loadable files
        """

        try:
            self.testMakeLoadPrdFiles()
            bsd = BirdSchemaDef(verbose=self.__verbose)
            databaseName = bsd.getDatabaseName()
            tableIdList = bsd.getTableIdList()

            myAd = MyDbAdminSqlGen(self.__verbose)

            for tableId in tableIdList:
                fn = os.path.join(HERE, "test-output", tableId + "-1.tdd")
                if os.access(fn, os.F_OK):
                    logger.debug("+INFO - Found for %s\n" % fn)
                    tableDefObj = bsd.getTable(tableId)
                    sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=fn, withTruncate=True)
                    logger.debug("\n\n+MyDbSqlGenTests table import SQL string\n %s\n\n" % sqlImport)
                    #
                    lfn = os.path.join(HERE, "test-output", tableId + "-load.sql")
                    with open(lfn, 'w') as ofh:
                        ofh.write("%s\n" % sqlImport)
                    #
                    myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
                    myQ.setWarning('error')
                    ret = myQ.sqlCommand2(sqlImport)
                    self.assertTrue(ret)
                    logger.debug("mysql server returns %r\n" % ret)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testBirdInsertImport(self):
        """Test case -  import loadable data via SQL inserts
        """

        try:
            self.testBirdPathList()
            bsd = BirdSchemaDef()
            sml = SchemaDefDataPrep(schemaDefObj=bsd, verbose=self.__verbose)
            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            #
            tableDataDict, containerNameList = sml.fetch(self.__loadPathList)

            databaseName = bsd.getDatabaseName()
            tableIdList = bsd.getTableIdList()

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myAd = MyDbAdminSqlGen(self.__verbose)
            #
            for tableId in tableIdList:
                tableDefObj = bsd.getTable(tableId)
                tableName = tableDefObj.getName()
                tableAttributeIdList = tableDefObj.getAttributeIdList()
                tableAttributeNameList = tableDefObj.getAttributeNameList()

                if tableId in tableDataDict:
                    rowList = tableDataDict[tableId]
                    for row in rowList:
                        vList = []
                        aList = []
                        for id, nm in zip(tableAttributeIdList, tableAttributeNameList):
                            vList.append(row[id])
                            aList.append(nm)
                        insertTemplate = myAd.insertTemplateSQL(databaseName, tableName, aList)

                        ok = myQ.sqlTemplateCommand(sqlTemplate=insertTemplate, valueList=vList)
                        self.assertTrue(ok)
                        logger.debug("mysql server returns %r\n" % ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testBirdBatchInsertImport(self):
        """Test case -  import loadable data via SQL inserts -
        """

        try:
            self.testBirdPathList()
            bsd = BirdSchemaDef()
            sml = SchemaDefDataPrep(schemaDefObj=bsd, verbose=self.__verbose)
            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            #
            tableDataDict, containerNameList = sml.fetch(self.__loadPathList)

            databaseName = bsd.getDatabaseName()
            tableIdList = bsd.getTableIdList()

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myAd = MyDbAdminSqlGen(self.__verbose)
            #
            for tableId in tableIdList:
                tableDefObj = bsd.getTable(tableId)
                tableName = tableDefObj.getName()
                tableAttributeIdList = tableDefObj.getAttributeIdList()
                tableAttributeNameList = tableDefObj.getAttributeNameList()

                sqlL = []
                if tableId in tableDataDict:
                    rowList = tableDataDict[tableId]
                    for row in rowList:
                        vList = []
                        aList = []
                        for id, nm in zip(tableAttributeIdList, tableAttributeNameList):
                            vList.append(row[id])
                            aList.append(nm)
                        sqlL.append((myAd.insertTemplateSQL(databaseName, tableName, aList), vList))

                    ok = myQ.sqlBatchTemplateCommand(sqlL)
                    self.assertTrue(ok)
                    logger.debug("mysql server returns %r\n" % ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def loadBatchFileSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(BirdLoaderTests("testBirdSchemaCreate"))
    # suiteSelect.addTest(BirdLoaderTests("testPrdPathList"))
    suiteSelect.addTest(BirdLoaderTests("testMakeLoadPrdFiles"))
    suiteSelect.addTest(BirdLoaderTests("testBirdBatchImport"))
    return suiteSelect


def loadBatchInsertSuite1():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(BirdLoaderTests("testBirdSchemaCreate"))
    suiteSelect.addTest(BirdLoaderTests("testBirdInsertImport"))
    return suiteSelect


def loadBatchInsertSuite2():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(BirdLoaderTests("testBirdSchemaCreate"))
    suiteSelect.addTest(BirdLoaderTests("testBirdBatchInsertImport"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = loadBatchFileSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    if True:
        mySuite = loadBatchInsertSuite1()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
        #
        mySuite = loadBatchInsertSuite2()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
