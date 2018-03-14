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

SchemaDefLoader() uses default native Python IoAdapter.

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
from rcsb_db.mysql.MyDbUtil import MyDbConnect, MyDbQuery
from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef

from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo


class BirdLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(BirdLoaderTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__tddFileList = []
        self.__lfh = sys.stderr
        self.__verbose = True

    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__databaseName = 'prdv4'
        self.__topCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        self.open()
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        self.close()
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def open(self, dbUserId=None, dbUserPwd=None):
        myC = MyDbConnect(dbName=self.__databaseName, dbUser=dbUserId, dbPw=dbUserPwd, verbose=self.__verbose)
        self.__dbCon = myC.connect()
        if self.__dbCon is not None:
            return True
        else:
            return False

    def close(self):
        if self.__dbCon is not None:
            self.__dbCon.close()

    def testBirdSchemaCreate(self):
        """Test case -  create table schema using BIRD schema definition
        """
        try:
            msd = BirdSchemaDef(verbose=self.__verbose)
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=msd.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("\n\n+BIRD table creation SQL string\n %s\n\n" % '\n'.join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            logger.debug("\n\n+INFO mysql server returns %r\n" % ret)
            self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrdPathList(self):
        """Test case -  get the path list of PRD definitions in the CVS repository.
        """
        try:
            prd = PdbxPrdIo(verbose=self.__verbose)
            prd.setCachePath(self.__topCachePath)
            self.__loadPathList = prd.makeDefinitionPathList()
            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            self.assertGreaterEqual(len(self.__loadPathList), 3)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testMakeLoadPrdFiles(self):
        """Test case - for loading BIRD definition data files
        """
        try:
            self.testPrdPathList()
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

            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)

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
            self.testPrdPathList()
            bsd = BirdSchemaDef()
            sml = SchemaDefDataPrep(schemaDefObj=bsd, verbose=self.__verbose)
            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            #
            tableDataDict, containerNameList = sml.fetch(self.__loadPathList)

            databaseName = bsd.getDatabaseName()
            tableIdList = bsd.getTableIdList()

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
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
                            if len(row[id]) > 0 and row[id] != r'\N':
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
            self.testPrdPathList()
            bsd = BirdSchemaDef()
            sml = SchemaDefDataPrep(schemaDefObj=bsd, verbose=self.__verbose)
            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            #
            tableDataDict, containerNameList = sml.fetch(self.__loadPathList)

            databaseName = bsd.getDatabaseName()
            tableIdList = bsd.getTableIdList()

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
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
                            if len(row[id]) > 0 and row[id] != r'\N':
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
    mySuite = loadBatchFileSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    mySuite = loadBatchInsertSuite1()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    mySuite = loadBatchInsertSuite2()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
