##
# File:    BirdLoaderTests.py
# Author:  J. Westbrook
# Date:    9-Jan-2013
# Version: 0.001
#
# Updates:
#  11-Jan-2013  jdw revise treatment of null values in inserts.
#   2-Oct-2017  jdw escape null string '\N'
#  31-Mar-2018  jdw nuck and pave
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

from rcsb_db.sql.SqlGen import SqlGenAdmin
from rcsb_db.loaders.SchemaDefLoader import SchemaDefLoader
from rcsb_db.loaders.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.mysql.Connection import Connection

from rcsb_db.utils.ConfigUtil import ConfigUtil
from rcsb_db.utils.ContentTypeUtil import ContentTypeUtil


class BirdLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(BirdLoaderTests, self).__init__(methodName)

        self.__tddFileList = []
        self.__verbose = True

    def setUp(self):
        self.__verbose = True
        self.__databaseName = 'bird_v5'
        self.__topCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        self.__fileLimit = 100
        self.__numProc = 2
        self.__birdMockLen = 4

        self.__mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data")
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "MYSQL_DB"
        self.__ctU = ContentTypeUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
        #
        #
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testBirdSchemaCreate(self):
        """Test case -  create table schema using BIRD schema definition
        """
        try:
            msd, dbName, _ = self.__ctU.getSchemaInfo(contentType='bird')
            #
            tableIdList = msd.getTableIdList()
            myAd = SqlGenAdmin(self.__verbose)
            sqlL = myAd.createDatabaseSQL(dbName)
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=msd.getDatabaseName(), tableDefObj=tableDefObj))
            logger.debug("+BIRD table creation SQL string\n %s\n\n" % '\n'.join(sqlL))

            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                myQ = MyDbQuery(dbcon=client, verbose=self.__verbose)
                myQ.setWarning('ignore')
                ret = myQ.sqlCommand(sqlCommandList=sqlL)
                logger.debug("\n\n+INFO mysql server returns %r\n" % ret)
                self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __testMakeLoadPrdFiles(self):
        """Test case - for loading BIRD definition data files
        """
        try:
            loadPathList = self.__ctU.getPathList(contentType='bird')
            sd, _, _ = self.__ctU.getSchemaInfo(contentType='bird')

            sml = SchemaDefLoader(schemaDefObj=sd, verbose=self.__verbose, workPath=os.path.join(HERE, "test-output"))
            logger.debug("Length of path list %d\n" % len(loadPathList))
            containerNameList, tddFileList = sml.makeLoadFiles(loadPathList)
            for tId, tPath in tddFileList:
                logger.debug("\nCreate loadable file %s %s\n" % (tId, tPath))
            self.assertGreaterEqual(len(tddFileList), 6)
            #
            return tddFileList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testBirdBatchImport(self):
        """Test case -  import loadable files
        """

        try:
            tddFileList = self.__testMakeLoadPrdFiles()
            sd, databaseName, _ = self.__ctU.getSchemaInfo(contentType='bird')

            myAd = SqlGenAdmin(self.__verbose)

            for tableId, tPath in tddFileList:

                if os.access(tPath, os.F_OK):
                    logger.debug("+INFO - Found for %s\n" % tPath)
                    tableDefObj = sd.getTable(tableId)
                    sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=tPath, withTruncate=True)
                    logger.debug("\n\n+SqlGenTests table import SQL string\n %s\n\n" % sqlImport)
                    #
                    lfn = os.path.join(HERE, "test-output", tableId + "-load.sql")
                    with open(lfn, 'w') as ofh:
                        ofh.write("%s\n" % sqlImport)
                    #
                    with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                        myQ = MyDbQuery(dbcon=client, verbose=self.__verbose)
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
            loadPathList = self.__ctU.getPathList(contentType='bird')
            sd, databaseName, _ = self.__ctU.getSchemaInfo(contentType='bird')

            sml = SchemaDefDataPrep(schemaDefObj=sd, verbose=self.__verbose)
            logger.debug("Length of path list %d\n" % len(loadPathList))
            #
            tableDataDict, containerNameList = sml.fetch(loadPathList)
            tableIdList = sd.getTableIdList()

            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                myQ = MyDbQuery(dbcon=client, verbose=self.__verbose)
                myAd = SqlGenAdmin(self.__verbose)
                #
                for tableId in tableIdList:
                    tableDefObj = sd.getTable(tableId)
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
            loadPathList = self.__ctU.getPathList(contentType='bird')
            sd, databaseName, _ = self.__ctU.getSchemaInfo(contentType='bird')

            sml = SchemaDefDataPrep(schemaDefObj=sd, verbose=self.__verbose)
            logger.debug("Length of path list %d\n" % len(loadPathList))
            #
            tableDataDict, containerNameList = sml.fetch(loadPathList)

            databaseName = sd.getDatabaseName()
            tableIdList = sd.getTableIdList()

            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                myQ = MyDbQuery(dbcon=client, verbose=self.__verbose)
                myAd = SqlGenAdmin(self.__verbose)
                #
                for tableId in tableIdList:
                    tableDefObj = sd.getTable(tableId)
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
