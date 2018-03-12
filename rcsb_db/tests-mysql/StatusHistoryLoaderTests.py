##
# File:    StatusHistoryLoaderTests.py
# Author:  J. Westbrook
# Date:    6-Jan-2015
# Version: 0.001
#
# Updates:
#    16-Aug-2015  jdw   add tests to create file inventory table  -
#
##
"""
Tests for creating and loading status history data using PDBx/mmCIF data files
and external schema definition.

These test database connections deferring to authentication details defined
in the environment.   See class MyDbConnect() for the environment requirements.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import os
import sys
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

from rcsb_db.mysql.MyDbUtil import MyDbConnect, MyDbQuery
from rcsb_db.schema.StatusHistorySchemaDef import StatusHistorySchemaDef
from mmcif.io.IoAdapterPy import IoAdapterPy
from rcsb_db.sql.MyDbSqlGen import MyDbAdminSqlGen
from rcsb_db.loaders.SchemaDefLoader import SchemaDefLoader


class StatusHistoryLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(StatusHistoryLoaderTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__tddFileList = []
        self.__verbose = True
        self.__ioObj = IoAdapterPy(verbose=self.__verbose)

    def setUp(self):
        self.__verbose = True
        self.__msd = StatusHistorySchemaDef(verbose=self.__verbose)
        self.__databaseName = 'da_internal'

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

    def testStatusHistorySchemaCreate(self):
        """Test case -  create table schema using status history schema definition
        """
        try:
            msd = StatusHistorySchemaDef(verbose=self.__verbose)
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=self.__databaseName, tableDefObj=tableDefObj))

            if (self.__verbose):
                logger.info("\n\n+Status history  table creation SQL string\n %s\n\n" % '\n'.join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            if (self.__verbose):
                logger.info("\n\n+INFO mysql server returns %r\n" % ret)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testFileInventorySchemaCreate(self):
        """Test case -  create table schema for file inventory table using status history schema definition
        """
        try:
            msd = StatusHistorySchemaDef(verbose=self.__verbose)
            tableIdList = ['PDBX_ARCHIVE_FILE_INVENTORY']
            myAd = MyDbAdminSqlGen(self.__verbose)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=self.__databaseName, tableDefObj=tableDefObj))

            if (self.__verbose):
                logger.info("\n\n+FileInventory table creation SQL string\n %s\n\n" % '\n'.join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            if (self.__verbose):
                logger.info("\n\n+INFO mysql server returns %r\n" % ret)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadInventoryFile(self):
        """Test case - create batch load files for all chemical component definition data files -
        """

        try:
            loadPathList = [os.path.join(TOPDIR, "rcsb_db", "data", "test_file_inventory.cif")]
            sml = SchemaDefLoader(schemaDefObj=self.__msd, ioObj=self.__ioObj, dbCon=None, workPath='.', cleanUp=False, warnings='default', verbose=self.__verbose)
            containerNameList, tList = sml.makeLoadFiles(loadPathList)
            for tId, fn in tList:
                logger.info("\nCreated table %s load file %s\n" % (tId, fn))

            self.open()
            sdl = SchemaDefLoader(schemaDefObj=self.__msd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose)

            sdl.loadBatchFiles(loadList=tList, containerNameList=containerNameList, deleteOpt='all')
            # self.close()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def createHistoryFullSchemaSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(StatusHistoryLoaderTests("testStatusHistorySchemaCreate"))
    return suiteSelect


def createFileInventoryLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(StatusHistoryLoaderTests("testFileInventorySchemaCreate"))
    suiteSelect.addTest(StatusHistoryLoaderTests("testLoadInventoryFile"))
    return suiteSelect


if __name__ == '__main__':
    #
    if False:
        mySuite = createHistoryFullSchemaSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    mySuite = createFileInventoryLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
