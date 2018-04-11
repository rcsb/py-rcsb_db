##
# File:    StatusHistoryLoaderTests.py
# Author:  J. Westbrook
# Date:    6-Jan-2015
# Version: 0.001
#
# Updates:
#    16-Aug-2015  jdw   add tests to create file inventory table  -
#    31-Mar-2018  jdw   nuke and pave
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

from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.mysql.Connection import Connection
from rcsb_db.schema.StatusHistorySchemaDef import StatusHistorySchemaDef
from rcsb_db.sql.SqlGen import SqlGenAdmin
from rcsb_db.mysql.SchemaDefLoaderimport SchemaDefLoader
from rcsb_db.utils.ConfigUtil import ConfigUtil
from rcsb_db.utils.ContentTypeUtil import ContentTypeUtil

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


class StatusHistoryLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(StatusHistoryLoaderTests, self).__init__(methodName)

        self.__verbose = True
        self.__ioObj = IoAdapter(verbose=self.__verbose)

    def setUp(self):
        self.__verbose = True
        self.__msd = StatusHistorySchemaDef(verbose=self.__verbose)
        self.__databaseName = 'da_internal'
        self.__fileLimit = 100
        self.__numProc = 2
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data")
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "MYSQL_DB"
        self.__ctU = ContentTypeUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)

        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testStatusHistorySchemaCreate(self):
        """Test case - create table schema using status history schema definition
        """
        try:
            msd, databaseName, _, _ = self.__ctU.getSchemaInfo(contentType='status_history')
            tableIdList = msd.getTableIdList()
            myAd = SqlGenAdmin(self.__verbose)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=databaseName, tableDefObj=tableDefObj))

            logger.debug("Status history  table creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                myQ = MyDbQuery(dbcon=client, verbose=self.__verbose)
                ret = myQ.sqlCommand(sqlCommandList=sqlL)
                logger.debug("\n\n+INFO mysql server returns %r\n" % ret)
                self.assertTrue(ret)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testFileInventorySchemaCreate(self):
        """Test case -  create table schema for file inventory table using status history schema definition
        """
        try:
            msd, databaseName, _, _ = self.__ctU.getSchemaInfo(contentType='status_history')
            tableIdList = ['PDBX_ARCHIVE_FILE_INVENTORY']
            myAd = SqlGenAdmin(self.__verbose)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=databaseName, tableDefObj=tableDefObj))

            logger.debug("FileInventory table creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                myQ = MyDbQuery(dbcon=client, verbose=self.__verbose)
                ret = myQ.sqlCommand(sqlCommandList=sqlL)
                logger.debug("\n\n+INFO mysql server returns %r\n" % ret)
                self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadInventoryFile(self):
        """Test case - load inventory files-
        """

        try:
            loadPathList = [os.path.join(TOPDIR, "rcsb_db", "data", "test_file_inventory.cif")]
            workPath = os.path.join(HERE, "test-output")
            sml = SchemaDefLoader(schemaDefObj=self.__msd, ioObj=self.__ioObj, dbCon=None, workPath=workPath, cleanUp=False, warnings='error', verbose=self.__verbose)
            containerNameList, tList = sml.makeLoadFiles(loadPathList)
            for tId, fn in tList:
                logger.debug("\nCreated table %s load file %s\n" % (tId, fn))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(schemaDefObj=self.__msd, ioObj=self.__ioObj, dbCon=client, workPath=workPath, cleanUp=False,
                                      warnings='error', verbose=self.__verbose)

                sdl.loadBatchFiles(loadList=tList, containerNameList=containerNameList, deleteOpt='all')

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
    if True:
        mySuite = createHistoryFullSchemaSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
        #
        mySuite = createFileInventoryLoadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
