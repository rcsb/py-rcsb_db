##
# File:    ChemCompLoaderTests.py
# Author:  J. Westbrook
# Date:    7-Nov-2014
# Version: 0.001
#
# Update:
#   10-Nov-2014 -- add scandir.walk() and multiprocess all tasks -
#   20-Dec-2017 -- use IoAdapterPy()
##
"""
Tests for loading instance data using schema definition -

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
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.mysql.Connection import Connection
from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.loaders.SchemaDefLoader import SchemaDefLoader
from rcsb_db.schema.ChemCompSchemaDef import ChemCompSchemaDef

from rcsb_db.utils.MultiProcUtil import MultiProcUtil
from rcsb_db.utils.RepoPathUtil import RepoPathUtil
from rcsb_db.utils.ConfigUtil import ConfigUtil
from rcsb_db.sql.MyDbSqlGen import MyDbAdminSqlGen


try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


class ChemCompLoaderTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        self.__loadPathList = []
        #
        self.__fileLimit = 100
        self.__chemCompMockLen = 4
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data")
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "MYSQL_DB"
        self.__connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName)
        self.__ioObj = IoAdapter(verbose=self.__verbose)
        self.__topCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_CHEM_COMP_REPO")
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        #

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def __assignResource(self, cfgOb, resourceName="MYSQL_DB"):
        cn = Connection(cfgOb=cfgOb)
        return cn.assignResource(resourceName=resourceName)

    def open(self, dbName):
        return self.__open(self.__connectD)

    def __open(self, connectD):
        cObj = Connection()
        cObj.setPreferences(connectD)
        ok = cObj.openConnection()
        if ok:
            return cObj
        else:
            return None

    def close(self, cObj):
        if cObj is not None:
            cObj.closeConnection()
            self.__dbCon = None
            return True
        else:
            return False

    def getClientConnection(self, cObj):
        return cObj.getClientConnection()

    def __makeComponentPathList(self):
        """Test case -  get the path list of Chem Comp definitions in the repository.
        """
        try:
            rpU = RepoPathUtil(self.__cfgOb, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
            loadPathList = rpU.getChemCompPathList()

            logger.debug("Length of path list %d\n" % len(loadPathList))
            self.assertGreaterEqual(len(loadPathList), self.__chemCompMockLen)
            return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testListFiles(self):
        """Test case - for loading chemical component definition data files -
        """

        try:
            pathList = self.__makeComponentPathList()
            logger.debug("\nFound %d files\n" % len(pathList))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testConnect(self):
        """Test case - for creating a test connection
        """

        try:
            ccsd = ChemCompSchemaDef()
            cObj = self.open(dbName=ccsd.getDatabaseName())
            self.close(cObj)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def schemaCreate(self, schemaDefObj, dbCon):
        """ -  create table schema using input schema definition
        """
        try:
            dbName = schemaDefObj.getDatabaseName()
            tableIdList = schemaDefObj.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose)
            sqlL = myAd.createDatabaseSQL(dbName)
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("Table creation SQL string\n %s\n\n" % '\n'.join(sqlL))

            myQ = MyDbQuery(dbcon=dbCon, verbose=self.__verbose)
            myQ.setWarning('ignore')
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            logger.debug("\n\n+INFO mysql server returns %r\n" % ret)
            self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadFiles(self):
        """Test case - create batch load files for all chemical component definition data files -
        """

        try:
            ccsd = ChemCompSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=None, workPath=os.path.join(HERE, "test-output"), cleanUp=False,
                                  warnings='default', verbose=self.__verbose)
            pathList = self.__makeComponentPathList()

            containerNameList, tList = sml.makeLoadFiles(pathList, append=False)
            for tId, fn in tList:
                logger.info("\nCreated table %s load file %s\n" % (tId, fn))
            #

            cObj = self.open(dbName=ccsd.getDatabaseName())
            dbCon = self.getClientConnection(cObj)
            #
            self.schemaCreate(ccsd, dbCon)
            #
            sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=dbCon, workPath=os.path.join(HERE, "test-output"), cleanUp=False,
                                  warnings='default', verbose=self.__verbose)

            ok = sdl.loadBatchFiles(loadList=tList, containerNameList=containerNameList, deleteOpt='all')
            self.assertTrue(ok)
            self.close(cObj)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def loadBatchFilesMulti(self, dataList, procName, optionsD, workingDir):
        ccsd = ChemCompSchemaDef()
        cObj = self.open(dbName=ccsd.getDatabaseName())
        dbCon = self.getClientConnection(cObj)
        sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=dbCon, workPath=os.path.join(HERE, 'test-output'), cleanUp=False,
                              warnings='default', verbose=self.__verbose)
        #
        ok = sdl.loadBatchFiles(loadList=dataList, containerNameList=None, deleteOpt=None)
        self.assertTrue(ok)
        self.close(cObj)
        return dataList, dataList, []

    def testLoadFilesMulti(self):
        """Test case - create batch load files for all chemical component definition data files - (multiproc test)
        """
        startTime = time.time()
        numProc = 2
        try:

            pathList = self.__makeComponentPathList()
            logger.info("\nPath list %r\n" % pathList[:20])

            ccsd = ChemCompSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=None, workPath=os.path.join(HERE, "test-output"), cleanUp=False,
                                  warnings='default', verbose=self.__verbose)

            #
            mpu = MultiProcUtil(verbose=True)
            mpu.set(workerObj=sml, workerMethod="makeLoadFilesMulti")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=pathList, numProc=numProc, numResults=2)
            #
            containerNameList = retLists[0]
            tList = retLists[1]

            for tId, fn in tList:
                logger.debug("\nCreated table %s load file %s\n" % (tId, fn))
            #

            endTime1 = time.time()
            logger.debug("\nBatch files created in %.2f seconds\n" % (endTime1 - startTime))
            cObj = self.open(dbName=ccsd.getDatabaseName())
            dbCon = self.getClientConnection(cObj)
            self.schemaCreate(ccsd, dbCon)

            sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=dbCon, workPath=os.path.join(HERE, "test-output"), cleanUp=False,
                                  warnings='default', verbose=self.__verbose)
            #
            for tId, fn in tList:
                sdl.delete(tId, containerNameList=containerNameList, deleteOpt='all')
            self.close(cObj)
            #
            mpu = MultiProcUtil(verbose=True)
            mpu.set(workerObj=self, workerMethod="loadBatchFilesMulti")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=tList, numProc=numProc, numResults=1)

            endTime2 = time.time()
            logger.debug("\nLoad completed in %.2f seconds\n" % (endTime2 - endTime1))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def loadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemCompLoaderTests("testConnect"))
    suiteSelect.addTest(ChemCompLoaderTests("testListFiles"))
    suiteSelect.addTest(ChemCompLoaderTests("testLoadFiles"))
    suiteSelect.addTest(ChemCompLoaderTests("testLoadFilesMulti"))
    return suiteSelect

if __name__ == '__main__':
    #
    mySuite = loadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
