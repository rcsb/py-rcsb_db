##
# File:    SchemaDefLoaderCrateDbMultiTests.py
# Author:  J. Westbrook
# Date:    10-Feb-2018
# Version: 0.001
#
# Updates:
#      2-Apr-2018 jdw update for refactored api's an utils
#
##
"""
Tests for creating and loading distributed rdbms database using PDBx/mmCIF data files
and external schema definitions using crateDb services -  Covers BIRD, CCD and PDBx/mmCIF
model files - Multiprocessor mode tests

The following test settings from the configuration file be used will a fallback to localhost/26257.

    CRATE_DB_USER_NAME
      [CRATE_DB_PW]
    CRATE_DB_NAME
    CRATE_DB_HOST

See also the load length limit for each file type for testing  -  Set to None to remove -

        self.__fileLimit = 100

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

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

try:
    from rcsb.db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb.db import __version__

from rcsb.db.crate.Connection import Connection
from rcsb.db.crate.CrateDbLoader import CrateDbLoader
from rcsb.db.crate.CrateDbUtil import CrateDbQuery
#
from rcsb.db.sql.SqlGen import SqlGenAdmin
from rcsb.db.utils.ConfigUtil import ConfigUtil
from rcsb.db.utils.MultiProcUtil import MultiProcUtil
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


class SchemaDefLoadercrateDbMultiTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefLoadercrateDbMultiTests, self).__init__(methodName)
        self.__verbose = True
        self.__createFlag = True

    def setUp(self):
        self.__verbose = True
        self.__numProc = 2
        self.__fileLimit = 100
        self.__chunkSize = 0
        self.__workPath = os.path.join(HERE, "test-output")
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "db", "data")
        configPath = os.path.join(TOPDIR, 'rcsb', 'db', 'data', 'config', 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "CRATE_DB"
        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
        #
        self.__tableIdSkipD = {'ATOM_SITE': True, 'ATOM_SITE_ANISOTROP': True, '__LOAD_STATUS__': True}
        self.__ioObj = IoAdapter(verbose=self.__verbose)
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

    def testConnection(self):
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                self.assertNotEqual(client, None)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSchemaCreate(self):
        """  Create table schema (live) for BIRD, chemical component, and PDBx data.
        """
        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(schemaName='bird')
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(schemaName='chem_comp')
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(schemaName='pdbx')
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSchemaRemove(self):
        """  Remove table schema (live) for BIRD, chemical component, and PDBx data.
        """
        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(schemaName='bird')
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(schemaName='chem_comp')
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(schemaName='pdbx')
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadChemCompMulti(self):
        self.__testLoadFilesMulti(contentType="chem_comp")

    def testLoadBirdMulti(self):
        self.__testLoadFilesMulti(contentType="bird")

    def testLoadPdbxMulti(self):
        self.__testLoadFilesMulti(contentType="pdbx")

    def __getPathList(self, fType):
        pathList = []
        if fType == "chem_comp":
            pathList = self.__schU.getPathList(schemaName='chem_comp')
        elif fType == "bird":
            pathList = self.__schU.getPathList(schemaName='bird')
            pathList.extend(self.__schU.getPathList(schemaName='bird_family'))
        elif fType == "pdbx":
            pathList = self.__schU.getPathList(schemaName='pdbx')
        return pathList

    def loadInsertMany(self, dataList, procName, optionsD, workingDir):

        try:
            ret = None
            sd = optionsD['sd']
            skipD = optionsD['skip']
            ioObj = IoAdapter(verbose=self.__verbose)
            logger.debug("%s pathlist %r" % (procName, dataList))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = CrateDbLoader(schemaDefObj=sd, ioObj=ioObj, dbCon=client, workPath=self.__workPath, cleanUp=False, warnings='default', verbose=self.__verbose)
                ret = sdl.load(inputPathList=dataList, loadType='crate-insert-many', deleteOpt='selected', tableIdSkipD=skipD)
            # all or nothing here
            if ret:
                return dataList, dataList, []
            else:
                return [], [], []
        except Exception as e:
            logger.info("Failing with dataList %r" % dataList)
            logger.exception("Failing with %s" % str(e))

        return [], [], []

    def __testLoadFilesMulti(self, contentType):
        """Test case - create load w/insert-many all chemical component definition data files - (multiproc test)
        """
        numProc = self.__numProc
        chunkSize = self.__chunkSize
        try:
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(schemaName=contentType)
            if (self.__createFlag):
                self.__schemaCreate(schemaDefObj=sd)

            optD = {}
            optD['sd'] = sd
            if contentType == 'pdbx':
                optD['skip'] = self.__tableIdSkipD
            else:
                optD['skip'] = {}

            #
            pathList = self.__getPathList(fType=contentType)
            logger.debug("Input path list %r" % pathList)
            mpu = MultiProcUtil(verbose=True)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="loadInsertMany")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=pathList, numProc=numProc, numResults=1, chunkSize=chunkSize)
            self.assertEqual(ok, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __schemaCreate(self, schemaDefObj):
        """Test case -  create table schema using schema definition
        """
        ret = 0
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = SqlGenAdmin(self.__verbose, serverType="cratedb")
            sqlL = []
            for tableId in tableIdList:
                if tableId in self.__tableIdSkipD:
                    continue
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getVersionedDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("\nSchema creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            logger.info("Creating schema using database %s" % schemaDefObj.getVersionedDatabaseName())
            #
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                crQ = CrateDbQuery(dbcon=client, verbose=self.__verbose)
                ret = crQ.sqlCommandList(sqlCommandList=sqlL)
                logger.debug("Schema create command returns %r\n" % ret)
            return ret
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __schemaRemove(self, schemaDefObj):
        """Test case -  remove table schema using schema definition
        """
        ret = 0
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = SqlGenAdmin(self.__verbose, serverType="cratedb")
            sqlL = []
            for tableId in tableIdList:
                if tableId in self.__tableIdSkipD:
                    continue
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.dropTableSQL(databaseName=schemaDefObj.getVersionedDatabaseName(), tableDefObj=tableDefObj))
                sqlL.extend(sqlGen.dropTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("Schema Remove SQL string\n %s" % '\n'.join(sqlL))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                crQ = CrateDbQuery(dbcon=client, verbose=self.__verbose)
                ret = crQ.sqlCommandList(sqlCommandList=sqlL)
                logger.debug("Schema remove command returns %r\n" % ret)
            return ret
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def baseSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoadercrateDbMultiTests("testConnection"))
    suiteSelect.addTest(SchemaDefLoadercrateDbMultiTests("testSchemaCreate"))
    suiteSelect.addTest(SchemaDefLoadercrateDbMultiTests("testSchemaRemove"))
    return suiteSelect


def loadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoadercrateDbMultiTests("testConnection"))
    suiteSelect.addTest(SchemaDefLoadercrateDbMultiTests("testLoadChemCompMulti"))
    suiteSelect.addTest(SchemaDefLoadercrateDbMultiTests("testLoadBirdMulti"))
    suiteSelect.addTest(SchemaDefLoadercrateDbMultiTests("testLoadPdbxMulti"))
    return suiteSelect


if __name__ == '__main__':
    if True:
        mySuite = baseSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    if True:
        mySuite = loadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
