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
import time
import unittest

from rcsb.db.crate.Connection import Connection
from rcsb.db.crate.CrateDbLoader import CrateDbLoader
from rcsb.db.crate.CrateDbUtil import CrateDbQuery

#
from rcsb.db.sql.SqlGen import SqlGenAdmin
from rcsb.utils.repository.RepositoryProvider import RepositoryProvider
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefLoadercrateDbMultiTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(SchemaDefLoadercrateDbMultiTests, self).__init__(methodName)
        self.__verbose = True
        self.__createFlag = True

    def setUp(self):
        self.__verbose = True
        self.__numProc = 2
        self.__fileLimit = 100
        self.__chunkSize = 0
        self.__workPath = os.path.join(HERE, "test-output")
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName)
        self.__resourceName = "CRATE_DB"
        self.__schP = SchemaProvider(self.__cfgOb, self.__workPath, useCache=True)
        self.__rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, cachePath=self.__workPath)
        #
        #
        self.__tableIdSkipD = {"ATOM_SITE": True, "ATOM_SITE_ANISOTROP": True, "__LOAD_STATUS__": True}
        self.__ioObj = IoAdapter(verbose=self.__verbose)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testConnection(self):
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                self.assertNotEqual(client, None)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSchemaCreate(self):
        """Create table schema (live) for BIRD, chemical component, and PDBx data."""
        try:
            sd, _, _, _ = self.__schP.getSchemaInfo("bird")
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schP.getSchemaInfo("chem_comp")
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schP.getSchemaInfo("pdbx")
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSchemaRemove(self):
        """Remove table schema (live) for BIRD, chemical component, and PDBx data."""
        try:
            sd, _, _, _ = self.__schP.getSchemaInfo("bird")
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schP.getSchemaInfo("chem_comp")
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schP.getSchemaInfo("pdbx")
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadChemCompMulti(self):
        self.__testLoadFilesMulti("chem_comp")

    def testLoadBirdMulti(self):
        self.__testLoadFilesMulti("bird")

    def testLoadPdbxMulti(self):
        self.__testLoadFilesMulti("pdbx")

    def __getPathList(self, fType):
        pathList = []
        if fType == "chem_comp":
            pathList = self.__rpP.getLocatorObjList("chem_comp")
        elif fType == "bird":
            pathList = self.__rpP.getLocatorObjList("bird")
            pathList.extend(self.__rpP.getLocatorObjList("bird_family"))
        elif fType == "pdbx":
            pathList = self.__rpP.getLocatorObjList("pdbx")
        return pathList

    def loadInsertMany(self, dataList, procName, optionsD, workingDir):

        try:
            _ = workingDir
            ret = None
            sd = optionsD["sd"]
            skipD = optionsD["skip"]
            ioObj = IoAdapter(verbose=self.__verbose)
            logger.debug("%s pathlist %r", procName, dataList)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = CrateDbLoader(schemaDefObj=sd, ioObj=ioObj, dbCon=client, workPath=self.__workPath, cleanUp=False, warnings="default", verbose=self.__verbose)
                ret = sdl.load(inputPathList=dataList, loadType="crate-insert-many", deleteOpt="selected", tableIdSkipD=skipD)
            # all or nothing here
            if ret:
                return dataList, dataList, []
            else:
                return [], [], []
        except Exception as e:
            logger.info("Failing with dataList %r", dataList)
            logger.exception("Failing with %s", str(e))

        return [], [], []

    def __testLoadFilesMulti(self, contentType):
        """Test case - create load w/insert-many all chemical component definition data files - (multiproc test)"""
        numProc = self.__numProc
        chunkSize = self.__chunkSize
        try:
            #
            sd, _, _, _ = self.__schP.getSchemaInfo(contentType)
            if self.__createFlag:
                self.__schemaCreate(schemaDefObj=sd)

            optD = {}
            optD["sd"] = sd
            if contentType == "pdbx":
                optD["skip"] = self.__tableIdSkipD
            else:
                optD["skip"] = {}

            #
            pathList = self.__getPathList(fType=contentType)
            logger.debug("Input path list %r", pathList)
            mpu = MultiProcUtil(verbose=True)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="loadInsertMany")
            ok, _, _, _ = mpu.runMulti(dataList=pathList, numProc=numProc, numResults=1, chunkSize=chunkSize)
            self.assertEqual(ok, True)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def __schemaCreate(self, schemaDefObj):
        """Test case -  create table schema using schema definition"""
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

            logger.debug("Schema creation SQL string\n %s\n\n", "\n".join(sqlL))
            logger.info("Creating schema using database %s", schemaDefObj.getVersionedDatabaseName())
            #
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                crQ = CrateDbQuery(dbcon=client, verbose=self.__verbose)
                ret = crQ.sqlCommandList(sqlCommandList=sqlL)
                logger.debug("Schema create command returns %r\n", ret)
            return ret
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def __schemaRemove(self, schemaDefObj):
        """Test case -  remove table schema using schema definition"""
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

            logger.debug("Schema Remove SQL string\n %s", "\n".join(sqlL))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                crQ = CrateDbQuery(dbcon=client, verbose=self.__verbose)
                ret = crQ.sqlCommandList(sqlCommandList=sqlL)
                logger.debug("Schema remove command returns %r\n", ret)
            return ret
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
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


if __name__ == "__main__":

    mySuite = baseSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = loadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
