##
# File:    SchemaDefLoaderCrateDbTests.py
# Author:  J. Westbrook
# Date:    21-Dec-2017
# Version: 0.001
#
# Updates:
#
#   2-Apr-2018  jdw
##
"""
Tests for creating and loading distributed rdbms database using PDBx/mmCIF data files
and external schema definitions using CrateDb services -  Covers BIRD, CCD and PDBx/mmCIF
model files -

The following test settings from the enviroment will be used will a fallback to localhost/4200.

    CRATE_DB_HOST
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
from rcsb.db.sql.SqlGen import SqlGenAdmin
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefLoaderCrateDbTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(SchemaDefLoaderCrateDbTests, self).__init__(methodName)
        self.__verbose = True
        self.__debug = False

    def setUp(self):
        self.__verbose = True
        self.__numProc = 2
        self.__fileLimit = 100
        self.__workPath = os.path.join(HERE, "test-output")
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName)
        self.__resourceName = "CRATE_DB"
        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
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
        """  Create table schema (live) for BIRD, chemical component, and PDBx data.
        """
        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="bird")
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="chem_comp")
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="pdbx")
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSchemaRemove(self):
        """  Remove table schema (live) for BIRD, chemical component, and PDBx data.
        """
        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="bird")
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="chem_comp")
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="pdbx")
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadInsertBirdReference(self):

        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="bird")
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__schU.getLocatorObjList(contentType="bird")
            inputPathList.extend(self.__schU.getLocatorObjList(contentType="bird_family"))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = CrateDbLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=client, workPath=self.__workPath, cleanUp=False, warnings="default", verbose=self.__verbose)
                ret = sdl.load(inputPathList=inputPathList, loadType="crate-insert", deleteOpt="selected")
                self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadInsertManyBirdReference(self):
        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="bird")
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__schU.getLocatorObjList(contentType="bird")
            inputPathList.extend(self.__schU.getLocatorObjList(contentType="bird_family"))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = CrateDbLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=client, workPath=self.__workPath, cleanUp=False, warnings="default", verbose=self.__verbose)
                ret = sdl.load(inputPathList=inputPathList, loadType="crate-insert-many", deleteOpt="selected")
                self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadInsertChemCompReference(self):

        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="chem_comp")
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__schU.getLocatorObjList(contentType="chem_comp")
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = CrateDbLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=client, workPath=self.__workPath, cleanUp=False, warnings="default", verbose=self.__verbose)
                ret = sdl.load(inputPathList=inputPathList, loadType="crate-insert", deleteOpt="selected")
                self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadInsertManyChemCompReference(self):

        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="chem_comp")
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__schU.getLocatorObjList(contentType="chem_comp")
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = CrateDbLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=client, workPath=self.__workPath, cleanUp=False, warnings="default", verbose=self.__verbose)
                ret = sdl.load(inputPathList=inputPathList, loadType="crate-insert-many", deleteOpt="selected")
                self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadInsertPdbxExampleFiles(self):
        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="pdbx")
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__schU.getLocatorObjList(contentType="pdbx")
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = CrateDbLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=client, workPath=self.__workPath, cleanUp=False, warnings="default", verbose=self.__verbose)
                ret = sdl.load(inputPathList=inputPathList, loadType="crate-insert", deleteOpt="selected", tableIdSkipD=self.__tableIdSkipD)
                self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadInsertManyPdbxExampleFiles(self):
        try:
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="pdbx")
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__schU.getLocatorObjList(contentType="pdbx")
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = CrateDbLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=client, workPath=self.__workPath, cleanUp=False, warnings="default", verbose=self.__verbose)
                ret = sdl.load(inputPathList=inputPathList, loadType="crate-insert-many", deleteOpt="selected", tableIdSkipD=self.__tableIdSkipD)
                self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
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

            logger.debug("\nSchema creation SQL string\n %s\n\n", "\n".join(sqlL))
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


def createConnectionSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testConnection"))
    return suiteSelect


def createSchemaSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testSchemaCreate"))
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testSchemaRemove"))
    return suiteSelect


def loadBirdReferenceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testLoadInsertBirdReference"))
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testLoadInsertManyBirdReference"))
    return suiteSelect


def loadCCReferenceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testLoadInsertChemCompReference"))
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testLoadInsertManyChemCompReference"))
    return suiteSelect


def loadPdbxSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testLoadInsertPdbxExampleFiles"))
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testLoadInsertManyPdbxExampleFiles"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = createConnectionSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = createSchemaSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #

    mySuite = loadBirdReferenceSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #

    mySuite = loadCCReferenceSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #

    mySuite = loadPdbxSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
#
