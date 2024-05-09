##
# File:    SchemaDefLoaderDbTests.py
# Author:  J. Westbrook
# Date:    29-Mar-2018
# Version: 0.001
#
# Updates:
#   20-Jun-2018  jdw updates for new schema generation and data preparation tools
#
##
"""
Tests for creating and loading rdbms database (mysql) using PDBx/mmCIF data files
and external schema definition.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import platform
import time
import unittest

from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.db.mysql.Connection import Connection
from rcsb.db.mysql.MyDbUtil import MyDbQuery
from rcsb.db.mysql.SchemaDefLoader import SchemaDefLoader
from rcsb.db.sql.SqlGen import SqlGenAdmin
from rcsb.utils.repository.RepositoryProvider import RepositoryProvider
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefLoaderDbTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(SchemaDefLoaderDbTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        self.__isMac = platform.system() == "Darwin"
        self.__excludeTypeL = None if self.__isMac else ["optional"]
        self.__verbose = True
        #
        fileLimit = 100
        numProc = 2
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__workPath = os.path.join(HERE, "test-output")
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__resourceName = "MYSQL_DB"
        #
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=True)
        self.__rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=numProc, fileLimit=fileLimit, cachePath=self.__cachePath)
        #
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def __schemaCreate(self, schemaDefObj):
        """Create table schema using schema definition"""
        try:
            tableIdList = schemaDefObj.getSchemaIdList()
            sqlGen = SqlGenAdmin(self.__verbose)
            sqlL = sqlGen.createDatabaseSQL(schemaDefObj.getDatabaseName())
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getSchemaObject(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("Schema creation SQL string\n %s\n\n", "\n".join(sqlL))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                myQ = MyDbQuery(dbcon=client, verbose=self.__verbose)
                #
                # Permit warnings to support "drop table if exists" for missing tables.
                #
                myQ.setWarning("ignore")
                ret = myQ.sqlCommand(sqlCommandList=sqlL)
                logger.debug("\n\n+INFO mysql server returns %r\n", ret)
                self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    # ------------- - -------------------------------------------------------------------------------------------
    def testSchemaCreate(self):
        """Create table schema for BIRD, chemical component, and PDBx data."""
        cD = self.__schP.makeSchemaDef("bird", dataTyping="SQL", saveSchema=True)
        sd = SchemaDefAccess(cD)
        self.__schemaCreate(sd)
        #
        cD = self.__schP.makeSchemaDef("chem_comp", dataTyping="SQL", saveSchema=True)
        sd = SchemaDefAccess(cD)
        self.__schemaCreate(sd)
        #
        # cD = self.__schP.makeSchemaDef("pdbx", dataTyping="SQL", saveSchema=True)
        # sd = SchemaDefAccess(cD)
        self.__schemaCreate(sd)

    def testLoadBirdReference(self):
        try:
            cD = self.__schP.makeSchemaDef("bird", dataTyping="SQL", saveSchema=True)
            sd = SchemaDefAccess(cD)
            self.__schemaCreate(sd)

            inputPathList = self.__rpP.getLocatorObjList(contentType="bird")
            inputPathList.extend(self.__rpP.getLocatorObjList(contentType="bird_family"))
            #
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(
                    self.__cfgOb,
                    schemaDefObj=sd,
                    dbCon=client,
                    cachePath=self.__cachePath,
                    workPath=self.__workPath,
                    cleanUp=False,
                    warnings="error",
                    verbose=self.__verbose,
                    restoreUseStash=False,
                    restoreUseGit=True,
                    providerTypeExcludeL=self.__excludeTypeL,
                )
                ok = sdl.load(inputPathList=inputPathList, loadType="batch-file")
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testReLoadBirdReference(self):
        try:
            cD = self.__schP.makeSchemaDef("bird", dataTyping="SQL", saveSchema=True)
            sd = SchemaDefAccess(cD)
            self.__schemaCreate(sd)

            inputPathList = self.__rpP.getLocatorObjList(contentType="bird")
            inputPathList.extend(self.__rpP.getLocatorObjList(contentType="bird_family"))
            #
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(
                    self.__cfgOb,
                    schemaDefObj=sd,
                    dbCon=client,
                    cachePath=self.__cachePath,
                    workPath=self.__workPath,
                    cleanUp=False,
                    warnings="error",
                    verbose=self.__verbose,
                    restoreUseStash=False,
                    restoreUseGit=True,
                    providerTypeExcludeL=self.__excludeTypeL,
                )
                sdl.load(inputPathList=inputPathList, loadType="batch-file")
                #
                logger.debug("INFO BATCH FILE RELOAD TEST --------------------------------------------\n")
                ok = sdl.load(inputPathList=inputPathList, loadType="batch-file", deleteOpt="all")
                self.assertTrue(ok)
                #
                logger.debug("\n\n\n+INFO BATCH INSERT RELOAD TEST --------------------------------------------\n")
                ok = sdl.load(inputPathList=inputPathList, loadType="batch-file", deleteOpt="selected")
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadChemCompReference(self):
        try:
            cD = self.__schP.makeSchemaDef("chem_comp", dataTyping="SQL", saveSchema=True)
            sd = SchemaDefAccess(cD)
            self.__schemaCreate(sd)

            inputPathList = self.__rpP.getLocatorObjList(contentType="chem_comp")
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(
                    self.__cfgOb,
                    schemaDefObj=sd,
                    dbCon=client,
                    cachePath=self.__cachePath,
                    workPath=self.__workPath,
                    cleanUp=False,
                    warnings="error",
                    verbose=self.__verbose,
                    restoreUseStash=False,
                    restoreUseGit=True,
                    providerTypeExcludeL=self.__excludeTypeL,
                )
                ok = sdl.load(inputPathList=inputPathList, loadType="batch-file")
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skip("Disable test - schema not optimized for mysql limitations")
    def testLoadPdbxFiles(self):
        try:
            cD = self.__schP.makeSchemaDef("pdbx", dataTyping="SQL", saveSchema=True)
            sd = SchemaDefAccess(cD)
            self.__schemaCreate(sd)

            inputPathList = self.__rpP.getLocatorObjList(contentType="pdbx")
            logger.debug("Input path list %r", inputPathList)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(
                    self.__cfgOb,
                    schemaDefObj=sd,
                    dbCon=client,
                    cachePath=self.__cachePath,
                    workPath=self.__workPath,
                    cleanUp=False,
                    warnings="error",
                    verbose=self.__verbose,
                    restoreUseStash=False,
                    restoreUseGit=True,
                    providerTypeExcludeL=self.__excludeTypeL,
                )
                ok = sdl.load(inputPathList=inputPathList, loadType="batch-insert", deleteOpt="all")
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def createSchemaSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testSchemaCreate"))
    return suiteSelect


def loadReferenceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadBirdReference"))
    suiteSelect.addTest(SchemaDefLoaderDbTests("testReLoadBirdReference"))
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadChemCompReference"))
    # suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadPdbxFiles"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = createSchemaSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    mySuite = loadReferenceSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
