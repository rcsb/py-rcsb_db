##
# File:    SchemaDefLoaderDbTests.py
# Author:  J. Westbrook
# Date:    29-Mar-2018
# Version: 0.001
#
# Updates:
#
#
##
"""
Tests for creating and loading rdbms database using PDBx/mmCIF data files
and external schema definition.

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
from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.mysql.Connection import Connection

from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef
from rcsb_db.schema.ChemCompSchemaDef import ChemCompSchemaDef
from rcsb_db.schema.PdbxSchemaDef import PdbxSchemaDef

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


from rcsb_db.utils.RepoPathUtil import RepoPathUtil
from rcsb_db.utils.ConfigUtil import ConfigUtil


class SchemaDefLoaderDbTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefLoaderDbTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        self.__verbose = True
        # default database
        self.__ioObj = IoAdapter(verbose=self.__verbose)
        #
        self.__fileLimit = 100
        self.__numProc = 2
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data")
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "MYSQL_DB"
        connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName)
        #
        self.__cObj = self.__open(connectD)
        self.__dbCon = self.__getClientConnection(self.__cObj)

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

    def __getPathInfo(self, contentType, inputPathList=None):
        outputPathList = []
        inputPathList = inputPathList if inputPathList else []
        rpU = RepoPathUtil(self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
        try:
            if contentType == "bird":
                outputPathList = inputPathList if inputPathList else rpU.getBirdPathList()
            elif contentType == "bird_family":
                outputPathList = inputPathList if inputPathList else rpU.getBirdFamilyPathList()
            elif contentType == 'chem_comp':
                outputPathList = inputPathList if inputPathList else rpU.getChemCompPathList()
            elif contentType == 'bird_chem_comp':
                outputPathList = inputPathList if inputPathList else rpU.getBirdChemCompPathList()
            elif contentType == 'pdbx':
                outputPathList = inputPathList if inputPathList else rpU.getEntryPathList()
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        if self.__fileLimit:
            inputPathList = inputPathList[:self.__fileLimit]

        return outputPathList

    def __getSchemaInfo(self, contentType):
        sd = None
        dbName = None
        try:
            if contentType == "bird":
                sd = BirdSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
            elif contentType == "bird_family":
                sd = BirdSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
            elif contentType == 'chem_comp':
                sd = ChemCompSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
            elif contentType == 'bird_chem_comp':
                sd = ChemCompSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
            elif contentType == 'pdbx':
                sd = PdbxSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
            else:
                logger.warning("Unsupported contentType %s" % contentType)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return sd, dbName

    def __schemaCreate(self, schemaDefObj):
        """ Create table schema using schema definition
        """
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = MyDbAdminSqlGen(self.__verbose)
            sqlL = sqlGen.createDatabaseSQL(schemaDefObj.getDatabaseName())
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("Schema creation SQL string\n %s\n\n" % '\n'.join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            #
            # Permit warnings to support "drop table if exists" for missing tables.
            #
            myQ.setWarning('ignore')
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            logger.debug("\n\n+INFO mysql server returns %r\n" % ret)
            self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

# ------------- - -------------------------------------------------------------------------------------------
    def testSchemaCreate(self):
        """  Create table schema for BIRD, chemical component, and PDBx data.
        """
        sd, dbName = self.__getSchemaInfo(contentType='bird')
        self.__schemaCreate(schemaDefObj=sd)
        #
        sd, dbName = self.__getSchemaInfo(contentType='bird_family')
        self.__schemaCreate(schemaDefObj=sd)

        sd, dbName = self.__getSchemaInfo(contentType='chem_comp')
        self.__schemaCreate(schemaDefObj=sd)

        sd, dbName = self.__getSchemaInfo(contentType='bird_chem_comp')
        self.__schemaCreate(schemaDefObj=sd)

        sd, dbName = self.__getSchemaInfo(contentType='pdbx')
        self.__schemaCreate(schemaDefObj=sd)

    def testLoadBirdReference(self):
        try:
            sd, dbName = self.__getSchemaInfo(contentType='bird')
            self.__schemaCreate(schemaDefObj=sd)

            inputPathList = self.__getPathInfo(contentType='bird')
            inputPathList.extend(self.__getPathInfo(contentType='bird_family'))
            #
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=os.path.join(HERE, "test-output"),
                                  cleanUp=False, warnings='error', verbose=self.__verbose)
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-file')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testReLoadBirdReference(self):
        try:
            sd, dbName = self.__getSchemaInfo(contentType='bird')
            self.__schemaCreate(schemaDefObj=sd)

            inputPathList = self.__getPathInfo(contentType='bird')
            inputPathList.extend(self.__getPathInfo(contentType='bird_family'))
            #
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=os.path.join(HERE, "test-output"),
                                  cleanUp=False, warnings='error', verbose=self.__verbose)
            sdl.load(inputPathList=inputPathList, loadType='batch-file')
            #
            logger.debug("INFO BATCH FILE RELOAD TEST --------------------------------------------\n")
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-file', deleteOpt='all')
            self.assertTrue(ok)
            #
            logger.debug("\n\n\n+INFO BATCH INSERT RELOAD TEST --------------------------------------------\n")
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-file', deleteOpt='selected')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadChemCompReference(self):
        try:
            sd, dbName = self.__getSchemaInfo(contentType='chem_comp')
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPathInfo(contentType='chem_comp')

            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=os.path.join(HERE, "test-output"),
                                  cleanUp=False, warnings='error', verbose=self.__verbose)
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-file')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadPdbxFiles(self):
        try:
            sd, dbName = self.__getSchemaInfo(contentType='pdbx')
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPathInfo(contentType='pdbx')

            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=os.path.join(HERE, "test-output"),
                                  cleanUp=False, warnings='error', verbose=self.__verbose)
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-insert', deleteOpt='all')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
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
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadPdbxFiles"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = createSchemaSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    if True:
        mySuite = loadReferenceSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
