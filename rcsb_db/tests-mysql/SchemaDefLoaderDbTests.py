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
import sys
import time
import unittest

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.mysql.Connection import Connection
from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.mysql.SchemaDefLoader import SchemaDefLoader
from rcsb_db.sql.SqlGen import SqlGenAdmin
from rcsb_db.utils.ConfigUtil import ConfigUtil
from rcsb_db.utils.SchemaDefUtil import SchemaDefUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


class SchemaDefLoaderDbTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefLoaderDbTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        self.__verbose = True
        # default database
        self.__ioObj = IoAdapter(verbose=self.__verbose)
        #
        fileLimit = 100
        numProc = 2
        workPath = os.path.join(HERE, 'test-output')
        mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data")
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        #
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName, mockTopPath=mockTopPath)
        self.__resourceName = "MYSQL_DB"
        #
        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=numProc, fileLimit=fileLimit, workPath=workPath)
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

    def __schemaCreate(self, schemaDefObj):
        """ Create table schema using schema definition
        """
        try:
            tableIdList = schemaDefObj.getSchemaIdList()
            sqlGen = SqlGenAdmin(self.__verbose)
            sqlL = sqlGen.createDatabaseSQL(schemaDefObj.getDatabaseName())
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getSchemaObject(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("Schema creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                myQ = MyDbQuery(dbcon=client, verbose=self.__verbose)
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
        sd, dbName, _, _ = self.__schU.getSchemaInfo(schemaName='bird', applicationName='SQL')
        self.__schemaCreate(schemaDefObj=sd)
        #
        sd, dbName, _, _ = self.__schU.getSchemaInfo(schemaName='chem_comp', applicationName='SQL')
        self.__schemaCreate(schemaDefObj=sd)
        #
        sd, dbName, _, _ = self.__schU.getSchemaInfo(schemaName='pdbx', applicationName='SQL')
        self.__schemaCreate(schemaDefObj=sd)

    def testLoadBirdReference(self):
        try:
            sd, dbName, _, _ = self.__schU.getSchemaInfo(schemaName='bird', applicationName='SQL')
            self.__schemaCreate(schemaDefObj=sd)

            inputPathList = self.__schU.getPathList(schemaName='bird')
            inputPathList.extend(self.__schU.getPathList(schemaName='bird_family'))
            #
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(schemaDefObj=sd, dbCon=client, workPath=os.path.join(HERE, "test-output"),
                                      cleanUp=False, warnings='error', verbose=self.__verbose)
                ok = sdl.load(inputPathList=inputPathList, loadType='batch-file')
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testReLoadBirdReference(self):
        try:
            sd, dbName, _, _ = self.__schU.getSchemaInfo(schemaName='bird', applicationName='SQL')
            self.__schemaCreate(schemaDefObj=sd)

            inputPathList = self.__schU.getPathList(schemaName='bird')
            inputPathList.extend(self.__schU.getPathList(schemaName='bird_family'))
            #
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(schemaDefObj=sd, dbCon=client, workPath=os.path.join(HERE, "test-output"),
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
            sd, dbName, _, _ = self.__schU.getSchemaInfo(schemaName='chem_comp', applicationName='SQL')
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__schU.getPathList(schemaName='chem_comp')
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(schemaDefObj=sd, dbCon=client, workPath=os.path.join(HERE, "test-output"),
                                      cleanUp=False, warnings='error', verbose=self.__verbose)
                ok = sdl.load(inputPathList=inputPathList, loadType='batch-file')
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadPdbxFiles(self):
        try:
            sd, dbName, _, _ = self.__schU.getSchemaInfo(schemaName='pdbx', applicationName='SQL')
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__schU.getPathList(schemaName='pdbx')
            logger.debug("Input path list %r " % inputPathList)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                sdl = SchemaDefLoader(schemaDefObj=sd, dbCon=client, workPath=os.path.join(HERE, "test-output"),
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
