##
# File:    SchemaDefLoaderDbTests.py
# Author:  J. Westbrook
# Date:    12-Jan-2013
# Version: 0.001
#
# Updates:
#
# 13-Jan-2013 jdw  Schema creation and loading tests provided for Bird, Chemical Component and PDBx
#                  entry files.
# 20-Jan-2013 jdw  Add test for materializing sequences data for Bird definitions.
# 21-Jan-2013 jdw  add family definition files with PRD/Family loading
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
import traceback

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
from rcsb_db.mysql.MyDbUtil import MyDbConnect, MyDbQuery

from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef
from rcsb_db.schema.ChemCompSchemaDef import ChemCompSchemaDef
from rcsb_db.schema.PdbxSchemaDef import PdbxSchemaDef
from rcsb_db.schema.DaInternalSchemaDef import DaInternalSchemaDef

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter

from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo
from mmcif_utils.bird.PdbxFamilyIo import PdbxFamilyIo
from pdbx_v2.bird.PdbxPrdUtils import PdbxPrdUtils
from mmcif_utils.chemcomp.PdbxChemCompIo import PdbxChemCompIo


class SchemaDefLoaderDbTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefLoaderDbTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__debug = False

    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = True
        # default database
        self.__databaseName = 'prdv4'
        self.__birdCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        self.__birdFamilyCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_FAMILY_REPO")
        self.__ccCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_CHEM_COMP_REPO")
        #
        self.__ccFileList = ["BA1T.cif"]
        self.__ccPath = os.path.join(TOPDIR, "rcsb_db", "data")
        #
        self.__pdbxPath = os.path.join(TOPDIR, "rcsb_db", "data")
        self.__pdbxFileList = ['1cbs.cif', '1o3q.cif', '1xbb.cif', '3of4.cif', '3oqp.cif', '3rer.cif', '3rij.cif', '5hoh.cif']

        self.__ioObj = IoAdapter(verbose=self.__verbose)
        #
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

    def testSchemaCreate(self):
        """  Create table schema for BIRD, chemical component, and PDBx data.
        """
        sd = BirdSchemaDef()
        self.__schemaCreate(schemaDefObj=sd)
        #
        sd = ChemCompSchemaDef()
        self.__schemaCreate(schemaDefObj=sd)
        #
        #sd = PdbxSchemaDef()
        #self.__schemaCreate(schemaDefObj=sd)

    def testLoadBirdReference(self):
        try:
            sd = BirdSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPrdPathList()
            inputPathList.extend(self.__getPrdFamilyPathList())
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=os.path.join(HERE, "test-output"),
                                  cleanUp=False, warnings='default', verbose=self.__verbose)
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-file')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testReloadBirdReference(self):
        try:
            sd = BirdSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPrdPathList()
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=os.path.join(HERE, "test-output"),
                                  cleanUp=False, warnings='default', verbose=self.__verbose)
            sdl.load(inputPathList=inputPathList, loadType='batch-file')
            #
            logger.debug("\n\n\n+INFO BATCH FILE RELOAD TEST --------------------------------------------\n")
            sdl.load(inputPathList=inputPathList, loadType='batch-file', deleteOpt='all')
            logger.debug("\n\n\n+INFO BATCH INSERT RELOAD TEST --------------------------------------------\n")
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-file', deleteOpt='selected')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdReferenceWithSequence(self):
        try:
            sd = BirdSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            #
            prd = PdbxPrdIo(verbose=self.__verbose)
            prd.setCachePath(self.__birdCachePath)
            self.__pathList = prd.makeDefinitionPathList()
            #
            for pth in self.__pathList:
                prd.setFilePath(pth)
            logger.debug("PRD repository read completed\n")
            #
            prdU = PdbxPrdUtils(prd, verbose=self.__verbose)
            rD = prdU.getComponentSequences(addCategory=True)
            #
            #
            prdFam = PdbxFamilyIo(verbose=self.__verbose)
            prdFam.setCachePath(self.__birdFamilyCachePath)
            self.__familyPathList = prdFam.makeDefinitionPathList()
            #
            for pth in self.__familyPathList:
                prdFam.setFilePath(pth)
            logger.debug("Family repository read completed\n")
            #
            # combine containers -
            containerList = prd.getCurrentContainerList()
            containerList.extend(prdFam.getCurrentContainerList())
            #
            # Run loader on container list --
            #
            sdl = SchemaDefLoader(
                schemaDefObj=sd,
                ioObj=self.__ioObj,
                dbCon=self.__dbCon,
                workPath=os.path.join(HERE, "test-output"),
                cleanUp=False,
                warnings='error',
                verbose=self.__verbose)
            ok = sdl.load(containerList=containerList, loadType='batch-file', deleteOpt='selected')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadChemCompReference(self):
        try:
            sd = ChemCompSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getChemCompPathList()
            sdl = SchemaDefLoader(
                schemaDefObj=sd,
                ioObj=self.__ioObj,
                dbCon=self.__dbCon,
                workPath=os.path.join(
                    HERE,
                    "test-output"),
                cleanUp=False,
                warnings='default',
                verbose=self.__verbose)
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-file')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    @unittest.skip("PDBx test skipping")
    def testLoadPdbxFiles(self):
        try:
            sd = PdbxSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPdbxPathList()
            sdl = SchemaDefLoader(
                schemaDefObj=sd,
                ioObj=self.__ioObj,
                dbCon=self.__dbCon,
                workPath=os.path.join(
                    HERE,
                    "test-output"),
                cleanUp=False,
                warnings='default',
                verbose=self.__verbose)
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-insert', deleteOpt='all')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadChemCompExamples(self):
        try:
            sd = ChemCompSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = [os.path.join(self.__ccPath, fn) for fn in self.__ccFileList]
            sdl = SchemaDefLoader(
                schemaDefObj=sd,
                ioObj=self.__ioObj,
                dbCon=self.__dbCon,
                workPath=os.path.join(HERE, "test-output"),
                cleanUp=False,
                warnings='default',
                verbose=self.__verbose)
            ok = sdl.load(inputPathList=inputPathList, loadType='batch-insert', deleteOpt="selected")
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testGenSchemaDaInternal(self):
        try:
            sd = DaInternalSchemaDef()
            self.__schemaCreateSQL(schemaDefObj=sd)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __schemaCreateSQL(self, schemaDefObj):
        """Test case -  create table schema using schema definition
        """
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("\nSchema creation SQL string\n %s\n\n" % '\n'.join(sqlL))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __schemaCreate(self, schemaDefObj):
        """Test case -  create table schema using schema definition
        """
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            if (self.__debug):
                logger.debug("\nSchema creation SQL string\n %s\n\n" % '\n'.join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            #
            # Permit warnings to support "drop table if exists" for missing tables.
            #
            myQ.setWarning('default')
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            if (self.__verbose):
                logger.debug("\n\n+INFO mysql server returns %r\n" % ret)
            self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getPdbxPathList(self):
        """Test case -  get the path list of PDBx instance example files -
        """
        try:
            loadPathList = [os.path.join(self.__pdbxPath, v) for v in self.__pdbxFileList]
            logger.debug("Length of PDBx file path list %d\n" % len(loadPathList))
            return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getPrdPathList(self):
        """Test case -  get the path list of PRD definitions in the CVS repository.
        """
        try:
            refIo = PdbxPrdIo(verbose=self.__verbose)
            refIo.setCachePath(self.__birdCachePath)
            loadPathList = refIo.makeDefinitionPathList()
            logger.debug("Length of CVS path list %d\n" % len(loadPathList))
            return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getPrdFamilyPathList(self):
        """Test case -  get the path list of PRD Family definitions in the CVS repository.
        """
        try:
            refIo = PdbxFamilyIo(verbose=self.__verbose)
            refIo.setCachePath(self.__birdFamilyCachePath)
            loadPathList = refIo.makeDefinitionPathList()
            logger.debug("Length of CVS path list %d\n" % len(loadPathList))
            return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getChemCompPathList(self):
        """Test case -  get the path list of definitions in the CVS repository.
        """
        try:
            refIo = PdbxChemCompIo(verbose=self.__verbose)
            refIo.setCachePath(self.__ccCachePath)
            loadPathList = refIo.makeComponentPathList()
            logger.debug("Length of CVS path list %d\n" % len(loadPathList))
            return loadPathList
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
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadChemCompReference"))
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadPdbxFiles"))
    return suiteSelect


def loadReferenceWithSequenceSuite():
    suiteSelect = unittest.TestSuite()
    # suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadBirdReference"))
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadBirdReferenceWithSequence"))
    return suiteSelect


def reloadReferenceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testReloadBirdReference"))
    return suiteSelect


def loadSpecialReferenceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadChemCompExamples"))
    return suiteSelect


def genSchemaSQLSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testGenSchemaDaInternal"))
    return suiteSelect

if __name__ == '__main__':
    #
    if True:
        mySuite = createSchemaSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = loadReferenceSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = reloadReferenceSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        # mySuite=loadSpecialReferenceSuite()
        # unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = loadReferenceWithSequenceSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = genSchemaSQLSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
