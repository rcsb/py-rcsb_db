##
# File:    SchemaDefLoaderCrateDbTests.py
# Author:  J. Westbrook
# Date:    21-Dec-2017
# Version: 0.001
#
# Updates:
#
#   All s
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

from rcsb_db.crate.CrateDbUtil import CrateDbConnect, CrateDbQuery

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
from mmcif_utils.chemcomp.PdbxChemCompIo import PdbxChemCompIo

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


class SchemaDefLoaderCrateDbTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefLoaderCrateDbTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__debug = False
        self.__dbCon = None

    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = True
        # default database
        #
        self.__dbHost = os.getenv("CRATE_DB_HOST", 'localhost')
        self.__dbPort = os.getenv("CRATE_DB_PORT", 4200)

        #
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__fileLimit = 100
        #
        self.__birdCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        self.__birdFamilyCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_FAMILY_REPO")
        self.__ccCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_CHEM_COMP_REPO")
        #
        self.__ccPath = os.path.join(TOPDIR, "rcsb_db", "data")
        #
        # Local model files for testing -
        self.__pdbxPath = os.path.join(TOPDIR, "rcsb_db", "data")
        self.__pdbxFileList = ['1cbs.cif', '1o3q.cif', '1xbb.cif', '3of4.cif', '3oqp.cif', '3rer.cif', '3rij.cif', '5hoh.cif']
        self.__tableIdSkipD = {'ATOM_SITE': True, 'ATOM_SITE_ANISOTROP': True}
        self.__ioObj = IoAdapter(verbose=self.__verbose)
        #
        #
        self.open(self.__dbHost, self.__dbPort)
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

    def open(self, dbHost, dbPort):
        self.close()
        try:
            myC = CrateDbConnect(dbHost=dbHost, dbPort=dbPort)
            self.__dbCon = myC.connect()
            if self.__dbCon is not None:
                return True
            else:
                return False
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            return False

    def close(self):
        try:
            if self.__dbCon is not None:
                self.__dbCon.close()
                self.__dbCon = None
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.__dbCon = None

    def testConnection(self):
        try:
            ok = self.open(self.__dbHost, self.__dbPort)
            self.close()
            self.assertEqual(ok, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testClusterConnections(self):
        try:
            clusterHostList = str(os.getenv("CRATE_DB_CLUSTER_HOSTS", "")).split(',')
            #
            for clusterHost in clusterHostList:
                ok = self.open(self.__dbHost, self.__dbPort)
                self.close()
                self.assertEqual(ok, True, "Connection failing for node %s" % clusterHost)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSchemaCreateSQL(self):
        """  Create table schema SQL for BIRD, chemical component, and PDBx data.
        """
        try:
            sd = BirdSchemaDef(convertNames=True)
            sL = self.__schemaCreateSQL(schemaDefObj=sd)
            self.assertGreater(len(sL), 0)
            #
            sd = ChemCompSchemaDef(convertNames=True)
            sL = self.__schemaCreateSQL(schemaDefObj=sd)
            self.assertGreater(len(sL), 0)
            #
            sd = PdbxSchemaDef(convertNames=True)
            sL = self.__schemaCreateSQL(schemaDefObj=sd)
            self.assertGreater(len(sL), 0)
            #
            sd = DaInternalSchemaDef(convertNames=True)
            sL = self.__schemaCreateSQL(schemaDefObj=sd)
            self.assertGreater(len(sL), 0)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSchemaCreate(self):
        """  Create table schema (live) for BIRD, chemical component, and PDBx data.
        """
        try:
            sd = BirdSchemaDef(convertNames=True)
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd = ChemCompSchemaDef(convertNames=True)
            ret = self.__schemaCreate(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd = PdbxSchemaDef(convertNames=True)
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
            sd = BirdSchemaDef(convertNames=True)
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd = ChemCompSchemaDef(convertNames=True)
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
            sd = PdbxSchemaDef(convertNames=True)
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadInsertBirdReference(self):

        try:
            sd = BirdSchemaDef(convertNames=True)
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPrdPathList()
            inputPathList.extend(self.__getPrdFamilyPathList())
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.',
                                  cleanUp=False, warnings='default', verbose=self.__verbose)
            ret = sdl.load(inputPathList=inputPathList, loadType='crate-insert', deleteOpt='selected')
            self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadInsertManyBirdReference(self):
        try:
            sd = BirdSchemaDef(convertNames=True)
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPrdPathList()
            inputPathList.extend(self.__getPrdFamilyPathList())
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.',
                                  cleanUp=False, warnings='default', verbose=self.__verbose)
            ret = sdl.load(inputPathList=inputPathList, loadType='crate-insert-many', deleteOpt='selected')
            self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadInsertChemCompReference(self):

        try:
            sd = ChemCompSchemaDef(convertNames=True)
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getChemCompPathList()
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False, warnings='default', verbose=self.__verbose)
            ret = sdl.load(inputPathList=inputPathList, loadType='crate-insert', deleteOpt='selected')
            self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadInsertManyChemCompReference(self):

        try:
            sd = ChemCompSchemaDef(convertNames=True)
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getChemCompPathList()
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False, warnings='default', verbose=self.__verbose)
            ret = sdl.load(inputPathList=inputPathList, loadType='crate-insert-many', deleteOpt='selected')
            self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadInsertPdbxExampleFiles(self):
        try:
            sd = PdbxSchemaDef(convertNames=True)
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPdbxPathList()
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False, warnings='default', verbose=self.__verbose)
            ret = sdl.load(inputPathList=inputPathList, loadType='crate-insert', deleteOpt='selected', tableIdSkipD=self.__tableIdSkipD)
            self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadInsertManyPdbxExampleFiles(self):
        try:
            sd = PdbxSchemaDef(convertNames=True)
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPdbxPathList()
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False, warnings='default', verbose=self.__verbose)
            ret = sdl.load(inputPathList=inputPathList, loadType='crate-insert-many', deleteOpt='selected', tableIdSkipD=self.__tableIdSkipD)
            self.assertEqual(ret, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __schemaCreateSQL(self, schemaDefObj):
        """Test case -  create table schema using schema definition
        """
        sqlL = []
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh, serverType="cratedb")

            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getVersionedDatabaseName(), tableDefObj=tableDefObj))
            logger.debug("\nSchema creation SQL string\n %s\n\n" % '\n'.join(sqlL))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()
        return sqlL

    def __schemaCreate(self, schemaDefObj):
        """Test case -  create table schema using schema definition
        """
        ret = 0
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh, serverType="cratedb")
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getVersionedDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("\nSchema creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            logger.info("Creating schema using database %s" % schemaDefObj.getVersionedDatabaseName())
            #
            crQ = CrateDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
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
            sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh, serverType="cratedb")
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.dropTableSQL(databaseName=schemaDefObj.getVersionedDatabaseName(), tableDefObj=tableDefObj))
                sqlL.extend(sqlGen.dropTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("Schema Remove SQL string\n %s" % '\n'.join(sqlL))

            crQ = CrateDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            ret = crQ.sqlCommandList(sqlCommandList=sqlL)
            logger.debug("Schema remove command returns %r\n" % ret)
            return ret
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getPdbxPathList(self):
        """Test case -  get the path list of PDBx instance example files -
        """

        try:
            loadPathList = [os.path.join(self.__pdbxPath, v) for v in self.__pdbxFileList]
            logger.info("Length of PDBx file path list %d (limit %r)" % (len(loadPathList), self.__fileLimit))
            if self.__fileLimit:
                return loadPathList[:self.__fileLimit]
            else:
                return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getPrdPathList(self):
        """Test case -  get the path list of PRD definitions in the CVS repository.
        """
        logger.info("\nStarting %s %s\n" % (self.__class__.__name__,
                                            sys._getframe().f_code.co_name))
        try:
            refIo = PdbxPrdIo(verbose=self.__verbose)
            refIo.setCachePath(self.__birdCachePath)
            loadPathList = refIo.makeDefinitionPathList()
            logger.info("Length of BIRD file path list %d (limit %r)" % (len(loadPathList), self.__fileLimit))
            if self.__fileLimit:
                return loadPathList[:self.__fileLimit]
            else:
                return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getPrdFamilyPathList(self):
        """Test case -  get the path list of PRD Family definitions in the CVS repository.
        """
        logger.info("\nStarting %s %s\n" % (self.__class__.__name__,
                                            sys._getframe().f_code.co_name))
        try:
            refIo = PdbxFamilyIo(verbose=self.__verbose)
            refIo.setCachePath(self.__birdFamilyCachePath)
            loadPathList = refIo.makeDefinitionPathList()
            logger.info("Length of BIRD FAMILY file path list %d (limit %r)" % (len(loadPathList), self.__fileLimit))
            if self.__fileLimit:
                return loadPathList[:self.__fileLimit]
            else:
                return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getChemCompPathList(self):
        """Test case -  get the path list of definitions in the CVS repository.
        """
        logger.info("\nStarting %s %s\n" % (self.__class__.__name__,
                                            sys._getframe().f_code.co_name))
        try:
            refIo = PdbxChemCompIo(verbose=self.__verbose)
            refIo.setCachePath(self.__ccCachePath)
            loadPathList = refIo.makeComponentPathList()
            logger.info("Length of CCD file path list %d (limit %r)" % (len(loadPathList), self.__fileLimit))
            if self.__fileLimit:
                return loadPathList[:self.__fileLimit]
            else:
                return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def createConnectionSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testConnection"))
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testClusterConnections"))
    return suiteSelect


def removeSchemaSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testSchemaRemove"))
    return suiteSelect


def createSchemaSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testSchemaCreateSQL"))
    suiteSelect.addTest(SchemaDefLoaderCrateDbTests("testSchemaCreate"))
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


if __name__ == '__main__':
    #
    if (True):
        mySuite = createConnectionSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if (True):
        mySuite = removeSchemaSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if (False):
        mySuite = createSchemaSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    if (False):
        mySuite = loadBirdReferenceSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    if (False):
        mySuite = loadCCReferenceSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    if (False):
        mySuite = loadPdbxSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
