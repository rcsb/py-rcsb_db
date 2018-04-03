##
# File:    SchemaDefPdbxLoaderCockroachDbMultiTests.py
# Author:  J. Westbrook
# Date:    19-Jan-2018
# Version: 0.001
#
# Update:
##
"""
Tests for creating and loading distributed rdbms database using PDBx/mmCIF data files
and external schema definitions using CockroachDb services -   This version divides parsing
and loading tasks in multiple processes, see numProc and chunkSize.

The following test settings from the enviroment will be used will a fallback to localhost/4200.

The following test settings from the enviroment will be used will a fallback to localhost/26257.

    COCKROACH_DB_USER
      [COCKROACH_DB_PW]
    COCKROACH_DB_NAME
    COCKROACH_DB_HOST

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import os
import time
import scandir
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

from rcsb_db.sql.SqlGen import SqlGenAdmin
from rcsb_db.loaders.SchemaDefLoader import SchemaDefLoader
from rcsb_db.cockroach.CockroachDbUtil import CockroachDbConnect, CockroachDbQuery

from rcsb_db.schema.PdbxSchemaDef import PdbxSchemaDef
#

from rcsb_db.utils.MultiProcUtil import MultiProcUtil

try:
    # from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


#
# How to use the default preference
# from mmcif.io import IoAdapter
# from mmcif.io.PdbxExceptions import PdbxError, SyntaxError


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


class SchemaDefLoaderCockroachDbMultiTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefLoaderCockroachDbMultiTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__verbose = True
        self.__debug = False
        self.__dbCon = None

    def setUp(self):
        self.__verbose = True
        #
        # default database
        #
        self.__dbHost = os.getenv("COCKROACH_DB_HOST", 'localhost')
        self.__dbPort = os.getenv("COCKROACH_DB_PORT", 26257)
        self.__dbName = os.getenv("COCKROACH_DB_NAME", None)
        self.__dbUser = os.getenv("COCKROACH_DB_USER", 'root')
        #
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__fileLimit = None
        self.__numProc = 6
        # Default to divide the load evenly across mulitple processes.
        #  GUESSED FOR NOW  --
        # self.__chunkSize = 50
        self.__chunkSize = 15
        #
        self.__ccPath = os.path.join(TOPDIR, "rcsb_db", "data")
        #
        # Local model files for testing -
        self.__pdbxPath = os.path.join(TOPDIR, "rcsb_db", "data")
        self.__pdbxFileList = ['1cbs.cif', '1o3q.cif', '1xbb.cif', '3of4.cif', '3oqp.cif', '3rer.cif', '3rij.cif', '5hoh.cif']
        self.__tableIdSkipD = {'ATOM_SITE': True, 'ATOM_SITE_ANISOTROP': True}
        self.__ioObj = IoAdapter(verbose=self.__verbose)
        #
        self.__pdbxFileCache = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_PDBX_SANDBOX")
        self.__pdbxSavedPathList = os.path.join(HERE, "test-output", "PDBXPATHLIST.txt")
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

    def open(self, dbName, dbUser, dbHost, dbPort):
        self.close()
        try:
            myC = CockroachDbConnect(dbName=dbName, dbUser=dbUser, dbHost=dbHost, dbPort=dbPort)
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
            ok = self.open(self.__dbName, self.__dbUser, self.__dbHost, self.__dbPort)
            self.close()
            self.assertEqual(ok, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSchemaRemove(self):
        """  Remove table schema (live) for PDBx data.
        """
        try:
            sd = PdbxSchemaDef(convertNames=True)
            ret = self.__schemaRemove(schemaDefObj=sd)
            self.assertEqual(ret, True)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __readPdbxPathList(self, fp):
        pathList = []
        try:
            with open(fp, 'r') as ifh:
                for line in ifh:
                    pathList.append(line[:-1])
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()
        #
        logger.info("\nFound %d files in %s\n" % (len(pathList), fp))
        return pathList

    def __makePdbxPathList(self, fp, cachePath=os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_PDBX_SANDBOX")):
        """ Return the list of pdbx file paths in the current repository.
        """

        try:
            with open(fp, 'w') as ofh:
                for root, dirs, files in scandir.walk(cachePath, topdown=False):
                    if "REMOVE" in root:
                        continue
                    for name in files:
                        if name.endswith(".cif") and len(name) == 8:
                            ofh.write("%s\n" % os.path.join(root, name))
                #
                # logger.info("\nFound %d files in %s\n" % (len(pathList), cachePath))
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def __makePdbxPathListInLine(self, cachePath):
        """ Return the list of pdbx file paths in the current repository.
        """
        pathList = []
        try:
            for root, dirs, files in scandir.walk(cachePath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) == 8:
                        pathList.append(os.path.join(root, name))

            logger.info("\nFound %d files in %s\n" % (len(pathList), cachePath))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            # self.fail()
        return pathList

    def testLoadPdbxAllMulti(self):
        pathList = self.__makePdbxPathList(self.__pdbxFileCache)
        if os.access(self.__pdbxSavedPathList, os.R_OK):
            pathList = self.__readPdbxPathList(fp=self.__pdbxSavedPathList)
        else:
            ok = self.__makePdbxPathList(self.__pdbxSavedPathList, cachePath=self.__pdbxFileCache)
            self.assertEqual(ok, True)
            pathList = self.__readPdbxPathList(fp=self.__pdbxSavedPathList)
        #self.assertGreater(len(pathList), 140000)
        sd = PdbxSchemaDef(convertNames=True)
        self.__testLoadFilesMulti(sd, pathList, fType="pdbx")

    def testLoadPdbxMulti(self):
        pathList = self.__getPathList(fType='pdbx')
        sd = PdbxSchemaDef(convertNames=True)
        self.__testLoadFilesMulti(sd, pathList, fType="pdbx")

    def loadInsertMany(self, dataList, procName, optionsD, workingDir):

        try:
            ioObj = IoAdapter(verbose=self.__verbose)
            myC = CockroachDbConnect(dbHost=self.__dbHost, dbPort=self.__dbPort)
            dbCon = myC.connect()
            sd = optionsD['sd']
            skipD = optionsD['skip']
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=ioObj, dbCon=dbCon, workPath='.', cleanUp=False, warnings='default', verbose=self.__verbose)
            ret = sdl.load(inputPathList=dataList, loadType='cockroach-insert-many', deleteOpt='selected', tableIdSkipD=skipD)
            myC.close()
            # all or nothing here
            if ret:
                return dataList, dataList, []
            else:
                return [], [], []
        except Exception as e:
            logger.info("Failing with dataList %r" % dataList)
            logger.exception("Failing with %s" % str(e))

        return [], [], []

    def __testLoadFilesMulti(self, sd, pathList, fType):
        """Test case - create load w/insert-many all chemical component definition data files - (multiproc test)
        """
        numProc = self.__numProc
        chunkSize = self.__chunkSize if self.__chunkSize < len(pathList) else 0
        try:
            #
            optD = {}
            optD['sd'] = sd
            if fType == 'pdbx':
                optD['skip'] = self.__tableIdSkipD
            else:
                optD['skip'] = {}
            # ret = self.__schemaCreate(schemaDefObj=optD['sd'])
            # self.assertEqual(ret, True)
            #
            mpu = MultiProcUtil(verbose=True)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="loadInsertMany")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=pathList, numProc=numProc, numResults=1, chunkSize=chunkSize)
            self.assertEqual(ok, True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getPathList(self, fType):
        pathList = []
        if fType == "pdbx":
            pathList = self.__getPdbxPathList()

        return pathList

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

    def __schemaCreate(self, schemaDefObj):
        """Test case -  create table schema using schema definition
        """
        ret = 0
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = SqlGenAdmin(self.__verbose, serverType="cockroachdb")
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getVersionedDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("\nSchema creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            myC = CockroachDbConnect(dbHost=self.__dbHost, dbPort=self.__dbPort)
            dbCon = myC.connect()
            crQ = CockroachDbQuery(dbcon=dbCon, verbose=self.__verbose)
            ret = crQ.sqlCommandList(sqlCommandList=sqlL)
            myC.close()
            #
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
            sqlGen = SqlGenAdmin(self.__verbose, serverType="cockroachdb")
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.dropTableSQL(databaseName=schemaDefObj.getVersionedDatabaseName(), tableDefObj=tableDefObj))
                sqlL.extend(sqlGen.dropTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            logger.debug("Schema Remove SQL string\n %s" % '\n'.join(sqlL))
            myC = CockroachDbConnect(dbHost=self.__dbHost, dbPort=self.__dbPort)
            dbCon = myC.connect()
            crQ = CockroachDbQuery(dbcon=dbCon, verbose=self.__verbose)
            ret = crQ.sqlCommandList(sqlCommandList=sqlL)
            myC.close()
            logger.debug("Schema remove command returns %r\n" % ret)
            return ret
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def loadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderCockroachDbMultiTests("testConnection"))
    # suiteSelect.addTest(SchemaDefLoaderCockroachDbMultiTests("testSchemaRemove"))
    # suiteSelect.addTest(SchemaDefLoaderCockroachDbMultiTests("testLoadPdbxMulti"))
    suiteSelect.addTest(SchemaDefLoaderCockroachDbMultiTests("testLoadPdbxAllMulti"))
    return suiteSelect


if __name__ == '__main__':
    mySuite = loadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
