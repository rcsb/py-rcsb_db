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
import scandir

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

from rcsb_db.mysql.MyDbUtil import MyDbConnect
from rcsb_db.loaders.SchemaDefLoader import SchemaDefLoader
from rcsb_db.schema.ChemCompSchemaDef import ChemCompSchemaDef

from rcsb_db.utils.MultiProcUtil import MultiProcUtil
try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter


class ChemCompLoaderTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = False
        self.__loadPathList = []
        self.__ioObj = IoAdapter(verbose=self.__verbose)
        self.__topCachePath = '../../../../../../reference/components/ligand-dict-v3'
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

    def open(self, dbName=None, dbUserId=None, dbUserPwd=None):
        myC = MyDbConnect(dbName=dbName, dbUser=dbUserId, dbPw=dbUserPwd, verbose=self.__verbose)
        self.__dbCon = myC.connect()
        if self.__dbCon is not None:
            return True
        else:
            return False

    def close(self):
        if self.__dbCon is not None:
            self.__dbCon.close()

    def __makeComponentPathList(self):
        """ Return the list of chemical component definition file paths in the current repository.
        """

        pathList = []
        for root, dirs, files in scandir.walk(self.__topCachePath, topdown=False):
            if "REMOVE" in root:
                continue
            for name in files:
                if name.endswith(".cif") and len(name) <= 7:
                    pathList.append(os.path.join(root, name))
        logger.info("\nFound %d files in %s\n" % (len(pathList), self.__topCachePath))
        return pathList

    def testListFiles(self):
        """Test case - for loading chemical component definition data files -
        """

        try:
            pathList = self.__makeComponentPathList()
            logger.info("\nFound %d files\n" % len(pathList))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testConnect(self):
        """Test case - for creating a test connection
        """

        try:
            ccsd = ChemCompSchemaDef()
            self.open(dbName=ccsd.getDatabaseName())
            self.close()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadFiles(self):
        """Test case - create batch load files for all chemical component definition data files -
        """

        try:
            ccsd = ChemCompSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=None, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose)
            pathList = self.__makeComponentPathList()

            containerNameList, tList = sml.makeLoadFiles(pathList, append=False)
            for tId, fn in tList:
                logger.info("\nCreated table %s load file %s\n" % (tId, fn))
            #

            self.open(dbName=ccsd.getDatabaseName())
            sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose)

            sdl.loadBatchFiles(loadList=tList, containerNameList=containerNameList, deleteOpt='all')
            self.close()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def loadBatchFilesMulti(self, dataList, procName, optionsD, workingDir):
        ccsd = ChemCompSchemaDef()
        self.open(dbName=ccsd.getDatabaseName())
        sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False,
                              warnings='default', verbose=self.__verbose)
        #
        sdl.loadBatchFiles(loadList=dataList, containerNameList=None, deleteOpt=None)
        self.close()
        return dataList, dataList, []

    def makeComponentPathListMulti(self, dataList, procName, optionsD, workingDir):
        """ Return the list of chemical component definition file paths in the current repository.
        """
        pathList = []
        for subdir in dataList:
            dd = os.path.join(self.__topCachePath, subdir)
            for root, dirs, files in scandir.walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) <= 7:
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def testLoadFilesMulti(self):
        """Test case - create batch load files for all chemical component definition data files - (multiproc test)
        """
        logger.debug("\nStarting %s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name))
        startTime = time.time()
        numProc = 4
        try:
            dataS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
            dataList = [a for a in dataS]
            mpu = MultiProcUtil(verbose=True)
            mpu.set(workerObj=self, workerMethod="makeComponentPathListMulti")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.info("\nPath list length %d  in %.2f seconds\n" % (len(pathList), endTime0 - startTime))

            # logger.info("\nPath list %r\n" % pathList[:20])
            # pathList=self.__makeComponentPathList()

            ccsd = ChemCompSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=None, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose)

            #
            mpu = MultiProcUtil(verbose=True)
            mpu.set(workerObj=sml, workerMethod="makeLoadFilesMulti")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=pathList, numProc=numProc, numResults=2)
            #
            containerNameList = retLists[0]
            tList = retLists[1]

            for tId, fn in tList:
                logger.info("\nCreated table %s load file %s\n" % (tId, fn))
            #

            endTime1 = time.time()
            logger.info("\nBatch files created in %.2f seconds\n" % (endTime1 - endTime0))
            self.open(dbName=ccsd.getDatabaseName())
            sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose)
            #
            for tId, fn in tList:
                sdl.delete(tId, containerNameList=containerNameList, deleteOpt='all')
            self.close()
            #
            mpu = MultiProcUtil(verbose=True)
            mpu.set(workerObj=self, workerMethod="loadBatchFilesMulti")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=tList, numProc=numProc, numResults=1)

            endTime2 = time.time()
            logger.info("\nLoad completed in %.2f seconds\n" % (endTime2 - endTime1))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def loadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemCompLoaderTests("testConnect"))
    # suiteSelect.addTest(ChemCompLoaderTests("testListFiles"))
    # suiteSelect.addTest(ChemCompLoaderTests("testLoadFiles"))
    suiteSelect.addTest(ChemCompLoaderTests("testLoadFilesMulti"))
    return suiteSelect

if __name__ == '__main__':
    #
    mySuite = loadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
