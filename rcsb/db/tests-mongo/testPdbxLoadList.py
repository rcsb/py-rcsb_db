##
# File:    PdbxLoadListTests.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#
##
"""
Tests for creating and loading MongoDb using BIRD, CCD and PDBx/mmCIF data files
and following external schema definitions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import glob
import logging
import os
import time
import unittest

from rcsb.db.mongo.PdbxLoader import PdbxLoader
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PdbxLoadListTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(PdbxLoadListTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        # self.__cfgOb.dump()
        self.__resourceName = "MONGO_DB"
        self.__failedEntityFilePath = os.path.join(HERE, "test-output", "failed-entity-list.txt")
        self.__failedEntryFilePath = os.path.join(HERE, "test-output", "failed-entry-list.txt")
        self.__failedCcFilePath = os.path.join(HERE, "test-output", "failed-cc-list.txt")
        self.__workPath = os.path.join(HERE, "test-output")
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__fileLimit = None
        self.__documentStyle = "rowwise_by_name_with_cardinality"
        #
        # self.__testDirPath = os.path.join(HERE, "test-output", 'pdbx-fails')
        self.__testDirPath = os.path.join(TOPDIR, "rcsb", "db", "tests-mongo", "test-output", "pdbx-fails")
        self.__testChemCompDirPath = os.path.join(TOPDIR, "rcsb", "db", "tests", "test-output", "cc-fails")
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def specialTestLoadChemCompCoreReference(self):
        """ Test case -  Load chemical component core reference data
        """
        try:
            inputPathList = glob.glob(self.__testChemCompDirPath + "/*.cif")
            logger.info("Found %d files in test path %s", len(inputPathList), self.__testDirPath)
            mw = PdbxLoader(
                self.__cfgOb,
                resourceName=self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                fileLimit=self.__fileLimit,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
                workPath=self.__workPath,
            )
            ok = mw.load(
                "chem_comp_core",
                loadType="full",
                inputPathList=inputPathList,
                styleType=self.__documentStyle,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=self.__failedCcFilePath,
            )
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def specialTestLoadPdbxCoreEntry(self):
        """ Test case -  Load PDBx core entry data with pdbx_core_entry schema
        """
        try:
            inputPathList = glob.glob(self.__testDirPath + "/*.cif")
            logger.info("Found %d files in test path %s", len(inputPathList), self.__testDirPath)
            mw = PdbxLoader(
                self.__cfgOb,
                resourceName=self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                fileLimit=self.__fileLimit,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
                workPath=self.__workPath,
            )
            ok = mw.load(
                "pdbx_core",
                collectionLoadList=["pdbx_core_entry"],
                loadType="full",
                inputPathList=inputPathList,
                styleType=self.__documentStyle,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=self.__failedEntryFilePath,
                pruneDocumentSize=14.0,
            )
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def specialTestLoadPdbxCore(self):
        """ Test case -  Load PDBx core collections with merging
        """
        try:
            inputPathList = glob.glob(self.__testDirPath + "/*.cif")
            logger.info("Found %d files in test path %s", len(inputPathList), self.__testDirPath)
            mw = PdbxLoader(
                self.__cfgOb,
                resourceName=self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                fileLimit=self.__fileLimit,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
                workPath=self.__workPath,
            )
            ok = mw.load(
                "pdbx_core",
                loadType="full",
                inputPathList=inputPathList,
                styleType=self.__documentStyle,
                dataSelectors=["PUBLIC_RELEASE"],
                collectionLoadList=["pdbx_core_entity"],
                failedFilePath=self.__failedEntityFilePath,
                mergeContentTypes=["vrpt"],
                logSize=True,
                pruneDocumentSize=15.8,
            )
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def mongoLoadPdbxList():
    suiteSelect = unittest.TestSuite()
    # suiteSelect.addTest(PdbxLoadListTests("specialTestLoadPdbxCoreEntry"))
    suiteSelect.addTest(PdbxLoadListTests("specialTestLoadPdbxCore"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = mongoLoadPdbxList()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
