##
# File:    PdbxLoaderTests.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#   22-Mar-2018 jdw  Revise all tests
#   23-Mar-2018 jdw  Add reload test cases
#   27-Mar-2018 jdw  Update configuration handling and mocking
#    4-Apr-2018 jdw  Add size pruning tests
#   25-Jul-2018 jdw  Add large test case to test failure and salvage scenarios
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


import logging
import os
import time
import unittest

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.mongo.PdbxLoader import PdbxLoader
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PdbxLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(PdbxLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName, mockTopPath=mockTopPath)
        # self.__cfgOb.dump()
        self.__resourceName = "MONGO_DB"
        self.__failedFilePath = os.path.join(HERE, 'test-output', 'failed-list.txt')
        self.__workPath = os.path.join(HERE, 'test-output')
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__fileLimit = 12
        self.__documentStyle = 'rowwise_by_name_with_cardinality'
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testLoadChemCompReference(self):
        """ Test case -  Load chemical component reference data
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('chem_comp', loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdChemCompReference(self):
        """ Test case -  Load Bird chemical component reference data
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('bird_chem_comp', loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdReference(self):
        """ Test case -  Load Bird reference data
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('bird', loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdFamilyReference(self):
        """ Test case -  Load Bird family reference data
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('bird_family', loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["BIRD_FAMILY_PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertFalse(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testReLoadChemCompReference(self):
        """ Test case -  Load and reload chemical component reference data
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('chem_comp', loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertTrue(ok)
            ok = mw.load('chem_comp', loadType='replace', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadPdbxEntryData(self):
        """ Test case -  Load PDBx entry data
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('pdbx', loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertFalse(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testReLoadPdbxEntryData(self):
        """ Test case -  Load PDBx entry data with pdbx schema
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('pdbx', loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertFalse(ok)
            ok = mw.load('pdbx', loadType='replace', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath, pruneDocumentSize=14.0)
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadPdbxEntryDataSizeLimit(self):
        """ Test case -  Load PDBx entry data with pdbx schema (testing document size handling)
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('pdbx', loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath, pruneDocumentSize=14.0)
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testReLoadPdbxEntryDataSliced(self):
        """ Test case -  Load PDBx entry data with pdbx_core schema
        """
        try:
            mw = PdbxLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck, workPath=self.__workPath)
            ok = mw.load('pdbx_core', collectionLoadList=['pdbx_core_entity_v5_0_2'], loadType='full', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath)
            self.assertTrue(ok)
            ok = mw.load('pdbx_core', collectionLoadList=['pdbx_core_entry_v5_0_2'], loadType='replace', inputPathList=None, styleType=self.__documentStyle,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=self.__failedFilePath, pruneDocumentSize=14.0)
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __loadStatus(self, statusList):
        sectionName = 'data_exchange_status'
        dl = DocumentLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                            documentLimit=None, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
        #
        databaseName = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionName) + '_' + self.__cfgOb.get('DATABASE_VERSION_STRING', sectionName=sectionName)
        collectionVersion = self.__cfgOb.get('COLLECTION_VERSION_STRING', sectionName=sectionName)
        collectionName = self.__cfgOb.get('COLLECTION_UPDATE_STATUS', sectionName=sectionName) + '_' + collectionVersion
        ok = dl.load(databaseName, collectionName, loadType='append', documentList=statusList,
                     indexAttributeList=['update_id', 'database_name', 'object_name'], keyNames=None)
        return ok


def mongoLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxLoaderTests("testLoadChemCompReference"))
    suiteSelect.addTest(PdbxLoaderTests("testLoadBirdChemCompReference"))
    suiteSelect.addTest(PdbxLoaderTests("testLoadBirdReference"))
    suiteSelect.addTest(PdbxLoaderTests("testLoadBirdFamilyReference"))
    return suiteSelect


def mongoLoadPdbxSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxLoaderTests("testLoadPdbxEntryData"))
    return suiteSelect


def mongoLoadPdbxLimitSizeSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxLoaderTests("testLoadPdbxEntryDataSizeLimit"))
    return suiteSelect


def mongoReLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxLoaderTests("testReLoadChemCompReference"))
    suiteSelect.addTest(PdbxLoaderTests("testReLoadPdbxEntryData"))
    return suiteSelect


def mongoSlicedSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxLoaderTests("testReLoadPdbxEntryDataSliced"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = mongoLoadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if (True):
        mySuite = mongoReLoadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if (True):
        mySuite = mongoLoadPdbxLimitSizeSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if (True):
        mySuite = mongoSlicedSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
        #
    if (True):
        mySuite = mongoLoadPdbxSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
