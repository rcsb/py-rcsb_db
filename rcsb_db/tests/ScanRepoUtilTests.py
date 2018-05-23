##
# File:    ScanRepoUtilTests.py
# Author:  J. Westbrook
# Date:    1-May-2018
# Version: 0.001
#
# Updates:
#  9-May-2018 jdw add tests for incremental scanning.
# 23-May-2018 jdw simplify dependencies - Get input paths internally in ScanRepoUtil()
##
"""
Tests for scanning BIRD, CCD and PDBx/mmCIF data files for essential
data type and coverage features.  Generate data sets used to construct schema maps.
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

from rcsb_db.utils.ScanRepoUtil import ScanRepoUtil
from rcsb_db.utils.ConfigUtil import ConfigUtil
from rcsb_db.schema.SchemaDefDictInfo import SchemaDefDictInfo


class ScanRepoUtilTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(ScanRepoUtilTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__mockTopPath = os.path.join(TOPDIR, 'rcsb_db', 'data')
        configPath = os.path.join(TOPDIR, 'rcsb_db', 'data', 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb_db', 'data', 'mmcif_pdbx_v5_next.dic')
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        #
        self.__failedFilePath = os.path.join(HERE, 'test-output', 'failed-list.txt')
        self.__savedFilePath = os.path.join(HERE, 'test-output', 'path-list.txt')
        self.__scanDataFilePath = os.path.join(HERE, 'test-output', 'scan-data.pic')

        self.__numProc = 2
        self.__chunkSize = 10
        self.__fileLimit = 10
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

    def testScanChemCompRepo(self):
        """ Test case - scan chem comp reference data
        """
        ok = self.__testScanRepo(contentType='chem_comp')
        self.assertTrue(ok)

    def testScanBirdRepo(self):
        """ Test case - scan BIRD reference data
        """
        ok = self.__testScanRepo(contentType='bird')
        self.assertTrue(ok)

    def testScanBirdFamilyRepo(self):
        """ Test case - scan BIRD Family reference data
        """
        ok = self.__testScanRepo(contentType='bird_family')
        self.assertTrue(ok)

    def testScanPdbxRepo(self):
        """ Test case - scan PDBx structure model data
        """
        ok = self.__testScanRepo(contentType='pdbx')
        self.assertTrue(ok)

    def testScanPdbxRepoIncr(self):
        """ Test case - scan PDBx structure model data
        """
        ok = self.__testScanRepo(contentType='pdbx', scanType='incr')
        self.assertTrue(ok)

    def __testScanRepo(self, contentType, scanType='full'):
        """ Utility method to scan repo for data type and coverage content.

            Using mock repos for tests.
        """
        try:
            failedFilePath = os.path.join(HERE, 'test-output', '%s-failed-list.txt' % contentType)
            savedFilePath = os.path.join(HERE, 'test-output', '%s-path-list.txt' % contentType)
            #
            scanDataFilePath = os.path.join(HERE, 'test-output', '%s-scan-data.pic' % contentType)
            #
            dataCoverageFilePath = os.path.join(HERE, 'test-output', '%s-scan-data-coverage.json' % contentType)
            dataTypeFilePath = os.path.join(HERE, 'test-output', '%s-scan-data-type.json' % contentType)
            #
            sdi = SchemaDefDictInfo(dictPath=self.__pathPdbxDictionaryFile)
            dataTypeD = sdi.getDataTypeD()
            #
            sr = ScanRepoUtil(
                self.__cfgOb,
                dataTypeD=dataTypeD,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                fileLimit=self.__fileLimit,
                mockTopPath=self.__mockTopPath)
            ok = sr.scanContentType(
                contentType,
                scanType=scanType,
                inputPathList=None,
                scanDataFilePath=scanDataFilePath,
                failedFilePath=failedFilePath,
                saveInputFileListPath=savedFilePath)
            self.assertTrue(ok)
            ok = sr.evalScan(scanDataFilePath, dataTypeFilePath, evalType='data_type')
            self.assertTrue(ok)
            ok = sr.evalScan(scanDataFilePath, dataCoverageFilePath, evalType='data_coverage')
            self.assertTrue(ok)

            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def scanSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ScanRepoUtilTests("testScanChemCompRepo"))
    suiteSelect.addTest(ScanRepoUtilTests("testScanBirdRepo"))
    suiteSelect.addTest(ScanRepoUtilTests("testScanBirdFamilyRepo"))
    suiteSelect.addTest(ScanRepoUtilTests("testScanPdbxRepo"))
    suiteSelect.addTest(ScanRepoUtilTests("testScanPdbxRepoIncr"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = scanSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
