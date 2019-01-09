##
# File:    ScanRepoUtilTests.py
# Author:  J. Westbrook
# Date:    1-May-2018
# Version: 0.001
#
# Updates:
#  9-May-2018 jdw add tests for incremental scanning.
# 23-May-2018 jdw simplify dependencies - Get input paths internally in ScanRepoUtil()
# 16-Jun-2018 jdw update DictInfo() prototype
# 18-Jun-2018 jdw update ScanRepoUtil prototype
# 28-Jun-2018 jdw update ScanRepoUtil prototype with workPath
#  2-Jul-2018 jdw remove dependency on mmcif_utils.
# 13-Dec-2018 jdw add IHM support
##
"""
Tests for scanning BIRD, CCD and PDBx/mmCIF data files for essential
data type and coverage features.  Generate data sets used to construct schema maps.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.define.DictInfo import DictInfo
from rcsb.db.utils.ScanRepoUtil import ScanRepoUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ScanRepoUtilTests(unittest.TestCase):

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.yml')
        configName = 'site_info'

        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__pathPdbxDictionaryFile = self.__cfgOb.getPath('PDBX_DICT_LOCATOR', sectionName=configName)
        self.__pathIhmDictionaryFile = self.__cfgOb.getPath('IHMDEV_DICT_LOCATOR', sectionName=configName)
        self.__pathFlrDictionaryFile = self.__cfgOb.getPath('FLR_DICT_LOCATOR', sectionName=configName)
        #
        self.__failedFilePath = os.path.join(HERE, 'test-output', 'failed-list.txt')
        self.__savedFilePath = os.path.join(HERE, 'test-output', 'path-list.txt')
        self.__scanDataFilePath = os.path.join(HERE, 'test-output', 'scan-data.pic')
        self.__workPath = os.path.join(HERE, 'test-output')

        self.__numProc = 2
        self.__chunkSize = 20
        self.__fileLimit = 20
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testScanIhmDevRepo(self):
        """ Test case - scan chem comp reference data
        """
        ok = self.__testScanRepo(contentType='ihm_dev')
        self.assertTrue(ok)

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
            dI = DictInfo(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathIhmDictionaryFile, self.__pathFlrDictionaryFile])
            attributeDataTypeD = dI.getAttributeDataTypeD()
            #
            sr = ScanRepoUtil(
                self.__cfgOb,
                attributeDataTypeD=attributeDataTypeD,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                fileLimit=self.__fileLimit,
                workPath=self.__workPath)
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
    suiteSelect.addTest(ScanRepoUtilTests("testScanIhmDevRepo"))
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
