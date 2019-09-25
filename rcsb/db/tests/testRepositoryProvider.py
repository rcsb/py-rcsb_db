##
# File:    testRepositoryProvider.py
# Author:  J. Westbrook
# Date:    19-Aug-2019
# Version: 0.001
#
# Updates:

##
"""
Tests repository path and object utilities.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.utils.RepositoryProvider import RepositoryProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ScanRepoUtilTests(unittest.TestCase):
    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        self.__configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=mockTopPath)
        self.__cachePath = os.path.join(TOPDIR, "CACHE")

        self.__numProc = 2
        self.__chunkSize = 20
        self.__fileLimit = 20
        #
        self.__rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, cachePath=self.__cachePath)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testRepoUtils(self):
        """ Test case - repository locator path utilities
        """
        for contentType in ["bird_chem_comp_core", "pdbx_core", "ihm_dev"]:
            mergeContentTypes = None
            if contentType in ["pdbx_core"]:
                mergeContentTypes = ["vrpt"]
            #
            locatorObjList = self.__rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            pathList = self.__rpP.getLocatorPaths(locatorObjList)
            locatorObjList2 = self.__rpP.getLocatorsFromPaths(locatorObjList, pathList)
            logger.debug("pathList %r", pathList)
            self.assertEqual(len(locatorObjList), len(pathList))
            self.assertEqual(len(locatorObjList), len(locatorObjList2))


def repoSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ScanRepoUtilTests("testRepoUtils"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = repoSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
