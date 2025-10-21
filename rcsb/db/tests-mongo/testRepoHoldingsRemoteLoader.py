##
# File:    testRepoHoldingsRemoteLoader.py
# Author:  J. Westbrook
# Date:    13-Jul-2018
# Version: 0.001
#
# Updates:
# 14-Jul-2018 jdw add configuration options
#  7-Oct-2018 jdw add schema validation to the underlying load processing
# 21-Sep-2021 jdw overhaul using new resource files and with support for remote access
# 21-Oct-2025 dwp make use of RepoHoldingsEtlWorker instead of re-writing the code here
#
##
"""
Tests for loading repository holdings using remote resource information.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.cli.RepoHoldingsEtlWorker import RepoHoldingsEtlWorker
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.struct.EntryInfoProvider import EntryInfoProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RepoHoldingsRemoteLoaderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(RepoHoldingsRemoteLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)

        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__documentLimit = 10
        #
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__sandboxPath = self.__cfgOb.getPath("RCSB_EXCHANGE_SANDBOX_PATH", sectionName=configName)
        # sample data set
        self.__updateId = "2021_36"
        #
        eiP = EntryInfoProvider(cachePath=self.__cachePath, useCache=True)
        ok = eiP.testCache(minCount=0)
        self.assertTrue(ok)
        ok = eiP.restore(self.__cfgOb, configName, useStash=False, useGit=True)
        self.assertTrue(ok)
        ok = eiP.reload()
        self.assertTrue(ok)

        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testLoadHoldingsRemote(self):
        """Test case - load legacy repository holdings and status data -
        """
        try:
            rhw = RepoHoldingsEtlWorker(
                self.__cfgOb,
                self.__sandboxPath,
                self.__cachePath,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                documentLimit=self.__documentLimit,
                readBackCheck=self.__readBackCheck,
            )
            # First load PDB holdings (with loadType="full")
            ok = rhw.loadRepoType(self.__updateId, loadType="full", repoType="pdb")
            logger.info("RepoHoldingsEtlWorker repoType 'pdb' loaded with status %r", ok)
            self.assertTrue(ok)
            #
            # Next load IHM holdings (with loadType="replace")
            ok = rhw.loadRepoType(self.__updateId, loadType="replace", repoType="pdb_ihm")
            logger.info("RepoHoldingsEtlWorker repoType 'pdb_ihm' loaded with status %r", ok)
            self.assertTrue(ok)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def holdingsLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepoHoldingsRemoteLoaderTests("testLoadHoldingsRemote"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = holdingsLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
