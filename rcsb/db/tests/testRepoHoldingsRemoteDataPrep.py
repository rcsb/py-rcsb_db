# File:    testRepoHoldingsRemoteDataPrep.py
# Author:  D. Piehl
# Date:    04-Feb-2022
# Version: 0.001
#
# Update:
#
#
#
##
"""
Tests for processing remote repository holdings and status records.

"""

__docformat__ = "restructuredtext en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.db.processors.RepoHoldingsRemoteDataPrep import RepoHoldingsRemoteDataPrep
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RepoHoldingsRemoteDataPrepTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__updateId = "2019_25"
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__sandboxPath = self.__cfgOb.getPath("RCSB_EXCHANGE_SANDBOX_PATH", sectionName=configName)
        #
        self.__startTime = time.monotonic()
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        logger.debug("Completed %s in %.3f s", self.id(), time.monotonic() - self.__startTime)

    def testProcessLegacyFiles(self):
        """Test loading and processing operations for repository holdings and status echange data."""
        try:
            # rhrdp = RepoHoldingsRemoteDataPrep(cachePath=self.__cachePath, useCache=True, cfgOb=self.__cfgOb)
            rhrdp = RepoHoldingsRemoteDataPrep(cachePath=self.__cachePath, useCache=True)
            rL = rhrdp.getHoldingsUpdateEntry(updateId=self.__updateId)
            logger.info("update data length %r", len(rL))
            self.assertGreaterEqual(len(rL), 10)
            #
            rL = rhrdp.getHoldingsCurrentEntry(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL), 10)
            logger.info("holdings data length %r", len(rL))
            #
            rL = rhrdp.getHoldingsUnreleasedEntry(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL), 10)
            logger.info("unreleased data length %r", len(rL))
            #
            rL = rhrdp.getHoldingsRemovedEntry(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL), 10)
            logger.info("removed data length %r", len(rL))
            #
            rL = rhrdp.getHoldingsCombinedEntry(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL), 10)
            logger.info("combined data length %r", len(rL))
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def repoHoldingsSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepoHoldingsRemoteDataPrepTests("testProcessLegacyFiles"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = repoHoldingsSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
