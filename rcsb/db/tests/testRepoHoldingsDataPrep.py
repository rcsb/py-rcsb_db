# File:    RepoHoldingsDataPrepTests.py
# Author:  J. Westbrook
# Date:    11-Jul-2018
# Version: 0.001
#
# Update:
#
#
#
##
"""
Tests for processing legacy repository holdings and status records.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.db.processors.RepoHoldingsDataPrep import RepoHoldingsDataPrep
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RepoHoldingsDataPrepTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__updateId = "2018_25"
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__sandboxPath = self.__cfgOb.getPath("RCSB_EXCHANGE_SANDBOX_PATH", sectionName=configName)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testProcessLegacyFiles(self):
        """ Test loading and processing operations for legacy holdings and status echange data.
        """
        try:
            rhdp = RepoHoldingsDataPrep(sandboxPath=self.__sandboxPath, cachePath=self.__cachePath)
            rL = rhdp.getHoldingsUpdate(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL), 10)
            logger.debug("update data length %r", len(rL))
            #
            rL = rhdp.getHoldingsCurrent(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL), 10)
            logger.debug("holdings data length %r", len(rL))
            #
            rL = rhdp.getHoldingsUnreleased(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL), 10)
            logger.debug("unreleased data length %r", len(rL))
            #
            rL = rhdp.getHoldingsPrerelease(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL), 10)
            logger.debug("prerelease data length %r", len(rL))
            #
            rL1, rL2 = rhdp.getHoldingsTransferred(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL1), 10)
            logger.debug("transferred data length %r", len(rL1))
            self.assertGreaterEqual(len(rL2), 10)
            logger.debug("Insilico data length %r", len(rL2))

            rL1, rL2, rL3 = rhdp.getHoldingsRemoved(updateId=self.__updateId)
            self.assertGreaterEqual(len(rL1), 10)
            logger.debug("removed data length %r", len(rL1))

            self.assertGreaterEqual(len(rL2), 10)
            logger.debug("removed author data length %r", len(rL2))

            self.assertGreaterEqual(len(rL3), 10)
            logger.debug("removed data length %r", len(rL3))

            #
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def repoHoldingsSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepoHoldingsDataPrepTests("testProcessLegacyFiles"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = repoHoldingsSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
