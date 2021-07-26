# File:    DictMethodResourceProviderFixture.py
# Author:  J. Westbrook
# Date:    12-Aug-2019
# Version: 0.001
#
# Update:

##
"""
Fixture for setting up cached resources for dictionary method helpers

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest
from numpy import True_

from rcsb.utils.dictionary.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DictMethodResourceProviderFixture(unittest.TestCase):
    def setUp(self):
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__configName = configName
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)

        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def qtestBuildResourceCache(self):
        """Fixture - generate and check resource caches"""
        try:
            rp = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, cachePath=self.__cachePath, siftsAbbreviated="TEST")
            # ret = rp.cacheResources(useCache=False)
            ret = rp.cacheResources(useCache=True_)
            self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testRecoverResourceCache(self):
        """Fixture - generate and check resource caches

        TOTAL - Maximum resident memory size 7902.0442 MB
        SIFTS(2.9499 secs/1115.558 MB)
        """
        try:
            rp = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, cachePath=self.__cachePath)
            ret = rp.cacheResources(useCache=True)
            self.assertTrue(ret)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def dictMethodResourceProviderSuite():
    suiteSelect = unittest.TestSuite()
    # suiteSelect.addTest(DictMethodResourceProviderFixture("testBuildResourceCache"))
    suiteSelect.addTest(DictMethodResourceProviderFixture("testRecoverResourceCache"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = dictMethodResourceProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
