##
# File:    testDataTypeApiProvider.py
# Author:  J. Westbrook
# Date:    23-Aug-2019
# Version: 0.001
#
# Update:

##
"""
Tests for data type API provider.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.db.define.DataTypeApiProvider import DataTypeApiProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DataTypeApiProviderTests(unittest.TestCase):
    def setUp(self):
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")

        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__configName = configName
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)

        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testApplicationProvider(self):
        """Test case - get application data type API
        """
        try:
            dta = DataTypeApiProvider(self.__cfgOb, self.__cachePath, useCache=False)
            for appName in ["ANY", "SQL", "JSON", "BSON"]:
                dtApi = dta.getDataTypeApplicationApi(appName)
                ok = dtApi.testCache()
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCoverageProvider(self):
        """Test case - get application data type API
        """
        try:
            dta = DataTypeApiProvider(self.__cfgOb, self.__cachePath, useCache=False)
            for schemaName in ["pdbx_core", "chem_comp_core", "bird_chem_comp_core"]:
                dtApi = dta.getDataTypeInstanceApi(schemaName)
                ok = dtApi.testCache()
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def dataTypeApiProviderSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DataTypeApiProviderTests("testApplicationProvider"))
    suiteSelect.addTest(DataTypeApiProviderTests("testCoverageProvider"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = dataTypeApiProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
