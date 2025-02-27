# File:    DataTypeApplicationInfoTests.py
# Author:  J. Westbrook
# Date:    22-May-2013
# Version: 0.001
#
# Update:
#  5-Jun-2018  jdw update prototypes for IoUtil() methods
# 12-Oct-2018  jdw add tests of store type mapping
#  7-Jan-2019  jdw update argument naming conventions
#  6-Feb-2019  jdw replace IoUtil() with MarshalUtil()
#
#
#
##
"""
Tests for managing access to application data type mapping information.

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
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DataTypeApplicationInfoTests(unittest.TestCase):
    def setUp(self):
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")

        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__configName = configName
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__mU = MarshalUtil()
        #
        self.__pathSaveTypeMap = os.path.join(HERE, "test-output", "app_data_type_mapping.cif")
        self.__pathSaveTypeMapJson = os.path.join(HERE, "test-output", "app_data_type_mapping.json")

        # self.__pathDataTypeMap = os.path.join(self.__mockTopPath, "data_type_info", "app_data_type_mapping.cif")
        self.__startTime = time.monotonic()
        logger.debug("Starting %s now", self.id())

    def tearDown(self):
        logger.debug("Completed %s in %.3f s", self.id(), time.monotonic() - self.__startTime)

    def testDefaults(self):
        """Verify default type assignments and read, write and update operations."""
        try:
            dta = DataTypeApiProvider(self.__cfgOb, self.__cachePath, useCache=False)
            dtInfo = dta.getDataTypeApplicationApi("ANY")
            mapD = dtInfo.getDefaultDataTypeMap()
            logger.debug("Default type map length %d", len(mapD))
            ok = self.__mU.doExport(self.__pathSaveTypeMapJson, mapD, fmt="json", indent=3)
            self.assertTrue(ok)
            ok = dtInfo.writeDefaultDataTypeMap(self.__pathSaveTypeMap, dataTyping="ANY")
            #
            rMapD = dtInfo.readDefaultDataTypeMap(self.__pathSaveTypeMap, dataTyping="ANY")
            self.assertEqual(len(mapD), len(rMapD))
            # Note treating all data as strings to facilitate differencing.
            rMapD["new_type"] = {"application_name": "ANY", "app_type_code": "app_new_type", "app_precision_default": "0", "app_width_default": "80", "type_code": "new_type"}
            #
            ok = dtInfo.updateDefaultDataTypeMap(self.__pathSaveTypeMap, rMapD, dataTyping="ANY")
            uMapD = dtInfo.readDefaultDataTypeMap(self.__pathSaveTypeMap, dataTyping="ANY")
            self.assertEqual(len(uMapD), len(rMapD))
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testStored(self):
        """Verify stored type mapping assignments."""
        try:
            dta = DataTypeApiProvider(self.__cfgOb, self.__cachePath, useCache=False)
            dtInfo = dta.getDataTypeApplicationApi("JSON")
            mapD = dtInfo.getDefaultDataTypeMap()
            logger.debug("Default type map length %d", len(mapD))
            self.assertGreaterEqual(len(mapD), 38)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def dictTypeInfoDefaultSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DataTypeApplicationInfoTests("testDefaults"))
    return suiteSelect


def dictTypeInfoStoredSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DataTypeApplicationInfoTests("testStored"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = dictTypeInfoDefaultSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    mySuite = dictTypeInfoStoredSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
