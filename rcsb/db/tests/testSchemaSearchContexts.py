##
# File:    SchemaProviderTests.py
# Author:  J. Westbrook
# Date:    9-Dec-2019
# Version: 0.001
#
# Update:
##
"""
Tests for essential access features of SchemaProvider() module

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.helpers.DocumentDefinitionHelper import DocumentDefinitionHelper
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaSearchContextsTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__docHelper = DocumentDefinitionHelper(cfgOb=self.__cfgOb)
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSearchGroups(self):
        ok = self.__docHelper.checkSearchGroups()
        self.assertTrue(ok)


def schemaSearchGroupSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaSearchContextsTests("testSearchGroups"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = schemaSearchGroupSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
