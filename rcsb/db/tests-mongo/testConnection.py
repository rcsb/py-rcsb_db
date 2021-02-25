##
# File:    ConnectionTests.py
# Author:  J. Westbrook
# Date:    12-Mar-2018
# Version: 0.001
#
# Updates:
#   27-Mar-2018 jdw inject configuration for configuration object rather than environment
##
"""
Test cases opening database connections.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.mongo.Connection import Connection
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ConnectionBaseTests(unittest.TestCase):
    def setUp(self):
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName)
        self.__resourceName = "MONGO_DB"

        self.__startTime = time.time()
        logger.debug("Starting at %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed at %s (%.4f seconds)", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testCreateConnection(self):
        """Test case -  connection creation"""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                self.assertNotEqual(client, None)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCreateMultipleConnections(self):
        """Test case -  multiple connection creation"""
        try:
            for _ in range(25):
                with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                    self.assertNotEqual(client, None)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def suiteOpen():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ConnectionBaseTests("testCreateConnection"))
    suiteSelect.addTest(ConnectionBaseTests("testCreateMultipleConnections"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteOpen()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
