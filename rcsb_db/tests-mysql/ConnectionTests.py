##
#
# File:    ConnectionTests.py
# Author:  J. Westbrook
# Date:    26-Mar-2018
# Version: 0.001
#
# Updates:
##
"""
Test cases opening database connections.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import os
import sys
import unittest
import time

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.mysql.Connection import Connection
from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.utils.ConfigUtil import ConfigUtil


class ConnectionTests(unittest.TestCase):

    def setUp(self):
        self.__dbName = 'test_database'
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__myC = None

        configPath = os.path.join(TOPDIR, 'rcsb_db', 'data', 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "MYSQL_DB"
        self.__connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName)

        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting at %s" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed at %s (%.4f seconds)" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime))

    def __assignResource(self, cfgOb, resourceName="MYSQL_DB"):
        cn = Connection(cfgOb=cfgOb)
        return cn.assignResource(resourceName=resourceName)

    def __open(self, connectD):
        cObj = Connection()
        cObj.setPreferences(connectD)
        ok = cObj.openConnection()
        if ok:
            return cObj
        else:
            return None

    def __close(self, cObj):
        if cObj is not None:
            cObj.closeConnection()
            return True
        else:
            return False

    def __getClientConnection(self, cObj):
        return cObj.getClientConnection()

    def testCreateConnection(self):
        """Test case -  connection creation
        """
        try:
            cObj = self.__open(self.__connectD)
            client = self.__getClientConnection(cObj)
            self.assertNotEqual(client, None)
            ok = self.__close(cObj)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testCreateMultipleConnections(self):
        """Test case -  multiple connection creation
        """
        try:
            for ii in range(100):
                cObj = self.__open(self.__connectD)
                client = self.__getClientConnection(cObj)
                self.assertNotEqual(client, None)
                ok = self.__close(cObj)
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testCreateMultipleConnectionsWithQuery(self):
        """Test case -  multiple connection creation
        """
        try:
            for ii in range(100):
                cObj = self.__open(self.__connectD)
                client = self.__getClientConnection(cObj)
                self.assertNotEqual(client, None)
                for jj in range(100):
                    my = MyDbQuery(dbcon=client)
                    ok = my.testSelectQuery(count=ii + jj)
                    self.assertTrue(ok)
                ok = self.__close(cObj)
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suiteOpen():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ConnectionTests("testCreateConnection"))
    suiteSelect.addTest(ConnectionTests("testCreateMultipleConnections"))
    suiteSelect.addTest(ConnectionTests("testCreateMultipleConnectionsWithQuery"))
    return suiteSelect


if __name__ == '__main__':
    if (True):
        mySuite = suiteOpen()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
