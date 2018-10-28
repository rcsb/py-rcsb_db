##
#
# File:    ConnectionTests.py
# Author:  J. Westbrook
# Date:    26-Mar-2018
# Version: 0.001
#
# Updates:
#  30-Mar-2018 jdw add tests for context manager style opens
#  25-Oct-2018 jdw add section name to connnection resource assignment method
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
import sys
import time
import unittest

from rcsb.db.mysql.Connection import Connection
from rcsb.db.mysql.MyDbUtil import MyDbQuery
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ConnectionTests(unittest.TestCase):

    def setUp(self):
        self.__dbName = 'test_database'
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__myC = None

        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.yml')
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName)
        self.__resourceName = "MYSQL_DB"
        self.__connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName, sectionName='')

        self.__startTime = time.time()
        logger.debug("Starting at %s" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed at %s (%.4f seconds)" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime))

    def __assignResource(self, cfgOb, sectionName='site_server_info', resourceName="MYSQL_DB"):
        cn = Connection(cfgOb=cfgOb)
        return cn.assignResource(resourceName=resourceName, sectionName='site_server_info')

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

    def testCreateConnectionContext(self):
        """Test case -  connection creation using context manager
        """
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName="MYSQL_DB") as client:
                self.assertNotEqual(client, None)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

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

    def testCreateMultipleConnectionsContext(self):
        """Test case -  multiple connection creation
        """
        try:
            for ii in range(100):
                with Connection(cfgOb=self.__cfgOb, resourceName="MYSQL_DB") as client:
                    self.assertNotEqual(client, None)
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

    def testCreateMultipleConnectionsWithQueryContext(self):
        """Test case -  multiple connection creation
        """
        try:
            for ii in range(100):
                with Connection(cfgOb=self.__cfgOb, resourceName="MYSQL_DB") as client:
                    self.assertNotEqual(client, None)
                    for jj in range(100):
                        my = MyDbQuery(dbcon=client)
                        ok = my.testSelectQuery(count=ii + jj)
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


def suiteOpenContext():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ConnectionTests("testCreateConnectionContext"))
    suiteSelect.addTest(ConnectionTests("testCreateMultipleConnectionsContext"))
    suiteSelect.addTest(ConnectionTests("testCreateMultipleConnectionsWithQueryContext"))
    return suiteSelect


if __name__ == '__main__':
    if (True):
        mySuite = suiteOpen()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    if (True):
        mySuite = suiteOpenContext()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
