##
#
# File:    ConnectionBaseTests.py
# Author:  J. Westbrook
# Date:    12-Mar-2018
# Version: 0.001
#
# Updates:
##
"""
Test cases opening database connections.

Adjust environment setup  -

        . set-test-env.sh


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

from rcsb_db.mongo.ConnectionBase import ConnectionBase


class ConnectionBaseTests(unittest.TestCase):

    def setUp(self):
        self.__dbName = 'test_database'
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__myC = None
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        # close any open connections -
        self.close()
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def open(self, dbUserId=None, dbUserPwd=None, dbHost=None, dbName=None, dbPort=None, dbAdminDb=None):
        authD = {"DB_HOST": dbHost, 'DB_USER': dbUserId, 'DB_PW': dbUserPwd, 'DB_NAME': dbName, "DB_PORT": dbPort, 'DB_ADMIN_DB_NAME': dbAdminDb}
        self.__myC = ConnectionBase()
        self.__myC.setAuth(authD)

        ok = self.__myC.openConnection()
        if ok:
            return True
        else:
            return False

    def close(self):
        if self.__myC is not None:
            self.__myC.closeConnection()
            self.__myC = None
            return True
        else:
            return False

    def getConnection(self):
        return self.__myC.getConnection()

    def testOpen1(self):
        """Test case -  connection with all arguments provided

        Environment setup --

        . set-test-env.sh

        """
        try:
            dbUserId = os.getenv("MONGO_DB_USER_NAME")
            dbUserPwd = os.getenv("MONGO_DB_PASSWORD")
            dbName = os.getenv("MONGO_DB_NAME")
            dbHost = os.getenv("MONGO_DB_HOST")
            dbPort = os.getenv("MONGO_DB_PORT")
            dbAdminDb = os.getenv("MONGO_DB_ADMIN_DB_NAME")
            ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName, dbPort=dbPort, dbAdminDb=dbAdminDb)
            self.assertTrue(ok)
            ok = self.close()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testOpen2(self):
        """Test case -  connection with limited arguments provided

            Environment setup --

            . set-test-env.sh

        """
        try:
            dbUserId = os.getenv("MONGO_DB_USER_NAME")
            dbUserPwd = os.getenv("MONGO_DB_PASSWORD")
            dbName = os.getenv("MONGO_DB_NAME")
            dbHost = os.getenv("MONGO_DB_HOST")
            dbAdminDb = os.getenv("MONGO_DB_ADMIN_DB_NAME")
            ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName, dbAdminDb=dbAdminDb)
            self.assertTrue(ok)
            ok = self.close()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suiteOpen():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ConnectionBaseTests("testOpen1"))
    suiteSelect.addTest(ConnectionBaseTests("testOpen2"))
    return suiteSelect


if __name__ == '__main__':
    if (True):
        mySuite = suiteOpen()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
