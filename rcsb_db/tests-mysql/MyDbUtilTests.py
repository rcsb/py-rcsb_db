##
#
# File:    MyDbUtilTests.py
# Author:  J. Westbrook
# Date:    20-June-2015
# Version: 0.001
#
# Updates:  20-Dec-2017 jdw py2/py3 working in compat23 branch
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

from rcsb_db.mysql.MyDbUtil import MyDbConnect
from rcsb_db.mysql.MyDbUtil import MyDbQuery


class MyDbUtilTests(unittest.TestCase):

    def setUp(self):
        self.__dbName = 'stat'
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__dbCon = None
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        self.close()
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def open(self, dbUserId=None, dbUserPwd=None, dbHost=None, dbName=None, dbSocket=None):
        myC = MyDbConnect(dbServer='mysql', dbHost=dbHost, dbName=dbName, dbUser=dbUserId, dbPw=dbUserPwd, dbSocket=dbSocket, verbose=self.__verbose)
        self.__dbCon = myC.connect()
        if self.__dbCon is not None:
            if self.__verbose:
                logger.info("\nDatabase connection opened %s %s at %s\n" % (self.__class__.__name__,
                                                                            sys._getframe().f_code.co_name,
                                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
            return True
        else:
            return False

    def close(self):
        if self.__dbCon is not None:
            if self.__verbose:
                logger.info("\nDatabase connection closed %s %s at %s\n" % (self.__class__.__name__,
                                                                            sys._getframe().f_code.co_name,
                                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
            self.__dbCon.close()
            self.__dbCon = None
            return True
        else:
            return False

    def testOpen1(self):
        """Test case -  all values specified

        Environment setup --

        . set-test-env.sh

        """

        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = os.getenv("TEST_DB_HOST")
            dbSocket = os.getenv("TEST_DB_SOCKET")
            ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName, dbSocket=dbSocket)
            self.assertTrue(ok)
            ok = self.close()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testOpen2(self):
        """Test case -  w/o socket

            Environment setup --

            . set-test-env.sh

        """

        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = os.getenv("TEST_DB_HOST")

            ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName)
            self.assertTrue(ok)
            ok = self.close()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testOpen3(self):
        """Test case -  w/o socket w/ localhost

            Environment setup --

            . set-test-env.sh

        """

        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = 'localhost'

            ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName)
            self.assertTrue(ok)
            ok = self.close()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPool1(self):
        """Test case -  connection pool management -

        Setup -
        . set-test-env.sh

        """

        self.__verbose = False
        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = os.getenv("TEST_DB_HOST")
            for ii in range(5000):
                ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName)
                self.assertTrue(ok)
                ok = self.close()
                self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPoolQuery(self):
        """Test case -  connection pool management -

        Setup -
        . set-test-env.sh

        """

        self.__verbose = False
        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = os.getenv("TEST_DB_HOST")
            for ii in range(5000):
                ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName)
                self.assertTrue(ok)
                for jj in range(100):
                    my = MyDbQuery(dbcon=self.__dbCon)
                    ok = my.testSelectQuery(count=ii + jj)
                    self.assertTrue(ok)

                ok = self.close()
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()



def suiteOpen():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbUtilTests("testOpen1"))
    suiteSelect.addTest(MyDbUtilTests("testOpen2"))
    suiteSelect.addTest(MyDbUtilTests("testOpen3"))
    return suiteSelect


def suitePool():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbUtilTests("testPool1"))
    suiteSelect.addTest(MyDbUtilTests("testPoolQuery"))
    return suiteSelect


if __name__ == '__main__':
    if (True):
        mySuite = suiteOpen()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if (True):
        mySuite = suitePool()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
