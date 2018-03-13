##
#
# File:    MongoDbUtilTests.py
# Author:  J. Westbrook
# Date:    12-Mar-2018
# Version: 0.001
#
# Updates:
##
"""
Test cases for simple MongoDb client opeations .

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
import pprint

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
from rcsb_db.mongo.MongoDbUtil import MongoDbUtil


class MongoDbUtilTests(unittest.TestCase):

    def setUp(self):
        self.__dbName = 'test_database'
        self.__collectionName = 'test_collection'
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__myC = None
        dbUserId = os.getenv("MONGO_DB_USER_NAME")
        dbUserPwd = os.getenv("MONGO_DB_PASSWORD")
        dbName = os.getenv("MONGO_DB_NAME")
        dbHost = os.getenv("MONGO_DB_HOST")
        dbPort = os.getenv("MONGO_DB_PORT")
        ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName, dbPort=dbPort)
        self.assertTrue(ok)
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

    def open(self, dbUserId=None, dbUserPwd=None, dbHost=None, dbName=None, dbPort=None):
        authD = {"DB_HOST": dbHost, 'DB_USER': dbUserId, 'DB_PW': dbUserPwd, 'DB_NAME': dbName, "DB_PORT": dbPort}
        self.__myC = ConnectionBase()
        self.__myC.setAuth(authD)

        ok = self.__myC.openConnection()
        if ok:
            logger.debug("Database connection opened %s %s at %s" % (self.__class__.__name__,
                                                                     sys._getframe().f_code.co_name,
                                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
            return True
        else:
            return False

    def close(self):
        if self.__myC is not None:
            logger.debug("Database connection closed %s %s at %s" % (self.__class__.__name__,
                                                                     sys._getframe().f_code.co_name,
                                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
            self.__myC.closeConnection()
            self.__myC = None
            return True
        else:
            return False

    def getClientConnection(self):
        return self.__myC.getClientConnection()

    def __makeDataObj(self, nCats, Nattribs, Nrows):
        rD = {}
        for cat in range(nCats):
            catName = "category_%d" % cat
            rD[catName] = []
            for row in range(Nrows):
                d = {}
                for attrib in range(Nattribs):
                    val = "val_%d_%d" % (row, attrib)
                    attribName = "attrib_%d" % attrib
                    d[attribName] = val
                rD[catName].append(d)
        return rD

    def testCreateDatabase(self):
        """Test case -  create database -

        Environment setup --

        . set-test-env.sh

        """
        try:
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createDatabase(self.__dbName)
            self.assertTrue(ok)
            ok = mg.createDatabase(self.__dbName)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testCreateCollection(self):
        """Test case -  create collection -

        Environment setup --

        . set-test-env.sh

        """
        try:
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            #
            ok = mg.createCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testCreateDropDatabase(self):
        """Test case -  create/drop database -

        Environment setup --

        . set-test-env.sh

        """
        try:
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createDatabase(self.__dbName)
            self.assertTrue(ok)
            ok = mg.dropDatabase(self.__dbName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertFalse(ok)
            #
            ok = mg.createDatabase(self.__dbName)
            self.assertTrue(ok)
            ok = mg.dropDatabase(self.__dbName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertFalse(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testCreateCollectionDropDatabase(self):
        """Test case -  create/drop collection -

        Environment setup --

        . set-test-env.sh

        """
        try:
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            #
            ok = mg.dropDatabase(self.__dbName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertFalse(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertFalse(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testCreateDropCollection(self):
        """Test case -  create/drop collection -

        Environment setup --

        . set-test-env.sh

        """
        try:
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            #
            logger.debug("Databases = %r" % mg.getDatabaseNames())
            logger.debug("Collections = %r" % mg.getCollectionNames(self.__dbName))
            ok = mg.dropCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            logger.debug("Databases = %r" % mg.getDatabaseNames())
            logger.debug("Collections = %r" % mg.getCollectionNames(self.__dbName))
            # Removing the last collection will remove the database
            ok = mg.databaseExists(self.__dbName)
            self.assertFalse(ok)
            #
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertFalse(ok)
            logger.debug("Collections = %r" % mg.getCollectionNames(self.__dbName))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testInsertSingle(self):
        """Test case -  create collection and insert data -

        Environment setup --

        . set-test-env.sh

        """
        try:
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            #
            dObj = self.__makeDataObj(2, 5, 5)
            rId = mg.insert(self.__dbName, self.__collectionName, dObj)
            self.assertTrue(rId is not None)
            # Note that dObj is mutated by additional key '_id' that is added on insert -
            #
            rObj = mg.fetchOne(self.__dbName, self.__collectionName, '_id', rId)
            logger.debug("Return Object %s" % pprint.pformat(rObj))
            self.assertEqual(len(dObj), len(rObj))
            self.assertEqual(dObj, rObj)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testInsertList(self):
        """Test case -  create collection and insert data -

        Environment setup --

        . set-test-env.sh

        """
        try:
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            #
            dList = []
            for ii in range(10):
                dList.append(self.__makeDataObj(2, 5, 5))
            #
            rIdL = mg.insertList(self.__dbName, self.__collectionName, dList)
            self.assertTrue(len(rIdL), len(dList))
            # Note that dObj is mutated by additional key '_id' that is added on insert -
            #
            for ii, rId in enumerate(rIdL):
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, '_id', rId)
                logger.debug("Return Object %s" % pprint.pformat(rObj))
                self.assertEqual(len(dList[ii]), len(rObj))
                self.assertEqual(dList[ii], rObj)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suiteOps():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MongoDbUtilTests("testCreateDatabase"))
    suiteSelect.addTest(MongoDbUtilTests("testCreateDropDatabase"))
    suiteSelect.addTest(MongoDbUtilTests("testCreateCollection"))
    suiteSelect.addTest(MongoDbUtilTests("testCreateCollectionDropDatabase"))
    suiteSelect.addTest(MongoDbUtilTests("testCreateDropCollection"))
    return suiteSelect


def suiteInsert():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MongoDbUtilTests("testInsertSingle"))
    suiteSelect.addTest(MongoDbUtilTests("testInsertList"))
    return suiteSelect

if __name__ == '__main__':
    if (True):
        mySuite = suiteOps()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteInsert()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
