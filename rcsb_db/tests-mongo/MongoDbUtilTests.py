##
#
# File:    MongoDbUtilTests.py
# Author:  J. Westbrook
# Date:    12-Mar-2018
# Version: 0.001
#
# Updates:
#    19-Mar-2018 jdw remove any assumptions about the order of bulk inserts.
#    27-Mar-2018 jdw connection configuration now via ConfigUtil -
##
"""
Test cases for simple MongoDb client opeations .

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
import dateutil.parser

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

from rcsb_db.mongo.Connection import Connection
from rcsb_db.mongo.MongoDbUtil import MongoDbUtil
from rcsb_db.utils.ConfigUtil import ConfigUtil


class MongoDbUtilTests(unittest.TestCase):

    def setUp(self):
        self.__dbName = 'test_database'
        self.__collectionName = 'test_collection'
        #
        configPath = os.path.join(TOPDIR, 'rcsb_db', 'data', 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "MONGO_DB"
        self.__connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName)
        self.__cObj = self.__open(self.__connectD)
        #
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        self.__close(self.__cObj)
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def __assignResource(self, cfgOb, resourceName="MONGO_DB"):
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

    def getClientConnection(self):
        return self.__cObj.getClientConnection()

    def __makeDataObj(self, nCats, Nattribs, Nrows, docId=1):
        rD = {}
        for cat in range(nCats):
            catName = "category_%d" % cat
            rD[catName] = []
            for row in range(Nrows):
                d = {}
                for attrib in range(Nattribs):
                    val = "val_%d_%d" % (row, attrib)
                    attribName = "attribute_%d" % attrib
                    d[attribName] = val
                rD[catName].append(d)
            d = {}
            for attrib in range(Nattribs):
                val = "2018-01-30 12:01"
                attribName = "attribute_%d" % attrib
                d[attribName] = dateutil.parser.parse(val)
            rD[catName].append(d)
        rD['DOC_ID'] = "DOC_%d" % docId
        return rD

    def testCreateDatabase(self):
        """Test case -  create database -

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
            # Removing the last collection will remove the database (results appear differ between mac and linux - )
            ok = mg.databaseExists(self.__dbName)
            # self.assertFalse(ok)
            #
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertFalse(ok)
            logger.debug("Collections = %r" % mg.getCollectionNames(self.__dbName))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testInsertSingle(self):
        """Test case -  create collection and insert data -

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
            for ii in range(100):
                dList.append(self.__makeDataObj(2, 5, 5, ii))
            #
            keyName = 'DOC_ID'
            rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyName)
            self.assertEqual(len(rIdL), len(dList))
            #
            # Note that dObj is mutated by additional key '_id' that is added on insert -
            #
            for ii, rId in enumerate(rIdL):
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, '_id', rId)
                logger.debug("Return Object %s" % pprint.pformat(rObj))
                jj = int(rObj['DOC_ID'][4:])
                self.assertEqual(len(dList[jj]), len(rObj))
                self.assertEqual(dList[jj], rObj)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testReplaceSingle(self):
        """Test case -  create collection and insert document  and then replace document -

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
            dObj = self.__makeDataObj(2, 5, 5, 1)
            rId = mg.insert(self.__dbName, self.__collectionName, dObj)
            self.assertTrue(rId is not None)
            # Note that dObj is mutated by additional key '_id' that is added on insert -
            #
            rObj = mg.fetchOne(self.__dbName, self.__collectionName, '_id', rId)
            logger.debug("Return Object %s" % pprint.pformat(rObj))
            self.assertEqual(len(dObj), len(rObj))
            self.assertEqual(dObj, rObj)
            #
            # Now replace with a new document with the same document id
            dObj = self.__makeDataObj(3, 2, 2, 1)
            logger.debug("Replace Object %s" % pprint.pformat(dObj))

            rId = mg.replace(self.__dbName, self.__collectionName, dObj, {'DOC_ID': 'DOC_1'}, upsertFlag=True)
            # self.assertTrue(rId is not None)
            rObj = mg.fetchOne(self.__dbName, self.__collectionName, 'DOC_ID', 'DOC_1')
            rObj.pop('_id', None)
            dObj.pop('_id', None)
            logger.debug("Return Object %s" % pprint.pformat(rObj))
            self.assertEqual(len(dObj), len(rObj))
            self.assertEqual(dObj, rObj)
            #
            # Now replace with a new document with a different key
            dObj2 = self.__makeDataObj(5, 5, 5, 2)
            logger.debug("Replace Object %s" % pprint.pformat(dObj))
            #
            rId = mg.replace(self.__dbName, self.__collectionName, dObj2, {'DOC_ID': 'DOC_2'}, upsertFlag=True)
            rObj = mg.fetchOne(self.__dbName, self.__collectionName, 'DOC_ID', 'DOC_2')
            rObj.pop('_id', None)
            dObj2.pop('_id', None)
            logger.debug("Return Object %s" % pprint.pformat(rObj))
            self.assertEqual(len(dObj2), len(rObj))
            self.assertEqual(dObj2, rObj)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testReplaceList(self):
        """Test case -  create collection and insert document list - replace and upsert document list

        """
        try:
            nDocs = 10
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
            for ii in range(nDocs):
                dObj = self.__makeDataObj(2, 5, 5, ii)
                dList.append(dObj)
            #
            keyName = 'DOC_ID'
            rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyName)
            self.assertEqual(len(rIdL), len(dList))
            #
            for ii, rId in enumerate(rIdL):
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, '_id', rId)
                # logger.debug("Return Object %s" % pprint.pformat(rObj))
                self.assertEqual(len(dList[ii]), len(rObj))
                self.assertEqual(dList[ii], rObj)
            #
            #  Replace with 2x the list length - half are duplicates id's
            dList = []
            for ii in range(nDocs + nDocs):
                dObj = self.__makeDataObj(4, 10, 10, ii)
                dList.append(dObj)
            #
            updL = mg.replaceList(self.__dbName, self.__collectionName, dList, 'DOC_ID', upsertFlag=True)
            logger.debug("Upserted id list length %d" % len(updL))
            for ii in range(nDocs + nDocs):
                kVal = 'DOC_%d' % ii
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, 'DOC_ID', kVal)
                # logger.debug("Return Object %s" % pprint.pformat(rObj))
                rObj.pop('_id', None)
                dList[ii].pop('_id', None)
                self.assertEqual(len(dList[ii]), len(rObj))
                self.assertEqual(dList[ii], rObj)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSingleIndex(self):
        """Test case -  create collection, create simple single index, insert document list, read check documents

        """
        try:
            nDocs = 100
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            #
            # Create before insert
            ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=['DOC_ID'], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
            self.assertTrue(ok)

            dList = []
            for ii in range(nDocs):
                dObj = self.__makeDataObj(2, 5, 5, ii)
                dList.append(dObj)
            #
            keyName = 'DOC_ID'
            rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyName)
            self.assertEqual(len(dList), len(rIdL))
            #
            for ii in range(nDocs):
                kVal = 'DOC_%d' % ii
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, 'DOC_ID', kVal)
                # logger.debug("Return Object %s" % pprint.pformat(rObj))
                rObj.pop('_id', None)
                dList[ii].pop('_id', None)
                self.assertEqual(len(dList[ii]), len(rObj))
                self.assertEqual(dList[ii], rObj)
            #
            ok = mg.dropIndex(self.__dbName, self.__collectionName, indexName="primary")
            self.assertTrue(ok)
            ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=['DOC_ID'], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
            self.assertTrue(ok)
            ok = mg.reIndex(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSingleIndexSelect(self):
        """Test case -  create collection, create simple single index, insert document list, read check documents.

        """
        try:
            nDocs = 100
            client = self.getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createCollection(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(self.__dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            #
            # Create before insert
            ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=['DOC_ID'], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
            self.assertTrue(ok)

            dList = []
            for ii in range(nDocs):
                dObj = self.__makeDataObj(2, 5, 5, ii)
                dList.append(dObj)
            #
            keyName = 'DOC_ID'
            rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyName)
            self.assertEqual(len(dList), len(rIdL))
            #
            for ii in range(nDocs):
                kVal = 'DOC_%d' % ii
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, 'DOC_ID', kVal)
                # logger.debug("Return Object %s" % pprint.pformat(rObj))
                rObj.pop('_id', None)
                dList[ii].pop('_id', None)
                self.assertEqual(len(dList[ii]), len(rObj))
                self.assertEqual(dList[ii], rObj)
            #
            ok = mg.dropIndex(self.__dbName, self.__collectionName, indexName="primary")
            self.assertTrue(ok)
            ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=['DOC_ID'], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
            self.assertTrue(ok)
            ok = mg.reIndex(self.__dbName, self.__collectionName)
            self.assertTrue(ok)
            #
            #
            cur = mg.fetch(self.__dbName, self.__collectionName, ['DOC_ID'])
            self.assertEqual(cur.count(), nDocs)
            logger.debug("Fetch length %d" % cur.count())
            for ii, d in enumerate(cur):
                logger.debug("Fetch num %d: %r" % (ii, d))
            #
            #
            cur = mg.fetch(self.__dbName, self.__collectionName, ['category_0.attribute_0'], {'category_0.attribute_0': 'val_0_0'})
            self.assertEqual(cur.count(), nDocs)
            logger.debug("Fetch length %d" % cur.count())
            for ii, d in enumerate(cur):
                logger.debug("Fetch num %d: %r" % (ii, d))
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


def suiteReplace():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MongoDbUtilTests("testReplaceSingle"))
    suiteSelect.addTest(MongoDbUtilTests("testReplaceList"))
    return suiteSelect


def suiteIndex():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MongoDbUtilTests("testSingleIndex"))
    suiteSelect.addTest(MongoDbUtilTests("testSingleIndexSelect"))
    return suiteSelect

if __name__ == '__main__':
    if (True):
        mySuite = suiteOps()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteInsert()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteReplace()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteIndex()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
