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
#     1-Apr-2018 jdw update test connectionse
#     6-Sep-2018 jdw add schema validation tests
##
"""
Test cases for simple MongoDb client opeations .

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import pprint
import time
import unittest
from collections import OrderedDict

import dateutil.parser

from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class MongoDbUtilTests(unittest.TestCase):

    def setUp(self):
        self.__dbName = 'test_database'
        self.__collectionName = 'test_collection'
        #
        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.yml')
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName)
        self.__resourceName = "MONGO_DB"
        self.__connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName, sectionName='site_server_info')
        # self.__cObj = self.__open(self.__connectD)
        #
        self.__mongoSchema = {
            "bsonType": "object",
            "required": ["strField1", "intField1", "enumField1", "dblField1"],
            "properties": {
                "strField1": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "strField2": {
                    "bsonType": "string",
                    "description": "must be a string and is not required"
                },
                "intField1": {
                    "bsonType": "int",
                    "minimum": 1,
                    "maximum": 100,
                    "exclusiveMaximum": False,
                    "description": "must be an integer in [ 1, 100 ] and is required"
                },
                "enumField1": {
                    "enum": ["v1", "v2", "v3", "v4", None],
                    "description": "can only be one of the enum values and is required"
                },
                "dblField1": {
                    "bsonType": ["double"],
                    "minimum": 0,
                    "description": "must be a double and is required"
                },
                "dateField1": {
                    "bsonType": "date",
                    "description": "must be a date and is not required"
                },
            }
        }
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        # self.__close(self.__cObj)
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def __assignResource(self, cfgOb, resourceName="MONGO_DB", sectionName=None):
        cn = Connection(cfgOb=cfgOb)
        return cn.assignResource(resourceName=resourceName, sectionName=sectionName)

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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
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
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=[keyName], salvage=True)
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                nDocs = 10
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
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=[keyName], salvage=True)
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
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                nDocs = 100
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
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=[keyName], salvage=True)
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
            logger.debug("Starting testSingleIndexSelect")
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                nDocs = 100
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
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=[keyName], salvage=True)
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
                logger.debug("HERE NOW")
                #
            logger.debug("ReStarting client")
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                ii = mg.count(self.__dbName, self.__collectionName)
                logger.debug("Fetch length %d" % ii)
                #
                cur = mg.fetch(self.__dbName, self.__collectionName, ['DOC_ID'])
                self.assertEqual(cur.count_documents(), nDocs)
                logger.debug("Fetch length %d" % cur.count_documents())
                for ii, d in enumerate(cur):
                    logger.debug("Fetch num %d: %r" % (ii, d))
                #
                #
                logger.debug("HERE NOW")
                cur = mg.fetch(self.__dbName, self.__collectionName, ['category_0.attribute_0'], {'category_0.attribute_0': 'val_0_0'})
                self.assertEqual(cur.count_documents(), nDocs)
                logger.debug("Fetch length %d" % cur.count_documents())
                for ii, d in enumerate(cur):
                    logger.debug("Fetch num %d: %r" % (ii, d))
            logger.debug("HERE NOW")
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSchemaValidation1(self):
        """Test case -  create collection and insert data with schema validation (ext. schema assignment)

        """

        #  Example of a Mongo flavor of JsonSchema
        vexpr = {"$jsonSchema": self.__mongoSchema}

        query = [('collMod', self.__collectionName),
                 ('validator', vexpr),
                 ('validationLevel', 'moderate')]
        query = OrderedDict(query)

        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.databaseExists(self.__dbName):
                    ok = mg.dropDatabase(self.__dbName)
                    self.assertTrue(ok)
                #
                ok = mg.createDatabase(self.__dbName)
                self.assertTrue(ok)
                #
                ok = mg.createCollection(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                ok = mg.databaseExists(self.__dbName)
                self.assertTrue(ok)
                ok = mg.collectionExists(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                #
                mg.databaseCommand(self.__dbName, query)
                dObj = {"x": 1}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                logger.info("rId is %r" % rId)
                self.assertEqual(rId, None)
                #
                dObj = {"strField1": "test value", "intField1": 50, "enumField1": "v3", "dblField1": 100.1}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                logger.info("rId is %r" % rId)
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, '_id', rId)
                logger.debug("Return Object %s" % pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSchemaValidation2(self):
        """Test case -  create collection and insert data with schema validation (strict mode) (integrated schema assignment)

        """
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.databaseExists(self.__dbName):
                    ok = mg.dropDatabase(self.__dbName)
                    self.assertTrue(ok)
                #
                ok = mg.createDatabase(self.__dbName)
                self.assertTrue(ok)
                #
                ok = mg.createCollection(self.__dbName, self.__collectionName, overWrite=True, bsonSchema=self.__mongoSchema)
                self.assertTrue(ok)
                ok = mg.databaseExists(self.__dbName)
                self.assertTrue(ok)
                ok = mg.collectionExists(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                #
                dObj = {"x": 1}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                self.assertEqual(rId, None)
                logger.info("rId is %r" % rId)
                dtVal = dateutil.parser.parse("2018-01-30 12:01")
                logger.debug("date value is %r" % dtVal)
                dObj = {"strField1": "test value", "intField1": 50, "enumField1": "v3", "dblField1": 100.1, "dateField1": dtVal}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                logger.info("rId is %r" % rId)
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, '_id', rId)
                logger.debug("Return Object %s" % pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSchemaValidation3(self):
        """Test case -  create collection and insert data with schema validation (warn mode) (integrated schema assignment)

        """
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.databaseExists(self.__dbName):
                    ok = mg.dropDatabase(self.__dbName)
                    self.assertTrue(ok)
                #
                ok = mg.createDatabase(self.__dbName)
                self.assertTrue(ok)
                #
                ok = mg.createCollection(self.__dbName, self.__collectionName, overWrite=True, bsonSchema=self.__mongoSchema, validationAction='warn')
                self.assertTrue(ok)
                ok = mg.databaseExists(self.__dbName)
                self.assertTrue(ok)
                ok = mg.collectionExists(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                #
                dObj = {"x": 1}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                logger.info("rId is %r" % rId)
                self.assertNotEqual(rId, None)

                dObj = {"strField1": "test value", "intField1": 50, "enumField1": "v3a", "dblField1": 100.1}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                self.assertNotEqual(rId, None)
                logger.info("rId is %r" % rId)
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, '_id', rId)
                logger.debug("Return Object %s" % pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)

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


def suiteValidation():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MongoDbUtilTests("testSchemaValidation1"))
    suiteSelect.addTest(MongoDbUtilTests("testSchemaValidation2"))
    suiteSelect.addTest(MongoDbUtilTests("testSchemaValidation3"))
    return suiteSelect


def suiteIndex1():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MongoDbUtilTests("testSingleIndexSelect"))
    return suiteSelect


if __name__ == '__main__':
    if (False):
        mySuite = suiteIndex1()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if (True):
        mySuite = suiteOps()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteInsert()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteReplace()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteIndex()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteValidation()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
