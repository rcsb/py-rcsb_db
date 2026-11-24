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
#     8-Jan-2019 jdw add tests for loading and recovering translated XML character references
#    24-Nov-2026 dwp add testFetchProjection for fetch()/fetchBatched() with a projection document (projectionD),
#                    and testFetchOptions for the configured paged-fetch/count defaults (MongoDbUtil(..., cfgOb=...))
##
"""
Test cases for simple MongoDb client operations.

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
from rcsb.db.utils.TextUtil import unescapeXmlCharRef
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class MongoDbUtilTests(unittest.TestCase):
    def setUp(self):
        self.__dbName = "test_database"
        self.__collectionName = "test_collection"
        #
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__configPath = configPath
        self.__configName = configName
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName)
        self.__resourceName = "MONGO_DB"
        self.__connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName, sectionName=configName)
        # self.__cObj = self.__open(self.__connectD)
        #
        self.__mongoSchema = {
            "bsonType": "object",
            "required": ["strField1", "intField1", "enumField1", "dblField1"],
            "properties": {
                "strField1": {"bsonType": "string", "description": "must be a string and is required"},
                "strField2": {"bsonType": "string", "description": "must be a string and is not required"},
                "strField3": {"bsonType": "string", "description": "must be a string and is not required"},
                "intField1": {"bsonType": "int", "minimum": 1, "maximum": 100, "exclusiveMaximum": False, "description": "must be an integer in [ 1, 100 ] and is required"},
                "enumField1": {"enum": ["v1", "v2", "v3", "v4", None], "description": "can only be one of the enum values and is required"},
                "dblField1": {"bsonType": ["double"], "minimum": 0, "description": "must be a double and is required"},
                "dateField1": {"bsonType": "date", "description": "must be a date and is not required"},
            },
        }
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        # self.__close(self.__cObj)
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

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

    # def getClientConnection(self):
    #    return self.__cObj.getClientConnection()

    def __unescape(self, iStr):
        """
        Convert html character entities into unicode.
        """
        return unescapeXmlCharRef(iStr)

    def __makeDataObj(self, nCats, nAttribs, nRows, docId=1):
        rD = {}
        for cat in range(nCats):
            catName = "category_%d" % cat
            rD[catName] = []
            for row in range(nRows):
                dD = {}
                for attrib in range(nAttribs):
                    val = "val_%d_%d" % (row, attrib)
                    attribName = "attribute_%d" % attrib
                    dD[attribName] = val
                rD[catName].append(dD)
            dD = {}
            for attrib in range(nAttribs):
                val = "2018-01-30 12:01"
                attribName = "attribute_%d" % attrib
                dD[attribName] = dateutil.parser.parse(val)
            rD[catName].append(dD)
            #
            dD = {}
            for attrib in range(nAttribs):
                val = " &quot; &Phi; &Psi; &alpha; &#xa3;  &#8453;  &#9734;  &#120171; "
                attribName = "attribute_%d" % attrib
                dD[attribName] = unescapeXmlCharRef(val).encode("utf-8").decode("utf-8")
            rD[catName].append(dD)
        rD["DOC_ID"] = "DOC_%d" % docId
        return rD

    def testCreateDatabase(self):
        """Test case -  create database -"""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                ok = mg.createDatabase(self.__dbName)
                self.assertTrue(ok)
                ok = mg.createDatabase(self.__dbName)
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCreateCollection(self):
        """Test case -  create collection -"""
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
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCreateDropDatabase(self):
        """Test case -  create/drop database -"""
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
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCreateCollectionDropDatabase(self):
        """Test case -  create/drop collection -"""
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
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCreateDropCollection(self):
        """Test case -  create/drop collection -"""
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
                logger.debug("Databases = %r", mg.getDatabaseNames())
                logger.debug("Collections = %r", mg.getCollectionNames(self.__dbName))
                ok = mg.dropCollection(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                logger.debug("Databases = %r", mg.getDatabaseNames())
                logger.debug("Collections = %r", mg.getCollectionNames(self.__dbName))
                # Removing the last collection will remove the database (results appear differ between mac and linux - )
                ok = mg.databaseExists(self.__dbName)
                # self.assertFalse(ok)
                #
                ok = mg.collectionExists(self.__dbName, self.__collectionName)
                self.assertFalse(ok)
                logger.debug("Collections = %r", mg.getCollectionNames(self.__dbName))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testInsertSingle(self):
        """Test case -  create collection and insert data -"""
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
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, "_id", rId)
                logger.debug("Return Object %s", pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testInsertList(self):
        """Test case -  create collection and insert data -"""
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
                keyName = "DOC_ID"
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=[keyName], salvage=True)
                self.assertEqual(len(rIdL), len(dList))
                #
                # Note that dObj is mutated by additional key '_id' that is added on insert -
                #
                for ii, rId in enumerate(rIdL):
                    rObj = mg.fetchOne(self.__dbName, self.__collectionName, "_id", rId)
                    logger.debug("Return Object %s", pprint.pformat(rObj))
                    jj = int(rObj["DOC_ID"][4:])
                    self.assertEqual(len(dList[jj]), len(rObj))
                    self.assertEqual(dList[jj], rObj)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testReplaceSingle(self):
        """Test case -  create collection and insert document  and then replace document -"""
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
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, "_id", rId)
                logger.debug("Return Object %s", pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)
                #
                # Now replace with a new document with the same document id
                dObj = self.__makeDataObj(3, 2, 2, 1)
                logger.debug("Replace Object %s", pprint.pformat(dObj))

                rId = mg.replace(self.__dbName, self.__collectionName, dObj, {"DOC_ID": "DOC_1"}, upsertFlag=True)
                # self.assertTrue(rId is not None)
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, "DOC_ID", "DOC_1")
                rObj.pop("_id", None)
                dObj.pop("_id", None)
                logger.debug("Return Object %s", pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)
                #
                # Now replace with a new document with a different key
                dObj2 = self.__makeDataObj(5, 5, 5, 2)
                logger.debug("Replace Object %s", pprint.pformat(dObj))
                #
                rId = mg.replace(self.__dbName, self.__collectionName, dObj2, {"DOC_ID": "DOC_2"}, upsertFlag=True)
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, "DOC_ID", "DOC_2")
                rObj.pop("_id", None)
                dObj2.pop("_id", None)
                logger.debug("Return Object %s", pprint.pformat(rObj))
                self.assertEqual(len(dObj2), len(rObj))
                self.assertEqual(dObj2, rObj)
                #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testReplaceList(self):
        """Test case -  create collection and insert document list - replace and upsert document list"""
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
                keyName = "DOC_ID"
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=[keyName], salvage=True)
                self.assertEqual(len(rIdL), len(dList))
                #
                for ii, rId in enumerate(rIdL):
                    rObj = mg.fetchOne(self.__dbName, self.__collectionName, "_id", rId)
                    # logger.debug("Return Object %s", pprint.pformat(rObj))
                    self.assertEqual(len(dList[ii]), len(rObj))
                    self.assertEqual(dList[ii], rObj)
                #
                #  Replace with 2x the list length - half are duplicates id's
                dList = []
                for ii in range(nDocs + nDocs):
                    dObj = self.__makeDataObj(4, 10, 10, ii)
                    dList.append(dObj)
                #
                updL = mg.replaceList(self.__dbName, self.__collectionName, dList, ["DOC_ID"], upsertFlag=True)
                #
                logger.info("Upserted id list length %d", len(updL))
                for ii in range(nDocs + nDocs):
                    kVal = "DOC_%d" % ii
                    rObj = mg.fetchOne(self.__dbName, self.__collectionName, "DOC_ID", kVal)
                    if not rObj:
                        logger.info("Failing to recover doc %s", kVal)
                    # logger.debug("Return Object %s", pprint.pformat(rObj))
                    rObj.pop("_id", None)
                    dList[ii].pop("_id", None)
                    self.assertEqual(len(dList[ii]), len(rObj))
                    self.assertEqual(dList[ii], rObj)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSingleIndex(self):
        """Test case -  create collection, create simple single index, insert document list, read check documents"""
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
                ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=["DOC_ID"], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
                self.assertTrue(ok)

                dList = []
                for ii in range(nDocs):
                    dObj = self.__makeDataObj(2, 5, 5, ii)
                    dList.append(dObj)
                #
                keyName = "DOC_ID"
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=[keyName], salvage=True)
                self.assertEqual(len(dList), len(rIdL))
                #
                for ii in range(nDocs):
                    kVal = "DOC_%d" % ii
                    rObj = mg.fetchOne(self.__dbName, self.__collectionName, "DOC_ID", kVal)
                    # logger.debug("Return Object %s" % pprint.pformat(rObj))
                    rObj.pop("_id", None)
                    dList[ii].pop("_id", None)
                    self.assertEqual(len(dList[ii]), len(rObj))
                    self.assertEqual(dList[ii], rObj)
                #
                ok = mg.dropIndex(self.__dbName, self.__collectionName, indexName="primary")
                self.assertTrue(ok)
                ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=["DOC_ID"], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
                self.assertTrue(ok)
                ok = mg.reIndex(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSingleIndexSelect(self):
        """Test case -  create collection, create simple single index, insert document list, read check documents."""
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
                ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=["DOC_ID"], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
                self.assertTrue(ok)

                dList = []
                nRows = 5
                for ii in range(nDocs):
                    dObj = self.__makeDataObj(2, 5, nRows, ii)
                    dList.append(dObj)
                #
                keyName = "DOC_ID"
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=[keyName], salvage=True)
                self.assertEqual(len(dList), len(rIdL))
                #
                for ii in range(nDocs):
                    kVal = "DOC_%d" % ii
                    rObj = mg.fetchOne(self.__dbName, self.__collectionName, "DOC_ID", kVal)
                    # logger.debug("Return Object %s" % pprint.pformat(rObj))
                    rObj.pop("_id", None)
                    dList[ii].pop("_id", None)
                    self.assertEqual(len(dList[ii]), len(rObj))
                    self.assertEqual(dList[ii], rObj)
                #
                ok = mg.dropIndex(self.__dbName, self.__collectionName, indexName="primary")
                self.assertTrue(ok)
                ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=["DOC_ID"], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
                self.assertTrue(ok)
                ok = mg.reIndex(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                #
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                ii = mg.count(self.__dbName, self.__collectionName)
                logger.debug("collection length %d", ii)
                #
                dList = mg.fetch(self.__dbName, self.__collectionName, ["DOC_ID"])
                self.assertEqual(len(dList), nDocs)
                logger.debug("Fetch length %d", len(dList))
                for ii, dD in enumerate(dList):
                    logger.debug("Fetch num %d: %r", ii, dD)
                #
                dList = mg.fetch(self.__dbName, self.__collectionName, ["category_0.attribute_0"], queryD={"category_0.attribute_0": "val_0_0"})
                self.assertEqual(len(dList), nDocs)
                logger.debug("Fetch length %d", len(dList))
                for ii, dD in enumerate(dList):
                    logger.debug("Fetch num %d: %r", ii, dD)
                atName = "category_0.attribute_0"
                vL0 = mg.distinct(self.__dbName, self.__collectionName, atName)
                self.assertEqual(len(vL0), nRows + 2)
                logger.debug("vL0 %r", vL0)
                vL1 = mg.distinct(self.__dbName, self.__collectionName, "category_1.attribute_0")
                self.assertEqual(len(vL1), nRows + 2)
                for v in vL0:
                    num = mg.count(self.__dbName, self.__collectionName, countFilter={atName: v})
                    logger.debug("%s value %s (%d)", atName, v, num)
                    self.assertGreaterEqual(num, 100)
                #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSchemaValidation1(self):
        """Test case -  create collection and insert data with schema validation (ext. schema assignment)"""

        #  Example of a Mongo flavor of JsonSchema
        vexpr = {"$jsonSchema": self.__mongoSchema}

        query = [("collMod", self.__collectionName), ("validator", vexpr), ("validationLevel", "moderate")]
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
                logger.info("rId is %r", rId)
                self.assertEqual(rId, None)
                #
                s2 = unescapeXmlCharRef(" &quot; &Phi; &Psi; &alpha; &#xa3;  &#8453;  &#9734;  &#120171;")
                dObj = {"strField1": "test value", "strField2": s2, "intField1": 50, "enumField1": "v3", "dblField1": 100.1}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                logger.info("rId is %r", rId)
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, "_id", rId)
                logger.debug("Return Object %s", pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSchemaValidation2(self):
        """Test case -  create collection and insert data with schema validation (strict mode) (integrated schema assignment)"""
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
                logger.info("rId is %r", rId)
                dtVal = dateutil.parser.parse("2018-01-30 12:01")
                logger.debug("date value is %r", dtVal)
                s2 = unescapeXmlCharRef(" &quot; &Phi; &Psi; &alpha; &#xa3;  &#8453;  &#9734;  &#120171;")
                dObj = {"strField1": "test value", "strField2": s2, "intField1": 50, "enumField1": "v3", "dblField1": 100.1, "dateField1": dtVal}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                logger.info("rId is %r", rId)
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, "_id", rId)
                logger.debug("Return Object %s", pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSchemaValidation3(self):
        """Test case -  create collection and insert data with schema validation (warn mode) (integrated schema assignment)"""
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
                ok = mg.createCollection(self.__dbName, self.__collectionName, overWrite=True, bsonSchema=self.__mongoSchema, validationAction="warn")
                self.assertTrue(ok)
                ok = mg.databaseExists(self.__dbName)
                self.assertTrue(ok)
                ok = mg.collectionExists(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                #
                dObj = {"x": 1}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                logger.info("rId is %r", rId)
                self.assertNotEqual(rId, None)
                #
                s2 = unescapeXmlCharRef(" &quot; &Phi; &Psi; &alpha; &#xa3;  &#8453;  &#9734;  &#120171;")
                dObj = {"strField1": "test value", "strField2": s2, "intField1": 50, "enumField1": "v3a", "dblField1": 100.1}
                rId = mg.insert(self.__dbName, self.__collectionName, dObj)
                self.assertNotEqual(rId, None)
                logger.info("rId is %r", rId)
                rObj = mg.fetchOne(self.__dbName, self.__collectionName, "_id", rId)
                logger.debug("Return Object %s", pprint.pformat(rObj))
                self.assertEqual(len(dObj), len(rObj))
                self.assertEqual(dObj, rObj)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testFetchProjection(self):
        """Test case -  fetch()/fetchBatched() with a projection document (projectionD), incl. expressions and paging."""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                ok = mg.createCollection(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                ok = mg.createIndex(self.__dbName, self.__collectionName, keyList=["DOC_NUM"], indexName="primary", indexType="DESCENDING", uniqueFlag=True)
                self.assertTrue(ok)
                nDocs = 25
                dList = []
                for ii in range(nDocs):
                    dList.append(
                        {
                            "DOC_NUM": ii,
                            "DOC_ID": "DOC_%d" % ii,
                            "container": {"entry_id": "ENTRY_%d" % (ii // 3)},
                            "features": [{"type": "A", "values": [ii, ii + 1]}, {"type": "B", "values": [ii]}, {"type": "C", "values": []}],
                        }
                    )
                rIdL = mg.insertList(self.__dbName, self.__collectionName, dList, keyNames=["DOC_ID"], salvage=True)
                self.assertEqual(len(rIdL), nDocs)
                filterD = {"$filter": {"input": "$features", "as": "f", "cond": {"$in": ["$$f.type", ["A", "C"]]}}}
                #
                # Expression projection, '_id' excluded (paging adds and strips it), several batches
                projD = {"_id": 0, "DOC_ID": 1, "entry_id": "$container.entry_id", "features": filterD}
                batchL = list(mg.fetchBatched(self.__dbName, self.__collectionName, None, projectionD=projD, batchSize=10))
                self.assertEqual([len(bL) for bL in batchL], [10, 10, 5])
                rL = [dD for bL in batchL for dD in bL]
                self.assertEqual([dD["DOC_ID"] for dD in rL], ["DOC_%d" % ii for ii in range(nDocs)])  # _id (insert) order
                for ii, dD in enumerate(rL):
                    self.assertEqual(set(dD.keys()), {"DOC_ID", "entry_id", "features"})
                    self.assertEqual(dD["entry_id"], "ENTRY_%d" % (ii // 3))
                    self.assertEqual([fD["type"] for fD in dD["features"]], ["A", "C"])
                    self.assertEqual(dD["features"][0]["values"], [ii, ii + 1])
                #
                # Query constraint plus a non-'_id' paging key that the projection leaves out (added and stripped)
                rL = mg.fetch(self.__dbName, self.__collectionName, None, queryD={"DOC_NUM": {"$gte": 5}}, projectionD={"_id": 0, "features": filterD}, batchSize=4, sortKey="DOC_NUM")
                self.assertEqual(len(rL), nDocs - 5)
                self.assertTrue(all(set(dD.keys()) == {"features"} for dD in rL))
                self.assertEqual([dD["features"][0]["values"][0] for dD in rL], list(range(5, nDocs)))
                #
                # Inclusion projection without '_id': '_id' is returned as usual
                rL = mg.fetch(self.__dbName, self.__collectionName, None, projectionD={"DOC_ID": 1}, batchSize=7)
                self.assertEqual(len(rL), nDocs)
                self.assertTrue(all(set(dD.keys()) == {"_id", "DOC_ID"} for dD in rL))
                #
                # Exclusion projection
                rL = mg.fetch(self.__dbName, self.__collectionName, None, projectionD={"features": 0, "container": 0}, batchSize=7)
                self.assertEqual(len(rL), nDocs)
                self.assertTrue(all(set(dD.keys()) == {"_id", "DOC_NUM", "DOC_ID"} for dD in rL))
                #
                # selectL and projectionD are mutually exclusive
                with self.assertRaises(ValueError):
                    list(mg.fetchBatched(self.__dbName, self.__collectionName, ["DOC_ID"], projectionD={"DOC_ID": 1}))
                self.assertIsNone(mg.fetch(self.__dbName, self.__collectionName, ["DOC_ID"], projectionD={"DOC_ID": 1}))
                #
                ok = mg.dropCollection(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testFetchOptions(self):
        """Test case -  paged-fetch/count defaults read from the configuration (MongoDbUtil(client, cfgOb=...))."""
        try:
            defaultD = {"batchSize": 10000, "batchTimeoutSeconds": 600, "maxRetries": 3, "retryDelaySeconds": 10, "countTimeoutSeconds": 300}
            # No configuration object: defaults
            self.assertEqual(MongoDbUtil(None).getFetchOptions(), defaultD)
            # The example configuration lists the defaults explicitly
            self.assertEqual(MongoDbUtil(None, cfgOb=self.__cfgOb).getFetchOptions(), defaultD)
            #
            # Configured values (others keep their defaults); a value that isn't a number raises
            cfgOb = ConfigUtil(configPath=self.__configPath, defaultSectionName=self.__configName)
            sD = cfgOb.exportConfig(self.__configName)
            for key in ["MONGO_DB_FETCH_RETRY_DELAY_SECONDS", "MONGO_DB_COUNT_TIMEOUT_SECONDS"]:
                sD.pop(key, None)
            sD.update({"MONGO_DB_FETCH_BATCH_SIZE": 4, "MONGO_DB_FETCH_BATCH_TIMEOUT_SECONDS": "120", "MONGO_DB_FETCH_MAX_RETRIES": "2"})
            cfgOb.importConfig({self.__configName: sD})
            badCfgOb = ConfigUtil(configPath=self.__configPath, defaultSectionName=self.__configName)
            badCfgOb.importConfig({self.__configName: dict(sD, MONGO_DB_FETCH_RETRY_DELAY_SECONDS="abc")})
            with self.assertRaises(ValueError):
                MongoDbUtil(None, cfgOb=badCfgOb)
            with Connection(cfgOb=cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client, cfgOb=cfgOb)
                self.assertEqual(mg.getFetchOptions(), dict(defaultD, batchSize=4, batchTimeoutSeconds=120.0, maxRetries=2))
                ok = mg.createCollection(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
                rIdL = mg.insertList(self.__dbName, self.__collectionName, [{"DOC_ID": "DOC_%d" % ii} for ii in range(10)], keyNames=["DOC_ID"])
                self.assertEqual(len(rIdL), 10)
                # configured batch size, an explicit argument overriding it, and an instance without cfgOb
                batchL = list(mg.fetchBatched(self.__dbName, self.__collectionName, ["DOC_ID"]))
                self.assertEqual([len(bL) for bL in batchL], [4, 4, 2])
                batchL = list(mg.fetchBatched(self.__dbName, self.__collectionName, ["DOC_ID"], batchSize=5))
                self.assertEqual([len(bL) for bL in batchL], [5, 5])
                batchL = list(MongoDbUtil(client).fetchBatched(self.__dbName, self.__collectionName, ["DOC_ID"]))
                self.assertEqual([len(bL) for bL in batchL], [10])
                self.assertEqual(mg.count(self.__dbName, self.__collectionName), 10)
                ok = mg.dropCollection(self.__dbName, self.__collectionName)
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def suiteFetch():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MongoDbUtilTests("testFetchProjection"))
    suiteSelect.addTest(MongoDbUtilTests("testFetchOptions"))
    return suiteSelect


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


if __name__ == "__main__":

    mySuite = suiteOps()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteReplace()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteIndex()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteInsert()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteValidation()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteFetch()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
