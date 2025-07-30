##
#
# File:    testDocumentLoader.py
# Author:  D. Piehl
# Date:    15-Jul-2025
# Version: 0.001
#
# Updates:
##
"""
Test cases for MongoDB document laoder client operations.
  - Load a set of documents with two indexed fields
  - Check that the indexes were created OK

"""
__docformat__ = "restructuredtext en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.db.mongo.DocumentLoader import DocumentLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DocumentLoaderTests(unittest.TestCase):
    def setUp(self):
        self.__dbName = "test_database"
        self.__collectionName = "test_collection_2"
        #
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName)
        self.__resourceName = "MONGO_DB"
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        #
        self.__testDocs = [
            {
                "id": "1",
                "name": "Mainly Alpha",
                "depth": 0
            },
            {
                "id": "1.10.540",
                "name": "Butyryl-Coa Dehydrogenase, subunit A; domain 1",
                "parents": [
                    "1.10"
                ],
                "depth": 2
            }
        ]
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testLoadDocuments(self):
        """Test case -  load documents"""
        try:
            dl = DocumentLoader(
                self.__cfgOb,
                self.__cachePath,
                self.__resourceName,
                numProc=1,
                chunkSize=10,
                documentLimit=None,
                verbose=True,
                readBackCheck=False,
            )
            indAtDictList = [
                {
                    "ATTRIBUTE_NAMES": ["id"],
                    "INDEX_NAME": "index_1",
                    "UNIQUE": True
                },
                {
                    "ATTRIBUTE_NAMES": ["parents"],
                    "INDEX_NAME": "index_2"
                }
            ]
            ok = dl.load(self.__dbName, self.__collectionName, loadType="full", documentList=self.__testDocs, schemaLevel=None, indexDL=indAtDictList)
            logger.info("Document loader status %r", ok)
            self.assertTrue(ok)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                idxL = mg.getCollectionIndexes(self.__dbName, self.__collectionName)
            logger.info("Indexes for %s.%s: %r", self.__dbName, self.__collectionName, idxL)
            self.assertGreaterEqual(len(idxL), 3)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def suiteOps():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DocumentLoaderTests("testLoadDocuments"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = suiteOps()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
