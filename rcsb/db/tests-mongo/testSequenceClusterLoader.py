##
# File:    SequenceClusterLoaderTests.py
# Author:  J. Westbrook
# Date:    25-Jun-2018
# Version: 0.001
#
# Updates:
#  6-Jul-2018 jdw rename methods and incorporate provenance details -
# 28-Oct-2018 jdw adjustments for new configuration organization
#
#
##
"""
Tests for ETL on sequence cluster data set following external schema definitions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.ClusterDataPrep import ClusterDataPrep
from rcsb.db.utils.ProvenanceProvider import ProvenanceProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SequenceClusterLoaderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(SequenceClusterLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        # self.__cfgOb.dump()
        self.__resourceName = "MONGO_DB"
        self.__failedFilePath = os.path.join(HERE, "test-output", "failed-list.txt")
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__documentLimit = 1000
        #
        # sample data set
        self.__dataSetId = "2018_23"
        self.__pathClusterData = self.__cfgOb.getPath("RCSB_SEQUENCE_CLUSTER_DATA_PATH", sectionName=configName)
        self.__levels = ["100", "95", "90", "70", "50", "30"]
        #
        self.__workPath = os.path.join(HERE, "test-output")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__pathSaveStyleCif = os.path.join(HERE, "test-output", "cluster-data-cif.json")
        self.__pathSaveStyleDocSequence = os.path.join(HERE, "test-output", "cluster-data-doc-sequence.json")
        self.__pathSaveStyleDocCluster = os.path.join(HERE, "test-output", "cluster-data-doc-cluster.json")
        #
        self.__entitySchemaName = "rcsb_entity_sequence_cluster_list"
        self.__clusterSchemaName = "rcsb_entity_sequence_cluster_identifer_list"
        self.__provKeyName = "rcsb_entity_sequence_cluster_prov"
        #
        #
        self.__startTime = time.monotonic()
        logger.debug("Starting %s now", self.id())

    def tearDown(self):
        logger.debug("Completed %s in %.3f s", self.id(), time.monotonic() - self.__startTime)

    def __fetchProvenance(self):
        """Test case for fetching a provenance dictionary content."""
        try:
            provU = ProvenanceProvider(self.__cfgOb, self.__cachePath)
            pD = provU.fetch()
            return pD[self.__provKeyName] if self.__provKeyName in pD else {}
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def __testExtract(self, dataSetId, dataLocator, levels):
        """Test extraction on an example sequence cluster data set."""
        try:
            cdp = ClusterDataPrep(workPath=self.__workPath, entitySchemaName=self.__entitySchemaName, clusterSchemaName=self.__clusterSchemaName)
            cifD, docBySequenceD, docByClusterD = cdp.extract(dataSetId, clusterSetLocator=dataLocator, levels=levels, clusterType="entity")
            self.assertEqual(len(cifD), 1)
            self.assertEqual(len(docBySequenceD), 1)
            self.assertEqual(len(docByClusterD), 1)
            return docBySequenceD, docByClusterD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLoadCluster(self):
        """Test case - load example sequence cluster document data"""
        try:
            dl = DocumentLoader(
                self.__cfgOb,
                self.__cachePath,
                self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                documentLimit=self.__documentLimit,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
            )
            #
            docBySequenceD, docByClusterD = self.__testExtract(dataSetId=self.__dataSetId, dataLocator=self.__pathClusterData, levels=self.__levels)
            #
            dList = docBySequenceD[self.__entitySchemaName]
            ok = dl.load("sequence_clusters", "entity_members", loadType="full", documentList=dList, indexAttributeList=["data_set_id", "entry_id", "entity_id"], keyNames=None)
            self.assertTrue(ok)
            dList = docByClusterD[self.__clusterSchemaName]
            ok = dl.load("sequence_clusters", "cluster_members", loadType="full", documentList=dList, indexAttributeList=["data_set_id", "identity", "cluster_id"], keyNames=None)
            self.assertTrue(ok)
            pD = self.__fetchProvenance()
            ok = dl.load("sequence_clusters", "cluster_provenance", loadType="full", documentList=[pD], indexAttributeList=None, keyNames=None)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def clusterLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SequenceClusterLoaderTests("testLoadCluster"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = clusterLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
