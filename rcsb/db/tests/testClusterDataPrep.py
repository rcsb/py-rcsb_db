##
# File:    ClusterDataPrepTests.py
# Author:  J. Westbrook
# Date:    24-Jun-2018
# Version: 0.001
#
# Update:
#
##
"""
Tests for data sequence cluster data sets.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.processors.ClusterDataPrep import ClusterDataPrep
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ClusterDataPrepTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        pathConfig = os.path.join(mockTopPath, "config", "dbload-setup-example.yml")
        #
        configName = "site_info"
        cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__pathClusterData = cfgOb.getPath("RCSB_SEQUENCE_CLUSTER_DATA_PATH", sectionName=configName)
        # sample data set
        self.__dataSetId = "2018_23"

        self.__levels = ["100", "95", "90", "70", "50", "30"]
        #
        self.__workPath = os.path.join(HERE, "test-output")
        self.__pathSaveStyleCif = os.path.join(HERE, "test-output", "cluster-data-cif.json")
        self.__pathSaveStyleDocSequence = os.path.join(HERE, "test-output", "cluster-data-doc-sequence.json")
        self.__pathSaveStyleDocCluster = os.path.join(HERE, "test-output", "cluster-data-doc-cluster.json")
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testExtract(self):
        """ Test extraction on an example sequence cluster data set.
        """
        try:
            cdp = ClusterDataPrep(workPath=self.__workPath)
            cifD, docBySequenceD, docByClusterD = cdp.extract(self.__dataSetId, clusterSetLocator=self.__pathClusterData, levels=self.__levels, clusterType="entity")
            self.assertEqual(len(cifD), 1)
            self.assertEqual(len(docBySequenceD), 1)
            self.assertEqual(len(docByClusterD), 1)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtractAndSerialize(self):
        """ Test extraction on an example sequence cluster data set.
        """
        try:
            cdp = ClusterDataPrep(workPath=self.__workPath)
            cifD, docBySequenceD, docByClusterD = cdp.extract(self.__dataSetId, clusterSetLocator=self.__pathClusterData, levels=self.__levels, clusterType="entity")
            mU = MarshalUtil(workPath=self.__workPath)
            ok = mU.doExport(self.__pathSaveStyleCif, cifD, fmt="json", indent=3)
            self.assertTrue(ok)
            ok = mU.doExport(self.__pathSaveStyleDocSequence, docBySequenceD, fmt="json", indent=3)
            self.assertTrue(ok)
            ok = mU.doExport(self.__pathSaveStyleDocCluster, docByClusterD, fmt="json", indent=3)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def prepSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ClusterDataPrepTests("testExtract"))
    suiteSelect.addTest(ClusterDataPrepTests("testExtractAndSerialize"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = prepSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
