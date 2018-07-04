##
# File:    DocumentLoaderTests.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#   22-Mar-2018 jdw  Revise all tests
#   23-Mar-2018 jdw  Add reload test cases
#   27-Mar-2018 jdw  Update configuration handling and mocking
#    4-Apr-2018 jdw  Add size pruning tests
#
##
"""
Tests for creating and loading MongoDb using BIRD, CCD and PDBx/mmCIF data files
and following external schema definitions.

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

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.loaders.ClusterDataPrep import ClusterDataPrep
from rcsb_db.mongo.DocumentLoader import DocumentLoader
from rcsb_db.utils.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


class DocumentLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(DocumentLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, 'rcsb_db', 'data')
        configPath = os.path.join(TOPDIR, 'rcsb_db', 'data', 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName, mockTopPath=mockTopPath)
        # self.__cfgOb.dump()
        self.__resourceName = "MONGO_DB"
        self.__failedFilePath = os.path.join(HERE, 'test-output', 'failed-list.txt')
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__documentLimit = 1000
        #
        # sample data set
        self.__dataSetId = '2018_23'
        self.__pathClusterData = os.path.join(TOPDIR, 'rcsb_db', 'data', 'cluster_data', 'mmseqs-20180608')
        self.__levels = ['100', '95', '90', '70', '50', '30']
        #
        self.__workPath = os.path.join(HERE, 'test-output')
        self.__pathSaveStyleCif = os.path.join(HERE, 'test-output', 'cluster-data-cif.json')
        self.__pathSaveStyleDocSequence = os.path.join(HERE, 'test-output', 'cluster-data-doc-sequence.json')
        self.__pathSaveStyleDocCluster = os.path.join(HERE, 'test-output', 'cluster-data-doc-cluster.json')
        #
        self.__entitySchemaName = 'rcsb_sequence_cluster_entity_list'
        self.__clusterSchemaName = 'rcsb_sequence_cluster_identifer_list'
        #
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def __testExtract(self, dataSetId, dataLocator, levels):
        """ Test extraction on an example sequence cluster data set.
        """
        try:
            cdp = ClusterDataPrep(workPath=self.__workPath, entitySchemaName=self.__entitySchemaName,
                                  clusterSchemaName=self.__clusterSchemaName)
            cifD, docBySequenceD, docByClusterD = cdp.extract(dataSetId, clusterSetLocator=dataLocator, levels=levels,
                                                              clusterType='entity')
            self.assertEqual(len(cifD), 1)
            self.assertEqual(len(docBySequenceD), 1)
            self.assertEqual(len(docByClusterD), 1)
            return docBySequenceD, docByClusterD
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadCluster(self):
        """ Test case - load example sequence cluster document data
        """
        try:
            dl = DocumentLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                documentLimit=self.__documentLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            #
            docBySequenceD, docByClusterD = self.__testExtract(dataSetId=self.__dataSetId, dataLocator=self.__pathClusterData, levels=self.__levels)
            #
            dList = docBySequenceD[self.__entitySchemaName]
            ok = dl.load('sequence_clusters', 'entity_members_v0_1', loadType='full', documentList=dList, indexAttributeList=['data_set_id','entry_id','entity_id'], keyName=None)
            self.assertTrue(ok)
            dList = docByClusterD[self.__clusterSchemaName]
            ok = dl.load('sequence_clusters', 'cluster_members_v0_1', loadType='full', documentList=dList, indexAttributeList=['data_set_id','identity','cluster_id'], keyName=None)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def clusterLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DocumentLoaderTests("testLoadCluster"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = clusterLoadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
