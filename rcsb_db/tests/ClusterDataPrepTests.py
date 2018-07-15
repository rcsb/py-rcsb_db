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

from rcsb_db.io.MarshalUtil import MarshalUtil
from rcsb_db.processors.ClusterDataPrep import ClusterDataPrep

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ClusterDataPrepTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
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
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testExtract(self):
        """ Test extraction on an example sequence cluster data set.
        """
        try:
            cdp = ClusterDataPrep(workPath=self.__workPath)
            cifD, docBySequenceD, docByClusterD = cdp.extract(self.__dataSetId, clusterSetLocator=self.__pathClusterData, levels=self.__levels, clusterType='entity')
            self.assertEqual(len(cifD), 1)
            self.assertEqual(len(docBySequenceD), 1)
            self.assertEqual(len(docByClusterD), 1)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testExtractAndSerialize(self):
        """ Test extraction on an example sequence cluster data set.
        """
        try:
            cdp = ClusterDataPrep(workPath=self.__workPath)
            cifD, docBySequenceD, docByClusterD = cdp.extract(self.__dataSetId, clusterSetLocator=self.__pathClusterData, levels=self.__levels, clusterType='entity')
            mU = MarshalUtil(workPath=self.__workPath)
            ok = mU.doExport(self.__pathSaveStyleCif, cifD, format="json", indent=3)
            self.assertTrue(ok)
            ok = mU.doExport(self.__pathSaveStyleDocSequence, docBySequenceD, format="json", indent=3)
            self.assertTrue(ok)
            ok = mU.doExport(self.__pathSaveStyleDocCluster, docByClusterD, format="json", indent=3)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def prepSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ClusterDataPrepTests("testExtract"))
    suiteSelect.addTest(ClusterDataPrepTests("testExtractAndSerialize"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = prepSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
