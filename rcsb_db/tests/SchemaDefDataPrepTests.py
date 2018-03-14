##
# File:    SchemaDefDataPrepTests.py
# Author:  J. Westbrook
# Date:    13-Mar-2018
# Version: 0.001
#
# Updates:
#
#
#
##
"""
Tests for preparing loadable data based on external schema definition.

         No specific database conection depedencies -

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import os
import time
import unittest
import pprint
import json

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
#
from rcsb_db.loaders.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef

from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo


class BirdLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(BirdLoaderTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__verbose = True

    def setUp(self):
        self.__databaseName = 'prdv4'
        self.__birdCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        #
        self.__birdMockLen = 3
        self.__verbose = True
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testPrdPathList(self):
        """Test case -  get the path list of PRD definitions in the CVS repository.
        """
        try:
            prd = PdbxPrdIo(verbose=self.__verbose)
            prd.setCachePath(self.__birdCachePath)
            self.__loadPathList = prd.makeDefinitionPathList()
            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            self.assertGreaterEqual(len(self.__loadPathList), self.__birdMockLen)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrepBirdDocumentsFromFiles(self):
        """Test case -  create loadable BIRD data from files
        """

        try:
            self.testPrdPathList()
            bsd = BirdSchemaDef(convertNames=True)
            sdp = SchemaDefDataPrep(schemaDefObj=bsd, verbose=self.__verbose)
            #
            logger.debug("Length of path list %d\n" % len(self.__loadPathList))
            self.assertGreaterEqual(len(self.__loadPathList), self.__birdMockLen)

            tableDataDictList, containerNameList = sdp.fetchDocuments(self.__loadPathList, styleType="rowwise_by_name")
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            with open(os.path.join(HERE, "test-output", "bird-file-prep-rowwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList = sdp.fetchDocuments(self.__loadPathList, styleType="columnwise_by_name")
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            with open(os.path.join(HERE, "test-output", "bird-file-prep-columnwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList = sdp.fetchDocuments(self.__loadPathList, styleType="rowwise_no_name")
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            with open(os.path.join(HERE, "test-output", "bird-file-prep-rowwise-no-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrepBirdDocumentsFromContainers(self):
        """Test case -  create loadable BIRD data from containers
        """

        try:
            self.testPrdPathList()
            bsd = BirdSchemaDef(convertNames=True)

            prd = PdbxPrdIo(verbose=self.__verbose)
            prd.setCachePath(self.__birdCachePath)
            self.__pathList = prd.makeDefinitionPathList()
            for pth in self.__pathList:
                prd.setFilePath(pth)
            containerList = prd.getCurrentContainerList()
            self.assertGreaterEqual(len(containerList), self.__birdMockLen)
            #
            sdp = SchemaDefDataPrep(schemaDefObj=bsd, verbose=self.__verbose)
            #
            tableDataDictList, containerNameList = sdp.processDocuments(containerList, styleType="rowwise_by_name")
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            with open(os.path.join(HERE, "test-output", "bird-container-prep-rowwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList = sdp.processDocuments(containerList, styleType="columnwise_by_name")
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            with open(os.path.join(HERE, "test-output", "bird-container-prep-columnwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList = sdp.processDocuments(containerList, styleType="rowwise_no_name")
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            with open(os.path.join(HERE, "test-output", "bird-container-prep-rowwise-no-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

def prepBirdSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(BirdLoaderTests("testPrepBirdDocumentsFromFiles"))
    suiteSelect.addTest(BirdLoaderTests("testPrepBirdDocumentsFromContainers"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = prepBirdSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

