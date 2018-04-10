##
# File:    SchemaDefDataPrepTests.py
# Author:  J. Westbrook
# Date:    13-Mar-2018
# Version: 0.001
#
# Updates:
#
#   9-Apr-2018 jdw add new schema and file management
#   9-Apr-2019 jdw add pdbx prep examples -
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
import json

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
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
from rcsb_db.utils.ConfigUtil import ConfigUtil
from rcsb_db.utils.ContentTypeUtil import ContentTypeUtil


class SchemaDefDataPrepTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefDataPrepTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__verbose = True

    def setUp(self):
        self.__numProc = 2
        self.__fileLimit = 200
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data")
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)

        self.__ctU = ContentTypeUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
        self.__birdRepoPath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        #
        self.__fTypeRow = "drop-empty-attributes|drop-empty-tables|skip-max-width"
        self.__fTypeCol = "drop-empty-tables|skip-max-width"
        self.__birdMockLen = 3
        self.__pdbxMockLen = 8
        self.__verbose = True
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testPrepBirdDocumentsFromFiles(self):
        """Test case -  create loadable BIRD data from files
        """
        try:
            inputPathList = self.__ctU.getPathList(contentType='bird')
            bsd, _, _, _ = self.__ctU.getSchemaInfo(contentType='bird')
            #
            sdp = SchemaDefDataPrep(schemaDefObj=bsd, verbose=self.__verbose)
            #
            logger.debug("Length of path list %d\n" % len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__birdMockLen)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name", filterType=self.__fTypeRow)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "bird-file-prep-rowwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name_with_cardinality",
                                                                                  filterType=self.__fTypeRow, documentSelectors=["BIRD_PUBLIC_RELEASE"])
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 1)
            with open(os.path.join(HERE, "test-output", "bird-file-prep-rowwise-by-name-with-cardinality.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="columnwise_by_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "bird-file-prep-columnwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_no_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "bird-file-prep-rowwise-no-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrepBirdDocumentsFromContainers(self):
        """Test case -  create loadable BIRD data from containers
        """

        try:
            # self.testBirdPathList()
            inputPathList = self.__ctU.getPathList(contentType='bird')
            bsd, _, _, _ = self.__ctU.getSchemaInfo(contentType='bird')
            #
            sdp = SchemaDefDataPrep(schemaDefObj=bsd, verbose=self.__verbose)
            containerList = sdp.getContainerList(inputPathList, filterType="none")
            self.assertGreaterEqual(len(containerList), self.__birdMockLen)
            #
            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType="rowwise_by_name", filterType=self.__fTypeRow)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "bird-container-prep-rowwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(
                containerList, styleType="rowwise_by_name_with_cardinality", filterType=self.__fTypeRow, documentSelectors=["BIRD_PUBLIC_RELEASE"])
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 1)
            with open(os.path.join(HERE, "test-output", "bird-container-prep-rowwise-by-name-with-cardinality.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType="columnwise_by_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "bird-container-prep-columnwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType="rowwise_no_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "bird-container-prep-rowwise-no-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrepPdbxDocumentsFromFiles(self):
        """Test case -  create loadable PDBx data from files
        """
        try:
            inputPathList = self.__ctU.getPathList(contentType='pdbx')
            sd, _, _, _ = self.__ctU.getSchemaInfo(contentType='pdbx')
            #
            sdp = SchemaDefDataPrep(schemaDefObj=sd, verbose=self.__verbose)
            #
            logger.debug("Length of path list %d\n" % len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__pdbxMockLen)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name", filterType=self.__fTypeRow)
            self.assertGreaterEqual(len(tableDataDictList), self.__pdbxMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__pdbxMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "pdbx-file-prep-rowwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name_with_cardinality",
                                                                                  filterType=self.__fTypeRow, documentSelectors=["PDBX_ENTRY_PUBLIC_RELEASE"])
            self.assertGreaterEqual(len(tableDataDictList), self.__pdbxMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__pdbxMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "pdbx-file-prep-rowwise-by-name-with-cardinality.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="columnwise_by_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__pdbxMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__pdbxMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "pdbx-file-prep-columnwise-by-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_no_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__pdbxMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__pdbxMockLen)
            self.assertEqual(len(rejectList), 0)
            with open(os.path.join(HERE, "test-output", "pdbx-file-prep-rowwise-no-name.json"), 'w') as ofh:
                ofh.write(json.dumps(tableDataDictList, indent=3))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def prepBirdSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDataPrepTests("testPrepBirdDocumentsFromFiles"))
    suiteSelect.addTest(SchemaDefDataPrepTests("testPrepBirdDocumentsFromContainers"))
    return suiteSelect


def prepPdbxSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDataPrepTests("testPrepPdbxDocumentsFromFiles"))
    return suiteSelect


if __name__ == '__main__':
    #
    if False:
        mySuite = prepBirdSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    if True:
        mySuite = prepPdbxSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
