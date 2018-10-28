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
#  11-Apr-2018 jdw integrate DataTransformFactory()
#  19-Jun-2018 jdw Add MarshalUtil() for for serialization
#  24-Jul-2018 jdw Update selection filter names.
#   5-Sep-2018 jdw Add tests for core collections including executing dictionary methods.
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


import logging
import os
import time
import unittest

from rcsb.db.define.DictMethodRunner import DictMethodRunner
from rcsb.db.helpers.DictMethodRunnerHelper import DictMethodRunnerHelper
from rcsb.db.processors.DataTransformFactory import DataTransformFactory
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefDataPrepTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefDataPrepTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__verbose = True

    def setUp(self):
        self.__numProc = 2
        self.__fileLimit = 200
        mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        self.__workPath = os.path.join(HERE, 'test-output')
        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.yml')
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__mU = MarshalUtil(workPath=self.__workPath)

        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath)
        self.__birdRepoPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'MOCK_BIRD_REPO')
        #
        self.__fTypeRow = "drop-empty-attributes|drop-empty-tables|skip-max-width|convert-iterables|normalize-enums"
        self.__fTypeCol = "drop-empty-tables|skip-max-width|convert-iterables|normalize-enums"
        self.__chemCompMockLen = 4
        self.__birdMockLen = 4
        self.__pdbxMockLen = 8
        self.__verbose = True
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'mmcif_pdbx_v5_next.dic')
        self.__pathRcsbDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'rcsb_mmcif_ext_v1.dic')
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testPrepChemCompDocumentsFromFiles(self):
        """Test case -  create loadable chem_comp data from files
        """
        try:
            inputPathList = self.__schU.getPathList(contentType='chem_comp')
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType='chem_comp')
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            #
            #
            logger.debug("Length of path list %d\n" % len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__birdMockLen)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name", filterType=self.__fTypeRow)
            self.assertGreaterEqual(len(tableDataDictList), self.__chemCompMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__chemCompMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "chem-comp-file-prep-rowwise-by-name.json"), tableDataDictList, format="json", indent=3)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name_with_cardinality",
                                                                                  filterType=self.__fTypeRow, dataSelectors=["PUBLIC_RELEASE"])
            self.assertGreaterEqual(len(tableDataDictList), self.__chemCompMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__chemCompMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "chem-comp-file-prep-rowwise-by-name-with-cardinality.json"), tableDataDictList, format="json", indent=3)

            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeCol)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="columnwise_by_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__chemCompMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__chemCompMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "chem-comp-file-prep-columnwise-by-name.json"), tableDataDictList, format="json", indent=3)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_no_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__chemCompMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__chemCompMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "chem-comp-file-prep-rowwise-no-name.json"), tableDataDictList, format="json", indent=3)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrepBirdDocumentsFromFiles(self):
        """Test case -  create loadable BIRD data from files
        """
        try:
            inputPathList = self.__schU.getPathList(contentType='bird')
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType='bird')
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            #
            #
            logger.debug("Length of path list %d\n" % len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__birdMockLen)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name", filterType=self.__fTypeRow)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "bird-file-prep-rowwise-by-name.json"), tableDataDictList, format="json", indent=3)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name_with_cardinality",
                                                                                  filterType=self.__fTypeRow, dataSelectors=["PUBLIC_RELEASE"])
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen - 1)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen - 1)
            self.assertEqual(len(rejectList), 1)
            self.__mU.doExport(os.path.join(HERE, "test-output", "bird-file-prep-rowwise-by-name-with-cardinality.json"), tableDataDictList, format="json", indent=3)

            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeCol)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="columnwise_by_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "bird-file-prep-columnwise-by-name.json"), tableDataDictList, format="json", indent=3)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_no_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "bird-file-prep-rowwise-no-name.json"), tableDataDictList, format="json", indent=3)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrepBirdDocumentsFromContainers(self):
        """Test case -  create loadable BIRD data from PDBx/mmCIF containers
        """

        try:
            inputPathList = self.__schU.getPathList(contentType='bird')
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType='bird')
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            #
            containerList = sdp.getContainerList(inputPathList, filterType="none")
            self.assertGreaterEqual(len(containerList), self.__birdMockLen)
            #
            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType="rowwise_by_name", filterType=self.__fTypeRow)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "bird-container-prep-rowwise-by-name.json"), tableDataDictList, format="json", indent=3)

            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(
                containerList, styleType="rowwise_by_name_with_cardinality", filterType=self.__fTypeRow, dataSelectors=["PUBLIC_RELEASE"])
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen - 1)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen - 1)
            self.assertEqual(len(rejectList), 1)
            self.__mU.doExport(os.path.join(HERE, "test-output", "bird-container-prep-rowwise-by-name-with-cardinality.json"), tableDataDictList, format="json", indent=3)

            #
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeCol)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType="columnwise_by_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "bird-container-prep-columnwise-by-name.json"), tableDataDictList, format="json", indent=3)

            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType="rowwise_no_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__birdMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__birdMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "bird-container-prep-rowwise-no-name.json"), tableDataDictList, format="json", indent=3)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrepPdbxDocumentsFromFiles(self):
        """Test case -  create loadable PDBx data from repository files (pdbx_core)
        """
        try:
            inputPathList = self.__schU.getPathList(contentType='pdbx_core')
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType='pdbx_core')
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            #
            logger.debug("Length of path list %d\n" % len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__pdbxMockLen)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name", filterType=self.__fTypeRow)
            self.assertGreaterEqual(len(tableDataDictList), self.__pdbxMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__pdbxMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "pdbx-file-prep-rowwise-by-name.json"), tableDataDictList, format="json", indent=3)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_by_name_with_cardinality",
                                                                                  filterType=self.__fTypeRow, dataSelectors=["PUBLIC_RELEASE"])
            self.assertGreaterEqual(len(tableDataDictList), self.__pdbxMockLen - 1)
            self.assertGreaterEqual(len(containerNameList), self.__pdbxMockLen - 1)
            self.assertEqual(len(rejectList), 1)
            self.__mU.doExport(os.path.join(HERE, "test-output", "pdbx-file-prep-rowwise-by-name-with-cardinality.json"), tableDataDictList, format="json", indent=3)

            # ---------------------  change global filters ----------------------------
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeCol)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="columnwise_by_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__pdbxMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__pdbxMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "pdbx-file-prep-columnwise-by-name.json"), tableDataDictList, format="json", indent=3)

            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType="rowwise_no_name", filterType=self.__fTypeCol)
            self.assertGreaterEqual(len(tableDataDictList), self.__pdbxMockLen)
            self.assertGreaterEqual(len(containerNameList), self.__pdbxMockLen)
            self.assertEqual(len(rejectList), 0)
            self.__mU.doExport(os.path.join(HERE, "test-output", "pdbx-file-prep-rowwise-no-name.json"), tableDataDictList, format="json", indent=3)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testPrepPdbxDocumentsFromContainers(self):
        """Test case -  create and save loadable content for core PDBx collections including dictionary methods and reusing PDBx/mmCIF container data.
        """
        try:
            styleType = "rowwise_by_name_with_cardinality"
            schemaName = 'pdbx_core'
            inputPathList = self.__schU.getPathList(contentType=schemaName)
            sd, _, collectionNameList, _ = self.__schU.getSchemaInfo(contentType=schemaName)
            #
            dH = DictMethodRunnerHelper()
            dmh = DictMethodRunner(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile], methodHelper=dH)

            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            containerList = sdp.getContainerList(inputPathList)
            for container in containerList:
                cName = container.getName()
                logger.debug("Processing container %s" % cName)
                dmh.apply(container)
            #
            for collectionName in collectionNameList:
                logger.debug("inputPathList %r" % inputPathList)
                tableIdExcludeList = sd.getCollectionExcluded(collectionName)
                tableIdIncludeList = sd.getCollectionSelected(collectionName)
                sliceFilter = sd.getCollectionSliceFilter(collectionName)
                sdp.setSchemaIdExcludeList(tableIdExcludeList)
                sdp.setSchemaIdIncludeList(tableIdIncludeList)
                #
                tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType=styleType,
                                                                                        filterType=self.__fTypeRow, dataSelectors=["PUBLIC_RELEASE"],
                                                                                        sliceFilter=sliceFilter)

                fp = os.path.join(HERE, "test-output", "sdp-export-%s-%s-rowwise-by-name-with-cardinality.json" % (schemaName, collectionName))
                self.__mU.doExport(fp, tableDataDictList, format="json", indent=3)
                logger.debug("Exported %r" % fp)
                #
                #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def prepChemCompSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDataPrepTests("testPrepChemCompDocumentsFromFiles"))
    return suiteSelect


def prepBirdSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDataPrepTests("testPrepBirdDocumentsFromFiles"))
    suiteSelect.addTest(SchemaDefDataPrepTests("testPrepBirdDocumentsFromContainers"))
    return suiteSelect


def prepPdbxSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDataPrepTests("testPrepPdbxDocumentsFromFiles"))
    return suiteSelect


def prepPdbxSlicedSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDataPrepTests("testPrepPdbxDocumentsFromContainers"))
    return suiteSelect


if __name__ == '__main__':
    if True:
        mySuite = prepChemCompSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = prepBirdSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = prepPdbxSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if True:
        mySuite = prepPdbxSlicedSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
