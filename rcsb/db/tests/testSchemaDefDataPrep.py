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
#  14-Feb-2019 jdw Add tests for merged validation report content - add flag to suppress data export
#  11-Mar-2019 jdw add tests for sdp.addDocumentSubCategoryAggregates()
#  21-Mar-2019 jdw make all test cases reference core collections
#   5-Jun-2019 jdw update to new method runner api
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
import platform
import pprint
import resource
import time
import unittest

from jsondiff import diff

from mmcif.api.DictMethodRunner import DictMethodRunner
from rcsb.utils.dictionary.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.utils.dictionary.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.db.processors.DataTransformFactory import DataTransformFactory
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.utils.repository.RepositoryProvider import RepositoryProvider
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDefDataPrepTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(SchemaDefDataPrepTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__verbose = True

    def setUp(self):
        self.__isMac = platform.system() == "Darwin"
        self.__excludeTypeL = None if self.__isMac else ["optional"]
        self.__numProc = 2
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__outputPath = os.path.join(HERE, "test-output")
        self.__savedOutputPath = os.path.join(HERE, "test-saved-output")

        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__configName = configName
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__mU = MarshalUtil(workPath=self.__cachePath)
        self.__discoveryMode = self.__cfgOb.get("DISCOVERY_MODE", sectionName=configName, default="local")
        self.__fileLimit = 100 if self.__discoveryMode == "local" else 10
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=True)
        self.__rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, cachePath=self.__cachePath)
        #
        #
        self.__fTypeRow = "drop-empty-attributes|drop-empty-tables|skip-max-width|convert-iterables|normalize-enums|translateXMLCharRefs"
        self.__fTypeCol = "drop-empty-tables|skip-max-width|convert-iterables|normalize-enums|translateXMLCharRefs"
        self.__chemCompMockLen = 24
        self.__pdbxMockLen = 30
        # removes timestamped data items to allow diffs.)
        excludeExtras = ["rcsb_load_status"]
        # excludeExtras = []
        #
        self.__verbose = True
        self.__modulePathMap = self.__cfgOb.get("DICT_METHOD_HELPER_MODULE_PATH_MAP", sectionName=configName)
        #
        self.__exportFlag = True
        self.__diffFlag = False
        #
        self.__simpleTestCaseList = [
            {
                "contentType": "chem_comp",
                "mockLength": self.__chemCompMockLen,
                "filterType": self.__fTypeRow,
                "styleType": "rowwise_by_name",
                "mergeContentTypes": None,
                "rejectLength": 2,
            },
            {
                "contentType": "chem_comp",
                "mockLength": self.__chemCompMockLen,
                "filterType": self.__fTypeRow,
                "styleType": "rowwise_no_name",
                "mergeContentTypes": None,
                "rejectLength": 2,
            },
            {
                "contentType": "chem_comp",
                "mockLength": self.__chemCompMockLen,
                "filterType": self.__fTypeCol,
                "styleType": "columnwise_by_name",
                "mergeContentTypes": None,
                "rejectLength": 2,
            },
            {
                "contentType": "chem_comp",
                "mockLength": self.__chemCompMockLen,
                "filterType": self.__fTypeRow,
                "styleType": "rowwise_by_name",
                "mergeContentTypes": None,
                "rejectLength": 2,
            },
            {
                "contentType": "pdbx_core",
                "mockLength": self.__pdbxMockLen,
                "filterType": self.__fTypeRow,
                "styleType": "rowwise_by_name",
                "mergeContentTypes": None,
                "rejectLength": 0,
            },
        ]
        #
        self.__fullTestCaseList = [
            {
                "contentType": "pdbx_core",
                "mockLength": self.__pdbxMockLen,
                "filterType": self.__fTypeRow,
                "styleType": "rowwise_by_name_with_cardinality",
                "mergeContentTypes": ["vrpt"],
                "rejectLength": 0,
                "excludeExtras": excludeExtras,
            },
            {
                "contentType": "core_chem_comp",
                "mockLength": self.__chemCompMockLen,
                "filterType": self.__fTypeRow,
                "styleType": "rowwise_by_name_with_cardinality",
                "mergeContentTypes": None,
                "rejectLength": 2,
                "excludeExtras": excludeExtras,
            },
        ]
        #
        self.__fullTestCaseListA = [
            {
                "contentType": "pdbx_core",
                "mockLength": self.__pdbxMockLen,
                "filterType": self.__fTypeRow,
                "styleType": "rowwise_by_name_with_cardinality",
                "mergeContentTypes": ["vrpt"],
                "rejectLength": 0,
                "excludeExtras": excludeExtras,
            },
        ]
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 1.0e6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def __timeStep(self, msg):
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", msg, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleSchemaDefDataPrep(self):
        for tcD in self.__simpleTestCaseList:
            rejectLength = 0 if self.__discoveryMode == "remote" else tcD["rejectLength"]
            mockLength = self.__fileLimit if self.__discoveryMode == "remote" else tcD["mockLength"]
            if tcD["contentType"] == "core_chem_comp" and self.__discoveryMode == "remote":
                logger.info("Skipping %r in discovery mode %r", tcD["contentType"], self.__discoveryMode)
                continue
            self.__simpleSchemaDataPrep(tcD["contentType"], tcD["filterType"], tcD["styleType"], mockLength, rejectLength=rejectLength, mergeContentTypes=tcD["mergeContentTypes"])

    def testFullSchemaDefDataPrep(self):
        for tcD in self.__fullTestCaseList:
            rejectLength = 0 if self.__discoveryMode == "remote" else tcD["rejectLength"]
            mockLength = self.__fileLimit if self.__discoveryMode == "remote" else tcD["mockLength"]
            if tcD["contentType"] == "core_chem_comp" and self.__discoveryMode == "remote":
                logger.info("Skipping %r in discovery mode %r", tcD["contentType"], self.__discoveryMode)
                continue
            self.__fullSchemaDataPrep(
                tcD["contentType"],
                tcD["filterType"],
                tcD["styleType"],
                mockLength,
                rejectLength=rejectLength,
                mergeContentTypes=tcD["mergeContentTypes"],
                excludeExtras=tcD["excludeExtras"],
            )

    def __simpleSchemaDataPrep(self, contentType, filterType, styleType, mockLength, rejectLength=0, dataSelectors=None, mergeContentTypes=None):
        """Internal method for preparing file-based data NOT requiring dynamic methods, slicing, or key injection.

        Args:
            contentType (str): Content type name
            filterType (str): List of data processing options (separated by '|') (e.g. "drop-empty-attributes|drop-empty-tables|skip-max-width|...)
            styleType (str): organization of output document (e.g. rowise-by-name)
            mockLength (int): Expected length of the test data for the input content type
            rejectLength (int, optional): number of input data sets rejected by the dataselection criteria. Defaults to 0.
            dataSelectors (list of str, optional): data selection criteria. Defaults to None.
            mergeContentTypes (list of str, optional): list content types to merge with the input data set. Defaults to None. (e.g. ['vrpt'])
        """
        try:
            dataSelectors = dataSelectors if dataSelectors else ["PUBLIC_RELEASE"]
            dD = self.__schP.makeSchemaDef(contentType, dataTyping="ANY", saveSchema=True)
            _ = SchemaDefAccess(dD)
            inputPathList = self.__rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            sd, _, _, _ = self.__schP.getSchemaInfo(collectionGroupName=contentType, dataTyping="ANY")
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=filterType)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__cachePath, verbose=self.__verbose)
            #

            logger.debug("For %s mock length %d length of path list %d\n", contentType, mockLength, len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), mockLength)
            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(inputPathList, styleType=styleType, filterType=filterType, dataSelectors=dataSelectors)
            logger.debug("For %s mock length %d reject length %d length of tddl list %d\n", contentType, mockLength, rejectLength, len(tableDataDictList))
            self.assertGreaterEqual(len(tableDataDictList), mockLength - rejectLength)
            self.assertGreaterEqual(len(containerNameList), mockLength - rejectLength)

            if rejectList:
                logger.debug("For %s rejecting components %r", contentType, rejectList)
            #
            self.assertEqual(len(rejectList), rejectLength)
            fName = "simple-prep-%s-%s.json" % (contentType, styleType)
            if self.__exportFlag:
                fPath = os.path.join(self.__outputPath, fName)
                self.__mU.doExport(fPath, tableDataDictList, fmt="json", indent=3)
            if self.__diffFlag:
                fPath = os.path.join(self.__savedOutputPath, fName)
                refDocList = self.__mU.doImport(fPath, fmt="json")
                self.assertEqual(len(refDocList), len(tableDataDictList))
                #
                jD = diff(refDocList, tableDataDictList, syntax="explicit", marshal=True)
                if jD:
                    _, fn = os.path.split(fPath)
                    bn, _ = os.path.splitext(fn)
                    fPath = os.path.join(self.__outputPath, bn + "-diff.json")
                    logger.debug("jsondiff for %s %s = \n%s", contentType, styleType, pprint.pformat(jD, indent=3, width=100))
                    self.__mU.doExport(fPath, jD, fmt="json", indent=3)
                self.assertEqual(len(jD), 0)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def __logDocumentOrder(self, docList):
        for doc in docList:
            logger.debug("keys %r", list(doc.keys()))

    def __filterDocuments(self, docList, excludeList=None):
        excludeList = excludeList if excludeList else []
        for doc in docList:
            for excl in excludeList:
                if excl in doc:
                    del doc[excl]

    def __fullSchemaDataPrep(self, contentType, filterType, styleType, mockLength, rejectLength=0, dataSelectors=None, mergeContentTypes=None, excludeExtras=None):
        """Internal method for preparing file-based data requiring dynamic methods, slicing, or key injection.

        Args:
            contentType (str): Content type name
            filterType (str): List of data processing options (separated by '|') (e.g. "drop-empty-attributes|drop-empty-tables|skip-max-width|...)
            styleType (str): organization of output document (e.g. rowise-by-name)
            mockLength (int): Expected length of the test data for the input content type
            rejectLength (int, optional): number of input data sets rejected by the dataselection criteria. Defaults to 0.
            dataSelectors (list of str, optional): data selection criteria. Defaults to None.
            mergeContentTypes (list of str, optional): list content types to merge with the input data set. Defaults to None. (e.g. ['vrpt'])
        """
        try:
            excludeExtras = excludeExtras if excludeExtras else []
            _ = mockLength
            _ = rejectLength
            dD = self.__schP.makeSchemaDef(contentType, dataTyping="ANY", saveSchema=True)
            _ = SchemaDefAccess(dD)
            inputPathList = self.__rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            sd, _, collectionNameList, _ = self.__schP.getSchemaInfo(collectionGroupName=contentType, dataTyping="ANY")
            #
            dP = DictionaryApiProviderWrapper(self.__cachePath, cfgOb=self.__cfgOb, configName=self.__configName, useCache=True)
            dictApi = dP.getApiByName(contentType)
            #
            rP = DictMethodResourceProvider(
                self.__cfgOb,
                configName=self.__configName,
                cachePath=self.__cachePath,
                restoreUseStash=False,
                restoreUseGit=True,
                providerTypeExcludeL=self.__excludeTypeL,
            )
            dmh = DictMethodRunner(dictApi, modulePathMap=self.__modulePathMap, resourceProvider=rP)
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=filterType)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__cachePath, verbose=self.__verbose)
            containerList = self.__rpP.getContainerList(inputPathList)
            for container in containerList:
                cName = container.getName()
                logger.debug("Processing container %s", cName)
                dmh.apply(container)
            #
            for collectionName in collectionNameList:
                tableIdExcludeList = sd.getCollectionExcluded(collectionName)
                tableIdIncludeList = sd.getCollectionSelected(collectionName)
                sliceFilter = sd.getCollectionSliceFilter(collectionName)
                sdp.setSchemaIdExcludeList(tableIdExcludeList)
                sdp.setSchemaIdIncludeList(tableIdIncludeList)
                #
                docList, _, _ = sdp.processDocuments(
                    containerList, styleType=styleType, sliceFilter=sliceFilter, filterType=filterType, dataSelectors=dataSelectors, collectionName=collectionName
                )

                docList = sdp.addDocumentPrivateAttributes(docList, collectionName)
                docList = sdp.addDocumentSubCategoryAggregates(docList, collectionName)

                # Special exclusions for the test harness. (removes timestamped data items to allow diffs.)
                self.__filterDocuments(docList, excludeExtras)
                mergeS = "-".join(mergeContentTypes) if mergeContentTypes else ""
                fName = "full-prep-%s-%s-%s-%s.json" % (contentType, collectionName, mergeS, styleType)
                if self.__exportFlag:
                    self.__logDocumentOrder(docList)
                    fPath = os.path.join(self.__outputPath, fName)
                    self.__mU.doExport(fPath, docList, fmt="json", indent=3)
                    logger.debug("Exported %r", fPath)
                #
                if self.__diffFlag:
                    fPath = os.path.join(self.__savedOutputPath, fName)
                    refDocList = self.__mU.doImport(fPath, fmt="json")
                    self.assertEqual(len(refDocList), len(docList))
                    logger.debug("For %s %s len refDocList %d", contentType, collectionName, len(refDocList))
                    logger.debug("For %s %s len docList %d", contentType, collectionName, len(docList))
                    jD = diff(refDocList, docList, syntax="explicit", marshal=True)
                    if jD:
                        _, fn = os.path.split(fPath)
                        bn, _ = os.path.splitext(fn)
                        fPath = os.path.join(self.__outputPath, bn + "-diff.json")
                        logger.debug("jsondiff for %s %s = \n%s", contentType, collectionName, pprint.pformat(jD, indent=3, width=100))
                        self.__mU.doExport(fPath, jD, fmt="json", indent=3)
                    self.assertEqual(len(jD), 0)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def prepSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDataPrepTests("testSimpleSchemaDefDataPrep"))
    suiteSelect.addTest(SchemaDefDataPrepTests("testFullSchemaDefDataPrep"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = prepSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
