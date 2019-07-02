# File:    DictMethodRunnerTests.py
# Author:  J. Westbrook
# Date:    18-Aug-2018
# Version: 0.001
#
# Update:
#    12-Nov-2018 jdw add chemical component and bird chemical component tests
#     5-Jun-2019 jdw revise for new method runner api
##
"""
Tests for applying dictionary methods defined as references to helper plugin methods .

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from mmcif.api.DictMethodRunner import DictMethodRunner
from rcsb.db.define.DictionaryProvider import DictionaryProvider
from rcsb.db.helpers.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.db.processors.DataTransformFactory import DataTransformFactory
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DictMethodRunnerTests(unittest.TestCase):
    def setUp(self):
        self.__numProc = 2
        self.__fileLimit = 200
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__workPath = os.path.join(HERE, "test-output")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info"
        self.__configName = configName
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__mU = MarshalUtil(workPath=self.__workPath)

        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath)
        self.__birdRepoPath = self.__cfgOb.getPath("BIRD_REPO_PATH", sectionName=configName)
        #
        self.__fTypeRow = "drop-empty-attributes|drop-empty-tables|skip-max-width|convert-iterables|normalize-enums|translateXMLCharRefs"
        self.__fTypeCol = "drop-empty-tables|skip-max-width|convert-iterables|normalize-enums|translateXMLCharRefs"
        self.__chemCompMockLen = 5
        self.__birdMockLen = 3
        self.__pdbxMockLen = 14
        self.__verbose = True
        #
        self.__pathPdbxDictionaryFile = self.__cfgOb.getPath("PDBX_DICT_LOCATOR", sectionName=configName)
        self.__pathRcsbDictionaryFile = self.__cfgOb.getPath("RCSB_DICT_LOCATOR", sectionName=configName)
        #
        self.__modulePathMap = {"rcsb.db.helpers.DictMethodRunnerHelper": "rcsb.db.helpers.DictMethodRunnerHelper"}
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testProcessPdbxDocumentsFromContainers(self):
        """Test case -  create loadable PDBx data from files

        """
        try:
            dP = DictionaryProvider()
            dictApi = dP.getApi(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile])
            rP = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, workPath=self.__workPath)
            dmh = DictMethodRunner(dictApi, modulePathMap=self.__modulePathMap, resourceProvider=rP)
            #
            inputPathList = self.__schU.getLocatorObjList(contentType="pdbx_core")
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="pdbx_core")
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            containerList = sdp.getContainerList(inputPathList)
            #
            #
            logger.debug("Length of path list %d\n", len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__pdbxMockLen)

            for container in containerList:
                cName = container.getName()
                logger.debug("Processing container %s", cName)
                #
                dmh.apply(container)
                #
                savePath = os.path.join(HERE, "test-output", cName + "-with-method.cif")
                self.__mU.doExport(savePath, [container], fmt="mmcif")

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testChemCompDocumentsFromContainers(self):
        """Test case -  create loadable PDBx data from files
        """
        try:
            dP = DictionaryProvider()
            dictApi = dP.getApi(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile])
            rP = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, workPath=self.__workPath)
            dmh = DictMethodRunner(dictApi, modulePathMap=self.__modulePathMap, resourceProvider=rP)
            #
            inputPathList = self.__schU.getLocatorObjList(contentType="chem_comp_core")
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="chem_comp_core")
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            containerList = sdp.getContainerList(inputPathList)
            #
            #
            logger.debug("Length of path list %d", len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__chemCompMockLen)

            for container in containerList:
                cName = container.getName()
                logger.debug("Processing container %s", cName)
                #
                dmh.apply(container)
                #
                savePath = os.path.join(HERE, "test-output", cName + "-with-method.cif")
                self.__mU.doExport(savePath, [container], fmt="mmcif")

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBirdChemCompDocumentsFromContainers(self):
        """Test case -  create loadable PDBx data from files
        """
        try:
            dP = DictionaryProvider()
            dictApi = dP.getApi(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile])
            rP = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, workPath=self.__workPath)
            dmh = DictMethodRunner(dictApi, modulePathMap=self.__modulePathMap, resourceProvider=rP)
            #
            inputPathList = self.__schU.getLocatorObjList(contentType="bird_chem_comp_core")
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType="bird_chem_comp_core")
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            containerList = sdp.getContainerList(inputPathList)
            #
            #
            logger.debug("Length of path list %d", len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__birdMockLen)

            for container in containerList:
                cName = container.getName()
                logger.debug("Processing container %s", cName)
                #
                dmh.apply(container)
                #
                savePath = os.path.join(HERE, "test-output", cName + "-with-method.cif")
                self.__mU.doExport(savePath, [container], fmt="mmcif")

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testMethodRunnerSetup(self):
        """ Test the setup methods for method runner class

        """
        try:
            dP = DictionaryProvider()
            dictApi = dP.getApi(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile])
            rP = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, workPath=self.__workPath)
            dmh = DictMethodRunner(dictApi, modulePathMap=self.__modulePathMap, resourceProvider=rP)
            ok = dmh is not None
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def dictMethodRunnerHelperPdbxSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictMethodRunnerTests("testProcessPdbxDocumentsFromContainers"))
    return suiteSelect


def dictMethodRunnerHelperChemCompSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictMethodRunnerTests("testChemCompDocumentsFromContainers"))
    suiteSelect.addTest(DictMethodRunnerTests("testBirdChemCompDocumentsFromContainers"))
    return suiteSelect


def dictMethodRunnerSetupSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictMethodRunnerTests("testMethodRunnerSetup"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = dictMethodRunnerHelperChemCompSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = dictMethodRunnerSetupSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = dictMethodRunnerHelperPdbxSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = dictMethodRunnerHelperChemCompSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)