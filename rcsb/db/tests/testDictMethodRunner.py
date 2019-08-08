# File:    DictMethodRunnerTests.py
# Author:  J. Westbrook
# Date:    18-Aug-2018
# Version: 0.001
#
# Update:
#    12-Nov-2018 jdw add chemical component and bird chemical component tests
#     5-Jun-2019 jdw revise for new method runner api
#    16-Jul-2019 jdw remove schema processing.
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
from rcsb.db.utils.RepoPathUtil import RepoPathUtil
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
        self.__rpU = RepoPathUtil(cfgOb=self.__cfgOb, cfgSectionName=configName, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath, verbose=False)
        #
        self.__testCaseList = [
            {"contentType": "chem_comp_core", "mockLength": 5},
            {"contentType": "bird_chem_comp_core", "mockLength": 3},
            {"contentType": "pdbx_core", "mockLength": 14},
        ]
        #
        self.__modulePathMap = self.__cfgOb.get("DICT_HELPER_MODULE_PATH_MAP", sectionName=configName)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def __runContentType(self, contentType, mockLength):
        """ Read and process test fixture data files from the input content type.
        """
        try:
            dP = DictionaryProvider()
            dictLocatorMap = self.__cfgOb.get("DICT_LOCATOR_CONFIG_MAP", sectionName=self.__configName)
            if contentType not in dictLocatorMap:
                logger.error("Missing dictionary locator configuration for %s", contentType)
                dictLocators = []
            else:
                dictLocators = [self.__cfgOb.getPath(configLocator, sectionName=self.__configName) for configLocator in dictLocatorMap[contentType]]
            #
            dictApi = dP.getApi(dictLocators=dictLocators)
            rP = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, workPath=self.__workPath)
            dmh = DictMethodRunner(dictApi, modulePathMap=self.__modulePathMap, resourceProvider=rP)
            inputPathList = self.__rpU.getLocatorList(contentType=contentType)
            #
            logger.debug("Length of path list %d\n", len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), mockLength)
            for inputPath in inputPathList:
                containerList = self.__mU.doImport(inputPath, fmt="mmcif")
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

    def testMethodRunner(self):
        """Test method runner for multiple content types.
        """
        for tD in self.__testCaseList:
            self.__runContentType(tD["contentType"], tD["mockLength"])

    def testMethodRunnerSetup(self):
        """ Test the setup methods for method runner class

        """
        try:
            dP = DictionaryProvider()
            dictLocatorMap = self.__cfgOb.get("DICT_LOCATOR_CONFIG_MAP", sectionName=self.__configName)
            dictLocators = [self.__cfgOb.getPath(configLocator, sectionName=self.__configName) for configLocator in dictLocatorMap["pdbx"]]
            dictApi = dP.getApi(dictLocators=dictLocators)
            rP = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, workPath=self.__workPath)
            dmh = DictMethodRunner(dictApi, modulePathMap=self.__modulePathMap, resourceProvider=rP)
            ok = dmh is not None
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def dictMethodRunnerSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictMethodRunnerTests("testMethodRunner"))
    return suiteSelect


def dictMethodRunnerSetupSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictMethodRunnerTests("testMethodRunnerSetup"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = dictMethodRunnerSetupSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = dictMethodRunnerSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
