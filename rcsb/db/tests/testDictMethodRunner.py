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
from rcsb.db.define.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
from rcsb.db.helpers.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.db.utils.RepositoryProvider import RepositoryProvider
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
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__configName = configName
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__mU = MarshalUtil(workPath=self.__cachePath)
        self.__rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, cachePath=self.__cachePath)
        #
        self.__testCaseList = [
            # {"contentType": "chem_comp_core", "mockLength": 5, 'mergeContent': None},
            {"contentType": "bird_chem_comp_core", "mockLength": 17, "mergeContent": None},
            {"contentType": "pdbx_core", "mockLength": 14, "mergeContent": ["vrpt"]},
        ]
        #
        self.__modulePathMap = self.__cfgOb.get("DICT_METHOD_HELPER_MODULE_PATH_MAP", sectionName=configName)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def __runContentType(self, contentType, mockLength, mergeContent):
        """ Read and process test fixture data files from the input content type.
        """
        try:
            dP = DictionaryApiProviderWrapper(self.__cfgOb, self.__cachePath, useCache=True)
            dictApi = dP.getApiByName(contentType)
            rP = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, cachePath=self.__cachePath, siftsAbbreviated="TEST")
            dmh = DictMethodRunner(dictApi, modulePathMap=self.__modulePathMap, resourceProvider=rP)
            locatorObjList = self.__rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContent)
            containerList = self.__rpP.getContainerList(locatorObjList)
            #
            logger.debug("Length of locator list %d\n", len(locatorObjList))
            self.assertGreaterEqual(len(locatorObjList), mockLength)
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
            self.__runContentType(tD["contentType"], tD["mockLength"], tD["mergeContent"])

    def testMethodRunnerSetup(self):
        """ Test the setup methods for method runner class

        """
        try:
            dP = DictionaryApiProviderWrapper(self.__cfgOb, self.__cachePath, useCache=True)
            dictApi = dP.getApiByName("pdbx")
            rP = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, cachePath=self.__cachePath, siftsAbbreviated="TEST")
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
