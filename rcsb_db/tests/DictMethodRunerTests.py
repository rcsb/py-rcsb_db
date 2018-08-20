# File:    DictMethodRunnerTests.py
# Author:  J. Westbrook
# Date:    18-Aug-2018
# Version: 0.001
#
# Update:
#
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

from rcsb_db.define.DictMethodRunner import DictMethodRunner
from rcsb_db.helpers.DictMethodRunnerHelper import DictMethodRunnerHelper
from rcsb_db.io.MarshalUtil import MarshalUtil
from rcsb_db.processors.DataTransformFactory import DataTransformFactory
from rcsb_db.processors.SchemaDefDataPrep import SchemaDefDataPrep
#
from rcsb_db.utils.ConfigUtil import ConfigUtil
from rcsb_db.utils.SchemaDefUtil import SchemaDefUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DictMethodRunnerTests(unittest.TestCase):

    def setUp(self):
        self.__numProc = 2
        self.__fileLimit = 200
        mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data")
        self.__workPath = os.path.join(HERE, 'test-output')
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName, mockTopPath=mockTopPath)
        self.__mU = MarshalUtil(workPath=self.__workPath)

        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath)
        self.__birdRepoPath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        #
        self.__fTypeRow = "drop-empty-attributes|drop-empty-tables|skip-max-width|convert-iterables"
        self.__fTypeCol = "drop-empty-tables|skip-max-width|convert-iterables"
        self.__chemCompMockLen = 4
        self.__birdMockLen = 4
        self.__pdbxMockLen = 8
        self.__verbose = True
        #
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb_db', 'data', 'dictionaries', 'mmcif_pdbx_v5_next.dic')
        self.__pathRcsbDictionaryFile = os.path.join(TOPDIR, 'rcsb_db', 'data', 'dictionaries', 'rcsb_mmcif_ext_v1.dic')
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

    def testProcessPdbxDocumentsFromContainers(self):
        """Test case -  create loadable PDBx data from files
        """
        try:
            dH = DictMethodRunnerHelper()
            dmh = DictMethodRunner(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile], methodHelper=dH)
            #
            inputPathList = self.__schU.getPathList(schemaName='pdbx')
            sd, _, _, _ = self.__schU.getSchemaInfo(schemaName='pdbx')
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            containerList = sdp.getContainerList(inputPathList)
            #
            #
            logger.debug("Length of path list %d\n" % len(inputPathList))
            self.assertGreaterEqual(len(inputPathList), self.__pdbxMockLen)

            for container in containerList:
                cName = container.getName()
                logger.info("Processing container %s" % cName)
                #
                dmh.apply(container)
                #
                savePath = os.path.join(HERE, "test-output", cName + '-with-method.cif')
                self.__mU.doExport(savePath, [container], format="mmcif")

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testMethodRunnerSetup(self):
        """ Test the setup methods for method runner class

        """
        try:
            dH = DictMethodRunnerHelper()
            dmh = DictMethodRunner(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile], methodHelper=dH)
            ok = dmh is not None
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def DictMethodRunnerHelperSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictMethodRunnerTests("testProcessPdbxDocumentsFromContainers"))
    return suiteSelect


def DictMethodRunnerSetupSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictMethodRunnerTests("testMethodRunnerSetup"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = DictMethodRunnerSetupSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if True:
        mySuite = DictMethodRunnerHelperSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
