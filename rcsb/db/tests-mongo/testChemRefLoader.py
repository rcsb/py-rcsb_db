##
# File:    ChemRefLoaderTests.py
# Author:  J. Westbrook
# Date:    9-Dec-2018
#
# Updates:
#
##
"""
Tests for loading repository holdings information.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.mongo.ChemRefExtractor import ChemRefExtractor
from rcsb.db.scripts.ChemRefEtlWorker import ChemRefEtlWorker
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ChemRefLoaderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ChemRefLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        # self.__cfgOb.dump()
        self.__resourceName = "MONGO_DB"
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__documentLimit = 1000
        self.__filterType = "assign-dates"
        #
        self.__workPath = os.path.join(HERE, "test-output")
        self.__schemaPath = os.path.join(self.__workPath, "json-schema-drugbank_core.json")
        self.__dataPath = os.path.join(self.__workPath, "json-data-drugbank_core.json")
        #
        # sample data set
        self.__updateId = "2018_23"
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testLoadChemRef(self):
        """ Test case - load chemical reference ETL data -

        """
        try:
            crw = ChemRefEtlWorker(self.__cfgOb)
            crExt = ChemRefExtractor(self.__cfgOb)

            idD = crExt.getChemCompAccesionMapping(extResource="DrugBank")
            logger.info("Mapping dictionary %r", len(idD))
            #
            ok = crw.load(self.__updateId, extResource="DrugBank", loadType="full")
            #
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def chemRefLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemRefLoaderTests("testLoadChemRef"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = chemRefLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
