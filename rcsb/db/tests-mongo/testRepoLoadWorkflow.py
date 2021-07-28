##
# File:    RepoLoadWorkflowTests.py
# Author:  J. Westbrook
# Date:    16-Dec-2019
# Version: 0.001
#
# Updates:

#
##
"""
Tests for simple workflows to excute loading operations of
PDBx data and other repository data.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import platform
import resource
import time
import unittest

from rcsb.db.wf.RepoLoadWorkflow import RepoLoadWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RepoLoadWorkflowTests(unittest.TestCase):
    skipFull = platform.system() != "Darwin"

    def __init__(self, methodName="runTest"):
        super(RepoLoadWorkflowTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        cachePath = os.path.join(TOPDIR, "CACHE")
        #
        self.__commonD = {"configPath": configPath, "mockTopPath": mockTopPath, "configName": configName, "cachePath": cachePath}
        self.__loadCommonD = {
            "failedFilePath": os.path.join(HERE, "test-output", "failed-list.txt"),
            "readBackCheck": True,
            "numProc": 2,
            "chunkSize": 10,
        }
        self.__ldList = [
            {"databaseName": "bird_chem_comp_core", "collectionNameList": None, "loadType": "full"},
            {"databaseName": "bird_chem_comp_core", "collectionNameList": None, "loadType": "replace", "updateSchemaOnReplace": True},
            {"databaseName": "pdbx_core", "collectionNameList": None, "loadType": "full"},
        ]
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    @unittest.skipIf(skipFull, "Long and redundant test")
    def testReourceCacheWorkflow(self):
        """Test resource cache rebuild"""
        #
        try:
            ok = RepoLoadWorkflow(**self.__commonD).buildResourceCache(rebuildCache=True)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Long and redundant test")
    def testPdbxLoaderWorkflow(self):
        #
        try:
            rlWf = RepoLoadWorkflow(**self.__commonD)
            ok = rlWf.buildResourceCache(rebuildCache=False)
            self.assertTrue(ok)
            for ld in self.__ldList:
                ld.update(self.__loadCommonD)
                ok = rlWf.load("pdbx-loader", **ld)
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skip("Disable long test")
    def testEtlLoaderWorkflow(self):
        #
        try:
            etlCommonD = {
                "loadType": "full",
                "readBackCheck": True,
                "numProc": 2,
                "chunkSize": 10,
            }
            rlWf = RepoLoadWorkflow(**self.__commonD)
            ok = rlWf.buildResourceCache(rebuildCache=False)
            self.assertTrue(ok)
            #
            ok = rlWf.load("etl-repository-holdings", **etlCommonD)
            self.assertTrue(ok)
            #
            ok = rlWf.load("etl-entity-sequence-clusters", **etlCommonD)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def workflowLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepoLoadWorkflowTests("testPdbxLoaderWorkflow"))
    suiteSelect.addTest(RepoLoadWorkflowTests("testEtlLoaderWorkflow"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = workflowLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
