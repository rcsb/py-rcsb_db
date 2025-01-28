##
# File:    testPdbCsmImageSplitter.py
# Author:  Michael Trumbull
# Date:    12-Dec-2024
# Version: 0.01
#
# Updates:
#
##

__docformat__ = "google en"
__author__ = "Michael Trumbull"
__email__ = "michael.trumbull@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest
from pathlib import Path

from rcsb.db.wf.RepoLoadWorkflow import RepoLoadWorkflow

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger(__name__)


class TestPdbCsmImagesSplitter(unittest.TestCase):
    def setUp(self) -> None:
        self.__startTime = time.time()
        # self.__cachePath = os.path.join(HERE, "test-data")
        self.__workPath = os.path.join(HERE, "test-output")
        self.mockdataDir = os.path.join(TOPDIR, "rcsb", "mock-data", "MOCK_IMGS_WF_BCIF_DATA")
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self) -> None:
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testIdListGeneration(self) -> None:
        """Test id list file generation ..."""
        try:
            logger.info("creating object")
            rlWf = RepoLoadWorkflow()
            logger.info("Generating 3 id lists to run through.")
            logger.info("mockdataDir %s", self.mockdataDir)
            logger.info("workpath %s", self.__workPath)

            ok = rlWf.splitIdList(
                "pdbx_id_list_splitter",
                databaseName="pdbx_core",
                holdingsFilePath=os.path.join(self.mockdataDir, "holdings/released_structures_last_modified_dates.json.gz"),
                loadFileListDir=self.__workPath,
                numSublistFiles=3,
                imgsWfFormat=True,
                updateAllImages=True,
                noBcifSubdirs=True,
                bcifBaseDir=self.mockdataDir,
            )
            self.assertTrue(ok)
            ok1 = self.checkList(os.path.join(self.__workPath, "pdbx_core_ids-1.txt"))
            if not ok1:
                logger.error("idList_0.txt failed")
            self.assertTrue(ok1)
            ok2 = self.checkList(os.path.join(self.__workPath, "pdbx_core_ids-2.txt"))
            if not ok2:
                logger.error("idList_1.txt failed")
            self.assertTrue(ok2)
            ok3 = self.checkList(os.path.join(self.__workPath, "pdbx_core_ids-3.txt"))
            if not ok3:
                logger.error("idList_2.txt failed")
            self.assertTrue(ok3)
            ok = rlWf.splitIdList(
                "pdbx_id_list_splitter",
                databaseName="pdbx_comp_model_core",
                holdingsFilePath=os.path.join(self.mockdataDir, "holdings/computed-models-holdings-list.json"),
                loadFileListDir=self.__workPath,
                numSublistFiles=3,
                imgsWfFormat=True,
                updateAllImages=True,
                noBcifSubdirs=True,
                bcifBaseDir=self.mockdataDir,
            )
            self.assertTrue(ok)
            ok1 = self.checkList(os.path.join(self.__workPath, "pdbx_comp_model_core_ids-1.txt"))
            if not ok1:
                logger.error("idList_0.txt failed")
            self.assertTrue(ok1)
            ok2 = self.checkList(os.path.join(self.__workPath, "pdbx_comp_model_core_ids-2.txt"))
            if not ok2:
                logger.error("idList_1.txt failed")
            self.assertTrue(ok2)
            ok3 = self.checkList(os.path.join(self.__workPath, "pdbx_comp_model_core_ids-3.txt"))
            if not ok3:
                logger.error("idList_2.txt failed")
            self.assertTrue(ok3)

            logger.info("Reading generated lists and checking for format.")

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail("Failed to build idLists")

    def checkList(self, ids: str) -> bool:

                try:
                    logger.info('ids path for checkList %s', ids)
                    allDataPresent = True
                    with Path(ids).open("r", encoding="utf-8") as file:
                        idList = [line.rstrip("\n") for line in file]
                    for line in idList:
                        logger.info('line from file is: %s', line)
                        fileId, bcifFileName, sdm = line.split()
                        if not ((len(fileId) > 0) and (len(bcifFileName) > 0) and (len(sdm) > 0)):
                            logger.error('Found one of the following had a length of zero %s %s %s', fileId, bcifFileName, sdm)
                            allDataPresent = False
                    logger.info('End of a single checkList. Returning a value of %s', allDataPresent)
                    return allDataPresent
                except Exception:
                    logger.exception("Failed to find created file %s", ids)
                    return False


def suiteFileGeneration():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(TestPdbCsmImagesSplitter("testIdListGeneration"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteFileGeneration()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
