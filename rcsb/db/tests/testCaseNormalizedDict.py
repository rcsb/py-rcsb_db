# File:    CaseNormalizedDictTests.py
# Author:  J. Westbrook
# Date:    4-Sep-2018
# Version: 0.001
#
# Update:
#
#
#
##
"""
Test cases for case normalized dictionary variant.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import json
import logging
import os
import pickle
import time
import unittest

# from rcsb.db.utils.CaseNormalizedDict import CaseNormalizedDict2 as CaseNormalizedDict
from rcsb.db.utils.CaseNormalizedDict import CaseNormalizedDict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class CaseNormalizedDictTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        #
        self.__picklePath = os.path.join(HERE, "test-output", "cnd-data.pic")
        self.__jsonPath = os.path.join(HERE, "test-output", "cnd-data.json")
        self.__startTime = time.monotonic()
        logger.debug("Starting %s now", self.id())

    def tearDown(self):
        logger.debug("Completed %s in %.3f s", self.id(), time.monotonic() - self.__startTime)

    def testCaseCompareExamples1(self):
        """Verify case comparison operations."""
        try:
            examples = ["Test1", "test1", "TEST1", "tesT1", "tEst1"]
            #
            cnd = CaseNormalizedDict({k: k for k in examples})
            #
            for ex in examples:
                self.assertTrue(ex in cnd)
            #
            logger.debug("String representation %s", cnd)
            logger.debug("String representation (items) %s", cnd.items())
            logger.debug("Raw representation %r", cnd)
            #
            logger.debug("normalized values %s", cnd["test1"])
            self.assertTrue(cnd["test1"], examples[-1])
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCaseCompareExamples2(self):
        """Verify case comparison operations."""
        try:
            examples = ["Test1", "Test2", "Test3", "Test4"]
            #
            cnd = CaseNormalizedDict({k: k for k in examples})
            #
            for ex in examples:
                exl = ex.lower()
                self.assertEqual(ex, cnd[exl])
                logger.debug("Comparing ex %s and cnd{} %s", ex, cnd[exl])
            #

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSerializationJson(self):
        """Verify case comparison operations."""
        try:
            examples = ["Test1", "test1", "TEST1", "tesT1", "tEst1"]
            #
            cnd = CaseNormalizedDict({k: True for k in examples})
            #
            ts = json.dumps(cnd)
            logger.debug("Json %r", ts)
            #
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testSerializationPickle(self):
        """Verify case comparison operations."""
        try:
            examples = ["Test1", "test1", "TEST1", "tesT1", "tEst1"]
            #
            cnd = CaseNormalizedDict({k: True for k in examples})
            #
            ts = pickle.dumps(cnd)
            logger.debug("Pickle %r", ts)
            #
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def testCaseCompareSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(CaseNormalizedDictTests("testCaseCompareExamples1"))
    suiteSelect.addTest(CaseNormalizedDictTests("testCaseCompareExamples2"))
    # suiteSelect.addTest(CaseNormalizedDictTests("testSerializationJson"))
    suiteSelect.addTest(CaseNormalizedDictTests("testSerializationPickle"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = testCaseCompareSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
