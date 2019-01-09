# File:    DataExchangeStatusPrepTests.py
# Author:  J. Westbrook
# Date:    11-Jul-2018
# Version: 0.001
#
# Update:
#  14-Jul-2018 jdw update api prototypes
#
#
##
"""
Tests for creation of data exchange status records.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DataExchangeStatusTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testCreateStatusRecord(self):
        """ Verify time stamp operations.
        """
        try:
            dL = []
            desp = DataExchangeStatus()
            ok = desp.setObject('my_database', 'my_collection')
            self.assertTrue(ok)
            tS = desp.setStartTime()
            self.assertGreaterEqual(len(tS), 15)
            time.sleep(1)
            ok = desp.setStatus(updateId=None, successFlag='Y')
            self.assertTrue(ok)
            tS = desp.setEndTime()
            self.assertGreaterEqual(len(tS), 15)
            dL.append(desp.getStatus())
            self.assertEqual(len(dL), 1)
            logger.debug("Status record %r" % dL[0])
            #
            ok = desp.setObject('my_database', 'my_other_collection')
            self.assertTrue(ok)
            tS = desp.setStartTime()
            self.assertGreaterEqual(len(tS), 15)
            time.sleep(1)
            ok = desp.setStatus(updateId='2018_40', successFlag='Y')
            self.assertTrue(ok)
            tS = desp.setEndTime()
            self.assertGreaterEqual(len(tS), 15)
            dL.append(desp.getStatus())
            self.assertEqual(len(dL), 2)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def statusRecordSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DataExchangeStatusTests("testCreateStatusRecord"))
    return suiteSelect


if __name__ == '__main__':
    #
    mySuite = statusRecordSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
