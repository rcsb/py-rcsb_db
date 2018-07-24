# File:    TimeUtilTests.py
# Author:  J. Westbrook
# Date:    9-Jul-2018
# Version: 0.001
#
# Update:
#
#
#
##
"""
Tests for Convenience utilities to manipulate time stamps.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import datetime
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

from rcsb_db.utils.TimeUtil import TimeUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TimeUtilTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
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

    def testTimeStamps(self):
        """ Verify time stamp operations.
        """
        try:
            tU = TimeUtil()
            tS = tU.getTimestamp(useUtc=True)
            logger.debug("TS (UTC) = %s(%d)" % (tS, len(tS)))
            self.assertTrue(len(tS) >= 32)
            #
            tS = tU.getTimestamp(useUtc=False)
            logger.debug("TS = %s(%d)" % (tS, len(tS)))
            self.assertTrue(len(tS) >= 32)

            # self.assertTrue(ok)
            wS1 = tU.getCurrentWeekSignature()
            logger.debug("Current week signature %s" % wS1)
            td = datetime.date.today()

            wS2 = tU.getWeekSignature(td.year, td.month, td.day)
            logger.debug("Computed week signature %s" % wS2)
            self.assertEqual(wS1, wS2)
            #
            tS = tU.getTimestamp(useUtc=True)
            logger.debug("TS (UTC) = %s(%d)" % (tS, len(tS)))
            self.assertTrue(len(tS) >= 32)
            dt = tU.getDateTimeObj(tS)
            logger.debug("Recycled DT (UTC) %s" % dt.isoformat(' '))
            #
            tS = tU.getTimestamp(useUtc=False)
            logger.debug("TS (local) = %s(%d)" % (tS, len(tS)))
            self.assertTrue(len(tS) >= 32)
            #
            dt = tU.getDateTimeObj(tS)
            logger.debug("Recycled DT (local) %s" % dt.isoformat(' '))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def timeStampSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(TimeUtilTests("testTimeStamps"))
    return suiteSelect


if __name__ == '__main__':
    #
    mySuite = timeStampSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
