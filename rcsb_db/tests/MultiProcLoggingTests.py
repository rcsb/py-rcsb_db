##
#
# File:    MultiProcLoggingTests.py
# Author:  J. Westbrook
# Date:    5-Apr-2018
# Version: 0.001
#
# Updates:
#
##
"""
Test cases logger context manager --

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import os
import sys
import unittest
import time
from io import StringIO

import logging
logging.basicConfig(level=logging.DEBUG, format='MAIN-%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.utils.MultiProcUtil import MultiProcUtil
from rcsb_db.utils.MultiProcLogging import MultiProcLogging


class MultiProcLoggingTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        self.__logRecordMax = 5
        self.__startTime = time.time()
        self.__testLogPath = os.path.join(HERE, "test-output", "TESTLOGFILE.LOG")
        self.__mpFormat = '%(asctime)s [%(levelname)s]-%(processName)s-%(module)s.%(funcName)s: %(message)s'
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def workerOne(self, dataList, procName, optionsD, workingDir):
        """
            Worker method must support the following prototype -

            sucessList,resultList,diagList=workerFunc(runList=nextList,procName, optionsD, workingDir)

        """
        successList = []
        for d in dataList:
            logger.info(" %s value %s" % (procName, d))
            successList.append(d)

        return successList, [], []

    def testLogStream(self):
        """Test case -  context manager - with default root logger

        """
        try:
            #
            myLen = self.__logRecordMax
            dataList = [i for i in range(1, myLen)]
            logger.info("dataList %d:  %r" % (len(dataList), dataList))
            #
            with MultiProcLogging(logger=logger, format=self.__mpFormat, level=logging.DEBUG) as wlogger:
                sL, _, _ = self.workerOne(dataList, "test", optionsD={}, workingDir='.')
            logger.info("dataList %d:  %r" % (len(dataList), dataList))
            self.assertEqual(len(sL), len(dataList))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLogStringStream(self):
        """Test case -   context manager - to custom string stream
        """
        try:
            myLen = self.__logRecordMax
            dataList = [i for i in range(1, myLen)]
            logger.info("dataList %d:  %r" % (len(dataList), dataList))
            #
            slogger = logging.getLogger()
            slogger.propagate = False
            for handler in slogger.handlers:
                slogger.removeHandler(handler)
            #
            stream = StringIO()
            sh = logging.StreamHandler(stream=stream)
            sh.setLevel(logging.DEBUG)
            fmt = logging.Formatter("STRING-%(processName)s: %(message)s")
            sh.setFormatter(fmt)
            slogger.addHandler(sh)
            #
            with MultiProcLogging(logger=slogger, format=self.__mpFormat, level=logging.DEBUG) as wlogger:
                for ii in range(myLen):
                    wlogger.debug("context logging record %d" % ii)
            #
            stream.seek(0)
            logLines = stream.readlines()
            logger.debug(">> dataList %d:  %r" % (len(logLines), logLines))
            self.assertEqual(len(logLines), myLen)
            for line in logLines:
                self.assertIn("context logging record", line)
        except Exception as e:
            logger.exception("context logging record %s" % str(e))
            self.fail()

    def testLogFileHandler(self):
        """Test case -  context manager - to string stream and custom file stream
        """
        try:

            #
            myLen = self.__logRecordMax
            dataList = [i for i in range(1, myLen)]
            logger.info("dataList %d:  %r" % (len(dataList), dataList))
            #
            flogger = logging.getLogger()
            for handler in flogger.handlers:
                flogger.removeHandler(handler)
            fh = logging.FileHandler(self.__testLogPath, mode='w', encoding='utf-8')
            fh.setLevel(logging.DEBUG)
            fmt = logging.Formatter("FILE-%(processName)s: %(message)s")
            fh.setFormatter(fmt)
            flogger.addHandler(fh)
            #
            with MultiProcLogging(logger=flogger, format=self.__mpFormat, level=logging.DEBUG) as wlogger:
                for ii in range(myLen):
                    wlogger.debug("context logging record %d" % ii)

            #
            logLines = []
            with open(self.__testLogPath, 'r') as ifh:
                for line in ifh:
                    logLines.append(line)
            self.assertEqual(len(logLines), myLen)
            for line in logLines:
                self.assertIn("context logging record", line)
        except Exception as e:
            logger.exception("context logging record %s" % str(e))
            self.fail()

    def testLogStringPlusFileHandlers(self):
        """Test case -  context manager - to custom file stream
        """
        try:

            #
            myLen = self.__logRecordMax
            dataList = [i for i in range(1, myLen)]
            logger.info("dataList %d:  %r" % (len(dataList), dataList))
            #
            mlogger = logging.getLogger()
            for handler in mlogger.handlers:
                mlogger.removeHandler(handler)
            #
            fh = logging.FileHandler(self.__testLogPath, mode='w', encoding='utf-8')
            fh.setLevel(logging.DEBUG)
            fmt = logging.Formatter("FILE-%(processName)s: %(message)s")
            fh.setFormatter(fmt)
            mlogger.addHandler(fh)
            #
            stream = StringIO()
            sh = logging.StreamHandler(stream=stream)
            sh.setLevel(logging.DEBUG)
            fmt = logging.Formatter("STRING-%(processName)s: %(message)s")
            sh.setFormatter(fmt)
            mlogger.addHandler(sh)
            #
            with MultiProcLogging(logger=mlogger, format=self.__mpFormat, level=logging.DEBUG) as wlogger:
                for ii in range(myLen):
                    wlogger.debug("context logging record %d" % ii)

            #
            logLines = []
            with open(self.__testLogPath, 'r') as ifh:
                for line in ifh:
                    logLines.append(line)
            self.assertEqual(len(logLines), myLen)
            for line in logLines:
                self.assertIn("context logging record", line)
            #
            stream.seek(0)
            logLines = stream.readlines()
            logger.debug(">> dataList %d:  %r" % (len(logLines), logLines))
            self.assertEqual(len(logLines), myLen)
            for line in logLines:
                self.assertIn("context logging record", line)
            #
        except Exception as e:
            logger.exception("context logging record %s" % str(e))
            self.fail()

#
    def testLogStringStreamMultiProc(self):
        """Test case -   context manager - to custom string stream
        """
        try:

            #
            myLen = self.__logRecordMax
            dataList = [i for i in range(1, myLen)]
            logger.info("dataList %d:  %r" % (len(dataList), dataList))
            #
            slogger = logging.getLogger()
            slogger.propagate = False
            for handler in slogger.handlers:
                slogger.removeHandler(handler)
            #
            stream = StringIO()
            sh = logging.StreamHandler(stream=stream)
            sh.setLevel(logging.DEBUG)
            fmt = logging.Formatter("STRING-%(processName)s: %(message)s")
            sh.setFormatter(fmt)
            slogger.addHandler(sh)
            #
            logger.debug("Starting string stream logging(root)")
            #
            with MultiProcLogging(logger=slogger, format=self.__mpFormat, level=logging.DEBUG) as wlogger:
                numProc = 2
                chunkSize = 0
                optD = {}
                mpu = MultiProcUtil(verbose=True)
                mpu.setOptions(optionsD=optD)
                mpu.set(workerObj=self, workerMethod="workerOne")
                ok, failList, retLists, diagList = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1, chunkSize=chunkSize)
                self.assertEqual(len(failList), 0)
                self.assertTrue(ok)
                #

            #
            sh.flush()
            slogger.removeHandler(sh)
            #
            stream.seek(0)
            logLines = stream.readlines()
            logger.debug(">> dataList %d:  %r" % (len(logLines), logLines))
            self.assertGreaterEqual(len(logLines), myLen)
            # for line in logLines:
            #    self.assertIn("context logging record", line)
        except Exception as e:
            logger.exception("context logging record %s" % str(e))
            self.fail()

    def testLogFileHandlerMultiProc(self):
        """Test case -  context manager - to string stream and custom file stream
        """
        try:

            #
            myLen = self.__logRecordMax
            dataList = [i for i in range(1, myLen)]
            logger.info("dataList %d:  %r" % (len(dataList), dataList))
            #
            # For multiprocessing start with the root logger ...
            flogger = logging.getLogger()
            for handler in flogger.handlers:
                flogger.removeHandler(handler)
            fh = logging.FileHandler(self.__testLogPath, mode='w', encoding='utf-8')
            fh.setLevel(logging.DEBUG)
            fmt = logging.Formatter("FILE-%(processName)s: %(message)s")
            fh.setFormatter(fmt)
            flogger.addHandler(fh)
            #
            #
            with MultiProcLogging(logger=flogger, format=self.__mpFormat, level=logging.DEBUG) as wlogger:
                numProc = 2
                chunkSize = 0
                optD = {}
                mpu = MultiProcUtil(verbose=True)
                mpu.setOptions(optionsD=optD)
                mpu.set(workerObj=self, workerMethod="workerOne")
                ok, failList, retLists, diagList = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1, chunkSize=chunkSize)
                self.assertEqual(len(failList), 0)
                self.assertTrue(ok)
                #
            fh.close()
            flogger.removeHandler(fh)

            #
            logLines = []
            with open(self.__testLogPath, 'r') as ifh:
                for line in ifh:
                    logLines.append(line)
            self.assertGreaterEqual(len(logLines), myLen)
            # for line in logLines:
            #    self.assertIn("context logging record", line)
        except Exception as e:
            logger.exception("context logging record %s" % str(e))
            self.fail()


def suiteContextManagerLogging():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MultiProcLoggingTests("testLogStream"))
    suiteSelect.addTest(MultiProcLoggingTests("testLogStringStream"))
    suiteSelect.addTest(MultiProcLoggingTests("testLogFileHandler"))
    suiteSelect.addTest(MultiProcLoggingTests("testLogStringPlusFileHandlers"))
    #
    return suiteSelect


def suiteMultiProcLogging():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MultiProcLoggingTests("testLogStringStreamMultiProc"))
    suiteSelect.addTest(MultiProcLoggingTests("testLogFileHandlerMultiProc"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = suiteContextManagerLogging()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if True:
        mySuite = suiteMultiProcLogging()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
