##
# File:    MongoDbLoaderWorkerTests.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#
##
"""
Tests for creating and loading MongoDb using BIRD, CCD and PDBx/mmCIF data files
and following external schema definitions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import os
import time
import unittest
import scandir
import pprint

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.mongo.MongoDbLoaderWorker import MongoDbLoaderWorker


class MongoDbLoaderWorkerTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(MongoDbLoaderWorkerTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__configPath = 'dbload-setup.cfg'
        self.__configName = 'DEFAULT'
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__fileLimit = 300
        self.__documentStyle = 'rowwise_by_name_with_cardinality'
        #
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testLoadChemCompReference(self):
        """ Test case -  Load chemical component reference data
        """
        try:
            mw = MongoDbLoaderWorker(self.__configPath, self.__configName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                     fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            ok = mw.loadContentType('chem-comp', styleType=self.__documentStyle, contentSelectors=["CHEM_COMP_PUBLIC_RELEASE"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdChemCompReference(self):
        """ Test case -  Load Bird chemical component reference data
        """
        try:
            mw = MongoDbLoaderWorker(self.__configPath, self.__configName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                     fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            ok = mw.loadContentType('bird-chem-comp', styleType=self.__documentStyle, contentSelectors=["CHEM_COMP_PUBLIC_RELEASE"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdReference(self):
        """ Test case -  Load Bird reference data
        """
        try:
            mw = MongoDbLoaderWorker(self.__configPath, self.__configName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                     fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            ok = mw.loadContentType('bird', styleType=self.__documentStyle, contentSelectors=["BIRD_PUBLIC_RELEASE"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdFamilyReference(self):
        """ Test case -  Load Bird family reference data
        """
        try:
            mw = MongoDbLoaderWorker(self.__configPath, self.__configName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                     fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            ok = mw.loadContentType('bird-family', styleType=self.__documentStyle, contentSelectors=["BIRD_FAMILY_PUBLIC_RELEASE"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadPdbxEntryData(self):
        """ Test case -  Load PDBx entry data
        """

        try:
            mw = MongoDbLoaderWorker(self.__configPath, self.__configName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                     fileLimit=self.__fileLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            ok = mw.loadContentType('pdbx', styleType=self.__documentStyle)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def mongoLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MongoDbLoaderWorkerTests("testLoadChemCompReference"))
    suiteSelect.addTest(MongoDbLoaderWorkerTests("testLoadBirdChemCompReference"))
    suiteSelect.addTest(MongoDbLoaderWorkerTests("testLoadBirdReference"))
    suiteSelect.addTest(MongoDbLoaderWorkerTests("testLoadBirdFamilyReference"))
    suiteSelect.addTest(MongoDbLoaderWorkerTests("testLoadPdbxEntryData"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = mongoLoadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
