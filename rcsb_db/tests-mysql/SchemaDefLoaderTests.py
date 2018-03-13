##
# File:    SchemaMapLoaderTests.py
# Author:  J. Westbrook
# Date:    7-Jan-2013
# Version: 0.001
#
# Update:
#
##
"""
Tests for loading instance data using schema definition.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import unittest
import os
import time

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

from rcsb_db.loaders.SchemaDefLoader import SchemaDefLoader
from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef

from mmcif.io.IoAdapterPy import IoAdapterPy


class SchemaDefLoaderTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = False
        self.__loadPathList = [os.path.join(TOPDIR, "rcsb_db", "data", "PRD_000001.cif"), os.path.join(TOPDIR, "rcsb_db", "data", "PRD_000012.cif")]
        self.__ioObj = IoAdapterPy(verbose=self.__verbose)
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testLoadFile(self):
        """Test case - for loading BIRD definition data files
        """
        try:
            bsd = BirdSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=bsd,
                                  ioObj=self.__ioObj,
                                  dbCon=None,
                                  workPath=os.path.join(HERE, 'test-output'),
                                  cleanUp=False,
                                  warnings='default',
                                  verbose=self.__verbose)
            containerNameList, tList = sml.makeLoadFiles(self.__loadPathList)
            for tId, fn in tList:
                logger.debug("\nCreated table %s load file %s\n" % (tId, fn))
            self.assertGreaterEqual(len(tList), 9)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def loadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderTests("testLoadFile"))
    return suiteSelect

if __name__ == '__main__':
    #
    mySuite = loadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
