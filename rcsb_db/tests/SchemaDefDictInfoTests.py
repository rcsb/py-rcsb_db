# File:    SchemaDefDictInfoTests.py.py
# Author:  J. Westbrook
# Date:    22-May-2013
# Version: 0.001
#
# Update:
#  23-May-2018  jdw add preliminary default and helper tests
#
#
#
##
"""
Tests for extraction, supplementing and packaging dictionary metadata for schema construction.

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
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.schema.SchemaDefDictInfo import SchemaDefDictInfo
from rcsb_db.schema.DictInfoHelper import DictInfoHelper
from rcsb_db.utils.IoUtil import IoUtil


class SchemaDefDictInfoTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb_db', 'data', 'mmcif_pdbx_v5_next.dic')
        self.__ioU = IoUtil()
        #
        self.__pathSaveDefJson = os.path.join(HERE, 'test-output', 'schema_dict_info.json')
        self.__pathSaveDefText = os.path.join(HERE, 'test-output', 'schema_dict_info.txt')
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

    def testDefaults(self):
        """ Test the default case of using only dictionary content.
        """
        try:
            sdi = SchemaDefDictInfo(dictPath=self.__pathPdbxDictionaryFile)
            nS = sdi.getNameSchema()
            #
            logger.info("Dictionary category name length %d" % len(nS))
            ok = self.__ioU.serializeJson(self.__pathSaveDefJson, nS, indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testHelper(self):
        """ Test the dictionary content supplemented by helper function
        """
        try:
            dH = DictInfoHelper()
            sdi = SchemaDefDictInfo(dictPath=self.__pathPdbxDictionaryFile, dictSubset='chem_comp', dictHelper=dH)
            nS = sdi.getNameSchema()
            #
            logger.info("Dictionary category name length %d" % len(nS))
            ok = self.__ioU.serializeJson(self.__pathSaveDefJson, nS, indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def dictInfoDefaultSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDictInfoTests("testDefaults"))
    return suiteSelect


def dictInfoHelperSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDictInfoTests("testHelper"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = dictInfoDefaultSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = dictInfoHelperSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
