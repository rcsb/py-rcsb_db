# File:    DictInfoTests.py
# Author:  J. Westbrook
# Date:    22-May-2013
# Version: 0.001
#
# Update:
#  23-May-2018  jdw add preliminary default and helper tests
#   5-Jun-2018  jdw update prototypes for IoUtil() methods
#  13-Jun-2018  jdw add content classes
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


import logging
import os
import time
import unittest

from rcsb.db.define.DictInfo import DictInfo
from rcsb.db.helpers.DictInfoHelper import DictInfoHelper
from rcsb.utils.io.IoUtil import IoUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DictInfoTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'mmcif_pdbx_v5_next.dic')
        self.__pathRcsbDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'rcsb_mmcif_ext_v1.dic')
        self.__ioU = IoUtil()
        #
        self.__pathSaveDictInfoDefaultJson = os.path.join(HERE, 'test-output', 'dict_info_default.json')
        self.__pathSaveDictInfoJson = os.path.join(HERE, 'test-output', 'dict_info.json')
        self.__pathSaveDictInfoExtJson = os.path.join(HERE, 'test-output', 'dict_info_with_ext.json')
        self.__pathSaveDefText = os.path.join(HERE, 'test-output', 'dict_info.txt')
        #
        self.__startTime = time.time()
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
            sdi = DictInfo(dictLocators=[self.__pathPdbxDictionaryFile])
            nS = sdi.getNameSchema()
            #
            logger.debug("Dictionary category name length %d" % len(nS))
            ok = self.__ioU.serialize(self.__pathSaveDictInfoDefaultJson, nS, format="json", indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testHelper(self):
        """ Test the dictionary content supplemented by helper function

        """
        try:
            dH = DictInfoHelper()
            sdi = DictInfo(dictLocators=[self.__pathPdbxDictionaryFile], dictSubset='chem_comp', dictHelper=dH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)

            #
            logger.debug("Dictionary category name length %d" % len(catNameL))
            ok = self.__ioU.serialize(self.__pathSaveDictInfoJson, afD, format="json", indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testExtensionWithHelper(self):
        """ Test the dictionary content supplemented by helper function

        """
        try:
            dH = DictInfoHelper()
            sdi = DictInfo(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile], dictSubset='pdbx', dictHelper=dH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)

            #
            logger.debug("Dictionary category name length %d" % len(catNameL))
            ok = self.__ioU.serialize(self.__pathSaveDictInfoExtJson, afD, format="json", indent=3)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def dictInfoDefaultSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictInfoTests("testDefaults"))
    return suiteSelect


def dictInfoHelperSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictInfoTests("testHelper"))
    return suiteSelect


def dictInfoExtensionSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DictInfoTests("testExtensionWithHelper"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = dictInfoDefaultSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = dictInfoHelperSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = dictInfoExtensionSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
