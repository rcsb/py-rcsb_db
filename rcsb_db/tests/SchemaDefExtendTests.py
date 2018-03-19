##
# File:    SchemaDefDataPrepTests.py
# Author:  J. Westbrook
# Date:    13-Mar-2018
# Version: 0.001
#
# Updates:
#
#
#
##
"""
Tests for preparing loadable data based on external schema definition.

         No specific database conection depedencies -

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import os
import time
import unittest
import pprint
import json

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
#
from rcsb_db.schema.PdbxSchemaDef import PdbxSchemaDef
#
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.api.PdbxContainers import CifName


class SchemaDefDataPrepTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefDataPrepTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__verbose = True

    def setUp(self):
        self.__pathPdbxDictionary = os.path.join(TOPDIR, "rcsb_db", "data", "mmcif_pdbx_v5_next.dic")
        #
        self.__verbose = True
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testGetKeyRelationships(self):
        """Test case -  dump dicationary categories with unit cardinality
        """

        try:
            unitCardL = []
            myIo = IoAdapter(raiseExceptions=True)
            self.__containerList = myIo.readFile(inputFilePath=self.__pathPdbxDictionary)
            dApi = DictionaryApi(containerList=self.__containerList, consolidate=True, expandItemLinked=False, verbose=self.__verbose)
            for itemName in ['entry.id', '_chem_comp.id']:
                categoryName = CifName.categoryPart(itemName)
                attributeName = CifName.attributePart(itemName)
                childItemList = dApi.getFullChildList(categoryName, attributeName)
                logger.info("Full child  list for  %s : %s\n" % (itemName, childItemList))
                for childItem in childItemList:
                    childCategoryName = CifName.categoryPart(childItem)
                    pKyL = dApi.getCategoryKeyList(childCategoryName)
                    if len(pKyL) == 1:
                        logger.info("Primary key list : %r" % pKyL)
                        unitCardL.append(CifName.categoryPart(pKyL[0]))
                logger.info("Unit Card Categories : %r" % unitCardL)
                self.assertIsNotNone(dApi.getTypeCode(categoryName, attributeName))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testGetIterableItems(self):
        """Test case -  dump dictionary items with iterable types
        """

        try:
            iTypeList = ['ucode-alphanum-csv', 'id_list']
            iL = []
            myIo = IoAdapter(raiseExceptions=True)
            self.__containerList = myIo.readFile(inputFilePath=self.__pathPdbxDictionary)
            dApi = DictionaryApi(containerList=self.__containerList, consolidate=True, expandItemLinked=False, verbose=self.__verbose)
            for categoryName in dApi.getCategoryList():
                for attributeName in dApi.getAttributeNameList(categoryName):
                    tc = dApi.getTypeCode(categoryName, attributeName)
                    tcA = dApi.getTypeCodeAlt(categoryName, attributeName)
                    dt = dApi.getDescription(categoryName, attributeName)
                    if tc in iTypeList or tcA in iTypeList or 'comma separate' in dt.lower():
                        iL.append((categoryName.upper(), attributeName.upper(), ','))

            logger.info("Iterables : %r" % iL)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testFindSchemaFeatures(self):
        """Test case -  create loadable BIRD data from files
        """
        try:
            sd = PdbxSchemaDef(convertNames=True)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def schemaFeaturesSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefDataPrepTests("testGetKeyRelationships"))
    suiteSelect.addTest(SchemaDefDataPrepTests("testFindSchemaFeatures"))
    suiteSelect.addTest(SchemaDefDataPrepTests("testGetIterableItems"))
    return suiteSelect

if __name__ == '__main__':
    #
    if True:
        mySuite = schemaFeaturesSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
