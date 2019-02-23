##
# File:    EntityInstanceExtractorTests.py
# Author:  J. Westbrook
# Date:    19-Dec-2019
#
# Updates:
#
##
"""
Tests for extractor selected values from entity instance collections

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.mongo.EntityInstanceExtractor import EntityInstanceExtractor
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ChemRefLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(ChemRefLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.yml')
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        # self.__cfgOb.dump()
        self.__resourceName = "MONGO_DB"
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__documentLimit = None
        self.__filterType = "assign-dates"
        #
        #
        self.__workPath = os.path.join(HERE, 'test-output')
        self.__entitySavePath = os.path.join(HERE, 'test-output', 'entity-data-dictionary.pic')
        self.__entrySavePath = os.path.join(HERE, 'test-output', 'entry-data-dictionary.pic')
        self.__instanceSavePath = os.path.join(HERE, 'test-output', 'instance-data-dictionary.pic')
        self.__mU = MarshalUtil()
        self.__entryLimit = None
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testExtractEntityPolymers(self):
        """ Test case - extract entity instance data -

        """
        try:
            eiExt = EntityInstanceExtractor(self.__cfgOb)
            entryD = eiExt.getEntryInfo()
            self.assertTrue(len(entryD) > 0)
            ok = self.__mU.doExport(self.__entrySavePath, entryD, format='pickle')
            self.assertTrue(ok)
            #
            logger.info('EntryD length %d' % len(entryD))
            #
            entryD = self.__mU.doImport(self.__entrySavePath, format='pickle')
            #
            entryD = eiExt.getPolymerEntities(entryD, savePath=self.__entitySavePath, entryLimit=None)
            self.assertTrue(len(entryD) > 0)
            logger.info('EntryD + polymer entities length %d' % len(entryD))
            #
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testExtractEntityInstances(self):
        """ Test case - extract entity instance data -

        """
        try:
            eiExt = EntityInstanceExtractor(self.__cfgOb)
            entryD = self.__mU.doImport(self.__entitySavePath, format="pickle")
            #
            entryD = eiExt.getEntityInstances(entryD, savePath=self.__instanceSavePath, entryLimit=self.__entryLimit)
            self.assertTrue(len(entryD) > 0)
            ok = self.__mU.doExport(self.__instanceSavePath, entryD, format='pickle')
            self.assertTrue(ok)
            logger.info('EntryD + polymer entities instances length %d' % len(entryD))
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testAnalEntityInstances(self):
        """ Test case - extract entity instance data -

        """
        try:
            entryD = self.__mU.doImport(self.__instanceSavePath, format="pickle")
            for entryId in entryD:
                for entityId, eD in entryD[entryId]['selected_polymer_entities'].items():

                    analD = eD['anal_instances'] if 'anal_instances' in eD else {}
                    for asymId, aD in analD.items():
                        logger.info("entryId %s entityId %s asymId %s analD: %r" % (entryId, entityId, asymId, aD))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def entityInstanceExtractSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemRefLoaderTests("testExtractEntityPolymers"))
    suiteSelect.addTest(ChemRefLoaderTests("testExtractEntityInstances"))
    suiteSelect.addTest(ChemRefLoaderTests("testAnalEntityInstances"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = entityInstanceExtractSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
