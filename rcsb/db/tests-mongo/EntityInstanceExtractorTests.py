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
        self.__documentLimit = 1000
        self.__filterType = "assign-dates"
        #
        self.__workPath = os.path.join(HERE, 'test-output')
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testExtractEntityInstance(self):
        """ Test case - extract entity instance data -

        """
        try:
            eiExt = EntityInstanceExtractor(self.__cfgOb)
            entryD = eiExt.getEntryInfo()
            logger.info(">>entryD %r" % entryD)
            self.assertTrue(len(entryD) > 0)
            #entityD = eiExt.getEntityIds(entryIdL)
            # self.assertTrue(len(entityD)>0)
            entryD = eiExt.getPolymerEntities(entryD)
            self.assertTrue(len(entryD) > 0)
            #
            entryD = eiExt.getEntityInstances(entryD)
            self.assertTrue(len(entryD) > 0)
            #
            eId = '3RER'
            logger.info(">>>>>> entryD %s %r" % (eId, entryD[eId]))
            for entryId, topD in entryD.items():
                for entityId, eD in topD['selected_polymer_entities'].items():
                    eiExt.analEntity(eD)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def entityInstanceExtractSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemRefLoaderTests("testExtractEntityInstance"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = entityInstanceExtractSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
