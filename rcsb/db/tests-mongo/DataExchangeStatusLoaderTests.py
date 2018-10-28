##
# File:    DataExchangeStatusLoaderTests.py
# Author:  J. Westbrook
# Date:    14-Jul-2018
# Version: 0.001
#
# Updates:
#
##
"""
Tests for loading data exchange status information.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class DataExchangeStatusLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(DataExchangeStatusLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.yml')
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        # self.__cfgOb.dump()
        self.__resourceName = "MONGO_DB"
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__documentLimit = 1000
        #
        # sample data set
        self.__updateId = '2018_23'
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testLoadExchangeStatus(self):
        """ Test case - load data exchange status objects.

        [data_exchange]
        DATABASE_NAME=data_exchange
        DATABASE_VERSION_STRING=v5
        COLLECTION_UPDATE_STATUS=rcsb_data_exchange_status
        COLLECTION_VERSION_STRING=v0_1

        """
        try:
            for ii in range(1, 100):
                collectionName = 'my_collection_' + str(ii)
                dList = []
                desp = DataExchangeStatus()
                tS = desp.setStartTime()
                self.assertGreaterEqual(len(tS), 15)
                ok = desp.setObject('my_database', collectionName)
                self.assertTrue(ok)
                ok = desp.setStatus(updateId=None, successFlag='Y')
                self.assertTrue(ok)
                #
                tS = desp.setEndTime()
                self.assertGreaterEqual(len(tS), 15)
                dList.append(desp.getStatus())
                #
                self.assertEqual(len(dList), 1)
                logger.debug("Status record %r" % dList[0])

                sectionName = 'data_exchange'
                dl = DocumentLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                    documentLimit=self.__documentLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
                #
                databaseName = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionName) + '_' + self.__cfgOb.get('DATABASE_VERSION_STRING', sectionName=sectionName)
                collectionVersion = self.__cfgOb.get('COLLECTION_VERSION_STRING', sectionName=sectionName)
                collectionName = self.__cfgOb.get('COLLECTION_UPDATE_STATUS', sectionName=sectionName) + '_' + collectionVersion
                if ii == 1:
                    loadType = 'full'
                else:
                    loadType = 'append'
                ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList,
                             indexAttributeList=['update_id', 'database_name', 'object_name'], keyNames=None)
                self.assertTrue(ok)
                #

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def exchangeStatusLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DataExchangeStatusLoaderTests("testLoadExchangeStatus"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = exchangeStatusLoadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
