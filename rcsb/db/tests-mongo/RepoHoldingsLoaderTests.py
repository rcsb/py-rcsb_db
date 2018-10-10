##
# File:    RepoHoldingsLoaderTests.py
# Author:  J. Westbrook
# Date:    13-Jul-2018
# Version: 0.001
#
# Updates:
# 14-Jul-2018 jdw add configuration options
#  7-Oct-2018 jdw add schema validation to the underlying load processing
##
"""
Tests for loading repository holdings information.

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
from rcsb.db.processors.RepoHoldingsDataPrep import RepoHoldingsDataPrep
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RepoHoldingsLoaderTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(RepoHoldingsLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName, mockTopPath=mockTopPath)
        # self.__cfgOb.dump()
        self.__resourceName = "MONGO_DB"
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__documentLimit = 1000
        self.__filterType = "assign-dates"
        #
        self.__workPath = os.path.join(HERE, 'test-output')
        self.__sandboxPath = self.__cfgOb.getPath('RCSB_EXCHANGE_SANDBOX_PATH', sectionName='DEFAULT')
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

    def testLoadHoldings(self):
        """ Test case - load legacy repository holdings and status data -

        [repository_holdings]
        DATABASE_NAME=repository_holdings
        DATABASE_VERSION_STRING=v5
        COLLECTION_HOLDINGS_UPDATE=rcsb_repository_holdings_update
        COLLECTION_HOLDINGS_CURRENT=rcsb_repository_holdings_current
        COLLECTION_HOLDINGS_UNRELEASED=rcsb_repository_holdings_unreleased
        COLLECTION_HOLDINGS_REMOVED=rcsb_repository_holdings_removed
        COLLECTION_HOLDINGS_REMOVED_AUTHORS=rcsb_repository_holdings_removed_audit_authors
        COLLECTION_HOLDINGS_SUPERSEDED=rcsb_repository_holdings_superseded
        COLLECTION_VERSION_STRING=v0_1

        """
        try:
            sectionName = 'repository_holdings'
            rhdp = RepoHoldingsDataPrep(sandboxPath=self.__sandboxPath, workPath=self.__workPath, filterType=self.__filterType)
            #
            dl = DocumentLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                documentLimit=self.__documentLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            #
            databaseName = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionName) + '_' + self.__cfgOb.get('DATABASE_VERSION_STRING', sectionName=sectionName)
            collectionVersion = self.__cfgOb.get('COLLECTION_VERSION_STRING', sectionName=sectionName)
            #
            dList = rhdp.getHoldingsUpdate(updateId=self.__updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_UPDATE', sectionName=sectionName) + '_' + collectionVersion
            ok = dl.load(databaseName, collectionName, loadType='append', documentList=dList,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None)
            logger.info("Collection %r length %d load status %r" % (collectionName, len(dList), ok))
            self.assertTrue(ok)
            #
            dList = rhdp.getHoldingsCurrent(updateId=self.__updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_CURRENT', sectionName=sectionName) + '_' + collectionVersion
            ok = dl.load(databaseName, collectionName, loadType='append', documentList=dList,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None)
            logger.info("Collection %r length %d load status %r" % (collectionName, len(dList), ok))
            self.assertTrue(ok)

            dList = rhdp.getHoldingsUnreleased(updateId=self.__updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_UNRELEASED', sectionName=sectionName) + '_' + collectionVersion
            ok = dl.load(databaseName, collectionName, loadType='append', documentList=dList,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None)
            logger.info("Collection %r length %d load status %r" % (collectionName, len(dList), ok))
            self.assertTrue(ok)
            #
            dList1, dList2, dList3 = rhdp.getHoldingsRemoved(updateId=self.__updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_REMOVED', sectionName=sectionName) + '_' + collectionVersion
            ok = dl.load(databaseName, collectionName, loadType='append', documentList=dList1,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None)
            logger.info("Collection %r length %d load status %r" % (collectionName, len(dList1), ok))
            self.assertTrue(ok)

            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_REMOVED_AUTHORS', sectionName=sectionName) + '_' + collectionVersion
            ok = dl.load(databaseName, collectionName, loadType='append', documentList=dList2,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None)
            logger.info("Collection %r length %d load status %r" % (collectionName, len(dList2), ok))
            #
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_SUPERSEDED', sectionName=sectionName) + '_' + collectionVersion
            ok = dl.load(databaseName, collectionName, loadType='append', documentList=dList3,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None)
            logger.info("Collection %r length %d load status %r" % (collectionName, len(dList3), ok))
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def holdingsLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepoHoldingsLoaderTests("testLoadHoldings"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = holdingsLoadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
