##
# File: RepoHoldingsEtlWorker.py
# Date: 2-Jul-2018  jdw
#
# ETL utilities for processing repository holding data.
# Updates:
#  15-Jul-2018 jdw split out to separate module and add status tracking
#  26-Nov-2018 jdw add COLLECTION_HOLDINGS_PRERELEASE
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.processors.RepoHoldingsDataPrep import RepoHoldingsDataPrep

logger = logging.getLogger(__name__)


class RepoHoldingsEtlWorker(object):
    """ Prepare and load repository holdings and repository update data.
    """

    def __init__(self, cfgOb, sandboxPath, workPath=None, numProc=2, chunkSize=10, readBackCheck=False, documentLimit=None, verbose=False):
        self.__cfgOb = cfgOb
        self.__sandboxPath = sandboxPath
        self.__workPath = workPath
        self.__readBackCheck = readBackCheck
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        self.__documentLimit = documentLimit
        self.__resourceName = "MONGO_DB"
        self.__filterType = "assign-dates"
        self.__verbose = verbose
        self.__statusList = []

    def __updateStatus(self, updateId, databaseName, collectionName, status, startTimestamp):
        try:
            sFlag = 'Y' if status else 'N'
            desp = DataExchangeStatus()
            desp.setStartTime(tS=startTimestamp)
            desp.setObject(databaseName, collectionName)
            desp.setStatus(updateId=updateId, successFlag=sFlag)
            desp.setEndTime()
            self.__statusList.append(desp.getStatus())
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def load(self, updateId, loadType='full'):
        """ Load legacy repository holdings and status data -

        Relevant configuration options:

        [DEFAULT]
        RCSB_EXCHANGE_SANDBOX_PATH=MOCK_EXCHANGE_SANDBOX

        [repository_holdings]
        DATABASE_NAME=repository_holdings
        DATABASE_VERSION_STRING=v5
        COLLECTION_HOLDINGS_UPDATE=rcsb_repository_holdings_update
        COLLECTION_HOLDINGS_CURRENT=rcsb_repository_holdings_current
        COLLECTION_HOLDINGS_UNRELEASED=rcsb_repository_holdings_unreleased
        COLLECTION_HOLDINGS_PRERELEASE=rcsb_repository_holdings_prerelease
        COLLECTION_HOLDINGS_REMOVED=rcsb_repository_holdings_removed
        COLLECTION_HOLDINGS_REMOVED_AUTHORS=rcsb_repository_holdings_removed_audit_authors
        COLLECTION_HOLDINGS_SUPERSEDED=rcsb_repository_holdings_superseded
        COLLECTION_VERSION_STRING=v0_1

        """
        try:
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()

            sectionName = 'repository_holdings'

            rhdp = RepoHoldingsDataPrep(sandboxPath=self.__sandboxPath, workPath=self.__workPath, filterType=self.__filterType)
            #
            dl = DocumentLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                documentLimit=self.__documentLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            #
            databaseName = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionName)
            collectionVersion = self.__cfgOb.get('COLLECTION_VERSION_STRING', sectionName=sectionName)
            addValues = {'_schema_version': collectionVersion}
            #
            dList = rhdp.getHoldingsUpdate(updateId=updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_UPDATE', sectionName=sectionName)

            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)
            #
            dList = rhdp.getHoldingsCurrent(updateId=updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_CURRENT', sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            dList = rhdp.getHoldingsUnreleased(updateId=updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_UNRELEASED', sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)
            #
            dList = rhdp.getHoldingsPrerelease(updateId=updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_PRERELEASE', sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)
            #
            dList1, dList2, dList3 = rhdp.getHoldingsRemoved(updateId=updateId)
            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_REMOVED', sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList1,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_REMOVED_AUTHORS', sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList2,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_SUPERSEDED', sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList3,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

#
            dList1, dList2 = rhdp.getHoldingsTransferred(updateId=updateId)

            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_TRANSFERRED', sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList1,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            collectionName = self.__cfgOb.get('COLLECTION_HOLDINGS_INSILICO_MODELS', sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList2,
                         indexAttributeList=['update_id', 'entry_id'], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def getLoadStatus(self):
        return self.__statusList
