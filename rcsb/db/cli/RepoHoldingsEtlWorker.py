##
# File: RepoHoldingsEtlWorker.py
# Date: 2-Jul-2018  jdw
#
# ETL utilities for processing repository holding data.
# Updates:
#  15-Jul-2018 jdw split out to separate module and add status tracking
#  26-Nov-2018 jdw add COLLECTION_HOLDINGS_PRERELEASE
#  14-Dec-2019 jdw reorganize and consolidate repository_holdings collections
#  25-Sep-2021 jdw substitute RepoHoldingsRemoteDataPrep() for data processing
#   4-Apr-2023 dwp Add maxStepLength input argument; update success/failure catching logic;
#                  add final verification step to ensure all entries were loaded
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.processors.RepoHoldingsDataPrep import RepoHoldingsDataPrep
from rcsb.db.processors.RepoHoldingsRemoteDataPrep import RepoHoldingsRemoteDataPrep
from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil

logger = logging.getLogger(__name__)


class RepoHoldingsEtlWorker(object):
    """Prepare and load repository holdings and repository update data."""

    def __init__(self, cfgOb, sandboxPath, cachePath, numProc=2, chunkSize=10, maxStepLength=4000, readBackCheck=False, documentLimit=None, verbose=False):
        self.__cfgOb = cfgOb
        self.__cfgSectionName = self.__cfgOb.getDefaultSectionName()
        self.__sandboxPath = sandboxPath
        self.__cachePath = cachePath
        self.__readBackCheck = readBackCheck
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        self.__maxStepLength = maxStepLength
        self.__documentLimit = documentLimit
        self.__resourceName = "MONGO_DB"
        self.__filterType = "assign-dates"
        self.__verbose = verbose
        self.__statusList = []

    def __updateStatus(self, updateId, databaseName, collectionName, status, startTimestamp):
        try:
            sFlag = "Y" if status else "N"
            desp = DataExchangeStatus()
            desp.setStartTime(tS=startTimestamp)
            desp.setObject(databaseName, collectionName)
            desp.setStatus(updateId=updateId, successFlag=sFlag)
            desp.setEndTime()
            self.__statusList.append(desp.getStatus())
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def load(self, updateId, loadType="full"):
        """Load legacy repository holdings and status data -

        Relevant configuration options:

        [DEFAULT]
        RCSB_EXCHANGE_SANDBOX_PATH=MOCK_EXCHANGE_SANDBOX

        [repository_holdings_configuration]
        DATABASE_NAME=repository_holdings
        DATABASE_VERSION_STRING=v5
        COLLECTION_HOLDINGS_UPDATE=rcsb_repository_holdings_update_entry
        COLLECTION_HOLDINGS_CURRENT=rcsb_repository_holdings_current_entry
        COLLECTION_HOLDINGS_UNRELEASED=rcsb_repository_holdings_unreleased_entry
        COLLECTION_HOLDINGS_REMOVED=rcsb_repository_holdings_removed_entry
        COLLECTION_VERSION_STRING=v0_1

        """
        try:
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()

            discoveryMode = self.__cfgOb.get("DISCOVERY_MODE", sectionName=self.__cfgSectionName, default="local")
            # ---
            baseUrlPDB = self.__cfgOb.getPath("PDB_REPO_URL", sectionName=self.__cfgSectionName, default="https://files.wwpdb.org/pub")
            fallbackUrlPDB = self.__cfgOb.getPath("PDB_REPO_FALLBACK_URL", sectionName=self.__cfgSectionName, default="https://files.wwpdb.org/pub")
            edMapUrl = self.__cfgOb.getPath("RCSB_EDMAP_LIST_PATH", sectionName=self.__cfgSectionName, default=None)
            #
            kwD = {
                "holdingsTargetUrl": os.path.join(baseUrlPDB, "pdb", "holdings"),
                "holdingsFallbackUrl": os.path.join(fallbackUrlPDB, "pdb", "holdings"),
                "edmapsLocator": edMapUrl,
                "updateTargetUrl": os.path.join(baseUrlPDB, "pdb", "data", "status", "latest"),
                "updateFallbackUrl": os.path.join(fallbackUrlPDB, "pdb", "data", "status", "latest"),
                "filterType": self.__filterType,
            }
            # ---
            if discoveryMode == "local":
                rhdp = RepoHoldingsDataPrep(cfgOb=self.__cfgOb, sandboxPath=self.__sandboxPath, cachePath=self.__cachePath, filterType=self.__filterType)
            else:
                rhdp = RepoHoldingsRemoteDataPrep(cachePath=self.__cachePath, **kwD)
            #
            dl = DocumentLoader(
                self.__cfgOb,
                self.__cachePath,
                self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                maxStepLength=self.__maxStepLength,
                documentLimit=self.__documentLimit,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
            )
            #
            sectionName = "repository_holdings_configuration"
            databaseName = self.__cfgOb.get("DATABASE_NAME", sectionName=sectionName)
            # collectionVersion = self.__cfgOb.get("COLLECTION_VERSION_STRING", sectionName=sectionName)
            # addValues = {"_schema_version": collectionVersion}
            addValues = None
            #
            dList = rhdp.getHoldingsUpdateEntry(updateId=updateId)
            collectionName = self.__cfgOb.get("COLLECTION_HOLDINGS_UPDATE", sectionName=sectionName)
            ok1 = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList, indexAttributeList=["update_id", "entry_id"], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok1, statusStartTimestamp)
            #
            dList = rhdp.getHoldingsCurrentEntry(updateId=updateId)
            collectionName = self.__cfgOb.get("COLLECTION_HOLDINGS_CURRENT", sectionName=sectionName)
            ok2 = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList, indexAttributeList=["update_id", "entry_id"], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok2, statusStartTimestamp)

            dList = rhdp.getHoldingsUnreleasedEntry(updateId=updateId)
            collectionName = self.__cfgOb.get("COLLECTION_HOLDINGS_UNRELEASED", sectionName=sectionName)
            ok3 = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList, indexAttributeList=["update_id", "entry_id"], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok3, statusStartTimestamp)
            #
            dList = rhdp.getHoldingsRemovedEntry(updateId=updateId)
            collectionName = self.__cfgOb.get("COLLECTION_HOLDINGS_REMOVED", sectionName=sectionName)
            ok4 = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList, indexAttributeList=["update_id", "entry_id"], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok4, statusStartTimestamp)
            #
            dList = rhdp.getHoldingsCombinedEntry(updateId=updateId)
            collectionName = self.__cfgOb.get("COLLECTION_HOLDINGS_COMBINED", sectionName=sectionName)
            ok5 = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList, indexAttributeList=["update_id", "entry_id"], keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok5, statusStartTimestamp)
            #
            ok6 = self.verifyCompleteLoad()
            logger.info("Verification of complete load status %r", ok6)
            #
            return ok1 and ok2 and ok3 and ok4 and ok5 and ok6
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def getLoadStatus(self):
        return self.__statusList

    def verifyCompleteLoad(self):
        """
        Compare the document counts of the pdbx_core and repository holdings collections.
        """
        ok = True

        pdbxCoreEntryL = self.__getUniqueLoadedRcsbIds("pdbx_core", "pdbx_core_entry")

        repoHoldingsCurrentEntryL = self.__getUniqueLoadedRcsbIds("repository_holdings", "repository_holdings_current_entry")
        repoHoldingsCombinedEntryL = self.__getUniqueLoadedRcsbIds("repository_holdings", "repository_holdings_combined_entry")
        repoHoldingsRemovedEntryL = self.__getUniqueLoadedRcsbIds("repository_holdings", "repository_holdings_removed_entry")
        repoHoldingsUnreleasedEntryL = self.__getUniqueLoadedRcsbIds("repository_holdings", "repository_holdings_unreleased_entry")

        combinedHoldingsExpectedCount = len(repoHoldingsCurrentEntryL) + len(repoHoldingsRemovedEntryL) + len(repoHoldingsUnreleasedEntryL)

        if len(pdbxCoreEntryL) != len(repoHoldingsCurrentEntryL):
            logger.error(
                "The total entries in the collections of core entry (%d) and current repository holdings (%d) differ.",
                len(pdbxCoreEntryL),
                len(repoHoldingsCurrentEntryL)
            )
            delta = list(set(pdbxCoreEntryL) ^ set(repoHoldingsCurrentEntryL))
            logger.error("List delta is of length %r", len(delta))
            if len(delta) > 0 and len(delta) < 10000:
                logger.error("Delta entry ID list: %r", delta)
            ok = False
        if len(repoHoldingsCombinedEntryL) != combinedHoldingsExpectedCount:
            logger.error(
                "The total entries in repoHoldingsCombinedEntryL (%d) and combinedHoldingsExpectedCount (%d) differ.",
                len(repoHoldingsCombinedEntryL),
                combinedHoldingsExpectedCount
            )
            ok = False

        if not ok:
            raise ValueError("Data Errors found in ExchangeDB loading")

        return ok

    def __getUniqueLoadedRcsbIds(self, databaseName, collectionName):
        """Get the list of unique loaded 'rcsb_id' values in the given database and collection"""
        loadedRcsbIdL = []
        try:
            #
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                selectL = ["rcsb_id"]
                queryD = {}
                loadedDocL = mg.fetch(databaseName, collectionName, selectL, queryD=queryD, suppressId=True)
                loadedRcsbIdL = [docD["rcsb_id"] for docD in loadedDocL]
                loadedRcsbIdL = list(set(loadedRcsbIdL))
                logger.info("Number of entries loaded to database %s collection %s: %r", databaseName, collectionName, len(loadedRcsbIdL))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return loadedRcsbIdL
