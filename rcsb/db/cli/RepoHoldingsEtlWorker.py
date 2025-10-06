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
#  16-Oct-2024 dwp Remove usage of EDMAPS holdings file
#  18-Feb-2025 dwp Add support for IHM repository holdings file loading;
#                  Change indexed field to be 'rcsb_id'
#  15-Jul-2025 dwp Adjust loader to load holdings data to DW repository_holdings collections;
#                  Turn off loading of repository_holdings_update_entry (since not used by anything downstream);
#                  Use indexes defined in py-rcsb_exdb_assets schemas and configuration
#   6-Aug-2025 dwp Make use of schema configuration file for loading collections and setting indexed fields
#   6-Oct-2025 dwp Turned OFF loading of "repository_holdings_update_entry" collection as part of transition to DW consolidation (since not used by anything)
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
from rcsb.db.utils.SchemaProvider import SchemaProvider

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
        #
        self.__collectionGroupName = "repository_holdings"
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath)
        self.__databaseNameMongo = self.__schP.getDatabaseMongoName(collectionGroupName=self.__collectionGroupName)

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

    def load(self, updateId):
        # First load PDB holdings (with loadType="full")
        ok1 = self.loadRepoType(updateId, loadType="full", repoType="pdb")
        #
        # Next load IHM holdings (with loadType="replace")
        ok2 = self.loadRepoType(updateId, loadType="replace", repoType="pdb_ihm")
        #
        # Last, verify all pdbx_core data has been loaded (based on repository holdings)
        ok3 = self.verifyCompleteLoad()
        logger.info("Verification of complete load status %r", ok3)
        #
        ok = ok1 and ok2 and ok3
        return ok

    def loadRepoType(self, updateId, loadType="full", repoType="pdb"):
        """Load legacy repository holdings and status data -
        """
        try:
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            # ---
            discoveryMode = self.__cfgOb.get("DISCOVERY_MODE", sectionName=self.__cfgSectionName, default="local")
            baseUrlPDB = self.__cfgOb.getPath("PDB_REPO_URL", sectionName=self.__cfgSectionName, default="https://files.wwpdb.org/pub")
            fallbackUrlPDB = self.__cfgOb.getPath("PDB_REPO_FALLBACK_URL", sectionName=self.__cfgSectionName, default="https://files.wwpdb.org/pub")
            # addValues = {"_schema_version": collectionVersion}
            addValues = None
            #
            kwD = {
                "repoType": repoType,  # either "pdb" or "pdb_ihm"
                "holdingsTargetUrl": os.path.join(baseUrlPDB, repoType, "holdings"),
                "holdingsFallbackUrl": os.path.join(fallbackUrlPDB, repoType, "holdings"),
                "updateTargetUrl": os.path.join(baseUrlPDB, repoType, "data", "status", "latest"),
                "updateFallbackUrl": os.path.join(fallbackUrlPDB, repoType, "data", "status", "latest"),
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
            _, _, collectionNameList, docIndexD = self.__schP.getSchemaInfo(collectionGroupName=self.__collectionGroupName, dataTyping="ANY")
            collectionNameList = [cN for cN in collectionNameList if "_update_entry" not in cN]  # Turned OFF loading "update" collection in OCT 2025 for transition to DW loading
            # ['repository_holdings_combined_entry', 'repository_holdings_current_entry', 'repository_holdings_unreleased_entry', 'repository_holdings_removed_entry']
            logger.info("RepoHoldings collectionNameList: %r", collectionNameList)

            ok = True
            for collectionName in collectionNameList:
                indexDL = docIndexD[collectionName] if collectionName in docIndexD else []
                dList = self.__getHoldingsDocList(repoType, collectionName, rhdp, updateId)
                if dList:
                    ok = dl.load(self.__databaseNameMongo, collectionName, loadType=loadType, documentList=dList, keyNames=None, addValues=addValues, indexDL=indexDL) and ok
                self.__updateStatus(updateId, self.__databaseNameMongo, collectionName, ok, statusStartTimestamp)
                logger.info(
                    "Completed load of repository holdings for repoType %r, database %r, collection %r, len(dList) %r (status %r)",
                    repoType, self.__databaseNameMongo, collectionName, len(dList), ok
                )
            logger.info("Completed load of repository holdings for repoType %r, loadType %r (status %r)", repoType, loadType, ok)
            return ok
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

        repoHoldingsCurrentEntryL = self.__getUniqueLoadedRcsbIds(self.__databaseNameMongo, "repository_holdings_current_entry")
        repoHoldingsCombinedEntryL = self.__getUniqueLoadedRcsbIds(self.__databaseNameMongo, "repository_holdings_combined_entry")
        repoHoldingsRemovedEntryL = self.__getUniqueLoadedRcsbIds(self.__databaseNameMongo, "repository_holdings_removed_entry")
        repoHoldingsUnreleasedEntryL = self.__getUniqueLoadedRcsbIds(self.__databaseNameMongo, "repository_holdings_unreleased_entry")

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

    def __getHoldingsDocList(self, repoType, holdingsCollectionName, repoHoldingsDataPrep, updateId):
        dList = []
        if "_current_" in holdingsCollectionName.lower():
            dList = repoHoldingsDataPrep.getHoldingsCurrentEntry(updateId=updateId)
        elif "_unreleased_" in holdingsCollectionName.lower():
            dList = repoHoldingsDataPrep.getHoldingsUnreleasedEntry(updateId=updateId)
        elif "_combined_" in holdingsCollectionName.lower():
            dList = repoHoldingsDataPrep.getHoldingsCombinedEntry(updateId=updateId)
        elif repoType != "pdb_ihm" and "_removed_" in holdingsCollectionName.lower():
            dList = repoHoldingsDataPrep.getHoldingsRemovedEntry(updateId=updateId)
        # elif repoType != "pdb_ihm" and "_update_" in holdingsCollectionName.lower():  # TURNED OFF 15-JUL-2025 for transition to DW loading (since not used by anything)
        #     dList = repoHoldingsDataPrep.getHoldingsUpdateEntry(updateId=updateId)
        return dList
