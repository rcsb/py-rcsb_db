##
# File: RepoHoldingsRemoteDataPrep.py
# Author:  J. Westbrook
# Date:  12-Jul-2018
#
# Note:  This module uses well sandbox and repository defined file names
#        within the configuration defined RCSB exchange sandbox path.
#
# Update:
# 16-Jul-2018 jdw adjust naming to current sandbox conventions.
# 30-Oct-2018 jdw naming to conform to dictioanry and schema conventions.
#  8-Oct-2018 jdw adjustments for greater schema compliance and uniformity
#                 provide alternative date assignment for JSON and mongo validation
# 10-Oct-2018 jdw Add getHoldingsTransferred() for homology models transfered to Model Archive
# 28-Oct-2018 jdw update with semi-colon separated author lists in status and theoretical model files
# 25-Nov-2018 jdw add sequence/pdb_seq_prerelease.fasta and  _rcsb_repository_holdings_prerelease.seq_one_letter_code
# 27-Nov-2018 jdw update rcsb_repository_holdings_current for all entry content types
# 29-Nov-2018 jdw Add support for NMR restraint versions.
# 30-Nov-3018 jdw explicitly filter obsolete entries from current holdings
# 13-Dec-2018 jdw Adjust logic for reporting assembly format availibility
#  5-Feb-2020 jdw Drop superseded entries from the removed entry candidate list.
#                 Avoid overlap between current and removed/unreleased entries.
# 30-Apr-2020 jdw new NMR content types and support for config option RCSB_EDMAP_LIST_PATH
# 23-Oct-2020 jdw add getHoldingsCombined()
# 21-Sep-2021 jdw overhaul using new resource files and with support for remote access
#  4-Feb-2022 dwp Further overhaul for using new resource files and support for remote access
##

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging


from rcsb.utils.repository.CurrentHoldingsProvider import CurrentHoldingsProvider
from rcsb.utils.repository.RemovedHoldingsProvider import RemovedHoldingsProvider
from rcsb.utils.repository.UnreleasedHoldingsProvider import UnreleasedHoldingsProvider
from rcsb.utils.repository.UpdateHoldingsProvider import UpdateHoldingsProvider


logger = logging.getLogger(__name__)


class RepoHoldingsRemoteDataPrep(object):
    """Assemble repository current and update content holdings."""

    def __init__(self, cachePath=None, useCache=False, **kwargs):
        # filterType is passed along in kwargs to these supporting classes -
        # self.__filterType = kwargs.get("filterType", "")
        # self.__assignDates = "assign-dates" in self.__filterType
        #
        self.__cfgOb = kwargs.get("cfgOb", None)
        #
        self.__chP = CurrentHoldingsProvider(cachePath, useCache, **kwargs)
        self.__uphP = UpdateHoldingsProvider(cachePath, useCache, **kwargs)
        self.__unhP = UnreleasedHoldingsProvider(cachePath, useCache, **kwargs)
        self.__rmhP = RemovedHoldingsProvider(cachePath, useCache, **kwargs)
        self.__currentCacheD = None

    def getHoldingsUpdateEntry(self, updateId):
        dList = []
        retD = self.__uphP.getUpdateData()
        for entryId, qD in retD.items():
            tD = {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId}
            rD = {
                "rcsb_id": entryId,
                "rcsb_repository_holdings_update_entry_container_identifiers": tD,
                "rcsb_repository_holdings_update": qD,
            }
            dList.append(rD)
        return dList

    def getHoldingsCurrentEntry(self, updateId):
        dList = []
        retD = self.__currentCacheD if self.__currentCacheD else self.__getHoldingsCurrent()
        self.__currentCacheD = retD
        for entryId, qD in retD.items():
            tD = (
                {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId, "assembly_ids": qD["assembly_ids"]}
                if "assembly_ids" in qD
                else {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId}
            )
            rD = {
                "rcsb_id": entryId,
                "rcsb_repository_holdings_current_entry_container_identifiers": tD,
                "rcsb_repository_holdings_current": {"repository_content_types": qD["repository_content_types"]},
            }
            dList.append(rD)
        return dList

    def __getHoldingsCurrent(self):
        retD = {}
        rD, assemD = self.__chP.getRcsbContentAndAssemblies()
        for entryId in rD:
            if entryId in assemD:
                retD[entryId] = {"assembly_ids": assemD[entryId], "repository_content_types": rD[entryId]}
            else:
                retD[entryId] = {"repository_content_types": rD[entryId]}
        return retD

    def getHoldingsUnreleasedEntry(self, updateId):
        dList = []

        retD, prD = self.__unhP.getRcsbUnreleasedData()
        #
        currentD = self.__currentCacheD if self.__currentCacheD else self.__getHoldingsCurrent()
        self.__currentCacheD = currentD
        for entryId, qD in retD.items():
            if entryId in currentD:
                continue
            rD = {"rcsb_id": entryId}
            rD["rcsb_repository_holdings_unreleased_entry_container_identifiers"] = {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId}
            if entryId in prD:
                rD["rcsb_repository_holdings_prerelease"] = prD[entryId]
            rD["rcsb_repository_holdings_unreleased"] = qD
            #
            dList.append(rD)
        return dList

    def getHoldingsRemovedEntry(self, updateId):
        dList = []
        trfD, insD, aaD, rmvD, spsD = self.__rmhP.getRcsbRemovedData()
        #
        # rmvD, aaD, spsD = self.__getHoldingsRemoved(dirPath=dirPath)
        # trfD, insD = self.__getHoldingsTransferred(dirPath=dirPath)
        #
        currentD = self.__currentCacheD if self.__currentCacheD else self.__getHoldingsCurrent()
        self.__currentCacheD = currentD
        #
        # Get the list of candidate keys for removed entries -
        #
        entryIdL = sorted(set(list(insD.keys()) + list(rmvD.keys())))
        for entryId in entryIdL:
            if entryId in currentD:
                continue
            rD = {"rcsb_id": entryId}
            rD["rcsb_repository_holdings_removed_entry_container_identifiers"] = {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId}
            #
            if entryId in rmvD:
                rD["rcsb_repository_holdings_removed"] = rmvD[entryId]
            if entryId in aaD:
                rD["rcsb_repository_holdings_removed_audit_author"] = aaD[entryId]
            if entryId in spsD:
                rD["rcsb_repository_holdings_superseded"] = spsD[entryId]
            if entryId in trfD:
                rD["rcsb_repository_holdings_transferred"] = trfD[entryId]
            if entryId in insD:
                rD["rcsb_repository_holdings_insilico_models"] = insD[entryId]
            dList.append(rD)
        return dList

    def getHoldingsCombinedEntry(self, updateId):
        dList = []
        retD = self.__chP.getStatusDetails()
        umD = self.__unhP.getStatusDetails(retD)
        rmD = self.__rmhP.getStatusDetails(retD)
        retD.update(umD)
        retD.update(rmD)
        #
        for entryId, qD in retD.items():
            tD = {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId}
            rD = {
                "rcsb_id": entryId,
                "rcsb_repository_holdings_combined_entry_container_identifiers": tD,
                "rcsb_repository_holdings_combined": qD,
            }
            dList.append(rD)
        return dList

    #
