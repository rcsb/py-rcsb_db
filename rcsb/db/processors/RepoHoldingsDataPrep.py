##
# File: RepoHoldingsDataPrep.py
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
##

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os

import dateutil.parser
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class RepoHoldingsDataPrep(object):
    """Consolidate legacy data describing repository content updates and repository entry status."""

    def __init__(self, **kwargs):
        self.__cfgOb = kwargs.get("cfgOb", None)
        self.__cachePath = kwargs.get("cachePath", None)
        self.__sandboxPath = kwargs.get("sandboxPath", None)
        self.__filterType = kwargs.get("filterType", "")
        self.__assignDates = "assign-dates" in self.__filterType
        #
        self.__mU = MarshalUtil(workPath=self.__cachePath)
        self.__currentCacheD = None
        #

    def getHoldingsCombinedEntry(self, updateId, dirPath=None):
        dList = []
        retD = self.__getHoldingsCombined(dirPath=dirPath)
        for entryId, qD in retD.items():
            tD = {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId}
            rD = {
                "rcsb_id": entryId,
                "rcsb_repository_holdings_combined_entry_container_identifiers": tD,
                "rcsb_repository_holdings_combined": qD,
            }
            dList.append(rD)
        return dList

    def __getHoldingsCombined(self, dirPath=None):
        retD = {}
        dirPath = dirPath if dirPath else self.__sandboxPath
        currentD = self.__currentCacheD if self.__currentCacheD else self.__getHoldingsCurrent(dirPath=dirPath)
        for entryId, tD in currentD.items():
            retD[entryId] = {"status": "CURRENT", "status_code": "REL"}
        logger.debug("Released entries %d", len(retD))
        #
        unRelD = self.__getHoldingsUnreleased(dirPath=dirPath)
        # logger.info("@@@ unRelD %r", unRelD)
        for entryId, tD in unRelD.items():
            if entryId not in retD and tD["status_code"] in ["AUCO", "AUTH", "HOLD", "HPUB", "POLC", "PROC", "REFI", "REPL", "WAIT", "WDRN"]:
                retD[entryId] = {"status": "UNRELEASED", "status_code": tD["status_code"]}
        logger.debug("Released & unreleased entries %d", len(retD))
        #
        trfD, _ = self.__getHoldingsTransferred(dirPath=dirPath)
        for entryId, tD in trfD.items():
            if entryId not in retD and tD["status_code"] in ["TRSF"]:
                retD[entryId] = {"status": "REMOVED", "status_code": tD["status_code"]}
        #
        logger.debug("Released & unreleased & transferred entries %d", len(retD))
        #
        rmvD, _, replacesD = self.__getHoldingsRemoved(dirPath=dirPath)
        #
        # for entryId in rmvD:
        #    if entryId not in retD:
        #        retD[entryId] = {"status": "REMOVED", "status_code": "OBS"}
        #
        replacedByD = {}
        for entryId, tD in replacesD.items():
            for sId in tD["id_codes_superseded"]:
                replacedByD[sId.strip().upper()] = entryId.strip().upper()
        #
        logger.info("replacedbyD (%d) rmvD (%d) currentD (%d) retD (%d)", len(replacedByD), len(rmvD), len(currentD), len(retD))
        for entryId in rmvD:
            if entryId in currentD:
                continue
            tId = entryId
            if tId in replacedByD:
                if tId == replacedByD[tId]:
                    logger.info("Inconsistent obsolete entry info for %r", tId)
                while tId in replacedByD and tId != replacedByD[tId]:
                    # logger.debug("tId %r replacedByD[tId] %r", tId, replacedByD[tId])
                    tId = replacedByD[tId]
                if tId in currentD:
                    retD[entryId] = {"status": "REMOVED", "status_code": "OBS", "id_code_replaced_by_latest": tId}
                else:
                    logger.debug("%r missing replacedby entry %r", entryId, tId)
            else:
                retD[entryId] = {"status": "REMOVED", "status_code": "OBS"}
        #
        logger.debug("Released & unreleased & transferred & removed entries %d", len(retD))
        return retD

    def getHoldingsCurrentEntry(self, updateId, dirPath=None):
        dList = []
        retD = self.__currentCacheD if self.__currentCacheD else self.__getHoldingsCurrent(dirPath=dirPath)
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

    def getHoldingsUpdateEntry(self, updateId, dirPath=None):
        dList = []
        retD = self.__getHoldingsUpdate(dirPath=dirPath)
        for entryId, qD in retD.items():
            tD = {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId}
            rD = {
                "rcsb_id": entryId,
                "rcsb_repository_holdings_update_entry_container_identifiers": tD,
                "rcsb_repository_holdings_update": qD,
            }
            dList.append(rD)
        return dList

    def getHoldingsUnreleasedEntry(self, updateId, dirPath=None):
        dList = []
        retD = self.__getHoldingsUnreleased(dirPath=dirPath)
        prD = self.__getHoldingsPrerelease(dirPath=dirPath)
        currentD = self.__currentCacheD if self.__currentCacheD else self.__getHoldingsCurrent(dirPath=dirPath)
        self.__currentCacheD = currentD
        for entryId, qD in retD.items():
            if entryId in currentD:
                continue
            rD = {"rcsb_id": entryId}
            rD["rcsb_repository_holdings_unreleased_entry_container_identifiers"] = {"rcsb_id": entryId, "entry_id": entryId, "update_id": updateId}
            if entryId in prD:
                rD["rcsb_repository_holdings_prerelease"] = prD[entryId]
                qD["prerelease_sequence_available_flag"] = "Y"
            else:
                qD["prerelease_sequence_available_flag"] = "N"
            rD["rcsb_repository_holdings_unreleased"] = qD
            #
            dList.append(rD)
        return dList

    def getHoldingsRemovedEntry(self, updateId, dirPath=None):
        dList = []
        rmvD, aaD, spsD = self.__getHoldingsRemoved(dirPath=dirPath)
        trfD, insD = self.__getHoldingsTransferred(dirPath=dirPath)
        currentD = self.__currentCacheD if self.__currentCacheD else self.__getHoldingsCurrent(dirPath=dirPath)
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

    def __getHoldingsTransferred(self, dirPath=None):
        """Parse legacy lists defining the repository contents transferred to alternative repositories

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            (dict): dictionaries containing data for rcsb_repository_holdings_transferred
            (dict): dictionaries containing data for rcsb_repository_holdings_insilico_models

        Example input data:

        ma-czyyf : 262D - TITLE A THREE-DIMENSIONAL MODEL OF THE REV BINDING ELEMENT OF HIV- TITLE 2 1 DERIVED FROM ANALYSES OF IN VITRO SELECTED VARIANTS
        ma-cfqla : 163D - TITLE A THREE-DIMENSIONAL MODEL OF THE REV BINDING ELEMENT OF HIV- TITLE 2 1 DERIVED FROM ANALYSES OF IN VITRO SELECTED VARIANTS

        and -

        1DX2    REL 1999-12-16  2000-12-15  Tumour Targetting Human ...  Beiboer, S.H.W., Reurs, A., Roovers, R.C., Arends, J., Whitelegg, N.R.J., Rees, A.R., Hoogenboom, H.R.

        and -

        1APD    OBSLTE  1992-10-15      2APD
        1BU0    OBSLTE  1998-10-07      2BU0
        1CLJ    OBSLTE  1998-03-04      2CLJ
        1DU8    OBSLTE  2001-01-31      1GIE
        1I2J    OBSLTE  2001-01-06      1JA5

        """
        trsfD = {}
        insD = {}
        dirPath = dirPath if dirPath else self.__sandboxPath

        try:
            fp = os.path.join(dirPath, "status", "theoretical_model_obsolete.tsv")
            lineL = self.__mU.doImport(fp, "list")  # pylint: disable=no-member
            #
            obsDateD = {}
            obsIdD = {}
            for line in lineL:
                fields = line.split("\t")
                if len(fields) < 3:
                    continue
                entryId = str(fields[0]).strip().upper()
                obsDateD[entryId] = dateutil.parser.parse(fields[2]) if self.__assignDates else fields[2]
                if len(fields) > 3 and len(fields[3]) > 3:
                    obsIdD[entryId] = str(fields[3]).strip().upper()
            logger.debug("Read %d obsolete insilico id codes", len(obsDateD))
            # ---------  ---------  ---------  ---------  ---------  ---------  ---------
            fp = os.path.join(dirPath, "status", "model-archive-PDB-insilico-mapping.list")
            lineL = self.__mU.doImport(fp, "list")
            #
            trD = {}
            for line in lineL:
                fields = line.split(":")
                if len(fields) < 2:
                    continue
                entryId = str(fields[1]).strip().upper()[:4]
                maId = str(fields[0]).strip()
                trD[entryId] = maId
            logger.debug("Read %d model archive id codes", len(trD))
            #
            # ---------  ---------  ---------  ---------  ---------  ---------  ---------
            fp = os.path.join(dirPath, "status", "theoretical_model_v2.tsv")
            lineL = self.__mU.doImport(fp, "list")
            #
            logger.debug("Read %d insilico id codes", len(lineL))
            for line in lineL:
                fields = str(line).split("\t")
                if len(fields) < 6:
                    continue
                depDate = dateutil.parser.parse(fields[2]) if self.__assignDates else fields[2]
                relDate = None
                if len(fields[3]) >= 10 and not fields[3].startswith("0000"):
                    relDate = dateutil.parser.parse(fields[3]) if self.__assignDates else fields[3]

                statusCode = "TRSF" if fields[1] == "REL" else fields[1]

                entryId = str(fields[0]).upper()
                title = fields[4]
                #
                auditAuthors = [t.strip() for t in fields[5].split(";")]
                repId = None
                repName = None
                if entryId in trD:
                    repName = "Model Archive"
                    repId = trD[entryId]

                #
                dD = {
                    "status_code": statusCode,
                    "deposit_date": depDate,
                    "repository_content_types": ["coordinates"],
                    "title": title,
                    "audit_authors": auditAuthors,
                }
                #
                if relDate:
                    dD["release_date"] = relDate
                #
                if repId:
                    dD["remote_accession_code"] = repId
                    dD["remote_repository_name"] = repName
                if statusCode == "TRSF":
                    trsfD[entryId] = dD
                #
                #
                dD = {"status_code": statusCode, "deposit_date": depDate, "title": title, "audit_authors": auditAuthors}
                #
                if relDate:
                    dD["release_date"] = relDate
                #
                if entryId in obsDateD:
                    dD["remove_date"] = relDate
                #
                if entryId in obsIdD:
                    dD["id_codes_replaced_by"] = [obsIdD[entryId]]
                #
                insD[entryId] = dD
            #
            logger.info("Transferred entries %d - insilico models %d", len(trsfD), len(insD))
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return trsfD, insD

    def __getHoldingsUpdate(self, dirPath=None):
        """Parse legacy lists defining the contents of the repository update

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing rcsb_repository_holdings_update
        """
        retD = {}
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            updateTypeList = ["added", "modified", "obsolete"]
            contentTypeList = ["entries", "mr", "cs", "sf", "nef", "nmr-str"]
            contentNameD = {
                "entries": "coordinates",
                "mr": "NMR restraints",
                "cs": "NMR chemical shifts",
                "sf": "structure factors",
                "nef": "Combined NMR data (NEF)",
                "nmr-str": "Combined NMR data (NMR-STAR)",
            }
            #
            for updateType in updateTypeList:
                uD = {}
                for contentType in contentTypeList:
                    fp = os.path.join(dirPath, "update-lists", updateType + "-" + contentType)
                    if not self.__mU.exists(fp):
                        continue
                    entryIdL = self.__mU.doImport(fp, "list")
                    #
                    for entryId in entryIdL:
                        entryId = entryId.strip().upper()
                        uD.setdefault(entryId, []).append(contentNameD[contentType])
                for entryId in uD:
                    uType = "removed" if updateType == "obsolete" else updateType
                    # retD[entryId] = {"update_id": updateId, "entry_id": entryId, "update_type": uType, "repository_content_types": uD[entryId]}
                    retD[entryId] = {"update_type": uType, "repository_content_types": uD[entryId]}
            return retD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return retD

    def __getHoldingsCurrent(self, dirPath=None):
        """Parse legacy lists defining the current contents of the repository update

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing data for rcsb_repository_holdings_current
        """
        rD = {}
        retD = {}
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            updateTypeList = ["all"]
            contentTypeList = ["pdb", "pdb-format", "mr", "cs", "sf", "nef", "nmr-str"]
            contentNameD = {
                "pdb": "coordinates",
                "pdb-format": "PDB format coordinates",
                "mr": "NMR restraints",
                "cs": "NMR chemical shifts",
                "sf": "structure factors",
                "nef": "Combined NMR data (NEF)",
                "nmr-str": "Combined NMR data (NMR-STAR)",
            }
            #
            tD = {}
            for updateType in updateTypeList:
                for contentType in contentTypeList:
                    fp = os.path.join(dirPath, "update-lists", updateType + "-" + contentType + "-list")
                    if not self.__mU.exists(fp):
                        continue
                    entryIdL = self.__mU.doImport(fp, "list")
                    #
                    for entryId in entryIdL:
                        entryId = entryId.strip().upper()
                        if entryId not in tD:
                            tD[entryId.upper()] = {}
                        tD[entryId.upper()][contentNameD[contentType]] = True
            #
            fp = os.path.join(dirPath, "status", "biounit_file_list.tsv")
            lines = self.__mU.doImport(fp, "list")
            assemD = {}
            for line in lines:
                fields = line.split("\t")
                entryId = fields[0].strip().upper()
                assemId = fields[1].strip()
                if entryId not in assemD:
                    assemD[entryId.upper()] = []
                assemD[entryId.upper()].append(assemId)
            #
            #
            fp = os.path.join(dirPath, "status", "pdb_bundle_index_list.tsv")
            bundleIdList = self.__mU.doImport(fp, "list")
            bundleD = {}
            for entryId in bundleIdList:
                bundleD[entryId.strip().upper()] = True
            #
            fp = os.path.join(dirPath, "status", "validation_report_list_new.tsv")
            vList = self.__mU.doImport(fp, "list")
            valD = {}
            valImageD = {}
            valCifD = {}
            for line in vList:
                fields = line.split("\t")
                entryId = fields[0].strip().upper()
                imageFlag = fields[1].strip().upper()
                valD[entryId] = True
                valImageD[entryId] = imageFlag == "Y"
                if len(fields) > 2:
                    valCifD[entryId] = fields[2].strip().upper() == "Y"
            #
            #
            fp = os.path.join(dirPath, "status", "entries_without_polymers.tsv")
            pList = self.__mU.doImport(fp, "list")
            pD = {}
            for entryId in pList:
                pD[entryId.strip().upper()] = False
            #
            #
            fp = os.path.join(dirPath, "status", "nmr_restraints_v2_list.tsv")
            nmrV2List = self.__mU.doImport(fp, "list")
            nmrV2D = {}
            for entryId in nmrV2List:
                nmrV2D[entryId.strip().upper()] = False
            #
            if self.__cfgOb:
                configName = self.__cfgOb.getDefaultSectionName()
                fp = self.__cfgOb.getPath("RCSB_EDMAP_LIST_PATH", sectionName=configName)
            else:
                fp = os.path.join(dirPath, "status", "edmaps.json")
            qD = self.__mU.doImport(fp, "json")
            edD = {}
            for entryId in qD:
                edD[entryId.upper()] = qD[entryId]
            #
            fp = os.path.join(dirPath, "status", "obsolete_entry.json_2")
            oL = self.__mU.doImport(fp, "json")
            obsD = {}
            for dD in oL:
                obsD[dD["entryId"].upper()] = True
            logger.info("Removed entry length %d", len(obsD))
            #
            #
            # Revise content types bundles and assemblies
            #
            for qId, dD in tD.items():
                entryId = qId.strip().upper()
                if entryId in obsD:
                    continue
                rD[entryId] = []
                if entryId in bundleD:
                    rD[entryId].append("entry PDB bundle")
                if "coordinates" in dD:
                    rD[entryId].append("entry mmCIF")
                    rD[entryId].append("entry PDBML")
                if "PDB format coordinates" in dD:
                    rD[entryId].append("entry PDB")
                if entryId in assemD:
                    if entryId in bundleD:
                        rD[entryId].append("assembly mmCIF")
                    else:
                        rD[entryId].append("assembly PDB")
                #
                for cType in dD:
                    if cType not in ["coordinates", "PDB format coordinates", "NMR restraints"]:
                        rD[entryId].append(cType)
                    if cType == "NMR restraints":
                        rD[entryId].append("NMR restraints V1")

                if entryId in nmrV2D:
                    rD[entryId].append("NMR restraints V2")
                #
                if entryId in valD:
                    rD[entryId].append("validation report")
                if entryId in valImageD and valImageD[entryId]:
                    rD[entryId].append("validation slider image")
                if entryId in valCifD and valCifD[entryId]:
                    rD[entryId].append("validation data mmCIF")
                if entryId in edD:
                    rD[entryId].append("2fo-fc Map")
                    rD[entryId].append("fo-fc Map")
                    rD[entryId].append("Map Coefficients")
                if entryId not in pD:
                    rD[entryId].append("FASTA sequence")
            #
            for entryId in rD:
                if entryId in assemD:
                    retD[entryId] = {"assembly_ids": assemD[entryId], "repository_content_types": rD[entryId]}
                else:
                    retD[entryId] = {"repository_content_types": rD[entryId]}
            return retD
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return retD

    def __getHoldingsUnreleased(self, dirPath=None):
        """Parse the legacy exchange status file containing details for unreleased entries:

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing data for rcsb_repository_holdings_unreleased

        """
        retD = {}
        fields = []
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            #
            fp = os.path.join(dirPath, "status", "status_v2.txt")
            lines = self.__mU.doImport(fp, "list")
            for line in lines:
                fields = line.split("\t")
                if len(fields) < 15:
                    continue
                entryId = fields[1]
                dD = {
                    "status_code": fields[2]
                    # 'sg_project_name': fields[14],
                    # 'sg_project_abbreviation_': fields[15]}
                }
                if fields[11] and fields[11].strip():
                    dD["title"] = fields[11]
                if fields[10] and fields[10].strip():
                    dD["audit_authors"] = [t.strip() for t in fields[10].split(";")]
                    # d['audit_authors'] = fields[10]
                if fields[12] and fields[12].strip():
                    dD["author_prerelease_sequence_status"] = str(fields[12]).strip().replace("REALEASE", "RELEASE")
                dTupL = [
                    ("deposit_date", 3),
                    ("deposit_date_coordinates", 4),
                    ("deposit_date_structure_factors", 5),
                    ("hold_date_structure_factors", 6),
                    ("deposit_date_nmr_restraints", 7),
                    ("hold_date_nmr_restraints", 8),
                    ("release_date", 9),
                    ("hold_date_coordinates", 13),
                ]
                for dTup in dTupL:
                    fN = dTup[1]
                    if fields[fN] and len(fields[fN]) >= 4:
                        dD[dTup[0]] = dateutil.parser.parse(fields[fN]) if self.__assignDates else fields[fN]
                #
                retD[entryId] = {k: v for k, v in dD.items() if v}
        except Exception as e:
            logger.error("Fields: %r", fields)
            logger.exception("Failing with %s", str(e))

        return retD

    def __getHoldingsRemoved(self, dirPath=None):
        """Parse the legacy exchange file containing details of removed entries:

            {
                "entryId": "125D",
                "obsoletedDate": "1998-04-15",
                "title": "SOLUTION STRUCTURE OF THE DNA-BINDING DOMAIN OF CD=2=-GAL4 FROM S. CEREVISIAE",
                "details": "",
                "depositionAuthors": [
                    "Baleja, J.D.",
                    "Wagner, G."
                ],
                "depositionDate": "1993-05-05",
                "releaseDate": "1994-01-31",
                "obsoletedBy": [
                    "1AW6"
                ],
                "content_type": [
                    "entry mmCIF",
                    "entry PDB",
                    "entry PDBML",
                    "structure factors"
                ]},

        Returns;
            (dict) : dictionaries for rcsb_repository_holdings_removed
            (dict) : dictionaries for rcsb_repository_holdings_removed_audit_authors
            (dict) : dictionaries for rcsb_repository_holdings_superseded

        """
        # rcsb_repository_holdings_removed
        rL1D = {}
        # rcsb_repository_holdings_removed_audit_authors
        rL2D = {}
        # rcsb_repository_holdings_superseded
        rL3D = {}
        #
        sD = {}
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            fp = os.path.join(dirPath, "status", "obsolete_entry.json_2")
            dD = self.__mU.doImport(fp, "json")
            for dT in dD:
                # ---
                ctL = dT["content_type"] if "content_type" in dT else []
                # ---
                rbL = dT["obsoletedBy"] if "obsoletedBy" in dT else []
                d1 = {"title": dT["title"], "details": dT["details"], "audit_authors": dT["depositionAuthors"]}
                if rbL:
                    d1["id_codes_replaced_by"] = [t.upper() for t in rbL]
                if ctL:
                    d1["repository_content_types"] = ctL

                dTupL = [("deposit_date", "depositionDate"), ("remove_date", "obsoletedDate"), ("release_date", "releaseDate")]
                for dTup in dTupL:
                    fN = dTup[1]
                    if dT[fN] and len(dT[fN]) > 4:
                        d1[dTup[0]] = dateutil.parser.parse(dT[fN]) if self.__assignDates else dT[fN]

                rL1D[dT["entryId"]] = {k: v for k, v in d1.items() if v}
                #
                for ii, author in enumerate(dT["depositionAuthors"]):
                    d2 = {"ordinal_id": ii + 1, "audit_author": author}
                    rL2D.setdefault(dT["entryId"], []).append(d2)
                if "obsoletedBy" in dT:
                    for pdbId in dT["obsoletedBy"]:
                        if pdbId not in sD:
                            sD[pdbId] = []
                        sD[pdbId].append(dT["entryId"])
            #
            for pdbId in sD:
                if sD[pdbId]:
                    rL3D[pdbId] = {"id_codes_superseded": sD[pdbId]}

            logger.debug("Computed data lengths  %d %d %d", len(rL1D), len(rL2D), len(rL3D))
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return rL1D, rL2D, rL3D

    def __getHoldingsPrerelease(self, dirPath=None):
        """Parse the legacy exchange status file containing prerelease sequence data.

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing data for rcsb_repository_holdings_prerelease

        >6I99 Entity 1
        HHHHHHENLYFQGELKREEITLLKELGSGQFGVVKLGKWKGQYDVAVKMIKEG....
        >6JKE Entity 1
        GRVTNQLQYLHKVVMKALWKHQFAWPFRQPVDAVKLGLPDYHKIIKQPMDMGTI....

        """
        retD = {}
        fields = []
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            # Get prerelease sequence data
            fp = os.path.join(dirPath, "sequence", "pdb_seq_prerelease.fasta")
            sD = self.__mU.doImport(fp, "fasta", commentStyle="prerelease")
            seqD = {}
            for sid in sD:
                fields = sid.split("_")
                entryId = str(fields[0]).upper()
                entityId = str(fields[1])
                if entryId not in seqD:
                    seqD[entryId] = []
                seqD[entryId].append((entityId, sD[sid]["sequence"]))
            logger.debug("Loaded prerelease sequences for %d entries", len(seqD))
            #
            for entryId, seqTupL in seqD.items():
                # dD = {"seq_one_letter_code": seqL}
                logger.debug("Adding prerelease sequences for %s", entryId)
                for entityId, seqS in seqTupL:
                    if not seqS:
                        continue
                    retD.setdefault(entryId, []).append({"entity_id": entityId, "seq_one_letter_code": seqS})
                #
                # retD[entryId] = {k: v for k, vTup in dD.items() if vTup[1]}
        except Exception as e:
            logger.error("Fields: %r", fields)
            logger.exception("Failing with %s", str(e))

        return retD
