##
# File:    DictMethodEntityInstanceHelper.py
# Author:  J. Westbrook
# Date:    16-Jul-2019
# Version: 0.001 Initial version
#
##
"""
This helper class implements methods supporting entity-instance-level functions in the RCSB dictionary extension.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

# pylint: disable=too-many-lines

import logging
import re
from collections import OrderedDict

from mmcif.api.DataCategory import DataCategory

logger = logging.getLogger(__name__)


class DictMethodEntityInstanceHelper(object):
    """ This helper class implements methods supporting entity-instance-level functions in the RCSB dictionary extension.
    """

    def __init__(self, **kwargs):
        """
        Args:
            resourceProvider: (obj) instance of DictMethodResourceProvider()
            raiseExceptions: (bool, optional) flag to raise rather than handle exceptions

        """
        #
        self._raiseExceptions = kwargs.get("raiseExceptions", False)
        self.__wsPattern = re.compile(r"\s+", flags=re.UNICODE | re.MULTILINE)
        self.__reNonDigit = re.compile(r"[^\d]+")
        #
        rP = kwargs.get("resourceProvider")
        self.__commonU = rP.getResource("DictMethodCommonUtils instance") if rP else None
        self.__dApi = rP.getResource("Dictionary API instance (pdbx_core)") if rP else None
        #
        logger.debug("Dictionary entity-instance level method helper init")

    def buildContainerEntityInstanceIds(self, dataContainer, catName, **kwargs):
        """
        Build:

        loop_
        _rcsb_entity_instance_container_identifiers.entry_id
        _rcsb_entity_instance_container_identifiers.entity_id
        _rcsb_entity_instance_container_identifiers.entity_type
        _rcsb_entity_instance_container_identifiers.asym_id
        _rcsb_entity_instance_container_identifiers.auth_asym_id
        _rcsb_entity_instance_container_identifiers.comp_id
        _rcsb_entity_instance_container_identifiers.auth_seq_id
        ...

        """
        logger.debug("Starting catName %s kwargs %r", catName, kwargs)
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("entity")):
                return False
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            cObj = dataContainer.getObj(catName)
            asymD = self.__commonU.getInstanceIdMap(dataContainer)
            npAuthAsymD = self.__commonU.getNonPolymerIdMap(dataContainer)
            brAuthAsymD = self.__commonU.getBranchedIdMap(dataContainer)
            seqIdMapAsymD = self.__commonU.getAuthToSeqIdMap(dataContainer)
            #
            for ii, asymId in enumerate(sorted(asymD)):
                for k, v in asymD[asymId].items():
                    cObj.setValue(v, k, ii)
                v = ",".join(seqIdMapAsymD[asymId]) if asymId in seqIdMapAsymD else "?"
                cObj.setValue(v, "auth_to_entity_poly_seq_mapping", ii)

            ok = self.__addPdbxValidateAsymIds(dataContainer, asymD, npAuthAsymD, brAuthAsymD)
            return ok
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def __addPdbxValidateAsymIds(self, dataContainer, asymMapD, npAuthAsymMapD, brAuthAsymMapD):
        """ Internal method to insert Asym_id's into the following categories:

                _pdbx_validate_close_contact.rcsb_label_asym_id_1
                _pdbx_validate_close_contact.rcsb_label_asym_id_2
                _pdbx_validate_symm_contact.rcsb_label_asym_id_1
                _pdbx_validate_symm_contact.rcsb_label_asym_id_2
                _pdbx_validate_rmsd_bond.rcsb_label_asym_id_1
                _pdbx_validate_rmsd_bond.rcsb_label_asym_id_2
                _pdbx_validate_rmsd_angle.rcsb_label_asym_id_1
                _pdbx_validate_rmsd_angle.rcsb_label_asym_id_2
                _pdbx_validate_rmsd_angle.rcsb_label_asym_id_3
                _pdbx_validate_torsion.rcsb_label_asym_id
                _pdbx_validate_peptide_omega.rcsb_label_asym_id_1
                _pdbx_validate_peptide_omega.rcsb_label_asym_id_2
                _pdbx_validate_chiral.rcsb_label_asym_id
                _pdbx_validate_planes.rcsb_label_asym_id
                _pdbx_validate_planes_atom.rcsb_label_asym_id
                _pdbx_validate_main_chain_plane.rcsb_label_asym_id
                _pdbx_validate_polymer_linkage.rcsb_label_asym_id_1
                _pdbx_validate_polymer_linkage.rcsb_label_asym_id_2
        """
        #
        mD = {
            "pdbx_validate_close_contact": [("auth_asym_id_1", "auth_seq_id_1", "rcsb_label_asym_id_1"), ("auth_asym_id_2", "auth_seq_id_2", "rcsb_label_asym_id_2")],
            "pdbx_validate_symm_contact": [("auth_asym_id_1", "auth_seq_id_1", "rcsb_label_asym_id_1"), ("auth_asym_id_2", "auth_seq_id_2", "rcsb_label_asym_id_2")],
            "pdbx_validate_rmsd_bond": [("auth_asym_id_1", "auth_seq_id_1", "rcsb_label_asym_id_1"), ("auth_asym_id_2", "auth_seq_id_2", "rcsb_label_asym_id_2")],
            "pdbx_validate_rmsd_angle": [
                ("auth_asym_id_1", "auth_seq_id_1", "rcsb_label_asym_id_1"),
                ("auth_asym_id_2", "auth_seq_id_2", "rcsb_label_asym_id_2"),
                ("auth_asym_id_3", "auth_seq_id_3", "rcsb_label_asym_id_3"),
            ],
            "pdbx_validate_torsion": [("auth_asym_id", "auth_seq_id", "rcsb_label_asym_id")],
            "pdbx_validate_peptide_omega": [("auth_asym_id_1", "auth_seq_id_1", "rcsb_label_asym_id_1"), ("auth_asym_id_2", "auth_seq_id_2", "rcsb_label_asym_id_2")],
            "pdbx_validate_chiral": [("auth_asym_id", "auth_seq_id", "rcsb_label_asym_id")],
            "pdbx_validate_planes": [("auth_asym_id", "auth_seq_id", "rcsb_label_asym_id")],
            "pdbx_validate_planes_atom": [("auth_asym_id", "auth_seq_id", "rcsb_label_asym_id")],
            "pdbx_validate_main_chain_plane": [("auth_asym_id", "auth_seq_id", "rcsb_label_asym_id")],
            "pdbx_validate_polymer_linkage": [("auth_asym_id_1", "auth_seq_id_1", "rcsb_label_asym_id_1"), ("auth_asym_id_2", "auth_seq_id_2", "rcsb_label_asym_id_2")],
            "pdbx_distant_solvent_atoms": [("auth_asym_id", "auth_seq_id", "rcsb_label_asym_id")],
        }
        # -- JDW
        # polymer lookup
        authAsymD = {}
        for asymId, dD in asymMapD.items():
            if dD["entity_type"].lower() in ["polymer", "branched"]:
                authAsymD[(dD["auth_asym_id"], "?")] = asymId
        #
        # non-polymer lookup
        #
        logger.debug("%s authAsymD %r", dataContainer.getName(), authAsymD)
        for (authAsymId, seqId), dD in npAuthAsymMapD.items():
            if dD["entity_type"].lower() not in ["polymer", "branched"]:
                authAsymD[(authAsymId, seqId)] = dD["asym_id"]
        #
        # branched lookup
        logger.debug("%s authAsymD %r", dataContainer.getName(), authAsymD)
        for (authAsymId, seqId), dD in brAuthAsymMapD.items():
            if dD["entity_type"].lower() in ["branched"]:
                authAsymD[(authAsymId, seqId)] = dD["asym_id"]
        #
        #
        for catName, mTupL in mD.items():
            if not dataContainer.exists(catName):
                continue
            cObj = dataContainer.getObj(catName)
            for ii in range(cObj.getRowCount()):
                for mTup in mTupL:
                    try:
                        authVal = cObj.getValue(mTup[0], ii)
                    except Exception:
                        authVal = "?"
                    try:
                        authSeqId = cObj.getValue(mTup[1], ii)
                    except Exception:
                        authSeqId = "?"

                    # authVal = cObj.getValue(mTup[0], ii)
                    # authSeqId = cObj.getValue(mTup[1], ii)
                    #
                    # logger.debug("%s %4d authAsymId %r authSeqId %r" % (catName, ii, authVal, authSeqId))
                    #
                    if (authVal, authSeqId) in authAsymD:
                        if not cObj.hasAttribute(mTup[2]):
                            cObj.appendAttribute(mTup[2])
                        cObj.setValue(authAsymD[(authVal, authSeqId)], mTup[2], ii)
                    elif (authVal, "?") in authAsymD:
                        if not cObj.hasAttribute(mTup[2]):
                            cObj.appendAttribute(mTup[2])
                        cObj.setValue(authAsymD[(authVal, "?")], mTup[2], ii)
                    else:
                        if authVal not in ["."]:
                            logger.warning("%s %s missing mapping auth asymId %s authSeqId %r", dataContainer.getName(), catName, authVal, authSeqId)
                        if not cObj.hasAttribute(mTup[2]):
                            cObj.appendAttribute(mTup[2])
                        cObj.setValue("?", mTup[2], ii)

        return True

    def __initializeInstanceFeatureType(self, dataContainer, asymId, fCountD, countType="set"):
        instTypeD = self.__commonU.getInstanceTypes(dataContainer)
        eType = instTypeD[asymId]
        if eType == "polymer":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_polymer_instance_feature_summary", "type")
        elif eType in ["non-polymer", "water"]:
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_nonpolymer_instance_feature_summary", "type")
        elif eType == "branched":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_branched_instance_feature_summary", "type")
        else:
            logger.error("%r asymId %r eType %r", dataContainer.getName(), asymId, eType)
        #
        fTypeL = sorted([tup[0] for tup in eTupL])
        #
        for fType in fTypeL:
            if countType == "set":
                fCountD.setdefault(asymId, {}).setdefault(fType, set())
            else:
                fCountD.setdefault(asymId, {}).setdefault(fType, [])
            #
        return fCountD

    # ---- JDW
    def buildEntityInstanceFeatureSummaryPrev(self, dataContainer, catName, **kwargs):
        """ Build category rcsb_entity_instance_feature_summary (UPDATED)

        Example:

            loop_
            _rcsb_entity_instance_feature_summary.ordinal
            _rcsb_entity_instance_feature_summary.entry_id
            _rcsb_entity_instance_feature_summary.entity_id
            _rcsb_entity_instance_feature_summary.asym_id
            _rcsb_entity_instance_feature_summary.auth_asym_id
            #
            _rcsb_entity_instance_feature_summary.type
            _rcsb_entity_instance_feature_summary.count
            _rcsb_entity_instance_feature_summary.coverage
            # ...
        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_instance_feature_summary":
                return False
            if not dataContainer.exists("rcsb_entity_instance_feature") and not dataContainer.exists("entry"):
                return False

            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            sObj = dataContainer.getObj(catName)
            fObj = dataContainer.getObj("rcsb_entity_instance_feature")
            #
            instEntityD = self.__commonU.getInstanceEntityMap(dataContainer)
            entityPolymerLengthD = self.__commonU.getPolymerEntityLengthsEnumerated(dataContainer)
            # typeList = self.__dApi.getEnumList("rcsb_entity_instance_feature_summary", "type", sortFlag=True)
            asymAuthD = self.__commonU.getAsymAuthIdMap(dataContainer)
            instIdMapD = self.__commonU.getInstanceIdMap(dataContainer)

            fCountD = OrderedDict()
            fMonomerCountD = OrderedDict()
            for ii in range(fObj.getRowCount()):
                asymId = fObj.getValue("asym_id", ii)
                # ---- initialize counts
                # fCountD = self.__initializeInstanceFeatureType(dataContainer, asymId, fCountD, countType="set")
                # fMonomerCountD = self.__initializeInstanceFeatureType(dataContainer, asymId, fMonomerCountD, countType="list")
                # ----
                fType = fObj.getValue("type", ii)
                fId = fObj.getValue("feature_id", ii)
                fCountD.setdefault(asymId, {}).setdefault(fType, set()).add(fId)
                #
                tbegS = fObj.getValueOrDefault("feature_positions_beg_seq_id", ii, defaultValue=None)
                tendS = fObj.getValueOrDefault("feature_positions_end_seq_id", ii, defaultValue=None)
                if fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS is not None and fObj.hasAttribute("feature_positions_end_seq_id") and tendS is not None:
                    begSeqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    endSeqIdL = str(fObj.getValue("feature_positions_end_seq_id", ii)).split(";")
                    monCount = 0
                    for begSeqId, endSeqId in zip(begSeqIdL, endSeqIdL):
                        try:
                            monCount += abs(int(endSeqId) - int(begSeqId) + 1)
                        except Exception:
                            logger.warning(
                                "%s fType %r fId %r bad sequence begSeqIdL %r endSeqIdL %r tbegS %r tendS %r", dataContainer.getName(), fType, fId, begSeqIdL, endSeqIdL, tbegS, tendS,
                            )

                    fMonomerCountD.setdefault(asymId, {}).setdefault(fType, []).append(monCount)
                elif fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS:
                    seqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    fMonomerCountD.setdefault(asymId, {}).setdefault(fType, []).append(len(seqIdL))
            #
            logger.debug("%s fCountD %r", entryId, fCountD)
            #

            ii = 0
            for asymId, fTypeD in fCountD.items():
                entityId = instEntityD[asymId]
                authAsymId = asymAuthD[asymId]
                for fType, fS in fTypeD.items():
                    sObj.setValue(ii + 1, "ordinal", ii)
                    sObj.setValue(entryId, "entry_id", ii)
                    sObj.setValue(entityId, "entity_id", ii)
                    sObj.setValue(asymId, "asym_id", ii)
                    sObj.setValue(authAsymId, "auth_asym_id", ii)
                    # add comp
                    if asymId in instIdMapD and "comp_id" in instIdMapD[asymId] and instIdMapD[asymId]["comp_id"]:
                        sObj.setValue(instIdMapD[asymId]["comp_id"], "comp_id", ii)
                    sObj.setValue(fType, "type", ii)
                    #
                    if fType.startswith("UNOBSERVED") and asymId in fMonomerCountD and fType in fMonomerCountD[asymId]:
                        fCount = sum(fMonomerCountD[asymId][fType])
                    else:
                        fCount = len(fS)
                    sObj.setValue(fCount, "count", ii)
                    fracC = 0.0
                    if asymId in fMonomerCountD and fType in fMonomerCountD[asymId] and entityId in entityPolymerLengthD:
                        fracC = float(sum(fMonomerCountD[asymId][fType])) / float(entityPolymerLengthD[entityId])
                    sObj.setValue(round(fracC, 5), "coverage", ii)
                    if (
                        fType in ["CATH", "SCOP", "HELIX_P", "SHEET", "UNASSIGNED_SEC_STRUCT", "UNOBSERVED_RESIDUE_XYZ", "ZERO_OCCUPANCY_RESIDUE_XYZ"]
                        and asymId in fMonomerCountD
                        and fType in fMonomerCountD[asymId]
                    ):
                        minL = min(fMonomerCountD[asymId][fType]) if fMonomerCountD[asymId][fType] else 0
                        maxL = max(fMonomerCountD[asymId][fType]) if fMonomerCountD[asymId][fType] else 0
                        sObj.setValue(minL, "minimum_length", ii)
                        sObj.setValue(maxL, "maximum_length", ii)
                    ii += 1
        except Exception as e:
            logger.exception("Failing for %s with %s", dataContainer.getName(), str(e))
        return True

    # ---- JDW

    def buildEntityInstanceFeatures(self, dataContainer, catName, **kwargs):
        """ Build category rcsb_entity_instance_feature ...

        Example:
            loop_
            _rcsb_entity_instance_feature.ordinal
            _rcsb_entity_instance_feature.entry_id
            _rcsb_entity_instance_feature.entity_id
            _rcsb_entity_instance_feature.asym_id
            _rcsb_entity_instance_feature.auth_asym_id
            _rcsb_entity_instance_feature.feature_id
            _rcsb_entity_instance_feature.type
            _rcsb_entity_instance_feature.name
            _rcsb_entity_instance_feature.description
            _rcsb_entity_instance_feature.reference_scheme
            _rcsb_entity_instance_feature.provenance_source
            _rcsb_entity_instance_feature.assignment_version
            _rcsb_entity_instance_feature.feature_positions_beg_seq_id
            _rcsb_entity_instance_feature.feature_positions_end_seq_id
            _rcsb_entity_instance_feature.feature_positions_value

        """
        doLineage = False
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_instance_feature":
                return False
            # Exit if source categories are missing
            if not dataContainer.exists("entry"):
                return False
            #
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            cObj = dataContainer.getObj(catName)
            #
            rP = kwargs.get("resourceProvider")

            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            asymIdD = self.__commonU.getInstanceEntityMap(dataContainer)
            asymAuthIdD = self.__commonU.getAsymAuthIdMap(dataContainer)
            asymIdRangesD = self.__commonU.getInstancePolymerRanges(dataContainer)
            pAuthAsymD = self.__commonU.getPolymerIdMap(dataContainer)
            instTypeD = self.__commonU.getInstanceTypes(dataContainer)
            # ---------------
            # Add CATH assignments
            cathU = rP.getResource("CathProvider instance") if rP else None
            ii = cObj.getRowCount()
            #
            for asymId, authAsymId in asymAuthIdD.items():
                if instTypeD[asymId] not in ["polymer", "branched"]:
                    continue
                entityId = asymIdD[asymId]
                dL = cathU.getCathResidueRanges(entryId.lower(), authAsymId)
                logger.debug("%s asymId %s authAsymId %s dL %r", entryId, asymId, authAsymId, dL)
                vL = cathU.getCathVersions(entryId.lower(), authAsymId)
                for (cathId, domId, tId, authSeqBeg, authSeqEnd) in dL:
                    begSeqId = pAuthAsymD[(authAsymId, authSeqBeg, None)]["seq_id"] if (authAsymId, authSeqBeg, None) in pAuthAsymD else None
                    endSeqId = pAuthAsymD[(authAsymId, authSeqEnd, None)]["seq_id"] if (authAsymId, authSeqEnd, None) in pAuthAsymD else None
                    if not (begSeqId and endSeqId):
                        # take the full chain
                        begSeqId = asymIdRangesD[asymId]["begSeqId"] if asymId in asymIdRangesD else None
                        endSeqId = asymIdRangesD[asymId]["endSeqId"] if asymId in asymIdRangesD else None
                        if not (begSeqId and endSeqId):
                            logger.info(
                                "%s CATH cathId %r domId %r tId %r asymId %r authAsymId %r authSeqBeg %r authSeqEnd %r",
                                entryId,
                                cathId,
                                domId,
                                tId,
                                asymId,
                                authAsymId,
                                authSeqBeg,
                                authSeqEnd,
                            )
                            continue

                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue("CATH", "type", ii)
                    #
                    cObj.setValue(str(cathId), "feature_id", ii)
                    # cObj.setValue(str(domId), "feature_id", ii)
                    # cObj.setValue(cathId, "name", ii)
                    cObj.setValue(cathU.getCathName(cathId), "name", ii)
                    #
                    if doLineage:
                        cObj.setValue(";".join(cathU.getNameLineage(cathId)), "annotation_lineage_name", ii)
                        idLinL = cathU.getIdLineage(cathId)
                        cObj.setValue(";".join(idLinL), "annotation_lineage_id", ii)
                        cObj.setValue(";".join([str(jj) for jj in range(1, len(idLinL) + 1)]), "annotation_lineage_depth", ii)
                    #
                    #
                    cObj.setValue(begSeqId, "feature_positions_beg_seq_id", ii)
                    cObj.setValue(endSeqId, "feature_positions_end_seq_id", ii)
                    #
                    cObj.setValue("PDB entity", "reference_scheme", ii)
                    cObj.setValue("CATH", "provenance_source", ii)
                    cObj.setValue(vL[0], "assignment_version", ii)
                    #
                    ii += 1
            # ------------
            # Add SCOP assignments
            oldCode = False
            scopU = rP.getResource("ScopProvider instance") if rP else None
            for asymId, authAsymId in asymAuthIdD.items():
                if instTypeD[asymId] not in ["polymer", "branched"]:
                    continue
                entityId = asymIdD[asymId]
                dL = scopU.getScopResidueRanges(entryId.lower(), authAsymId)
                version = scopU.getScopVersion()
                for (sunId, domId, sccs, tId, authSeqBeg, authSeqEnd) in dL:
                    begSeqId = pAuthAsymD[(authAsymId, authSeqBeg, None)]["seq_id"] if (authAsymId, authSeqBeg, None) in pAuthAsymD else None
                    endSeqId = pAuthAsymD[(authAsymId, authSeqEnd, None)]["seq_id"] if (authAsymId, authSeqEnd, None) in pAuthAsymD else None
                    # logger.info("%s (first) begSeqId %r endSeqId %r", entryId, begSeqId, endSeqId)
                    if not (begSeqId and endSeqId):
                        # try another full range
                        # begSeqId = asymIdRangesD[asymId]["begAuthSeqId"] if asymId in asymIdRangesD and "begAuthSeqId" in asymIdRangesD[asymId] else None
                        # endSeqId = asymIdRangesD[asymId]["endAuthSeqId"] if asymId in asymIdRangesD and "endAuthSeqId" in asymIdRangesD[asymId] else None
                        begSeqId = asymIdRangesD[asymId]["begSeqId"] if asymId in asymIdRangesD else None
                        endSeqId = asymIdRangesD[asymId]["endSeqId"] if asymId in asymIdRangesD else None
                        # logger.info("%s (altd) begSeqId %r endSeqId %r", entryId, begSeqId, endSeqId)
                        if not (begSeqId and endSeqId):
                            logger.debug(
                                "%s unqalified SCOP sunId %r domId %r sccs %r asymId %r authAsymId %r authSeqBeg %r authSeqEnd %r",
                                entryId,
                                sunId,
                                domId,
                                sccs,
                                asymId,
                                authAsymId,
                                authSeqBeg,
                                authSeqEnd,
                            )
                            continue

                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue("SCOP", "type", ii)
                    #
                    # cObj.setValue(str(sunId), "domain_id", ii)
                    cObj.setValue(domId, "feature_id", ii)
                    cObj.setValue(scopU.getScopName(sunId), "name", ii)
                    #
                    if doLineage:
                        tL = [t if t is not None else "" for t in scopU.getNameLineage(sunId)]
                        cObj.setValue(";".join(tL), "annotation_lineage_name", ii)
                        idLinL = scopU.getIdLineage(sunId)
                        cObj.setValue(";".join([str(t) for t in idLinL]), "annotation_lineage_id", ii)
                        cObj.setValue(";".join([str(jj) for jj in range(1, len(idLinL) + 1)]), "annotation_lineage_depth", ii)
                        #
                    cObj.setValue(begSeqId, "feature_positions_beg_seq_id", ii)
                    cObj.setValue(endSeqId, "feature_positions_end_seq_id", ii)
                    if oldCode:
                        if begSeqId is not None and endSeqId is not None:
                            if begSeqId == 0:
                                begSeqId += 1
                                endSeqId += 1
                            cObj.setValue(begSeqId, "feature_positions_beg_seq_id", ii)
                            cObj.setValue(endSeqId, "feature_positions_end_seq_id", ii)
                        else:
                            tSeqBeg = asymIdRangesD[asymId]["begAuthSeqId"] if asymId in asymIdRangesD and "begAuthSeqId" in asymIdRangesD[asymId] else None
                            cObj.setValue(tSeqBeg, "feature_positions_beg_seq_id", ii)
                            tSeqEnd = asymIdRangesD[asymId]["endAuthSeqId"] if asymId in asymIdRangesD and "endAuthSeqId" in asymIdRangesD[asymId] else None
                            cObj.setValue(tSeqEnd, "feature_positions_end_seq_id", ii)
                        #
                    cObj.setValue("PDB entity", "reference_scheme", ii)
                    cObj.setValue("SCOPe", "provenance_source", ii)
                    cObj.setValue(version, "assignment_version", ii)
                    #
                    ii += 1
            # ------------
            # Add sheet features
            instSheetRangeD = self.__commonU.getProtSheetFeatures(dataContainer)
            sheetSenseD = self.__commonU.getProtSheetSense(dataContainer)
            for sId, sD in instSheetRangeD.items():
                for asymId, rTupL in sD.items():
                    entityId = asymIdD[asymId]
                    authAsymId = asymAuthIdD[asymId]
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue("SHEET", "type", ii)
                    #
                    cObj.setValue(str(sId), "feature_id", ii)
                    cObj.setValue("sheet", "name", ii)
                    if sId in sheetSenseD:
                        cObj.setValue(sheetSenseD[sId] + " sense sheet", "description", ii)
                    #
                    tSeqId = ";".join([str(rTup[0]) for rTup in rTupL])
                    cObj.setValue(tSeqId, "feature_positions_beg_seq_id", ii)
                    tSeqId = ";".join([str(rTup[1]) for rTup in rTupL])
                    cObj.setValue(tSeqId, "feature_positions_end_seq_id", ii)
                    #
                    cObj.setValue("PDB entity", "reference_scheme", ii)
                    cObj.setValue("PROMOTIF", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    #
                    ii += 1
            # ------------------
            # Helix features
            helixRangeD = self.__commonU.getProtHelixFeatures(dataContainer)
            for hId, hL in helixRangeD.items():
                for (asymId, begSeqId, endSeqId) in hL:
                    entityId = asymIdD[asymId]
                    authAsymId = asymAuthIdD[asymId]
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue("HELIX_P", "type", ii)
                    #
                    cObj.setValue(str(hId), "feature_id", ii)
                    cObj.setValue("helix", "name", ii)
                    #
                    cObj.setValue(begSeqId, "feature_positions_beg_seq_id", ii)
                    cObj.setValue(endSeqId, "feature_positions_end_seq_id", ii)
                    #
                    cObj.setValue("PDB entity", "reference_scheme", ii)
                    cObj.setValue("PROMOTIF", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    #
                    ii += 1
            #
            # ------------------
            # Unassigned SS features
            unassignedRangeD = self.__commonU.getProtUnassignedSecStructFeatures(dataContainer)
            for asymId, rTupL in unassignedRangeD.items():
                if not rTupL:
                    continue
                entityId = asymIdD[asymId]
                authAsymId = asymAuthIdD[asymId]
                cObj.setValue(ii + 1, "ordinal", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(asymId, "asym_id", ii)
                cObj.setValue(authAsymId, "auth_asym_id", ii)
                cObj.setValue("UNASSIGNED_SEC_STRUCT", "type", ii)
                #
                cObj.setValue(str(1), "feature_id", ii)
                cObj.setValue("unassigned secondary structure", "name", ii)
                #
                cObj.setValue(";".join([str(rTup[0]) for rTup in rTupL]), "feature_positions_beg_seq_id", ii)
                cObj.setValue(";".join([str(rTup[1]) for rTup in rTupL]), "feature_positions_end_seq_id", ii)
                #
                cObj.setValue("PDB entity", "reference_scheme", ii)
                cObj.setValue("PROMOTIF", "provenance_source", ii)
                cObj.setValue("V1.0", "assignment_version", ii)
                #
                ii += 1
            #
            cisPeptideD = self.__commonU.getCisPeptides(dataContainer)
            for cId, cL in cisPeptideD.items():
                for (asymId, begSeqId, endSeqId, modelId, omegaAngle) in cL:
                    entityId = asymIdD[asymId]
                    authAsymId = asymAuthIdD[asymId]
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue("CIS-PEPTIDE", "type", ii)
                    cObj.setValue(str(cId), "feature_id", ii)
                    cObj.setValue("cis-peptide", "name", ii)
                    #
                    cObj.setValue(begSeqId, "feature_positions_beg_seq_id", ii)
                    cObj.setValue(endSeqId, "feature_positions_end_seq_id", ii)
                    #
                    cObj.setValue("PDB entity", "reference_scheme", ii)
                    cObj.setValue("PDB", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    tS = "cis-peptide bond in model %d with omega angle %.2f" % (modelId, omegaAngle)
                    cObj.setValue(tS, "description", ii)
                    #
                    ii += 1
            #
            targetSiteD = self.__commonU.getTargetSiteInfo(dataContainer)
            ligandSiteD = self.__commonU.getLigandSiteInfo(dataContainer)
            for tId, tL in targetSiteD.items():
                aD = OrderedDict()
                for tD in tL:
                    aD.setdefault(tD["asymId"], []).append((tD["compId"], tD["seqId"]))
                for asymId, aL in aD.items():
                    entityId = asymIdD[asymId]
                    authAsymId = asymAuthIdD[asymId]
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue("BINDING_SITE", "type", ii)
                    cObj.setValue(str(tId), "feature_id", ii)
                    cObj.setValue("binding_site", "name", ii)
                    #
                    cObj.setValue(";".join([tup[0] for tup in aL]), "feature_positions_beg_comp_id", ii)
                    cObj.setValue(";".join([tup[1] for tup in aL]), "feature_positions_beg_seq_id", ii)
                    #
                    cObj.setValue("PDB entity", "reference_scheme", ii)
                    cObj.setValue("PDB", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    if tId in ligandSiteD:
                        cObj.setValue(ligandSiteD[tId]["description"], "description", ii)
                        if ligandSiteD[tId]["siteLabel"]:
                            cObj.setValue(ligandSiteD[tId]["siteLabel"], "name", ii)
                    #
                    ii += 1
            #
            unObsPolyResRngD = self.__commonU.getUnobservedPolymerResidueInfo(dataContainer)
            for (modelId, asymId, zeroOccFlag), rTupL in unObsPolyResRngD.items():
                entityId = asymIdD[asymId]
                authAsymId = asymAuthIdD[asymId]
                cObj.setValue(ii + 1, "ordinal", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(asymId, "asym_id", ii)
                cObj.setValue(authAsymId, "auth_asym_id", ii)
                #
                if zeroOccFlag:
                    cObj.setValue("ZERO_OCCUPANCY_RESIDUE_XYZ", "type", ii)
                    tS = "All atom coordinates for this residue are reported with zero-occupancy in model %s" % modelId
                    cObj.setValue(tS, "description", ii)
                    cObj.setValue("residue coordinates with zero occupancy", "name", ii)
                else:
                    cObj.setValue("UNOBSERVED_RESIDUE_XYZ", "type", ii)
                    tS = "No coordinates for this residue are reported in model %s" % modelId
                    cObj.setValue(tS, "description", ii)
                    cObj.setValue("unmodeled residue", "name", ii)
                #
                cObj.setValue(str(1), "feature_id", ii)
                #
                cObj.setValue(";".join([str(rTup[0]) for rTup in rTupL]), "feature_positions_beg_seq_id", ii)
                cObj.setValue(";".join([str(rTup[1]) for rTup in rTupL]), "feature_positions_end_seq_id", ii)
                #
                cObj.setValue("PDB entity", "reference_scheme", ii)
                cObj.setValue("PDB", "provenance_source", ii)
                cObj.setValue("V1.0", "assignment_version", ii)
                #
                ii += 1

            unObsPolyAtomRngD = self.__commonU.getUnobservedPolymerAtomInfo(dataContainer)
            for (modelId, asymId, zeroOccFlag), rTupL in unObsPolyAtomRngD.items():
                entityId = asymIdD[asymId]
                authAsymId = asymAuthIdD[asymId]
                cObj.setValue(ii + 1, "ordinal", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(asymId, "asym_id", ii)
                cObj.setValue(authAsymId, "auth_asym_id", ii)
                #
                if zeroOccFlag:
                    cObj.setValue("ZERO_OCCUPANCY_ATOM_XYZ", "type", ii)
                    tS = "Some atom coordinates in this residue are reported with zero-occupancy in model %s" % modelId
                    cObj.setValue(tS, "description", ii)
                    cObj.setValue("atom coordinates with zero occupancy", "name", ii)
                else:
                    cObj.setValue("UNOBSERVED_ATOM_XYZ", "type", ii)
                    tS = "Some atom coordinates in this residue are not reported in model %s" % modelId
                    cObj.setValue(tS, "description", ii)
                    cObj.setValue("partially modeled residue", "name", ii)
                #
                cObj.setValue(str(1), "feature_id", ii)
                #
                cObj.setValue(";".join([str(rTup[0]) for rTup in rTupL]), "feature_positions_beg_seq_id", ii)
                cObj.setValue(";".join([str(rTup[1]) for rTup in rTupL]), "feature_positions_end_seq_id", ii)
                #
                cObj.setValue("PDB entity", "reference_scheme", ii)
                cObj.setValue("PDB", "provenance_source", ii)
                cObj.setValue("V1.0", "assignment_version", ii)
                #
                ii += 1

            npbD = self.__commonU.getBoundNonpolymersByInstance(dataContainer)
            jj = 1
            for asymId, rTupL in npbD.items():
                for rTup in rTupL:
                    if rTup.connectType in ["covalent bond"]:
                        fType = "HAS_COVALENT_LINKAGE"
                        fId = "COVALENT_LINKAGE_%d" % jj

                    elif rTup.connectType in ["metal coordination"]:
                        fType = "HAS_METAL_COORDINATION_LINKAGE"
                        fId = "METAL_COORDINATION_LINKAGE_%d" % jj
                    else:
                        continue

                    entityId = asymIdD[asymId]
                    authAsymId = asymAuthIdD[asymId]
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue(rTup.targetCompId, "comp_id", ii)
                    cObj.setValue(fId, "feature_id", ii)
                    cObj.setValue(fType, "type", ii)
                    #
                    # ("targetCompId", "connectType", "partnerCompId", "partnerAsymId", "partnerEntityType", "bondDistance", "bondOrder")
                    cObj.setValue(
                        ";".join(["%s has %s with %s instance %s in model 1" % (rTup.targetCompId, rTup.connectType, rTup.partnerEntityType, rTup.partnerAsymId) for rTup in rTupL]),
                        "feature_value_details",
                        ii,
                    )
                    cObj.setValue(";".join([rTup.partnerCompId if rTup.partnerCompId else "?" for rTup in rTupL]), "feature_value_comp_id", ii)
                    cObj.setValue(";".join([rTup.bondDistance if rTup.bondDistance else "?" for rTup in rTupL]), "feature_value_reported", ii)
                    cObj.setValue(";".join(["?" for rTup in rTupL]), "feature_value_reference", ii)
                    cObj.setValue(";".join(["?" for rTup in rTupL]), "feature_value_uncertainty_estimate", ii)
                    cObj.setValue(";".join(["?" for rTup in rTupL]), "feature_value_uncertainty_estimate_type", ii)

                    cObj.setValue("PDB", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    #
                    ii += 1
                    jj += 1

            return True
        except Exception as e:
            logger.exception("%s %s failing with %s", dataContainer.getName(), catName, str(e))
        return False

    def addProtSecStructInfo(self, dataContainer, catName, **kwargs):
        """
        Add category rcsb_prot_sec_struct_info.

        """
        try:
            logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
            # Exit if source categories are missing
            if not dataContainer.exists("entry") and not (dataContainer.exists("struct_conf") or dataContainer.exists("struct_sheet_range")):
                return False
            #
            # Create the new target category rcsb_prot_sec_struct_info
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            sD = self.__commonU.getProtSecStructFeatures(dataContainer)
            # catName = rcsb_prot_sec_struct_info
            cObj = dataContainer.getObj(catName)
            #
            xObj = dataContainer.getObj("entry")
            entryId = xObj.getValue("id", 0)
            #
            for ii, asymId in enumerate(sD["helixCountD"]):
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(asymId, "label_asym_id", ii)
                #
                cObj.setValue(sD["helixCountD"][asymId], "helix_count", ii)
                cObj.setValue(sD["sheetStrandCountD"][asymId], "beta_strand_count", ii)
                cObj.setValue(sD["unassignedCountD"][asymId], "unassigned_count", ii)
                #
                cObj.setValue(",".join([str(t) for t in sD["helixLengthD"][asymId]]), "helix_length", ii)
                cObj.setValue(",".join([str(t) for t in sD["sheetStrandLengthD"][asymId]]), "beta_strand_length", ii)
                cObj.setValue(",".join([str(t) for t in sD["unassignedLengthD"][asymId]]), "unassigned_length", ii)

                cObj.setValue("%.2f" % (100.0 * sD["helixFracD"][asymId]), "helix_coverage_percent", ii)
                cObj.setValue("%.2f" % (100.0 * sD["sheetStrandFracD"][asymId]), "beta_strand_coverage_percent", ii)
                cObj.setValue("%.2f" % (100.0 * sD["unassignedFracD"][asymId]), "unassigned_coverage_percent", ii)

                cObj.setValue(",".join(sD["sheetSenseD"][asymId]), "beta_sheet_sense", ii)
                cObj.setValue(",".join([str(t) for t in sD["sheetFullStrandCountD"][asymId]]), "beta_sheet_strand_count", ii)

                cObj.setValue(sD["featureMonomerSequenceD"][asymId], "feature_monomer_sequence", ii)
                cObj.setValue(sD["featureSequenceD"][asymId], "feature_sequence", ii)

            return True
        except Exception as e:
            logger.exception("For %s %r failing with %s", dataContainer.getName(), catName, str(e))
        return False

    def addConnectionDetails(self, dataContainer, catName, **kwargs):
        """ Build rcsb_struct_conn category -

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance
            catName (str): category name

        Returns:
            bool: True for success or False otherwise

        Example:
                loop_
                _rcsb_struct_conn.ordinal_id
                _rcsb_struct_conn.id
                _rcsb_struct_conn.conn_type
                _rcsb_struct_conn.connect_target_label_comp_id
                _rcsb_struct_conn.connect_target_label_asym_id
                _rcsb_struct_conn.connect_target_label_seq_id
                _rcsb_struct_conn.connect_target_label_atom_id
                _rcsb_struct_conn.connect_target_label_alt_id
                _rcsb_struct_conn.connect_target_auth_asym_id
                _rcsb_struct_conn.connect_target_auth_seq_id
                _rcsb_struct_conn.connect_target_symmetry
                _rcsb_struct_conn.connect_partner_label_comp_id
                _rcsb_struct_conn.connect_partner_label_asym_id
                _rcsb_struct_conn.connect_partner_label_seq_id
                _rcsb_struct_conn.connect_partner_label_atom_id
                _rcsb_struct_conn.connect_partner_label_alt_id
                _rcsb_struct_conn.connect_partner_symmetry
                _rcsb_struct_conn.details

                # - - - - data truncated for brevity - - - -
        """
        try:
            logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
            # Exit if source categories are missing
            if not dataContainer.exists("entry") and not dataContainer.exists("struct_conn"):
                return False
            #
            # Create the new target category rcsb_struct_conn
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            cDL = self.__commonU.getInstanceConnections(dataContainer)
            asymIdD = self.__commonU.getInstanceEntityMap(dataContainer)
            asymAuthIdD = self.__commonU.getAsymAuthIdMap(dataContainer)
            #
            # catName = rcsb_struct_conn
            cObj = dataContainer.getObj(catName)
            #
            xObj = dataContainer.getObj("entry")
            entryId = xObj.getValue("id", 0)
            #
            for ii, cD in enumerate(cDL):
                asymId = cD["connect_target_label_asym_id"]
                entityId = asymIdD[asymId]
                authAsymId = asymAuthIdD[asymId] if asymId in asymAuthIdD else None
                cObj.setValue(ii + 1, "ordinal_id", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(asymId, "asym_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                if authAsymId:
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                else:
                    logger.error("Missing mapping for %s asymId %s to authAsymId ", entryId, asymId)
                for ky, val in cD.items():
                    cObj.setValue(val, ky, ii)
                #
            return True
        except Exception as e:
            logger.exception("For %s %r failing with %s", dataContainer.getName(), catName, str(e))
        return False

    def __stripWhiteSpace(self, val):
        """ Remove all white space from the input value.

        """
        if val is None:
            return val
        return self.__wsPattern.sub("", val)

    def buildInstanceValidationFeatures(self, dataContainer, catName, **kwargs):
        """ Build category rcsb_entity_instance_validation_feature ...

        Example:
            loop_
            _rcsb_entity_instance_validation_feature.ordinal
            _rcsb_entity_instance_validation_feature.entry_id
            _rcsb_entity_instance_validation_feature.entity_id
            _rcsb_entity_instance_validation_feature.asym_id
            _rcsb_entity_instance_validation_feature.auth_asym_id
            _rcsb_entity_instance_validation_feature.feature_id
            _rcsb_entity_instance_validation_feature.type
            _rcsb_entity_instance_validation_feature.name
            _rcsb_entity_instance_validation_feature.description
            _rcsb_entity_instance_validation_feature.annotation_lineage_id
            _rcsb_entity_instance_validation_feature.annotation_lineage_name
            _rcsb_entity_instance_validation_feature.annotation_lineage_depth
            _rcsb_entity_instance_validation_feature.reference_scheme
            _rcsb_entity_instance_validation_feature.provenance_source
            _rcsb_entity_instance_validation_feature.assignment_version
            _rcsb_entity_instance_validation_feature.feature_positions_beg_seq_id
            _rcsb_entity_instance_validation_feature.feature_positions_end_seq_id
            _rcsb_entity_instance_validation_feature.feature_positions_beg_comp_id
            #
            _rcsb_entity_instance_validation_feature.feature_value_comp_id
            _rcsb_entity_instance_validation_feature.feature_value_reported
            _rcsb_entity_instance_validation_feature.feature_value_reference
            _rcsb_entity_instance_validation_feature.feature_value_uncertainty_estimate
            _rcsb_entity_instance_validation_feature.feature_value_uncertainty_estimate_type
            _rcsb_entity_instance_validation_feature.feature_value_details

        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        typeMapD = {
            "ROTAMER_OUTLIER": "Molprobity rotamer outlier",
            "RAMACHANDRAN_OUTLIER": "Molprobity Ramachandran outlier",
            "RSRZ_OUTLIER": "Real space R-value Z score > 2",
            "RSRCC_OUTLIER": "Real space density correlation value < 0.65",
            "MOGUL_BOND_OUTLIER": "Mogul bond distance outlier",
            "MOGUL_ANGLE_OUTLIER": "Mogul bond angle outlier",
            "BOND_OUTLIER": "Molprobity bond distance outlier",
            "ANGLE_OUTLIER": "Molprobity bond angle outlier",
        }
        try:
            if catName != "rcsb_entity_instance_validation_feature":
                return False
            # Exit if source categories are missing
            if not dataContainer.exists("entry"):
                return False
            #
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            cObj = dataContainer.getObj(catName)
            ii = cObj.getRowCount()
            #
            asymIdD = self.__commonU.getInstanceEntityMap(dataContainer)
            asymAuthIdD = self.__commonU.getAsymAuthIdMap(dataContainer)
            #
            instanceModelOutlierD = self.__commonU.getInstanceModelOutlierInfo(dataContainer)
            #
            # ("OutlierValue", "compId, seqId, outlierType, description, reported, reference, uncertaintyValue, uncertaintyType")
            #
            logger.debug("Length instanceModelOutlierD %d", len(instanceModelOutlierD))
            #
            # (modelId, asymId), []).append((compId, int(seqId), "RSRCC_OUTLIER", tS)
            for (modelId, asymId, hasSeq), pTupL in instanceModelOutlierD.items():
                fTypeL = sorted(set([pTup.outlierType for pTup in pTupL]))
                jj = 1
                for fType in fTypeL:
                    if (asymId not in asymIdD) or (asymId not in asymAuthIdD):
                        continue
                    entityId = asymIdD[asymId]
                    authAsymId = asymAuthIdD[asymId]
                    #
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)

                    #
                    cObj.setValue(fType, "type", ii)
                    tN = typeMapD[fType] if fType in typeMapD else fType
                    cObj.setValue(tN, "name", ii)
                    #
                    tFn = "%s_%d" % (fType, jj)
                    cObj.setValue(tFn, "feature_id", ii)
                    #
                    if hasSeq:
                        descriptionS = tN + " in instance %s model %s" % (asymId, modelId)
                        cObj.setValue(";".join([pTup.compId for pTup in pTupL if pTup.outlierType == fType]), "feature_positions_beg_comp_id", ii)
                        cObj.setValue(";".join([str(pTup.seqId) for pTup in pTupL if pTup.outlierType == fType]), "feature_positions_beg_seq_id", ii)

                    else:
                        cObj.setValue(pTupL[0].compId, "comp_id", ii)
                        descriptionS = tN + " in %s instance %s model %s" % (pTupL[0].compId, asymId, modelId)
                        cObj.setValue(";".join([pTup.compId if pTup.compId else "?" for pTup in pTupL if pTup.outlierType == fType]), "feature_value_comp_id", ii)
                        cObj.setValue(";".join([pTup.description if pTup.description else "?" for pTup in pTupL if pTup.outlierType == fType]), "feature_value_details", ii)
                        cObj.setValue(";".join([pTup.reported if pTup.reported else "?" for pTup in pTupL if pTup.outlierType == fType]), "feature_value_reported", ii)
                        cObj.setValue(";".join([pTup.reference if pTup.reference else "?" for pTup in pTupL if pTup.outlierType == fType]), "feature_value_reference", ii)
                        cObj.setValue(
                            ";".join([pTup.uncertaintyValue if pTup.uncertaintyValue else "?" for pTup in pTupL if pTup.outlierType == fType]), "feature_value_uncertainty_estimate", ii,
                        )
                        cObj.setValue(
                            ";".join([pTup.uncertaintyType if pTup.uncertaintyType else "?" for pTup in pTupL if pTup.outlierType == fType]),
                            "feature_value_uncertainty_estimate_type",
                            ii,
                        )
                    cObj.setValue("PDB entity", "reference_scheme", ii)
                    cObj.setValue(descriptionS, "description", ii)
                    cObj.setValue("PDB", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    #
                    jj += 1
                    ii += 1
            #
            ##
            return True
        except Exception as e:
            logger.exception("For %s %r failing with %s", dataContainer.getName(), catName, str(e))
        return False

    # --- JDW
    def buildInstanceValidationFeatureSummaryPrev(self, dataContainer, catName, **kwargs):
        """ Build category rcsb_entity_instance_validation_feature_summary

        Example:

            loop_
            _rcsb_entity_instance_validation_feature_summary.ordinal
            _rcsb_entity_instance_validation_feature_summary.entry_id
            _rcsb_entity_instance_validation_feature_summary.entity_id
            _rcsb_entity_instance_validation_feature_summary.asym_id
            _rcsb_entity_instance_validation_feature_summary.auth_asym_id
            #validation_
            _rcsb_entity_instance_validation_feature_summary.type
            _rcsb_entity_instance_validation_feature_summary.count
            _rcsb_entity_instance_validation_feature_summary.coverage
            # ...
        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_instance_validation_feature_summary":
                return False
            if not dataContainer.exists("rcsb_entity_instance_validation_feature") and not dataContainer.exists("entry"):
                return False

            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            sObj = dataContainer.getObj(catName)
            fObj = dataContainer.getObj("rcsb_entity_instance_validation_feature")
            #
            instIdMapD = self.__commonU.getInstanceIdMap(dataContainer)
            instEntityD = self.__commonU.getInstanceEntityMap(dataContainer)
            entityPolymerLengthD = self.__commonU.getPolymerEntityLengthsEnumerated(dataContainer)
            asymAuthD = self.__commonU.getAsymAuthIdMap(dataContainer)

            fCountD = OrderedDict()
            fMonomerCountD = OrderedDict()
            fInstanceCountD = OrderedDict()
            for ii in range(fObj.getRowCount()):
                asymId = fObj.getValue("asym_id", ii)
                # ---- initialize counts
                # fCountD = self.__initializeInstanceValidationFeatureType(dataContainer, asymId, fCountD, countType="set")
                # fMonomerCountD = self.__initializeInstanceValidationFeatureType(dataContainer, asymId, fMonomerCountD, countType="list")
                # fInstanceCountD = self.__initializeInstanceValidationFeatureType(dataContainer, asymId, fInstanceCountD, countType="list")
                # ----
                fType = fObj.getValue("type", ii)
                fId = fObj.getValue("feature_id", ii)
                fCountD.setdefault(asymId, {}).setdefault(fType, set()).add(fId)
                #
                tbegS = fObj.getValueOrDefault("feature_positions_beg_seq_id", ii, defaultValue=None)
                tendS = fObj.getValueOrDefault("feature_positions_end_seq_id", ii, defaultValue=None)
                if fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS is not None and fObj.hasAttribute("feature_positions_end_seq_id") and tendS is not None:
                    begSeqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    endSeqIdL = str(fObj.getValue("feature_positions_end_seq_id", ii)).split(";")
                    monCount = 0
                    for begSeqId, endSeqId in zip(begSeqIdL, endSeqIdL):
                        try:
                            monCount += abs(int(endSeqId) - int(begSeqId) + 1)
                        except Exception:
                            logger.warning(
                                "In %s fType %r fId %r bad sequence range begSeqIdL %r endSeqIdL %r tbegS %r tendS %r",
                                dataContainer.getName(),
                                fType,
                                fId,
                                begSeqIdL,
                                endSeqIdL,
                                tbegS,
                                tendS,
                            )
                    fMonomerCountD.setdefault(asymId, {}).setdefault(fType, []).append(monCount)
                elif fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS:
                    seqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    fMonomerCountD.setdefault(asymId, {}).setdefault(fType, []).append(len(seqIdL))

                tS = fObj.getValueOrDefault("feature_value_details", ii, defaultValue=None)
                if fObj.hasAttribute("feature_value_details") and tS is not None:
                    dL = str(fObj.getValue("feature_value_details", ii)).split(";")
                    fInstanceCountD.setdefault(asymId, {}).setdefault(fType, []).append(len(dL))
            #
            # logger.debug("%s fCountD %r", entryId, fCountD)
            #
            ii = 0
            for asymId, fTypeD in fCountD.items():
                entityId = instEntityD[asymId]
                authAsymId = asymAuthD[asymId]
                for fType, fS in fTypeD.items():
                    #
                    sObj.setValue(ii + 1, "ordinal", ii)
                    sObj.setValue(entryId, "entry_id", ii)
                    sObj.setValue(entityId, "entity_id", ii)
                    sObj.setValue(asymId, "asym_id", ii)
                    if asymId in instIdMapD and "comp_id" in instIdMapD[asymId] and instIdMapD[asymId]["comp_id"]:
                        sObj.setValue(instIdMapD[asymId]["comp_id"], "comp_id", ii)
                    sObj.setValue(authAsymId, "auth_asym_id", ii)
                    sObj.setValue(fType, "type", ii)
                    fracC = 0.0
                    #
                    if asymId in fMonomerCountD and fType in fMonomerCountD[asymId] and fMonomerCountD[asymId][fType]:
                        fCount = sum(fMonomerCountD[asymId][fType])
                        if asymId in fMonomerCountD and fType in fMonomerCountD[asymId] and entityId in entityPolymerLengthD:
                            fracC = float(sum(fMonomerCountD[asymId][fType])) / float(entityPolymerLengthD[entityId])
                    elif asymId in fInstanceCountD and fType in fInstanceCountD[asymId] and fInstanceCountD[asymId][fType]:
                        fCount = sum(fInstanceCountD[asymId][fType])
                    else:
                        fCount = len(fS)
                    #
                    sObj.setValue(fCount, "count", ii)
                    sObj.setValue(round(fracC, 5), "coverage", ii)
                    #
                    ii += 1

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return True

    def __initializeInstanceValidationFeatureType(self, dataContainer, asymId, fCountD, countType="set"):
        instTypeD = self.__commonU.getInstanceTypes(dataContainer)
        eType = instTypeD[asymId]
        # rcsb_entity_instance_validation_feature_summary.type
        if eType == "polymer":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_entity_instance_validation_feature_summary", "type")
        elif eType in ["non-polymer", "water"]:
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_entity_instance_validation_feature_summary", "type")
        elif eType == "branched":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_entity_instance_validation_feature_summary", "type")
        else:
            logger.error("%r asymId %r eType %r", dataContainer.getName(), asymId, eType)
        #
        fTypeL = sorted([tup[0] for tup in eTupL])
        #
        for fType in fTypeL:
            if countType == "set":
                fCountD.setdefault(asymId, {}).setdefault(fType, set())
            else:
                fCountD.setdefault(asymId, {}).setdefault(fType, [])
            #
        return fCountD

    # --- JDW
    def __getInstanceFeatureTypes(self, eType):
        #
        vTupL = self.__dApi.getEnumListWithDetail("rcsb_entity_instance_validation_feature_summary", "type")
        if eType == "polymer":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_polymer_instance_feature_summary", "type")
        elif eType in ["non-polymer", "water"]:
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_nonpolymer_instance_feature_summary", "type")
        elif eType == "branched":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_branched_instance_feature_summary", "type")
        else:
            logger.error("Unexpected eType %r -- no features types provided", eType)
            eTupL = []
        # Distinct elements in the instance specific categories. (remove validation types)
        vTypeL = sorted([tup[0] for tup in vTupL])
        iTypeL = sorted([tup[0] for tup in eTupL])
        fTypeL = sorted(set(iTypeL) - set(vTypeL))
        return fTypeL

    def __getInstanceValidationFeatureTypes(self, eType):
        #
        vTupL = self.__dApi.getEnumListWithDetail("rcsb_entity_instance_validation_feature_summary", "type")
        if eType == "polymer":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_polymer_instance_feature_summary", "type")
        elif eType in ["non-polymer", "water"]:
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_nonpolymer_instance_feature_summary", "type")
        elif eType == "branched":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_branched_instance_feature_summary", "type")
        else:
            logger.error("Unexpected eType %r -- no features types provided", eType)
            eTupL = []
        # Common elements in the instance specific categories.
        vTypeL = sorted([tup[0] for tup in vTupL])
        iTypeL = sorted([tup[0] for tup in eTupL])
        fTypeL = sorted(set(vTypeL).intersection(iTypeL))
        return fTypeL

    # --- JDW
    def buildEntityInstanceFeatureSummary(self, dataContainer, catName, **kwargs):
        """ Build category rcsb_entity_instance_feature_summary (UPDATED)

        Example:

            loop_
            _rcsb_entity_instance_feature_summary.ordinal
            _rcsb_entity_instance_feature_summary.entry_id
            _rcsb_entity_instance_feature_summary.entity_id
            _rcsb_entity_instance_feature_summary.asym_id
            _rcsb_entity_instance_feature_summary.auth_asym_id
            #
            _rcsb_entity_instance_feature_summary.type
            _rcsb_entity_instance_feature_summary.count
            _rcsb_entity_instance_feature_summary.coverage
            # ...
        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_instance_feature_summary":
                return False
            if not dataContainer.exists("rcsb_entity_instance_feature") and not dataContainer.exists("entry"):
                return False

            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            sObj = dataContainer.getObj(catName)
            fObj = dataContainer.getObj("rcsb_entity_instance_feature")
            #
            instEntityD = self.__commonU.getInstanceEntityMap(dataContainer)
            entityPolymerLengthD = self.__commonU.getPolymerEntityLengthsEnumerated(dataContainer)
            # typeList = self.__dApi.getEnumList("rcsb_entity_instance_feature_summary", "type", sortFlag=True)
            asymAuthD = self.__commonU.getAsymAuthIdMap(dataContainer)
            instIdMapD = self.__commonU.getInstanceIdMap(dataContainer)
            instTypeD = self.__commonU.getInstanceTypes(dataContainer)
            #
            fCountD = OrderedDict()
            fValuesD = OrderedDict()
            fMonomerCountD = OrderedDict()
            for ii in range(fObj.getRowCount()):
                asymId = fObj.getValue("asym_id", ii)
                # ---- initialize counts
                # fCountD = self.__initializeInstanceFeatureType(dataContainer, asymId, fCountD, countType="set")
                # fMonomerCountD = self.__initializeInstanceFeatureType(dataContainer, asymId, fMonomerCountD, countType="list")
                # ----
                fType = fObj.getValue("type", ii)
                fId = fObj.getValue("feature_id", ii)
                fCountD.setdefault(asymId, {}).setdefault(fType, set()).add(fId)
                #
                tbegS = fObj.getValueOrDefault("feature_positions_beg_seq_id", ii, defaultValue=None)
                tendS = fObj.getValueOrDefault("feature_positions_end_seq_id", ii, defaultValue=None)
                if fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS is not None and fObj.hasAttribute("feature_positions_end_seq_id") and tendS is not None:
                    begSeqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    endSeqIdL = str(fObj.getValue("feature_positions_end_seq_id", ii)).split(";")
                    monCount = 0
                    for begSeqId, endSeqId in zip(begSeqIdL, endSeqIdL):
                        try:
                            monCount += abs(int(endSeqId) - int(begSeqId) + 1)
                        except Exception:
                            logger.warning(
                                "%s fType %r fId %r bad sequence begSeqIdL %r endSeqIdL %r tbegS %r tendS %r", dataContainer.getName(), fType, fId, begSeqIdL, endSeqIdL, tbegS, tendS,
                            )

                    fMonomerCountD.setdefault(asymId, {}).setdefault(fType, []).append(monCount)
                elif fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS:
                    seqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    fMonomerCountD.setdefault(asymId, {}).setdefault(fType, []).append(len(seqIdL))
                # JDW
                elif fObj.hasAttribute("feature_value_reported"):
                    tValue = fObj.getValueOrDefault("feature_value_reported", ii, defaultValue=None)
                    if tValue:
                        try:
                            tvL = [float(t) for t in tValue.split(";")]
                            fValuesD.setdefault(asymId, {}).setdefault(fType, []).extend(tvL)
                        except Exception:
                            pass

            #
            logger.debug("%s fCountD %r", entryId, fCountD)
            #

            ii = 0
            for asymId, entityId in instEntityD.items():
                eType = instTypeD[asymId]
                authAsymId = asymAuthD[asymId]
                fTypeL = self.__getInstanceFeatureTypes(eType)
                # All entity type specific features
                for fType in fTypeL:
                    sObj.setValue(ii + 1, "ordinal", ii)
                    sObj.setValue(entryId, "entry_id", ii)
                    sObj.setValue(entityId, "entity_id", ii)
                    sObj.setValue(asymId, "asym_id", ii)
                    sObj.setValue(authAsymId, "auth_asym_id", ii)
                    # add comp
                    if asymId in instIdMapD and "comp_id" in instIdMapD[asymId] and instIdMapD[asymId]["comp_id"]:
                        sObj.setValue(instIdMapD[asymId]["comp_id"], "comp_id", ii)
                    sObj.setValue(fType, "type", ii)
                    fracC = 0.0
                    minL = maxL = 0
                    if asymId in fMonomerCountD and fType in fMonomerCountD[asymId]:
                        if fType.startswith("UNOBSERVED"):
                            fCount = sum(fMonomerCountD[asymId][fType])
                        else:
                            fCount = len(fCountD[asymId][fType])

                        if entityId in entityPolymerLengthD:
                            fracC = float(sum(fMonomerCountD[asymId][fType])) / float(entityPolymerLengthD[entityId])

                        if fType in ["CATH", "SCOP", "HELIX_P", "SHEET", "UNASSIGNED_SEC_STRUCT", "UNOBSERVED_RESIDUE_XYZ", "ZERO_OCCUPANCY_RESIDUE_XYZ"]:
                            minL = min(fMonomerCountD[asymId][fType])
                            maxL = max(fMonomerCountD[asymId][fType])

                    elif asymId in fCountD and fType in fCountD[asymId] and fCountD[asymId][fType]:
                        fCount = len(fCountD[asymId][fType])
                    else:
                        fCount = 0
                    #
                    minV = maxV = 0
                    if asymId in fValuesD and fType in fValuesD[asymId]:
                        if fType in ["HAS_COVALENT_LINKAGE", "HAS_METAL_COORDINATION_LINKAGE"]:
                            try:
                                minV = min(fValuesD[asymId][fType])
                                maxV = max(fValuesD[asymId][fType])
                            except Exception:
                                pass

                    sObj.setValue(fCount, "count", ii)
                    sObj.setValue(round(fracC, 5), "coverage", ii)
                    if minL is not None:
                        sObj.setValue(minL, "minimum_length", ii)
                        sObj.setValue(maxL, "maximum_length", ii)
                    if minV is not None:
                        sObj.setValue(minV, "minimum_value", ii)
                        sObj.setValue(maxV, "maximum_value", ii)
                    #
                    ii += 1
        except Exception as e:
            logger.exception("Failing for %s with %s", dataContainer.getName(), str(e))
        return True

    def buildInstanceValidationFeatureSummary(self, dataContainer, catName, **kwargs):
        """ Build category rcsb_entity_instance_validation_feature_summary

        Example:

            loop_
            _rcsb_entity_instance_validation_feature_summary.ordinal
            _rcsb_entity_instance_validation_feature_summary.entry_id
            _rcsb_entity_instance_validation_feature_summary.entity_id
            _rcsb_entity_instance_validation_feature_summary.asym_id
            _rcsb_entity_instance_validation_feature_summary.auth_asym_id
            _rcsb_entity_instance_validation_feature_summary.type
            _rcsb_entity_instance_validation_feature_summary.count
            _rcsb_entity_instance_validation_feature_summary.coverage
            # ...
        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_instance_validation_feature_summary":
                return False
            if not dataContainer.exists("rcsb_entity_instance_validation_feature") and not dataContainer.exists("entry"):
                return False

            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            sObj = dataContainer.getObj(catName)
            fObj = dataContainer.getObj("rcsb_entity_instance_validation_feature")
            #
            instIdMapD = self.__commonU.getInstanceIdMap(dataContainer)
            instEntityD = self.__commonU.getInstanceEntityMap(dataContainer)
            entityPolymerLengthD = self.__commonU.getPolymerEntityLengthsEnumerated(dataContainer)
            asymAuthD = self.__commonU.getAsymAuthIdMap(dataContainer)
            instTypeD = self.__commonU.getInstanceTypes(dataContainer)

            fCountD = OrderedDict()
            fMonomerCountD = OrderedDict()
            fInstanceCountD = OrderedDict()
            for ii in range(fObj.getRowCount()):
                asymId = fObj.getValue("asym_id", ii)
                fType = fObj.getValue("type", ii)
                fId = fObj.getValue("feature_id", ii)
                fCountD.setdefault(asymId, {}).setdefault(fType, set()).add(fId)
                #
                tbegS = fObj.getValueOrDefault("feature_positions_beg_seq_id", ii, defaultValue=None)
                tendS = fObj.getValueOrDefault("feature_positions_end_seq_id", ii, defaultValue=None)
                if fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS is not None and fObj.hasAttribute("feature_positions_end_seq_id") and tendS is not None:
                    begSeqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    endSeqIdL = str(fObj.getValue("feature_positions_end_seq_id", ii)).split(";")
                    monCount = 0
                    for begSeqId, endSeqId in zip(begSeqIdL, endSeqIdL):
                        try:
                            monCount += abs(int(endSeqId) - int(begSeqId) + 1)
                        except Exception:
                            logger.warning(
                                "In %s fType %r fId %r bad sequence range begSeqIdL %r endSeqIdL %r tbegS %r tendS %r",
                                dataContainer.getName(),
                                fType,
                                fId,
                                begSeqIdL,
                                endSeqIdL,
                                tbegS,
                                tendS,
                            )
                    fMonomerCountD.setdefault(asymId, {}).setdefault(fType, []).append(monCount)
                elif fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS:
                    seqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    fMonomerCountD.setdefault(asymId, {}).setdefault(fType, []).append(len(seqIdL))

                tS = fObj.getValueOrDefault("feature_value_details", ii, defaultValue=None)
                if fObj.hasAttribute("feature_value_details") and tS is not None:
                    dL = str(fObj.getValue("feature_value_details", ii)).split(";")
                    fInstanceCountD.setdefault(asymId, {}).setdefault(fType, []).append(len(dL))
            #
            ii = 0
            # Summarize all instances -
            for asymId, entityId in instEntityD.items():
                eType = instTypeD[asymId]
                authAsymId = asymAuthD[asymId]
                fTypeL = self.__getInstanceValidationFeatureTypes(eType)
                # All entity type specific features
                for fType in fTypeL:
                    #
                    sObj.setValue(ii + 1, "ordinal", ii)
                    sObj.setValue(entryId, "entry_id", ii)
                    sObj.setValue(entityId, "entity_id", ii)
                    sObj.setValue(asymId, "asym_id", ii)
                    if asymId in instIdMapD and "comp_id" in instIdMapD[asymId] and instIdMapD[asymId]["comp_id"]:
                        sObj.setValue(instIdMapD[asymId]["comp_id"], "comp_id", ii)
                    sObj.setValue(authAsymId, "auth_asym_id", ii)
                    sObj.setValue(fType, "type", ii)
                    #
                    # Sum features of different granularities
                    #
                    fracC = 0.0
                    if asymId in fMonomerCountD and fType in fMonomerCountD[asymId] and fMonomerCountD[asymId][fType]:
                        fCount = sum(fMonomerCountD[asymId][fType])
                        if asymId in fMonomerCountD and fType in fMonomerCountD[asymId] and entityId in entityPolymerLengthD:
                            fracC = float(sum(fMonomerCountD[asymId][fType])) / float(entityPolymerLengthD[entityId])
                    elif asymId in fInstanceCountD and fType in fInstanceCountD[asymId] and fInstanceCountD[asymId][fType]:
                        fCount = sum(fInstanceCountD[asymId][fType])
                    elif asymId in fCountD and fType in fCountD[asymId] and fCountD[asymId][fType]:
                        fCount = len(fCountD[asymId][fType])
                    else:
                        # default zero value
                        fCount = 0
                    #
                    sObj.setValue(fCount, "count", ii)
                    sObj.setValue(round(fracC, 5), "coverage", ii)
                    #
                    ii += 1

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return True

    #
    def buildEntityInstanceAnnotations(self, dataContainer, catName, **kwargs):
        """ Build category rcsb_entity_instance_annotation ...

        Example:
            loop_
            _rcsb_entity_instance_annotation.ordinal
            _rcsb_entity_instance_annotation.entry_id
            _rcsb_entity_instance_annotation.entity_id
            _rcsb_entity_instance_annotation.asym_id
            _rcsb_entity_instance_annotation.auth_asym_id
            _rcsb_entity_instance_annotation.annotation_id
            _rcsb_entity_instance_annotation.type
            _rcsb_entity_instance_annotation.name
            _rcsb_entity_instance_annotation.description
            _rcsb_entity_instance_annotation.annotation_lineage_id
            _rcsb_entity_instance_annotation.annotation_lineage_name
            _rcsb_entity_instance_annotation.annotation_lineage_depth
            _rcsb_entity_instance_annotation.reference_scheme
            _rcsb_entity_instance_annotation.provenance_source
            _rcsb_entity_instance_annotation.assignment_version

        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_instance_annotation":
                return False
            # Exit if source categories are missing
            if not dataContainer.exists("entry"):
                return False
            #
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            cObj = dataContainer.getObj(catName)
            #
            rP = kwargs.get("resourceProvider")

            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            asymIdD = self.__commonU.getInstanceEntityMap(dataContainer)
            asymAuthIdD = self.__commonU.getAsymAuthIdMap(dataContainer)
            # asymIdRangesD = self.__commonU.getInstancePolymerRanges(dataContainer)
            # pAuthAsymD = self.__commonU.getPolymerIdMap(dataContainer)
            instTypeD = self.__commonU.getInstanceTypes(dataContainer)
            # ---------------
            # Add CATH assignments
            cathU = rP.getResource("CathProvider instance") if rP else None
            ii = cObj.getRowCount()
            #
            for asymId, authAsymId in asymAuthIdD.items():
                if instTypeD[asymId] not in ["polymer", "branched"]:
                    continue
                entityId = asymIdD[asymId]
                dL = cathU.getCathResidueRanges(entryId.lower(), authAsymId)
                logger.debug("%s asymId %s authAsymId %s dL %r", entryId, asymId, authAsymId, dL)
                vL = cathU.getCathVersions(entryId.lower(), authAsymId)
                for (cathId, domId, _, _, _) in dL:
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue("CATH", "type", ii)
                    #
                    cObj.setValue(str(cathId), "annotation_id", ii)
                    # cObj.setValue(str(domId), "annotation_id", ii)
                    # cObj.setValue(cathId, "name", ii)
                    cObj.setValue(cathU.getCathName(cathId), "name", ii)
                    #
                    cObj.setValue(";".join(cathU.getNameLineage(cathId)), "annotation_lineage_name", ii)
                    idLinL = cathU.getIdLineage(cathId)
                    cObj.setValue(";".join(idLinL), "annotation_lineage_id", ii)
                    cObj.setValue(";".join([str(jj) for jj in range(1, len(idLinL) + 1)]), "annotation_lineage_depth", ii)
                    #
                    cObj.setValue("CATH", "provenance_source", ii)
                    cObj.setValue(vL[0], "assignment_version", ii)
                    #
                    ii += 1
            # ------------
            # Add SCOP assignments
            scopU = rP.getResource("ScopProvider instance") if rP else None
            for asymId, authAsymId in asymAuthIdD.items():
                if instTypeD[asymId] not in ["polymer", "branched"]:
                    continue
                entityId = asymIdD[asymId]
                dL = scopU.getScopResidueRanges(entryId.lower(), authAsymId)
                version = scopU.getScopVersion()
                for (sunId, domId, _, _, _, _) in dL:
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue("SCOP", "type", ii)
                    #
                    # cObj.setValue(str(sunId), "domain_id", ii)
                    cObj.setValue(domId, "annotation_id", ii)
                    cObj.setValue(scopU.getScopName(sunId), "name", ii)
                    #
                    tL = [t if t is not None else "" for t in scopU.getNameLineage(sunId)]
                    cObj.setValue(";".join(tL), "annotation_lineage_name", ii)
                    idLinL = scopU.getIdLineage(sunId)
                    cObj.setValue(";".join([str(t) for t in idLinL]), "annotation_lineage_id", ii)
                    cObj.setValue(";".join([str(jj) for jj in range(1, len(idLinL) + 1)]), "annotation_lineage_depth", ii)
                    #
                    cObj.setValue("SCOPe", "provenance_source", ii)
                    cObj.setValue(version, "assignment_version", ii)
                    #
                    ii += 1
            # ------------
            #  Add covalent attchment property
            npbD = self.__commonU.getBoundNonpolymersByInstance(dataContainer)
            jj = 1
            for asymId, rTupL in npbD.items():
                for rTup in rTupL:
                    if rTup.connectType in ["covalent bond"]:
                        fType = "HAS_COVALENT_LINKAGE"
                        fId = "COVALENT_LINKAGE_%d" % jj

                    elif rTup.connectType in ["metal coordination"]:
                        fType = "HAS_METAL_COORDINATION_LINKAGE"
                        fId = "METAL_COORDINATION_LINKAGE_%d" % jj
                    else:
                        continue

                    entityId = asymIdD[asymId]
                    authAsymId = asymAuthIdD[asymId]
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(asymId, "asym_id", ii)
                    cObj.setValue(authAsymId, "auth_asym_id", ii)
                    cObj.setValue(rTup.targetCompId, "comp_id", ii)
                    cObj.setValue(fId, "annotation_id", ii)
                    cObj.setValue(fType, "type", ii)
                    #
                    # ("targetCompId", "connectType", "partnerCompId", "partnerAsymId", "partnerEntityType", "bondDistance", "bondOrder")
                    cObj.setValue(
                        "%s has %s with %s instance %s in model 1" % (rTup.targetCompId, rTup.connectType, rTup.partnerEntityType, rTup.partnerAsymId), "description", ii,
                    )

                    cObj.setValue("PDB", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    #
                    ii += 1
                    jj += 1

            return True
        except Exception as e:
            logger.exception("%s %s failing with %s", dataContainer.getName(), catName, str(e))
        return False
