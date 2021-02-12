##
# File:    DictMethodEntityHelper.py
# Author:  J. Westbrook
# Date:    16-Jul-2019
# Version: 0.001 Initial version
#
##
"""
Helper class implements methods supporting entity-level item and category methods in the RCSB dictionary extension.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

# pylint: disable=too-many-lines

import functools
import itertools
import logging
import re

from collections import defaultdict, OrderedDict

from mmcif.api.DataCategory import DataCategory
from rcsb.utils.seq.SeqAlign import splitSeqAlignObjList

logger = logging.getLogger(__name__)


def cmpElements(lhs, rhs):
    return 0 if (lhs[-1].isdigit() or lhs[-1] in ["R", "S"]) and rhs[0].isdigit() else -1


class DictMethodEntityHelper(object):
    """Helper class implements methods supporting entity-level item and category methods in the RCSB dictionary extension."""

    def __init__(self, **kwargs):
        """
        Args:
            resourceProvider: (obj) instance of DictMethodResourceProvider()

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
        self.__useSiftsAlign = rP.getReferenceSequenceAlignmentOpt() == "SIFTS"
        # logger.info("SIFTS alignment option %r", self.__useSiftsAlign)
        #
        self.__ssP = None
        if self.__useSiftsAlign:
            self.__ssP = rP.getResource("SiftsSummaryProvider instance") if rP else None
            self.__useSiftsAlign = False if not self.__ssP else self.__useSiftsAlign
        #
        self.__ccP = rP.getResource("ChemCompProvider instance") if rP else None

        #
        logger.debug("Dictionary entity method helper init")

    def __processSiftsAlignments(self, dataContainer):
        #
        tObj = dataContainer.getObj("entry")
        entryId = tObj.getValue("id", 0)
        #
        asymIdD = self.__commonU.getInstanceEntityMap(dataContainer)
        asymAuthIdD = self.__commonU.getAsymAuthIdMap(dataContainer)
        instTypeD = self.__commonU.getInstanceTypes(dataContainer)
        siftsEntityAlignD = {}
        #
        # Process sifts alignments -
        siftsAlignD = {}
        for asymId, authAsymId in asymAuthIdD.items():
            if instTypeD[asymId] not in ["polymer", "branched"]:
                continue
            entityId = asymIdD[asymId]
            # accumulate the sifts alignments by entity.
            # siftsAlignD.setdefault((entryId, entityId), []).extend([SeqAlign("SIFTS", **sa) for sa in self.__ssP.getIdentifiers(entryId, authAsymId, idType="UNPAL")])
            siftsAlignD.setdefault((entryId, entityId), []).extend(self.__ssP.getSeqAlignObjList(entryId, authAsymId))
        for (entryId, entityId), seqAlignObjL in siftsAlignD.items():
            if seqAlignObjL:
                # re-group alignments by common accession
                alRefD = {}
                for seqAlignObj in seqAlignObjL:
                    alRefD.setdefault((seqAlignObj.getDbName(), seqAlignObj.getDbAccession(), seqAlignObj.getDbIsoform()), []).append(seqAlignObj)
                #
                # Get the longest overlapping entity region of each ref alignment -
                for (dbName, dbAcc, dbIsoform), aL in alRefD.items():
                    alGrpD = splitSeqAlignObjList(aL)
                    logger.debug("SIFTS -> entryId %s entityId %s dbName %r dbAcc %r dbIsoform %r alGrpD %r", entryId, entityId, dbName, dbAcc, dbIsoform, alGrpD)
                    for _, grpAlignL in alGrpD.items():

                        lenL = [seqAlignObj.getEntityAlignLength() for seqAlignObj in grpAlignL]
                        idxMax = lenL.index(max(lenL))
                        siftsEntityAlignD.setdefault((entryId, entityId, "SIFTS"), {}).setdefault((dbName, dbAcc, dbIsoform), []).append(grpAlignL[idxMax])
        #
        logger.debug("PROCESSED SIFTS ->  %r", siftsEntityAlignD)
        return siftsEntityAlignD

    def __processPdbAlignments(self, dataContainer):
        #
        tObj = dataContainer.getObj("entry")
        entryId = tObj.getValue("id", 0)
        #
        entityRefAlignmentD = self.__commonU.getEntityReferenceAlignments(dataContainer)
        pdbEntityAlignD = {}
        # --- PDB alignments -
        for entityId, seqAlignObjL in entityRefAlignmentD.items():
            # seqAlignObjL = [SeqAlign("PDB", **sa) for sa in entityAlignL]
            if seqAlignObjL:
                alRefD = {}
                for seqAlignObj in seqAlignObjL:
                    alRefD.setdefault((seqAlignObj.getDbName(), seqAlignObj.getDbAccession(), seqAlignObj.getDbIsoform()), []).append(seqAlignObj)
                for (dbName, dbAcc, dbIsoform), aL in alRefD.items():
                    alGrpD = splitSeqAlignObjList(aL)
                    logger.debug("PDB -> entryId %s entityId %s dbName %r dbAcc %r dbIsoform %r alGrpD %r", entryId, entityId, dbName, dbAcc, dbIsoform, alGrpD)
                    for _, grpAlignL in alGrpD.items():
                        # get the longest overlapping entity region of each ref seq -
                        lenL = [seqAlignObj.getEntityAlignLength() for seqAlignObj in grpAlignL]
                        idxMax = lenL.index(max(lenL))
                        try:
                            tLen = grpAlignL[idxMax].getEntityAlignLength()
                            if tLen and tLen > 0:
                                pdbEntityAlignD.setdefault((entryId, entityId, "PDB"), {}).setdefault((dbName, dbAcc, dbIsoform), []).append(grpAlignL[idxMax])
                            else:
                                logger.warning("Skipping %s inconsistent alignment for entity %r %r", entryId, entityId, seqAlignObjL)
                        except Exception:
                            pass
            #
        logger.debug("PROCESSED PDB   ->  %r", pdbEntityAlignD)
        return pdbEntityAlignD

    def addPolymerEntityReferenceAlignments(self, dataContainer, catName, **kwargs):
        """[summary]

        Args:
            dataContainer ([type]): [description]
            catName ([type]): [description]

        Returns:
            [type]: [description]

        Example:
            _rcsb_polymer_entity_align.ordinal
            _rcsb_polymer_entity_align.entry_id
            _rcsb_polymer_entity_align.entity_id
            #
            _rcsb_polymer_entity_align.reference_database_name
            _rcsb_polymer_entity_align.reference_database_accession
            _rcsb_polymer_entity_align.provenance_source
            #
            _rcsb_polymer_entity_align.aligned_regions_ref_beg_seq_id
            _rcsb_polymer_entity_align.aligned_regions_entity_beg_seq_id
            _rcsb_polymer_entity_align.aligned_regions_length
            #
        """
        dbNameMapD = self.__commonU.getDatabaseNameMap()
        logger.debug("Starting %s catName %s  kwargs %r", dataContainer.getName(), catName, kwargs)
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("entity")):
                return False
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            cObj = dataContainer.getObj(catName)
            #
            pdbEntityAlignD = self.__processPdbAlignments(dataContainer)
            #
            if self.__useSiftsAlign:
                siftsEntityAlignD = self.__processSiftsAlignments(dataContainer)
                logger.debug("siftsEntityAlignD %d", len(siftsEntityAlignD))
                #
                for (entryId, entityId, provSource), refD in siftsEntityAlignD.items():
                    if (entryId, entityId, "PDB") in pdbEntityAlignD:
                        del pdbEntityAlignD[(entryId, entityId, "PDB")]
                    pdbEntityAlignD.update({(entryId, entityId, provSource): refD})
            #
            # ---

            iRow = cObj.getRowCount()
            for (entryId, entityId, provSource), refD in pdbEntityAlignD.items():
                #
                for (dbName, dbAcc, dbIsoform), saoL in refD.items():
                    #
                    if dbName not in dbNameMapD:
                        logger.error("Skipping unsupported reference database %r for entry %s entity %s", dbName, entryId, entityId)
                        continue
                    #
                    cObj.setValue(iRow + 1, "ordinal", iRow)
                    cObj.setValue(entryId, "entry_id", iRow)
                    cObj.setValue(entityId, "entity_id", iRow)
                    #
                    dispDbName = dbNameMapD[dbName]
                    cObj.setValue(dispDbName, "reference_database_name", iRow)
                    cObj.setValue(dbAcc, "reference_database_accession", iRow)
                    if dbIsoform:
                        cObj.setValue(dbIsoform, "reference_database_isoform", iRow)
                    cObj.setValue(provSource, "provenance_source", iRow)
                    #
                    cObj.setValue(",".join([str(sao.getDbSeqIdBeg()) for sao in saoL]), "aligned_regions_ref_beg_seq_id", iRow)
                    cObj.setValue(",".join([str(sao.getEntitySeqIdBeg()) for sao in saoL]), "aligned_regions_entity_beg_seq_id", iRow)
                    cObj.setValue(",".join([str(sao.getEntityAlignLength()) for sao in saoL]), "aligned_regions_length", iRow)
                    iRow += 1

            return True
        except Exception as e:
            logger.exception("For %s  %s failing with %s", dataContainer.getName(), catName, str(e))
        return False

        #

    def buildContainerEntityIds(self, dataContainer, catName, **kwargs):
        """Load the input category with rcsb_entity_container_identifiers content.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For example, build:

        loop_
        _rcsb_entity_container_identifiers.entry_id
        _rcsb_entity_container_identifiers.entity_id
        #
        _rcsb_entity_container_identifiers.asym_ids
        _rcsb_entity_container_identifiers.auth_asym_ids
        #
        _rcsb_entity_container_identifiers.nonpolymer_comp_id
        _rcsb_entity_container_identifiers.chem_comp_monomers

        _rcsb_entity_container_identifiers.prd_id
        ...
        """
        logger.debug("Starting catName %s  kwargs %r", catName, kwargs)
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("entity")):
                return False
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            cObj = dataContainer.getObj(catName)
            #
            tObj = dataContainer.getObj("entry")
            entryId = tObj.getValue("id", 0)
            cObj.setValue(entryId, "entry_id", 0)
            #
            tObj = dataContainer.getObj("entity")
            entityIdL = tObj.getAttributeValueList("id")
            seqEntityRefDbD = self.__commonU.getEntitySequenceReferenceCodes(dataContainer)
            #
            entityTypeUniqueIds = self.__commonU.getEntityTypeUniqueIds(dataContainer)
            entityPolymerModMonomerIds = self.__commonU.getPolymerEntityModifiedMonomers(dataContainer)
            #  -------
            eTypeD = self.__commonU.getEntityTypes(dataContainer)
            aObj = dataContainer.getObj("struct_asym")
            if not aObj.hasAttribute("rcsb_entity_type"):
                aObj.appendAttribute("rcsb_entity_type")
            for ii in range(aObj.getRowCount()):
                entityId = aObj.getValue("entity_id", ii)
                aObj.setValue(eTypeD[entityId], "rcsb_entity_type", ii)
            # ---------
            ii = 0
            for entityId in entityIdL:
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(entryId + "_" + entityId, "rcsb_id", ii)
                eType = tObj.getValue("type", ii)
                asymIdL = []
                authAsymIdL = []
                ccMonomerL = []
                ccLigandL = []
                modPolymerMonomerL = entityPolymerModMonomerIds[entityId] if entityId in entityPolymerModMonomerIds else []
                #
                refSeqIdD = {"dbName": [], "dbAccession": [], "provSource": [], "dbIsoform": []}

                asymIdL = entityTypeUniqueIds[eType][entityId]["asymIds"] if eType in entityTypeUniqueIds else []
                authAsymIdL = entityTypeUniqueIds[eType][entityId]["authAsymIds"] if eType in entityTypeUniqueIds else []
                ccMonomerL = entityTypeUniqueIds[eType][entityId]["ccIds"] if eType in entityTypeUniqueIds else []

                if eType in ["polymer", "non-polymer", "branched"] and not asymIdL:
                    logger.warning("%s inconsistent molecular system (no instances) for %r entity %s", entryId, eType, entityId)
                #
                if eType == "polymer":

                    if self.__useSiftsAlign:
                        dbIdL = []
                        for authAsymId in authAsymIdL:
                            dbIdL.extend(self.__ssP.getIdentifiers(entryId, authAsymId, idType="UNPID"))
                        # If SIFTS is defined
                        if dbIdL:
                            for dbId in sorted(set(dbIdL)):
                                refSeqIdD["dbName"].append("UniProt")
                                refSeqIdD["provSource"].append("SIFTS")
                                refSeqIdD["dbAccession"].append(dbId)
                                refSeqIdD["dbIsoform"].append("?")
                        # else fallback to PDB
                        elif entityId in seqEntityRefDbD:
                            for dbD in seqEntityRefDbD[entityId]:
                                refSeqIdD["dbName"].append(dbD["dbName"])
                                refSeqIdD["provSource"].append("PDB")
                                refSeqIdD["dbAccession"].append(dbD["dbAccession"])
                                #
                                if dbD["dbIsoform"]:
                                    refSeqIdD["dbIsoform"].append(dbD["dbIsoform"])
                                else:
                                    refSeqIdD["dbIsoform"].append("?")

                    else:
                        if entityId in seqEntityRefDbD:
                            for dbD in seqEntityRefDbD[entityId]:
                                refSeqIdD["dbName"].append(dbD["dbName"])
                                refSeqIdD["provSource"].append("PDB")
                                refSeqIdD["dbAccession"].append(dbD["dbAccession"])
                                #
                                if dbD["dbIsoform"]:
                                    refSeqIdD["dbIsoform"].append(dbD["dbIsoform"])
                                else:
                                    refSeqIdD["dbIsoform"].append("?")

                #
                # logger.info("refSeqIdD %r %r %r", entryId, entityId, refSeqIdD)

                if asymIdL:
                    cObj.setValue(",".join(sorted(set(asymIdL))).strip(), "asym_ids", ii)
                if authAsymIdL:
                    cObj.setValue(",".join(sorted(set(authAsymIdL))).strip(), "auth_asym_ids", ii)
                if ccMonomerL and eType in ["branched", "polymer"]:
                    cObj.setValue(",".join(sorted(set(ccMonomerL))).strip(), "chem_comp_monomers", ii)
                else:
                    cObj.setValue("?", "chem_comp_monomers", ii)
                #
                if modPolymerMonomerL:
                    cObj.setValue(",".join(sorted(set(modPolymerMonomerL))).strip(), "chem_comp_nstd_monomers", ii)
                else:
                    cObj.setValue("?", "chem_comp_nstd_monomers", ii)
                #
                if eType in ["non-polymer"] and ccMonomerL:
                    cObj.setValue(",".join(sorted(set(ccLigandL))).strip(), "nonpolymer_comp_id", ii)
                else:
                    cObj.setValue("?", "nonpolymer_comp_id", ii)
                #
                if refSeqIdD["dbName"]:
                    cObj.setValue(",".join(refSeqIdD["dbName"]).strip(), "reference_sequence_identifiers_database_name", ii)
                    cObj.setValue(",".join(refSeqIdD["dbAccession"]).strip(), "reference_sequence_identifiers_database_accession", ii)
                    cObj.setValue(",".join(refSeqIdD["provSource"]).strip(), "reference_sequence_identifiers_provenance_source", ii)
                    cObj.setValue(",".join(refSeqIdD["dbIsoform"]).strip(), "reference_sequence_identifiers_database_isoform", ii)
                #

                ii += 1
            _ = self.__addEntityCompIds(dataContainer)
            _ = self.__addBirdEntityIds(dataContainer)

            return True
        except Exception as e:
            logger.exception("For %s  %s failing with %s", dataContainer.getName(), catName, str(e))
        return False

    def __salvageMissingTaxonomy(self, dataContainer, **kwargs):
        """Add missing taxonomy identifiers using scientific name as a surogate.

        Args:
            dataContainer (obj): data container object

        Returns:
            bool: True for success or False otherwise
        """
        #
        ok = False
        try:
            rP = kwargs.get("resourceProvider")
            taxU = rP.getResource("TaxonomyProvider instance") if rP else None
            # "pdbx_gene_src_scientific_name" "pdbx_gene_src_ncbi_taxonomy_id"
            for catName, atSn, atTaxId in [
                ("entity_src_gen", "pdbx_gene_src_scientific_name", "pdbx_gene_src_ncbi_taxonomy_id"),
                ("entity_src_gen", "pdbx_host_org_scientific_name", "pdbx_host_org_ncbi_taxonomy_id"),
                ("entity_src_nat", "pdbx_organism_scientific", "pdbx_ncbi_taxonomy_id"),
                ("entity_src_syn", "organism_scientific", "ncbi_taxonomy_id"),
            ]:
                if dataContainer.exists(catName):
                    sObj = dataContainer.getObj(catName)
                    for ii in range(sObj.getRowCount()):
                        taxId = sObj.getValueOrDefault(atTaxId, ii, defaultValue=None)
                        #
                        if taxId:
                            continue
                        sn = sObj.getValueOrDefault(atSn, ii, defaultValue=None)
                        if sn:
                            taxId = taxU.getTaxId(sn)
                            if taxId:
                                if not sObj.hasAttribute(atTaxId):
                                    sObj.appendAttribute(atTaxId)
                                logger.debug("%s salvaged taxId %r using %r", dataContainer.getName(), taxId, sn)
                                sObj.setValue(str(taxId), atTaxId, ii)
                            else:
                                logger.warning("%s taxId salvage fails for scientific name %s", dataContainer.getName(), sn)
            ok = True
        except Exception as e:
            logger.exception("Failing for %r with %s", dataContainer.getName(), str(e))

        return ok

    def filterSourceOrganismDetails(self, dataContainer, catName, **kwargs):
        """Load new categories rcsb_entity_source_organism and rcsb_entity_host_organism
         and add related source flags in the entity category.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For intance, select relevant source and host organism details from
        primary data categories and load

        Build:
            loop_
            _rcsb_entity_source_organism.entity_id
            _rcsb_entity_source_organism.pdbx_src_id
            _rcsb_entity_source_organism.source_type
            _rcsb_entity_source_organism.scientific_name
            _rcsb_entity_source_organism.common_name
            _rcsb_entity_source_organism.ncbi_taxonomy_id
            _rcsb_entity_source_organism.provenance_source
            _rcsb_entity_source_organism.beg_seq_num
            _rcsb_entity_source_organism.end_seq_num
            _rcsb_entity_source_organism.taxonomy_lineage_id
            _rcsb_entity_source_organism.taxonomy_lineage_name
            _rcsb_entity_source_organism.taxonomy_lineage_depth
            1 1 natural 'Homo sapiens' human 9606  'PDB Primary Data' 1 202 . . .
            # ... abbreviated


            loop_
            _rcsb_entity_host_organism.entity_id
            _rcsb_entity_host_organism.pdbx_src_id
            _rcsb_entity_host_organism.scientific_name
            _rcsb_entity_host_organism.common_name
            _rcsb_entity_host_organism.ncbi_taxonomy_id
            _rcsb_entity_host_organism.provenance_source
            _rcsb_entity_host_organism.beg_seq_num
            _rcsb_entity_host_organism.end_seq_num
            _rcsb_entity_host_organism.taxonomy_lineage_id
            _rcsb_entity_host_organism.taxonomy_lineage_name
            _rcsb_entity_host_organism.taxonomy_lineage_depth
                        1 1 'Escherichia coli' 'E. coli' 562  'PDB Primary Data' 1 102 .  . .
            # ... abbreviated

            And two related items -

            _entity.rcsb_multiple_source_flag
            _entity.rcsb_source_part_count

        """
        #
        hostCatName = "rcsb_entity_host_organism"
        try:
            logger.debug("Starting with  %r %r", dataContainer.getName(), catName)
            if catName == hostCatName:
                logger.debug("Skipping method for %r %r", dataContainer.getName(), catName)
                return True
            #
            # if there is no source information then exit
            if not (dataContainer.exists("entity_src_gen") or dataContainer.exists("entity_src_nat") or dataContainer.exists("pdbx_entity_src_syn")):
                return False
            #
            # Try to supply missing taxIds
            self.__salvageMissingTaxonomy(dataContainer, **kwargs)
            #
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            if not dataContainer.exists(hostCatName):
                dataContainer.append(DataCategory(hostCatName, attributeNameList=self.__dApi.getAttributeNameList(hostCatName)))
            #
            rP = kwargs.get("resourceProvider")
            taxU = rP.getResource("TaxonomyProvider instance") if rP else None
            #
            cObj = dataContainer.getObj(catName)
            hObj = dataContainer.getObj(hostCatName)
            #
            s1Obj = dataContainer.getObj("entity_src_gen")
            atHTupL = [
                ("entity_id", "entity_id"),
                ("pdbx_host_org_scientific_name", "scientific_name"),
                ("pdbx_host_org_common_name", "common_name"),
                ("pdbx_host_org_ncbi_taxonomy_id", "ncbi_taxonomy_id"),
                ("pdbx_src_id", "pdbx_src_id"),
                ("pdbx_beg_seq_num", "beg_seq_num"),
                ("pdbx_end_seq_num", "end_seq_num"),
            ]
            atHSL, atHL = self.__getAttribList(s1Obj, atHTupL)
            #
            at1TupL = [
                ("entity_id", "entity_id"),
                ("pdbx_gene_src_scientific_name", "scientific_name"),
                ("gene_src_common_name", "common_name"),
                ("pdbx_gene_src_ncbi_taxonomy_id", "ncbi_taxonomy_id"),
                ("pdbx_src_id", "pdbx_src_id"),
                ("pdbx_beg_seq_num", "beg_seq_num"),
                ("pdbx_end_seq_num", "end_seq_num"),
                ("pdbx_gene_src_gene", "rcsb_gene_name_value"),
            ]
            at1SL, at1L = self.__getAttribList(s1Obj, at1TupL)
            #
            s2Obj = dataContainer.getObj("entity_src_nat")
            at2TupL = [
                ("entity_id", "entity_id"),
                ("pdbx_organism_scientific", "scientific_name"),
                ("nat_common_name", "common_name"),
                ("pdbx_ncbi_taxonomy_id", "ncbi_taxonomy_id"),
                ("pdbx_src_id", "pdbx_src_id"),
                ("pdbx_beg_seq_num", "beg_seq_num"),
                ("pdbx_end_seq_num", "end_seq_num"),
            ]
            at2SL, at2L = self.__getAttribList(s2Obj, at2TupL)
            #
            s3Obj = dataContainer.getObj("pdbx_entity_src_syn")
            at3TupL = [
                ("entity_id", "entity_id"),
                ("organism_scientific", "scientific_name"),
                ("organism_common_name", "common_name"),
                ("ncbi_taxonomy_id", "ncbi_taxonomy_id"),
                ("pdbx_src_id", "pdbx_src_id"),
                ("beg_seq_num", "beg_seq_num"),
                ("end_seq_num", "end_seq_num"),
            ]
            at3SL, at3L = self.__getAttribList(s3Obj, at3TupL)
            #
            eObj = dataContainer.getObj("entity")
            entityIdL = eObj.getAttributeValueList("id")
            provSource = "PDB Primary Data"
            #
            partCountD = {}
            srcL = []
            hostL = []
            for entityId in entityIdL:
                partCountD[entityId] = 0
                eL = []
                tf = False
                if s1Obj:
                    sType = "genetically engineered"
                    vL = s1Obj.selectValueListWhere(at1SL, entityId, "entity_id")
                    if vL:
                        for v in vL:
                            eL.append((entityId, sType, at1L, v))
                        logger.debug("%r entity %r - %r", sType, entityId, vL)
                        partCountD[entityId] = len(eL)
                        srcL.extend(eL)
                        tf = True
                    #
                    vL = s1Obj.selectValueListWhere(atHSL, entityId, "entity_id")
                    if vL:
                        for v in vL:
                            hostL.append((entityId, sType, atHL, v))
                        logger.debug("%r entity %r - %r", sType, entityId, vL)
                    if tf:
                        continue

                if s2Obj:
                    sType = "natural"
                    vL = s2Obj.selectValueListWhere(at2SL, entityId, "entity_id")
                    if vL:
                        for v in vL:
                            eL.append((entityId, sType, at2L, v))
                        logger.debug("%r entity %r - %r", sType, entityId, vL)
                        partCountD[entityId] = len(eL)
                        srcL.extend(eL)
                        continue

                if s3Obj:
                    sType = "synthetic"
                    vL = s3Obj.selectValueListWhere(at3SL, entityId, "entity_id")
                    if vL:
                        for v in vL:
                            eL.append((entityId, sType, at3L, v))
                        logger.debug("%r entity %r - %r", sType, entityId, vL)
                        partCountD[entityId] = len(eL)
                        srcL.extend(eL)
                        continue

            iRow = 0
            entryTaxIdD = defaultdict(int)
            entityTaxIdD = {}
            for (entityId, sType, atL, tv) in srcL:
                ii = atL.index("ncbi_taxonomy_id") if "ncbi_taxonomy_id" in atL else -1
                if ii > 0 and len(tv[ii].split(",")) > 1:
                    tvL = self.__normalizeCsvToList(dataContainer.getName(), tv)
                    ii = atL.index("pdbx_src_id") if "pdbx_src_id" in atL else -1
                    for jj, row in enumerate(tvL, 1):
                        row[ii] = str(jj)
                    partCountD[entityId] = len(tvL)
                else:
                    tvL = [tv]
                for v in tvL:
                    cObj.setValue(sType, "source_type", iRow)
                    cObj.setValue(provSource, "provenance_source", iRow)
                    for ii, at in enumerate(atL):
                        # add check for missing values here
                        if at in ["rcsb_gene_name_value"] and v[ii] and v[ii] not in [".", "?"]:
                            tgL = v[ii].split(",")
                            fgL = self.__filterCaseDuplicates(tgL)
                            cObj.setValue(";".join(fgL), at, iRow)
                            cObj.setValue(";".join([provSource for jj in range(len(tgL))]), "rcsb_gene_name_provenance_source", iRow)
                        else:
                            cObj.setValue(v[ii], at, iRow)
                        # if at == 'ncbi_taxonomy_id' and v[ii] and v[ii] not in ['.', '?'] and v[ii].isdigit():
                        if at == "ncbi_taxonomy_id" and v[ii] and v[ii] not in [".", "?"]:
                            taxId = int(self.__reNonDigit.sub("", v[ii]))
                            taxId = taxU.getMergedTaxId(taxId)
                            cObj.setValue(str(taxId), "ncbi_taxonomy_id", iRow)
                            entryTaxIdD[taxId] += 1
                            entityTaxIdD.setdefault(entityId, set()).add(taxId)
                            #
                            sn = taxU.getScientificName(taxId)
                            if sn:
                                cObj.setValue(sn, "ncbi_scientific_name", iRow)
                            #
                            psn = taxU.getParentScientificName(taxId)
                            if psn:
                                cObj.setValue(psn, "ncbi_parent_scientific_name", iRow)
                            #
                            cnL = taxU.getCommonNames(taxId)
                            if cnL:
                                fcnL = self.__filterCaseDuplicates(cnL)
                                cObj.setValue(";".join(list(OrderedDict.fromkeys(fcnL))), "ncbi_common_names", iRow)
                            # Add lineage -
                            linL = taxU.getLineageWithNames(taxId)
                            if linL is not None:
                                cObj.setValue(";".join([str(tup[0]) for tup in OrderedDict.fromkeys(linL)]), "taxonomy_lineage_depth", iRow)
                                cObj.setValue(";".join([str(tup[1]) for tup in OrderedDict.fromkeys(linL)]), "taxonomy_lineage_id", iRow)
                                cObj.setValue(";".join([str(tup[2]) for tup in OrderedDict.fromkeys(linL)]), "taxonomy_lineage_name", iRow)
                            else:
                                logger.warning("%s taxId %r lineage %r", dataContainer.getName(), taxId, linL)

                    logger.debug("%r entity %r - UPDATED %r %r", sType, entityId, atL, v)
                    iRow += 1
            #
            iRow = 0
            for (entityId, sType, atL, tv) in hostL:
                ii = atL.index("ncbi_taxonomy_id") if "ncbi_taxonomy_id" in atL else -1
                if ii > 0 and len(tv[ii].split(",")) > 1:
                    tvL = self.__normalizeCsvToList(dataContainer.getName(), tv)
                    ii = atL.index("pdbx_src_id") if "pdbx_src_id" in atL else -1
                    for jj, row in enumerate(tvL, 1):
                        row[ii] = str(jj)
                    # partCountD[entityId] = len(tvL)
                else:
                    tvL = [tv]
                for v in tvL:
                    hObj.setValue(provSource, "provenance_source", iRow)
                    for ii, at in enumerate(atL):
                        hObj.setValue(v[ii], at, iRow)
                        #  if at == 'ncbi_taxonomy_id' and v[ii] and v[ii] not in ['.', '?'] and v[ii].isdigit():
                        if at == "ncbi_taxonomy_id" and v[ii] and v[ii] not in [".", "?"]:
                            taxId = int(self.__reNonDigit.sub("", v[ii]))
                            taxId = taxU.getMergedTaxId(taxId)
                            hObj.setValue(str(taxId), "ncbi_taxonomy_id", iRow)
                            sn = taxU.getScientificName(taxId)
                            if sn:
                                hObj.setValue(sn, "ncbi_scientific_name", iRow)
                            #
                            psn = taxU.getParentScientificName(taxId)
                            if psn:
                                hObj.setValue(psn, "ncbi_parent_scientific_name", iRow)
                            #
                            cnL = taxU.getCommonNames(taxId)
                            if cnL:
                                hObj.setValue(";".join(sorted(set(cnL))), "ncbi_common_names", iRow)
                            # Add lineage -
                            linL = taxU.getLineageWithNames(taxId)
                            if linL is not None:
                                hObj.setValue(";".join([str(tup[0]) for tup in OrderedDict.fromkeys(linL)]), "taxonomy_lineage_depth", iRow)
                                hObj.setValue(";".join([str(tup[1]) for tup in OrderedDict.fromkeys(linL)]), "taxonomy_lineage_id", iRow)
                                hObj.setValue(";".join([str(tup[2]) for tup in OrderedDict.fromkeys(linL)]), "taxonomy_lineage_name", iRow)
                            else:
                                logger.warning("%s taxId %r lineage %r", dataContainer.getName(), taxId, linL)
                    logger.debug("%r entity %r - UPDATED %r %r", sType, entityId, atL, v)
                    iRow += 1
            # -------------------------------------------------------------------------
            # -------------------------------------------------------------------------
            # Update entity attributes
            #    _entity.rcsb_multiple_source_flag
            #    _entity.rcsb_source_part_count
            for atName in ["rcsb_source_part_count", "rcsb_multiple_source_flag", "rcsb_source_taxonomy_count"]:
                if not eObj.hasAttribute(atName):
                    eObj.appendAttribute(atName)
            #
            taxCountTotal = 0
            for ii in range(eObj.getRowCount()):
                entityId = eObj.getValue("id", ii)
                cFlag = "Y" if partCountD[entityId] > 1 else "N"
                eObj.setValue(partCountD[entityId], "rcsb_source_part_count", ii)
                eObj.setValue(cFlag, "rcsb_multiple_source_flag", ii)
                taxCount = len(entityTaxIdD[entityId]) if entityId in entityTaxIdD else 0
                eObj.setValue(taxCount, "rcsb_source_taxonomy_count", ii)
                taxCountTotal += taxCount

            logger.debug("Entities with taxonomies %d entry total taxonomy count is %d", len(entryTaxIdD), taxCountTotal)
            if dataContainer.exists("rcsb_entry_info"):
                eiObj = dataContainer.getObj("rcsb_entry_info")
                eiObj.setValue(taxCountTotal, "polymer_entity_taxonomy_count", 0)
            #
            return True
        except Exception as e:
            logger.exception("In %s for %s failing with %s", dataContainer.getName(), catName, str(e))
        return False

    def __addEntityCompIds(self, dataContainer):
        """Add entity_id and BIRD codes to selected categories.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance

        Returns:
            bool: True for success or False otherwise

        For example, update/add identifiers:

            loop_
            _pdbx_entity_nonpoly.entity_id
            _pdbx_entity_nonpoly.name
            _pdbx_entity_nonpoly.comp_id

        """
        try:
            eD = {}
            if dataContainer.exists("pdbx_entity_nonpoly"):
                npObj = dataContainer.getObj("pdbx_entity_nonpoly")
                for ii in range(npObj.getRowCount()):
                    entityId = npObj.getValue("entity_id", ii)
                    compId = npObj.getValue("comp_id", ii)
                    eD[entityId] = compId

            if dataContainer.exists("rcsb_entity_container_identifiers"):
                pObj = dataContainer.getObj("rcsb_entity_container_identifiers")
                if not pObj.hasAttribute("nonpolymer_comp_id"):
                    pObj.appendAttribute("nonpolymer_comp_id")
                for ii in range(pObj.getRowCount()):
                    entityId = pObj.getValue("entity_id", ii)
                    compId = eD[entityId] if entityId in eD else "?"
                    pObj.setValue(compId, "nonpolymer_comp_id", ii)
            #
            return True
        except Exception as e:
            logger.exception("%s  failing with %s", dataContainer.getName(), str(e))
        return False

    def __addBirdEntityIds(self, dataContainer):
        """Add entity_id and BIRD codes to selected categories.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance

        Returns:
            bool: True for success or False otherwise

        For example, update/add identifiers:

            loop_
            _pdbx_molecule.instance_id
            _pdbx_molecule.prd_id
            _pdbx_molecule.asym_id

            loop_
            _pdbx_entity_nonpoly.entity_id
            _pdbx_entity_nonpoly.name
            _pdbx_entity_nonpoly.comp_id

        with:

        _pdbx_molecule.rcsb_entity_id
        _pdbx_molecule.rcsb_comp_id

        _pdbx_entity_nonpoly.rcsb_prd_id
        _entity_poly.rcsb_prd_id

        _rcsb_entity_containter_identifiers.prd_id

        """
        catName = "pdbx_molecule"
        atName = "rcsb_entity_id"
        try:

            logger.debug("Starting catName %s atName %s", catName, atName)
            if catName != "pdbx_molecule" and "atName" != "rcsb_entity_id":
                return False
            #
            if not (dataContainer.exists(catName) and dataContainer.exists("struct_asym")):
                return False
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            if not cObj.hasAttribute("rcsb_comp_id"):
                cObj.appendAttribute("rcsb_comp_id")
            #
            aD = {}
            aObj = dataContainer.getObj("struct_asym")
            for ii in range(aObj.getRowCount()):
                entityId = aObj.getValue("entity_id", ii)
                asymId = aObj.getValue("id", ii)
                aD[asymId] = entityId
            #
            eD = {}
            if dataContainer.exists("pdbx_entity_nonpoly"):
                npObj = dataContainer.getObj("pdbx_entity_nonpoly")
                for ii in range(npObj.getRowCount()):
                    entityId = npObj.getValue("entity_id", ii)
                    compId = npObj.getValue("comp_id", ii)
                    eD[entityId] = compId
            #
            #
            prdD = {}
            for ii in range(cObj.getRowCount()):
                asymId = cObj.getValue("asym_id", ii)
                prdId = cObj.getValue("prd_id", ii)
                if asymId in aD:
                    entityId = aD[asymId]
                    prdD[entityId] = prdId
                    cObj.setValue(entityId, atName, ii)
                    compId = eD[entityId] if entityId in eD else "."
                    cObj.setValue(compId, "rcsb_comp_id", ii)
                else:
                    logger.error("%s missing entityId for asymId %s", dataContainer.getName(), asymId)
            #
            if prdD and dataContainer.exists("pdbx_entity_nonpoly"):
                npObj = dataContainer.getObj("pdbx_entity_nonpoly")
                if not npObj.hasAttribute("rcsb_prd_id"):
                    npObj.appendAttribute("rcsb_prd_id")
                for ii in range(npObj.getRowCount()):
                    entityId = npObj.getValue("entity_id", ii)
                    prdId = prdD[entityId] if entityId in prdD else "."
                    npObj.setValue(prdId, "rcsb_prd_id", ii)
            #
            if prdD and dataContainer.exists("entity_poly"):
                pObj = dataContainer.getObj("entity_poly")
                if not pObj.hasAttribute("rcsb_prd_id"):
                    pObj.appendAttribute("rcsb_prd_id")
                for ii in range(pObj.getRowCount()):
                    entityId = pObj.getValue("entity_id", ii)
                    prdId = prdD[entityId] if entityId in prdD else "."
                    pObj.setValue(prdId, "rcsb_prd_id", ii)
            #
            #
            if prdD and dataContainer.exists("rcsb_entity_container_identifiers"):
                pObj = dataContainer.getObj("rcsb_entity_container_identifiers")
                if not pObj.hasAttribute("prd_id"):
                    pObj.appendAttribute("prd_id")
                if not pObj.hasAttribute("nonpolymer_comp_id"):
                    pObj.appendAttribute("nonpolymer_comp_id")
                for ii in range(pObj.getRowCount()):
                    entityId = pObj.getValue("entity_id", ii)
                    prdId = prdD[entityId] if entityId in prdD else "?"
                    pObj.setValue(prdId, "prd_id", ii)
                    compId = eD[entityId] if entityId in eD else "?"
                    pObj.setValue(compId, "nonpolymer_comp_id", ii)

            #
            #
            return True
        except Exception as e:
            logger.exception("%s %s %s failing with %s", dataContainer.getName(), catName, atName, str(e))
        return False

    def addStructRefSeqEntityIds(self, dataContainer, catName, **kwargs):
        """Add entity ids in categories struct_ref_seq and struct_ref_seq_dir instances.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name
            atName (str): Attribute name

        Returns:
            bool: True for success or False otherwise

        """
        try:
            logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
            if catName != "struct_ref_seq":
                return False
            #
            if not (dataContainer.exists(catName) and dataContainer.exists("struct_ref")):
                return False
            #
            atName = "rcsb_entity_id"
            srsObj = dataContainer.getObj(catName)
            if not srsObj.hasAttribute(atName):
                # srsObj.appendAttribute(atName)
                srsObj.appendAttributeExtendRows(atName, defaultValue="?")
            #
            srObj = dataContainer.getObj("struct_ref")
            #
            srsdObj = None
            if dataContainer.exists("struct_ref_seq_dif"):
                srsdObj = dataContainer.getObj("struct_ref_seq_dif")
                if not srsdObj.hasAttribute(atName):
                    # srsdObj.appendAttribute(atName)
                    srsdObj.appendAttributeExtendRows(atName, defaultValue="?")

            for ii in range(srObj.getRowCount()):
                entityId = srObj.getValue("entity_id", ii)
                refId = srObj.getValue("id", ii)
                #
                # Get indices for the target refId.
                iRowL = srsObj.selectIndices(refId, "ref_id")
                for iRow in iRowL:
                    srsObj.setValue(entityId, "rcsb_entity_id", iRow)
                    alignId = srsObj.getValue("align_id", iRow)
                    #
                    if srsdObj:
                        jRowL = srsdObj.selectIndices(alignId, "align_id")
                        for jRow in jRowL:
                            srsdObj.setValue(entityId, "rcsb_entity_id", jRow)

            return True
        except Exception as e:
            logger.exception("%s %s failing with %s", dataContainer.getName(), catName, str(e))
        return False

    def buildEntityPolyInfo(self, dataContainer, catName, **kwargs):
        """Build category rcsb_entity_poly_info and supplement category entity_poly.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For example, :
            loop_
            _rcsb_entity_poly_info.ordinal_id
            _rcsb_entity_poly_info.entry_id
            _rcsb_entity_poly_info.entity_id
            _rcsb_entity_poly_info.comp_id
            _rcsb_entity_poly_info.is_modified
            _rcsb_entity_poly_info.is_heterogeneous
            _rcsb_entity_poly_info.entity_sequence_length
            _rcsb_entity_poly_info.chem_comp_count

            1 1ABC 1 1 MSE Y N 100 1
            2 1ABC 1 2 TRP N N 100 4
            # ... abbreviated ...

        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            # Exit if source categories are missing
            if not (dataContainer.exists("entity_poly") and dataContainer.exists("entry")):
                return False
            #
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            cObj = dataContainer.getObj(catName)
            #
            cN = "rcsb_entity_monomer_container_identifiers"
            if not dataContainer.exists(cN):
                dataContainer.append(DataCategory(cN, attributeNameList=self.__dApi.getAttributeNameList(cN)))
            idObj = dataContainer.getObj(cN)

            #
            epObj = dataContainer.getObj("entity_poly")
            for atName in [
                "rcsb_mutation_count",
                "rcsb_artifact_monomer_count",
                "rcsb_conflict_count",
                "rcsb_insertion_count",
                "rcsb_deletion_count",
                "rcsb_sample_sequence_length",
                "rcsb_non_std_monomer_count",
                "rcsb_non_std_monomers",
            ]:
                if not epObj.hasAttribute(atName):
                    epObj.appendAttribute(atName)

            #
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            # ------- --------- ------- --------- ------- --------- ------- --------- ------- ---------
            seqDifD = self.__commonU.getEntitySequenceFeatureCounts(dataContainer)
            eD = self.__commonU.getPolymerEntityMonomerCounts(dataContainer)
            elD = self.__commonU.getPolymerEntityLengthsEnumerated(dataContainer)
            modMonD = self.__commonU.getPolymerEntityModifiedMonomers(dataContainer)
            #
            monDict3 = self.__commonU.monDict3
            ii = 0
            for entityId, cD in eD.items():
                for compId, chemCompCount in cD.items():
                    modFlag = "N" if compId in monDict3 else "Y"
                    cObj.setValue(ii + 1, "ordinal_id", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(compId, "comp_id", ii)
                    cObj.setValue(chemCompCount, "chem_comp_count", ii)
                    cObj.setValue(round(float(chemCompCount) / float(elD[entityId]), 5), "chem_comp_polymer_fraction", ii)
                    cObj.setValue(modFlag, "is_modified", ii)
                    #
                    idObj.setValue(ii + 1, "ordinal_id", ii)
                    idObj.setValue(entryId, "entry_id", ii)
                    idObj.setValue(entityId, "entity_id", ii)
                    idObj.setValue(compId, "comp_id", ii)
                    ii += 1
            #
            for ii in range(epObj.getRowCount()):
                entityId = epObj.getValue("entity_id", ii)
                mutations = seqDifD[entityId]["mutation"] if entityId in seqDifD else 0
                conflicts = seqDifD[entityId]["conflict"] if entityId in seqDifD else 0
                insertions = seqDifD[entityId]["insertion"] if entityId in seqDifD else 0
                deletions = seqDifD[entityId]["deletion"] if entityId in seqDifD else 0
                artifacts = seqDifD[entityId]["artifact"] if entityId in seqDifD else 0
                seqLen = elD[entityId] if entityId in elD else None
                epObj.setValue(mutations, "rcsb_mutation_count", ii)
                epObj.setValue(artifacts, "rcsb_artifact_monomer_count", ii)
                epObj.setValue(conflicts, "rcsb_conflict_count", ii)
                epObj.setValue(insertions, "rcsb_insertion_count", ii)
                epObj.setValue(deletions, "rcsb_deletion_count", ii)
                if seqLen is not None:
                    epObj.setValue(seqLen, "rcsb_sample_sequence_length", ii)
                #
                numMod = len(modMonD[entityId])
                uModL = ",".join(modMonD[entityId]) if numMod else "?"
                epObj.setValue(numMod, "rcsb_non_std_monomer_count", ii)
                epObj.setValue(uModL, "rcsb_non_std_monomers", ii)

            return True
        except Exception as e:
            logger.exception("%s %s failing with %s", dataContainer.getName(), catName, str(e))
        return False

    def addBranchedEntityComponentCounts(self, dataContainer, catName, atName, **kwargs):
        """Add total number branched components in the branched entity.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): target category name
            atName (str): target attribute name

        Returns:
            bool: True for success or False otherwise
        """
        try:
            logger.debug("Starting with %r %r %r %r", dataContainer.getName(), catName, atName, kwargs)
            if not (dataContainer.exists("pdbx_entity_branch") and dataContainer.exists("pdbx_entity_branch_list")):
                return False
            #
            ebObj = dataContainer.getObj("pdbx_entity_branch")
            eblObj = dataContainer.getObj("pdbx_entity_branch_list")
            #
            if not ebObj.hasAttribute(atName):
                ebObj.appendAttribute(atName)

            for ii in range(ebObj.getRowCount()):
                entityId = ebObj.getValue("entity_id", ii)
                tL = eblObj.selectValuesWhere("entity_id", entityId, "entity_id")
                ebObj.setValue(len(tL), atName, ii)

            return True
        except Exception as e:
            logger.exception("For %s %s failing with %s", catName, atName, str(e))
        return False

    def addEntityMisc(self, dataContainer, catName, atName, **kwargs):
        """Add consolidated enzyme classification macromolecule names to the entity category.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For instance, add:

        _entity.rcsb_macromolecular_names_combined  <<< Dictionary target

        _entity.rcsb_ec_lineage_name
        _entity.rcsb_ec_lineage_id
        _entity.rcsb_ec_lineage_depth

        """
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("entity")):
                return False
            #
            if catName == "entity" and atName in ["rcsb_ec_lineage_name", "rcsb_ec_lineage_id", "rcsb_ec_lineage_depth"]:
                return True
            #
            eObj = dataContainer.getObj("entity")
            atList = [
                "rcsb_ec_lineage_depth",
                "rcsb_ec_lineage_id",
                "rcsb_ec_lineage_name",
                "rcsb_macromolecular_names_combined_name",
                "rcsb_macromolecular_names_combined_provenance_source",
                "rcsb_macromolecular_names_combined_provenance_code",
                "rcsb_enzyme_class_combined_ec",
                "rcsb_enzyme_class_combined_provenance_source",
                "rcsb_enzyme_class_combined_depth",
            ]
            for at in atList:
                if not eObj.hasAttribute(at):
                    eObj.appendAttribute(at)

            hasEc = eObj.hasAttribute("pdbx_ec")
            #
            rP = kwargs.get("resourceProvider")
            ecU = None
            if hasEc:
                ecU = rP.getResource("EnzymeProvider instance") if rP else None
            #
            ncObj = None
            if dataContainer.exists("entity_name_com"):
                ncObj = dataContainer.getObj("entity_name_com")
            # get an BIRD assigned names -
            birdFeatureD = self.__getBirdFeatures(dataContainer)
            birdNameD = {}
            for (entityId, _, _, filteredFeature), fName in birdFeatureD.items():
                if filteredFeature == "BIRD_MOLECULAR_NAME" and fName:
                    birdNameD.setdefault(entityId, []).append(fName)

            for ii in range(eObj.getRowCount()):
                entityId = eObj.getValue("id", ii)
                entityType = eObj.getValue("type", ii)
                #
                eObj.setValue("?", "rcsb_ec_lineage_depth", ii)
                eObj.setValue("?", "rcsb_ec_lineage_id", ii)
                eObj.setValue("?", "rcsb_ec_lineage_name", ii)
                eObj.setValue("?", "rcsb_macromolecular_names_combined_name", ii)
                eObj.setValue("?", "rcsb_macromolecular_names_combined_provenance_source", ii)
                eObj.setValue("?", "rcsb_macromolecular_names_combined_provenance_code", ii)
                eObj.setValue("?", "rcsb_enzyme_class_combined_ec", ii)
                eObj.setValue("?", "rcsb_enzyme_class_combined_provenance_source", ii)
                eObj.setValue("?", "rcsb_enzyme_class_combined_depth", ii)
                #
                if entityType not in ["polymer", "branched"]:
                    continue
                #
                # --------------------------------------------------------------------------
                #  PDB assigned names
                nameL = []
                sourceL = []
                provCodeL = []
                nmL = str(eObj.getValue("pdbx_description", ii)).split(",")
                nmL = self.__cleanupCsv(nmL)
                nmL = [tV.strip() for tV in nmL if len(tV) > 3]
                nmLookUpD = {}
                for nm in nmL:
                    if nm.upper() in nmLookUpD:
                        continue
                    nmLookUpD[nm.upper()] = True
                    nameL.append(nm)
                    sourceL.append("PDB Preferred Name")
                    provCodeL.append("ECO:0000304")
                #
                # PDB common names/synonyms
                logger.debug("%s ii %d nmL %r", dataContainer.getName(), ii, nmL)
                #
                if ncObj:
                    ncL = []
                    tL = ncObj.selectValuesWhere("name", entityId, "entity_id")
                    logger.debug("%s ii %d tL %r", dataContainer.getName(), ii, tL)
                    for tV in tL:
                        tff = tV.split(",")
                        ncL.extend(tff)
                    ncL = self.__cleanupCsv(ncL)
                    ncL = [tV.strip() for tV in ncL if len(tV) > 3]
                    for nc in ncL:
                        if nc.upper() in nmLookUpD:
                            continue
                        nmLookUpD[nc.upper()] = True
                        nameL.append(nc)
                        sourceL.append("PDB Synonym")
                        provCodeL.append("ECO:0000303")
                    logger.debug("%s ii %d ncL %r", dataContainer.getName(), ii, ncL)
                #
                if entityId in birdNameD:
                    for nm in birdNameD[entityId]:
                        if nm.upper() in nmLookUpD:
                            continue
                        nmLookUpD[nm.upper()] = True
                        nameL.append(nm)
                        sourceL.append("PDB BIRD Name")
                        provCodeL.append("ECO:0000303")
                #
                if nameL:
                    eObj.setValue(";".join(nameL), "rcsb_macromolecular_names_combined_name", ii)
                    eObj.setValue(";".join(sourceL), "rcsb_macromolecular_names_combined_provenance_source", ii)
                    eObj.setValue(";".join(provCodeL), "rcsb_macromolecular_names_combined_provenance_code", ii)

                # --------------------------------------------------------------------------
                linL = []
                ecIdUpdL = []
                ecDepthUpdL = []
                ecV = eObj.getValueOrDefault("pdbx_ec", ii, defaultValue=None)
                if ecV:
                    ecIdL = ecV.split(",") if ecV else []
                    if ecIdL:
                        ecIdL = list(OrderedDict.fromkeys(ecIdL))
                        for tId in ecIdL:
                            ecId = ecU.normalize(tId)
                            if not ecU.exists(ecId):
                                continue
                            # tL = ecU.getLineage(ecId) if ecId and len(ecId) > 7 else None
                            tL = ecU.getLineage(ecId)
                            if tL:
                                linL.extend(tL)
                                ecIdUpdL.append(ecId)
                                ecDepthUpdL.append(str(ecId.count(".") + 1))

                    if linL:
                        eObj.setValue(";".join([str(tup[0]) for tup in linL]), "rcsb_ec_lineage_depth", ii)
                        eObj.setValue(";".join([str(tup[1]) for tup in linL]), "rcsb_ec_lineage_id", ii)
                        eObj.setValue(";".join([tup[2] for tup in linL]), "rcsb_ec_lineage_name", ii)
                    if ecIdUpdL:
                        eObj.setValue(",".join(ecIdUpdL), "pdbx_ec", ii)
                        eObj.setValue(";".join(ecIdUpdL), "rcsb_enzyme_class_combined_ec", ii)
                        eObj.setValue(";".join(ecDepthUpdL), "rcsb_enzyme_class_combined_depth", ii)
                        eObj.setValue(";".join(["PDB Primary Data" for _ in ecIdUpdL]), "rcsb_enzyme_class_combined_provenance_source", ii)
                    else:
                        eObj.setValue("?", "pdbx_ec", ii)
                        eObj.setValue("?", "rcsb_enzyme_class_combined_ec", ii)
                        eObj.setValue("?", "rcsb_enzyme_class_combined_provenance_source", ii)
                        eObj.setValue("?", "rcsb_enzyme_class_combined_depth", ii)
                        if ecIdL:
                            logger.debug("%s obsolete or undefined EC class detected %r", dataContainer.getName(), ecV)
            return True
        except Exception as e:
            logger.exception("For %s %s failing with %s", catName, atName, str(e))
        return False

    def __cleanupCsv(self, tL):
        """Ad hoc cleanup function for comma separated lists with embedded punctuation"""
        rL = []
        try:
            key_paths = functools.cmp_to_key(cmpElements)
            groups = [",".join(grp) for key, grp in itertools.groupby(tL, key_paths)]
            rL = list(OrderedDict.fromkeys(groups))
        except Exception:
            pass
        return rL

    def __filterCaseDuplicates(self, inpSL):
        oL = []
        try:
            lookUpD = {}
            for inpS in inpSL:
                if inpS.upper() in lookUpD:
                    continue
                lookUpD[inpS.upper()] = True
                oL.append(inpS)
        except Exception:
            return inpSL

        return oL

    def __getAttribList(self, sObj, atTupL):
        atL = []
        atSL = []
        if sObj:
            for (atS, at) in atTupL:
                if sObj.hasAttribute(atS):
                    atL.append(at)
                    atSL.append(atS)
        return atSL, atL

    def __normalizeCsvToList(self, entryId, colL, separator=","):
        """Normalize a row containing some character delimited fields.

        Expand list of uneven lists into unifornm list of lists.
        Only two list lengths are logically supported: 1 and second
        maximum length.

        returns: list of expanded rows or the original input.

        """
        tcL = []
        countL = []
        for col in colL:
            cL = [t.strip() for t in col.split(separator)]
            tcL.append(cL)
            countL.append(len(cL))
        #
        tL = list(OrderedDict.fromkeys(countL))
        if len(tL) == 1 and tL[0] == 1:
            return [colL]
        #
        orig = False
        # Report pathological cases ...
        if orig:
            if (len(tL) > 2) or (tL[0] != 1 and len(tL) == 2):
                logger.error("%s integrated source data inconsistent %r colL", entryId, colL)
                return [colL]
        #
        # Expand the columns with uniform length
        #

        if orig:
            icL = []
            maxL = tL[1]
            for tc in tcL:
                if len(tc) == 1:
                    tc = tc * maxL
                icL.append(tc)
        else:
            icL = []
            maxL = tL[1]
            for tc in tcL:
                if len(tc) == 1:
                    tc = tc * maxL
                if len(tc) < maxL:
                    for _ in range(maxL - len(tc)):
                        tc.append("?")
                icL.append(tc[:maxL])

        #
        logger.debug("%s icL %r", entryId, icL)
        # Convert back to a row list
        #
        iRow = 0
        rL = []
        for iRow in range(maxL):
            row = []
            for ic in icL:
                row.append(ic[iRow])
            rL.append(row)

        return rL

    def __stripWhiteSpace(self, val):
        """Remove all white space from the input value."""
        if val is None:
            return val
        return self.__wsPattern.sub("", val)

    #
    def __getTargetComponentFeatures(self, dataContainer):
        """Get targeted components-

        Args:
            dataContainer ([type]): [description]

        Returns:
            dict: {(entityId, compId, "SUBJECT_OF_INVESTIGATION")} = True

        """
        tcD = {}
        try:
            if not dataContainer.exists("pdbx_entity_nonpoly"):
                return tcD
            ccTargets = self.__commonU.getTargetComponents(dataContainer)
            if dataContainer.exists("pdbx_entity_nonpoly"):
                npObj = dataContainer.getObj("pdbx_entity_nonpoly")
                for ii in range(npObj.getRowCount()):
                    entityId = npObj.getValue("entity_id", ii)
                    compId = npObj.getValue("comp_id", ii)
                    if compId in ccTargets:
                        tcD[(entityId, compId, "SUBJECT_OF_INVESTIGATION")] = True
        except Exception as e:
            logger.exception("Failing for %s with %s", dataContainer.getName(), str(e))
        return tcD

    #
    def __getBirdFeatures(self, dataContainer):
        """Get type and class Bird annotations -

        Args:
            dataContainer ([type]): [description]

        Returns:
            dict: {(entityId, compId, "BIRD MOLECULE CLASS|BIRD MOLECULE CLASS")} = <class name or type name>


        Example:
            _pdbx_molecule_features.prd_id    PRD_002214
            _pdbx_molecule_features.name      'N-[(5-METHYLISOXAZOL-3-Y ...'
            _pdbx_molecule_features.type      Peptide-like
            _pdbx_molecule_features.class     Inhibitor
            _pdbx_molecule_features.details   ?
            #
            _pdbx_molecule.instance_id      1
            _pdbx_molecule.prd_id           PRD_002214
            _pdbx_molecule.asym_id          B
            _pdbx_molecule.rcsb_entity_id   2
            _pdbx_molecule.rcsb_comp_id     .
            #

        """
        bD = {}
        if not (dataContainer.exists("pdbx_molecule_features") and dataContainer.exists("pdbx_molecule")):
            return bD

        try:
            asymIdEntityIdD = self.__commonU.getInstanceEntityMap(dataContainer)
            eD = {}
            if dataContainer.exists("pdbx_entity_nonpoly"):
                npObj = dataContainer.getObj("pdbx_entity_nonpoly")
                for ii in range(npObj.getRowCount()):
                    entityId = npObj.getValue("entity_id", ii)
                    compId = npObj.getValue("comp_id", ii)
                    eD[entityId] = compId

            pfObj = dataContainer.getObj("pdbx_molecule_features")
            pfD = {}
            for ii in range(pfObj.getRowCount()):
                prdId = pfObj.getValue("prd_id", ii)
                prdType = pfObj.getValueOrDefault("class", ii, defaultValue=None)
                prdClass = pfObj.getValueOrDefault("type", ii, defaultValue=None)
                prdName = pfObj.getValueOrDefault("name", ii, defaultValue=None)
                pfD[prdId] = (prdType, prdClass, prdName)

            pObj = dataContainer.getObj("pdbx_molecule")
            bD = {}
            for ii in range(pObj.getRowCount()):
                asymId = pObj.getValue("asym_id", ii)
                prdId = pObj.getValue("prd_id", ii)
                entityId = asymIdEntityIdD[asymId]
                compId = eD[entityId] if entityId in eD else None
                if pfD[prdId][0]:
                    bD[(entityId, compId, prdId, "BIRD_MOLECULE_TYPE")] = pfD[prdId][0]
                if pfD[prdId][1]:
                    bD[(entityId, compId, prdId, "BIRD_MOLECULE_CLASS")] = pfD[prdId][1]
                if pfD[prdId][2]:
                    bD[(entityId, compId, prdId, "BIRD_MOLECULE_NAME")] = pfD[prdId][2]

        except Exception as e:
            logger.exception("Failing for %s with %s", dataContainer.getName(), str(e))
        return bD

    def __getEntityFeatureTypes(self, eType):
        eTupL = []
        if eType == "polymer":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_polymer_entity_feature_summary", "type")
        elif eType == "non-polymer":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_nonpolymer_entity_feature_summary", "type")
        elif eType == "branched":
            eTupL = self.__dApi.getEnumListWithDetail("rcsb_branched_entity_feature_summary", "type")
        #
        fTypeL = sorted([tup[0] for tup in eTupL])
        return fTypeL

    def buildEntityFeatureSummary(self, dataContainer, catName, **kwargs):
        """Build category rcsb_entity_feature_summary (UPDATED)

        Example:

            loop_
            _rcsb_entity_feature_summary.ordinal
            _rcsb_entity_feature_summary.entry_id
            _rcsb_entity_feature_summary.entity_id
            _rcsb_entity_feature_summary.type
            _rcsb_entity_feature_summary.count
            _rcsb_entity_feature_summary.coverage
            # ...
        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_feature_summary":
                return False
            if not dataContainer.exists("rcsb_entity_feature") and not dataContainer.exists("entry"):
                return False

            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            sObj = dataContainer.getObj(catName)
            fObj = dataContainer.getObj("rcsb_entity_feature")
            #
            entityPolymerLengthD = self.__commonU.getPolymerEntityLengthsEnumerated(dataContainer)
            eTypeD = self.__commonU.getEntityTypes(dataContainer)

            fCountD = OrderedDict()
            fMonomerCountD = OrderedDict()
            for ii in range(fObj.getRowCount()):
                entityId = fObj.getValue("entity_id", ii)
                #
                fType = fObj.getValue("type", ii)
                fId = fObj.getValue("feature_id", ii)
                fCountD.setdefault(entityId, {}).setdefault(fType, set()).add(fId)

                #
                tbegS = fObj.getValueOrDefault("feature_positions_beg_seq_id", ii, defaultValue=None)
                tendS = fObj.getValueOrDefault("feature_positions_end_seq_id", ii, defaultValue=None)
                if fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS is not None and fObj.hasAttribute("feature_positions_end_seq_id") and tendS is not None:
                    begSeqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    endSeqIdL = str(fObj.getValue("feature_positions_end_seq_id", ii)).split(";")
                    monCount = 0
                    for begSeqId, endSeqId in zip(begSeqIdL, endSeqIdL):
                        monCount += abs(int(endSeqId) - int(begSeqId) + 1)
                    fMonomerCountD.setdefault(entityId, {}).setdefault(fType, []).append(monCount)
                elif fObj.hasAttribute("feature_positions_beg_seq_id") and tbegS:
                    seqIdL = str(fObj.getValue("feature_positions_beg_seq_id", ii)).split(";")
                    fMonomerCountD.setdefault(entityId, {}).setdefault(fType, []).append(len(seqIdL))
            #
            ii = 0
            for entityId, eType in eTypeD.items():
                fTypes = self.__getEntityFeatureTypes(eType)
                for fType in fTypes:
                    sObj.setValue(ii + 1, "ordinal", ii)
                    sObj.setValue(entryId, "entry_id", ii)
                    sObj.setValue(entityId, "entity_id", ii)
                    sObj.setValue(fType, "type", ii)

                    minL = maxL = None
                    fracC = 0.0
                    fCount = 0
                    if entityId in fCountD and fType in fCountD[entityId]:
                        fCount = len(fCountD[entityId][fType])

                    if entityId in fMonomerCountD and fType in fMonomerCountD[entityId] and entityId in entityPolymerLengthD:
                        fracC = float(sum(fMonomerCountD[entityId][fType])) / float(entityPolymerLengthD[entityId])
                    #
                    if fType in ["artifact"] and entityId in fMonomerCountD and fType in fMonomerCountD[entityId]:
                        minL = min(fMonomerCountD[entityId][fType])
                        maxL = max(fMonomerCountD[entityId][fType])

                    sObj.setValue(round(fracC, 5), "coverage", ii)
                    sObj.setValue(fCount, "count", ii)
                    if minL is not None:
                        sObj.setValue(minL, "minimum_length", ii)
                        sObj.setValue(maxL, "maximum_length", ii)
                    #
                    ii += 1
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return True

    def buildEntityFeatures(self, dataContainer, catName, **kwargs):
        """Build category rcsb_entity_feature ...

        Example:
            loop_
            _rcsb_entity_feature.ordinal
            _rcsb_entity_feature.entry_id
            _rcsb_entity_feature.entity_id
            _rcsb_entity_feature.feature_id
            _rcsb_entity_feature.type
            _rcsb_entity_feature.name
            _rcsb_entity_feature.description
            _rcsb_entity_feature.reference_scheme
            _rcsb_entity_feature.provenance_source
            _rcsb_entity_feature.assignment_version
            _rcsb_entity_feature.feature_positions_beg_seq_id
            _rcsb_entity_feature.feature_positions_end_seq_id
            _rcsb_entity_feature.feature_positions_value

        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_feature":
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
            # rP = kwargs.get("resourceProvider")

            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            # ---------------
            ii = cObj.getRowCount()
            jj = 1
            #
            targetFeatureD = self.__getTargetComponentFeatures(dataContainer)
            for (entityId, compId, filteredFeature) in targetFeatureD:
                cObj.setValue(ii + 1, "ordinal", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(compId, "comp_id", ii)
                cObj.setValue(filteredFeature, "type", ii)
                cObj.setValue("entity_feature_%d" % jj, "feature_id", ii)
                details = "Ligand targeted in this investigation"
                cObj.setValue(details, "description", ii)
                cObj.setValue(compId, "name", ii)
                cObj.setValue("PDB", "provenance_source", ii)
                cObj.setValue("V1.0", "assignment_version", ii)
                #
                jj += 1
                ii += 1
            #
            # BIRD type and class
            skipBird = True
            if not skipBird:
                birdFeatureD = self.__getBirdFeatures(dataContainer)
                for (entityId, compId, prdId, filteredFeature), fName in birdFeatureD.items():
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(compId, "comp_id", ii)
                    cObj.setValue(filteredFeature, "type", ii)
                    cObj.setValue("entity_feature_%d" % jj, "feature_id", ii)
                    if compId:
                        details = "Non-polymer BIRD %s chemical component %s" % (prdId, compId)
                    else:
                        details = "Polymer BIRD %s entity %s" % (prdId, entityId)
                    cObj.setValue(details, "description", ii)
                    #
                    cObj.setValue(fName, "name", ii)
                    cObj.setValue("PDB", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    #
                    jj += 1
                    ii += 1
            #
            # Monomer modifications
            jj = 1
            modMonomerFeatures = self.__commonU.getPolymerModifiedMonomerFeatures(dataContainer)
            for (entityId, seqId, compId, filteredFeature) in modMonomerFeatures:
                parentCompId = self.__ccP.getParentComponent(compId)

                cObj.setValue(ii + 1, "ordinal", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(filteredFeature, "type", ii)
                cObj.setValue("monomer_feature_%d" % jj, "feature_id", ii)
                if parentCompId:
                    details = "Parent monomer %s" % parentCompId
                    cObj.setValue(details, "name", ii)
                #
                cObj.setValue(compId, "feature_positions_beg_comp_id", ii)
                cObj.setValue(seqId, "feature_positions_beg_seq_id", ii)
                #
                cObj.setValue("PDB entity", "reference_scheme", ii)
                cObj.setValue("PDB", "provenance_source", ii)
                cObj.setValue("V1.0", "assignment_version", ii)
                #
                jj += 1
                ii += 1
            #
            #
            seqMonomerFeatures = self.__commonU.getEntitySequenceMonomerFeatures(dataContainer)
            for (entityId, seqId, compId, filteredFeature), sDetails in seqMonomerFeatures.items():
                if filteredFeature not in ["mutation"]:
                    continue
                cObj.setValue(ii + 1, "ordinal", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(filteredFeature, "type", ii)
                cObj.setValue("monomer_feature_%d" % jj, "feature_id", ii)
                details = ",".join(list(sDetails))
                cObj.setValue(details, "name", ii)
                #
                cObj.setValue(compId, "feature_positions_beg_comp_id", ii)
                cObj.setValue(seqId, "feature_positions_beg_seq_id", ii)
                #
                cObj.setValue("PDB entity", "reference_scheme", ii)
                cObj.setValue("PDB", "provenance_source", ii)
                cObj.setValue("V1.0", "assignment_version", ii)
                #
                jj += 1
                ii += 1
            #
            jj = 1
            seqRangeFeatures = self.__commonU.getEntitySequenceRangeFeatures(dataContainer)
            for (entityId, begSeqId, endSeqId, filteredFeature), sDetails in seqRangeFeatures.items():
                if filteredFeature not in ["artifact"]:
                    continue
                cObj.setValue(ii + 1, "ordinal", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(filteredFeature, "type", ii)
                cObj.setValue("range_feature_%d" % jj, "feature_id", ii)
                details = ",".join(list(sDetails))
                cObj.setValue(details, "name", ii)
                #
                cObj.setValue(begSeqId, "feature_positions_beg_seq_id", ii)
                cObj.setValue(endSeqId, "feature_positions_end_seq_id", ii)
                #
                cObj.setValue("PDB entity", "reference_scheme", ii)
                cObj.setValue("PDB", "provenance_source", ii)
                cObj.setValue("V1.0", "assignment_version", ii)
                #
                jj += 1
                ii += 1
            return True
        except Exception as e:
            logger.exception("%s %s failing with %s", dataContainer.getName(), catName, str(e))
        return False

    def addTypedEntityCategories(self, dataContainer, blockName, **kwargs):
        """Slice common entity categories into type specific entity categories.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            blockName (str): Block name

        Returns:
            bool: True for success or False otherwise

        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), blockName, kwargs)
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("entity")):
                return False
            if dataContainer.exists("rcsb_polymer_entity") or dataContainer.exists("rcsb_nonpolymer_entity") or dataContainer.exists("rcsb_branched_entity"):
                return True
            # -----
            categoryMapD = {
                "polymer": [
                    ("entity", "rcsb_polymer_entity", "id"),
                    ("entity_keywords", "rcsb_polymer_entity_keywords", "entity_id"),
                    ("entity_name_com", "rcsb_polymer_entity_name_com", "entity_id"),
                    ("entity_name_sys", "rcsb_polymer_entity_name_sys", "entity_id"),
                    ("rcsb_entity_container_identifiers", "rcsb_polymer_entity_container_identifiers", "entity_id"),
                    ("rcsb_entity_instance_container_identifiers", "rcsb_polymer_entity_instance_container_identifiers", "entity_id"),
                ],
                "non-polymer": [
                    ("entity", "rcsb_nonpolymer_entity", "id"),
                    ("entity_keywords", "rcsb_nonpolymer_entity_keywords", "entity_id"),
                    ("entity_name_com", "rcsb_nonpolymer_entity_name_com", "entity_id"),
                    ("entity_name_sys", "rcsb_nonpolymer_entity_name_sys", "entity_id"),
                    ("rcsb_entity_container_identifiers", "rcsb_nonpolymer_entity_container_identifiers", "entity_id"),
                    ("rcsb_entity_instance_container_identifiers", "rcsb_nonpolymer_entity_instance_container_identifiers", "entity_id"),
                ],
                "branched": [
                    ("entity", "rcsb_branched_entity", "id"),
                    ("entity_keywords", "rcsb_branched_entity_keywords", "entity_id"),
                    ("entity_name_com", "rcsb_branched_entity_name_com", "entity_id"),
                    ("entity_name_sys", "rcsb_branched_entity_name_sys", "entity_id"),
                    ("rcsb_entity_container_identifiers", "rcsb_branched_entity_container_identifiers", "entity_id"),
                    ("rcsb_entity_instance_container_identifiers", "rcsb_branched_entity_instance_container_identifiers", "entity_id"),
                ],
            }
            ok = self.__sliceCategoriesByEntityType(dataContainer, categoryMapD)
            return ok
        except Exception as e:
            logger.exception("%s for %s failing with %s", dataContainer.getName(), blockName, str(e))
        return False

    def addTypedEntityFeatureCategories(self, dataContainer, blockName, **kwargs):
        """Slice common entity categories into type specific entity categories.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            blockName (str): Block name

        Returns:
            bool: True for success or False otherwise

        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), blockName, kwargs)
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("entity")):
                return False
            if (
                dataContainer.exists("rcsb_polymer_entity_feature")
                or dataContainer.exists("rcsb_nonpolymer_entity_feature")
                or dataContainer.exists("rcsb_branched_entity_feature")
            ):
                return True
            # -----
            categoryMapD = {
                "polymer": [
                    ("rcsb_entity_feature", "rcsb_polymer_entity_feature", "entity_id"),
                    ("rcsb_entity_feature_summary", "rcsb_polymer_entity_feature_summary", "entity_id"),
                    ("rcsb_entity_instance_feature", "rcsb_polymer_instance_feature", "entity_id"),
                    ("rcsb_entity_instance_feature_summary", "rcsb_polymer_instance_feature_summary", "entity_id"),
                    ("rcsb_entity_instance_validation_feature", "rcsb_polymer_instance_feature", "entity_id"),
                    ("rcsb_entity_instance_validation_feature_summary", "rcsb_polymer_instance_feature_summary", "entity_id"),
                    ("rcsb_struct_conn", "rcsb_polymer_struct_conn", "entity_id"),
                    ("rcsb_entity_annotation", "rcsb_polymer_entity_annotation", "entity_id"),
                    ("rcsb_entity_instance_annotation", "rcsb_polymer_instance_annotation", "entity_id"),
                ],
                "non-polymer": [
                    ("rcsb_entity_feature", "rcsb_nonpolymer_entity_feature", "entity_id"),
                    ("rcsb_entity_feature_summary", "rcsb_nonpolymer_entity_feature_summary", "entity_id"),
                    ("rcsb_entity_instance_feature", "rcsb_nonpolymer_instance_feature", "entity_id"),
                    ("rcsb_entity_instance_feature_summary", "rcsb_nonpolymer_instance_feature_summary", "entity_id"),
                    ("rcsb_entity_instance_validation_feature", "rcsb_nonpolymer_instance_feature", "entity_id"),
                    ("rcsb_entity_instance_validation_feature_summary", "rcsb_nonpolymer_instance_feature_summary", "entity_id"),
                    ("rcsb_struct_conn", "rcsb_nonpolymer_struct_conn", "entity_id"),
                    ("rcsb_entity_annotation", "rcsb_nonpolymer_entity_annotation", "entity_id"),
                    ("rcsb_entity_instance_annotation", "rcsb_nonpolymer_instance_annotation", "entity_id"),
                ],
                "branched": [
                    ("rcsb_entity_feature", "rcsb_branched_entity_feature", "entity_id"),
                    ("rcsb_entity_feature_summary", "rcsb_branched_entity_feature_summary", "entity_id"),
                    ("rcsb_entity_instance_feature", "rcsb_branched_instance_feature", "entity_id"),
                    ("rcsb_entity_instance_feature_summary", "rcsb_branched_instance_feature_summary", "entity_id"),
                    ("rcsb_entity_instance_validation_feature", "rcsb_branched_instance_feature", "entity_id"),
                    ("rcsb_entity_instance_validation_feature_summary", "rcsb_branched_instance_feature_summary", "entity_id"),
                    ("rcsb_struct_conn", "rcsb_branched_struct_conn", "entity_id"),
                    ("rcsb_entity_annotation", "rcsb_branched_entity_annotation", "entity_id"),
                    ("rcsb_entity_instance_annotation", "rcsb_branched_instance_annotation", "entity_id"),
                ],
            }
            ok = self.__sliceCategoriesByEntityType(dataContainer, categoryMapD)
            return ok
        except Exception as e:
            logger.exception("%s for %s failing with %s", dataContainer.getName(), blockName, str(e))
        return False

    def __sliceCategoriesByEntityType(self, dataContainer, categoryMapD):
        """Slice common entity categories into type specific entity categories.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            categoryMapD  (dict): {<entity_type>: [{<source category>, <destination cateogory>, <source entity key>}, ... ], ... }

        Returns:
            bool: True for success or False otherwise

        """
        logger.debug("Starting with %r", dataContainer.getName())
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("entity")):
                return False
            eObj = dataContainer.getObj("entity")
            eCount = eObj.getRowCount()
            eTypeD = {eObj.getValue("id", ii): eObj.getValue("type", ii) for ii in range(eCount) if eObj.getValue("type", ii)}
            eTypes = list(set(eTypeD.values()))
            logger.debug("%s entity types %r map %r", dataContainer.getName(), eTypes, eTypeD)
            for eType, catTupL in categoryMapD.items():
                if eType in eTypes:
                    # create new categories as needed
                    for srcCatN, dstCatN, entityIdKey in catTupL:
                        if dataContainer.exists(srcCatN):
                            if not dataContainer.exists(dstCatN):
                                dataContainer.append(DataCategory(dstCatN, attributeNameList=self.__dApi.getAttributeNameList(dstCatN)))
                            srcObj = dataContainer.getObj(srcCatN)
                            dstObj = dataContainer.getObj(dstCatN)
                            jj = dstObj.getRowCount()
                            for ii in range(srcObj.getRowCount()):
                                entityId = srcObj.getValue(entityIdKey, ii)
                                logger.debug("%s srcCatN %s row %d key %r entityId %r", dataContainer.getName(), srcCatN, ii, entityIdKey, entityId)
                                if eTypeD[entityId] != eType:
                                    continue
                                for dstAtName in dstObj.getAttributeList():
                                    srcAtName = entityIdKey if dstAtName == "entity_id" else dstAtName
                                    logger.debug(
                                        "%s entityId %r srcCatN %r srcAtName %s dstCatN %s dstAtName %s", dataContainer.getName(), entityId, srcCatN, srcAtName, dstCatN, dstAtName
                                    )
                                    if srcObj.hasAttribute(srcAtName):
                                        tS = srcObj.getValue(srcAtName, ii)
                                        logger.debug("%s entityId %r srcCatN %r srcAtName %s value %s", dataContainer.getName(), entityId, srcCatN, srcAtName, tS)
                                        if srcAtName in ["formula_weight"]:
                                            # dalton to kiloDalton
                                            try:
                                                tV = float(tS) / 1000.0
                                                tS = "%.3f" % tV
                                            except Exception:
                                                tS = "?"
                                        if dstAtName in ["ordinal"]:
                                            tS = jj + 1
                                        _ = dstObj.setValue(tS, dstAtName, jj)
                                    else:
                                        logger.debug("Missing srcCatN %s srcAtName %s", srcCatN, srcAtName)
                                        _ = dstObj.setValue("?", dstAtName, jj)
                                jj += 1
            return True
        except Exception as e:
            logger.exception("%s failing with %s", dataContainer.getName(), str(e))
        return False

    #
    def buildEntityAnnotations(self, dataContainer, catName, **kwargs):
        """Build category rcsb_entity_annotation ...

        Example:
            loop_
            _rcsb_entity_annotation.ordinal
            _rcsb_entity_annotation.entry_id
            _rcsb_entity_annotation.entity_id
            _rcsb_entity_annotation.annotation_id
            _rcsb_entity_annotation.type
            _rcsb_entity_annotation.name
            _rcsb_entity_annotation.description
            _rcsb_entity_annotation.annotation_lineage_id
            _rcsb_entity_annotation.annotation_lineage_name
            _rcsb_entity_annotation.annotation_lineage_depth
            _rcsb_entity_annotation.provenance_source
            _rcsb_entity_annotation.assignment_version

        """
        logger.debug("Starting with %r %r %r", dataContainer.getName(), catName, kwargs)
        try:
            if catName != "rcsb_entity_annotation":
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
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            # ---------------
            ii = cObj.getRowCount()
            jj = 1
            #
            targetFeatureD = self.__getTargetComponentFeatures(dataContainer)
            #
            for (entityId, compId, filteredFeature) in targetFeatureD:
                cObj.setValue(ii + 1, "ordinal", ii)
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(entityId, "entity_id", ii)
                cObj.setValue(compId, "comp_id", ii)
                cObj.setValue(filteredFeature, "type", ii)
                cObj.setValue("entity_annotation_%d" % jj, "annotation_id", ii)
                details = "Ligand targeted in this investigation"
                cObj.setValue(details, "description", ii)
                cObj.setValue(compId, "name", ii)
                cObj.setValue("PDB", "provenance_source", ii)
                cObj.setValue("V1.0", "assignment_version", ii)
                #
                jj += 1
                ii += 1
            #
            skipBird = True
            if not skipBird:
                # BIRD type and class
                birdFeatureD = self.__getBirdFeatures(dataContainer)
                for (entityId, compId, prdId, filteredFeature), fName in birdFeatureD.items():
                    cObj.setValue(ii + 1, "ordinal", ii)
                    cObj.setValue(entryId, "entry_id", ii)
                    cObj.setValue(entityId, "entity_id", ii)
                    cObj.setValue(compId, "comp_id", ii)
                    cObj.setValue(filteredFeature, "type", ii)
                    cObj.setValue("entity_annotation_%d" % jj, "annotation_id", ii)
                    if compId:
                        details = "Non-polymer BIRD %s chemical component %s" % (prdId, compId)
                    else:
                        details = "Polymer BIRD %s entity %s" % (prdId, entityId)
                    cObj.setValue(details, "description", ii)
                    #
                    cObj.setValue(fName, "name", ii)
                    cObj.setValue("PDB", "provenance_source", ii)
                    cObj.setValue("V1.0", "assignment_version", ii)
                    #
                    jj += 1
                    ii += 1
            return True
        except Exception as e:
            logger.exception("%s %s failing with %s", dataContainer.getName(), catName, str(e))
        return False
