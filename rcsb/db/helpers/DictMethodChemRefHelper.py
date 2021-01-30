##
# File:    DictMethodChemRefHelper.py
# Author:  J. Westbrook
# Date:    16-Jul-2019
# Version: 0.001 Initial version
#
##
"""
Helper class implements external method references supporting chemical
reference data definitions in the RCSB dictionary extension.
"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

# from collections import Counter, OrderedDict

from mmcif.api.DataCategory import DataCategory

logger = logging.getLogger(__name__)


class DictMethodChemRefHelper(object):
    """Helper class implements external method references supporting chemical
    reference data definitions in the RCSB dictionary extension.
    """

    def __init__(self, **kwargs):
        """
        Args:
            resourceProvider: (obj) instance of DictMethodResourceProvider()

        """
        #
        self._raiseExceptions = kwargs.get("raiseExceptions", False)
        #
        rP = kwargs.get("resourceProvider")
        self.__dApi = rP.getResource("Dictionary API instance (pdbx_core)") if rP else None
        logger.debug("Dictionary method helper init")

    def echo(self, msg):
        logger.info(msg)

    def addChemCompRelated(self, dataContainer, catName, **kwargs):
        """Add category rcsb_chem_comp_related.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For example,

             loop_
             _rcsb_chem_comp_related.comp_id
             _rcsb_chem_comp_related.ordinal
             _rcsb_chem_comp_related.resource_name
             _rcsb_chem_comp_related.resource_accession_code
             _rcsb_chem_comp_related.related_mapping_method
             ATP 1 DrugBank DB00171 'assigned by resource'
        """
        try:
            logger.debug("Starting with  %r %r", dataContainer.getName(), catName)
            if not (dataContainer.exists("chem_comp_atom") and dataContainer.exists("chem_comp_bond")):
                return False
            rP = kwargs.get("resourceProvider")
            # ------- new
            ccId = self.__getChemCompId(dataContainer)
            dbId, atcIdL, mappingType, dbVersion = self.__getDrugBankMapping(dataContainer, rP)
            logger.debug("Using DrugBank version %r", dbVersion)
            #  ------------ ----------------------- ----------------------- ----------------------- -----------
            if dbId:
                #
                if dataContainer.exists("rcsb_chem_comp_container_identifiers"):
                    tObj = dataContainer.getObj("rcsb_chem_comp_container_identifiers")
                    if not tObj.hasAttribute("drugbank_id"):
                        tObj.appendAttribute("drugbank_id")
                    tObj.setValue(dbId, "drugbank_id", 0)
                    if atcIdL:
                        if not tObj.hasAttribute("atc_codes"):
                            tObj.appendAttribute("atc_codes")
                        tObj.setValue(",".join(atcIdL), "atc_codes", 0)
                #
                if not dataContainer.exists(catName):
                    dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
                wObj = dataContainer.getObj(catName)
                rL = wObj.selectIndices("DrugBank", "resource_name")
                ok = False
                if rL:
                    ok = wObj.removeRows(rL)
                    if not ok:
                        logger.debug("Error removing rows in %r %r", catName, rL)
                # ---
                iRow = wObj.getRowCount()
                wObj.setValue(ccId, "comp_id", iRow)
                wObj.setValue(iRow + 1, "ordinal", iRow)
                wObj.setValue("DrugBank", "resource_name", iRow)
                wObj.setValue(dbId, "resource_accession_code", iRow)
                wObj.setValue(mappingType, "related_mapping_method", iRow)
                #
            #  ------------ ----------------------- ----------------------- ----------------------- -----------
            ccmProvider = rP.getResource("ChemCompModelProvider instance") if rP else None
            csdMapD = ccmProvider.getMapping()
            #
            if csdMapD and ccId in csdMapD:
                if not dataContainer.exists(catName):
                    dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
                wObj = dataContainer.getObj(catName)
                logger.debug("Using CSD model mapping length %d", len(csdMapD))
                dbId = csdMapD[ccId][0]["db_code"]
                rL = wObj.selectIndices("CCDC/CSD", "resource_name")
                if rL:
                    ok = wObj.removeRows(rL)
                    if not ok:
                        logger.debug("Error removing rows in %r %r", catName, rL)
                iRow = wObj.getRowCount()
                wObj.setValue(ccId, "comp_id", iRow)
                wObj.setValue(iRow + 1, "ordinal", iRow)
                wObj.setValue("CCDC/CSD", "resource_name", iRow)
                wObj.setValue(dbId, "resource_accession_code", iRow)
                wObj.setValue("assigned by PDB", "related_mapping_method", iRow)
            #
            residProvider = rP.getResource("ResidProvider instance") if rP else None
            residMapD = residProvider.getMapping()
            #
            if residMapD and ccId in residMapD:
                if not dataContainer.exists(catName):
                    dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
                wObj = dataContainer.getObj(catName)
                rL = wObj.selectIndices("RESID", "resource_name")
                if rL:
                    ok = wObj.removeRows(rL)
                    if not ok:
                        logger.debug("Error removing rows in %r %r", catName, rL)
                logger.debug("Using RESID model mapping length %d", len(residMapD))
                for rD in residMapD[ccId]:
                    dbId = rD["residCode"]
                    iRow = wObj.getRowCount()
                    wObj.setValue(ccId, "comp_id", iRow)
                    wObj.setValue(iRow + 1, "ordinal", iRow)
                    wObj.setValue("RESID", "resource_name", iRow)
                    wObj.setValue(dbId, "resource_accession_code", iRow)
                    wObj.setValue("matching by RESID resource", "related_mapping_method", iRow)
            #
            pubchemProvider = rP.getResource("PubChemProvider instance") if rP else None
            pubchemMapD = pubchemProvider.getIdentifiers()
            if pubchemMapD and ccId in pubchemMapD:
                pharosProvider = rP.getResource("PharosProvider instance") if rP else None
                pharosChemblD = pharosProvider.getIdentifiers()

                if not dataContainer.exists(catName):
                    dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
                wObj = dataContainer.getObj(catName)
                for rName in ["ChEBI", "ChEMBL", "CAS", "PubChem"]:
                    rL = wObj.selectIndices(rName, "resource_name")
                    if rL:
                        ok = wObj.removeRows(rL)
                        if not ok:
                            logger.debug("Error removing rows in %r %r", catName, rL)
                #
                logger.debug("Using PubChem mapping length %d", len(pubchemMapD))
                xD = {}
                for rD in pubchemMapD[ccId]:
                    for tName, tObj in rD.items():
                        if tName == "pcId":
                            xD.setdefault("PubChem", set()).add(tObj)
                        elif tName in ["CAS", "ChEBI"]:
                            for tId in tObj:
                                xD.setdefault(tName, set()).add(tId)
                        elif tName in ["ChEMBL"]:
                            for tId in tObj:
                                xD.setdefault(tName, set()).add(tId)
                                if pharosChemblD and tId in pharosChemblD:
                                    logger.debug("Mapping ccId %r to Pharos %r", ccId, tId)
                                    xD.setdefault("Pharos", set()).add(tId)

                #
                for rName, rIdS in xD.items():
                    if rName in ["PubChem", "Pharos"]:
                        aMethod = "matching InChIKey in PubChem"
                    elif rName in ["CAS", "ChEMBL", "ChEBI"]:
                        aMethod = "assigned by PubChem resource"
                    elif rName in ["Pharos"]:
                        aMethod = "matching ChEMBL ID in Pharos"
                    for rId in rIdS:
                        iRow = wObj.getRowCount()
                        wObj.setValue(ccId, "comp_id", iRow)
                        wObj.setValue(iRow + 1, "ordinal", iRow)
                        wObj.setValue(rName, "resource_name", iRow)
                        wObj.setValue(rId, "resource_accession_code", iRow)
                        wObj.setValue(aMethod, "related_mapping_method", iRow)

            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def __getChemCompId(self, dataContainer):
        if not dataContainer.exists("chem_comp"):
            return None
        ccObj = dataContainer.getObj("chem_comp")
        if not ccObj.hasAttribute("pdbx_release_status"):
            return None
        return ccObj.getValueOrDefault("id", 0, None)

    def __getDrugBankMapping(self, dataContainer, resourceProvider):
        """Return the DrugBank mapping for the chemical definition in the input dataContainer.

        Args:
            dataContainer (obj): instance of a DataContainer() object
            resourceProvider (obj): instance of a ResourceProvider() object

        Returns:
            mType, DrugBankId, actL (str,str, list): mapping type and DrugBank accession code, list of ATC assignments
        """
        try:
            dbId = None
            atcL = []
            mappingType = None

            dbProvider = resourceProvider.getResource("DrugBankProvider instance") if resourceProvider else None
            dbD = dbProvider.getMapping()
            dbVersion = dbProvider.getVersion()
            if dbD:
                ccId = self.__getChemCompId(dataContainer)
                #
                dbMapD = dbD["id_map"]
                inKeyD = dbD["inchikey_map"]
                atcD = dbD["db_atc_map"]
                logger.debug("DrugBank correspondence length is %d", len(dbMapD))
                logger.debug("atcD length is %d", len(atcD))
                logger.debug("inKeyD length is %d", len(inKeyD))
                #
                if dataContainer.exists("rcsb_chem_comp_descriptor"):
                    ccIObj = dataContainer.getObj("rcsb_chem_comp_descriptor")

                    if ccIObj.hasAttribute("InChIKey"):
                        inky = ccIObj.getValue("InChIKey", 0)
                        logger.debug("inKeyD length is %d testing %r", len(inKeyD), inky)
                        if inky in inKeyD:
                            logger.debug("Matching inchikey for %s", ccId)
                            dbId = inKeyD[inky][0]["drugbank_id"]
                            mappingType = "matching InChIKey in DrugBank"
                #

                if not dbId and dbMapD and dataContainer.getName() in dbMapD:
                    dbId = dbMapD[ccId]["drugbank_id"]
                    mappingType = "assigned by DrugBank resource"
                    logger.debug("Matching db assignment for %s", ccId)
                if atcD and dbId in atcD:
                    atcL = atcD[dbId]

        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return dbId, atcL, mappingType, dbVersion

    def addChemCompAnnotation(self, dataContainer, catName, **kwargs):
        """Generate the rcsb_chem_annotation category -

        Args:
            dataContainer ([type]): [description]
            catName ([type]): [description]

        Returns:
            [type]: [description]

                loop_
                _rcsb_chem_comp_annotation.ordinal
                _rcsb_chem_comp_annotation.entry_id
                _rcsb_chem_comp_annotation.entity_id
                #
                _rcsb_chem_comp_annotation.annotation_id
                _rcsb_chem_comp_annotation.type
                _rcsb_chem_comp_annotation.name
                _rcsb_chem_comp_annotation.description
                #
                _rcsb_chem_comp_annotation.annotation_lineage_id
                _rcsb_chem_comp_annotation.annotation_lineage_name
                _rcsb_chem_comp_annotation.annotation_lineage_depth
                #
                _rcsb_chem_comp_annotation.provenance_source
                _rcsb_chem_comp_annotation.assignment_version
                # ...

                loop_
                _pdbx_chem_comp_feature.comp_id
                _pdbx_chem_comp_feature.type
                _pdbx_chem_comp_feature.value
                _pdbx_chem_comp_feature.source
                _pdbx_chem_comp_feature.support
                NAG 'CARBOHYDRATE ISOMER' D        PDB ?
                NAG 'CARBOHYDRATE RING'   pyranose PDB ?
                NAG 'CARBOHYDRATE ANOMER' beta     PDB ?
        """
        try:
            if not (dataContainer.exists("chem_comp_atom") and dataContainer.exists("chem_comp_bond")):
                return False
            #
            logger.debug("Starting with  %r %r", dataContainer.getName(), catName)
            rP = kwargs.get("resourceProvider")
            ccId = self.__getChemCompId(dataContainer)
            # ----
            if dataContainer.exists("pdbx_chem_comp_feature"):
                fObj = dataContainer.getObj("pdbx_chem_comp_feature")
                if not dataContainer.exists(catName):
                    dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
                wObj = dataContainer.getObj(catName)
                #
                modDate = None
                if dataContainer.exists("chem_comp"):
                    cObj = dataContainer.getObj("chem_comp")
                    if cObj.hasAttribute("pdbx_modified_date"):
                        modDate = cObj.getValue("pdbx_modified_date", 0)
                    else:
                        logger.info("%r missing modified_date", ccId)
                #
                fD = {}
                for ii in range(fObj.getRowCount()):
                    pSource = fObj.getValue("source", ii)
                    pCode = "PDB Reference Data" if pSource.upper() == "PDB" else None
                    if not pCode:
                        continue
                    fType = fObj.getValue("type", ii)
                    if fType.upper() not in ["CARBOHYDRATE ISOMER", "CARBOHYDRATE RING", "CARBOHYDRATE ANOMER", "CARBOHYDRATE PRIMARY CARBONYL GROUP"]:
                        continue
                    fType = fType.title()
                    fValue = fObj.getValue("value", ii)
                    if (fType, fValue, pCode) in fD:
                        continue
                    fD[(fType, fValue, pCode)] = True
                    #
                    iRow = wObj.getRowCount()
                    wObj.setValue(ccId, "comp_id", iRow)
                    wObj.setValue(iRow + 1, "ordinal", iRow)
                    wObj.setValue(fType, "type", iRow)
                    wObj.setValue("%s_%d" % (ccId, ii + 1), "annotation_id", iRow)
                    wObj.setValue(fValue, "name", iRow)
                    wObj.setValue(pCode, "provenance_source", iRow)
                    av = modDate if modDate else "1.0"
                    wObj.setValue(av, "assignment_version", iRow)
            #
            # ----

            dbId, atcIdL, mappingType, dbVersion = self.__getDrugBankMapping(dataContainer, rP)
            atcP = rP.getResource("AtcProvider instance") if rP else None
            if atcIdL and atcP:
                #
                if not dataContainer.exists(catName):
                    dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
                # -----
                wObj = dataContainer.getObj(catName)
                #
                for atcId in atcIdL:
                    iRow = wObj.getRowCount()
                    wObj.setValue(ccId, "comp_id", iRow)
                    wObj.setValue(iRow + 1, "ordinal", iRow)
                    wObj.setValue("ATC", "type", iRow)
                    wObj.setValue(atcId, "annotation_id", iRow)
                    wObj.setValue(atcP.getAtcName(atcId), "name", iRow)
                    #
                    wObj.setValue("ATC " + mappingType, "description", iRow)
                    # ---
                    wObj.setValue(";".join(atcP.getNameLineage(atcId)), "annotation_lineage_name", iRow)
                    idLinL = atcP.getIdLineage(atcId)
                    wObj.setValue(";".join(idLinL), "annotation_lineage_id", iRow)
                    wObj.setValue(";".join([str(jj) for jj in range(0, len(idLinL) + 1)]), "annotation_lineage_depth", iRow)
                    #
                    wObj.setValue("DrugBank", "provenance_source", iRow)
                    wObj.setValue(dbVersion, "assignment_version", iRow)
                    logger.debug("dbId %r atcId %r lineage %r", dbId, atcId, idLinL)
            # -----
            rsProvider = rP.getResource("ResidProvider instance") if rP else None
            residD = rsProvider.getMapping()
            #
            if residD and (ccId in residD):
                if not dataContainer.exists(catName):
                    dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
                wObj = dataContainer.getObj(catName)
                # -----
                residVersion = rsProvider.getVersion()
                jj = 1
                for rD in residD[ccId]:
                    if "modRes" not in rD:
                        continue
                    for modRes in rD["modRes"]:
                        iRow = wObj.getRowCount()
                        wObj.setValue(ccId, "comp_id", iRow)
                        wObj.setValue(iRow + 1, "ordinal", iRow)
                        wObj.setValue("Modification Type", "type", iRow)
                        wObj.setValue("modres_%d" % jj, "annotation_id", iRow)
                        wObj.setValue(modRes, "name", iRow)
                        wObj.setValue("RESID", "provenance_source", iRow)
                        wObj.setValue(residVersion, "assignment_version", iRow)
                        jj += 1
                #
                jj = 1
                for rD in residD[ccId]:
                    if "genEnzymes" not in rD:
                        continue
                    for genEnzyme in rD["genEnzymes"]:
                        iRow = wObj.getRowCount()
                        wObj.setValue(ccId, "comp_id", iRow)
                        wObj.setValue(iRow + 1, "ordinal", iRow)
                        wObj.setValue("Generating Enzyme", "type", iRow)
                        wObj.setValue("enzyme_%d" % jj, "annotation_id", iRow)
                        wObj.setValue(genEnzyme, "name", iRow)
                        wObj.setValue("RESID", "provenance_source", iRow)
                        wObj.setValue(residVersion, "assignment_version", iRow)
                        jj += 1
                #
                psimodP = rP.getResource("PsiModProvider instance") if rP else None
                if psimodP:
                    jj = 1
                    for rD in residD[ccId]:
                        if "ontRefs" not in rD:
                            continue
                        for ontId in rD["ontRefs"]:
                            if ontId[:3] != "MOD":
                                continue
                            iRow = wObj.getRowCount()
                            wObj.setValue(ccId, "comp_id", iRow)
                            wObj.setValue(iRow + 1, "ordinal", iRow)
                            wObj.setValue("PSI-MOD", "type", iRow)
                            wObj.setValue(ontId, "annotation_id", iRow)
                            wObj.setValue(psimodP.getName(ontId), "name", iRow)
                            wObj.setValue("RESID", "provenance_source", iRow)
                            wObj.setValue(residVersion, "assignment_version", iRow)
                            #
                            linL = psimodP.getLineage(ontId)
                            wObj.setValue(";".join([tup[0] for tup in linL]), "annotation_lineage_id", iRow)
                            wObj.setValue(";".join([tup[1] for tup in linL]), "annotation_lineage_name", iRow)
                            wObj.setValue(";".join([str(tup[2]) for tup in linL]), "annotation_lineage_depth", iRow)
                    #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def addChemCompTargets(self, dataContainer, catName, **kwargs):
        """Add category rcsb_chem_comp_target using DrugBank annotations.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        Example:
             loop_
             _rcsb_chem_comp_target.comp_id
             _rcsb_chem_comp_target.ordinal
             _rcsb_chem_comp_target.name
             _rcsb_chem_comp_target.interaction_type
             _rcsb_chem_comp_target.target_actions
             _rcsb_chem_comp_target.organism_common_name
             _rcsb_chem_comp_target.reference_database_name
             _rcsb_chem_comp_target.reference_database_accession_code
             _rcsb_chem_comp_target.provenance_code
             ATP 1 "O-phosphoseryl-tRNA(Sec) selenium transferase" target cofactor Human UniProt Q9HD40 DrugBank

        DrugBank target info:
        {
            "type": "target",
            "name": "Alanine--glyoxylate aminotransferase 2, mitochondrial",
            "organism": "Human",
            "actions": [
               "cofactor"
            ],
            "known_action": "unknown",
            "uniprot_ids": "Q9BYV1"
         },

        """
        try:
            logger.debug("Starting with  %r %r", dataContainer.getName(), catName)
            # Exit if source categories are missing
            if not (dataContainer.exists("chem_comp_atom") and dataContainer.exists("chem_comp_bond")):
                return False

            #
            rP = kwargs.get("resourceProvider")
            dbProvider = rP.getResource("DrugBankProvider instance") if rP else None
            dbD = dbProvider.getMapping()
            if not dbD:
                return False

            dbMapD = dbD["id_map"] if "id_map" in dbD else None
            #
            ccId = dataContainer.getName()
            if dbMapD and ccId in dbMapD and "target_interactions" in dbMapD[ccId]:
                #
                # Create the new target category
                if not dataContainer.exists(catName):
                    dataContainer.append(
                        DataCategory(
                            catName,
                            attributeNameList=[
                                "comp_id",
                                "ordinal",
                                "name",
                                "interaction_type",
                                "target_actions",
                                "organism_common_name",
                                "reference_database_name",
                                "reference_database_accession_code",
                                "provenance_code",
                            ],
                        )
                    )
                wObj = dataContainer.getObj(catName)
                logger.debug("Using DrugBank mapping length %d", len(dbMapD))
                rL = wObj.selectIndices("DrugBank", "provenance_code")
                if rL:
                    ok = wObj.removeRows(rL)
                    if not ok:
                        logger.debug("Error removing rows in %r %r", catName, rL)
                #
                iRow = wObj.getRowCount()
                iRow = wObj.getRowCount()
                for tD in dbMapD[ccId]["target_interactions"]:
                    wObj.setValue(ccId, "comp_id", iRow)
                    wObj.setValue(iRow + 1, "ordinal", iRow)
                    wObj.setValue(tD["name"], "name", iRow)
                    wObj.setValue(tD["type"], "interaction_type", iRow)
                    if "actions" in tD and tD["actions"]:
                        wObj.setValue(";".join(tD["actions"]), "target_actions", iRow)
                    if "organism" in tD:
                        wObj.setValue(tD["organism"], "organism_common_name", iRow)
                    if "uniprot_ids" in tD:
                        wObj.setValue("UniProt", "reference_database_name", iRow)
                        wObj.setValue(tD["uniprot_ids"], "reference_database_accession_code", iRow)
                    wObj.setValue("DrugBank", "provenance_code", iRow)
                    iRow += 1

            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def __getAuditDates(self, dataContainer, catName):
        createDate = None
        releaseDate = None
        reviseDate = None
        try:
            if dataContainer.exists(catName):
                cObj = dataContainer.getObj(catName)
                for iRow in range(cObj.getRowCount()):
                    aType = cObj.getValueOrDefault("action_type", iRow, defaultValue=None)
                    dateVal = cObj.getValueOrDefault("date", iRow, defaultValue=None)
                    if aType in ["Create component"]:
                        createDate = dateVal
                    elif aType in ["Initial release"]:
                        releaseDate = dateVal
                reviseDate = cObj.getValueOrDefault("date", cObj.getRowCount() - 1, defaultValue=None)
        except Exception as e:
            logger.exception("Faling with %s", str(e))
        return createDate, releaseDate, reviseDate

    def addChemCompInfo(self, dataContainer, catName, **kwargs):
        """Add category rcsb_chem_comp_info and rcsb_chem_comp_container_identifiers.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For example,
             _rcsb_chem_comp_info.comp_id                 BNZ
             _rcsb_chem_comp_info.atom_count              12
             _rcsb_chem_comp_info.atom_count_chiral        0
             _rcsb_chem_comp_info.bond_count              12
             _rcsb_chem_comp_info.bond_count_aromatic      6
             _rcsb_chem_comp_info.atom_count_heavy         6
        """
        try:
            logger.debug("Starting with  %r %r %r", dataContainer.getName(), catName, kwargs)
            # Exit if source categories are missing
            if not dataContainer.exists("chem_comp"):
                return False
            ccObj = dataContainer.getObj("chem_comp")
            if not ccObj.hasAttribute("pdbx_release_status"):
                return False
            ccId = ccObj.getValue("id", 0)
            ccReleaseStatus = ccObj.getValue("pdbx_release_status", 0)
            subComponentIds = ccObj.getValueOrDefault("pdbx_subcomponent_list", 0, defaultValue=None)
            #
            #
            prdId = prdReleaseStatus = representAs = None
            if dataContainer.exists("pdbx_reference_molecule"):
                prdObj = dataContainer.getObj("pdbx_reference_molecule")
                prdId = prdObj.getValueOrDefault("prd_id", 0, defaultValue=None)
                prdReleaseStatus = prdObj.getValueOrDefault("release_status", 0, defaultValue=None)
                representAs = prdObj.getValueOrDefault("represent_as", 0, defaultValue=None)
            #
            # ------- add the canonical identifiers --------
            cN = "rcsb_chem_comp_container_identifiers"
            if not dataContainer.exists(cN):
                dataContainer.append(DataCategory(cN, attributeNameList=self.__dApi.getAttributeNameList(cN)))
            idObj = dataContainer.getObj(cN)
            idObj.setValue(ccId, "comp_id", 0)
            if prdId:
                idObj.setValue(prdId, "prd_id", 0)
            idObj.setValue(ccId, "rcsb_id", 0)
            if subComponentIds:
                tL = [tV.strip() for tV in subComponentIds.split()]
                idObj.setValue(",".join(tL), "subcomponent_ids", 0)
            #
            # Get audit info -
            if representAs and representAs.lower() in ["polymer"]:
                _, releaseDate, revisionDate = self.__getAuditDates(dataContainer, "pdbx_prd_audit")
            else:
                _, releaseDate, revisionDate = self.__getAuditDates(dataContainer, "pdbx_chem_comp_audit")
            #
            #  --------- --------- --------- ---------
            # Create the new target category
            #
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            # -------
            wObj = dataContainer.getObj(catName)
            #
            numAtoms = 0
            numAtomsHeavy = 0
            numAtomsChiral = 0
            try:
                cObj = dataContainer.getObj("chem_comp_atom")
                numAtoms = cObj.getRowCount()
                numAtomsHeavy = 0
                numAtomsChiral = 0
                for ii in range(numAtoms):
                    el = cObj.getValue("type_symbol", ii)
                    if el != "H":
                        numAtomsHeavy += 1
                    chFlag = cObj.getValue("pdbx_stereo_config", ii)
                    if chFlag != "N":
                        numAtomsChiral += 1
            except Exception:
                logger.warning("Missing chem_comp_atom category for %s", ccId)
                numAtoms = 0
                numAtomsHeavy = 0
                numAtomsChiral = 0
            #
            wObj.setValue(ccId, "comp_id", 0)
            if prdReleaseStatus:
                wObj.setValue(prdReleaseStatus, "release_status", 0)
            else:
                wObj.setValue(ccReleaseStatus, "release_status", 0)
            #
            wObj.setValue(releaseDate, "initial_release_date", 0)
            wObj.setValue(revisionDate, "revision_date", 0)
            #
            wObj.setValue(numAtoms, "atom_count", 0)
            wObj.setValue(numAtomsChiral, "atom_count_chiral", 0)
            wObj.setValue(numAtomsHeavy, "atom_count_heavy", 0)
            #
            #  ------
            numBonds = 0
            numBondsAro = 0
            try:
                cObj = dataContainer.getObj("chem_comp_bond")
                numBonds = cObj.getRowCount()
                numBondsAro = 0
                for ii in range(numAtoms):
                    aroFlag = cObj.getValue("pdbx_aromatic_flag", ii)
                    if aroFlag != "N":
                        numBondsAro += 1
            except Exception:
                pass
            #
            wObj.setValue(numBonds, "bond_count", 0)
            wObj.setValue(numBondsAro, "bond_count_aromatic", 0)
            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def addChemCompDescriptor(self, dataContainer, catName, **kwargs):
        """Add category rcsb_chem_comp_descriptor.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For example, parse the pdbx_chem_comp_descriptor category and extract SMILES/CACTVS and InChI descriptors -

        loop_
        _pdbx_chem_comp_descriptor.comp_id
        _pdbx_chem_comp_descriptor.type
        _pdbx_chem_comp_descriptor.program
        _pdbx_chem_comp_descriptor.program_version
        _pdbx_chem_comp_descriptor.descriptor
        ATP SMILES           ACDLabs              10.04 "O=P(O)(O)OP(=O)(O)OP(=O)(O)OCC3OC(n2cnc1c(ncnc12)N)C(O)C3O"
        ATP SMILES_CANONICAL CACTVS               3.341 "Nc1ncnc2n(cnc12)[C@@H]3O[C@H](CO[P@](O)(=O)O[P@@](O)(=O)O[P](O)(O)=O)[C@@H](O)[C@H]3O"
        ATP SMILES           CACTVS               3.341 "Nc1ncnc2n(cnc12)[CH]3O[CH](CO[P](O)(=O)O[P](O)(=O)O[P](O)(O)=O)[CH](O)[CH]3O"
        ATP SMILES_CANONICAL "OpenEye OEToolkits" 1.5.0 "c1nc(c2c(n1)n(cn2)[C@H]3[C@@H]([C@@H]([C@H](O3)CO[P@@](=O)(O)O[P@](=O)(O)OP(=O)(O)O)O)O)N"
        ATP SMILES           "OpenEye OEToolkits" 1.5.0 "c1nc(c2c(n1)n(cn2)C3C(C(C(O3)COP(=O)(O)OP(=O)(O)OP(=O)(O)O)O)O)N"
        ATP InChI            InChI                1.03  "InChI=1S/C10H16N5O13P3/c11-8-5-9(13-2-12-8)15(3- ...."
        ATP InChIKey         InChI                1.03  ZKHQWZAMYRWXGA-KQYNXXCUSA-N

        To produce -
             _rcsb_chem_comp_descriptor.comp_id                 ATP
             _rcsb_chem_comp_descriptor.SMILES                  'Nc1ncnc2n(cnc12)[CH]3O[CH](CO[P](O)(=O)O[P](O)(=O)O[P](O)(O)=O)[CH](O)[CH]3O'
             _rcsb_chem_comp_descriptor.SMILES_stereo           'Nc1ncnc2n(cnc12)[C@@H]3O[C@H](CO[P@](O)(=O)O[P@@](O)(=O)O[P](O)(O)=O)[C@@H](O)[C@H]3O'
             _rcsb_chem_comp_descriptor.InChI                   'InChI=1S/C10H16N5O13P3/c11-8-5-9(13-2-12-8)15(3-14-5)10-7(17)6(16)4(26-10)1-25 ...'
             _rcsb_chem_comp_descriptor.InChIKey                'ZKHQWZAMYRWXGA-KQYNXXCUSA-N'
        """
        try:
            logger.debug("Starting with  %r %r %r", dataContainer.getName(), catName, kwargs)
            # Exit if source categories are missing
            if not (dataContainer.exists("chem_comp") and dataContainer.exists("pdbx_chem_comp_descriptor")):
                return False
            #
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=["comp_id", "SMILES", "SMILES_stereo", "InChI", "InChIKey"]))
            #
            wObj = dataContainer.getObj(catName)
            ccIObj = dataContainer.getObj("pdbx_chem_comp_descriptor")
            iRow = 0
            ccId = ""
            for ii in range(ccIObj.getRowCount()):
                ccId = ccIObj.getValue("comp_id", ii)
                nm = ccIObj.getValue("descriptor", ii)
                prog = ccIObj.getValue("program", ii)
                typ = ccIObj.getValue("type", ii)
                #
                if typ == "SMILES_CANONICAL" and prog.upper().startswith("OPENEYE"):
                    wObj.setValue(nm, "SMILES_stereo", iRow)
                elif typ == "SMILES" and prog.upper().startswith("OPENEYE"):
                    wObj.setValue(nm, "SMILES", iRow)
                elif typ == "InChI" and prog == "InChI":
                    wObj.setValue(nm, "InChI", iRow)
                elif typ == "InChIKey" and prog == "InChI":
                    wObj.setValue(nm, "InChIKey", iRow)
            #
            wObj.setValue(ccId, "comp_id", iRow)
            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def renameCitationCategory(self, dataContainer, catName, **kwargs):
        """Rename citation and citation author categories.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise
        """
        try:
            _ = kwargs
            logger.debug("Starting with  %r %r", dataContainer.getName(), catName)
            if not (dataContainer.exists("chem_comp") and dataContainer.exists("pdbx_chem_comp_identifier")):
                return False
            #
            # Rename target categories
            if dataContainer.exists("citation"):
                dataContainer.rename("citation", "rcsb_bird_citation")
            if dataContainer.exists("citation_author"):
                dataContainer.rename("citation_author", "rcsb_bird_citation_author")
            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))

        return False

    def addChemCompSynonyms(self, dataContainer, catName, **kwargs):
        """Add category rcsb_chem_comp_synonyms including PDB and DrugBank annotations.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For example,

             loop_
                 _rcsb_chem_comp_synonyms.comp_id
                 _rcsb_chem_comp_synonyms.ordinal
                 _rcsb_chem_comp_synonyms.name
                 _rcsb_chem_comp_synonyms.provenance_code
                 _rcsb_chem_comp_synonyms.type

                    ATP 1 "adenosine 5'-(tetrahydrogen triphosphate)"  'PDB Reference Data' 'Preferred Name'
                    ATP 2 "Adenosine 5'-triphosphate"  'PDB Reference Data' 'PDB Reference Data' 'Preferred Common Name'
                    ATP 3 Atriphos  DrugBank 'Synonym'
                    ATP 4 Striadyne DrugBank 'Synonym'

        """
        try:
            logger.debug("Starting with  %r %r", dataContainer.getName(), catName)
            if not (dataContainer.exists("chem_comp") and dataContainer.exists("chem_comp_atom") and dataContainer.exists("pdbx_chem_comp_identifier")):
                return False
            #
            #
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            else:
                # remove the rowlist -
                pass
            #
            tTupL = self.__dApi.getEnumListWithDetail(catName, "type")
            typeLookupD = {tTup[0].upper(): tTup[0] for tTup in tTupL}

            pTupL = self.__dApi.getEnumListWithDetail(catName, "provenance_source")
            provLookupD = {pTup[0].upper(): pTup[0] for pTup in pTupL}

            provLookupD["ACD-LABS"] = "ACDLabs"
            provLookupD["PDB"] = "PDB Reference Data"

            wObj = dataContainer.getObj(catName)
            #
            # Get all of the names relevant names from the definition -
            #
            iRow = 0
            nmD = {}
            provCode = "PDB Reference Data"
            ccObj = dataContainer.getObj("chem_comp")
            ccId = ccObj.getValue("id", 0)
            ccName = ccObj.getValue("name", 0)
            # ccSynonymL = []
            # if ccObj.hasAttribute("pdbx_synonyms"):
            #    ccSynonymL = str(ccObj.getValue("pdbx_synonyms", 0)).split(";")
            #
            wObj.setValue(ccId, "comp_id", iRow)
            wObj.setValue(ccName, "name", iRow)
            wObj.setValue(iRow + 1, "ordinal", iRow)
            wObj.setValue(provCode, "provenance_source", iRow)
            wObj.setValue("Preferred Name", "type", iRow)
            iRow += 1
            nmD[ccName] = True
            #
            if dataContainer.exists("pdbx_chem_comp_synonyms"):
                qObj = dataContainer.getObj("pdbx_chem_comp_synonyms")
                for ii in range(qObj.getRowCount()):
                    sType = qObj.getValue("type", ii)
                    pCode = provCode
                    pType = "Preferred Synonym" if sType.upper() == "PREFERRED" else "Synonym"
                    nm = str(qObj.getValue("name", ii)).strip()
                    if nm in nmD:
                        continue
                    nmD[nm] = True
                    logger.debug("Synonym %r sType %r pCode %r", nm, sType, pCode)
                    wObj.setValue(ccId, "comp_id", iRow)
                    wObj.setValue(nm, "name", iRow)
                    wObj.setValue(iRow + 1, "ordinal", iRow)
                    wObj.setValue(pCode, "provenance_source", iRow)
                    wObj.setValue(pType, "type", iRow)
                    iRow += 1
            else:
                logger.debug("No synonyms for %s", ccId)

            # for nm in ccSynonymL:
            #     if nm in ["?", "."]:
            #         continue
            #     if nm in nmD:
            #         continue
            #     nmD[nm] = True
            #     wObj.setValue(ccId, "comp_id", iRow)
            #     wObj.setValue(nm, "name", iRow)
            #     wObj.setValue(iRow + 1, "ordinal", iRow)
            #     wObj.setValue(provCode, "provenance_source", iRow)
            #     wObj.setValue("Synonym", "type", iRow)
            #     iRow += 1
            #
            ccIObj = dataContainer.getObj("pdbx_chem_comp_identifier")
            for ii in range(ccIObj.getRowCount()):
                nm = str(ccIObj.getValue("identifier", ii)).strip()
                prog = ccIObj.getValue("program", ii)
                iType = ccIObj.getValue("type", ii)
                if not iType or iType.upper() not in typeLookupD:
                    continue
                if prog and prog.upper() in provLookupD:
                    sProg = provLookupD[prog.upper()]
                    sType = typeLookupD[iType.upper()]
                    wObj.setValue(ccId, "comp_id", iRow)
                    wObj.setValue(nm, "name", iRow)
                    wObj.setValue(iRow + 1, "ordinal", iRow)
                    wObj.setValue(sProg, "provenance_source", iRow)
                    wObj.setValue(sType, "type", iRow)

                    iRow += 1
                else:
                    logger.error("%s unknown provenance %r", ccId, prog)
            #
            rP = kwargs.get("resourceProvider")
            dbProvider = rP.getResource("DrugBankProvider instance") if rP else None
            dbD = dbProvider.getMapping()
            if dbD:
                dbMapD = dbD["id_map"]
                #
                if dbMapD and ccId in dbMapD:
                    if "aliases" in dbMapD[ccId]:
                        iRow = wObj.getRowCount()
                        for nm in dbMapD[ccId]["aliases"]:
                            wObj.setValue(ccId, "comp_id", iRow)
                            wObj.setValue(str(nm).strip(), "name", iRow)
                            wObj.setValue(iRow + 1, "ordinal", iRow)
                            wObj.setValue("DrugBank", "provenance_source", iRow)
                            wObj.setValue("Synonym", "type", iRow)
                            iRow += 1
                    if "brand_names" in dbMapD[ccId]:
                        iRow = wObj.getRowCount()
                        for nm in dbMapD[ccId]["brand_names"]:
                            wObj.setValue(ccId, "comp_id", iRow)
                            wObj.setValue(str(nm).strip(), "name", iRow)
                            wObj.setValue(iRow + 1, "ordinal", iRow)
                            wObj.setValue("DrugBank", "provenance_source", iRow)
                            wObj.setValue("Brand Name", "type", iRow)
                            iRow += 1

            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))

        return False
