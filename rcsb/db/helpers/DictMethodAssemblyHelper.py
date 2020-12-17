##
# File:    DictMethodAssemblyHelper.py
# Author:  J. Westbrook
# Date:    16-Jul-2019
# Version: 0.001 Initial version
#
##
"""
Helper class implementing external assembly-level methods  supporting the RCSB dictionary extension.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import re
from collections import Counter

from mmcif.api.DataCategory import DataCategory

logger = logging.getLogger(__name__)


def cmpElements(lhs, rhs):
    return 0 if (lhs[-1].isdigit() or lhs[-1] in ["R", "S"]) and rhs[0].isdigit() else -1


class DictMethodAssemblyHelper(object):
    """Helper class implementing external assembly-level methods  supporting the RCSB dictionary extension."""

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (dict)  Placeholder for future key-value arguments

        """
        #
        self._raiseExceptions = kwargs.get("raiseExceptions", False)
        #
        rP = kwargs.get("resourceProvider")
        self.__commonU = rP.getResource("DictMethodCommonUtils instance") if rP else None
        self.__dApi = rP.getResource("Dictionary API instance (pdbx_core)") if rP else None
        #
        logger.debug("Dictionary method helper init")

    def echo(self, msg):
        logger.info(msg)

    def addAssemblyInfo(self, dataContainer, catName, **kwargs):
        """Build rcsb_assembly_info category.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        """
        logger.debug("Starting catName %s kwargs %r", catName, kwargs)
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("pdbx_struct_assembly")):
                return False
            logger.debug("%s beginning for %s", dataContainer.getName(), catName)
            # Create the new target category rcsb_assembly_info
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            #
            logger.debug("%s beginning for %s", dataContainer.getName(), catName)
            #
            # Get assembly comp details -
            #
            rD = self.__getAssemblyComposition(dataContainer)
            #
            cObj = dataContainer.getObj(catName)

            tObj = dataContainer.getObj("entry")
            entryId = tObj.getValue("id", 0)
            #
            tObj = dataContainer.getObj("pdbx_struct_assembly")
            assemblyIdL = tObj.getAttributeValueList("id")
            #
            #
            for ii, assemblyId in enumerate(assemblyIdL):
                if assemblyId not in rD["assemblyHeavyAtomCountByTypeD"]:
                    continue
                if assemblyId not in rD["assemblyHeavyAtomCountD"]:
                    continue
                dD = rD["assemblyHeavyAtomCountByTypeD"][assemblyId]
                #
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(assemblyId, "assembly_id", ii)
                #

                num = dD["polymer"] if "polymer" in dD else 0
                cObj.setValue(num, "polymer_atom_count", ii)

                num = dD["non-polymer"] if "non-polymer" in dD else 0
                cObj.setValue(num, "nonpolymer_atom_count", ii)

                num = dD["water"] if "water" in dD else 0
                cObj.setValue(num, "solvent_atom_count", ii)

                num = dD["branched"] if "branched" in dD else 0
                cObj.setValue(num, "branched_atom_count", ii)

                num = rD["assemblyHeavyAtomCountD"][assemblyId]
                cObj.setValue(num, "atom_count", ii)
                #
                num = rD["assemblyHydrogenAtomCountD"][assemblyId]
                cObj.setValue(num, "hydrogen_atom_count", ii)
                #
                num1 = rD["assemblyModeledMonomerCountD"][assemblyId]
                num2 = rD["assemblyUnmodeledMonomerCountD"][assemblyId]
                cObj.setValue(num1, "modeled_polymer_monomer_count", ii)
                cObj.setValue(num2, "unmodeled_polymer_monomer_count", ii)
                cObj.setValue(num1 + num2, "polymer_monomer_count", ii)
                #
                dD = rD["assemblyPolymerClassD"][assemblyId]
                cObj.setValue(dD["polymerCompClass"], "polymer_composition", ii)
                cObj.setValue(dD["subsetCompClass"], "selected_polymer_entity_types", ii)
                cObj.setValue(dD["naCompClass"], "na_polymer_entity_types", ii)
                #
                dD = rD["assemblyInstanceCountByTypeD"][assemblyId]
                num = dD["polymer"] if "polymer" in dD else 0
                cObj.setValue(num, "polymer_entity_instance_count", ii)
                #
                num = dD["non-polymer"] if "non-polymer" in dD else 0
                cObj.setValue(num, "nonpolymer_entity_instance_count", ii)
                #
                num = dD["branched"] if "branched" in dD else 0
                cObj.setValue(num, "branched_entity_instance_count", ii)
                #
                num = dD["water"] if "water" in dD else 0
                cObj.setValue(num, "solvent_entity_instance_count", ii)
                #
                dD = rD["assemblyInstanceCountByPolymerTypeD"][assemblyId]
                num = dD["Protein"] if "Protein" in dD else 0
                cObj.setValue(num, "polymer_entity_instance_count_protein", ii)
                num1 = dD["DNA"] if "DNA" in dD else 0
                cObj.setValue(num1, "polymer_entity_instance_count_DNA", ii)
                num2 = dD["RNA"] if "RNA" in dD else 0
                cObj.setValue(num2, "polymer_entity_instance_count_RNA", ii)
                cObj.setValue(num1 + num2, "polymer_entity_instance_count_nucleic_acid", ii)
                num = dD["NA-hybrid"] if "NA-hybrid" in dD else 0
                cObj.setValue(num, "polymer_entity_instance_count_nucleic_acid_hybrid", ii)
                #
                dD = rD["assemblyEntityCountByPolymerTypeD"][assemblyId]
                num = dD["Protein"] if "Protein" in dD else 0
                cObj.setValue(num, "polymer_entity_count_protein", ii)
                num1 = dD["DNA"] if "DNA" in dD else 0
                cObj.setValue(num1, "polymer_entity_count_DNA", ii)
                num2 = dD["RNA"] if "RNA" in dD else 0
                cObj.setValue(num2, "polymer_entity_count_RNA", ii)
                cObj.setValue(num1 + num2, "polymer_entity_count_nucleic_acid", ii)
                num = dD["NA-hybrid"] if "NA-hybrid" in dD else 0
                cObj.setValue(num, "polymer_entity_count_nucleic_acid_hybrid", ii)
                #
                dD = rD["assemblyEntityCountByTypeD"][assemblyId]
                num = dD["polymer"] if "polymer" in dD else 0
                cObj.setValue(num, "polymer_entity_count", ii)
                #
                num = dD["non-polymer"] if "non-polymer" in dD else 0
                cObj.setValue(num, "nonpolymer_entity_count", ii)
                #
                num = dD["branched"] if "branched" in dD else 0
                cObj.setValue(num, "branched_entity_count", ii)
                #
                num = dD["water"] if "water" in dD else 0
                cObj.setValue(num, "solvent_entity_count", ii)
            #
            return
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def buildContainerAssemblyIds(self, dataContainer, catName, **kwargs):
        """Build category rcsb_assembly_container_identifiers.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For example,

        loop_
        _rcsb_assembly_container_identifiers.entry_id
        _rcsb_assembly_container_identifiers.assembly_id
        ...


        """
        logger.debug("Starting catName %s kwargs %r", catName, kwargs)
        try:
            if not (dataContainer.exists("entry") and dataContainer.exists("pdbx_struct_assembly")):
                return False
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=self.__dApi.getAttributeNameList(catName)))
            #
            cObj = dataContainer.getObj(catName)

            tObj = dataContainer.getObj("entry")
            entryId = tObj.getValue("id", 0)
            cObj.setValue(entryId, "entry_id", 0)
            #
            tObj = dataContainer.getObj("pdbx_struct_assembly")
            assemblyIdL = tObj.getAttributeValueList("id")
            for ii, assemblyId in enumerate(assemblyIdL):
                cObj.setValue(entryId, "entry_id", ii)
                cObj.setValue(assemblyId, "assembly_id", ii)
                cObj.setValue(entryId + "-" + assemblyId, "rcsb_id", ii)

            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def addDepositedAssembly(self, dataContainer, catName, **kwargs):
        """Add the deposited coordinates as an additional separate assembly labeled as 'deposited'
        to categories, pdbx_struct_assembly and pdb_struct_assembly_gen.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        """
        logger.debug("Starting catName %s kwargs %r", catName, kwargs)
        try:
            if not dataContainer.exists("struct_asym"):
                return False
            if not dataContainer.exists("pdbx_struct_assembly"):
                dataContainer.append(
                    DataCategory(
                        "pdbx_struct_assembly",
                        attributeNameList=["id", "details", "method_details", "oligomeric_details", "oligomeric_count", "rcsb_details", "rcsb_candidate_assembly"],
                    )
                )
            if not dataContainer.exists("pdbx_struct_assembly_gen"):
                dataContainer.append(DataCategory("pdbx_struct_assembly_gen", attributeNameList=["assembly_id", "oper_expression", "asym_id_list", "ordinal"]))

            if not dataContainer.exists("pdbx_struct_oper_list"):
                row = [
                    "1",
                    "identity operation",
                    "1_555",
                    "x, y, z",
                    "1.0000000000",
                    "0.0000000000",
                    "0.0000000000",
                    "0.0000000000",
                    "0.0000000000",
                    "1.0000000000",
                    "0.0000000000",
                    "0.0000000000",
                    "0.0000000000",
                    "0.0000000000",
                    "1.0000000000",
                    "0.0000000000",
                ]
                atList = [
                    "id",
                    "type",
                    "name",
                    "symmetry_operation",
                    "matrix[1][1]",
                    "matrix[1][2]",
                    "matrix[1][3]",
                    "vector[1]",
                    "matrix[2][1]",
                    "matrix[2][2]",
                    "matrix[2][3]",
                    "vector[2]",
                    "matrix[3][1]",
                    "matrix[3][2]",
                    "matrix[3][3]",
                    "vector[3]",
                ]
                dataContainer.append(DataCategory("pdbx_struct_oper_list", attributeNameList=atList, rowList=[row]))

            #
            logger.debug("Add deposited assembly for %s", dataContainer.getName())
            cObj = dataContainer.getObj("struct_asym")
            asymIdL = cObj.getAttributeValueList("id")
            logger.debug("AsymIdL %r", asymIdL)
            #
            # Ordinal is added by subsequent attribure-level method.
            tObj = dataContainer.getObj("pdbx_struct_assembly_gen")
            rowIdx = tObj.getRowCount()
            tObj.setValue("deposited", "assembly_id", rowIdx)
            tObj.setValue("1", "oper_expression", rowIdx)
            tObj.setValue(",".join(asymIdL), "asym_id_list", rowIdx)
            #
            tObj = dataContainer.getObj("pdbx_struct_assembly")
            rowIdx = tObj.getRowCount()
            tObj.setValue("deposited", "id", rowIdx)
            tObj.setValue("deposited_coordinates", "details", rowIdx)
            #
            for atName in ["oligomeric_details", "method_details", "oligomeric_count"]:
                if tObj.hasAttribute(atName):
                    tObj.setValue("?", atName, rowIdx)
            #
            #
            #
            logger.debug("Full row is %r", tObj.getRow(rowIdx))
            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def filterAssemblyDetails(self, dataContainer, catName, **kwargs):
        """Filter _pdbx_struct_assembly.details -> _pdbx_struct_assembly.rcsb_details
            with a more limited vocabulary -


        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        For example, mapping to the following limited enumeration,

                'author_and_software_defined_assembly'
                'author_defined_assembly'
                'software_defined_assembly'

        """
        logger.debug("Starting catName %s kwargs %r", catName, kwargs)
        mD = {
            "author_and_software_defined_assembly": "author_and_software_defined_assembly",
            "author_defined_assembly": "author_defined_assembly",
            "complete icosahedral assembly": "author_and_software_defined_assembly",
            "complete point assembly": "author_and_software_defined_assembly",
            "crystal asymmetric unit": "software_defined_assembly",
            "crystal asymmetric unit, crystal frame": "software_defined_assembly",
            "details": "software_defined_assembly",
            "helical asymmetric unit": "software_defined_assembly",
            "helical asymmetric unit, std helical frame": "software_defined_assembly",
            "icosahedral 23 hexamer": "software_defined_assembly",
            "icosahedral asymmetric unit": "software_defined_assembly",
            "icosahedral asymmetric unit, std point frame": "software_defined_assembly",
            "icosahedral pentamer": "software_defined_assembly",
            "pentasymmetron capsid unit": "software_defined_assembly",
            "point asymmetric unit": "software_defined_assembly",
            "point asymmetric unit, std point frame": "software_defined_assembly",
            "representative helical assembly": "author_and_software_defined_assembly",
            "software_defined_assembly": "software_defined_assembly",
            "trisymmetron capsid unit": "software_defined_assembly",
            "deposited_coordinates": "software_defined_assembly",
        }
        #
        try:
            if not dataContainer.exists("pdbx_struct_assembly"):
                return False

            logger.debug("Filter assembly details for %s", dataContainer.getName())
            tObj = dataContainer.getObj("pdbx_struct_assembly")
            atName = "rcsb_details"
            if not tObj.hasAttribute(atName):
                tObj.appendAttribute(atName)
            #
            for iRow in range(tObj.getRowCount()):
                details = tObj.getValue("details", iRow)
                if details in mD:
                    tObj.setValue(mD[details], "rcsb_details", iRow)
                else:
                    tObj.setValue("software_defined_assembly", "rcsb_details", iRow)
                # logger.debug("Full row is %r", tObj.getRow(iRow))
            return True
        except Exception as e:
            logger.exception("For %s %s failing with %s", catName, atName, str(e))
        return False

    def assignAssemblyCandidates(self, dataContainer, catName, **kwargs):
        """Flag candidate biological assemblies as 'author_defined_assembly' ad author_and_software_defined_assembly'

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise

        """
        logger.debug("Starting catName %s kwargs %r", catName, kwargs)
        mD = {
            "author_and_software_defined_assembly": "author_and_software_defined_assembly",
            "author_defined_assembly": "author_defined_assembly",
            "complete icosahedral assembly": "author_and_software_defined_assembly",
            "complete point assembly": "author_and_software_defined_assembly",
            "crystal asymmetric unit": "software_defined_assembly",
            "crystal asymmetric unit, crystal frame": "software_defined_assembly",
            "details": "software_defined_assembly",
            "helical asymmetric unit": "software_defined_assembly",
            "helical asymmetric unit, std helical frame": "software_defined_assembly",
            "icosahedral 23 hexamer": "software_defined_assembly",
            "icosahedral asymmetric unit": "software_defined_assembly",
            "icosahedral asymmetric unit, std point frame": "software_defined_assembly",
            "icosahedral pentamer": "software_defined_assembly",
            "pentasymmetron capsid unit": "software_defined_assembly",
            "point asymmetric unit": "software_defined_assembly",
            "point asymmetric unit, std point frame": "software_defined_assembly",
            "representative helical assembly": "author_and_software_defined_assembly",
            "software_defined_assembly": "software_defined_assembly",
            "trisymmetron capsid unit": "software_defined_assembly",
            "deposited_coordinates": "software_defined_assembly",
        }
        #
        eD = {
            k: True
            for k in [
                "crystal asymmetric unit",
                "crystal asymmetric unit, crystal frame",
                "helical asymmetric unit",
                "helical asymmetric unit, std helical frame",
                "icosahedral 23 hexamer",
                "icosahedral asymmetric unit",
                "icosahedral asymmetric unit, std point frame",
                "icosahedral pentamer",
                "pentasymmetron capsid unit",
                "point asymmetric unit",
                "point asymmetric unit, std point frame",
                "trisymmetron capsid unit",
                "deposited_coordinates",
                "details",
            ]
        }
        try:
            if not dataContainer.exists("pdbx_struct_assembly"):
                return False
            atName = "rcsb_candidate_assembly"
            tObj = dataContainer.getObj("pdbx_struct_assembly")
            if not tObj.hasAttribute(atName):
                tObj.appendAttribute(atName)
            #
            for iRow in range(tObj.getRowCount()):
                details = tObj.getValue("details", iRow)
                if details in mD and details not in eD:
                    tObj.setValue("Y", "rcsb_candidate_assembly", iRow)
                else:
                    tObj.setValue("N", "rcsb_candidate_assembly", iRow)
                # logger.debug("Full row is %r", tObj.getRow(iRow))

            #
            return True
        except Exception as e:
            logger.exception("For %s %s failing with %s", catName, atName, str(e))
        return False

    def filterAssemblyCandidates(self, dataContainer, catName, **kwargs):
        """Filter assemblies to only candidates and deposited cases


        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name

        Returns:
            bool: True for success or False otherwise


        """
        logger.debug("Starting catName %s kwargs %r", catName, kwargs)
        try:
            if not dataContainer.exists("pdbx_struct_assembly"):
                return False

            logger.debug("Filter candidate assemblyfor %s", dataContainer.getName())
            tObj = dataContainer.getObj("pdbx_struct_assembly")
            #
            indexList = []
            for iRow in range(tObj.getRowCount()):
                isCandidate = tObj.getValue("rcsb_candidate_assembly", iRow) == "Y"
                isDeposited = tObj.getValue("id", iRow) == "deposited"

                if not (isCandidate or isDeposited):
                    indexList.append(iRow)
            tObj.removeRows(indexList)
            #
            # ---
            numAssemblies = tObj.getRowCount()
            logger.debug("Assembly count is %d", numAssemblies)
            if dataContainer.exists("rcsb_entry_info"):
                eiObj = dataContainer.getObj("rcsb_entry_info")
                eiObj.setValue(numAssemblies, "assembly_count", 0)
            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s", catName, str(e))
        return False

    def __expandOperatorList(self, operExpression):
        """
        Operation expressions may have the forms:

                (1)        the single operation 1
                (1,2,5)    the operations 1, 2, 5
                (1-4)      the operations 1,2,3 and 4
                (1,2)(3,4) the combinations of operations
                           3 and 4 followed by 1 and 2 (i.e.
                           the cartesian product of parenthetical
                           groups applied from right to left)
        """

        rL = []
        opCount = 1
        try:
            if operExpression.find("(") < 0:
                opL = [operExpression]
            else:
                opL = [tV.strip().strip("(").rstrip(")") for tV in re.findall(r"\(.*?\)", operExpression)]
            #
            for op in opL:
                teL = []
                tL = op.split(",")
                for tV in tL:
                    trngL = tV.split("-")
                    if len(trngL) == 2:
                        rngL = [str(r) for r in range(int(trngL[0]), int(trngL[1]) + 1)]
                    else:
                        rngL = trngL
                    teL.extend(rngL)
                rL.append(teL)
                opCount *= len(teL)

        except Exception as e:
            logger.exception("Failing parsing %r with %s", operExpression, str(e))
        #
        if not rL:
            opCount = 0
        return opCount, rL

    def __getAssemblyComposition(self, dataContainer):
        """Return assembly composition by entity and instance type counts.

        Example -
            loop_
            _pdbx_struct_assembly.id
            _pdbx_struct_assembly.details
            _pdbx_struct_assembly.method_details
            _pdbx_struct_assembly.oligomeric_details
            _pdbx_struct_assembly.oligomeric_count
            1 'complete icosahedral assembly'                ? 180-meric      180
            2 'icosahedral asymmetric unit'                  ? trimeric       3
            3 'icosahedral pentamer'                         ? pentadecameric 15
            4 'icosahedral 23 hexamer'                       ? octadecameric  18
            5 'icosahedral asymmetric unit, std point frame' ? trimeric       3
            #
            loop_
            _pdbx_struct_assembly_gen.assembly_id
            _pdbx_struct_assembly_gen.oper_expression
            _pdbx_struct_assembly_gen.asym_id_list
            1 '(1-60)'           A,B,C
            2 1                  A,B,C
            3 '(1-5)'            A,B,C
            4 '(1,2,6,10,23,24)' A,B,C
            5 P                  A,B,C
            #
        """
        #
        instanceTypeD = self.__commonU.getInstanceTypes(dataContainer)
        instancePolymerTypeD = self.__commonU.getInstancePolymerTypes(dataContainer)
        instEntityD = self.__commonU.getInstanceEntityMap(dataContainer)
        #
        epTypeD = self.__commonU.getEntityPolymerTypes(dataContainer)
        eTypeD = self.__commonU.getEntityTypes(dataContainer)
        epTypeFilteredD = self.__commonU.getPolymerEntityFilteredTypes(dataContainer)
        # JDW
        instHeavyAtomCount = self.__commonU.getInstanceHeavyAtomCounts(dataContainer, modelId="1")
        instHydrogenAtomCount = self.__commonU.getInstanceHydrogenAtomCounts(dataContainer, modelId="1")
        #
        instModeledMonomerCount = self.__commonU.getInstanceModeledMonomerCounts(dataContainer, modelId="1")
        instUnmodeledMonomerCount = self.__commonU.getInstanceUnModeledMonomerCounts(dataContainer, modelId="1")
        # -------------------------
        assemblyInstanceCountByTypeD = {}
        assemblyHeavyAtomCountByTypeD = {}
        assemblyHeavyAtomCountD = {}
        assemblyHydrogenAtomCountD = {}
        assemblyModeledMonomerCountD = {}
        assemblyUnmodeledMonomerCountD = {}
        # Pre-generation (source instances)
        assemblyInstanceD = {}
        # Post-generation (gerated instances)
        assemblyInstanceGenD = {}
        assemblyInstanceCountByPolymerTypeD = {}
        assemblyPolymerInstanceCountD = {}
        assemblyPolymerClassD = {}
        #
        assemblyEntityCountByPolymerTypeD = {}
        assemblyEntityCountByTypeD = {}
        # --------------
        #
        try:
            if dataContainer.exists("pdbx_struct_assembly_gen"):
                tObj = dataContainer.getObj("pdbx_struct_assembly_gen")
                for ii in range(tObj.getRowCount()):
                    assemblyId = tObj.getValue("assembly_id", ii)
                    # Initialize instances count
                    if assemblyId not in assemblyInstanceCountByTypeD:
                        assemblyInstanceCountByTypeD[assemblyId] = {eType: 0 for eType in ["polymer", "non-polymer", "branched", "macrolide", "water"]}
                    if assemblyId not in assemblyHeavyAtomCountByTypeD:
                        assemblyHeavyAtomCountByTypeD[assemblyId] = {eType: 0 for eType in ["polymer", "non-polymer", "branched", "macrolide", "water"]}
                    if assemblyId not in assemblyModeledMonomerCountD:
                        assemblyModeledMonomerCountD[assemblyId] = 0
                    if assemblyId not in assemblyUnmodeledMonomerCountD:
                        assemblyUnmodeledMonomerCountD[assemblyId] = 0
                    if assemblyId not in assemblyHeavyAtomCountD:
                        assemblyHeavyAtomCountD[assemblyId] = 0
                    if assemblyId not in assemblyHydrogenAtomCountD:
                        assemblyHydrogenAtomCountD[assemblyId] = 0
                    #
                    opExpression = tObj.getValue("oper_expression", ii)
                    opCount, opL = self.__expandOperatorList(opExpression)
                    tS = tObj.getValue("asym_id_list", ii)
                    asymIdList = [t.strip() for t in tS.strip().split(",")]
                    assemblyInstanceD.setdefault(assemblyId, []).extend(asymIdList)
                    assemblyInstanceGenD.setdefault(assemblyId, []).extend(asymIdList * opCount)
                    #
                    logger.debug("%s assembly %r opExpression %r opCount %d opL %r", dataContainer.getName(), assemblyId, opExpression, opCount, opL)
                    logger.debug("%s assembly %r length asymIdList %r", dataContainer.getName(), assemblyId, len(asymIdList))
                    #
                    for eType in ["polymer", "non-polymer", "branched", "macrolide", "water"]:
                        iList = [asymId for asymId in asymIdList if asymId in instanceTypeD and instanceTypeD[asymId] == eType]
                        assemblyInstanceCountByTypeD[assemblyId][eType] += len(iList) * opCount
                        #
                        atCountList = [
                            instHeavyAtomCount[asymId] for asymId in asymIdList if asymId in instanceTypeD and instanceTypeD[asymId] == eType and asymId in instHeavyAtomCount
                        ]
                        assemblyHeavyAtomCountByTypeD[assemblyId][eType] += sum(atCountList) * opCount
                        assemblyHeavyAtomCountD[assemblyId] += sum(atCountList) * opCount
                        #
                        hAtCountList = [
                            instHydrogenAtomCount[asymId] for asymId in asymIdList if asymId in instanceTypeD and instanceTypeD[asymId] == eType and asymId in instHydrogenAtomCount
                        ]
                        assemblyHydrogenAtomCountD[assemblyId] += sum(hAtCountList) * opCount
                    #
                    modeledMonomerCountList = [
                        instModeledMonomerCount[asymId]
                        for asymId in asymIdList
                        if asymId in instanceTypeD and instanceTypeD[asymId] == "polymer" and asymId in instModeledMonomerCount
                    ]
                    assemblyModeledMonomerCountD[assemblyId] += sum(modeledMonomerCountList) * opCount
                    #
                    unmodeledMonomerCountList = [
                        instUnmodeledMonomerCount[asymId]
                        for asymId in asymIdList
                        if asymId in instanceTypeD and instanceTypeD[asymId] == "polymer" and asymId in instUnmodeledMonomerCount
                    ]
                    assemblyUnmodeledMonomerCountD[assemblyId] += sum(unmodeledMonomerCountList) * opCount

                #
                assemblyInstanceCountByPolymerTypeD = {}
                assemblyPolymerInstanceCountD = {}
                assemblyPolymerClassD = {}
                #
                assemblyEntityCountByPolymerTypeD = {}
                assemblyEntityCountByTypeD = {}
                #
                # Using the generated list of instance assembly components ...
                for assemblyId, asymIdList in assemblyInstanceGenD.items():
                    # ------
                    #  Instance polymer composition
                    pInstTypeList = [instancePolymerTypeD[asymId] for asymId in asymIdList if asymId in instancePolymerTypeD]
                    pInstTypeD = Counter(pInstTypeList)
                    assemblyInstanceCountByPolymerTypeD[assemblyId] = {pType: 0 for pType in ["Protein", "DNA", "RNA", "NA-hybrid", "Other"]}
                    assemblyInstanceCountByPolymerTypeD[assemblyId] = {pType: pInstTypeD[pType] for pType in ["Protein", "DNA", "RNA", "NA-hybrid", "Other"] if pType in pInstTypeD}
                    assemblyPolymerInstanceCountD[assemblyId] = len(pInstTypeList)
                    #
                    logger.debug("%s assemblyId %r pInstTypeD %r", dataContainer.getName(), assemblyId, pInstTypeD.items())

                    # -------------
                    # Entity and polymer entity composition
                    #
                    entityIdList = list(set([instEntityD[asymId] for asymId in asymIdList if asymId in instEntityD]))
                    pTypeL = [epTypeD[entityId] for entityId in entityIdList if entityId in epTypeD]
                    #
                    polymerCompClass, subsetCompClass, naCompClass, _ = self.__commonU.getPolymerComposition(pTypeL)
                    assemblyPolymerClassD[assemblyId] = {"polymerCompClass": polymerCompClass, "subsetCompClass": subsetCompClass, "naCompClass": naCompClass}
                    #
                    logger.debug(
                        "%s assemblyId %s polymerCompClass %r subsetCompClass %r naCompClass %r pTypeL %r",
                        dataContainer.getName(),
                        assemblyId,
                        polymerCompClass,
                        subsetCompClass,
                        naCompClass,
                        pTypeL,
                    )
                    pTypeFilteredL = [epTypeFilteredD[entityId] for entityId in entityIdList if entityId in epTypeFilteredD]
                    #
                    pEntityTypeD = Counter(pTypeFilteredL)
                    assemblyEntityCountByPolymerTypeD[assemblyId] = {pType: 0 for pType in ["Protein", "DNA", "RNA", "NA-hybrid", "Other"]}
                    assemblyEntityCountByPolymerTypeD[assemblyId] = {
                        pType: pEntityTypeD[pType] for pType in ["Protein", "DNA", "RNA", "NA-hybrid", "Other"] if pType in pEntityTypeD
                    }
                    #
                    eTypeL = [eTypeD[entityId] for entityId in entityIdList if entityId in eTypeD]
                    entityTypeD = Counter(eTypeL)
                    assemblyEntityCountByTypeD[assemblyId] = {eType: 0 for eType in ["polymer", "non-polymer", "branched", "macrolide", "water"]}
                    assemblyEntityCountByTypeD[assemblyId] = {
                        eType: entityTypeD[eType] for eType in ["polymer", "non-polymer", "branched", "macrolide", "water"] if eType in entityTypeD
                    }
                    #
                    # ---------------
                    #
            #
            logger.debug("%s assemblyInstanceCountByTypeD %r", dataContainer.getName(), assemblyInstanceCountByTypeD.items())
            logger.debug("%s assemblyHeavyAtomCountByTypeD %r", dataContainer.getName(), assemblyHeavyAtomCountByTypeD.items())
            logger.debug("%s assemblyHeavyAtomCountD %r", dataContainer.getName(), assemblyHeavyAtomCountD.items())
            logger.debug("%s assemblyHydrogenAtomCountD %r", dataContainer.getName(), assemblyHydrogenAtomCountD.items())
            logger.debug("%s assemblyModeledMonomerCountD %r", dataContainer.getName(), assemblyModeledMonomerCountD.items())
            logger.debug("%s assemblyUnmodeledMonomerCountD %r", dataContainer.getName(), assemblyUnmodeledMonomerCountD.items())
            logger.debug("%s assemblyPolymerClassD %r", dataContainer.getName(), assemblyPolymerClassD.items())
            logger.debug("%s assemblyPolymerInstanceCountD %r", dataContainer.getName(), assemblyPolymerInstanceCountD.items())
            logger.debug("%s assemblyInstanceCountByPolymerTypeD %r", dataContainer.getName(), assemblyInstanceCountByPolymerTypeD.items())
            logger.debug("%s assemblyEntityCountByPolymerTypeD %r", dataContainer.getName(), assemblyEntityCountByPolymerTypeD.items())
            logger.debug("%s assemblyEntityCountByTypeD %r", dataContainer.getName(), assemblyEntityCountByTypeD.items())
            #
            rD = {
                "assemblyInstanceCountByTypeD": assemblyInstanceCountByTypeD,
                "assemblyHeavyAtomCountByTypeD": assemblyHeavyAtomCountByTypeD,
                "assemblyHeavyAtomCountD": assemblyHeavyAtomCountD,
                "assemblyHydrogenAtomCountD": assemblyHydrogenAtomCountD,
                "assemblyModeledMonomerCountD": assemblyModeledMonomerCountD,
                "assemblyUnmodeledMonomerCountD": assemblyUnmodeledMonomerCountD,
                "assemblyInstanceCountByPolymerTypeD": assemblyInstanceCountByPolymerTypeD,
                "assemblyPolymerInstanceCountD": assemblyPolymerInstanceCountD,
                "assemblyPolymerClassD": assemblyPolymerClassD,
                "assemblyEntityCountByPolymerTypeD": assemblyEntityCountByPolymerTypeD,
                "assemblyEntityCountByTypeD": assemblyEntityCountByTypeD,
            }
        except Exception as e:
            logger.exception("Failing %s with %s", dataContainer.getName(), str(e))
        return rD
