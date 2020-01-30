##
# File:    DictMethodCommonUtils.py
# Author:  J. Westbrook
# Date:    16-Jul-2019
# Version: 0.001 Initial version
#
# Updates:
# 26-Jul-2019 jdw Include struct_mon_prot_cis with secondary structure features
#                 Add general processing of intermolecular and other connections.
# 19-Sep-2019 jdw Add method getEntityReferenceAlignments()
# 13-Oct-2019 jdw add isoform support
##
"""
Helper class implements common utility external method references supporting the RCSB dictionary extension.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

# pylint: disable=too-many-lines

import datetime
import itertools
import logging
import re
import sys
from collections import OrderedDict, namedtuple

from rcsb.utils.io.CacheUtils import CacheUtils

logger = logging.getLogger(__name__)

OutlierValueFields = ("compId", "seqId", "outlierType", "description", "reported", "reference", "uncertaintyValue", "uncertaintyType")
OutlierValue = namedtuple("OutlierValue", OutlierValueFields, defaults=(None,) * len(OutlierValueFields))

BoundEntityFields = ("targetCompId", "connectType", "partnerCompId", "partnerEntityId", "partnerEntityType")
NonpolymerBoundEntity = namedtuple("NonpolymerBoundEntity", BoundEntityFields, defaults=(None,) * len(BoundEntityFields))

BoundInstanceFields = ("targetCompId", "connectType", "partnerCompId", "partnerAsymId", "partnerEntityType", "bondDistance", "bondOrder")
NonpolymerBoundInstance = namedtuple("NonpolymerBoundInstance", BoundInstanceFields, defaults=(None,) * len(BoundInstanceFields))


class DictMethodCommonUtils(object):
    """  Helper class implements common utility external method references supporting the RCSB dictionary extension.
    """

    # Dictionary of current standard monomers -
    aaDict3 = {
        "ALA": "A",
        "ARG": "R",
        "ASN": "N",
        "ASP": "D",
        "ASX": "B",
        "CYS": "C",
        "GLN": "Q",
        "GLU": "E",
        "GLX": "Z",
        "GLY": "G",
        "HIS": "H",
        "ILE": "I",
        "LEU": "L",
        "LYS": "K",
        "MET": "M",
        "PHE": "F",
        "PRO": "P",
        "SER": "S",
        "THR": "T",
        "TRP": "W",
        "TYR": "Y",
        "VAL": "V",
        "PYL": "O",
        "SEC": "U",
    }
    dnaDict3 = {"DA": "A", "DC": "C", "DG": "G", "DT": "T", "DU": "U", "DI": "I"}
    rnaDict3 = {"A": "A", "C": "C", "G": "G", "I": "I", "N": "N", "T": "T", "U": "U"}
    # "UNK": "X",
    # "MSE":"M",
    # ".": "."
    monDict3 = {**aaDict3, **dnaDict3, **rnaDict3}

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (dict)  Placeholder for future key-value arguments

        """
        #
        self._raiseExceptions = kwargs.get("raiseExceptions", False)
        self.__wsPattern = re.compile(r"\s+", flags=re.UNICODE | re.MULTILINE)
        self.__reNonDigit = re.compile(r"[^\d]+")
        #
        cacheSize = 5
        self.__entityAndInstanceMapCache = CacheUtils(size=cacheSize, label="instance mapping")
        self.__atomInfoCache = CacheUtils(size=cacheSize, label="atom site counts and mapping")
        self.__protSSCache = CacheUtils(size=cacheSize, label="protein secondary structure")
        self.__instanceConnectionCache = CacheUtils(size=cacheSize, label="instance connections")
        self.__entitySequenceFeatureCache = CacheUtils(size=cacheSize, label="entity sequence features")
        self.__instanceSiteInfoCache = CacheUtils(size=cacheSize, label="instance site details")
        self.__instanceUnobservedCache = CacheUtils(size=cacheSize, label="instance unobserved details")
        self.__modelOutliersCache = CacheUtils(size=cacheSize, label="model outlier details")
        #
        logger.debug("Dictionary common utilities init")

    def echo(self, msg):
        logger.info(msg)

    def testCache(self):
        return True

    def isFloat(self, val):
        try:
            float(val)
        except Exception:
            return False
        return True

    def __fetchEntityAndInstanceTypes(self, dataContainer):
        wD = self.__entityAndInstanceMapCache.get(dataContainer.getName())
        if not wD:
            wD = self.__getEntityAndInstanceTypes(dataContainer)
            self.__entityAndInstanceMapCache.set(dataContainer.getName(), wD)
        return wD

    def getFormulaWeightNonSolvent(self, dataContainer):
        """Return a formula weight of the non-solvent entities in the deposited entry.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            float: formula weight (kilodaltons)
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["fwNonSolvent"] if "fwNonSolvent" in wD else {}

    def getInstancePolymerTypes(self, dataContainer):
        """Return a dictionary of polymer types for each polymer instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'asymId': <dictionary polymer type>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["instancePolymerTypeD"] if "instancePolymerTypeD" in wD else {}

    def getInstanceTypes(self, dataContainer):
        """Return a dictionary of entity types for each entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'asymId': <entity type>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["instanceTypeD"] if "instanceTypeD" in wD else {}

    def getInstanceTypeCounts(self, dataContainer):
        """Return a dictionary of the counts entity types for each entity type.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'entity type': <# of instances>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["instanceTypeCountD"] if "instanceTypeCountD" in wD else {}

    def getInstanceEntityMap(self, dataContainer):
        """Return a dictionary of entities corresponding to each entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'asymId': <entity id>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["instEntityD"] if "instEntityD" in wD else {}

    def getEntityPolymerTypes(self, dataContainer):
        """Return a dictionary of polymer types for each polymer entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'entityId': <dictionary polymer types>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["epTypeD"] if "epTypeD" in wD else {}

    def getEntityTypes(self, dataContainer):
        """Return a dictionary of entity types for each entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'entityId': <dictionary entity types>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["eTypeD"] if "eTypeD" in wD else {}

    def getPolymerEntityFilteredTypes(self, dataContainer):
        """Return a dictionary of filtered entity polymer types for each polymer entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'entityId': <filtered entity polymer types>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["epTypeFilteredD"] if "epTypeFilteredD" in wD else {}

    def getPolymerEntityLengths(self, dataContainer):
        """Return a dictionary of entity polymer lengths for each polymer entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'entityId': <monomer length>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["epLengthD"] if "epLengthD" in wD else {}

    def getPolymerEntityLengthsEnumerated(self, dataContainer):
        """Return a dictionary of entity polymer lengths for each polymer entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'entityId': <monomer length>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["entityPolymerLengthD"] if "entityPolymerLengthD" in wD else {}

    def getPolymerEntityMonomerCounts(self, dataContainer):
        """Return a dictionary of monomer counts for each polymer entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'entityId': {'compId': <monomer count>, ... }}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["entityPolymerMonomerCountD"] if "entityPolymerMonomerCountD" in wD else {}

    def getPolymerEntityModifiedMonomers(self, dataContainer):
        """Return a dictionary of nonstandard monomers for each polymer entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'entityId': [mod_comp_id, mod_comp_id,...]}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["entityPolymerModifiedMonomers"] if "entityPolymerModifiedMonomers" in wD else {}

    def getPolymerModifiedMonomerFeatures(self, dataContainer):
        """Return a dictionary of nonstandard monomer features.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: [(entityId, seqId, compId, 'modified_monomer')] = set(compId)

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["seqModMonomerFeatureD"] if "seqModMonomerFeatureD" in wD else {}

    def getEntityPolymerLengthBounds(self, dataContainer):
        """Return a dictionary of polymer lenght bounds by entity type.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            tuple: (minLen, maxLen)
        """
        if not dataContainer or not dataContainer.getName():
            return ()
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["entityPolymerLenghtBounds"] if "entityPolymerLenghtBounds" in wD else ()

    def getEntityFormulaWeightBounds(self, dataContainer):
        """Return a dictionary of formula weight bounds by entity type.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: [entityType] = (minFw, maxFw)
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["fwTypeBoundD"] if "fwTypeBoundD" in wD else {}

    def getTargetComponents(self, dataContainer):
        """ Return a components targets.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            list: [compId, compId,...]
        """
        if not dataContainer or not dataContainer.getName():
            return []
        wD = self.__fetchEntityAndInstanceTypes(dataContainer)
        return wD["ccTargets"] if "ccTargets" in wD else []

    def __getEntityAndInstanceTypes(self, dataContainer):
        """ Internal method to collect and return entity/instance type, size and mapping information.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            (dict) : Return dictionary of entity types, type counts and polymer type (where applicable) for
                     each instance in the deposited unit.

            Type and count contents:

              instanceTypeD[asymId] = <entity_type>
              instanceTypeCountD[<entity_type>] = #
              instancePolymerTypeD[asymId] = <filtered polymer type>
              eTypeD[entityId] = <dictionary entity type>
              instEntityD[asymId] = entityId
              epTypeD[entityId] = <dictionary polymer type>
              epTypeFilteredD[entityId] = <dictionary polymer type>
              epLengthD[entityId] = polymer monomer length (from one-letter-code)
              entityPolymerLengthD[entityId] = polymer monomer length (from enumerated sequence)
              entityPolymerMonomerCountD[entityId][compId] = mononer count
              entityPolymerModifiedMonomers[entity]=[mod compId, mod compId]
              seqModMonomerFeatureD[(entityId, seqId, compId, 'modified_monomer')] = set(compId)
              fwNonSolvent = float value (kilodaltons)
              fwTypeBoundD[entityType] = (minFw, maxFw)
              entityPolymerLenghtBounds = (minL, maxL)
              ccTargets = [compId, compId]
        """
        rD = {}
        #
        try:
            #
            if not dataContainer.exists("entity") or not dataContainer.exists("struct_asym"):
                return {}
            eFwD = {}
            instanceTypeD = {}
            instancePolymerTypeD = {}
            instanceTypeCountD = {}
            #
            eObj = dataContainer.getObj("entity")
            eTypeD = {}
            for ii in range(eObj.getRowCount()):
                # logger.info("Attribute %r %r" % (ii, eObj.getAttributeList()))
                entityId = eObj.getValue("id", ii)
                eType = eObj.getValue("type", ii)
                eTypeD[entityId] = eType
                fw = eObj.getValue("formula_weight", ii)
                eFwD[entityId] = float(fw) if fw and fw not in [".", "?"] else 0.0
            #
            epTypeD = {}
            epLengthD = {}
            epTypeFilteredD = {}
            hasEntityPoly = False
            if dataContainer.exists("entity_poly"):
                hasEntityPoly = True
                epObj = dataContainer.getObj("entity_poly")
                for ii in range(epObj.getRowCount()):
                    entityId = epObj.getValue("entity_id", ii)
                    pType = epObj.getValue("type", ii)
                    epTypeFilteredD[entityId] = self.filterEntityPolyType(pType)
                    epTypeD[entityId] = pType
                    if epObj.hasAttribute("pdbx_seq_one_letter_code_can"):
                        sampleSeq = self.__stripWhiteSpace(epObj.getValue("pdbx_seq_one_letter_code_can", ii))
                        epLengthD[entityId] = len(sampleSeq) if sampleSeq and sampleSeq not in ["?", "."] else None

            #
            seqModMonomerFeatureD = {}
            entityPolymerMonomerCountD = {}
            entityPolymerLengthD = {}
            hasEntityPolySeq = False
            if dataContainer.exists("entity_poly_seq"):
                epsObj = dataContainer.getObj("entity_poly_seq")
                hasEntityPolySeq = True
                tSeqD = {}
                for ii in range(epsObj.getRowCount()):
                    entityId = epsObj.getValue("entity_id", ii)
                    seqNum = epsObj.getValue("num", ii)
                    compId = epsObj.getValue("mon_id", ii)
                    if compId not in DictMethodCommonUtils.monDict3:
                        seqModMonomerFeatureD.setdefault((entityId, seqNum, compId, "modified_monomer"), set()).add(compId)
                    # handle heterogeneity with the entityId,seqNum tuple
                    tSeqD.setdefault(entityId, set()).add((entityId, seqNum))
                    if entityId not in entityPolymerMonomerCountD:
                        entityPolymerMonomerCountD[entityId] = {}
                    entityPolymerMonomerCountD[entityId][compId] = entityPolymerMonomerCountD[entityId][compId] + 1 if compId in entityPolymerMonomerCountD[entityId] else 1
                #
                entityPolymerLengthD = {entityId: len(tSet) for entityId, tSet in tSeqD.items()}
            #
            if not hasEntityPoly and hasEntityPolySeq:
                for entityId, eType in eTypeD.items():
                    if eType in ["polymer"]:
                        monomerL = epsObj.selectValuesWhere("mon_id", entityId, "entity_id")
                        pType, fpType = self.guessEntityPolyTypes(monomerL)
                        epTypeFilteredD[entityId] = fpType
                        epTypeD[entityId] = pType
                        epLengthD[entityId] = len(monomerL)

            entityPolymerModifiedMonomers = {}
            for entityId, cD in entityPolymerMonomerCountD.items():
                tL = []
                for compId, _ in cD.items():
                    modFlag = "N" if compId in DictMethodCommonUtils.monDict3 else "Y"
                    if modFlag == "Y":
                        tL.append(compId)
                entityPolymerModifiedMonomers[entityId] = sorted(set(tL))
            #
            logger.debug("%s entityPolymerModifiedMonomers %r", dataContainer.getName(), entityPolymerModifiedMonomers)
            #  Add branched here
            #
            instEntityD = {}
            sObj = dataContainer.getObj("struct_asym")
            for ii in range(sObj.getRowCount()):
                entityId = sObj.getValue("entity_id", ii)
                asymId = sObj.getValue("id", ii)
                instEntityD[asymId] = entityId
                if entityId in eTypeD:
                    instanceTypeD[asymId] = eTypeD[entityId]
                else:
                    logger.warning("Missing entity id entry %r asymId %r entityId %r", dataContainer.getName(), entityId, asymId)
                if entityId in epTypeD:
                    instancePolymerTypeD[asymId] = epTypeFilteredD[entityId]
                #
            #
            # Count the instance by type - initialize all types
            #
            instanceTypeCountD = {k: 0 for k in ["polymer", "non-polymer", "branched", "macrolide", "water"]}
            for asymId, eType in instanceTypeD.items():
                instanceTypeCountD[eType] += 1
            #
            # Compute the total weight of polymer and non-polymer instances (full entities) - (kilodaltons)
            #
            fwNonSolvent = 0.0
            for asymId, eType in instanceTypeD.items():
                if eType not in ["water"]:
                    entityId = instEntityD[asymId]
                    fwNonSolvent += eFwD[entityId]
            fwNonSolvent = fwNonSolvent / 1000.0
            #
            # Get ligand of interest.
            #
            ccTargets = []
            if dataContainer.exists("pdbx_entity_instance_feature"):
                ifObj = dataContainer.getObj("pdbx_entity_instance_feature")
                for ii in range(ifObj.getRowCount()):
                    compId = ifObj.getValue("comp_id", ii)
                    ft = ifObj.getValue("feature_type", ii)
                    if ft.upper() in ["SUBJECT OF INVESTIGATION"]:
                        ccTargets.append(compId)
            #
            #
            fwTypeBoundD = {}
            tBoundD = {et: {"min": float("inf"), "max": -1.0} for eId, et in eTypeD.items()}
            for entityId, fw in eFwD.items():
                fw = fw / 1000.0
                eType = eTypeD[entityId]
                tBoundD[eType]["min"] = fw if fw < tBoundD[eType]["min"] else tBoundD[eType]["min"]
                tBoundD[eType]["max"] = fw if fw > tBoundD[eType]["max"] else tBoundD[eType]["max"]
            for eType in tBoundD:
                if tBoundD[eType]["min"] > 0.00000001:
                    fwTypeBoundD[eType] = tBoundD[eType]
            #

            entityPolymerLenghtBounds = None
            maxL = -1
            minL = sys.maxsize
            if epLengthD:
                for entityId, pLen in epLengthD.items():
                    minL = pLen if pLen < minL else minL
                    maxL = pLen if pLen > maxL else maxL
                entityPolymerLenghtBounds = (minL, maxL)
            #

            rD = {
                "instanceTypeD": instanceTypeD,
                "instancePolymerTypeD": instancePolymerTypeD,
                "instanceTypeCountD": instanceTypeCountD,
                "instEntityD": instEntityD,
                "eTypeD": eTypeD,
                "epLengthD": epLengthD,
                "epTypeD": epTypeD,
                "epTypeFilteredD": epTypeFilteredD,
                "entityPolymerMonomerCountD": entityPolymerMonomerCountD,
                "entityPolymerLengthD": entityPolymerLengthD,
                "entityPolymerModifiedMonomers": entityPolymerModifiedMonomers,
                "seqModMonomerFeatureD": seqModMonomerFeatureD,
                "fwNonSolvent": fwNonSolvent,
                "fwTypeBoundD": fwTypeBoundD,
                "entityPolymerLenghtBounds": entityPolymerLenghtBounds,
                "ccTargets": ccTargets,
            }
            logger.debug("%s length struct_asym %d (%d) instanceTypeD %r", dataContainer.getName(), sObj.getRowCount(), len(instanceTypeD), instanceTypeD)
        #
        except Exception as e:
            logger.exception("Failing with %r with %r", dataContainer.getName(), str(e))
        #
        return rD

    def getAsymAuthIdMap(self, dataContainer):
        """Return a dictionary of mapping between asymId and authAsymId.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {'asymId': authAsymId, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer)
        return wD["asymAuthIdD"] if "asymAuthIdD" in wD else {}

    def getInstanceAtomCounts(self, dataContainer, modelId="1"):
        """Return a dictionary of deposited atom counts for each entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance
            modelId (str, optional): model index. Defaults to "1".


        Returns:
            dict: {'asymId': <# of deposited atoms>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer, modelId=modelId)
        return wD["instanceAtomCountD"] if "instanceAtomCountD" in wD else {}

    def getEntityTypeAtomCounts(self, dataContainer, modelId="1"):
        """Return a dictionary of deposited atom counts for each entity type.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance
            modelId (str, optional): model index. Defaults to "1".

        Returns:
            dict: {'entity type': <# of deposited atoms>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer, modelId=modelId)
        return wD["typeAtomCountD"] if "typeAtomCountD" in wD else {}

    def getInstanceModeledMonomerCounts(self, dataContainer, modelId="1"):
        """Return a dictionary of deposited modeled monomer counts for each entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance
            modelId (str, optional): model index. Defaults to "1".

        Returns:
            dict: {'asymId': <# of deposited modeled monomers>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer, modelId=modelId)
        return wD["instancePolymerModeledMonomerCountD"] if "instancePolymerModeledMonomerCountD" in wD else {}

    def getInstanceUnModeledMonomerCounts(self, dataContainer, modelId="1"):
        """Return a dictionary of deposited unmodeled monomer counts for each entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance
            modelId (str, optional): model index. Defaults to "1".

        Returns:
            dict: {'asymId': <# of deposited unmodeled mononmers>, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer, modelId=modelId)
        return wD["instancePolymerUnmodeledMonomerCountD"] if "instancePolymerUnmodeledMonomerCountD" in wD else {}

    def getDepositedMonomerCounts(self, dataContainer, modelId="1"):
        """Return deposited modeled and unmodeled polymer monomer counts for the input modelid.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance
            modelId (str, optional): model index. Defaults to "1".


        Returns:
            (int,int):  modeled and unmodeled monomer counts
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer, modelId=modelId)
        modeledCount = sum(wD["instancePolymerModeledMonomerCountD"].values())
        unModeledCount = sum(wD["instancePolymerUnmodeledMonomerCountD"].values())
        return modeledCount, unModeledCount

    def getDepositedAtomCounts(self, dataContainer, modelId="1"):
        """Return the number of deposited atoms in the input model, the total deposited atom
        and the total model count.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance
            modelId (str, optional): model index. Defaults to "1".

        Returns:
            (int, int, int)  deposited atoms in input model, total deposited atom count, and total deposited model count
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer, modelId=modelId)
        numAtomsModel = wD["numAtomsModel"] if "numAtomsModel" in wD else 0
        numAtomsTotal = wD["numAtomsAll"] if "numAtomsAll" in wD else 0
        numModelsTotal = wD["numModels"] if "numModels" in wD else 0
        return numAtomsModel, numAtomsTotal, numModelsTotal

    def getInstancePolymerRanges(self, dataContainer):
        """Return a dictionary of polymer residue range and length for each entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {"asymId": , {"sampleSeqLen": sampleSeqLen,
                                "obsSeqLen": obsSeqLen,
                                "begSeqId": begSeqId,
                                "endSeqId": endSeqId,
                                "begAuthSeqId": begAuthSeqId,
                                "endAuthSeqId": endAuthSeqId,
                                "begInsCode": begAuthInsCode,
                                "endInsCode": endAuthInsCode,}...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer)
        return wD["asymIdPolymerRangesD"] if "asymIdPolymerRangesD" in wD else {}

    def getInstanceIdMap(self, dataContainer):
        """Return a dictionary of cardinal identifiers for each entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {"asymId":  {"entry_id": entryId,
                                "entity_id": entityId,
                                "entity_type": entityTypeD[entityId],
                                "asym_id": asymId,
                                "auth_asym_id": authAsymId,
                                "comp_id": monId,
                                "auth_seq_id": "?",}, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer)
        return wD["instanceIdMapD"] if "instanceIdMapD" in wD else {}

    def getNonPolymerIdMap(self, dataContainer):
        """Return a dictionary of cardinal identifiers for each non-polymer entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {(authAsymId, resNum):   {"entry_id": entryId,
                                            "entity_id": entityId,
                                            "entity_type": entityTypeD[entityId],
                                            "asym_id": asymId,
                                            "auth_asym_id": authAsymId,
                                            "comp_id": monId,
                                            "auth_seq_id": resNum,
                                            }, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer)
        return wD["npAuthAsymIdMapD"] if "npAuthAsymIdMapD" in wD else {}

    def getPolymerIdMap(self, dataContainer):
        """Return a dictionary of cardinal identifiers for each polymer entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {(authAsymId, authSeqId, insCode): {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "comp_id": compId,
                    }, ... }

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer)
        return wD["pAuthAsymIdMapD"] if "pAuthAsymIdMapD" in wD else {}

    def getBranchedIdMap(self, dataContainer):
        """Return a dictionary of cardinal identifiers for each branched entity instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict:  {(authAsymId, authSeqNum): {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "auth_asym_id": authAsymId,
                        "comp_id": monId,
                        "auth_seq_id": authSeqNum,
                        "seq_num": seqNum,
                    }, ...}

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer)
        return wD["brAuthAsymIdMapD"] if "brAuthAsymIdMapD" in wD else {}

    def getEntityTypeUniqueIds(self, dataContainer):
        """Return a nested dictionary of selected unique identifiers for entity types.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict:  [<entity_type>][<entity_id>] = {'asymIds': [...],'authAsymIds': [...], 'ccIds': [...]}


        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchAtomSiteInfo(dataContainer)
        return wD["entityTypeUniqueIds"] if "entityTypeUniqueIds" in wD else {}

    def __fetchAtomSiteInfo(self, dataContainer, modelId="1"):
        wD = self.__atomInfoCache.get((dataContainer.getName(), modelId))
        if not wD:
            wD = self.__getAtomSiteInfo(dataContainer, modelId=modelId)
            self.__atomInfoCache.set((dataContainer.getName(), modelId), wD)
        return wD

    def __getAtomSiteInfo(self, dataContainer, modelId="1"):
        """Get counting information for each instance in the deposited coordinates for the input model.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance
            modelId (str, optional): model index. Defaults to "1".

        Returns:
            (dict): with atom site counting and instance mapping details.

            For instance, the following are calculated:

                instanceAtomCountD[asymId]:  number of deposited atoms
                typeAtomCountD[entity type]: number of deposited atoms

                numAtomsModel:  number of deposited atoms in input model_id
                modelId: modelId

                instancePolymerModeledMonomerCountD[asymId]: number modeled polymer monomers in deposited coordinates
                instancePolymerUnmodeledMonomerCountD[asymId]: number of polymer unmodeled monomers in deposited coordinates

                numModels: total number of deposited models
                numAtomsAll: total number of deposited atoms

                asymAuthIdD = {asymId: authAsymId, ... }

                asymIdPolymerRangesD = {asymId: {"sampleSeqLen": sampleSeqLen,
                                                 "obsSeqLen": obsSeqLen,
                                                 "begSeqId": begSeqId,
                                                 "endSeqId": endSeqId,
                                                 "begAuthSeqId": begAuthSeqId,
                                                 "endAuthSeqId": endAuthSeqId,
                                                 "begInsCode": begAuthInsCode,
                                                 "endInsCode": endAuthInsCode,}, ...}
                instanceIdMapD = {asymId:  {"entry_id": entryId,
                                            "entity_id": entityId,
                                            "entity_type": entityTypeD[entityId],
                                            "asym_id": asymId,
                                            "auth_asym_id": authAsymId,
                                            "comp_id": monId,
                                            "auth_seq_id": "?",}, ...}

                 pAuthAsymIdMapD[(authAsymId, authSeqId, insCode)] = {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "comp_id": compId,
                        "seq_id": seqId,
                    }

                npAuthAsymIdMapD[(authAsymId, resNum)] = {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "auth_asym_id": authAsymId,
                        "comp_id": monId,
                        "auth_seq_id": resNum,
                    }

                brAuthAsymIdMapD[(authAsymId, authSeqNum)] = {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "auth_asym_id": authAsymId,
                        "comp_id": monId,
                        "auth_seq_id": authSeqNum,
                        "seq_num": seqNum,
                    }
                entityTypeUniqueIds[<entity_type>][<entity_id>] = {'asymIds': [...],'authAsymIds': [...], 'ccIds': [...]}

        """
        #
        numAtomsAll = 0
        numAtomsModel = 0
        typeCountD = {}
        instanceAtomCountD = {}
        instancePolymerModeledMonomerCountD = {}
        instancePolymerUnmodeledMonomerCountD = {}
        modelIdL = []
        asymAuthIdD = {}
        instanceTypeD = self.getInstanceTypes(dataContainer)
        entityTypeD = self.getEntityTypes(dataContainer)
        #
        eObj = dataContainer.getObj("entity")
        entityIdL = eObj.getAttributeValueList("id")
        #
        try:
            if dataContainer.exists("atom_site"):
                tObj = dataContainer.getObj("atom_site")
                numAtomsAll = tObj.getRowCount()
                conditionsD = {"pdbx_PDB_model_num": modelId}
                numAtomsModel = tObj.countValuesWhereConditions(conditionsD)
                modelIdL = tObj.getAttributeUniqueValueList("pdbx_PDB_model_num")
                cD = tObj.getCombinationCounts(["label_asym_id", "pdbx_PDB_model_num"])
                #
                for asymId, _ in instanceTypeD.items():
                    instanceAtomCountD[asymId] = cD[(asymId, modelId)] if (asymId, modelId) in cD else 0
                #
                # for eType in ['polymer', 'non-polymer', 'branched', 'macrolide', 'solvent']:
                typeCountD = {k: 0 for k in ["polymer", "non-polymer", "branched", "macrolide", "water"]}
                for asymId, aCount in instanceAtomCountD.items():
                    tt = instanceTypeD[asymId]
                    typeCountD[tt] += aCount
            else:
                logger.warning("Missing atom_site category for %s", dataContainer.getName())
            #
            numModels = len(modelIdL)
            if numModels < 1:
                logger.warning("Missing model details in atom_site category for %s", dataContainer.getName())
            #
            atomSiteInfoD = {
                "instanceAtomCountD": instanceAtomCountD,
                "typeAtomCountD": typeCountD,
                "numAtomsAll": numAtomsAll,
                "numAtomsModel": numAtomsModel,
                "numModels": len(modelIdL),
                "modelId": modelId,
                "instancePolymerModeledMonomerCountD": {},
                "instancePolymerUnmodeledMonomerCountD": {},
            }
        except Exception as e:
            logger.exception("Failing with %r with %r", dataContainer.getName(), str(e))

        #
        entityTypeUniqueIds = {}
        tAsymIdD = {}
        seqIdObsMapD = {}
        epLengthD = self.getPolymerEntityLengths(dataContainer)
        asymIdPolymerRangesD = {}
        instanceIdMapD = {}
        npAuthAsymIdMapD = {}
        pAuthAsymIdMapD = {}
        brAuthAsymIdMapD = {}
        try:
            eObj = dataContainer.getObj("entry")
            entryId = eObj.getValue("id", 0)
            #
            psObj = dataContainer.getObj("pdbx_poly_seq_scheme")
            if psObj is not None:
                # --
                for eId in entityIdL:
                    if entityTypeD[eId] in ["polymer"]:
                        tAsymIdL = psObj.selectValuesWhere("asym_id", eId, "entity_id")
                        tAuthAsymIdL = psObj.selectValuesWhere("pdb_strand_id", eId, "entity_id")
                        tCcIdL = psObj.selectValuesWhere("mon_id", eId, "entity_id")
                        entityTypeUniqueIds.setdefault(entityTypeD[eId], {}).setdefault(eId, {"asymIds": tAsymIdL, "authAsymIds": tAuthAsymIdL, "ccIds": tCcIdL})
                # ---
                aSeqD = {}
                for ii in range(psObj.getRowCount()):
                    asymId = psObj.getValue("asym_id", ii)
                    authSeqId = psObj.getValue("auth_seq_num", ii)
                    seqId = psObj.getValue("seq_id", ii)
                    compId = psObj.getValue("mon_id", ii)
                    entityId = psObj.getValue("entity_id", ii)
                    authAsymId = psObj.getValue("pdb_strand_id", ii)
                    #
                    insCode = psObj.getValueOrDefault("pdb_ins_code", ii, defaultValue=None)
                    aSeqD.setdefault(asymId, []).append(authSeqId)
                    #
                    if authSeqId not in [".", "?"]:
                        seqIdObsMapD.setdefault(asymId, {})[seqId] = (authSeqId, insCode)
                    #
                    pAuthAsymIdMapD[(authAsymId, authSeqId, insCode)] = {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "comp_id": compId,
                        "seq_id": seqId,
                    }
                    #
                    if asymId in tAsymIdD:
                        continue
                    tAsymIdD[asymId] = entityId
                    asymAuthIdD[asymId] = authAsymId
                    #
                    instanceIdMapD[asymId] = {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "auth_asym_id": authAsymId,
                        "rcsb_id": entryId + "." + asymId,
                        "comp_id": "?",
                        "auth_seq_id": "?",
                    }
                    #

                #
                #  Get the modeled and unmodeled monomer counts by asymId
                for asymId, sL in aSeqD.items():
                    instancePolymerModeledMonomerCountD[asymId] = len([t for t in sL if t not in ["?", "."]])
                    instancePolymerUnmodeledMonomerCountD[asymId] = len([t for t in sL if t in ["?", "."]])
                #  Get polymer range details for each polymer instance
                for asymId, entityId in tAsymIdD.items():
                    sampleSeqLen = epLengthD[entityId] if entityId in epLengthD else None
                    sL = list(seqIdObsMapD[asymId].items())
                    begSeqId, (begAuthSeqId, begAuthInsCode) = sL[0]
                    endSeqId, (endAuthSeqId, endAuthInsCode) = sL[-1]
                    obsSeqLen = len(sL)
                    #
                    asymIdPolymerRangesD[asymId] = {
                        "sampleSeqLen": sampleSeqLen,
                        "obsSeqLen": obsSeqLen,
                        "begSeqId": begSeqId,
                        "endSeqId": endSeqId,
                        "begAuthSeqId": begAuthSeqId,
                        "endAuthSeqId": endAuthSeqId,
                        "begInsCode": begAuthInsCode,
                        "endInsCode": endAuthInsCode,
                    }
            atomSiteInfoD["instancePolymerModeledMonomerCountD"] = instancePolymerModeledMonomerCountD
            atomSiteInfoD["instancePolymerUnmodeledMonomerCountD"] = instancePolymerUnmodeledMonomerCountD
            atomSiteInfoD["asymAuthIdD"] = asymAuthIdD
            atomSiteInfoD["asymIdPolymerRangesD"] = asymIdPolymerRangesD
            # --------------
            logger.debug(
                "%s instancePolymerModeledMonomerCountD(%d) %r",
                dataContainer.getName(),
                sum(atomSiteInfoD["instancePolymerModeledMonomerCountD"].values()),
                atomSiteInfoD["instancePolymerModeledMonomerCountD"],
            )
            logger.debug("%s instancePolymerUnmodeledMonomerCountD %r", dataContainer.getName(), atomSiteInfoD["instancePolymerUnmodeledMonomerCountD"])
            #
            # -------------- -------------- -------------- -------------- -------------- -------------- -------------- --------------
            #  Add nonpolymer instance mapping
            #
            npsObj = dataContainer.getObj("pdbx_nonpoly_scheme")
            if npsObj is not None:
                # --
                for eId in entityIdL:
                    if entityTypeD[eId] in ["non-polymer", "water"]:
                        tAsymIdL = npsObj.selectValuesWhere("asym_id", eId, "entity_id")
                        tAuthAsymIdL = npsObj.selectValuesWhere("pdb_strand_id", eId, "entity_id")
                        tCcIdL = npsObj.selectValuesWhere("mon_id", eId, "entity_id")
                        entityTypeUniqueIds.setdefault(entityTypeD[eId], {}).setdefault(eId, {"asymIds": tAsymIdL, "authAsymIds": tAuthAsymIdL, "ccIds": tCcIdL})
                # ---
                for ii in range(npsObj.getRowCount()):
                    asymId = npsObj.getValue("asym_id", ii)
                    entityId = npsObj.getValue("entity_id", ii)
                    authAsymId = npsObj.getValue("pdb_strand_id", ii)
                    resNum = npsObj.getValue("pdb_seq_num", ii)
                    monId = npsObj.getValue("mon_id", ii)
                    asymAuthIdD[asymId] = authAsymId
                    if asymId not in instanceIdMapD:
                        instanceIdMapD[asymId] = {
                            "entry_id": entryId,
                            "entity_id": entityId,
                            "entity_type": entityTypeD[entityId],
                            "asym_id": asymId,
                            "auth_asym_id": authAsymId,
                            "rcsb_id": entryId + "." + asymId,
                            "comp_id": monId,
                            "auth_seq_id": resNum,
                        }
                    npAuthAsymIdMapD[(authAsymId, resNum)] = {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "auth_asym_id": authAsymId,
                        "comp_id": monId,
                        "auth_seq_id": resNum,
                    }

            # ---------
            brsObj = dataContainer.getObj("pdbx_branch_scheme")
            if brsObj is not None:
                # --
                for eId in entityIdL:
                    if entityTypeD[eId] in ["branched"]:
                        tAsymIdL = brsObj.selectValuesWhere("asym_id", eId, "entity_id")
                        tAuthAsymIdL = brsObj.selectValuesWhere("auth_asym_id", eId, "entity_id")
                        tCcIdL = brsObj.selectValuesWhere("mon_id", eId, "entity_id")
                        entityTypeUniqueIds.setdefault(entityTypeD[eId], {}).setdefault(eId, {"asymIds": tAsymIdL, "authAsymIds": tAuthAsymIdL, "ccIds": tCcIdL})
                # ---
                for ii in range(brsObj.getRowCount()):
                    asymId = brsObj.getValue("asym_id", ii)
                    entityId = brsObj.getValue("entity_id", ii)
                    authAsymId = brsObj.getValue("auth_asym_id", ii)
                    authSeqNum = brsObj.getValue("auth_seq_num", ii)
                    monId = brsObj.getValue("mon_id", ii)
                    seqNum = brsObj.getValue("num", ii)
                    asymAuthIdD[asymId] = authAsymId
                    if asymId not in instanceIdMapD:
                        instanceIdMapD[asymId] = {
                            "entry_id": entryId,
                            "entity_id": entityId,
                            "entity_type": entityTypeD[entityId],
                            "asym_id": asymId,
                            "auth_asym_id": authAsymId,
                            "rcsb_id": entryId + "." + asymId,
                            "comp_id": monId,
                            "auth_seq_id": "?",
                        }
                    brAuthAsymIdMapD[(authAsymId, authSeqNum)] = {
                        "entry_id": entryId,
                        "entity_id": entityId,
                        "entity_type": entityTypeD[entityId],
                        "asym_id": asymId,
                        "auth_asym_id": authAsymId,
                        "comp_id": monId,
                        "auth_seq_id": authSeqNum,
                        "seq_num": seqNum,
                    }

            #
            atomSiteInfoD["instanceIdMapD"] = instanceIdMapD
            atomSiteInfoD["npAuthAsymIdMapD"] = npAuthAsymIdMapD
            atomSiteInfoD["pAuthAsymIdMapD"] = pAuthAsymIdMapD
            atomSiteInfoD["brAuthAsymIdMapD"] = brAuthAsymIdMapD
            atomSiteInfoD["entityTypeUniqueIds"] = entityTypeUniqueIds

        except Exception as e:
            logger.exception("Failing for %s with %s", dataContainer.getName(), str(e))

        #
        return atomSiteInfoD

    def getProtHelixFeatures(self, dataContainer):
        """Return a dictionary protein helical features (entity/label sequence coordinates).

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<helix_id>: (asymId, begSeqId, endSeqId), ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchProtSecStructFeatures(dataContainer)
        return wD["helixRangeD"] if "helixRangeD" in wD else {}

    def getProtUnassignedSecStructFeatures(self, dataContainer):
        """Return a dictionary protein regions lacking SS feature assignments (entity/label sequence coordinates).

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<id>: (asymId, begSeqId, endSeqId), ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchProtSecStructFeatures(dataContainer)
        return wD["unassignedRangeD"] if "unassignedRangeD" in wD else {}

    def getProtSheetFeatures(self, dataContainer):
        """Return a dictionary protein beta strand features (entity/label sequence coordinates).

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<sheet_id>: {asymId: [(begSeqId, endSeqId), ...], }
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchProtSecStructFeatures(dataContainer)
        return wD["instSheetRangeD"] if "instSheetRangeD" in wD else {}

    def getProtSheetSense(self, dataContainer):
        """Return a dictionary protein beta strand sense .

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<sheet_id>: mixed|parallel|anti-parallel, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchProtSecStructFeatures(dataContainer)
        return wD["senseTypeD"] if "senseTypeD" in wD else {}

    def getCisPeptides(self, dataContainer):
        """Return a dictionary cis-peptides linkages using standard nomenclature.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<id>: (begAsymId, begSeqId, endSeqId, modelId, omegaAngle), ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchProtSecStructFeatures(dataContainer)
        return wD["cisPeptideD"] if "cisPeptideD" in wD else {}

    def __fetchProtSecStructFeatures(self, dataContainer):
        wD = self.__protSSCache.get(dataContainer.getName())
        if not wD:
            wD = self.getProtSecStructFeatures(dataContainer)
            self.__protSSCache.set(dataContainer.getName(), wD)
        return wD

    def getProtSecStructFeatures(self, dataContainer):
        """ Get secondary structure features using standard nomenclature.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            (dict): with secondary structuree details

            For instance, the following are calculated:
                     {
                        "helixCountD": {},
                        "sheetStrandCountD": {},
                        "unassignedCountD": {},
                        "helixLengthD": {},
                        "sheetStrandLengthD": {},
                        "unassignedLengthD": {},
                        "helixFracD": {},
                        "sheetStrandFracD": {},
                        "unassignedFracD": {},
                        "sheetSenseD": {},
                        "sheetFullStrandCountD": {},
                        "featureMonomerSequenceD": {},
                        "featureSequenceD": {},
                        #
                        "unassignedRangeD": {},
                        "helixRangeD": {},
                        "instHelixD": {},
                        "sheetRangeD": {},
                        "instSheetD": {},
                         "senseTypeD": {}
                         "cisPeptideD": {},
                    }

            # -- Target data categories ---
            loop_
            _struct_conf.conf_type_id
            _struct_conf.id
            _struct_conf.pdbx_PDB_helix_id
            _struct_conf.beg_label_comp_id
            _struct_conf.beg_label_asym_id
            _struct_conf.beg_label_seq_id
            _struct_conf.pdbx_beg_PDB_ins_code
            _struct_conf.end_label_comp_id
            _struct_conf.end_label_asym_id
            _struct_conf.end_label_seq_id
            _struct_conf.pdbx_end_PDB_ins_code

            _struct_conf.beg_auth_comp_id
            _struct_conf.beg_auth_asym_id
            _struct_conf.beg_auth_seq_id
            _struct_conf.end_auth_comp_id
            _struct_conf.end_auth_asym_id
            _struct_conf.end_auth_seq_id
            _struct_conf.pdbx_PDB_helix_class
            _struct_conf.details
            _struct_conf.pdbx_PDB_helix_length
            HELX_P HELX_P1 AA1 SER A 5   ? LYS A 19  ? SER A 2   LYS A 16  1 ? 15
            HELX_P HELX_P2 AA2 GLU A 26  ? LYS A 30  ? GLU A 23  LYS A 27  5 ? 5
            HELX_P HELX_P3 AA3 GLY A 47  ? LYS A 60  ? GLY A 44  LYS A 57  1 ? 14
            HELX_P HELX_P4 AA4 ASP A 111 ? LEU A 125 ? ASP A 108 LEU A 122 1 ? 15
            #
            _struct_conf_type.id          HELX_P
            _struct_conf_type.criteria    ?
            _struct_conf_type.reference   ?
            # -------------------------------------------------------------------

            loop_
            _struct_asym.id
            _struct_asym.pdbx_blank_PDB_chainid_flag
            _struct_asym.pdbx_modified
            _struct_asym.entity_id
            _struct_asym.details
            A N N 1 ?
            B N N 1 ?
            #
            _struct_sheet.id               A
            _struct_sheet.type             ?
            _struct_sheet.number_strands   8
            _struct_sheet.details          ?
            #
            loop_
            _struct_sheet_order.sheet_id
            _struct_sheet_order.range_id_1
            _struct_sheet_order.range_id_2
            _struct_sheet_order.offset
            _struct_sheet_order.sense
            A 1 2 ? anti-parallel
            A 2 3 ? anti-parallel
            A 3 4 ? anti-parallel
            A 4 5 ? anti-parallel
            A 5 6 ? anti-parallel
            A 6 7 ? anti-parallel
            A 7 8 ? anti-parallel
            #
            loop_
            _struct_sheet_range.sheet_id
            _struct_sheet_range.id
            _struct_sheet_range.beg_label_comp_id
            _struct_sheet_range.beg_label_asym_id
            _struct_sheet_range.beg_label_seq_id
            _struct_sheet_range.pdbx_beg_PDB_ins_code
            _struct_sheet_range.end_label_comp_id
            _struct_sheet_range.end_label_asym_id
            _struct_sheet_range.end_label_seq_id
            _struct_sheet_range.pdbx_end_PDB_ins_code

            _struct_sheet_range.beg_auth_comp_id
            _struct_sheet_range.beg_auth_asym_id
            _struct_sheet_range.beg_auth_seq_id
            _struct_sheet_range.end_auth_comp_id
            _struct_sheet_range.end_auth_asym_id
            _struct_sheet_range.end_auth_seq_id
            A 1 LYS A 5  ? VAL A 8  ? LYS A 5  VAL A 8
            A 2 ARG A 11 ? THR A 16 ? ARG A 11 THR A 16
            A 3 VAL A 19 ? LEU A 26 ? VAL A 19 LEU A 26
            A 4 TYR A 29 ? ALA A 35 ? TYR A 29 ALA A 35
            A 5 TYR B 29 ? ALA B 35 ? TYR B 29 ALA B 35
            A 6 VAL B 19 ? LEU B 26 ? VAL B 19 LEU B 26
            A 7 ARG B 11 ? THR B 16 ? ARG B 11 THR B 16
            A 8 LYS B 5  ? VAL B 8  ? LYS B 5  VAL B 8
            #
            _struct_mon_prot_cis.pdbx_id                1
            _struct_mon_prot_cis.label_comp_id          ASN
            _struct_mon_prot_cis.label_seq_id           189
            _struct_mon_prot_cis.label_asym_id          C
            _struct_mon_prot_cis.label_alt_id           .
            _struct_mon_prot_cis.pdbx_PDB_ins_code      ?
            _struct_mon_prot_cis.auth_comp_id           ASN
            _struct_mon_prot_cis.auth_seq_id            2007
            _struct_mon_prot_cis.auth_asym_id           2

            _struct_mon_prot_cis.pdbx_label_comp_id_2   PRO
            _struct_mon_prot_cis.pdbx_label_seq_id_2    190
            _struct_mon_prot_cis.pdbx_label_asym_id_2   C
            _struct_mon_prot_cis.pdbx_PDB_ins_code_2    ?
            _struct_mon_prot_cis.pdbx_auth_comp_id_2    PRO
            _struct_mon_prot_cis.pdbx_auth_seq_id_2     2008
            _struct_mon_prot_cis.pdbx_auth_asym_id_2    2

            _struct_mon_prot_cis.pdbx_PDB_model_num     1
            _struct_mon_prot_cis.pdbx_omega_angle       -6.45
        """
        rD = {
            "helixCountD": {},
            "sheetStrandCountD": {},
            "unassignedCountD": {},
            "helixLengthD": {},
            "sheetStrandLengthD": {},
            "unassignedLengthD": {},
            "helixFracD": {},
            "sheetStrandFracD": {},
            "unassignedFracD": {},
            "sheetSenseD": {},
            "sheetFullStrandCountD": {},
            "featureMonomerSequenceD": {},
            "featureSequenceD": {},
            #
            "unassignedRangeD": {},
            "helixRangeD": {},
            "instHelixD": {},
            "sheetRangeD": {},
            "instSheetD": {},
            "senseTypeD": {},
            "cisPeptideD": {},
        }
        try:
            instancePolymerTypeD = self.getInstancePolymerTypes(dataContainer)
            instEntityD = self.getInstanceEntityMap(dataContainer)
            epLengthD = self.getPolymerEntityLengths(dataContainer)
            #
            helixRangeD = {}
            sheetRangeD = {}
            sheetSenseD = {}
            unassignedRangeD = {}
            cisPeptideD = OrderedDict()
            #
            if dataContainer.exists("struct_mon_prot_cis"):
                tObj = dataContainer.getObj("struct_mon_prot_cis")
                for ii in range(tObj.getRowCount()):
                    cId = tObj.getValue("pdbx_id", ii)
                    begAsymId = tObj.getValue("label_asym_id", ii)
                    # begCompId = tObj.getValue("label_comp_id", ii)
                    begSeqId = int(tObj.getValue("label_seq_id", ii))
                    endAsymId = tObj.getValue("pdbx_label_asym_id_2", ii)
                    # endCompId = int(tObj.getValue("pdbx_label_comp_id_2", ii))
                    endSeqId = int(tObj.getValue("pdbx_label_seq_id_2", ii))
                    modelId = int(tObj.getValue("pdbx_PDB_model_num", ii))
                    omegaAngle = float(tObj.getValue("pdbx_omega_angle", ii))
                    #
                    if (begAsymId == endAsymId) and (begSeqId <= endSeqId):
                        cisPeptideD.setdefault(cId, []).append((begAsymId, begSeqId, endSeqId, modelId, omegaAngle))
                    else:
                        logger.warning("%s inconsistent cis peptide description id = %s", dataContainer.getName(), cId)

            if dataContainer.exists("struct_conf"):
                tObj = dataContainer.getObj("struct_conf")
                helixRangeD = OrderedDict()
                for ii in range(tObj.getRowCount()):
                    confType = str(tObj.getValue("conf_type_id", ii)).strip().upper()
                    if confType in ["HELX_P"]:
                        hId = tObj.getValue("id", ii)
                        begAsymId = tObj.getValue("beg_label_asym_id", ii)
                        endAsymId = tObj.getValue("end_label_asym_id", ii)
                        try:
                            tbegSeqId = int(tObj.getValue("beg_label_seq_id", ii))
                            tendSeqId = int(tObj.getValue("end_label_seq_id", ii))
                            begSeqId = min(tbegSeqId, tendSeqId)
                            endSeqId = max(tbegSeqId, tendSeqId)
                        except Exception:
                            continue
                        if (begAsymId == endAsymId) and (begSeqId <= endSeqId):
                            helixRangeD.setdefault(hId, []).append((begAsymId, begSeqId, endSeqId))
                        else:
                            logger.warning("%s inconsistent struct_conf description id = %s", dataContainer.getName(), hId)

            logger.debug("%s helixRangeD %r", dataContainer.getName(), helixRangeD.items())

            if dataContainer.exists("struct_sheet_range"):
                tObj = dataContainer.getObj("struct_sheet_range")
                sheetRangeD = OrderedDict()
                for ii in range(tObj.getRowCount()):
                    sId = tObj.getValue("sheet_id", ii)
                    begAsymId = tObj.getValue("beg_label_asym_id", ii)
                    endAsymId = tObj.getValue("end_label_asym_id", ii)
                    # Most obsolete entries do no define this
                    try:
                        tbegSeqId = int(tObj.getValue("beg_label_seq_id", ii))
                        tendSeqId = int(tObj.getValue("end_label_seq_id", ii))
                        begSeqId = min(tbegSeqId, tendSeqId)
                        endSeqId = max(tbegSeqId, tendSeqId)
                    except Exception:
                        continue
                    if (begAsymId == endAsymId) and (begSeqId <= endSeqId):
                        sheetRangeD.setdefault(sId, []).append((begAsymId, begSeqId, endSeqId))
                    else:
                        logger.warning("%s inconsistent struct_sheet_range description id = %s", dataContainer.getName(), sId)

            logger.debug("%s sheetRangeD %r", dataContainer.getName(), sheetRangeD.items())
            #
            if dataContainer.exists("struct_sheet_order"):
                tObj = dataContainer.getObj("struct_sheet_order")
                #
                sheetSenseD = OrderedDict()
                for ii in range(tObj.getRowCount()):
                    sId = tObj.getValue("sheet_id", ii)
                    sense = str(tObj.getValue("sense", ii)).strip().lower()
                    sheetSenseD.setdefault(sId, []).append(sense)
            #
            logger.debug("%s sheetSenseD %r", dataContainer.getName(), sheetSenseD.items())
            # --------

            unassignedCoverageD = {}
            unassignedCountD = {}
            unassignedLengthD = {}
            unassignedFracD = {}

            helixCoverageD = {}
            helixCountD = {}
            helixLengthD = {}
            helixFracD = {}
            instHelixD = {}

            sheetCoverageD = {}
            sheetStrandCountD = {}
            sheetStrandLengthD = {}
            strandsPerBetaSheetD = {}
            sheetFullStrandCountD = {}
            sheetStrandFracD = {}
            instSheetD = {}
            instSheetSenseD = {}
            #
            featureMonomerSequenceD = {}
            featureSequenceD = {}
            #
            # ------------
            # Initialize over all protein instances
            for asymId, filteredType in instancePolymerTypeD.items():
                if filteredType != "Protein":
                    continue
                helixCoverageD[asymId] = []
                helixLengthD[asymId] = []
                helixCountD[asymId] = 0
                helixFracD[asymId] = 0.0
                instHelixD[asymId] = []
                #
                sheetCoverageD[asymId] = []
                sheetStrandCountD[asymId] = 0
                sheetStrandLengthD[asymId] = []
                sheetFullStrandCountD[asymId] = []
                sheetStrandFracD[asymId] = 0.0
                instSheetD[asymId] = []
                instSheetSenseD[asymId] = []
                #
                unassignedCountD[asymId] = 0
                unassignedLengthD[asymId] = []
                unassignedFracD[asymId] = 0.0
                #
                featureMonomerSequenceD[asymId] = None
                featureSequenceD[asymId] = None
            # -------------
            #
            for hId, hL in helixRangeD.items():
                for (asymId, begSeqId, endSeqId) in hL:
                    helixCoverageD.setdefault(asymId, []).extend(range(begSeqId, endSeqId + 1))
                    helixLengthD.setdefault(asymId, []).append(abs(begSeqId - endSeqId) + 1)
                    helixCountD[asymId] = helixCountD[asymId] + 1 if asymId in helixCountD else 0
                    instHelixD.setdefault(asymId, []).append(hId)
            #
            # ---------
            # betaSheetCount = len(sheetRangeD)
            #
            for sId, sL in sheetRangeD.items():
                strandsPerBetaSheetD[sId] = len(sL)
                for (asymId, begSeqId, endSeqId) in sL:
                    sheetCoverageD.setdefault(asymId, []).extend(range(begSeqId, endSeqId + 1))
                    sheetStrandLengthD.setdefault(asymId, []).append(abs(begSeqId - endSeqId) + 1)
                    sheetStrandCountD[asymId] = sheetStrandCountD[asymId] + 1 if asymId in sheetStrandCountD else 0
                    instSheetD.setdefault(asymId, []).append(sId)
            #
            instSheetRangeD = {}
            for sId, sL in sheetRangeD.items():
                aD = {}
                for (asymId, begSeqId, endSeqId) in sL:
                    aD.setdefault(asymId, []).append((begSeqId, endSeqId))
                instSheetRangeD[sId] = aD
            #
            # ---------
            senseTypeD = {}
            for sheetId, sL in sheetSenseD.items():
                if not sL:
                    continue
                usL = list(set(sL))
                if len(usL) == 1:
                    senseTypeD[sheetId] = usL[0]
                else:
                    senseTypeD[sheetId] = "mixed"
            # ---------
            #
            for asymId, filteredType in instancePolymerTypeD.items():
                logger.debug("%s processing %s type %r", dataContainer.getName(), asymId, filteredType)
                if filteredType != "Protein":
                    continue
                entityId = instEntityD[asymId]
                entityLen = epLengthD[entityId]
                entityS = set(range(1, entityLen + 1))
                eLen = len(entityS)
                #
                helixS = set(helixCoverageD[asymId])
                sheetS = set(sheetCoverageD[asymId])
                commonS = helixS & sheetS
                if commonS:
                    logger.debug("%s asymId %s overlapping secondary structure assignments for monomers %r", dataContainer.getName(), asymId, commonS)
                    # continue

                hLen = len(helixS) if asymId in helixCoverageD else 0
                sLen = len(sheetS) if asymId in sheetCoverageD else 0
                #
                unassignedS = entityS - helixS if hLen else entityS
                unassignedS = unassignedS - sheetS if sLen else unassignedS
                tLen = len(unassignedS)
                #
                # if eLen != hLen + sLen + tLen:
                #    logger.warning("%s overlapping secondary structure assignments for asymId %s", dataContainer.getName(), asymId)
                #    continue
                #
                unassignedCoverageD[asymId] = list(unassignedS)
                helixFracD[asymId] = float(hLen) / float(eLen)
                sheetStrandFracD[asymId] = float(sLen) / float(eLen)
                unassignedFracD[asymId] = float(tLen) / float(eLen)
                #
                unassignedRangeD[asymId] = list(self.__toRangeList(unassignedS))
                unassignedCountD[asymId] = len(unassignedRangeD[asymId])
                unassignedLengthD[asymId] = [abs(i - j) + 1 for (i, j) in unassignedRangeD[asymId]]
                #
                # ------
                sIdL = instSheetD[asymId]
                #
                instSheetSenseD[asymId] = [senseTypeD[sId] for sId in sIdL if sId in senseTypeD]
                sheetFullStrandCountD[asymId] = [strandsPerBetaSheetD[sId] for sId in sIdL if sId in strandsPerBetaSheetD]
                #

                # ------
                ssTypeL = ["_"] * eLen
                if hLen:
                    for idx in helixCoverageD[asymId]:
                        ssTypeL[idx - 1] = "H"
                if sLen:
                    for idx in sheetCoverageD[asymId]:
                        ssTypeL[idx - 1] = "S"
                if tLen:
                    for idx in unassignedCoverageD[asymId]:
                        ssTypeL[idx - 1] = "_"
                #
                featureMonomerSequenceD[asymId] = "".join(ssTypeL)
                featureSequenceD[asymId] = "".join([t[0] for t in itertools.groupby(ssTypeL)])
            # ---------

            rD = {
                "helixCountD": helixCountD,
                "sheetStrandCountD": sheetStrandCountD,
                "unassignedCountD": unassignedCountD,
                "helixLengthD": helixLengthD,
                "sheetStrandLengthD": sheetStrandLengthD,
                "unassignedLengthD": unassignedLengthD,
                "helixFracD": helixFracD,
                "sheetStrandFracD": sheetStrandFracD,
                "unassignedFracD": unassignedFracD,
                "sheetSenseD": instSheetSenseD,
                "sheetFullStrandCountD": sheetFullStrandCountD,
                "featureMonomerSequenceD": featureMonomerSequenceD,
                "featureSequenceD": featureSequenceD,
                #
                "unassignedRangeD": unassignedRangeD,
                "helixRangeD": helixRangeD,
                "instHelixD": instHelixD,
                # "sheetRangeD": sheetRangeD,
                "instSheetRangeD": instSheetRangeD,
                "instSheetD": instSheetD,
                "senseTypeD": senseTypeD,
                "cisPeptideD": cisPeptideD,
            }
            # self.__secondaryStructD = rD
            # self.__setEntryCache(dataContainer.getName())
        except Exception as e:
            logger.exception("Failing for %s with %s", dataContainer.getName(), str(e))
        #
        return rD

    # Connection related
    def getInstanceConnectionCounts(self, dataContainer):
        """Return a dictionary instance connection counts.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<connection type>: #count, ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchInstanceConnections(dataContainer)
        return wD["instConnectCountD"] if "instConnectCountD" in wD else {}

    def getInstanceConnections(self, dataContainer):
        """Return a list of instance connections.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            list: [{"connect_type": <val>, "connect_target_label_comp_id": <val>, ... },...]

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchInstanceConnections(dataContainer)
        return wD["instConnectL"] if "instConnectL" in wD else {}

    def getBoundNonpolymersComponentIds(self, dataContainer):
        """Return a list of bound non-polymers in the entry.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<entityId>: NonpolymerBoundEntity("targetCompId", "connectType", "partnerCompId", "partnerEntityId", "partnerEntityType"), }
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchInstanceConnections(dataContainer)
        return wD["boundNonpolymerComponentIdL"] if "boundNonpolymerComponentIdL" in wD else {}

    def getBoundNonpolymersByEntity(self, dataContainer):
        """Return a dictonary of bound non-polymers by entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<entityId>: NonpolymerBoundEntity("targetCompId", "connectType", "partnerCompId", "partnerEntityId", "partnerEntityType"), }
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchInstanceConnections(dataContainer)
        return wD["boundNonpolymerEntityD"] if "boundNonpolymerEntityD" in wD else {}

    def getBoundNonpolymersByInstance(self, dataContainer):
        """Return a dictonary of bound non-polymers by instance.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<asymId>: NonpolymerBoundInstance("targetCompId", "connectType", "partnerCompId", "partnerAsymId", "partnerEntityType", "bondDistance", "bondOrder"), }

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchInstanceConnections(dataContainer)
        return wD["boundNonpolymerInstanceD"] if "boundNonpolymerInstanceD" in wD else {}

    def __fetchInstanceConnections(self, dataContainer):
        wD = self.__instanceConnectionCache.get(dataContainer.getName())
        if not wD:
            wD = self.__getInstanceConnections(dataContainer)
            self.__instanceConnectionCache.set(dataContainer.getName(), wD)
        return wD

    def __getInstanceConnections(self, dataContainer):
        """ Get instance connections (e.g., intermolecular bonds and non-primary connectivity)

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: instConnectCountD{<bond_type>: count, ... }

            For instance, the following are calculated:
                     {Get counting information about intermolecular linkages.
            covale  .
            disulf  .
            hydrog  .
            metalc

            loop_
            _struct_asym.id
            _struct_asym.pdbx_blank_PDB_chainid_flag
            _struct_asym.pdbx_modified
            _struct_asym.entity_id
            _struct_asym.details
            A N N 1 ?
            B N N 1 ?
            #
            _struct_biol.id   1
            #
            loop_
            _struct_conn.id
            _struct_conn.conn_type_id
            _struct_conn.pdbx_leaving_atom_flag
            _struct_conn.pdbx_PDB_id
            _struct_conn.ptnr1_label_asym_id
            _struct_conn.ptnr1_label_comp_id
            _struct_conn.ptnr1_label_seq_id
            _struct_conn.ptnr1_label_atom_id
            _struct_conn.pdbx_ptnr1_label_alt_id
            _struct_conn.pdbx_ptnr1_PDB_ins_code
            _struct_conn.pdbx_ptnr1_standard_comp_id
            _struct_conn.ptnr1_symmetry
            _struct_conn.ptnr2_label_asym_id
            _struct_conn.ptnr2_label_comp_id
            _struct_conn.ptnr2_label_seq_id
            _struct_conn.ptnr2_label_atom_id
            _struct_conn.pdbx_ptnr2_label_alt_id
            _struct_conn.pdbx_ptnr2_PDB_ins_code
            _struct_conn.ptnr1_auth_asym_id
            _struct_conn.ptnr1_auth_comp_id
            _struct_conn.ptnr1_auth_seq_id
            _struct_conn.ptnr2_auth_asym_id
            _struct_conn.ptnr2_auth_comp_id
            _struct_conn.ptnr2_auth_seq_id
            _struct_conn.ptnr2_symmetry
            _struct_conn.pdbx_ptnr3_label_atom_id
            _struct_conn.pdbx_ptnr3_label_seq_id
            _struct_conn.pdbx_ptnr3_label_comp_id
            _struct_conn.pdbx_ptnr3_label_asym_id
            _struct_conn.pdbx_ptnr3_label_alt_id
            _struct_conn.pdbx_ptnr3_PDB_ins_code
            _struct_conn.details
            _struct_conn.pdbx_dist_value
            _struct_conn.pdbx_value_order
            disulf1  disulf ? ? A CYS 31 SG ? ? ? 1_555 B CYS 31 SG ? ? A CYS 31 B CYS 31 1_555 ? ? ? ? ? ? ? 1.997 ?
            covale1  covale ? ? A VAL 8  C  ? ? ? 1_555 A DPR 9  N  ? ? A VAL 8  A DPR 9  1_555 ? ? ? ? ? ? ? 1.360 ?
            covale2  covale ? ? A DPR 9  C  ? ? ? 1_555 A GLY 10 N  ? ? A DPR 9  A GLY 10 1_555 ? ? ? ? ? ? ? 1.324 ?
            #
        """
        iAttMapD = {
            "id": "id",
            "connect_type": "conn_type_id",
            "connect_target_label_comp_id": "ptnr1_label_comp_id",
            "connect_target_label_asym_id": "ptnr1_label_asym_id",
            "connect_target_label_seq_id": "ptnr1_label_seq_id",
            "connect_target_label_atom_id": "ptnr1_label_atom_id",
            "connect_target_label_alt_id": "pdbx_ptnr1_label_alt_id",
            "connect_target_symmetry": "ptnr1_symmetry",
            #
            "connect_partner_label_comp_id": "ptnr2_label_comp_id",
            "connect_partner_label_asym_id": "ptnr2_label_asym_id",
            "connect_partner_label_seq_id": "ptnr2_label_seq_id",
            "connect_partner_label_atom_id": "ptnr2_label_atom_id",
            "connect_partner_label_alt_id": "pdbx_ptnr2_label_alt_id",
            "connect_partner_symmetry": "ptnr2_symmetry",
            "value_order": "pdbx_value_order",
            "dist_value": "pdbx_dist_value",
            "description": "details",
        }
        jAttMapD = {
            "id": "id",
            "connect_type": "conn_type_id",
            "connect_target_label_comp_id": "ptnr2_label_comp_id",
            "connect_target_label_asym_id": "ptnr2_label_asym_id",
            "connect_target_label_seq_id": "ptnr2_label_seq_id",
            "connect_target_label_atom_id": "ptnr2_label_atom_id",
            "connect_target_label_alt_id": "pdbx_ptnr2_label_alt_id",
            "connect_target_symmetry": "ptnr2_symmetry",
            #
            "connect_partner_label_comp_id": "ptnr1_label_comp_id",
            "connect_partner_label_asym_id": "ptnr1_label_asym_id",
            "connect_partner_label_seq_id": "ptnr1_label_seq_id",
            "connect_partner_label_atom_id": "ptnr1_label_atom_id",
            "connect_partner_label_alt_id": "pdbx_ptnr1_label_alt_id",
            "connect_partner_symmetry": "ptnr1_symmetry",
            "value_order": "pdbx_value_order",
            "dist_value": "pdbx_dist_value",
            "description": "details",
        }
        typeMapD = {
            "covale": "covalent bond",
            "disulf": "disulfide bridge",
            "hydrog": "hydrogen bond",
            "metalc": "metal coordination",
            "mismat": "mismatched base pairs",
            "saltbr": "ionic interaction",
            "modres": "covalent residue modification",
            "covale_base": "covalent modification of a nucleotide base",
            "covale_sugar": "covalent modification of a nucleotide sugar",
            "covale_phosphate": "covalent modification of a nucleotide phosphate",
        }
        #
        instConnectL = []
        instConnectCountD = {ky: 0 for ky in typeMapD}
        boundNonpolymerEntityD = {}
        boundNonpolymerInstanceD = {}
        boundNonpolymerComponentIdL = []
        #
        if dataContainer.exists("struct_conn"):
            tObj = dataContainer.getObj("struct_conn")
            for ii in range(tObj.getRowCount()):
                bt = str(tObj.getValue("conn_type_id", ii)).strip().lower()
                if bt not in instConnectCountD:
                    logger.error("Unsupported intermolecular bond type %r in %r", bt, dataContainer.getName())
                    continue
                instConnectCountD[bt] = instConnectCountD[bt] + 1 if bt in instConnectCountD else instConnectCountD[bt]
                #
                tD = OrderedDict()
                for ky, atName in iAttMapD.items():
                    val = tObj.getValue(atName, ii) if atName != "conn_type_id" else typeMapD[tObj.getValue(atName, ii).lower()]
                    tD[ky] = val
                instConnectL.append(tD)
                # Flip the bond sense so all target connections are accounted for
                tD = OrderedDict()
                for ky, atName in jAttMapD.items():
                    val = tObj.getValue(atName, ii) if atName != "conn_type_id" else typeMapD[tObj.getValue(atName, ii).lower()]
                    tD[ky] = val
                instConnectL.append(tD)

            boundNonpolymerEntityD, boundNonpolymerInstanceD, boundNonpolymerComponentIdL = self.__getBoundNonpolymers(dataContainer, instConnectL)

        return {
            "instConnectL": instConnectL,
            "instConnectCountD": instConnectCountD,
            "boundNonpolymerEntityD": boundNonpolymerEntityD,
            "boundNonpolymerInstanceD": boundNonpolymerInstanceD,
            "boundNonpolymerComponentIdL": boundNonpolymerComponentIdL,
        }

    def __getBoundNonpolymers(self, dataContainer, instConnectL):
        """ Get nonpolymer bound

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            bool: True for success or False otherwise

        Example:
        """
        logger.debug("Starting with %r", dataContainer.getName())
        #
        boundNonpolymerEntityD = {}
        boundNonpolymerInstanceD = {}
        boundNonpolymerComponentIdL = []
        try:
            cDL = instConnectL
            asymIdD = self.getInstanceEntityMap(dataContainer)
            # asymAuthIdD = self.getAsymAuthIdMap(dataContainer)
            eTypeD = self.getEntityTypes(dataContainer)
            #
            ts = set()
            for cD in cDL:
                tAsymId = cD["connect_target_label_asym_id"]
                tEntityId = asymIdD[tAsymId]
                if eTypeD[tEntityId] == "non-polymer" and cD["connect_type"] in ["covale", "covalent bond", "metalc", "metal coordination"]:
                    pAsymId = cD["connect_partner_label_asym_id"]
                    pEntityId = asymIdD[pAsymId]
                    pCompId = cD["connect_partner_label_comp_id"]
                    tCompId = cD["connect_target_label_comp_id"]
                    bondOrder = cD["value_order"]
                    bondDist = cD["dist_value"]
                    pType = eTypeD[pEntityId]
                    #
                    ts.add(tCompId)
                    boundNonpolymerInstanceD.setdefault(tAsymId, []).append(NonpolymerBoundInstance(tCompId, cD["connect_type"], pCompId, pAsymId, pType, bondDist, bondOrder))
                    boundNonpolymerEntityD.setdefault(tEntityId, []).append(NonpolymerBoundEntity(tCompId, cD["connect_type"], pCompId, pEntityId, pType))
            #
            for asymId in boundNonpolymerInstanceD:
                boundNonpolymerInstanceD[asymId] = sorted(set(boundNonpolymerInstanceD[asymId]))
            for entityId in boundNonpolymerEntityD:
                boundNonpolymerEntityD[entityId] = sorted(set(boundNonpolymerEntityD[entityId]))
            boundNonpolymerComponentIdL = sorted(ts)
        except Exception as e:
            logger.exception("%s failing with %s", dataContainer.getName(), str(e))
        return boundNonpolymerEntityD, boundNonpolymerInstanceD, boundNonpolymerComponentIdL

    def getEntitySequenceFeatureCounts(self, dataContainer):
        """Return a dictionary of sequence feature counts.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {<entity>: {'mutation': #, 'artifact': #, 'conflict': #, ...  }, }

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchSequenceFeatures(dataContainer)
        return wD["seqFeatureCountsD"] if "seqFeatureCountsD" in wD else {}

    def getEntitySequenceMonomerFeatures(self, dataContainer):
        """Return a dictionary of sequence monomer features.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {(entityId,seqId,compId,filteredFeature): {detail,detail},  .. }

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchSequenceFeatures(dataContainer)
        return wD["seqMonomerFeatureD"] if "seqMonomerFeatureD" in wD else {}

    def getEntitySequenceRangeFeatures(self, dataContainer):
        """Return a dictionary of sequence range features.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {(entityId,benSeqId,endSeqId,filteredFeature): {detail,detail},  .. }

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchSequenceFeatures(dataContainer)
        return wD["seqRangeFeatureD"] if "seqRangeFeatureD" in wD else {}

    def getEntityReferenceAlignments(self, dataContainer):
        """Return a dictionary of reference sequence alignments for each entity.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {entityId: {'dbName': , 'dbAccession': , 'authAsymId': , 'entitySeqIdBeg':, 'dbSeqIdBeg':, ... },  .. }

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchSequenceFeatures(dataContainer)
        return wD["seqEntityAlignmentD"] if "seqEntityAlignmentD" in wD else {}

    def getEntitySequenceReferenceCodes(self, dataContainer):
        """Return a dictionary of reference database accession codes.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {entityId: {'dbName': , 'dbAccession': },  ... }

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchSequenceFeatures(dataContainer)
        return wD["seqEntityRefDbD"] if "seqEntityRefDbD" in wD else {}

    def __fetchSequenceFeatures(self, dataContainer):
        wD = self.__entitySequenceFeatureCache.get(dataContainer.getName())
        if not wD:
            wD = self.__getSequenceFeatures(dataContainer)
            self.__entitySequenceFeatureCache.set(dataContainer.getName(), wD)
        return wD

    def getDatabaseNameMap(self):
        dbNameMapD = {
            "UNP": "UniProt",
            "GB": "GenBank",
            "PDB": "PDB",
            "EMBL": "EMBL",
            "GENP": "GenBank",
            "NDB": "NDB",
            "NOR": "NORINE",
            "PIR": "PIR",
            "PRF": "PRF",
            "REF": "RefSeq",
            "TPG": "GenBank",
            "TREMBL": "UniProt",
            "SWS": "UniProt",
            "SWALL": "UniProt",
        }
        return dbNameMapD

    def __getSequenceFeatures(self, dataContainer):
        """ Get point sequence features.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance

        Returns:
            dict : {"seqFeatureCountsD": {entityId: {"mutation": #, "conflict": # ... }, }
                    "seqMonomerFeatureD": {(entityId, seqId, compId, filteredFeature): set(feature,...), ...}
                    "seqRangeFeatureD" : {(entityId, str(beg), str(end), "artifact"): set(details)}
                    "seqEntityAlignmentD" : {entityId: [{'dbName': 'UNP' , 'dbAccession': 'P000000', ... }]}
                    "seqEntityRefDbD":  {entityId: [{'dbName': 'UNP' , 'dbAccession': 'P000000'),  }]},
                    }

        Example source content:

            _struct_ref.id                         1
            _struct_ref.db_name                    UNP
            _struct_ref.db_code                    KSYK_HUMAN
            _struct_ref.pdbx_db_accession          P43405
            _struct_ref.entity_id                  1
            _struct_ref.pdbx_seq_one_letter_code
            ;ADPEEIRPKEVYLDRKLLTLEDKELGSGNFGTVKKGYYQMKKVVKTVAVKILKNEANDPALKDELLAEANVMQQLDNPYI
            VRMIGICEAESWMLVMEMAELGPLNKYLQQNRHVKDKNIIELVHQVSMGMKYLEESNFVHRDLAARNVLLVTQHYAKISD
            FGLSKALRADENYYKAQTHGKWPVKWYAPECINYYKFSSKSDVWSFGVLMWEAFSYGQKPYRGMKGSEVTAMLEKGERMG
            CPAGCPREMYDLMNLCWTYDVENRPGFAAVELRLRNYYYDVVN
            ;
            _struct_ref.pdbx_align_begin           353
            _struct_ref.pdbx_db_isoform            ?
            #
            _struct_ref_seq.align_id                      1
            _struct_ref_seq.ref_id                        1
            _struct_ref_seq.pdbx_PDB_id_code              1XBB
            _struct_ref_seq.pdbx_strand_id                A
            _struct_ref_seq.seq_align_beg                 1
            _struct_ref_seq.pdbx_seq_align_beg_ins_code   ?
            _struct_ref_seq.seq_align_end                 283
            _struct_ref_seq.pdbx_seq_align_end_ins_code   ?
            _struct_ref_seq.pdbx_db_accession             P43405
            _struct_ref_seq.db_align_beg                  353
            _struct_ref_seq.pdbx_db_align_beg_ins_code    ?
            _struct_ref_seq.db_align_end                  635
            _struct_ref_seq.pdbx_db_align_end_ins_code    ?
            _struct_ref_seq.pdbx_auth_seq_align_beg       353
            _struct_ref_seq.pdbx_auth_seq_align_end       635
            _struct_ref_seq.rcsb_entity_id                1
            #
            loop_
            _struct_ref_seq_dif.align_id
            _struct_ref_seq_dif.pdbx_pdb_id_code
            _struct_ref_seq_dif.mon_id
            _struct_ref_seq_dif.pdbx_pdb_strand_id
            _struct_ref_seq_dif.seq_num
            _struct_ref_seq_dif.pdbx_pdb_ins_code
            _struct_ref_seq_dif.pdbx_seq_db_name
            _struct_ref_seq_dif.pdbx_seq_db_accession_code
            _struct_ref_seq_dif.db_mon_id
            _struct_ref_seq_dif.pdbx_seq_db_seq_num
            _struct_ref_seq_dif.details
            _struct_ref_seq_dif.pdbx_auth_seq_num
            _struct_ref_seq_dif.pdbx_ordinal
            _struct_ref_seq_dif.rcsb_entity_id
            1 1XBB MET A 1   ? UNP P43405 ALA 353 'CLONING ARTIFACT' 353 1  1
            1 1XBB ALA A 2   ? UNP P43405 ASP 354 'CLONING ARTIFACT' 354 2  1
            1 1XBB LEU A 3   ? UNP P43405 PRO 355 'CLONING ARTIFACT' 355 3  1
            1 1XBB GLU A 284 ? UNP P43405 ?   ?   'CLONING ARTIFACT' 636 4  1
            1 1XBB GLY A 285 ? UNP P43405 ?   ?   'CLONING ARTIFACT' 637 5  1
            1 1XBB HIS A 286 ? UNP P43405 ?   ?   'EXPRESSION TAG'   638 6  1
            1 1XBB HIS A 287 ? UNP P43405 ?   ?   'EXPRESSION TAG'   639 7  1
            1 1XBB HIS A 288 ? UNP P43405 ?   ?   'EXPRESSION TAG'   640 8  1
            1 1XBB HIS A 289 ? UNP P43405 ?   ?   'EXPRESSION TAG'   641 9  1
            1 1XBB HIS A 290 ? UNP P43405 ?   ?   'EXPRESSION TAG'   642 10 1
            1 1XBB HIS A 291 ? UNP P43405 ?   ?   'EXPRESSION TAG'   643 11 1
            #
            #
            loop_
            _struct_ref_seq_dif.align_id
            _struct_ref_seq_dif.pdbx_pdb_id_code
            _struct_ref_seq_dif.mon_id
            _struct_ref_seq_dif.pdbx_pdb_strand_id
            _struct_ref_seq_dif.seq_num
            _struct_ref_seq_dif.pdbx_pdb_ins_code
            _struct_ref_seq_dif.pdbx_seq_db_name
            _struct_ref_seq_dif.pdbx_seq_db_accession_code
            _struct_ref_seq_dif.db_mon_id
            _struct_ref_seq_dif.pdbx_seq_db_seq_num
            _struct_ref_seq_dif.details
            _struct_ref_seq_dif.pdbx_auth_seq_num
            _struct_ref_seq_dif.pdbx_ordinal
            _struct_ref_seq_dif.rcsb_entity_id
            1 3RIJ TYR A 53  ? UNP Q5SHN1 PHE 54  'ENGINEERED MUTATION' 54  1  1
            1 3RIJ GLY A 54  ? UNP Q5SHN1 VAL 55  'ENGINEERED MUTATION' 55  2  1
            2 3RIJ ASP A 98  ? UNP Q5SHN1 ALA 99  'ENGINEERED MUTATION' 99  3  1
            2 3RIJ ALA A 99  ? UNP Q5SHN1 ILE 100 'ENGINEERED MUTATION' 100 4  1
            2 3RIJ LEU A 158 ? UNP Q5SHN1 ?   ?   INSERTION             159 5  1
            2 3RIJ GLU A 159 ? UNP Q5SHN1 ?   ?   INSERTION             160 6  1
            2 3RIJ HIS A 160 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      161 7  1
            2 3RIJ HIS A 161 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      162 8  1
            2 3RIJ HIS A 162 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      163 9  1
            2 3RIJ HIS A 163 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      164 10 1
            2 3RIJ HIS A 164 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      165 11 1
            2 3RIJ HIS A 165 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      166 12 1
            3 3RIJ TYR B 53  ? UNP Q5SHN1 PHE 54  'ENGINEERED MUTATION' 54  13 1
            3 3RIJ GLY B 54  ? UNP Q5SHN1 VAL 55  'ENGINEERED MUTATION' 55  14 1
            4 3RIJ ASP B 98  ? UNP Q5SHN1 ALA 99  'ENGINEERED MUTATION' 99  15 1
            4 3RIJ ALA B 99  ? UNP Q5SHN1 ILE 100 'ENGINEERED MUTATION' 100 16 1
            4 3RIJ LEU B 158 ? UNP Q5SHN1 ?   ?   INSERTION             159 17 1
            4 3RIJ GLU B 159 ? UNP Q5SHN1 ?   ?   INSERTION             160 18 1
            4 3RIJ HIS B 160 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      161 19 1
            4 3RIJ HIS B 161 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      162 20 1
            4 3RIJ HIS B 162 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      163 21 1
            4 3RIJ HIS B 163 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      164 22 1
            4 3RIJ HIS B 164 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      165 23 1
            4 3RIJ HIS B 165 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      166 24 1
            5 3RIJ TYR C 53  ? UNP Q5SHN1 PHE 54  'ENGINEERED MUTATION' 54  25 1
            5 3RIJ GLY C 54  ? UNP Q5SHN1 VAL 55  'ENGINEERED MUTATION' 55  26 1
            6 3RIJ ASP C 98  ? UNP Q5SHN1 ALA 99  'ENGINEERED MUTATION' 99  27 1
            6 3RIJ ALA C 99  ? UNP Q5SHN1 ILE 100 'ENGINEERED MUTATION' 100 28 1
            6 3RIJ LEU C 158 ? UNP Q5SHN1 ?   ?   INSERTION             159 29 1
            6 3RIJ GLU C 159 ? UNP Q5SHN1 ?   ?   INSERTION             160 30 1
            6 3RIJ HIS C 160 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      161 31 1
            6 3RIJ HIS C 161 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      162 32 1
            6 3RIJ HIS C 162 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      163 33 1
            6 3RIJ HIS C 163 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      164 34 1
            6 3RIJ HIS C 164 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      165 35 1
            6 3RIJ HIS C 165 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      166 36 1
            7 3RIJ TYR D 53  ? UNP Q5SHN1 PHE 54  'ENGINEERED MUTATION' 54  37 1
            7 3RIJ GLY D 54  ? UNP Q5SHN1 VAL 55  'ENGINEERED MUTATION' 55  38 1
            8 3RIJ ASP D 98  ? UNP Q5SHN1 ALA 99  'ENGINEERED MUTATION' 99  39 1
            8 3RIJ ALA D 99  ? UNP Q5SHN1 ILE 100 'ENGINEERED MUTATION' 100 40 1
            8 3RIJ LEU D 158 ? UNP Q5SHN1 ?   ?   INSERTION             159 41 1
            8 3RIJ GLU D 159 ? UNP Q5SHN1 ?   ?   INSERTION             160 42 1
            8 3RIJ HIS D 160 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      161 43 1
            8 3RIJ HIS D 161 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      162 44 1
            8 3RIJ HIS D 162 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      163 45 1
            8 3RIJ HIS D 163 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      164 46 1
            8 3RIJ HIS D 164 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      165 47 1
            8 3RIJ HIS D 165 ? UNP Q5SHN1 ?   ?   'EXPRESSION TAG'      166 48 1
            #
        """
        logger.debug("Starting with %r", dataContainer.getName())
        self.__addStructRefSeqEntityIds(dataContainer)
        #
        #  To exclude self references -
        excludeRefDbList = ["PDB"]
        rD = {"seqFeatureCountsD": {}, "seqMonomerFeatureD": {}, "seqRangeFeatureD": {}, "seqEntityAlignmentD": {}, "seqEntityRefDbD": {}}
        try:
            # Exit if source categories are missing
            if not (dataContainer.exists("struct_ref_seq") and dataContainer.exists("struct_ref")):
                return rD
            # ------- --------- ------- --------- ------- --------- ------- --------- ------- ---------
            srObj = None
            if dataContainer.exists("struct_ref"):
                srObj = dataContainer.getObj("struct_ref")
            #
            srsObj = None
            if dataContainer.exists("struct_ref_seq"):
                srsObj = dataContainer.getObj("struct_ref_seq")

            srsdObj = None
            if dataContainer.exists("struct_ref_seq_dif"):
                srsdObj = dataContainer.getObj("struct_ref_seq_dif")

            polymerEntityTypeD = self.getPolymerEntityFilteredTypes(dataContainer)
            # Map alignId -> entityId
            seqEntityRefDbD = {}
            tupSeqEntityRefDbD = {}
            alignEntityMapD = {}
            # entity alignment details
            seqEntityAlignmentD = {}
            for ii in range(srObj.getRowCount()):
                dbAccessionAlignS = set()
                entityId = srObj.getValue("entity_id", ii)
                refId = srObj.getValue("id", ii)
                dbName = str(srObj.getValue("db_name", ii)).strip().upper()
                #
                if dbName in excludeRefDbList:
                    continue
                #
                if entityId not in polymerEntityTypeD:
                    logger.warning("%s skipping non-polymer entity %r in sequence reference", dataContainer.getName(), entityId)
                    continue

                if dbName in ["UNP"] and polymerEntityTypeD[entityId] != "Protein":
                    logger.warning("%s skipping inconsistent reference assignment for %s polymer type %s", dataContainer.getName(), dbName, polymerEntityTypeD[entityId])
                    continue
                #
                tS = srObj.getValue("pdbx_db_accession", ii)
                dbAccession = tS if tS and tS not in [".", "?"] else None
                #
                tS = srObj.getValue("pdbx_db_isoform", ii)
                dbIsoform = tS if tS and tS not in [".", "?"] else None
                # Look for a stray isoform
                if dbName in ["UNP"] and dbAccession and "-" in dbAccession:
                    if not dbIsoform:
                        dbIsoform = dbAccession
                    ff = dbAccession.split("-")
                    dbAccession = ff[0]

                #
                if dbIsoform and dbAccession not in dbIsoform:
                    logger.warning("entryId %r entityId %r accession %r isoform %r inconsistency", dataContainer.getName(), entityId, dbAccession, dbIsoform)
                # ---
                # Get indices for the target refId.
                iRowL = srsObj.selectIndices(refId, "ref_id")
                logger.debug("entryId %r entityId %r refId %r rowList %r", dataContainer.getName(), entityId, refId, iRowL)

                for iRow in iRowL:
                    try:
                        entitySeqIdBeg = srsObj.getValue("seq_align_beg", iRow)
                        entitySeqIdEnd = srsObj.getValue("seq_align_end", iRow)
                        entityAlignLength = int(entitySeqIdEnd) - int(entitySeqIdBeg) + 1
                    except Exception:
                        entityAlignLength = 0
                    #
                    if entityAlignLength <= 0:
                        logger.warning("%s entity %r skipping bad alignment seqBeg %r seqEnd %r", dataContainer.getName(), entityId, entitySeqIdBeg, entitySeqIdEnd)
                        continue

                    alignId = srsObj.getValue("align_id", iRow)
                    alignEntityMapD[alignId] = entityId
                    #
                    authAsymId = srsObj.getValue("pdbx_strand_id", iRow)
                    dbSeqIdBeg = srsObj.getValue("db_align_beg", iRow)
                    dbSeqIdEnd = srsObj.getValue("db_align_end", iRow)
                    #
                    tS = srsObj.getValue("pdbx_db_accession", iRow)
                    # use the parent pdbx_accession
                    dbAccessionAlign = tS if tS and tS not in [".", "?"] else dbAccession
                    # Look for a stray isoform
                    if dbName in ["UNP"] and dbAccessionAlign and "-" in dbAccessionAlign:
                        if not dbIsoform:
                            dbIsoform = dbAccessionAlign
                        ff = dbAccessionAlign.split("-")
                        dbAccessionAlign = ff[0]

                    dbAccessionAlignS.add(dbAccessionAlign)
                    #
                    seqEntityAlignmentD.setdefault(entityId, []).append(
                        {
                            "authAsymId": authAsymId,
                            "entitySeqIdBeg": entitySeqIdBeg,
                            "entitySeqIdEnd": entitySeqIdEnd,
                            "dbSeqIdBeg": dbSeqIdBeg,
                            "dbSeqIdEnd": dbSeqIdEnd,
                            "dbName": dbName,
                            "dbAccession": dbAccessionAlign,
                            "dbIsoform": dbIsoform,
                            "entityAlignLength": entityAlignLength,
                        }
                    )
                # Check consistency
                try:
                    if len(dbAccessionAlignS) == 1 and list(dbAccessionAlignS)[0] == dbAccession:
                        tupSeqEntityRefDbD.setdefault(entityId, []).append((dbName, dbAccession, dbIsoform))
                    elif len(dbAccessionAlignS) == 1 and list(dbAccessionAlignS)[0]:
                        tupSeqEntityRefDbD.setdefault(entityId, []).append((dbName, list(dbAccessionAlignS)[0], None))
                    elif dbAccession:
                        tupSeqEntityRefDbD.setdefault(entityId, []).append((dbName, dbAccession, dbIsoform))
                    else:
                        logger.warning("%s entityId %r inconsistent reference sequence %r %r", dataContainer.getName(), entityId, dbAccession, dbAccessionAlignS)
                except Exception:
                    logger.exception("%s entityId %r inconsistent reference sequence %r %r", dataContainer.getName(), entityId, dbAccession, dbAccessionAlignS)

            # -----
            dbMapD = self.getDatabaseNameMap()
            for entityId, tupL in tupSeqEntityRefDbD.items():
                uTupL = list(OrderedDict({tup: True for tup in tupL}).keys())
                for tup in uTupL:
                    tS = dbMapD[tup[0]] if tup[0] in dbMapD else tup[0]
                    if tup[1]:
                        seqEntityRefDbD.setdefault(entityId, []).append({"dbName": tS, "dbAccession": tup[1], "dbIsoform": tup[2]})
                    else:
                        logger.warning("%s %s skipping incomplete sequence reference %r", dataContainer.getName(), entityId, tup)
            #
            # ------- --------- ------- --------- ------- --------- ------- --------- ------- ---------
            #   (entityId, seqId, compId, filteredFeature) -> set{details, ...}
            #
            seqFeatureCountsD = {}
            seqMonomerFeatureD = {}
            seqRangeFeatureD = {}
            entityArtifactD = {}
            seqIdDetailsD = {}
            if srsdObj:
                for ii in range(srsdObj.getRowCount()):
                    alignId = srsdObj.getValue("align_id", ii)
                    #
                    # entityId = alignEntityMapD[alignId]
                    entityId = srsdObj.getValueOrDefault("rcsb_entity_id", ii, defaultValue=None)
                    if not entityId:
                        continue
                    #
                    # authAsymId = srsdObj.getValue("pdbx_pdb_strand_id", ii)
                    dbName = srsdObj.getValue("pdbx_seq_db_name", ii)
                    #
                    # Can't rely on alignId
                    # Keep difference records for self-referenced entity sequences.
                    # if alignId not in alignEntityMapD and dbName not in excludeRefDbList:
                    #    logger.warning("%s inconsistent alignment ID %r in difference record %d", dataContainer.getName(), alignId, ii + 1)
                    #    continue
                    #
                    seqId = srsdObj.getValueOrDefault("seq_num", ii, defaultValue=None)
                    if not seqId:
                        continue
                    compId = srsdObj.getValue("mon_id", ii)
                    #
                    details = srsdObj.getValue("details", ii)
                    filteredDetails = self.filterRefSequenceDif(details)
                    if filteredDetails == "artifact":
                        try:
                            entityArtifactD.setdefault(entityId, []).append(int(seqId))
                            seqIdDetailsD[int(seqId)] = details.lower()
                        except Exception:
                            logger.warning("Incomplete sequence difference for %r %r %r %r", dataContainer.getName(), entityId, seqId, details)
                    else:
                        seqMonomerFeatureD.setdefault((entityId, seqId, compId, filteredDetails), set()).add(details.lower())
                #
                # Consolidate the artifacts as ranges -
                for entityId, sL in entityArtifactD.items():
                    # logger.debug("%s artifact ranges SL %r ranges %r", dataContainer.getName(), sL, list(self.__toRangeList(sL)))
                    srL = self.__toRangeList(sL)
                    for sr in srL:
                        seqRangeFeatureD.setdefault((entityId, str(sr[0]), str(sr[1]), "artifact"), set()).update([seqIdDetailsD[sr[0]], seqIdDetailsD[sr[1]]])
                # JDW
                # logger.info("%s seqMonomerFeatureD %r ", dataContainer.getName(), seqMonomerFeatureD)
                #
                # Tabulate sequence monomer features by entity for the filtered cases -
                for (entityId, _, _, fDetails), _ in seqMonomerFeatureD.items():
                    if entityId not in seqFeatureCountsD:
                        seqFeatureCountsD[entityId] = {"mutation": 0, "artifact": 0, "insertion": 0, "deletion": 0, "conflict": 0, "other": 0}
                    seqFeatureCountsD[entityId][fDetails] += 1
                #
                #
                # Tabulate sequence range features by entity for the filtered cases -
                for (entityId, _, _, fDetails), _ in seqRangeFeatureD.items():
                    if entityId not in seqFeatureCountsD:
                        seqFeatureCountsD[entityId] = {"mutation": 0, "artifact": 0, "insertion": 0, "deletion": 0, "conflict": 0, "other": 0}
                    seqFeatureCountsD[entityId][fDetails] += 1

            return {
                "seqFeatureCountsD": seqFeatureCountsD,
                "seqMonomerFeatureD": seqMonomerFeatureD,
                "seqRangeFeatureD": seqRangeFeatureD,
                "seqEntityAlignmentD": seqEntityAlignmentD,
                "seqEntityRefDbD": seqEntityRefDbD,
            }
        except Exception as e:
            logger.exception("%s failing with %s", dataContainer.getName(), str(e))
        return rD

    def __addStructRefSeqEntityIds(self, dataContainer):
        """ Add entity ids in categories struct_ref_seq and struct_ref_seq_dir instances.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            catName (str): Category name
            atName (str): Attribute name

        Returns:
            bool: True for success or False otherwise

        """
        try:
            catName = "struct_ref_seq"
            logger.debug("Starting with %r %r", dataContainer.getName(), catName)

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

    def filterRefSequenceDif(self, details):
        filteredDetails = details
        if details.upper() in [
            "ACETYLATION",
            "CHROMOPHORE",
            "VARIANT",
            "MODIFIED RESIDUE",
            "MODIFIED",
            "ENGINEERED",
            "ENGINEERED MUTATION",
            "AMIDATION",
            "FORMYLATION",
            "ALLELIC VARIANT",
            "AUTOPHOSPHORYLATION",
            "BENZOYLATION",
            "CHEMICAL MODIFICATION",
            "CHEMICALLY MODIFIED",
            "CHROMOPHOR, REM 999",
            "CHROMOPHORE, REM 999",
            "D-CONFIGURATION",
            "ENGINEERED AND OXIDIZED CYS",
            "ENGINEERED MUTANT",
            "ENGINERED MUTATION",
            "HYDROXYLATION",
            "METHYLATED ASN",
            "METHYLATION",
            "MICROHETEROGENEITY",
            "MODEIFED RESIDUE",
            "MODIFICATION",
            "MODIFIED AMINO ACID",
            "MODIFIED CHROMOPHORE",
            "MODIFIED GLN",
            "MODIFIED RESIDUES",
            "MUTATION",
            "MYC EPITOPE",
            "MYRISTOYLATED",
            "MYRISTOYLATION",
            "NATURAL VARIANT",
            "NATURAL VARIANTS",
            "OXIDIZED CY",
            "OXIDIZED CYS",
            "PHOSPHORYLATION",
            "POLYMORPHIC VARIANT",
            "PROPIONATION",
            "SOMATIC VARIANT",
            "SUBSTITUTION",
            "TRNA EDITING",
            "TRNA MODIFICATION",
            "TRNA",
            "VARIANT STRAIN",
            "VARIANTS",
        ]:
            filteredDetails = "mutation"
        elif details.upper() in [
            "LEADER SEQUENCE",
            "INITIATING METHIONINE",
            "INITIATOR METHIONINE",
            "LINKER",
            "EXPRESSION TAG",
            "CLONING",
            "CLONING ARTIFACT",
            "C-TERM CLONING ARTIFA",
            "C-TERMINAL HIS TAG",
            "C-TERMINLA HIS-TAG",
            "CLONING AETIFACT",
            "CLONING ARATIFACT",
            "CLONING ARTEFACT",
            "CLONING ARTFIACT",
            "CLONING ARTIACT",
            "CLONING ARTIFACTS",
            "CLONING ARTUFACT",
            "CLONING ATIFACT",
            "CLONING MUTATION",
            "CLONING REMNANT",
            "CLONING SITE RESIDUE",
            "CLONNG ARTIFACT",
            "CLONONG ARTIFACT",
            "DETECTION TAG",
            "ENGINEERED LINKER",
            "EXPRESSION ARTIFACT",
            "EXPRESSIOPN TAG",
            "EXPRSSION TAG",
            "FLAG TAG",
            "GCN4 TAG",
            "GPGS TAG",
            "GST TAG",
            "HIA TAG",
            "HIS TAG",
            "HIS-TAG",
            "INITIAL METHIONINE",
            "INITIATING MET",
            "INITIATING METHIONIE",
            "INITIATING MSE",
            "INITIATING RESIDUE",
            "INITIATOR N-FORMYL-MET",
            "INTIATING METHIONINE",
            "INTRACHAIN HIS TAG",
            "LINKER INSERTION",
            "LINKER PEPTIDE",
            "LINKER RESIDUE",
            "LINKER SEQUENCE",
            "LYS TAG",
            "MOD. RESIDUE/CLONING ARTIFACT",
            "MYC TAG",
            "N-TERMINAL EXTENSION",
            "N-TERMINAL HIS TAG",
            "PURIFICATION TAG",
            "RANDOM MUTAGENESIS",
            "RECOMBINANT HIS TAG",
            "RESIDUAL LINKER",
            "STREP-TAGII",
            "T7 EPITOPE TAG",
            "T7-TAG",
            "TAG",
        ]:
            filteredDetails = "artifact"
        elif details.upper() in ["INSERTION", "ENGINEERED INSERTION", "INSERTED", "INSERTION AT N-TERMINUS"]:
            filteredDetails = "insertion"
        elif details.upper() in ["DELETION", "CONFLICT/DELETION", "ENGINEERED DELETION"]:
            filteredDetails = "deletion"
        elif details.upper() in ["CONFLICT", "SEQUENCE CONFLICT", "SEQUENCE CONFLICT8"]:
            filteredDetails = "conflict"
        else:
            logger.debug("Unanticipated sequence difference details %r", details)
            filteredDetails = "other"
        #
        return filteredDetails

    def filterEntityPolyType(self, pType):
        """Map input dictionary polymer type to simplified molecular type.

        Args:
            pType (str): PDBx/mmCIF dictionary polymer type

        Returns:
            str: simplified mappings

        Returns mappings:
            'Protein'   'polypeptide(D) or polypeptide(L)'
            'DNA'       'polydeoxyribonucleotide'
            'RNA'       'polyribonucleotide'
            'NA-hybrid' 'polydeoxyribonucleotide/polyribonucleotide hybrid'
            'Other'      'polysaccharide(D), polysaccharide(L), cyclic-pseudo-peptide, peptide nucleic acid, or other'
        """
        polymerType = pType.lower()
        if polymerType in ["polypeptide(d)", "polypeptide(l)"]:
            rT = "Protein"
        elif polymerType in ["polydeoxyribonucleotide"]:
            rT = "DNA"
        elif polymerType in ["polyribonucleotide"]:
            rT = "RNA"
        elif polymerType in ["polydeoxyribonucleotide/polyribonucleotide hybrid"]:
            rT = "NA-hybrid"
        else:
            rT = "Other"
        return rT

    def guessEntityPolyTypes(self, monomerL):
        """ Guess the polymer types to from the monomer list.

        Args:
            monomerL (list): list of monomers (chemical component ids)

        Returns:
            tuple: polymerType, filtered polymer Type.

        Returns mappings:
            'Protein'   'polypeptide(D) or polypeptide(L)'
            'DNA'       'polydeoxyribonucleotide'
            'RNA'       'polyribonucleotide'
            'NA-hybrid' 'polydeoxyribonucleotide/polyribonucleotide hybrid'
            'Other'      'polysaccharide(D), polysaccharide(L), cyclic-pseudo-peptide, peptide nucleic acid, or other'
        """
        hasAA = hasDNA = hasRNA = False
        pType = fpType = None
        for monomer in monomerL:
            if monomer in DictMethodCommonUtils.aaDict3:
                hasAA = True
            elif monomer in DictMethodCommonUtils.dnaDict3:
                hasDNA = True
            elif monomer in DictMethodCommonUtils.rnaDict3:
                hasRNA = True
        #
        if hasAA and not hasDNA and not hasRNA:
            pType = "polypeptide(d)"
        elif hasDNA and not hasAA and not hasRNA:
            pType = "polydeoxyribonucleotide"
        elif hasRNA and not hasAA and not hasDNA:
            pType = "polyribonucleotide"
        elif not hasAA and hasDNA and hasRNA:
            pType = "polydeoxyribonucleotide/polyribonucleotide hybrid"

        if pType:
            fpType = self.filterEntityPolyType(pType)
        else:
            pType = None
            fpType = "Other"
        #
        return pType, fpType

    def getPolymerComposition(self, polymerTypeList):
        """ Map in list of dictionary entity polymer/branched types to a composition string.
            Input polymerTypeList contains entity_poly.type and entity_branch.type values.

        Args:
            polymerTypeList (list): List of PDBx/mmCIF dictionary polymer/branched types

        Returns:
            tuple: compClass, ptClass, naClass, cD

                   compClass - simplified composition string
                   ptClass - subset class
                   naClass - nucleic acid subset class
                   cD (dict) - composition type counts

        Current polymer type list:
             'polypeptide(D)'
             'polypeptide(L)'
             'polydeoxyribonucleotide'
             'polyribonucleotide'
             'polysaccharide(D)'
             'polysaccharide(L)'
             'polydeoxyribonucleotide/polyribonucleotide hybrid'
             'cyclic-pseudo-peptide'
             'peptide nucleic acid'
             'other'
             "other type pair (polymer type count = 2)"
             "other composition (polymer type count >= 3)"

        Current branch type list:
             'oligosaccharide'

        Output composition classes:

            'homomeric protein' 'single protein entity'
            'heteromeric protein' 'multiple protein entities'
            'DNA' 'DNA entity/entities only'
            'RNA' 'RNA entity/entities only'
            'NA-hybrid' 'DNA/RNA hybrid entity/entities only'
            'protein/NA' 'Both protein and nucleic acid polymer entities'
            'DNA/RNA' 'Both DNA and RNA polymer entities'
            'oligosaccharide' 'One of more oligosaccharide entities'
            'protein/oligosaccharide' 'Both protein and oligosaccharide entities'
            'NA/oligosaccharide' 'Both NA and oligosaccharide entities'
            'other' 'Neither an individual protein, nucleic acid polymer nor oligosaccharide entity'
            'other type pair' 'Other combinations of 2 polymer types'
            'other type composition' 'Other combinations of 3 or more polymer types'

        And selected types (ptClass)-
            'Protein (only)' 'protein entity/entities only'
            'Nucleic acid (only)' 'DNA, RNA or NA-hybrid entity/entities only'
            'Protein/NA' 'Both protein and nucleic acid (DNA, RNA, or NA-hybrid) polymer entities'
            'Other' 'Another polymer type composition'

        And selected NA types (naClass) -
            'DNA (only)' 'DNA entity/entities only'
            'RNA (only)' 'RNA entity/entities only'
            'NA-hybrid (only)' 'NA-hybrid entity/entities only'
            'DNA/RNA (only)' 'Both DNA and RNA polymer entities only'
            'Other' 'Another polymer type composition'
        """

        compClass = "other"
        # get type counts
        cD = {}
        for polymerType in polymerTypeList:
            if polymerType in ["polypeptide(D)", "polypeptide(L)"]:
                cD["protein"] = cD["protein"] + 1 if "protein" in cD else 1
            elif polymerType in ["polydeoxyribonucleotide"]:
                cD["DNA"] = cD["DNA"] + 1 if "DNA" in cD else 1
            elif polymerType in ["polyribonucleotide"]:
                cD["RNA"] = cD["RNA"] + 1 if "RNA" in cD else 1
            elif polymerType in ["polydeoxyribonucleotide/polyribonucleotide hybrid"]:
                cD["NA-hybrid"] = cD["NA-hybrid"] + 1 if "NA-hybrid" in cD else 1
            elif polymerType in ["oligosaccharide"]:
                cD["oligosaccharide"] = cD["oligosaccharide"] + 1 if "oligosaccharide" in cD else 1
            else:
                cD["other"] = cD["other"] + 1 if "other" in cD else 1
        #
        if len(cD) == 1:
            ky = list(cD.keys())[0]
            if "protein" in cD:
                if cD["protein"] == 1:
                    compClass = "homomeric protein"
                else:
                    compClass = "heteromeric protein"
            elif ky in ["DNA", "RNA", "NA-hybrid", "oligosaccharide", "other"]:
                compClass = ky
        elif len(cD) == 2:
            if "protein" in cD:
                if ("DNA" in cD) or ("RNA" in cD) or ("NA-hybrid" in cD):
                    compClass = "protein/NA"
                elif "oligosaccharide" in cD:
                    compClass = "protein/oligosaccharide"
            elif "DNA" in cD and "RNA" in cD:
                compClass = "DNA/RNA"
            elif "oligosaccharide" in cD and ("RNA" in cD or "DNA" in cD):
                compClass = "NA/oligosaccharide"
            else:
                compClass = "other type pair"
        elif len(cD) == 3:
            if "DNA" in cD and "RNA" in cD and "NA-hybrid" in cD:
                compClass = "DNA/RNA"
            elif "oligosaccharide" in cD and all([cD[j] in ["oligosaccharide", "DNA", "RNA", "NA-hybrid"] for j in cD]):
                compClass = "NA/oligosaccharide"
            elif "protein" in cD and all([cD[j] in ["protein", "DNA", "RNA", "NA-hybrid"] for j in cD]):
                compClass = "protein/NA"
            elif "oligosaccharide" in cD and "protein" in cD and all([cD[j] in ["protein", "oligosaccharide", "DNA", "RNA", "NA-hybrid"] for j in cD]):
                compClass = "protein/NA/oligosaccharide"
            else:
                compClass = "other type composition"
        elif len(cD) >= 4:
            if "oligosaccharide" in cD and all([cD[j] in ["oligosaccharide", "DNA", "RNA", "NA-hybrid"] for j in cD]):
                compClass = "NA/oligosaccharide"
            elif "protein" in cD and all([cD[j] in ["protein", "DNA", "RNA", "NA-hybrid"] for j in cD]):
                compClass = "protein/NA"
            elif "oligosaccharide" in cD and "protein" in cD and all([cD[j] in ["protein", "oligosaccharide", "DNA", "RNA", "NA-hybrid"] for j in cD]):
                compClass = "protein/NA/oligosaccharide"
            else:
                compClass = "other type composition"
        else:
            compClass = "other type composition"

        # Subset type class --
        #
        if compClass in ["homomeric protein", "heteromeric protein"]:
            ptClass = "Protein (only)"
        elif compClass in ["DNA", "RNA", "NA-hybrid"]:
            ptClass = "Nucleic acid (only)"
        elif compClass in ["protein/NA"]:
            ptClass = "Protein/NA"
        else:
            ptClass = "Other"
        #
        # NA subtype class ---
        #
        if compClass in ["DNA"]:
            naClass = "DNA (only)"
        elif compClass in ["RNA"]:
            naClass = "RNA (only)"
        elif compClass in ["NA-hybrid"]:
            naClass = "NA-hybrid (only)"
        elif compClass in ["DNA/RNA"]:
            naClass = "DNA/RNA (only)"
        else:
            naClass = "Other"
        #
        return compClass, ptClass, naClass, cD

    def filterExperimentalMethod(self, methodL):
        """ Apply a standard filter to the input experimental method list returning a method count and
            a simplified method name.

        Args:
            methodL (list): List of dictionary compliant experimental method names

        Returns:
            tuple(int,str): methodCount, simpleMethodName

        For example:
        'X-ray'            'X-RAY DIFFRACTION, FIBER DIFFRACTION, or POWDER DIFFRACTION'
        'NMR'              'SOLUTION NMR or SOLID-STATE NMR'
        'EM'               'ELECTRON MICROSCOPY or ELECTRON CRYSTALLOGRAPHY or ELECTRON TOMOGRAPHY'
        'Neutron'          'NEUTRON DIFFRACTION'
        'Multiple methods' 'Multiple experimental methods'
        'Other'            'SOLUTION SCATTERING, EPR, THEORETICAL MODEL, INFRARED SPECTROSCOPY or FLUORESCENCE TRANSFER'
        """
        methodCount = len(methodL)
        if methodCount > 1:
            expMethod = "Multiple methods"
        else:
            #
            mS = methodL[0].upper()
            expMethod = "Other"
            if mS in ["X-RAY DIFFRACTION", "FIBER DIFFRACTION", "POWDER DIFFRACTION"]:
                expMethod = "X-ray"
            elif mS in ["SOLUTION NMR", "SOLID-STATE NMR"]:
                expMethod = "NMR"
            elif mS in ["ELECTRON MICROSCOPY", "ELECTRON CRYSTALLOGRAPHY", "ELECTRON DIFFRACTION", "CRYO-ELECTRON MICROSCOPY", "ELECTRON TOMOGRAPHY"]:
                expMethod = "EM"
            elif mS in ["NEUTRON DIFFRACTION"]:
                expMethod = "Neutron"
            elif mS in ["SOLUTION SCATTERING", "EPR", "THEORETICAL MODEL", "INFRARED SPECTROSCOPY", "FLUORESCENCE TRANSFER"]:
                expMethod = "Other"
            else:
                logger.error("Unexpected method ")

        return methodCount, expMethod

    def hasMethodNMR(self, methodL):
        """Return if the input dictionary experimental method list contains an NMR experimental method.

        Args:
            methodL (list): List of dictionary experimental method names

        Returns:
            bool: True if the input contains NMR or False otherwise
        """
        ok = False
        for method in methodL:
            if method in ["SOLUTION NMR", "SOLID-STATE NMR"]:
                return True
        return ok

    def __getTimeStamp(self):
        utcnow = datetime.datetime.utcnow()
        ts = utcnow.strftime("%Y-%m-%d:%H:%M:%S")
        return ts

    def __stripWhiteSpace(self, val):
        """ Remove all white space from the input value.

        """
        if val is None:
            return val
        return self.__wsPattern.sub("", val)

    def __toRangeList(self, iterable):
        iterable = sorted(set(iterable))
        for _, group in itertools.groupby(enumerate(iterable), lambda t: t[1] - t[0]):
            group = list(group)
            yield group[0][1], group[-1][1]

    #
    def getTargetSiteInfo(self, dataContainer):
        """Return a dictionary of target site binding interactions using standard nomenclature.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {site_id: [{'asymId': , 'compId': , 'seqId': }, ...],  ... }

        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchInstanceSiteInfo(dataContainer)
        return wD["targetSiteD"] if "targetSiteD" in wD else {}

    def getLigandSiteInfo(self, dataContainer):
        """Return a dictionary of ligand site binding interactions.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {site_id: {"evCode": software|author,
                            "fromDetails": True|False,
                            "isRaw": True|False,
                            "entityType": polymer|non-polymer,
                            "polymerLigand": {"asymId": ., "entityId": ., "begSeqId": ., "endSeqId":. },
                            "nonPolymerLigands": [{"asymId": ., "entityId": ., "compId": .}, ...],
                            "description": raw or generated text,
                            }
                            }
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchInstanceSiteInfo(dataContainer)
        return wD["ligandSiteD"] if "ligandSiteD" in wD else {}

    def __fetchInstanceSiteInfo(self, dataContainer):
        wD = self.__instanceSiteInfoCache.get(dataContainer.getName())
        if not wD:
            wD = self.__getInstanceSiteInfo(dataContainer)
            self.__instanceSiteInfoCache.set(dataContainer.getName(), wD)
        return wD

    def __getInstanceSiteInfo(self, dataContainer):
        """[summary]

        Args:
            dataContainer (object): mmif.api.DataContainer object instance

        Returns:
            dict : {"targetSiteD" = {<site_id>: {}}
                    "ligandSiteD": {<site_id>: {}}
                    }

        For example:

                loop_
                _struct_site.id
                _struct_site.pdbx_evidence_code
                _struct_site.pdbx_auth_asym_id
                _struct_site.pdbx_auth_comp_id
                _struct_site.pdbx_auth_seq_id
                _struct_site.pdbx_auth_ins_code # never used
                _struct_site.pdbx_num_residues
                _struct_site.details
                AC1 Software ? ? ? ? 7  'BINDING SITE FOR RESIDUE ADP A 105'
                AC2 Software ? ? ? ? 16 'BINDING SITE FOR RESIDUE ADP B 101'
                AC3 Software ? ? ? ? 6  'BINDING SITE FOR RESIDUE MG B 66'
                AC4 Software ? ? ? ? 13 'BINDING SITE FOR RESIDUE ADP C 102'
                AC5 Software ? ? ? ? 16 'BINDING SITE FOR RESIDUE ADP E 103'
                AC6 Software ? ? ? ? 10 'BINDING SITE FOR RESIDUE ADP F 104'
                AC7 Software ? ? ? ? 6  'BINDING SITE FOR RESIDUE MG K 9'
                #
                loop_
                _struct_site_gen.id
                _struct_site_gen.site_id
                _struct_site_gen.pdbx_num_res
                _struct_site_gen.label_comp_id
                _struct_site_gen.label_asym_id
                _struct_site_gen.label_seq_id
                _struct_site_gen.pdbx_auth_ins_code
                _struct_site_gen.auth_comp_id
                _struct_site_gen.auth_asym_id
                _struct_site_gen.auth_seq_id
                _struct_site_gen.label_atom_id
                _struct_site_gen.label_alt_id
                _struct_site_gen.symmetry
                _struct_site_gen.details
                1  AC1 7  TYR A 25 ? TYR A 25  . ? 1_555 ?
                2  AC1 7  GLY A 29 ? GLY A 29  . ? 1_555 ?
                3  AC1 7  THR A 61 ? THR A 61  . ? 1_555 ?
                4  AC1 7  VAL A 63 ? VAL A 63  . ? 1_555 ?
                5  AC1 7  ILE B 30 ? ILE B 30  . ? 1_555 ?
                6  AC1 7  LEU B 32 ? LEU B 32  . ? 1_555 ?
                7  AC1 7  GLN B 52 ? GLN B 52  . ? 1_555 ?
                8  AC2 16 TYR B 25 ? TYR B 25  . ? 1_555 ?
                9  AC2 16 LEU B 26 ? LEU B 26  . ? 1_555 ?
                10 AC2 16 GLY B 29 ? GLY B 29  . ? 1_555 ?
                11 AC2 16 LYS B 31 ? LYS B 31  . ? 1_555 ?
                12 AC2 16 SER B 60 ? SER B 60  . ? 1_555 ?
                13 AC2 16 THR B 61 ? THR B 61  . ? 1_555 ?
                14 AC2 16 HOH P .  ? HOH B 113 . ? 1_555 ?
                15 AC2 16 HOH P .  ? HOH B 116 . ? 1_555 ?
                16 AC2 16 HOH P .  ? HOH B 201 . ? 1_555 ?
                17 AC2 16 HOH P .  ? HOH B 241 . ? 1_555 ?
                18 AC2 16 LEU C 26 ? LEU C 26  . ? 1_555 ?
                19 AC2 16 ASN C 28 ? ASN C 28  . ? 1_555 ?
                20 AC2 16 ILE C 30 ? ILE C 30  . ? 1_555 ?
                21 AC2 16 LEU C 32 ? LEU C 32  . ? 1_555 ?
                22 AC2 16 ARG F 16 ? ARG F 16  . ? 1_565 ?
                23 AC2 16 ARG F 17 ? ARG F 17  . ? 1_565 ?
        """
        logger.debug("Starting with %r", dataContainer.getName())
        #
        rD = {"targetSiteD": {}, "ligandSiteD": {}}
        try:
            # Exit if source categories are missing
            if not (dataContainer.exists("struct_site") and dataContainer.exists("struct_site_gen")):
                return rD
            # ------- --------- ------- --------- ------- --------- ------- --------- ------- ---------
            ssObj = None
            if dataContainer.exists("struct_site"):
                ssObj = dataContainer.getObj("struct_site")
            #
            ssgObj = None
            if dataContainer.exists("struct_site_gen"):
                ssgObj = dataContainer.getObj("struct_site_gen")

            #
            ligandSiteD = {}
            for ii in range(ssObj.getRowCount()):
                ligL = []
                evCode = str(ssObj.getValue("pdbx_evidence_code", ii)).lower()
                if evCode not in ["software", "author"]:
                    continue
                sId = ssObj.getValue("id", ii)
                authAsymId = ssObj.getValueOrDefault("pdbx_auth_asym_id", ii, defaultValue=None)
                compId = ssObj.getValueOrDefault("pdbx_auth_comp_id", ii, defaultValue=None)
                authSeqId = ssObj.getValueOrDefault("pdbx_auth_seq_id", ii, defaultValue=None)
                ssDetails = ssObj.getValueOrDefault("details", ii, defaultValue=None)
                fromDetails = False
                if authAsymId:
                    ligL.append((authAsymId, compId, authSeqId, ssDetails))
                else:
                    fromDetails = True
                    if evCode == "software":
                        ligL = self.__parseStructSiteLigandDetails(ssDetails)
                    elif evCode == "author":
                        ligL.append((None, None, None, ssDetails))
                #
                ligandSiteD[sId] = self.__transStructSiteLigandDetails(dataContainer, ligL, evCode=evCode, fromDetails=fromDetails)
            #

            targetSiteD = {}
            instTypeD = self.getInstanceTypes(dataContainer)
            for ii in range(ssgObj.getRowCount()):
                sId = ssgObj.getValue("site_id", ii)
                asymId = ssgObj.getValueOrDefault("label_asym_id", ii, defaultValue=None)
                compId = ssgObj.getValueOrDefault("label_comp_id", ii, defaultValue=None)
                seqId = ssgObj.getValueOrDefault("label_seq_id", ii, defaultValue=None)
                #
                if asymId and compId and seqId and asymId in instTypeD and instTypeD[asymId] == "polymer":
                    targetSiteD.setdefault(sId, []).append({"asymId": asymId, "compId": compId, "seqId": seqId})
            #
            return {"targetSiteD": targetSiteD, "ligandSiteD": ligandSiteD}
        except Exception as e:
            logger.exception("%s failing with %s", dataContainer.getName(), str(e))
        return rD

    def __transStructSiteLigandDetails(self, dataContainer, ligL, evCode="software", fromDetails=True):
        """Convert struct_site ligand details to standard nomenclature.

        Args:
            dataContainer (object): mmif.api.DataContainer object instance
            ligL (list): list of raw ligand details in author nomenclature
            evCode (str):  string  (software|author)
            fromDetails (bool, optional): details parsed from descriptive text. Defaults to True.

        Returns:
            dict: {"evCode": software|author,
                   "fromDetails": True|False,
                   "isRaw": True|False,
                   "entityType": polymer|non-polymer,
                   "polymerLigand": {"asymId": ., "entityId": ., "begSeqId": ., "endSeqId":. },
                   "nonPolymerLigands": [{"asymId": ., "entityId": ., "compId": .}, ...],
                   "description": raw or generated text,
                   }

        """
        rD = {"evCode": evCode, "fromDetails": fromDetails, "isRaw": True, "entityType": None, "polymerLigand": None, "nonPolymerLigands": None, "description": None}
        npAuthAsymD = self.getNonPolymerIdMap(dataContainer)
        pAuthAsymD = self.getPolymerIdMap(dataContainer)
        asymAuthIdD = self.getAsymAuthIdMap(dataContainer)
        asymIdPolymerRangesD = self.getInstancePolymerRanges(dataContainer)
        iTypeD = self.getInstanceTypes(dataContainer)
        asymAuthIdD = self.getAsymAuthIdMap(dataContainer)
        # Note that this is a non-unique index inversion
        authAsymD = {v: k for k, v in asymAuthIdD.items()}
        instEntityD = self.getInstanceEntityMap(dataContainer)
        #
        if len(ligL) == 1:
            authAsymId, compId, authSeqId, ssDetails = ligL[0]
            #
            if not authAsymId:
                rD["description"] = ssDetails
                rD["isRaw"] = True
            elif not authSeqId:
                # An unqualified authAsymId -
                asymId = authAsymD[authAsymId] if authAsymId in authAsymD else None
                entityId = instEntityD[asymId] if asymId in instEntityD else None
                if entityId and asymId and asymId in iTypeD and iTypeD[asymId] == "polymer" and asymId in asymIdPolymerRangesD:
                    # insert the full residue range -
                    rD["entityType"] = iTypeD[asymId]
                    begSeqId = asymIdPolymerRangesD[asymId]["begSeqId"]
                    endSeqId = asymIdPolymerRangesD[asymId]["endSeqId"]
                    tD = {"asymId": asymId, "entityId": instEntityD[asymId], "begSeqId": begSeqId, "endSeqId": endSeqId}
                    rD["description"] = "Binding site for entity %s instance %s (%s-%s)" % (entityId, asymId, begSeqId, endSeqId)
                    rD["polymerLigand"] = tD
            elif (authAsymId, authSeqId) in npAuthAsymD:
                # single non-polymer-ligand -
                asymId = npAuthAsymD[(authAsymId, authSeqId)]["asym_id"]
                rD["entityType"] = iTypeD[asymId]
                entityId = instEntityD[asymId]
                tD = {"asymId": asymId, "entityId": instEntityD[asymId], "compId": compId}
                rD["nonPolymerLigands"] = [tD]
                rD["description"] = "Binding site for ligand entity %s component %s instance %s" % (entityId, compId, asymId)
            elif (authAsymId, authSeqId, None) in pAuthAsymD:
                # single monomer ligand - an odd case
                asymId = pAuthAsymD[(authAsymId, authSeqId, None)]["asym_id"]
                entityId = pAuthAsymD[(authAsymId, authSeqId, None)]["entity_id"]
                seqId = pAuthAsymD[(authAsymId, authSeqId, None)]["seq_id"]
                rD["entityType"] = iTypeD[asymId]
                tD = {"asymId": asymId, "entityId": entityId, "begSeqId": seqId, "endSeqId": seqId}
                rD["description"] = "Binding site for entity %s instance %s (%s)" % (entityId, asymId, seqId)
                rD["polymerLigand"] = tD
            else:
                logger.debug("%s untranslated single ligand details %r", dataContainer.getName(), ligL)
                logger.debug("npAuthAsymD %r", npAuthAsymD)
                rD["description"] = ssDetails
                rD["isRaw"] = True
            #
        elif len(ligL) == 2:
            authAsymIdA, compIdA, authSeqIdA, ssDetailsA = ligL[0]
            authAsymIdB, compIdB, authSeqIdB, _ = ligL[1]
            #
            # is np
            if (authAsymIdA, authSeqIdA) in npAuthAsymD and (authAsymIdB, authSeqIdB) in npAuthAsymD:
                asymIdA = npAuthAsymD[(authAsymIdA, authSeqIdA)]["asym_id"]
                entityIdA = npAuthAsymD[(authAsymIdA, authSeqIdA)]["entity_id"]
                asymIdB = npAuthAsymD[(authAsymIdB, authSeqIdB)]["asym_id"]
                entityIdB = npAuthAsymD[(authAsymIdB, authSeqIdB)]["entity_id"]
                tDA = {"asymId": asymIdA, "entityId": entityIdA, "compId": compIdA}
                tDB = {"asymId": asymIdB, "entityId": entityIdB, "compId": compIdB}
                rD["nonPolymerLigands"] = [tDA, tDB]
                rD["entityType"] = iTypeD[asymIdA]
                rD["description"] = "Binding site for ligands entity %s component %s instance %s and entity %s component %s instance %s" % (
                    entityIdA,
                    compIdA,
                    asymIdA,
                    entityIdB,
                    compIdB,
                    asymIdB,
                )
            elif (authAsymIdA, authSeqIdA, None) in pAuthAsymD and (authAsymIdB, authSeqIdB, None) in pAuthAsymD and authAsymIdA == authAsymIdB:
                asymIdA = pAuthAsymD[(authAsymIdA, authSeqIdA, None)]["asym_id"]
                entityIdA = pAuthAsymD[(authAsymIdA, authSeqIdA, None)]["entity_id"]
                asymIdB = pAuthAsymD[(authAsymIdB, authSeqIdB, None)]["asym_id"]
                entityIdB = pAuthAsymD[(authAsymIdB, authSeqIdB, None)]["entity_id"]
                begSeqId = pAuthAsymD[(authAsymIdA, authSeqIdA, None)]["seq_id"]
                endSeqId = pAuthAsymD[(authAsymIdB, authSeqIdB, None)]["seq_id"]
                tD = {"asymId": asymIdA, "entityId": instEntityD[asymIdA], "begSeqId": begSeqId, "endSeqId": endSeqId}
                rD["entityType"] = iTypeD[asymIdA]
                rD["description"] = "Binding site for ligands entity %s instance %s and entity %s instance %s" % (entityIdA, asymIdA, entityIdB, asymIdB)
                rD["polymerLigand"] = tD
            else:
                logger.debug("%s untranslated ligand details %r", dataContainer.getName(), ligL)
                rD["description"] = ssDetailsA
                rD["isRaw"] = True
        else:
            logger.error("%s unexpected ligand expression %r", dataContainer.getName(), ligL)
        return rD

    def __parseStructSiteLigandDetails(self, ssDetails):
        """Parse the input site description text and returning structured details
        where possible.

        Args:
            ssDetails (str): struct_site.details text

        Returns:
            list: [(authAsymId, compId, authSeqId, ssDetails), ... ]

        """
        retL = []
        #
        try:
            if not ssDetails:
                retL.append((None, None, None, None))
                return retL
            prefixL = [
                "BINDING SITE FOR RESIDUE ",
                "binding site for residue ",
                "Binding site for Ligand ",
                "binding site for Ligand ",
                "Binding site for Mono-Saccharide ",
                "BINDING SITE FOR MONO-SACCHARIDE ",
                "binding site for Mono-Saccharide ",
                "binding site for Poly-Saccharide ",
                "binding site for nucleotide ",
            ]
            for prefix in prefixL:
                tup = ssDetails.partition(prefix)
                if tup[1] == prefix:
                    ff = tup[2].split(" ")
                    # binding site for Ligand residues POL d 4 through N7P d 1 bound to THR b 1
                    if ff[0] == "residues" and len(ff) > 8 and ff[4].lower() == "through":
                        compIdA = ff[1]
                        authAsymIdA = ff[2]
                        authSeqIdA = ff[3]
                        retL.append((authAsymIdA, compIdA, authSeqIdA, ssDetails))
                        #
                        compIdB = ff[5]
                        authAsymIdB = ff[6]
                        authSeqIdB = ff[7]
                        retL.append((authAsymIdB, compIdB, authSeqIdB, ssDetails))
                        return retL
                    elif len(ff) == 2:
                        compId = ff[0]
                        authAsymId = ff[1][0]
                        authSeqId = ff[1][1:]
                        retL.append((authAsymId, compId, authSeqId, ssDetails))
                        return retL
                    elif len(ff) == 3:
                        compId = ff[0]
                        authAsymId = ff[1]
                        authSeqId = ff[2]
                        retL.append((authAsymId, compId, authSeqId, ssDetails))
                        return retL

            #
            # Binding site for residues GCD A 900 and NGA A 901
            # Binding site for residues FUC A1118 and BGC A1119'
            prefixL = [
                "Binding site for residues ",
                "binding site for residues ",
                "BINDING SITE FOR DI-SACCHARIDE ",
                "Binding site for Di-Saccharide ",
                "binding site for Di-Saccharide ",
                "binding site for Di-peptide ",
                "Binding site for Di-peptide ",
                "binding site for Di-nucleotide ",
            ]
            for prefix in prefixL:
                tup = ssDetails.partition(prefix)
                if tup[1] == prefix:
                    ff = tup[2].split(" ")
                    if len(ff) == 5:
                        compIdA = ff[0]
                        authAsymIdA = ff[1][0]
                        authSeqIdA = ff[1][1:]
                        compIdB = ff[3]
                        authAsymIdB = ff[4][0]
                        authSeqIdB = ff[4][1:]
                    elif len(ff) == 7:
                        compIdA = ff[0]
                        authAsymIdA = ff[1]
                        authSeqIdA = ff[2]
                        compIdB = ff[4]
                        authAsymIdB = ff[5]
                        authSeqIdB = ff[6]
                    else:
                        compIdA = authAsymIdA = authSeqIdA = compIdB = authAsymIdB = authSeqIdB = None

                    retL.append((authAsymIdA, compIdA, authSeqIdA, ssDetails))
                    retL.append((authAsymIdB, compIdB, authSeqIdB, ssDetails))
                    return retL
            #
            # BINDING SITE FOR LINKED RESIDUES A 1519 A 1520 A 1521 A 1522 A 1523 A 1524 A 1525
            # BINDING SITE FOR LINKED RESIDUES A 801 to 802
            prefixL = ["BINDING SITE FOR LINKED RESIDUES "]
            for prefix in prefixL:
                tup = ssDetails.partition(prefix)
                if tup[1] == prefix:
                    ff = tup[2].split(" ")
                    if len(ff) == 2:
                        # BINDING SITE FOR LINKED RESIDUES A 502-507
                        try:
                            tff = ff[1].split("-")
                            authAsymIdA = ff[0]
                            authSeqIdA = tff[0]
                            authSeqIdB = tff[1]
                        except Exception:
                            continue
                    if len(ff) == 4 and ff[2].lower() == "to":
                        authAsymIdA = ff[0]
                        authSeqIdA = ff[1]
                        authSeqIdB = ff[3]
                    elif len(ff) == 4 and ff[2].lower() != "to":
                        authAsymIdA = ff[0]
                        authSeqIdA = ff[1]
                        authSeqIdB = ff[3]
                    elif len(ff) > 4:
                        authAsymIdA = ff[0]
                        authSeqIdA = ff[1]
                        authSeqIdB = ff[-1]
                    else:
                        continue
                    retL.append((authAsymIdA, None, authSeqIdA, ssDetails))
                    retL.append((authAsymIdA, None, authSeqIdB, ssDetails))
                    return retL

            #
            #
            prefixL = ["BINDING SITE FOR CHAIN ", "binding site for chain "]
            for prefix in prefixL:
                tup = ssDetails.partition(prefix)
                if tup[1] == prefix:
                    ff = tup[2].split(" ")
                    authAsymId = ff[0]
                    retL.append((authAsymId, None, None, ssDetails))
                    return retL
            # punt -
            retL.append((None, None, None, ssDetails))
            return retL
        except Exception as e:
            logger.exception("Failing with %s for %r", str(e), ssDetails)
        return [(None, None, None, ssDetails)]

    def getUnobservedPolymerResidueInfo(self, dataContainer):
        """Return a dictionary of unobserved regions of polymer instances.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {(modelId, asymId, occFlag): [seqId range list], ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchUnobservedInfo(dataContainer)
        return wD["polyResRng"] if "polyResRng" in wD else {}

    def getUnobservedPolymerAtomInfo(self, dataContainer):
        """Return a dictionary of polymer regions containing unobserved atoms.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {(modelId, asymId, occFlag): [seqId range list], ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchUnobservedInfo(dataContainer)
        return wD["polyAtomRng"] if "polyAtomRng" in wD else {}

    def __fetchUnobservedInfo(self, dataContainer):
        wD = self.__instanceUnobservedCache.get(dataContainer.getName())
        if not wD:
            wD = self.__getUnobserved(dataContainer)
            self.__instanceUnobservedCache.set(dataContainer.getName(), wD)
        return wD

    def __getUnobserved(self, dataContainer):
        """ Internal method to extract unobserved and zero occupancy features.

        Args:
            dataContainer ([type]): [description]

        Returns:
            {"polyResRng":  {(modelId, asymId, occFlag): [seqId range list], ...},
             "polyAtomRng": {(modelId, asymId, occFlag): [seqId range list], ...},
             }

            occFlag = 0 - zero occupancy
        Example:

                loop_
                _pdbx_unobs_or_zero_occ_atoms.id
                _pdbx_unobs_or_zero_occ_atoms.PDB_model_num
                _pdbx_unobs_or_zero_occ_atoms.polymer_flag
                _pdbx_unobs_or_zero_occ_atoms.occupancy_flag
                _pdbx_unobs_or_zero_occ_atoms.auth_asym_id
                _pdbx_unobs_or_zero_occ_atoms.auth_comp_id
                _pdbx_unobs_or_zero_occ_atoms.auth_seq_id
                _pdbx_unobs_or_zero_occ_atoms.PDB_ins_code
                _pdbx_unobs_or_zero_occ_atoms.auth_atom_id
                _pdbx_unobs_or_zero_occ_atoms.label_alt_id
                _pdbx_unobs_or_zero_occ_atoms.label_asym_id
                _pdbx_unobs_or_zero_occ_atoms.label_comp_id
                _pdbx_unobs_or_zero_occ_atoms.label_seq_id
                _pdbx_unobs_or_zero_occ_atoms.label_atom_id
                1  1 Y 1 B ARG 17  ? NE    ? B ARG 17 NE
                2  1 Y 1 B ARG 17  ? CZ    ? B ARG 17 CZ
                3  1 Y 1 B ARG 17  ? NH1   ? B ARG 17 NH1

                #
                loop_
                _pdbx_unobs_or_zero_occ_residues.id
                _pdbx_unobs_or_zero_occ_residues.PDB_model_num
                _pdbx_unobs_or_zero_occ_residues.polymer_flag
                _pdbx_unobs_or_zero_occ_residues.occupancy_flag
                _pdbx_unobs_or_zero_occ_residues.auth_asym_id
                _pdbx_unobs_or_zero_occ_residues.auth_comp_id
                _pdbx_unobs_or_zero_occ_residues.auth_seq_id
                _pdbx_unobs_or_zero_occ_residues.PDB_ins_code
                _pdbx_unobs_or_zero_occ_residues.label_asym_id
                _pdbx_unobs_or_zero_occ_residues.label_comp_id
                _pdbx_unobs_or_zero_occ_residues.label_seq_id
                1  1 Y 1 A MET 1 ? A MET 1
                2  1 Y 1 A ALA 2 ? A ALA 2
                3  1 Y 1 A LYS 3 ? A LYS 3
        """
        logger.debug("Starting with %r", dataContainer.getName())
        #
        rD = {}
        try:
            # Exit if source categories are missing
            if not (dataContainer.exists("pdbx_unobs_or_zero_occ_residues") or dataContainer.exists("pdbx_unobs_or_zero_occ_atoms")):
                return rD
            # ------- --------- ------- --------- ------- --------- ------- --------- ------- ---------
            resObj = None
            if dataContainer.exists("pdbx_unobs_or_zero_occ_residues"):
                resObj = dataContainer.getObj("pdbx_unobs_or_zero_occ_residues")
            #
            atomObj = None
            if dataContainer.exists("pdbx_unobs_or_zero_occ_atoms"):
                atomObj = dataContainer.getObj("pdbx_unobs_or_zero_occ_atoms")
            #
            polyResRngD = {}
            if resObj:
                for ii in range(resObj.getRowCount()):
                    modelId = resObj.getValueOrDefault("PDB_model_num", ii, defaultValue=None)
                    pFlag = resObj.getValueOrDefault("polymer_flag", ii, defaultValue=None)
                    if pFlag == "Y":
                        occFlag = resObj.getValueOrDefault("occupancy_flag", ii, defaultValue=None)
                        zeroOccFlag = int(occFlag) == 0
                        asymId = resObj.getValueOrDefault("label_asym_id", ii, defaultValue=None)
                        # authAsymId = resObj.getValueOrDefault("auth_asym_id", ii, defaultValue=None)
                        seqId = resObj.getValueOrDefault("label_seq_id", ii, defaultValue=None)
                        if seqId:
                            polyResRngD.setdefault((modelId, asymId, zeroOccFlag), []).append(int(seqId))
                #
                for tup in polyResRngD:
                    polyResRngD[tup] = list(self.__toRangeList(polyResRngD[tup]))
                logger.debug("polyResRngD %r", polyResRngD)
            #
            polyAtomRngD = {}
            if atomObj:
                for ii in range(atomObj.getRowCount()):
                    modelId = atomObj.getValueOrDefault("PDB_model_num", ii, defaultValue=None)
                    pFlag = atomObj.getValueOrDefault("polymer_flag", ii, defaultValue=None)
                    if pFlag == "Y":
                        occFlag = atomObj.getValueOrDefault("occupancy_flag", ii, defaultValue=None)
                        zeroOccFlag = occFlag and int(occFlag) == 0
                        asymId = atomObj.getValueOrDefault("label_asym_id", ii, defaultValue=None)
                        # authAsymId = resObj.getValueOrDefault("auth_asym_id", ii, defaultValue=None)
                        seqId = atomObj.getValueOrDefault("label_seq_id", ii, defaultValue=None)
                        if seqId:
                            polyAtomRngD.setdefault((modelId, asymId, zeroOccFlag), []).append(int(seqId))
                #
                for tup in polyAtomRngD:
                    polyAtomRngD[tup] = list(self.__toRangeList(polyAtomRngD[tup]))
                logger.debug("polyAtomRngD %r", polyAtomRngD)
            #
            rD = {"polyResRng": polyResRngD, "polyAtomRng": polyAtomRngD}
        except Exception as e:
            logger.exception("%s failing with %s", dataContainer.getName(), str(e))
        return rD

    def getInstanceModelOutlierInfo(self, dataContainer):
        """Return a dictionary of polymer model outliers.

        Args:
            dataContainer (object):  mmcif.api.mmif.api.DataContainer object instance

        Returns:
            dict: {(modelId, asymId): (seqId,compId), ...}
        """
        if not dataContainer or not dataContainer.getName():
            return {}
        wD = self.__fetchInstanceModelOutliers(dataContainer)
        return wD["instanceModelOutlierD"] if "instanceModelOutlierD" in wD else {}

    def __fetchInstanceModelOutliers(self, dataContainer):
        wD = self.__modelOutliersCache.get(dataContainer.getName())
        if not wD:
            wD = self.__getInstanceModelOutliers(dataContainer)
            self.__modelOutliersCache.set(dataContainer.getName(), wD)
        return wD

    def __getInstanceModelOutliers(self, dataContainer):
        """ Internal method to assemble model outliers details.

        Args:
            dataContainer ([type]): [description]

        Returns:
            {"instanceModelOutlierD": {(modelId, asymId): [(compId, seqId, "BOND_OUTLIER", optional_description), ...}}

        """
        logger.debug("Starting with %r", dataContainer.getName())
        #
        rD = {}
        try:
            # Exit if no source categories are present
            if not (
                dataContainer.exists("pdbx_vrpt_instance_results")
                or dataContainer.exists("pdbx_vrpt_bond_outliers")
                or dataContainer.exists("pdbx_vrpt_angle_outliers")
                or dataContainer.exists("pdbx_vrpt_mogul_bond_outliers")
                or dataContainer.exists("pdbx_vrpt_mogul_angle_outliers")
            ):
                return rD
            # ------- --------- ------- --------- ------- --------- ------- --------- ------- ---------
            #
            instanceModelOutlierD = {}
            vObj = None
            if dataContainer.exists("pdbx_vrpt_bond_outliers"):
                vObj = dataContainer.getObj("pdbx_vrpt_bond_outliers")
            if vObj:
                for ii in range(vObj.getRowCount()):
                    seqId = vObj.getValueOrDefault("label_seq_id", ii, defaultValue=None)
                    if seqId:
                        modelId = vObj.getValueOrDefault("PDB_model_num", ii, defaultValue=None)
                        asymId = vObj.getValueOrDefault("label_asym_id", ii, defaultValue=None)
                        compId = vObj.getValueOrDefault("label_comp_id", ii, defaultValue=None)
                        #
                        atomI = vObj.getValueOrDefault("atom0", ii, defaultValue=None)
                        atomJ = vObj.getValueOrDefault("atom1", ii, defaultValue=None)
                        obsDist = vObj.getValueOrDefault("obs", ii, defaultValue=None)
                        zVal = vObj.getValueOrDefault("Z", ii, defaultValue=None)
                        tS = "%s-%s dist=%s Z=%s" % (atomI, atomJ, obsDist, zVal)
                        #
                        instanceModelOutlierD.setdefault((modelId, asymId, True), []).append(OutlierValue(compId, int(seqId), "BOND_OUTLIER", tS,))
                #
                logger.debug("length instanceModelOutlierD %d", len(instanceModelOutlierD))
            # ----
            vObj = None
            if dataContainer.exists("pdbx_vrpt_angle_outliers"):
                vObj = dataContainer.getObj("pdbx_vrpt_angle_outliers")
            if vObj:
                for ii in range(vObj.getRowCount()):
                    seqId = vObj.getValueOrDefault("label_seq_id", ii, defaultValue=None)
                    if seqId:
                        modelId = vObj.getValueOrDefault("PDB_model_num", ii, defaultValue=None)
                        asymId = vObj.getValueOrDefault("label_asym_id", ii, defaultValue=None)
                        compId = vObj.getValueOrDefault("label_comp_id", ii, defaultValue=None)
                        #
                        atomI = vObj.getValueOrDefault("atom0", ii, defaultValue=None)
                        atomJ = vObj.getValueOrDefault("atom1", ii, defaultValue=None)
                        atomK = vObj.getValueOrDefault("atom2", ii, defaultValue=None)
                        obsDist = vObj.getValueOrDefault("obs", ii, defaultValue=None)
                        zVal = vObj.getValueOrDefault("Z", ii, defaultValue=None)
                        tS = "%s-%s-%s angle=%s Z=%s" % (atomI, atomJ, atomK, obsDist, zVal)
                        #
                        instanceModelOutlierD.setdefault((modelId, asymId, True), []).append(OutlierValue(compId, int(seqId), "ANGLE_OUTLIER", tS,))
                #
                logger.debug("length instanceModelOutlierD %d", len(instanceModelOutlierD))
            # ----
            vObj = None
            if dataContainer.exists("pdbx_vrpt_mogul_bond_outliers"):
                vObj = dataContainer.getObj("pdbx_vrpt_mogul_bond_outliers")
            if vObj:
                for ii in range(vObj.getRowCount()):
                    seqId = vObj.getValueOrDefault("label_seq_id", ii, defaultValue=None)

                    modelId = vObj.getValueOrDefault("PDB_model_num", ii, defaultValue=None)
                    asymId = vObj.getValueOrDefault("label_asym_id", ii, defaultValue=None)
                    compId = vObj.getValueOrDefault("label_comp_id", ii, defaultValue=None)
                    #
                    atoms = vObj.getValueOrDefault("atoms", ii, defaultValue=None)
                    obsDist = vObj.getValueOrDefault("obsval", ii, defaultValue=None)
                    meanValue = vObj.getValueOrDefault("mean", ii, defaultValue=None)
                    zVal = vObj.getValueOrDefault("Zscore", ii, defaultValue=None)
                    tS = "%s angle=%s Z=%s" % (atoms, obsDist, zVal)
                    # OutlierValue = collections.namedtuple("OutlierValue", "compId, seqId, outlierType, description, reported, reference, uncertaintyValue, uncertaintyType")
                    if seqId:
                        instanceModelOutlierD.setdefault((modelId, asymId, True), []).append(OutlierValue(compId, int(seqId), "MOGUL_BOND_OUTLIER", tS,))
                    else:
                        instanceModelOutlierD.setdefault((modelId, asymId, False), []).append(
                            OutlierValue(compId, None, "MOGUL_BOND_OUTLIER", tS, obsDist, meanValue, zVal, "Z-Score")
                        )
                #
                logger.debug("length instanceModelOutlierD %d", len(instanceModelOutlierD))

            vObj = None
            if dataContainer.exists("pdbx_vrpt_mogul_angle_outliers"):
                vObj = dataContainer.getObj("pdbx_vrpt_mogul_angle_outliers")
            if vObj:
                for ii in range(vObj.getRowCount()):
                    seqId = vObj.getValueOrDefault("label_seq_id", ii, defaultValue=None)

                    modelId = vObj.getValueOrDefault("PDB_model_num", ii, defaultValue=None)
                    asymId = vObj.getValueOrDefault("label_asym_id", ii, defaultValue=None)
                    compId = vObj.getValueOrDefault("label_comp_id", ii, defaultValue=None)
                    #
                    atoms = vObj.getValueOrDefault("atoms", ii, defaultValue=None)
                    obsDist = vObj.getValueOrDefault("obsval", ii, defaultValue=None)
                    meanValue = vObj.getValueOrDefault("mean", ii, defaultValue=None)
                    zVal = vObj.getValueOrDefault("Zscore", ii, defaultValue=None)
                    tS = "%s angle=%s Z=%s" % (atoms, obsDist, zVal)
                    if seqId:
                        instanceModelOutlierD.setdefault((modelId, asymId, True), []).append(OutlierValue(compId, int(seqId), "MOGUL_ANGLE_OUTLIER", tS,))
                    else:
                        instanceModelOutlierD.setdefault((modelId, asymId, False), []).append(
                            OutlierValue(compId, None, "MOGUL_ANGLE_OUTLIER", tS, obsDist, meanValue, zVal, "Z-Score")
                        )
                logger.debug("length instanceModelOutlierD %d", len(instanceModelOutlierD))
                #
                #
            vObj = None
            if dataContainer.exists("pdbx_vrpt_instance_results"):
                vObj = dataContainer.getObj("pdbx_vrpt_instance_results")

            if vObj:
                logger.debug("Row count for %s: %d", vObj.getName(), vObj.getRowCount())
                for ii in range(vObj.getRowCount()):
                    seqId = vObj.getValueOrDefault("label_seq_id", ii, defaultValue=None)
                    modelId = vObj.getValueOrDefault("PDB_model_num", ii, defaultValue=None)
                    asymId = vObj.getValueOrDefault("label_asym_id", ii, defaultValue=None)
                    compId = vObj.getValueOrDefault("label_comp_id", ii, defaultValue=None)
                    #
                    rotamerClass = vObj.getValueOrDefault("rotamer_class", ii, defaultValue=None)
                    ramaClass = vObj.getValueOrDefault("ramachandran_class", ii, defaultValue=None)
                    rsr = vObj.getValueOrDefault("RSR", ii, defaultValue=None)
                    rsrZ = vObj.getValueOrDefault("RSRZ", ii, defaultValue=None)
                    rsrCc = vObj.getValueOrDefault("RSRCC", ii, defaultValue=None)
                    #
                    if seqId:
                        if rotamerClass and rotamerClass.upper() == "OUTLIER":
                            instanceModelOutlierD.setdefault((modelId, asymId, True), []).append(OutlierValue(compId, int(seqId), "ROTAMER_OUTLIER", None,))
                        if ramaClass and ramaClass.upper() == "OUTLIER":
                            instanceModelOutlierD.setdefault((modelId, asymId, True), []).append(OutlierValue(compId, int(seqId), "RAMACHANDRAN_OUTLIER", None,))
                        if rsrZ and float(rsrZ) > 2.0:
                            tS = "%s > 2.0" % rsrZ
                            instanceModelOutlierD.setdefault((modelId, asymId, True), []).append(OutlierValue(compId, int(seqId), "RSRZ_OUTLIER", tS,))
                        if rsrCc and float(rsrCc) < 0.650:
                            tS = "RSRCC < 0.65"
                            instanceModelOutlierD.setdefault((modelId, asymId, True), []).append(OutlierValue(compId, int(seqId), "RSRCC_OUTLIER", tS,))
                    else:
                        if rsrZ and float(rsrZ) > 2.0:
                            tS = "%s > 2.0" % rsrZ
                            instanceModelOutlierD.setdefault((modelId, asymId, False), []).append(OutlierValue(compId, None, "RSRZ_OUTLIER", tS, rsr, None, rsrZ, "Z-Score"))
                        if rsrCc and float(rsrCc) < 0.650:
                            tS = "RSRCC < 0.65"
                            instanceModelOutlierD.setdefault((modelId, asymId, False), []).append(OutlierValue(compId, None, "RSRCC_OUTLIER", tS, rsrCc))
                #
            logger.debug("instanceModelOutlierD %r", instanceModelOutlierD)
            rD = {"instanceModelOutlierD": instanceModelOutlierD}
        except Exception as e:
            logger.exception("%s failing with %s", dataContainer.getName(), str(e))
        return rD
