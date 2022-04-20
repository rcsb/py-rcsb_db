##
# File:    SchemaDefBuild.py
# Author:  J. Westbrook
# Date:    1-May-2018
# Version: 0.001 Initial version
#
# Updates:
#
#  9-May-2018 jdw integrate dictionary and file based (type/coverage) data.
#  7-Aug-2018 jdw add slice definitions converted to schema id references
# 13-Aug-2018 jdw Refine the role of includeContentClasses -
# 14-Aug-2018 jdw Return 'COLLECTION_DOCUMENT_ATTRIBUTE_NAMES' as a list
#  6-Sep-2018 jdw Generalize JSON schema generation method
# 14-Sep-2018 jdw Require at least one record in any array type, adjust constraints on iterables.
# 18-Sep-2018 jdw Constrain categories/class to homogeneous content
#  7-Oct-2018 jdw Add subCategory aggregation in the JSON schema generator
#  9-Oct-2018 jdw push the constructor arguments into the constructor as configuration options
# 12-Oct-2018 jdw filter empty required attributes in subcategory aggregates
# 24-Oct-2018 jdw update for new configuration organization
# 18-Nov-2018 jdw add COLLECTION_DOCUMENT_ATTRIBUTE_INFO
#  3-Dec-2018 jdw add INTEGRATED_CONTENT
#  6-Jan-2019 jdw update to the change in configuration for dataTypeInstanceFile
# 16-Jan-2019 jdw add 'COLLECTION_DOCUMENT_REPLACE_ATTRIBUTE_NAMES'
# 31-Mar-2019 jdw add  support for 'addParentRefs' in enforceOpts to include relative $ref properties
#                 to describe parent relationships
#  3-Apr-2019 jdw add experimental primary key property controlled by 'addPrimaryKey'
# 22-Aug-2019 jdw DictInfo() replaced with new ContentInfo()
# 28-Feb-2022 bv add support for mandatory sub-categories in json schema
##
"""
Integrate dictionary metadata and file based (type/coverage) into internal and JSON/BSON schema defintions.


"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

# pylint: disable=too-many-lines

import copy
import logging
from collections import OrderedDict

from rcsb.db.define.DataTypeApiProvider import DataTypeApiProvider
from rcsb.db.define.ContentDefinition import ContentDefinition
from rcsb.utils.dictionary.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper

logger = logging.getLogger(__name__)


class SchemaDefBuild(object):
    """Integrate dictionary metadata and file based(type/coverage) into internal and JSON/BSON schema defintions."""

    def __init__(self, databaseName, cfgOb, cachePath=None, includeContentClasses=None):
        """Integrate dictionary metadata and file based(type/coverage) into internal and JSON/BSON schema defintions.

        Args:
            databaseName (str): schema name
            cfgOb (object): ConfigInfo() object instance
            cachePath (str): path to cached resources
            includeContentClasses (list, optional): content class list. Defaults to None.
        """
        configName = "site_info_configuration"
        self.__cfgOb = cfgOb
        self.__databaseName = databaseName
        self.__cachePath = cachePath if cachePath else "."
        self.__includeContentClasses = includeContentClasses if includeContentClasses else ["GENERATED_CONTENT", "EVOLVING_CONTENT", "CONSOLIDATED_BIRD_CONTENT", "INTEGRATED_CONTENT"]
        #
        self.__contentDefHelper = self.__cfgOb.getHelper("CONTENT_DEF_HELPER_MODULE", sectionName=configName, cfgOb=self.__cfgOb)
        self.__documentDefHelper = self.__cfgOb.getHelper("DOCUMENT_DEF_HELPER_MODULE", sectionName=configName, cfgOb=self.__cfgOb)
        #
        self.__dtP = DataTypeApiProvider(self.__cfgOb, cachePath, useCache=True)
        #
        dP = DictionaryApiProviderWrapper(self.__cachePath, cfgOb=cfgOb, configName=configName, useCache=True)
        dictApi = dP.getApiByName(databaseName)
        self.__contentInfo = ContentDefinition(dictApi, contentDefHelper=self.__contentDefHelper, databaseName=databaseName)
        #

    def build(self, collectionName=None, dataTyping="ANY", encodingType="rcsb", enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):
        rD = {}
        if encodingType.lower() == "rcsb":
            rD = self.__build(
                databaseName=self.__databaseName,
                dataTyping=dataTyping,
                contentDefHelper=self.__contentDefHelper,
                documentDefHelper=self.__documentDefHelper,
                includeContentClasses=self.__includeContentClasses,
            )
        elif encodingType.lower() in ["json", "bson"]:
            rD = self.__createJsonLikeSchema(
                databaseName=self.__databaseName,
                collectionName=collectionName,
                dataTyping=dataTyping.upper(),
                contentDefHelper=self.__contentDefHelper,
                documentDefHelper=self.__documentDefHelper,
                includeContentClasses=self.__includeContentClasses,
                enforceOpts=enforceOpts,
            )
        return rD

    def __build(self, databaseName, dataTyping, contentDefHelper, documentDefHelper, includeContentClasses):
        """ """
        databaseVersion = contentDefHelper.getDatabaseVersion(databaseName) if contentDefHelper else ""

        #
        schemaDef = {"NAME": databaseName, "APP_NAME": dataTyping, "DATABASE_NAME": databaseName, "DATABASE_VERSION": databaseVersion}
        #
        schemaDef["SELECTION_FILTERS"] = self.__contentInfo.getSelectionFiltersForDatabase()

        schemaDef["SCHEMA_DICT"] = self.__createSchemaDict(databaseName, dataTyping, contentDefHelper, includeContentClasses)
        schemaDef["DOCUMENT_DICT"] = self.__createDocumentDict(databaseName, documentDefHelper)
        schemaDef["SLICE_PARENT_ITEMS"] = self.__convertSliceParentItemNames(databaseName, dataTyping)
        schemaDef["SLICE_PARENT_FILTERS"] = self.__convertSliceParentFilterNames(databaseName, dataTyping)
        return schemaDef

    def __createDocumentDict(self, databaseName, documentDefHelper):
        """Internal method to assign document-level details to the schema definition,


        Args:
            databaseName (string): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            documentDefHelper (class instance):  Class instance providing additional document-level metadata

        Returns:
            dict: dictionary of document-level metadata


        """
        rD = {
            "CONTENT_TYPE_COLLECTION_INFO": [],
            "COLLECTION_DOCUMENT_ATTRIBUTE_NAMES": {},
            "COLLECTION_DOCUMENT_REPLACE_ATTRIBUTE_NAMES": {},
            "COLLECTION_DOCUMENT_PRIVATE_KEYS": {},
            "COLLECTION_DOCUMENT_INDICES": {},
            "COLLECTION_CONTENT": {},
            "COLLECTION_SUB_CATEGORY_AGGREGATES": {},
        }
        #
        dH = documentDefHelper
        if dH:
            # cdL  = list of [{'NAME': , 'VERSION': xx }, ...]
            cdL = dH.getCollectionInfo(databaseName)
            rD["CONTENT_TYPE_COLLECTION_INFO"] = cdL
            for cd in cdL:
                cN = cd["NAME"]
                rD["COLLECTION_CONTENT"][cN] = {
                    "INCLUDE": dH.getIncluded(cN),
                    "EXCLUDE": dH.getExcluded(cN),
                    "SLICE_FILTER": dH.getSliceFilter(cN),
                    "EXCLUDED_ATTRIBUTES": dH.getDocumentExcludedAttributes(cN, asTuple=False),
                }
                rD["COLLECTION_DOCUMENT_ATTRIBUTE_NAMES"][cN] = dH.getDocumentKeyAttributeNames(cN)
                rD["COLLECTION_DOCUMENT_REPLACE_ATTRIBUTE_NAMES"][cN] = dH.getDocumentReplaceAttributeNames(cN)
                rD["COLLECTION_DOCUMENT_PRIVATE_KEYS"][cN] = dH.getPrivateDocumentAttributes(cN)
                rD["COLLECTION_DOCUMENT_INDICES"][cN] = dH.getDocumentIndices(cN)
                rD["COLLECTION_SUB_CATEGORY_AGGREGATES"][cN] = dH.getSubCategoryAggregateFeatures(cN)
        #
        return rD

    def __testContentClasses(self, includeContentClasses, assignedContentClasses):
        """Return True if any of the include content classes are assigned."""
        # logger.debug("includeContentClasses %r assignedContentClasses %r" % (includeContentClasses, assignedContentClasses))
        for cc in includeContentClasses:
            if cc in assignedContentClasses:
                return True
        return False

    def __getConvertNameMethod(self, dataTyping):
        # Function to perform category and attribute name conversion.
        # convertNameF = self.__contentDefHelper.convertNameDefault if self.__contentDefHelper else self.__convertNameDefault
        #
        try:
            if dataTyping in ["ANY", "SQL", "DOCUMENT", "SOLR", "JSON", "BSON"]:
                nameConvention = dataTyping
            else:
                nameConvention = "DEFAULT"
            return self.__contentDefHelper.getConvertNameMethod(nameConvention) if self.__contentDefHelper else self.__convertNameDefault
        except Exception:
            pass

        return self.__convertNameDefault

    def __createSchemaDict(self, databaseName, dataTyping, contentDefHelper, includeContentClasses=None):
        """Internal method to integrate dictionary and instance metadata into a common schema description data structure.

        Args:
            databaseName (string): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            dataTyping (string): ANY|SQL
            contentDefHelper (class instance): Class instance providing additional schema details
            includeContentClasses (list, optional): list of additional content classes to be included (e.g. GENERATED_CONTENT)

        Returns:
            dict: definitions for each schema object


        """
        verbose = False
        contentClasses = includeContentClasses if includeContentClasses else []
        #
        logger.debug("Including additional category classes %r", contentClasses)
        #
        dtInstInfo = self.__dtP.getDataTypeInstanceApi(databaseName)
        dtAppInfo = self.__dtP.getDataTypeApplicationApi(dataTyping)
        #
        # Supplied by the contentDefHelper
        #
        includeList = contentDefHelper.getIncluded(databaseName) if contentDefHelper else []
        excludeList = contentDefHelper.getExcluded(databaseName) if contentDefHelper else []
        excludeAttributesD = contentDefHelper.getExcludedAttributes(databaseName) if contentDefHelper else {}
        #
        logger.debug("Schema include list length %d", len(includeList))
        logger.debug("Schema exclude list length %d", len(excludeList))
        #
        # Optional synthetic attribute added to each category with value linked to data block identifier (or other function)
        #
        blockAttributeName = contentDefHelper.getBlockAttributeName(databaseName) if contentDefHelper else None
        blockAttributeCifType = contentDefHelper.getBlockAttributeCifType(databaseName) if contentDefHelper else None
        blockAttributeAppType = dtAppInfo.getAppTypeName(blockAttributeCifType)
        blockAttributeWidth = contentDefHelper.getBlockAttributeMaxWidth(databaseName) if contentDefHelper else 0
        blockAttributeMethod = contentDefHelper.getBlockAttributeMethod(databaseName) if contentDefHelper else None
        #
        convertNameF = self.__getConvertNameMethod(dataTyping)
        #
        dictSchema = self.__contentInfo.getSchemaNames()
        logger.debug("Dictionary category length %d", len(dictSchema))
        #
        rD = OrderedDict()
        for catName, fullAtNameList in dictSchema.items():

            #
            atNameList = [at for at in fullAtNameList if (catName, at) not in excludeAttributesD]
            #
            cfD = self.__contentInfo.getCategoryFeatures(catName)
            #
            # logger.debug("catName %s contentClasses %r cfD %r" % (catName, contentClasses, cfD))

            if not dtInstInfo.exists(catName) and not self.__testContentClasses(contentClasses, cfD["CONTENT_CLASSES"]):
                logger.debug("Schema %r Skipping category %s content classes %r", databaseName, catName, cfD["CONTENT_CLASSES"])
                continue
            sName = convertNameF(catName)
            sId = sName.upper()
            #
            if excludeList and sId in excludeList:
                continue
            if includeList and sId not in includeList:
                continue
            # JDW
            if not cfD:
                logger.info("%s catName %s contentClasses %r cfD %r", databaseName, catName, contentClasses, cfD)
            #
            aD = self.__contentInfo.getAttributeFeatures(catName)
            #
            sliceNames = self.__contentInfo.getSliceNames()
            dD = OrderedDict()
            dD["SCHEMA_ID"] = sId
            dD["SCHEMA_NAME"] = sName
            dD["SCHEMA_TYPE"] = "transactional"
            dD["SCHEMA_UNIT_CARDINALITY"] = cfD["UNIT_CARDINALITY"] if "UNIT_CARDINALITY" in cfD else False
            dD["SCHEMA_CONTENT_CLASSES"] = cfD["CONTENT_CLASSES"] if "CONTENT_CLASSES" in cfD else []
            dD["SCHEMA_MANDATORY"] = cfD["IS_MANDATORY"]
            dD["SCHEMA_SUB_CATEGORIES"] = []
            #
            dD["ATTRIBUTES"] = OrderedDict({convertNameF(blockAttributeName).upper(): convertNameF(blockAttributeName)}) if blockAttributeName else {}
            #
            dD["ATTRIBUTE_MAP"] = (
                OrderedDict({(convertNameF(blockAttributeName)).upper(): {"CATEGORY": None, "ATTRIBUTE": None, "METHOD_NAME": blockAttributeMethod, "ARGUMENTS": None}})
                if blockAttributeName
                else OrderedDict()
            )

            dD["ATTRIBUTE_INFO"] = OrderedDict()
            atIdIndexList = []
            atNameIndexList = []
            iOrder = 1
            if blockAttributeName:
                td = OrderedDict(
                    {
                        "ORDER": iOrder,
                        "NULLABLE": False,
                        "PRECISION": 0,
                        "PRIMARY_KEY": True,
                        "APP_TYPE": blockAttributeAppType,
                        "WIDTH": blockAttributeWidth,
                        "ITERABLE_DELIMITER": None,
                        "FILTER_TYPES": [],
                        "ENUMERATION": [],
                        "IS_CHAR_TYPE": True,
                        "CONTENT_CLASSES": ["BLOCK_ATTRIBUTE"],
                        "SUB_CATEGORIES": [],
                    }
                )
                iOrder += 1
                atId = (convertNameF(blockAttributeName)).upper()
                atIdIndexList.append(atId)
                atNameIndexList.append(blockAttributeName)
                dD["ATTRIBUTE_INFO"][atId] = td
            #
            for atName in sorted(atNameList):
                fD = aD[atName]
                if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD["CONTENT_CLASSES"]):
                    continue
                if fD["IS_KEY"]:
                    appType = dtAppInfo.getAppTypeName(fD["TYPE_CODE"])
                    appWidth = dtAppInfo.getAppTypeDefaultWidth(fD["TYPE_CODE"])
                    instWidth = dtInstInfo.getMaxWidth(catName, atName)
                    #
                    try:
                        revAppType, revAppWidth = dtAppInfo.updateCharType(fD["IS_KEY"], appType, instWidth, appWidth, bufferPercent=20.0)
                    except Exception as e:
                        logger.exception("Failing for catName %r atName %r fD[TYPE_CODE] %r with %s", catName, atName, fD["TYPE_CODE"], str(e))

                    if verbose and dataTyping in ["SQL", "ANY"]:
                        logger.debug(
                            "catName %s atName %s cifType %s appType %s appWidth %r instWidth %r --> revAppType %r revAppWidth %r ",
                            catName,
                            atName,
                            fD["TYPE_CODE"],
                            appType,
                            appWidth,
                            instWidth,
                            revAppType,
                            revAppWidth,
                        )
                    #
                    appPrecision = dtAppInfo.getAppTypeDefaultPrecision(fD["TYPE_CODE"])
                    td = OrderedDict(
                        {
                            "ORDER": iOrder,
                            "NULLABLE": not fD["IS_MANDATORY"],
                            "PRECISION": appPrecision,
                            "PRIMARY_KEY": fD["IS_KEY"],
                            "APP_TYPE": revAppType,
                            "WIDTH": revAppWidth,
                            "ITERABLE_DELIMITER": None,
                            "FILTER_TYPES": fD["FILTER_TYPES"],
                            "IS_CHAR_TYPE": fD["IS_CHAR_TYPE"],
                            "ENUMERATION": fD["ENUMS"],
                            "CONTENT_CLASSES": fD["CONTENT_CLASSES"],
                            "SUB_CATEGORIES": [qtD["id"] for qtD in fD["SUB_CATEGORIES"]],
                        }
                    )
                    atId = (convertNameF(atName)).upper()
                    dD["ATTRIBUTE_INFO"][atId] = td
                    atIdIndexList.append(atId)
                    atNameIndexList.append(atName)
                    #
                    mI = self.__contentInfo.getMethodImplementation(catName, atName, methodCodes=["calculate_on_load"])
                    if mI:
                        dD["ATTRIBUTE_MAP"].update({(convertNameF(atName)).upper(): {"CATEGORY": None, "ATTRIBUTE": None, "METHOD_NAME": mI, "ARGUMENTS": None}})
                    else:
                        dD["ATTRIBUTE_MAP"].update({(convertNameF(atName)).upper(): {"CATEGORY": catName, "ATTRIBUTE": atName, "METHOD_NAME": None, "ARGUMENTS": None}})
                    iOrder += 1
            #
            for atName in sorted(atNameList):
                fD = aD[atName]
                if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD["CONTENT_CLASSES"]):
                    continue

                if not fD["IS_KEY"]:
                    appType = dtAppInfo.getAppTypeName(fD["TYPE_CODE"])
                    if not appType:
                        logger.error("Missing application data type mapping for %s %s (%r)", catName, atName, fD["TYPE_CODE"])
                    appWidth = dtAppInfo.getAppTypeDefaultWidth(fD["TYPE_CODE"])
                    instWidth = dtInstInfo.getMaxWidth(catName, atName)
                    revAppType, revAppWidth = dtAppInfo.updateCharType(fD["IS_KEY"], appType, instWidth, appWidth, bufferPercent=20.0)
                    if verbose and dataTyping in ["SQL", "ANY"]:
                        logger.debug(
                            "catName %s atName %s cifType %s appType %s appWidth %r instWidth %r --> revAppType %r revAppWidth %r ",
                            catName,
                            atName,
                            fD["TYPE_CODE"],
                            appType,
                            appWidth,
                            instWidth,
                            revAppType,
                            revAppWidth,
                        )

                    #
                    appPrecision = dtAppInfo.getAppTypeDefaultPrecision(fD["TYPE_CODE"])
                    td = OrderedDict(
                        {
                            "ORDER": iOrder,
                            "NULLABLE": not fD["IS_MANDATORY"],
                            "PRECISION": appPrecision,
                            "PRIMARY_KEY": fD["IS_KEY"],
                            "APP_TYPE": revAppType,
                            "WIDTH": revAppWidth,
                            "ITERABLE_DELIMITER": fD["ITERABLE_DELIMITER"],
                            "EMBEDDED_ITERABLE_DELIMITER": fD["EMBEDDED_ITERABLE_DELIMITER"],
                            "FILTER_TYPES": fD["FILTER_TYPES"],
                            "IS_CHAR_TYPE": fD["IS_CHAR_TYPE"],
                            "ENUMERATION": fD["ENUMS"],
                            "CONTENT_CLASSES": fD["CONTENT_CLASSES"],
                            "SUB_CATEGORIES": [qtD["id"] for qtD in fD["SUB_CATEGORIES"]],
                        }
                    )
                    atId = (convertNameF(atName)).upper()
                    dD["ATTRIBUTE_INFO"][atId] = td

                    mI = self.__contentInfo.getMethodImplementation(catName, atName, methodCodes=["calculate_on_load"])
                    if mI:
                        dD["ATTRIBUTE_MAP"].update({(convertNameF(atName)).upper(): {"CATEGORY": None, "ATTRIBUTE": None, "METHOD_NAME": mI, "ARGUMENTS": None}})
                    else:
                        dD["ATTRIBUTE_MAP"].update({(convertNameF(atName)).upper(): {"CATEGORY": catName, "ATTRIBUTE": atName, "METHOD_NAME": None, "ARGUMENTS": None}})
                    iOrder += 1
            #
            atIdDelete = convertNameF(blockAttributeName).upper() if blockAttributeName else None
            dD["SCHEMA_DELETE_ATTRIBUTE"] = atIdDelete

            dD["INDICES"] = {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": tuple(atIdIndexList)}}
            if len(atIdIndexList) > 1:
                dD["INDICES"]["s1"] = {"TYPE": "SEARCH", "ATTRIBUTES": tuple([atIdDelete])}
            #
            # JDW -  Need to review attribute names here -
            dD["MAP_MERGE_INDICES"] = {catName: {"ATTRIBUTES": tuple(atNameIndexList), "TYPE": "EQUI-JOIN"}}
            # ----
            tD = OrderedDict()
            logger.debug("Slice names %r", sliceNames)
            for sliceName in sorted(sliceNames):
                sL = self.__contentInfo.getSliceAttributes(sliceName, catName)
                logger.debug("Slice attributes %r", sL)
                if sL:
                    # Convert names to IDs --
                    tL = []
                    for slD in sL:
                        pD = OrderedDict(
                            {
                                "PARENT_CATEGORY": convertNameF(slD["PARENT_CATEGORY_NAME"]).upper(),
                                "PARENT_ATTRIBUTE": convertNameF(slD["PARENT_ATTRIBUTE_NAME"]).upper(),
                                "CHILD_ATTRIBUTE": convertNameF(slD["CHILD_ATTRIBUTE_NAME"]).upper(),
                            }
                        )
                        tL.append(pD)
                    tD[sliceName] = tL
            dD["SLICE_ATTRIBUTES"] = tD
            #
            # ---- slice cardinality
            #
            dD["SLICE_UNIT_CARDINALITY"] = OrderedDict()
            sliceCardD = self.__contentInfo.getSliceUnitCardinalityForDatabase()
            logger.debug("Slice card dict %r", sliceCardD.items())
            for sliceName, catL in sliceCardD.items():
                if catName in catL:
                    dD["SLICE_UNIT_CARDINALITY"][sliceName] = True
                else:
                    dD["SLICE_UNIT_CARDINALITY"][sliceName] = False
            #
            dD["SLICE_CATEGORY_EXTRAS"] = OrderedDict()
            sliceCatD = self.__contentInfo.getSliceCategoryExtrasForDatabase()
            logger.debug("Slice category extra dict %r", sliceCatD.items())
            for sliceName, catL in sliceCatD.items():
                if catName in catL:
                    dD["SLICE_CATEGORY_EXTRAS"][sliceName] = True
                else:
                    dD["SLICE_CATEGORY_EXTRAS"][sliceName] = False
            #
            scL = []
            for atId in dD["ATTRIBUTE_INFO"]:
                scL.extend(dD["ATTRIBUTE_INFO"][atId]["SUB_CATEGORIES"])
            dD["SCHEMA_SUB_CATEGORIES"] = sorted(set(scL))
            #
            # Make attributes dict consistent with map ...
            dD["ATTRIBUTES"].update({atId: convertNameF(tD["ATTRIBUTE"]) for atId, tD in dD["ATTRIBUTE_MAP"].items() if atId not in dD["ATTRIBUTES"]})

            #
            rD[sId] = dD
        #
        return rD

    def __convertSliceParentItemNames(self, databaseName, dataTyping):
        sliceD = {}
        try:
            convertNameF = self.__getConvertNameMethod(dataTyping)
            # [{'CATEGORY_NAME': 'entity', 'ATTRIBUTE_NAME': 'id'}
            spD = self.__contentInfo.getSliceParentItemsForDatabase()
            for ky in spD:
                rL = []
                for aL in spD[ky]:
                    dD = {"CATEGORY": convertNameF(aL["CATEGORY_NAME"]).upper(), "ATTRIBUTE": convertNameF(aL["ATTRIBUTE_NAME"]).upper()}
                    rL.append(dD)
                sliceD[ky] = rL
            #
            return sliceD
        except Exception as e:
            logger.exception("Failing for %s with %s", databaseName, str(e))

        return sliceD

    def __convertSliceParentFilterNames(self, databaseName, dataTyping):
        sliceD = {}
        try:
            convertNameF = self.__getConvertNameMethod(dataTyping)
            # [{'CATEGORY_NAME': 'entity', 'ATTRIBUTE_NAME': 'id'}
            spD = self.__contentInfo.getSliceParentFiltersForDatabase()
            for ky in spD:
                rL = []
                for aL in spD[ky]:
                    dD = {"CATEGORY": convertNameF(aL["CATEGORY_NAME"]).upper(), "ATTRIBUTE": convertNameF(aL["ATTRIBUTE_NAME"]).upper(), "VALUES": aL["VALUES"]}
                    rL.append(dD)
                sliceD[ky] = rL
            #
            return sliceD
        except Exception as e:
            logger.exception("Failing for %s with %s", databaseName, str(e))

        return sliceD

    def __convertNameDefault(self, name):
        """Default schema name converter -"""
        logger.info("Using default name conversion for %r", name)
        return name

    # -------------------------- ------------- ------------- ------------- ------------- ------------- -------------
    def __getAmendedAttributeFeatures(self, collectionName, catName, documentDefHelper):
        #
        useDefaultBrief = False
        aD = self.__contentInfo.getAttributeFeatures(catName)
        for atName, fD in aD.items():
            ascL = documentDefHelper.getAttributeSearchContexts(collectionName, catName, atName)
            fD["SEARCH_CONTEXTS"] = ascL
            # logger.info("collectionName %r catName %r atName %r context %r", collectionName, catName, atName, ascL)
            fD["IS_NESTED"] = documentDefHelper.isCategoryNested(collectionName, catName)
            fD["SEARCH_PRIORITY"] = documentDefHelper.getAttributeTextSearchPriority(collectionName, catName, atName)
            # logger.info("collectionName %r catName %r atName %r priority %r", collectionName, catName, atName, fD["SEARCH_PRIORITY"])
            tS = documentDefHelper.getAttributeDescription(catName, atName, contextType="brief")
            if tS:
                fD["DESCRIPTION_ANNOTATED"].append({"text": tS, "context": "brief"})
            elif useDefaultBrief:
                # Use content == deposition or then dictionary for 'brief' by default -
                for tD in fD["DESCRIPTION_ANNOTATED"]:
                    if tD["context"] == "deposition":
                        fD["DESCRIPTION_ANNOTATED"].append({"text": tD["text"], "context": "brief"})
                    elif tD["context"] == "dictionary":
                        fD["DESCRIPTION_ANNOTATED"].append({"text": tD["text"], "context": "brief"})
            # ----
            fD["SEARCH_GROUP_AND_PRIORITY"] = [{"group_name": tup[0], "priority_order": tup[1]} for tup in documentDefHelper.getSearchGroup(catName, atName)]
            # ----
        #
        return aD
        #

    def __exportSearchContext(self, collectionName, catName, atName, atPropD):
        if "bsonType" in atPropD:
            #
            sC = None
            tt = atPropD["bsonType"]
            if "enum" in atPropD:
                sC = "exact-match"
            elif "date" in tt or tt in ["int", "integer", "float", "double"]:
                sC = "default-match"
            elif tt in ["string"]:
                sC = "full-text"
            else:
                logger.warning("UNKNOWN type for %s %s %s", catName, atName, tt)
                return False

            logger.info("--SEARCH CONTEXT %s %s - %s.%s", collectionName, sC, catName, atName)
        return True

    def __createJsonLikeSchema(
        self,
        databaseName,
        collectionName,
        dataTyping,
        contentDefHelper,
        documentDefHelper,
        includeContentClasses=None,
        jsonSpecDraft="4",
        enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums",
        removeSubCategoryPrefix=True,
    ):
        """Internal method to integrate dictionary and instance metadata into a common json/bson schema description data structure.

           Working only for practical schema style: rowwise_by_name_with_cardinality

        Args:
            databaseName (str): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            collectionName (str): Collection defined within a schema/content type
            dataTyping (str): Target data type convention for the schema (e.g. JSON, BSON, or a variant of these...)
            contentDefHelper (class instance): Class instance providing additional schema details
            documentDefHelper (class instance): Class instance providing additional document schema details
            includeContentClasses (list, optional): list of additional content classes to be included (e.g. GENERATED_CONTENT)
            jsonSpecDraft (str, optional): The target draft schema specification '4|6'
            enforceOpts (str, optional): options for semantics are included in the schema (e.g. "mandatoryKeys|mandatoryAttributes|bounds|enums")

        Returns:
            dict: representation of JSON/BSON schema -


        """
        exportSearchContext = False
        addRcsbExtensions = "rcsb" in enforceOpts
        addBlockAttribute = True
        mandatorySubcategoryAttributes = True
        addPrimaryKey = "addPrimaryKey" in enforceOpts
        suppressSingleton = not documentDefHelper.getRetainSingletonObjects(collectionName)
        suppressRelations = documentDefHelper.getSuppressedCategoryRelationships(collectionName)
        logger.debug("Collection %s suppress singleton %r", collectionName, suppressSingleton)
        subCategoryAggregates = documentDefHelper.getSubCategoryAggregates(collectionName)
        logger.debug("%s %s Sub_category aggregates %r", databaseName, collectionName, subCategoryAggregates)
        privDocKeyL = documentDefHelper.getPrivateDocumentAttributes(collectionName)

        dataTypingU = dataTyping.upper()
        typeKey = "bsonType" if dataTypingU == "BSON" else "type"
        convertNameF = self.__getConvertNameMethod(dataTypingU)
        #
        contentClasses = includeContentClasses if includeContentClasses else []
        logger.debug("Including additional category classes %r", contentClasses)
        exportConfig = dataTypingU == "JSON" and False
        logger.debug("QQQQ COLLECTION %s", collectionName)
        if exportConfig:
            logger.info("CONFIG  %s:", collectionName)
        #
        dtInstInfo = self.__dtP.getDataTypeInstanceApi(databaseName)
        dtAppInfo = self.__dtP.getDataTypeApplicationApi(dataTypingU)
        #
        #      Supplied by the contentDefHelper for the content type (SchemaIds)
        #
        includeList = contentDefHelper.getIncluded(databaseName) if contentDefHelper else []
        excludeList = contentDefHelper.getExcluded(databaseName) if contentDefHelper else []
        excludeAttributesD = contentDefHelper.getExcludedAttributes(databaseName) if contentDefHelper else {}
        #
        #      Supplied by the documentDefHelp for the collection (SchemaIds)
        #
        docIncludeList = documentDefHelper.getIncluded(collectionName)
        docExcludeList = documentDefHelper.getExcluded(collectionName)
        docExcludeAttributes = documentDefHelper.getDocumentExcludedAttributes(collectionName)
        # JDW combine the schema and document excluded attributes here ...
        excludeAttributesD.update(docExcludeAttributes)

        sliceFilter = documentDefHelper.getSliceFilter(collectionName)
        sliceCategories = self.__contentInfo.getSliceCategories(sliceFilter) if sliceFilter else []
        sliceCategoryExtrasD = self.__contentInfo.getSliceCategoryExtrasForDatabase() if sliceFilter else {}
        if sliceFilter in sliceCategoryExtrasD:
            sliceCategories.extend(sliceCategoryExtrasD[sliceFilter])
        sliceCardD = self.__contentInfo.getSliceUnitCardinalityForDatabase() if sliceFilter else {}
        #
        if addBlockAttribute:
            # Optional synthetic attribute added to each category with value linked to data block identifier (or other function)
            blockAttributeName = contentDefHelper.getBlockAttributeName(databaseName) if contentDefHelper else None
            blockAttributeCifType = contentDefHelper.getBlockAttributeCifType(databaseName) if contentDefHelper else None
            blockAttributeAppType = dtAppInfo.getAppTypeName(blockAttributeCifType)
            blockRefPathList = None
            if "addParentRefs" in enforceOpts:
                refD = contentDefHelper.getBlockAttributeRefParent(databaseName)
                if refD is not None:
                    blockRefPathList = [convertNameF(refD["CATEGORY_NAME"]), convertNameF(refD["ATTRIBUTE_NAME"])]

        #
        dictSchema = self.__contentInfo.getSchemaNames()
        #
        sNameD = {}
        schemaPropD = {}
        mandatoryCategoryL = []
        for catName, fullAtNameList in dictSchema.items():
            atNameList = [at for at in fullAtNameList if (catName, at) not in excludeAttributesD]
            #

            #
            cfD = self.__contentInfo.getCategoryFeatures(catName)
            # logger.debug("catName %s contentClasses %r cfD %r" % (catName, contentClasses, cfD))

            #
            #  Skip categories that are uniformly unpopulated --
            #
            if not dtInstInfo.exists(catName) and not self.__testContentClasses(contentClasses, cfD["CONTENT_CLASSES"]):
                logger.debug("Schema %r Skipping category %s content classes %r", databaseName, catName, cfD["CONTENT_CLASSES"])
                continue
            #
            # -> Create a schema id  for catName <-
            sName = convertNameF(catName)
            sNameD[sName] = catName
            schemaId = sName.upper()
            #
            #  These are the content type schema level filters -
            if excludeList and schemaId in excludeList:
                continue
            if includeList and schemaId not in includeList:
                continue
            #
            # These are collection level filters
            #
            if docExcludeList and schemaId in docExcludeList:
                continue
            if docIncludeList and schemaId not in docIncludeList:
                continue
            #
            #  If there is a slice filter on this collection, the skip categories not connected to the slice
            if sliceFilter and catName not in sliceCategories:
                continue
            #
            #        Done with category filtering/selections
            # -------- ---------- ------------ -------- ---------- ------------ -------- ---------- ------------
            #
            # aD = self.__contentInfo.getAttributeFeatures(catName)
            aD = self.__getAmendedAttributeFeatures(collectionName, catName, documentDefHelper)
            #
            if cfD["IS_MANDATORY"]:
                if not catName.startswith("ma_"):
                    mandatoryCategoryL.append(catName)
            #
            isUnitCard = True if ("UNIT_CARDINALITY" in cfD and cfD["UNIT_CARDINALITY"]) else False
            if sliceFilter and sliceFilter in sliceCardD:
                isUnitCard = catName in sliceCardD[sliceFilter]
            #
            pD = {typeKey: "object", "properties": {}, "additionalProperties": False}
            #

            if isUnitCard:
                catPropD = pD
            else:
                if cfD["IS_MANDATORY"]:
                    catPropD = {typeKey: "array", "items": pD, "minItems": 1, "uniqueItems": True}
                else:
                    # JDW Adjusted minItems=1
                    catPropD = {typeKey: "array", "items": pD, "minItems": 1, "uniqueItems": True}
                #
            if dataTypingU == "JSON" and addRcsbExtensions:
                self.__updateCategoryNestedContext(catPropD, collectionName, catName, documentDefHelper)
                #

            if addBlockAttribute and blockAttributeName:
                schemaAttributeName = convertNameF(blockAttributeName)
                pD.setdefault("required", []).append(schemaAttributeName)
                #
                if blockRefPathList:
                    atPropD = self.__getJsonRef(blockRefPathList)
                else:
                    # atPropD = {typeKey: blockAttributeAppType, 'maxWidth': blockAttributeWidth}
                    atPropD = {typeKey: blockAttributeAppType}

                if addPrimaryKey:
                    atPropD["_primary_key"] = True
                #
                pD["properties"][schemaAttributeName] = atPropD

            #  First, filter any subcategory aggregates from the available list of a category attributes
            #

            subCatPropD = {}
            scMandatory = None
            if subCategoryAggregates:
                logger.debug("%s %s %s subcategories %r", databaseName, collectionName, catName, cfD["SUB_CATEGORIES"])
                for subCategory in subCategoryAggregates:
                    if subCategory not in cfD["SUB_CATEGORIES"]:
                        continue
                    logger.debug("%s %s %s processing subcategory %r", databaseName, collectionName, catName, subCategory)
                    reqL = []
                    scD = {typeKey: "object", "properties": {}, "additionalProperties": False}
                    scHasUnitCard = documentDefHelper.getSubCategoryAggregateUnitCardinality(collectionName, subCategory)
                    scMandatory = documentDefHelper.getSubCategoryAggregateMandatory(collectionName, subCategory)
                    for atName in sorted(atNameList):
                        fD = aD[atName]
                        #
                        if subCategory not in [qtD["id"] for qtD in fD["SUB_CATEGORIES"]]:
                            continue
                        #
                        schemaAttributeName = convertNameF(atName)
                        if removeSubCategoryPrefix:
                            schemaAttributeName = schemaAttributeName.replace(subCategory + "_", "")
                        #
                        # isRequired = "mandatoryAttributes" in enforceOpts and fD["IS_MANDATORY"]
                        # JDW separate general support for mandatory attributes and support in subcategories.
                        isRequired = mandatorySubcategoryAttributes and fD["IS_MANDATORY"]
                        if isRequired:
                            reqL.append(schemaAttributeName)
                        #
                        atPropD = self.__getJsonAttributeProperties(fD, dataTypingU, dtAppInfo, dtInstInfo, jsonSpecDraft, enforceOpts, suppressRelations, addRcsbExtensions)
                        # --- replace
                        # scD["properties"][schemaAttributeName] = atPropD
                        # --- with adding general support for embedded iterable
                        delimiter = fD["EMBEDDED_ITERABLE_DELIMITER"]
                        if delimiter:
                            logger.debug("embedded iterable %r %r (%r)", catName, atName, subCategory)
                            scD["properties"][schemaAttributeName] = {typeKey: "array", "items": atPropD, "uniqueItems": False}
                        else:
                            scD["properties"][schemaAttributeName] = atPropD
                        # ---
                        if exportSearchContext:
                            self.__exportSearchContext(collectionName, catName, atName, atPropD)

                    if scD["properties"]:
                        if reqL:
                            scD["required"] = reqL
                        if scMandatory and "mandatoryAttributes" in enforceOpts:
                            pD.setdefault("required", []).append(subCategory)
                        if scHasUnitCard:
                            subCatPropD[subCategory] = scD
                        else:
                            subCatPropD[subCategory] = {typeKey: "array", "items": scD, "uniqueItems": False}
                            if dataTypingU == "JSON" and addRcsbExtensions:
                                if documentDefHelper.isSubCategoryNested(collectionName, catName, subCategory):
                                    tD = documentDefHelper.getSubCategoryNestedContext(collectionName, catName, subCategory)
                                    if "FIRST_CONTEXT_PATH" in tD and tD["FIRST_CONTEXT_PATH"]:
                                        subCatPropD[subCategory]["rcsb_nested_indexing"] = True
                                        ###
                                        if "CONTEXT_ATTRIBUTE_VALUES" in tD and tD["CONTEXT_ATTRIBUTE_VALUES"]:
                                            vvDL = []
                                            for cavD in tD["CONTEXT_ATTRIBUTE_VALUES"]:
                                                vvD = {"context_value": cavD["CONTEXT_VALUE"]}
                                                #
                                                if "ATTRIBUTES" in cavD:
                                                    aL = []
                                                    for atD in cavD["ATTRIBUTES"]:
                                                        dD = {}
                                                        if "EXAMPLES" in atD:
                                                            dD["examples"] = atD["EXAMPLES"]
                                                        if "PATH" in atD:
                                                            dD["path"] = atD["PATH"]
                                                        aL.append(dD)
                                                    vvD["attributes"] = aL
                                                #
                                                vvDL.append(vvD)
                                            subCatPropD[subCategory]["rcsb_nested_indexing_context"] = [
                                                {"category_name": tD["CONTEXT_NAME"], "category_path": tD["FIRST_CONTEXT_PATH"], "context_attributes": vvDL}
                                            ]
                                        else:
                                            subCatPropD[subCategory]["rcsb_nested_indexing_context"] = [{"category_name": tD["CONTEXT_NAME"], "category_path": tD["FIRST_CONTEXT_PATH"]}]
                                    else:
                                        subCatPropD[subCategory]["rcsb_nested_indexing"] = True
            #
            if subCatPropD:
                logger.debug("%s %s %s processing subcategory properties %r", databaseName, collectionName, catName, subCatPropD.items())
            #
            if exportConfig:
                logger.info("CONFIG - CATEGORY_NAME: %s", catName)
                logger.info("CONFIG   ATTRIBUTE_NAME_LIST:")
            for atName in sorted(atNameList):
                fD = aD[atName]
                # Exclude primary data attributes with no instance coverage except if in a protected content class
                if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD["CONTENT_CLASSES"]):
                    continue
                if subCategoryAggregates and self.__subCategoryTest(subCategoryAggregates, [d["id"] for d in fD["SUB_CATEGORIES"]]):
                    continue
                #
                if exportConfig and "_id" in atName:
                    logger.info("CONFIG   - %s", atName)
                #
                schemaAttributeName = convertNameF(atName)
                isRequired = ("mandatoryKeys" in enforceOpts and fD["IS_KEY"]) or ("mandatoryAttributes" in enforceOpts and fD["IS_MANDATORY"])
                # subject to exclusion
                if isRequired and (catName, atName) not in excludeAttributesD:
                    pD.setdefault("required", []).append(schemaAttributeName)
                #
                atPropD = self.__getJsonAttributeProperties(fD, dataTypingU, dtAppInfo, dtInstInfo, jsonSpecDraft, enforceOpts, suppressRelations, addRcsbExtensions)

                delimiter = fD["ITERABLE_DELIMITER"]
                if delimiter:
                    pD["properties"][schemaAttributeName] = {typeKey: "array", "items": atPropD, "uniqueItems": False}
                else:
                    pD["properties"][schemaAttributeName] = atPropD

                if exportSearchContext:
                    self.__exportSearchContext(collectionName, catName, schemaAttributeName, atPropD)

            if subCatPropD:
                pD["properties"].update(copy.copy(subCatPropD))
            # pD['required'].extend(list(subCatPropD.keys()))
            #
            if "required" in catPropD and not catPropD["required"]:
                logger.debug("Category %s cfD %r", catName, cfD.items())
                del catPropD["required"]
            #
            if pD["properties"]:
                schemaPropD[sName] = copy.deepcopy(catPropD)
        #
        # Add any private keys to the object schema - Fetch the metadata for the private keys
        #
        privKeyD = {}
        privMandatoryD = {}
        if privDocKeyL:
            for pdk in privDocKeyL:
                catNameK = convertNameF(pdk["CATEGORY_NAME"])
                aD = self.__contentInfo.getAttributeFeatures(catNameK)
                atNameK = convertNameF(pdk["ATTRIBUTE_NAME"])
                fD = aD[atNameK]
                atPropD = self.__getJsonAttributeProperties(fD, dataTypingU, dtAppInfo, dtInstInfo, jsonSpecDraft, enforceOpts, suppressRelations, addRcsbExtensions)
                privKeyD[pdk["PRIVATE_DOCUMENT_NAME"]] = atPropD
                privMandatoryD[pdk["PRIVATE_DOCUMENT_NAME"]] = pdk["MANDATORY"]
        #
        # Suppress the category name for schemas with a single category -
        #
        if suppressSingleton and len(schemaPropD) == 1:
            logger.debug("%s %s suppressing category in singleton schema", databaseName, collectionName)
            sName = list(schemaPropD.keys())[0]
            catName = sNameD[sName]
            logger.debug("%s singleton state sName %r catName %r", collectionName, sName, catName)
            # rD = copy.deepcopy(catPropD)
            for k, v in privKeyD.items():
                pD["properties"][k] = v
                # pD['required'] = k
                if privMandatoryD[k]:
                    pD.setdefault("required", []).append(k)
            rD = copy.deepcopy(pD)
            # if "additionalProperties" in rD:
            #    rD["additionalProperties"] = True
            if dataTypingU == "JSON" and addRcsbExtensions:
                self.__updateCategoryNestedContext(rD, collectionName, catName, documentDefHelper)
            logger.debug("%s singleton state rD %r", collectionName, rD)
        else:
            for k, v in privKeyD.items():
                schemaPropD[k] = v
                if privMandatoryD[k]:
                    mandatoryCategoryL.append(k)
            #
            rD = {typeKey: "object", "properties": schemaPropD, "additionalProperties": False}
            if mandatoryCategoryL:
                rD["required"] = mandatoryCategoryL

        #
        if dataTypingU == "BSON":
            rD["properties"]["_id"] = {"bsonType": "objectId"}
            logger.debug("Adding mongo key %r", rD["properties"]["_id"])
        #
        if dataTypingU == "JSON":
            sdType = dataTyping.lower()
            sLevel = "full" if "bounds" in enforceOpts else "min"
            fn = "%s-schema-%s-%s.json" % (sdType, sLevel, collectionName)
            collectionVersion = documentDefHelper.getCollectionVersion(databaseName, collectionName)
            jsonSchemaUrl = "http://json-schema.org/draft-0%s/schema#" % jsonSpecDraft if jsonSpecDraft in ["3", "4", "6", "7"] else "http://json-schema.org/schema#"
            schemaRepo = "https://github.com/rcsb/py-rcsb.db/tree/master/rcsb.db/data/json-schema/"
            desc1 = "RCSB Exchange Database JSON schema derived from the %s content type schema. " % databaseName
            desc2 = "This schema supports collection %s version %s. " % (collectionName, collectionVersion)
            desc3 = "This schema is hosted in repository %s%s and follows JSON schema specification version %s" % (schemaRepo, fn, jsonSpecDraft)
            rD.update(
                {
                    "$id": "%s%s" % (schemaRepo, fn),
                    "$schema": jsonSchemaUrl,
                    "title": "schema: %s collection: %s version: %s" % (databaseName, collectionName, collectionVersion),
                    "description": desc1 + desc2 + desc3,
                    "$comment": "schema_version: %s" % collectionVersion,
                }
            )

        return rD

    def __updateCategoryNestedContext(self, rD, collectionName, catName, documentDefHelper):
        """Insert nested context details into a category schema object

        Args:
            rD (dict): current category schema object
            collectionName (string): collection name
            catName (str): data category Name
            documentDefHelper (obj): instance of DocumentDefHelper()

        Returns:
            dict : updated category schema object

        Output schema example:

        "rcsb_nested_indexing_context": [
                {
                "category_name": "annotation_type",
                "category_path": "rcsb_polymer_entity_annotation.type"
                "context_attributes": [
                    {
                        "context_value": 'value',
                        "attributes": [
                        {
                           "examples": [
                              "xxxxx",
                              "yyyyy"
                           ],
                           "path": "xxxx"
                        },
                    }
                ]
                }
            ]


        """
        if documentDefHelper.isCategoryNested(collectionName, catName):
            tD = documentDefHelper.getCategoryNestedContext(collectionName, catName)
            logger.debug("%s Nested context dict %r", collectionName, tD)
            if "FIRST_CONTEXT_PATH" in tD and tD["FIRST_CONTEXT_PATH"]:
                rD["rcsb_nested_indexing"] = True
                if "CONTEXT_ATTRIBUTE_VALUES" in tD and tD["CONTEXT_ATTRIBUTE_VALUES"]:
                    vDL = []
                    for cavD in tD["CONTEXT_ATTRIBUTE_VALUES"]:
                        vD = {"context_value": cavD["CONTEXT_VALUE"]}
                        if "ATTRIBUTES" in cavD:
                            aL = []
                            for atD in cavD["ATTRIBUTES"]:
                                dD = {}
                                if "EXAMPLES" in atD:
                                    dD["examples"] = atD["EXAMPLES"]
                                if "PATH" in atD:
                                    dD["path"] = atD["PATH"]
                                aL.append(dD)
                            vD["attributes"] = aL
                        vDL.append(vD)
                    rD["rcsb_nested_indexing_context"] = [{"category_name": tD["CONTEXT_NAME"], "category_path": tD["FIRST_CONTEXT_PATH"], "context_attributes": vDL}]
                else:
                    rD["rcsb_nested_indexing_context"] = [{"category_name": tD["CONTEXT_NAME"], "category_path": tD["FIRST_CONTEXT_PATH"]}]
                #
            else:
                rD["rcsb_nested_indexing"] = True
        return rD

    def __getJsonRef(self, pathList, itemSubCategoryList=None):
        """Add parent reference and optional group membership and labeling."""
        refD = {}
        try:
            refD = {"$ref": "#" + "/".join(pathList)}
            if itemSubCategoryList:
                logger.debug("Subcategory membership %r %r", pathList, itemSubCategoryList)
                refD["_attribute_groups"] = itemSubCategoryList
        except Exception as e:
            logger.exception("Failing with pathList %r %s", pathList, str(e))
        return refD

    def __testSuppressRelation(self, fD, suppressRelations):
        ret = False
        childCatName = fD["CATEGORY_NAME"]
        parentCatName = fD["PARENT"]["CATEGORY"]
        for srD in suppressRelations:
            if "CHILD_CATEGORY_NAME" in srD and "PARENT_CATEGORY_NAME" in srD:
                ret = (childCatName == srD["CHILD_CATEGORY_NAME"] and parentCatName == srD["PARENT_CATEGORY_NAME"]) or ret
            elif "PARENT_CATEGORY_NAME" in srD:
                ret = (parentCatName == srD["PARENT_CATEGORY_NAME"]) or ret
            elif "CHILD_CATEGORY_NAME" in srD:
                ret = (childCatName == srD["CHILD_CATEGORY_NAME"]) or ret
        if ret:
            logger.debug("Suppressing relationships for catName %r parent %r", childCatName, parentCatName)

        return ret

    def __getJsonAttributeProperties(self, fD, dataTypingU, dtAppInfo, dtInstInfo, jsonSpecDraft, enforceOpts, suppressRelations, addRcsbExtensions):
        #
        atPropD = {}
        addPrimaryKey = "addPrimaryKey" in enforceOpts
        addParentRefs = "addParentRefs" in enforceOpts
        try:
            catName = fD["CATEGORY_NAME"]
            atName = fD["ATTRIBUTE_NAME"]
            precMin = dtInstInfo.getMinPrecision(catName, atName)
            precMax = dtInstInfo.getMaxPrecision(catName, atName)
            # Adding a parent reference -
            if addParentRefs and fD["PARENT"] is not None and not self.__testSuppressRelation(fD, suppressRelations):
                convertNameF = self.__getConvertNameMethod(dataTypingU)
                pCatName = convertNameF(fD["PARENT"]["CATEGORY"])
                pAtName = convertNameF(fD["PARENT"]["ATTRIBUTE"])
                # logger.info("Using parent ref %r %r " % (pCatName, pAtName))
                # atPropD = {'$ref': '#/%s/%s' % (pCatName, pAtName)}
                atPropD = self.__getJsonRef([pCatName, pAtName], fD["SUB_CATEGORIES"])
                if addPrimaryKey and fD["IS_KEY"]:
                    atPropD["_primary_key"] = True
                return atPropD
            #
            # - assign data type attributes
            typeKey = "bsonType" if dataTypingU == "BSON" else "type"
            appType = dtAppInfo.getAppTypeName(fD["TYPE_CODE"])
            #
            #
            if appType in ["string"]:
                # atPropD = {typeKey: appType, 'maxWidth': instWidth}
                atPropD = {typeKey: appType}
            elif appType in ["date", "datetime"] and dataTypingU == "JSON":
                fmt = "date" if appType == "date" else "date-time"
                atPropD = {typeKey: "string", "format": fmt}
            elif appType in ["date", "datetime"] and dataTypingU == "BSON":
                atPropD = {typeKey: "date"}
            elif appType in ["number", "integer", "int", "double"]:
                atPropD = {typeKey: appType}
                #
                if "bounds" in enforceOpts:
                    if jsonSpecDraft in ["3", "4"]:
                        if "MIN_VALUE" in fD:
                            atPropD["minimum"] = fD["MIN_VALUE"]
                        elif "MIN_VALUE_EXCLUSIVE" in fD:
                            atPropD["minimum"] = fD["MIN_VALUE_EXCLUSIVE"]
                            atPropD["exclusiveMinimum"] = True
                        if "MAX_VALUE" in fD:
                            atPropD["maximum"] = fD["MAX_VALUE"]
                        elif "MAX_VALUE_EXCLUSIVE" in fD:
                            atPropD["maximum"] = fD["MAX_VALUE_EXCLUSIVE"]
                            atPropD["exclusiveMaximum"] = True
                    elif jsonSpecDraft in ["6", "7"]:
                        if "MIN_VALUE" in fD:
                            atPropD["minimum"] = fD["MIN_VALUE"]
                        elif "MIN_VALUE_EXCLUSIVE" in fD:
                            atPropD["exclusiveMinimum"] = fD["MIN_VALUE_EXCLUSIVE"]
                        if "MAX_VALUE" in fD:
                            atPropD["maximum"] = fD["MAX_VALUE"]
                        elif "MAX_VALUE_EXCLUSIVE" in fD:
                            atPropD["exclusiveMaximum"] = fD["MAX_VALUE_EXCLUSIVE"]
            elif appType.startswith("any"):
                # ---
                # logger.debug("Processing special type %s", appType)
                # if dataTypingU == "BSON":
                #    atPropD = {typeKey: "array", "items": {"anyOf": [{typeKey: "string"}, {typeKey: "int"}, {typeKey: "double"}]}}
                # else:
                #    atPropD = {typeKey: "array", "items": {"anyOf": [{typeKey: "string"}, {typeKey: "integer"}, {typeKey: "number"}]}}
                # ---
                if dataTypingU == "BSON":
                    atPropD = {"anyOf": [{typeKey: "string"}, {typeKey: "int"}, {typeKey: "double"}]}
                else:
                    atPropD = {"anyOf": [{typeKey: "string"}, {typeKey: "integer"}, {typeKey: "number"}]}
                #
            else:
                atPropD = {typeKey: appType}

            if "enums" in enforceOpts and fD["ENUMS"]:
                atPropD["enum"] = sorted(fD["ENUMS"])
            #
            if dataTypingU not in ["BSON"]:
                try:
                    if fD["EXAMPLES"]:
                        # atPropD["examples"] = [str(t1).strip() for t1, _ in fD["EXAMPLES"]]
                        atPropD["examples"] = [t1 for t1, _ in fD["EXAMPLES"]]
                except Exception as e:
                    logger.exception("Failing for %r with %s", fD["EXAMPLES"], str(e))
                if fD["DESCRIPTION"]:
                    atPropD["description"] = fD["DESCRIPTION"]
                #
            if addPrimaryKey and fD["IS_KEY"]:
                atPropD["_primary_key"] = True
            if addParentRefs and fD["SUB_CATEGORIES"] and fD["SUB_CATEGORIES"]:
                atPropD["_attribute_groups"] = fD["SUB_CATEGORIES"]
            #
            if addRcsbExtensions and dataTypingU not in ["BSON"]:
                #
                if appType in ["double"] and precMin and precMax:
                    # need median here -
                    # atPropD["rcsb_precision_digits"] = precMin
                    pass

                if fD["SEARCH_CONTEXTS"]:
                    atPropD["rcsb_search_context"] = fD["SEARCH_CONTEXTS"]
                if "SEARCH_PRIORITY" in fD and fD["SEARCH_PRIORITY"]:
                    atPropD["rcsb_full_text_priority"] = int(fD["SEARCH_PRIORITY"])
                if fD["UNITS"]:
                    atPropD["rcsb_units"] = fD["UNITS"]
                #
                if fD["ENUMS_ANNOTATED"]:
                    atPropD["rcsb_enum_annotated"] = fD["ENUMS_ANNOTATED"]
                if fD["DESCRIPTION_ANNOTATED"]:
                    atPropD["rcsb_description"] = fD["DESCRIPTION_ANNOTATED"]
                if fD["SEARCH_GROUP_AND_PRIORITY"]:
                    atPropD["rcsb_search_group"] = fD["SEARCH_GROUP_AND_PRIORITY"]
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return atPropD

    def __subCategoryTest(self, filterList, atSubCategoryList):
        """Return true if any element of filter list is in atSubCategoryList"""
        if not filterList or not atSubCategoryList:
            return False
        for subCat in filterList:
            if subCat in atSubCategoryList:
                return True
        return False
