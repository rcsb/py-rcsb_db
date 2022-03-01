##
# File:    SchemaDefAccess.py
# Author:  J. Westbrook
# Date:    11-Jun-2011
# Version: 0.001 Initial version
#
# Updates:
#       19-Jun-2018 jdw  add hasSchemaObject() and remove references to __baseTables.
#       22-Jun-2018 jdw  change collection attribute specification to dot notation
#        7-Aug-2018 jdw  add methods to return slice parent details
#       14-Aug-2018 jdw  generalize getDocumentKeyAttributeNames() to return list of attributes ...
#        4-Sep-2018 jdw  add methods to return attribute enumerations
#       18-Nov-2018 jdw  extend the content of COLLECTION_DOCUMENT_ATTRIBUTE_NAMES to _INFO and add method
#                        getDocumentKeyAttributeInfo()
#       16-Jan-2019 jdw  add method getDocumentReplaceAttributeNames() to return COLLECTION_DOCUMENT_REPLACE_ATTRIBUTE_NAMES
#        9-Feb-2019 jdw  add method getSliceExtraSchemaIds()
#       11-Mar-2019 jdw  add getSubCategories(), getAttributeSubCategories(), getSubCategoryAggregates(),
#                        getSubCategoryAggregatesUnitCardinality(), getSubCategorySchemaIdList(),
#                        getSubCategoryAttributeIdList()
#       16-Mar-2021 jdw add getAttributeEmbeddedIterableSeparator() and isAttributeEmbeddedIterable(),
#                           getEmbeddedIterableSeparator() and isEmbeddedIterable()
#       28-Feb-2022 bv  add method getSubCategoryAggregatesMandatory()
##
"""
Schema defintion accessors.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
from operator import itemgetter

logger = logging.getLogger(__name__)


class SchemaDefAccess(object):

    """Schema definition accessors."""

    def __init__(self, schemaDef, **kwargs):
        # def __init__(self, databaseName=None, schemaDefDict=None, convertNames=False, versionedDatabaseName=None, documentDefDict=None, verbose=True):
        #
        #
        self.__name = schemaDef["NAME"] if "NAME" in schemaDef else "unassigned"
        self.__appName = schemaDef["APP_NAME"] if "APP_NAME" in schemaDef else "unassigned"

        self.__databaseName = schemaDef["DATABASE_NAME"] if "DATABASE_NAME" in schemaDef else "unassigned"
        self.__databaseVersion = schemaDef["DATABASE_VERSION"] if "DATABASE_VERSION" in schemaDef else "0_0"
        self.__versionedDatabaseName = self.__databaseName + "_" + self.__databaseVersion
        #
        self.__schemaDefDict = schemaDef["SCHEMA_DICT"] if "SCHEMA_DICT" in schemaDef else {}
        self.__documentDefDict = schemaDef["DOCUMENT_DICT"] if "DOCUMENT_DICT" in schemaDef else {}
        #
        self.__selectionFilterDict = schemaDef["SELECTION_FILTERS"] if "SELECTION_FILTERS" in schemaDef else {}
        self.__sliceParentItemD = schemaDef["SLICE_PARENT_ITEMS"]
        self.__sliceParentFilterD = schemaDef["SLICE_PARENT_FILTERS"]
        self.__sliceIndexD, self.__sliceExtraD = self.__makeSliceIndex()
        self.__nameIndex = self.__makeNameIndex()
        self.__kwargs = kwargs
        #

    def getName(self):
        return self.__name

    def getAppName(self):
        return self.__appName

    def getContentClasses(self, schemaId):
        try:
            return self.__schemaDefDict[schemaId]["CONTENT_CLASSES"]
        except Exception:
            return []

    def getCollectionInfo(self):
        """Return the collection info [{'NAME': , 'VERSION': xx }, ... ] for the input content type."""
        cdL = []
        try:
            cdL = self.__documentDefDict["CONTENT_TYPE_COLLECTION_INFO"]
        except Exception as e:
            logger.error("Failing for with %s", str(e))
        return cdL

    def getCollectionVersion(self, collectionName):
        """Return the collection info [{'NAME': , 'VERSION': xx }, ... ] for the input content type."""
        ret = None
        try:
            for cd in self.__documentDefDict["CONTENT_TYPE_COLLECTION_INFO"]:
                if collectionName == cd["NAME"]:
                    return cd["VERSION"]
        except Exception as e:
            logger.error("Failing for with %s", str(e))
        return ret

    def getVersionedCollection(self, prefix):
        try:
            colL = list(self.__documentDefDict["COLLECTION_DOCUMENT_ATTRIBUTE_ID"].keys())
            for col in colL:
                if col.startswith(prefix):
                    return col
        except Exception as e:
            logger.exception("Faling with %s", str(e))
        return None

    def getCollectionExcluded(self, collectionName):
        """For input collection, return the list of excluded object ids."""
        excludeL = []
        try:
            excludeL = self.__documentDefDict["COLLECTION_CONTENT"][collectionName]["EXCLUDE"]
        except Exception as e:
            logger.debug("Collection %s failing with %s", collectionName, str(e))
        return excludeL

    def getCollectionSelected(self, collectionName):
        """For input collection, return the list of selected object ids or the full object id list if no selection is defined."""
        sL = []
        try:
            sL = self.__documentDefDict["COLLECTION_CONTENT"][collectionName]["INCLUDE"]
            if sL:
                sL = list(set(sL))
            else:
                sL = self.getSchemaIdList()
            return sL
        except Exception as e:
            logger.debug("Collection %s failing with %s", collectionName, str(e))

        return sL

    def getCollectionSliceFilter(self, collectionName):
        """For input collection, return the defined slice filter"""
        sf = None
        try:
            sf = self.__documentDefDict["COLLECTION_CONTENT"][collectionName]["SLICE_FILTER"]
        except Exception as e:
            logger.debug("Collection %s failing with %s", collectionName, str(e))
        return sf

    def getCollectionExcludedAttributes(self, collectionName, asSchemaIds=False):
        """For input collection, return a dictionary of attributes flagged for exclusion."""
        eD = None
        try:
            eD = {}
            for catName, atNameL in self.__documentDefDict["COLLECTION_CONTENT"][collectionName]["EXCLUDED_ATTRIBUTES"].items():
                for atName in atNameL:
                    eD[(catName, atName)] = collectionName
            if asSchemaIds:
                # convert to internal schema identifiers if asSchemaIds is set.
                rD = {}
                for (schemaName, atName), cn in eD.items():
                    if schemaName in self.__nameIndex and atName in self.__nameIndex[schemaName]["ATTRIBUTES"]:
                        rD[(self.__nameIndex[schemaName]["SCHEMA_ID"], self.__nameIndex[schemaName]["ATTRIBUTES"][atName])] = cn
                eD = rD
            #
        except Exception as e:
            logger.debug("Collection %s failing with %s", collectionName, str(e))
        #
        logger.debug("collectionName %s excluded attributes %r", collectionName, eD)
        return eD

    def getDataSelectors(self, selectorName):
        sL = []
        if selectorName in self.__selectionFilterDict:
            sL = self.__selectionFilterDict[selectorName]
        return sL

    def getDataSelectorNames(self):
        return list(self.__selectionFilterDict.keys())

    def getDocumentKeyAttributeNames(self, collectionName):
        """Return list of key document attributes required to uniquely identify a document."""
        ret = []
        try:
            return list(self.__documentDefDict["COLLECTION_DOCUMENT_ATTRIBUTE_NAMES"][collectionName])
        except Exception as e:
            logger.exception("Failing for collection %s with %r", collectionName, str(e))
        return ret

    def getDocumentReplaceAttributeNames(self, collectionName):
        """Return list of document attributes required to remove all relevant documents
        prior  to load 'replace' operation.
        """
        ret = []
        try:
            return list(self.__documentDefDict["COLLECTION_DOCUMENT_REPLACE_ATTRIBUTE_NAMES"][collectionName])
        except Exception as e:
            logger.exception("Failing for collection %s with %r", collectionName, str(e))
        return ret

    def getPrivateDocumentAttributes(self, collectionName):
        """Return list of key document identifiers and document private document names."""
        ret = []
        try:
            return [d for d in self.__documentDefDict["COLLECTION_DOCUMENT_PRIVATE_KEYS"][collectionName]]
        except Exception as e:
            logger.exception("Failing for collection %s with %r", collectionName, str(e))
        return ret

    def getDocumentIndices(self, collectionName):
        """Return list of document indices."""
        ret = []
        try:
            return [d for d in self.__documentDefDict["COLLECTION_DOCUMENT_INDICES"][collectionName]]
        except Exception as e:
            logger.exception("Failing for collection %s with %r", collectionName, str(e))
        return ret

    def getDocumentIndex(self, collectionName, indexName):
        """Return the attribute list for a particular document index."""
        ret = []
        try:
            for dD in self.__documentDefDict["COLLECTION_DOCUMENT_INDICES"][collectionName]:
                if dD["INDEX_NAME"] == indexName:
                    return dD["ATTRIBUTE_NAMES"]
        except Exception as e:
            logger.exception("Failing for collection %s index %r with %r", collectionName, indexName, str(e))
        return ret

    def getSubCategoryAggregates(self, collectionName):
        """Return the list of subcategory aggregates for the input collection."""
        try:
            return [d["NAME"] for d in self.__documentDefDict["COLLECTION_SUB_CATEGORY_AGGREGATES"][collectionName]]
        except Exception:
            # logger.exception('Failing with %s' % str(e))
            pass
        return []

    def getSubCategorySchemaIdList(self, subCategoryName):
        """Return the schema Ids containing the input subCategory"""
        sIdL = []
        try:
            for sId in self.__schemaDefDict:
                dD = self.__schemaDefDict[sId]
                if subCategoryName in dD["SCHEMA_SUB_CATEGORIES"]:
                    sIdL.append(sId)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return sIdL

    def getSubCategoryAttributeIdList(self, schemaId, subCategoryName):
        """Return the schema attribute Ids containing the input subCategory"""
        atIdL = []
        try:
            tD = self.__schemaDefDict[schemaId]
            for atId, v in tD["ATTRIBUTE_INFO"].items():
                if subCategoryName in v["SUB_CATEGORIES"]:
                    atIdL.append(atId)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return atIdL

    def getSubCategoryAggregatesUnitCardinality(self, collectionName, subCategoryName):
        """Return if the input subcategory aggregate has unit cardinality."""
        try:
            for dD in self.__documentDefDict["COLLECTION_SUB_CATEGORY_AGGREGATES"][collectionName]:
                if subCategoryName == dD["NAME"]:
                    return dD["HAS_UNIT_CARDINALITY"]
        except Exception:
            return False

    def getSubCategoryAggregatesMandatory(self, collectionName, subCategoryName):
        """Return if the input subcategory is mandatory."""
        try:
            for dD in self.__documentDefDict["COLLECTION_SUB_CATEGORY_AGGREGATES"][collectionName]:
                if subCategoryName == dD["NAME"] and dD["MANDATORY"]:
                    return dD["MANDATORY"]
                else:
                    return False
        except Exception:
            return False

    def hasUnitCardinality(self, schemaId):
        try:
            return self.__schemaDefDict[schemaId]["SCHEMA_UNIT_CARDINALITY"]
        except Exception:
            return False

    def getDatabaseName(self):
        return self.__databaseName

    def getVersionedDatabaseName(self):
        return self.__versionedDatabaseName

    def hasSchemaObject(self, schemaId):
        return schemaId in self.__schemaDefDict

    def getSchemaObject(self, schemaId):
        return SchemaDef(schemaDefDict=self.__schemaDefDict[schemaId])

    def getSchemaName(self, schemaId):
        try:
            return self.__schemaDefDict[schemaId]["SCHEMA_NAME"]
        except Exception:
            return None

    def getAttributeName(self, schemaId, attributeId):
        try:
            return self.__schemaDefDict[schemaId]["ATTRIBUTES"][attributeId]
        except Exception:
            return None

    def getAttributeEmbeddedIterableSeparator(self, schemaId, attributeId):
        try:
            return self.__schemaDefDict[schemaId]["ATTRIBUTE_INFO"][attributeId]["EMBEDDED_ITERABLE_DELIMITER"]
        except Exception:
            return None

    def isAttributeEmbeddedIterable(self, schemaId, attributeId):
        try:
            return self.__schemaDefDict[schemaId]["ATTRIBUTE_INFO"][attributeId]["EMBEDDED_ITERABLE_DELIMITER"] is not None
        except Exception:
            return False

    def getSchemaIdList(self):
        return list(self.__schemaDefDict.keys())

    def getAttributeIdList(self, schemaId):
        tD = self.__schemaDefDict[schemaId]
        tupL = []
        for attributeId, v in tD["ATTRIBUTE_INFO"].items():
            tupL.append((attributeId, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getDefaultAttributeParameterMap(self, schemaId, skipAuto=True):
        """For the input table, return a dictionary of attribute identifiers and parameter names.
        Default parameter names are compressed and camel-case conversions of the attribute ids.
        """
        dL = []
        aIdList = self.getAttributeIdList(schemaId)

        try:
            tDef = self.getSchemaObject(schemaId)
            for aId in aIdList:
                if skipAuto and tDef.isAutoIncrementType(aId):
                    continue
                pL = aId.lower().split("_")
                tL = []
                tL.append(pL[0])
                for pV in pL[1:]:
                    tt = pV[0].upper() + pV[1:]
                    tL.append(tt)
                dL.append((aId, "".join(tL)))
        except Exception:
            for aId in aIdList:
                dL.append((aId, aId))
        return dL

    def getAttributeNameList(self, schemaId):
        tD = self.__schemaDefDict[schemaId]
        tupL = []
        for k, v in tD["ATTRIBUTE_INFO"].items():
            attributeName = tD["ATTRIBUTES"][k]
            tupL.append((attributeName, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getQualifiedAttributeName(self, tableAttributeTuple=(None, None)):
        schemaId = tableAttributeTuple[0]
        attributeId = tableAttributeTuple[1]
        tD = self.__schemaDefDict[schemaId]
        tableName = tD["SCHEMA_NAME"]
        attributeName = tD["ATTRIBUTES"][attributeId]
        qAN = tableName + "." + attributeName
        return qAN

    def __makeSliceIndex(self):
        """Return a dictionary of slice parent and child attribures together with schema objects
        to be included in every slice.

        """
        sliceD = {}
        extraD = {}
        try:
            sliceNames = self.getSliceNames()
            for sliceName in sliceNames:
                dD = {}
                for schemaId, tD in self.__schemaDefDict.items():
                    if sliceName in tD["SLICE_ATTRIBUTES"]:
                        if schemaId not in dD:
                            dD[schemaId] = {}
                        for sD in tD["SLICE_ATTRIBUTES"][sliceName]:
                            # d[schemaId][(sD['PARENT_CATEGORY'], sD['PARENT_ATTRIBUTE'])] = sD['CHILD_ATTRIBUTE']
                            dD[schemaId].setdefault((sD["PARENT_CATEGORY"], sD["PARENT_ATTRIBUTE"]), []).append(sD["CHILD_ATTRIBUTE"])
                    #
                    if sliceName in tD["SLICE_CATEGORY_EXTRAS"] and tD["SLICE_CATEGORY_EXTRAS"][sliceName]:
                        extraD.setdefault(sliceName, []).append(schemaId)

                sliceD[sliceName] = dD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return sliceD, extraD

    def getSliceNames(self):
        """Return a list of the slice names defined for the current schema"""
        try:
            return list(self.__sliceParentItemD.keys())
        except Exception:
            return []

    def getSliceParentItems(self, sliceName):
        """Return a list of dictionaries containing the parent items for input slice name"""
        try:
            return self.__sliceParentItemD[sliceName]
        except Exception:
            return []

    def getSliceParentFilters(self, sliceName):
        """Return a list of dictionaries containing the parent items for input slice name"""
        try:
            return self.__sliceParentFilterD[sliceName]
        except Exception:
            return []

    def getSliceIndex(self, sliceName):
        try:
            return self.__sliceIndexD[sliceName]
        except Exception:
            return {}

    def getSliceExtraSchemaIds(self, sliceName):
        return self.__sliceExtraD[sliceName] if sliceName in self.__sliceExtraD else []

    def __makeNameIndex(self):
        """Return an index of schema and attribute names resolving to corresponding internal ids.

        Returns:
            dict : {<schemaName> : {'SCHEMA_ID": ... , 'ATTRIBUTES': {<atName>: <atId>, ... }}

        """
        nameIndexD = {}
        for schemaId, sD in self.__schemaDefDict.items():
            schemaName = sD["SCHEMA_NAME"]
            atD = {atName: atId for atId, atName in sD["ATTRIBUTES"].items()}
            nameIndexD[schemaName] = {"SCHEMA_ID": schemaId, "ATTRIBUTES": atD}
        return nameIndexD


class SchemaDef(object):

    """Wrapper class for table schema definition."""

    def __init__(self, schemaDefDict=None):
        self.__tD = schemaDefDict if schemaDefDict else {}
        self.__nomalizedEnumD = {}

    def getName(self):
        try:
            return self.__tD["SCHEMA_NAME"]
        except Exception:
            return None

    def isMandatory(self):
        try:
            return self.__tD["SCHEMA_MANDATORY"]
        except Exception:
            return False

    def getType(self):
        try:
            return self.__tD["SCHEMA_TYPE"]
        except Exception:
            return None

    def getContentClasses(self):
        try:
            return self.__tD["SCHEMA_CONTENT_CLASSES"]
        except Exception:
            return []

    def getSubCategories(self):
        try:
            return self.__tD["SCHEMA_SUB_CATEGORIES"]
        except Exception:
            return []

    def getId(self):
        try:
            return self.__tD["SCHEMA_ID"]
        except Exception:
            return None

    def getAttributeIdDict(self):
        try:
            return self.__tD["ATTRIBUTES"]
        except Exception:
            return {}

    def getAttributeNameDict(self):
        try:
            return {v: k for k, v in self.__tD["ATTRIBUTES"].items()}
        except Exception:
            return {}

    def getAttributeName(self, attributeId):
        try:
            return self.__tD["ATTRIBUTES"][attributeId]
        except Exception:
            return None

    # deprecated
    def getMapAttributeInfo(self, attributeId):
        """Return the tuple of mapping details for the input attribute id."""
        raise NotImplementedError()

    def getAttributeType(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["APP_TYPE"]
        except Exception:
            return None

    def isAutoIncrementType(self, attributeId):
        try:
            tL = [tt.upper() for tt in self.__tD["ATTRIBUTE_INFO"][attributeId]["APP_TYPE"].split()]
            if "AUTO_INCREMENT" in tL:
                return True
        except Exception:
            pass
        return False

    def isAttributeStringType(self, attributeId):
        try:
            return self.__isStringType(self.__tD["ATTRIBUTE_INFO"][attributeId]["APP_TYPE"].upper())
        except Exception:
            return False

    def isAttributeFloatType(self, attributeId):
        try:
            return self.__isFloatType(self.__tD["ATTRIBUTE_INFO"][attributeId]["APP_TYPE"].upper())
        except Exception:
            return False

    def isAttributeIntegerType(self, attributeId):
        try:
            return self.__isIntegerType(self.__tD["ATTRIBUTE_INFO"][attributeId]["APP_TYPE"].upper())
        except Exception:
            return False

    def isAttributeDateType(self, attributeId):
        try:
            return self.__isDateType(self.__tD["ATTRIBUTE_INFO"][attributeId]["APP_TYPE"].upper())
        except Exception:
            return False

    def getAttributeWidth(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["WIDTH"]
        except Exception:
            return None

    def getAttributePrecision(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["PRECISION"]
        except Exception:
            return None

    def getAttributeNullable(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["NULLABLE"]
        except Exception:
            return None

    def getAttributeIsPrimaryKey(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["PRIMARY_KEY"]
        except Exception:
            return None

    def getAttributeEnumList(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["ENUMERATION"]
        except Exception:
            return []

    def normalizeEnum(self, attributeId, enum):
        try:
            if self.__tD["ATTRIBUTE_INFO"][attributeId]["ENUMERATION"] and attributeId not in self.__nomalizedEnumD:
                self.__nomalizedEnumD[attributeId] = {str(ky).lower(): ky for ky in self.__tD["ATTRIBUTE_INFO"][attributeId]["ENUMERATION"]}
            return self.__nomalizedEnumD[attributeId][str(enum).lower()]
        except Exception as e:
            logger.debug("Failing for %s  %s and %r - %r with %r", self.__tD["SCHEMA_NAME"], attributeId, enum, self.__nomalizedEnumD, str(e))
        return enum

    def isEnumerated(self, attributeId):
        try:
            return len(self.__tD["ATTRIBUTE_INFO"][attributeId]["ENUMERATION"]) > 0
        except Exception:
            return False

    def getAttributeSubCategories(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["SUB_CATEGORIES"]
        except Exception:
            return []

    def getAttributeFilterTypes(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["FILTER_TYPES"]
        except Exception:
            return []

    def getPrimaryKeyAttributeIdList(self):
        try:
            return [atId for atId in self.__tD["ATTRIBUTE_INFO"].keys() if self.__tD["ATTRIBUTE_INFO"][atId]["PRIMARY_KEY"]]
        except Exception:
            pass

        return []

    def getAttributeIdList(self):
        """Get the ordered attribute Id list"""
        tupL = []
        for k, v in self.__tD["ATTRIBUTE_INFO"].items():
            tupL.append((k, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getAttributeNameList(self):
        """Get the ordered attribute name list"""
        tupL = []
        for k, v in self.__tD["ATTRIBUTE_INFO"].items():
            tupL.append((k, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [self.__tD["ATTRIBUTES"][tup[0]] for tup in sTupL]

    def getIndexNames(self):
        try:
            return list(self.__tD["INDICES"].keys())
        except Exception:
            return []

    def getIndexType(self, indexName):
        try:
            return self.__tD["INDICES"][indexName]["TYPE"]
        except Exception:
            return None

    def getIndexAttributeIdList(self, indexName):
        try:
            return self.__tD["INDICES"][indexName]["ATTRIBUTES"]
        except Exception:
            return []

    def getMapAttributeNameList(self):
        """Get the ordered mapped attribute name list"""
        try:
            tupL = []
            for k in self.__tD["ATTRIBUTE_MAP"].keys():
                iOrd = self.__tD["ATTRIBUTE_INFO"][k]["ORDER"]
                tupL.append((k, iOrd))

            sTupL = sorted(tupL, key=itemgetter(1))
            return [self.__tD["ATTRIBUTES"][tup[0]] for tup in sTupL]
        except Exception:
            return []

    def getMapAttributeIdList(self):
        """Get the ordered mapped attribute name list"""
        try:
            tupL = []
            for k in self.__tD["ATTRIBUTE_MAP"].keys():
                iOrd = self.__tD["ATTRIBUTE_INFO"][k]["ORDER"]
                tupL.append((k, iOrd))

            sTupL = sorted(tupL, key=itemgetter(1))

            return [tup[0] for tup in sTupL]
        except Exception:
            return []

    def getMapInstanceCategoryList(self):
        """Get the unique list of data instance categories within the attribute map."""
        try:
            cL = [vD["CATEGORY"] for k, vD in self.__tD["ATTRIBUTE_MAP"].items() if vD["CATEGORY"] is not None]  #
            uL = list(set(cL))
            return uL
        except Exception:
            return []

    def getMapOtherAttributeIdList(self):
        """Get the list of attributes that have no assigned data instance mapping."""
        try:
            aL = []
            for k, vD in self.__tD["ATTRIBUTE_MAP"].items():
                if vD["CATEGORY"] is None:
                    aL.append(k)
            return aL
        except Exception:
            return []

    def getMapInstanceAttributeList(self, categoryName):
        """Get the list of instance category attribute names for mapped attributes in the input instance category."""
        try:
            aL = []
            for _, vD in self.__tD["ATTRIBUTE_MAP"].items():
                if vD["CATEGORY"] == categoryName:
                    aL.append(vD["ATTRIBUTE"])
            return aL
        except Exception:
            return []

    def getMapInstanceAttributeIdList(self, categoryName):
        """Get the list of schema attribute Ids for mapped attributes from the input instance category."""
        try:
            aL = []
            for k, vD in self.__tD["ATTRIBUTE_MAP"].items():
                if vD["CATEGORY"] == categoryName:
                    aL.append(k)
            return aL
        except Exception:
            return []

    def getMapAttributeFunction(self, attributeId):
        """Return the tuple element of mapping details for the input attribute id for the optional function."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId]["METHOD_NAME"]
        except Exception:
            return None

    def getMapAttributeFunctionArgs(self, attributeId):
        """Return the tuple element of mapping details for the input attribute id for the optional function arguments."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId]["ARGUMENTS"]
        except Exception:
            return None

    def getMapAttributeIdDict(self):
        """Return the dictionary of d[schema attribute id] = mapped instance category attribute

        Exclude and attributes that lack a schema mapping (e.g. functional mappings )
        """
        dD = {}
        for k, vD in self.__tD["ATTRIBUTE_MAP"].items():
            if vD["ATTRIBUTE"]:
                dD[k] = vD["ATTRIBUTE"]
        return dD

    def getMapAttributeNameDict(self):
        """Return the dictionary of d[schema attribute id] = mapped instance category attribute

        Exclude and attributes that lack a schema mapping (e.g. functional mappings )
        """
        dD = {}
        for k, vD in self.__tD["ATTRIBUTE_MAP"].items():
            if vD["ATTRIBUTE"]:
                dD[vD["ATTRIBUTE"]] = k
        return dD

    def getMapMergeIndexAttributes(self, categoryName):
        """Return the list of merging index attribures for this mapped instance category."""
        try:
            return self.__tD["MAP_MERGE_INDICES"][categoryName]["ATTRIBUTES"]
        except Exception:
            return []

    def getMapMergeIndexType(self, indexName):
        """Return the merging index type for this mapped instance category."""
        try:
            return self.__tD["MAP_MERGE_INDICES"][indexName]["TYPE"]
        except Exception:
            return []

    def getAppNullValue(self, attributeId):
        """Return the appropriate NULL value for this attribute:."""
        try:
            if self.__isStringType(self.__tD["ATTRIBUTE_INFO"][attributeId]["APP_TYPE"].upper()):
                return ""
            elif self.__isDateType(self.__tD["ATTRIBUTE_INFO"][attributeId]["APP_TYPE"].upper()):
                return r"\N"
            else:
                return r"\N"
        except Exception:
            return r"\N"

    def getAppNullValueDict(self):
        """Return a dictionary containing appropriate NULL value for each attribute."""
        dD = {}
        atId = atInfo = None
        try:
            for atId, atInfo in self.__tD["ATTRIBUTE_INFO"].items():

                if self.__isStringType(atInfo["APP_TYPE"].upper()):
                    dD[atId] = ""
                elif self.__isDateType(atInfo["APP_TYPE"].upper()):
                    dD[atId] = r"\N"
                else:
                    dD[atId] = r"\N"
        except Exception as e:
            logger.exception("Failing with atId %r atInfo %r with %s", atId, atInfo, str(e))
        #
        return dD

    def getStringWidthDict(self):
        """Return a dictionary containing maximum string widths assigned to char data types.
        Non-character type data items are assigned zero width.
        """
        dD = {}
        for atId, atInfo in self.__tD["ATTRIBUTE_INFO"].items():
            if self.__isStringType(atInfo["APP_TYPE"].upper()):
                dD[atId] = int(atInfo["WIDTH"])
            else:
                dD[atId] = 0
        return dD

    def isIterable(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["ITERABLE_DELIMITER"] is not None
        except Exception:
            return False

    def isEmbeddedIterable(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["EMBEDDED_ITERABLE_DELIMITER"] is not None
        except Exception:
            return False

    def isOtherAttributeType(self, attributeId):
        """Get the list of attributes that have no assigned instance mapping."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId]["CATEGORY"] is None
        except Exception:
            return False

    def getIterableSeparator(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["ITERABLE_DELIMITER"]
        except Exception:
            return None

    def getEmbeddedIterableSeparator(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["EMBEDDED_ITERABLE_DELIMITER"]
        except Exception:
            return None

    def __isStringType(self, sqlType):
        """Return if input type corresponds to a common SQL string data type."""
        try:
            return sqlType.upper() in ["VARCHAR", "CHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ANY"]
        except Exception:
            return False

    def __isDateType(self, sqlType):
        """Return if input type corresponds to a common SQL date/time data type."""
        try:
            return sqlType.upper() in ["DATE", "DATETIME"]
        except Exception:
            return False

    def __isFloatType(self, sqlType):
        """Return if input type corresponds to a common SQL string data type."""
        try:
            return sqlType.upper() in ["FLOAT", "DECIMAL", "DOUBLE PRECISION", "NUMERIC"]
        except Exception:
            return False

    def __isIntegerType(self, sqlType):
        """Return if input type corresponds to a common SQL string data type."""
        try:
            return sqlType.upper().startswith("INT") or sqlType.upper() in ["INTEGER", "BIGINT", "SMALLINT"]
        except Exception:
            return False

    def getDeleteAttributeId(self):
        """Return the attribute identifier that is used to delete all of the
        records associated with the highest level of organizaiton provided by
        this schema definition (e.g. entry, definition, ...).
        """
        try:
            return self.__tD["SCHEMA_DELETE_ATTRIBUTE"]
        except Exception:
            return None

    def getDeleteAttributeName(self):
        """Return the attribute name that is used to delete all of the
        records associated with the highest level of organizaiton provided by
        this schema definition (e.g. entry, definition, ...).
        """
        try:
            return self.__tD["ATTRIBUTES"][self.__tD["SCHEMA_DELETE_ATTRIBUTE"]]
        except Exception:
            return None

    def hasSliceAttributes(self, sliceName):
        """Return True if slice attributes are defined for this schema object."""
        try:
            if self.__tD["SLICE_ATTRIBUTES"][sliceName]:
                return True
            else:
                return False
        except Exception:
            return False

    def hasSliceUnitCardinality(self, sliceName):
        """Return True if slice for this schema object has unit cardinality."""
        try:
            return self.__tD["SLICE_UNIT_CARDINALITY"][sliceName]
        except Exception:
            return False

    def isSliceExtra(self, sliceName):
        """Return True if this schema object is extra included content in the input slice."""
        try:
            return self.__tD["SLICE_CATEGORY_EXTRAS"][sliceName]
        except Exception:
            return False

    def getSliceAttributeId(self, sliceName, parentCategoryId, parentAttributeId):
        """Naive method to return corresponding child attribute for the input slice parent."""
        try:
            for dD in self.__tD["SLICE_ATTRIBUTES"][sliceName]:
                if dD["PARENT_CATEGORY"] == parentCategoryId and dD["PARENT_ATTRIBUTE"] == parentAttributeId:
                    return dD["CHILD_ATTRIBUTE"]
            return None
        except Exception:
            return None
