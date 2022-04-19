##
# File:    ContentDefinition.py
# Author:  J. Westbrook
# Date:    8-May-2018
# Version: 0.001 Initial version
#
# Updates:
#  23-May-2018 jdw add method to return vanilla data Type dictionary/revise api -
#   5-Jun-2018 jdw replace IoAdapter() with MarshalUtil() -
#  10-Jun-2018 jdw move the base table definitions in terms of content classes
#  16-Jun-2018 jdw standardize method name for getAttributeDataTypeD()
#  26-Nov-2018 jdw turn down boundary value level to standard ddl values
#   3-Feb-2019 jdw get all child items using self.__dApi.getFullDescendentList()
#  31-Mar-2019 jdw include parent details in __getAttributeFeatures()
#   6-Jun-2019 jdw take dictionary API as an argument.
#  22-Aug-2019 jdw unify naming conventions dictSubset->databaseName and *ForSubset() -> *ForDatabase()
#   5-Sep-2019 jdw add extended DDL metadata.
#  24-Jan-2022 dwp Exclude all categories beginning with "ma_" from being mandatory
#                  (temporarily hardcoded here until new configuration file section added to achieve same effect)
#
#
##
"""
Assemble configuration and dictionary metadata required to build/load database schema definitions ...

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import textwrap
from collections import OrderedDict

from mmcif.api.PdbxContainers import CifName

logger = logging.getLogger(__name__)


class ContentDefinition(object):
    """Assemble configuration and dictionary metadata required to build/load database schema definitions ..."""

    def __init__(self, dictApi, contentDefHelper=None, databaseName=None, **kwargs):
        """
        Args:
            dictApi (object): instance of DictionaryApi() class
            contentHelper (object, optional): an instance of a contentHelper().
            databaseName (string, optional): name of a content database (e.g. 'chem_comp', 'bird', 'bird_family', 'pdbx')

        """
        self.__dApi = dictApi
        self.__databaseName = databaseName
        self.__setup(contentDefHelper, databaseName)
        _ = kwargs
        #

    def __setup(self, contentDefHelper, databaseName):
        #
        iTypeCodes = []
        iQueryStrings = []
        unitCardinalityList = []
        dataSelectFilterD = OrderedDict()

        itemTransformD = OrderedDict()
        self.__categoryContentClasses = OrderedDict()
        self.__attributeContentClasses = OrderedDict()
        self.__intEnumD = OrderedDict()
        iterableD = OrderedDict()
        #
        self.__categoryList = sorted(self.__dApi.getCategoryList())
        self.__categorySchema = {catName: sorted(self.__dApi.getAttributeNameList(catName)) for catName in self.__categoryList}
        ##
        _, attributeContextIndex = self.__indexContexts(self.__categorySchema)
        self.__keyReplaceItems = attributeContextIndex["RCSB_CATEGORY_KEY_SUBSTITION"] if "RCSB_CATEGORY_KEY_SUBSTITION" in attributeContextIndex else []
        self.__keyReplaceCategoryD = OrderedDict()
        for catName, atName in self.__keyReplaceItems:
            self.__keyReplaceCategoryD.setdefault(catName, []).append(atName)
        #
        logger.debug("Primary key replacements: %r", self.__keyReplaceItems)
        logger.debug("Primary key replacement category index: %r", self.__keyReplaceCategoryD)
        #
        if contentDefHelper and databaseName:
            self.__intEnumD = {(tD["CATEGORY_NAME"], tD["ATTRIBUTE_NAME"]): True for tD in contentDefHelper.getInternalEnumItems(databaseName)}
            logger.debug("Internal enum items %r", self.__intEnumD)
            #
            cardD = contentDefHelper.getCardinalityKeyItem(databaseName)
            logger.debug("Cardinality attribute %r", cardD.items())
            #
            unitCardinalityList = self.__getUnitCardinalityCategories([cardD])
            unitCardinalityList.extend(contentDefHelper.getCardinalityCategoryExtras())
            logger.debug("Cardinality categories %r", unitCardinalityList)
            #
            self.__categoryContentClasses = contentDefHelper.getCategoryContentClasses(databaseName)
            logger.debug("categoryContentClasses %r", self.__categoryContentClasses)
            #
            self.__attributeContentClasses = contentDefHelper.getAttributeContentClasses(databaseName)
            logger.debug("attributeContentClasses %r", self.__attributeContentClasses)
            #
            self.__sliceParentItemsD = contentDefHelper.getDatabaseSliceParents(databaseName)
            self.__sliceUnitCardinalityD = OrderedDict()
            self.__sliceCategoryExtrasD = OrderedDict()
            for sliceName, pDL in self.__sliceParentItemsD.items():
                logger.debug("Slicename %s parents %r", sliceName, pDL)
                #
                # Some categories are included in a slice even if they are unconnected to the slice parent.
                self.__sliceCategoryExtrasD[sliceName] = contentDefHelper.getSliceCategoryExtras(databaseName, sliceName)
                logger.debug("Slice %s extra categories %r", sliceName, self.__sliceCategoryExtrasD[sliceName])
                #
                self.__sliceUnitCardinalityD[sliceName] = self.__getUnitCardinalityCategories(pDL)
                logger.debug("Slicename %s unit cardinality categories %r", sliceName, self.__sliceUnitCardinalityD[sliceName])
                #
                self.__sliceUnitCardinalityD[sliceName].extend(contentDefHelper.getSliceCardinalityCategoryExtras(databaseName, sliceName))
                logger.debug("Slicename %s unit cardinality categories %r", sliceName, self.__sliceUnitCardinalityD[sliceName])
            #
            #
            subCategoryD = {}
            for catName, atNameList in self.__categorySchema.items():
                scL = []
                for atName in atNameList:
                    scL.extend(self.__dApi.getItemSubCategoryIdList(catName, atName))
                scL = list(OrderedDict.fromkeys(scL)) if scL else []
                subCategoryD[catName] = scL
            logger.debug("Subcategory category dictionary %r", subCategoryD)
            #
            self.__categoryFeatures = {catName: self.__getCategoryFeatures(catName, unitCardinalityList, subCategoryD) for catName in self.__categoryList}
            iTypeCodes = contentDefHelper.getTypeCodes("iterable")
            emiTypeCodes = contentDefHelper.getTypeCodes("embedded_iterable")
            logger.debug("emiTypeCodes %r", emiTypeCodes)
            iQueryStrings = contentDefHelper.getQueryStrings("iterable")
            logger.debug("iterable types %r iterable query %r", iTypeCodes, iQueryStrings)
            iterableD = self.__getIterables(iTypeCodes, iQueryStrings, contentDefHelper)
            embeddedIterableD = self.__getEmbeddedIterables(emiTypeCodes)
            logger.debug("iterableD %r", iterableD.items())
            logger.debug("embeddedIterableD %r", embeddedIterableD.items())
            dataSelectFilterD = contentDefHelper.getDatabaseSelectionFilters(databaseName)
            itemTransformD = contentDefHelper.getItemTransformD()
            logger.debug("itemTransformD %r", itemTransformD.items())
            #
            self.__selectionFiltersD = contentDefHelper.getDatabaseSelectionFilters(databaseName)
            self.__sliceParentFiltersD = contentDefHelper.getDatabaseSliceParentFilters(databaseName)
        else:
            logger.debug("Dictionary helper not loaded for schema %r", databaseName, stack_info=True)
            self.__selectionFiltersD = {}
            self.__sliceParentItemsD = {}
            self.__sliceParentFiltersD = {}
            self.__sliceUnitCardinalityD = {}
            self.__sliceCategoryExtrasD = {}
            embeddedIterableD = {}
            logger.warning("Missing dictionary helper method or schema %r", databaseName)

        #
        self.__methodD = self.__getMethodInfo()
        self.__attributeFeatures = OrderedDict(
            {catName: self.__getAttributeFeatures(catName, iterableD, embeddedIterableD, itemTransformD, self.__methodD) for catName in self.__categoryList}
        )
        self.__attributeDataTypeD = OrderedDict({catName: self.__getAttributeTypeD(catName) for catName in self.__categoryList})
        self.__dataSelectFilterD = dataSelectFilterD
        self.__sliceD = self.__getSliceChildren(self.__sliceParentItemsD)

    def getSelectionFiltersForDatabase(self):
        try:
            return self.__selectionFiltersD
        except Exception:
            pass
        return {}

    def getSliceParentFiltersForDatabase(self):
        try:
            return self.__sliceParentFiltersD
        except Exception:
            pass
        return {}

    def getSliceParentItemsForDatabase(self):
        try:
            return self.__sliceParentItemsD
        except Exception:
            pass
        return {}

    def getSliceCategoryExtrasForDatabase(self):
        try:
            return self.__sliceCategoryExtrasD
        except Exception:
            pass
        return {}

    def getSliceUnitCardinalityForDatabase(self):
        try:
            return self.__sliceUnitCardinalityD
        except Exception:
            pass
        return {}

    def __getContentClasses(self, catName, atName=None, wildCardAtName="__all__"):
        """Return a list of contexts for input category and optional attribute.

        Handle the special case of unspecified attributes interpreted as wildcard.

        Return:
          contextList (list): list of context names
        """
        cL = []
        try:
            if atName is None:
                cL = self.__categoryContentClasses[catName] if catName in self.__categoryContentClasses else []
            elif (catName, wildCardAtName) in self.__attributeContentClasses:
                cL = self.__attributeContentClasses[(catName, wildCardAtName)]
            else:
                cL = self.__attributeContentClasses[(catName, atName)] if (catName, atName) in self.__attributeContentClasses else []
        except Exception as e:
            logger.exception("Failing catName %s atName %s with %s", catName, atName, str(e))
        #
        return cL

    def __getSliceChildren(self, sliceParentD):
        """Internal method to build data structure containing the parent-child relationships for the
        input slice parent construction.

        """
        retD = OrderedDict()
        for sliceName, sliceParents in sliceParentD.items():
            sD = OrderedDict()
            for pD in sliceParents:
                parentCategoryName = pD["CATEGORY_NAME"]
                parentAttributeName = pD["ATTRIBUTE_NAME"]
                #
                sD[parentCategoryName] = [{"PARENT_CATEGORY_NAME": parentCategoryName, "PARENT_ATTRIBUTE_NAME": parentAttributeName, "CHILD_ATTRIBUTE_NAME": parentAttributeName}]
                #
                # childItems = self.__dApi.getFullChildList(parentCategoryName, parentAttributeName)
                childItems = self.__dApi.getFullDescendentList(parentCategoryName, parentAttributeName)
                # logger.info("Slice parent %s %s  %r" % (parentCategoryName, parentAttributeName, childItems))
                for childItem in childItems:
                    atName = CifName.attributePart(childItem)
                    catName = CifName.categoryPart(childItem)
                    # Ignore children in the parent category
                    if catName == parentCategoryName:
                        continue
                    if catName not in sD:
                        sD[catName] = []
                    sD[catName].append({"PARENT_CATEGORY_NAME": parentCategoryName, "PARENT_ATTRIBUTE_NAME": parentAttributeName, "CHILD_ATTRIBUTE_NAME": atName})
                # Sort the list of dictionaries for each category
                for catName in sD:
                    sD[catName] = sorted(sD[catName], key=lambda k: (k["PARENT_CATEGORY_NAME"], k["PARENT_ATTRIBUTE_NAME"], k["CHILD_ATTRIBUTE_NAME"]))

            retD[sliceName] = sD
        return retD

    def __getIterables(self, iTypeCodes, iQueryStrings, dictHelper):
        itD = OrderedDict()
        #
        typD = {d["TYPE_CODE"]: d["DELIMITER"] for d in iTypeCodes}

        for catName in self.__categoryList:
            for atName in self.__categorySchema[catName]:
                typeCode = self.__dApi.getTypeCode(catName, atName)
                typeCodeAlt = self.__dApi.getTypeCodeAlt(catName, atName)
                if typeCode in typD or typeCodeAlt in typD:
                    itD[(catName, atName)] = typD[typeCode]
                    continue
                description = self.__dApi.getDescription(catName, atName)
                for qs in iQueryStrings:
                    if qs in description:
                        itD[(catName, atName)] = dictHelper.getDelimiter(catName, atName)
        logger.debug("iterableD: %r", list(itD.items()))
        return itD

    def __getEmbeddedIterables(self, iTypeCodes):
        itD = OrderedDict()
        #
        typD = {d["TYPE_CODE"]: d["DELIMITER"] for d in iTypeCodes}

        for catName in self.__categoryList:
            for atName in self.__categorySchema[catName]:
                typeCode = self.__dApi.getTypeCode(catName, atName)
                typeCodeAlt = self.__dApi.getTypeCodeAlt(catName, atName)
                if typeCode in typD or typeCodeAlt in typD:
                    itD[(catName, atName)] = typD[typeCode]
                    continue
        logger.debug("embedded iterableD: %r", list(itD.items()))
        return itD

    def __getMethodInfo(self):
        methodD = OrderedDict()
        methodIndex = self.__dApi.getMethodIndex()
        for _, mrL in methodIndex.items():
            for mr in mrL:
                mId = mr.getId()
                catName = mr.getCategoryName()
                atName = mr.getAttributeName()
                mType = mr.getType()
                logger.debug("mId %r catName %r atName %r mType %r", mId, catName, atName, mType)
                if (catName, atName) not in methodD:
                    methodD[(catName, atName)] = []
                methDef = self.__dApi.getMethod(mId)
                if methDef:
                    mLang = methDef.getLanguage()
                    mCode = methDef.getCode()
                    mImplement = methDef.getImplementation()
                    dD = {"METHOD_LANGUAGE": mLang, "METHOD_IMPLEMENT": mImplement, "METHOD_TYPE": mType, "METHOD_CODE": mCode}
                    methodD[(catName, atName)].append(dD)
                else:
                    logger.error("Missing method definition for %s", mId)
        ##
        logger.debug("Method dictionary %r", methodD)
        return methodD

    def getMethodImplementation(self, catName, atName, methodCodes=None):
        try:
            mC = methodCodes if methodCodes else ["calculation"]
            if (catName, atName) in self.__methodD:
                for mD in self.__methodD[(catName, atName)]:
                    if mD["METHOD_CODE"].lower() in mC:
                        return mD["METHOD_IMPLEMENT"]
            return None

        except Exception as e:
            logger.exception("Failing catName %s atName %s with %s", catName, atName, str(e))
        return None

    def getSelectionFilters(self):
        """
        Returns:  relevant data selection filters for in a dictionary by filter name.
        """
        return self.__dataSelectFilterD

    def getCategories(self):
        """
        Returns:  list of dictionary categories
        """
        #
        return self.__categoryList

    def getSchemaNames(self):
        return self.__categorySchema

    def getCategoryAttributes(self, catName):
        """

        Returns: list of attributes in the input category
        """
        return self.__categorySchema[catName]

    def getCategoryFeatures(self, catName):
        try:
            return self.__categoryFeatures[catName]
        except Exception as e:
            logger.error("Missing category %s %s", catName, str(e))
        return {}

    def getAttributeFeatures(self, catName):
        try:
            return self.__attributeFeatures[catName]
        except Exception as e:
            logger.error("Missing category %s %s", catName, str(e))
        return {}

    def getAttributeDataTypeD(self):
        return self.__attributeDataTypeD

    def getSliceNames(self):
        try:
            return list(self.__sliceD.keys())
        except Exception as e:
            logger.error("Failing with %s", str(e))
        return []

    def getSliceAttributes(self, sliceName, catName):
        try:
            return self.__sliceD[sliceName][catName]
        except Exception as e:
            logger.debug("Failing to access sliceName %s catName %s with %s", sliceName, catName, str(e))
        return []

    def getSliceCategories(self, sliceName):
        try:
            return list(self.__sliceD[sliceName].keys())
        except Exception as e:
            logger.debug("Failing to access sliceName %s with %s", sliceName, str(e))
        return []

    def __getAttributeTypeD(self, catName):
        aD = {}
        for atName in self.__categorySchema[catName]:
            aD[atName] = self.__dApi.getTypeCode(catName, atName)
        return aD

    def __assignEnumTypes(self, enumList, pT):
        rL = []
        if enumList:
            if pT != "numb":
                return enumList
            else:
                isFloat = False
                for enum in enumList:
                    if "." in enum:
                        isFloat = True
                        break
                if isFloat:
                    # a rare case
                    rL = [float(enum) for enum in enumList]
                else:
                    rL = [int(enum) for enum in enumList]
                return rL
        else:
            return enumList

    def __assignExampleTypes(self, catName, atName, examList, pT):

        try:
            rL = []
            if examList:
                if pT != "numb":
                    return examList
                else:
                    isFloat = False
                    for enum in examList:
                        if "." in enum:
                            isFloat = True
                            break
                    if isFloat:
                        # a rare case
                        rL = [float(enum) for enum in examList]
                    else:
                        rL = [int(enum) for enum in examList]
                    return rL

            else:
                return examList
        except Exception:
            logger.exception("catName %r atName %r primitive %r failing for examList %r", catName, atName, pT, examList)
        return examList

    def __assignExampleTupTypes(self, catName, atName, examTupList, pT):
        try:
            rL = []
            if examTupList:
                if pT != "numb":
                    for exam, detail in examTupList:
                        exam = str(exam).strip() if exam else exam
                        detail = str(detail).strip() if detail else detail
                        rL.append((exam, detail))
                    return rL
                else:
                    isFloat = False
                    for exam, _ in examTupList:
                        if "." in exam:
                            isFloat = True
                            break
                    if isFloat:
                        for exam, detail in examTupList:
                            try:
                                if isFloat:
                                    rL.append((float(exam), detail))
                                else:
                                    rL.append((int(exam), detail))
                            except Exception:
                                pass
                    return rL
            else:
                return examTupList
        except Exception as e:
            logger.exception("catName %r atName %r (%r) %r %s", catName, atName, pT, examTupList, str(e))
        #
        return examTupList

    def __assignEnumTupTypes(self, enumTupList, pT):
        rL = []
        if enumTupList:
            if pT != "numb":
                return enumTupList
            else:
                isFloat = False
                for enum, _, _, _ in enumTupList:
                    if "." in enum:
                        isFloat = True
                        break
                if isFloat:
                    for enum, detail, brief, units in enumTupList:
                        try:
                            if isFloat:
                                rL.append((float(enum), detail, brief, units))
                            else:
                                rL.append((int(enum), detail, brief, units))
                        except Exception:
                            pass
                return rL
        else:
            return enumTupList

    def __hasEnumDetails(self, enumTupList):
        dCount = 0
        for _, detail, brief, units in enumTupList:
            if detail or brief or units:
                dCount += 1
        return dCount > 0

    def __itemNameToDictList(self, itemNameList):
        rL = []
        for itemName in list(OrderedDict.fromkeys(itemNameList)):
            atName = CifName.attributePart(itemName)
            catName = CifName.categoryPart(itemName)
            rL.append({"CATEGORY": catName, "ATTRIBUTE": atName})
        return rL

    def __getAttributeFeatures(self, catName, iterableD, embeddedIterableD, itemTransformD, methodD):
        """
        Args:
            catName (string): Category name
            iterableD (tuple, optional): iterable dictionary type codes
            iQueryStrings (list, optional): search strings applied to item descriptions to identify iterable candidates
            itemTransformD (dict): dictionary of data transform filters  itd[(catName,atName)] = [f1,f2,...]

        Returns:
            dict: attribute features


             cL = self.getCategoryContextList(catName)
        """
        aD = {}

        #
        # keyAtNames = [CifName.attributePart(kyItem) for kyItem in self.__dApi.getCategoryKeyList(catName)]
        keyAtNames = [CifName.attributePart(kyItem) for kyItem in self.__getCategoryKeysWithReplacement(catName)]
        for atName in self.__categorySchema[catName]:
            itemName = CifName.itemName(catName, atName)
            fD = {
                "CATEGORY_NAME": catName,
                "ATTRIBUTE_NAME": atName,
                "TYPE_CODE": None,
                "TYPE_CODE_ALT": None,
                "IS_MANDATORY": False,
                "CHILD_ITEMS": [],
                "CHILDREN": [],
                "ROOT_PARENT_ITEM": None,
                "ROOT_PARENT": None,
                "PARENT": None,
                "DESCRIPTION": None,
                "DESCRIPTION_ANNOTATED": [],
                "IS_KEY": False,
                "ITERABLE_DELIMITER": None,
                "EMBEDDED_ITERABLE_DELIMITER": None,
                "FILTER_TYPES": [],
                "IS_CHAR_TYPE": False,
                "METHODS": [],
                "CONTENT_CLASSES": [],
                "UNITS": None,
                "ENUMS": None,
                "ENUMS_ANNOTATED": None,
                "SEARCH_CONTEXTS": None,
            }
            fD["TYPE_CODE"] = self.__dApi.getTypeCode(catName, atName)
            fD["TYPE_CODE_ALT"] = self.__dApi.getTypeCodeAlt(catName, atName)
            fD["IS_MANDATORY"] = True if str(self.__dApi.getMandatoryCode(catName, atName)).lower() in ["y", "yes"] else False
            fD["DESCRIPTION"] = textwrap.dedent(self.__dApi.getDescription(catName, atName)).lstrip().rstrip()
            #
            fD["DESCRIPTION_ANNOTATED"] = [{"text": fD["DESCRIPTION"], "context": "dictionary"}]
            tS = self.__dApi.getDescriptionPdbx(catName, atName)
            if tS:
                fD["DESCRIPTION_ANNOTATED"].append({"text": textwrap.dedent(tS).lstrip().rstrip(), "context": "deposition"})
            #
            fD["UNITS"] = self.__dApi.getUnits(catName, atName)

            #
            fD["CHILD_ITEMS"] = self.__dApi.getFullChildList(catName, atName)
            fD["CHILDREN"] = self.__itemNameToDictList(self.__dApi.getFullChildList(catName, atName))
            #
            pItemName = self.__dApi.getUltimateParent(catName, atName)
            pName = pItemName if pItemName != itemName else None
            fD["ROOT_PARENT_ITEM"] = pName

            fD["ROOT_PARENT"] = self.__itemNameToDictList([pName])[0] if pName else None
            #
            pL = self.__dApi.getFullParentList(catName, atName, stripSelfParent=True)
            if pL:
                rL = self.__itemNameToDictList(pL)
                fD["PARENT"] = rL[0] if rL else None
                if len(rL) > 1:
                    logger.warning("Unexpected multiple parent definition for %s %s : %r", catName, atName, rL)
            #
            # logger.debug("catName %s atName %s : parent %r root_parent %r", catName, atName, fD['PARENT'], fD['ROOT_PARENT'])
            #
            fD["IS_KEY"] = atName in keyAtNames
            pType = self.__dApi.getTypePrimitive(catName, atName)
            fD["IS_CHAR_TYPE"] = str(pType).lower() in ["char", "uchar"]
            #
            fD["ITERABLE_DELIMITER"] = iterableD[(catName, atName)] if (catName, atName) in iterableD else None
            fD["EMBEDDED_ITERABLE_DELIMITER"] = embeddedIterableD[(catName, atName)] if (catName, atName) in embeddedIterableD else None
            #
            fD["FILTER_TYPES"] = itemTransformD[(catName, "__all__")] if (catName, "__all__") in itemTransformD else []
            fD["FILTER_TYPES"] = itemTransformD[(catName, atName)] if (catName, atName) in itemTransformD else fD["FILTER_TYPES"]
            #
            fD["METHODS"] = methodD[(catName, atName)] if (catName, atName) in methodD else []
            fD["CONTENT_CLASSES"] = self.__getContentClasses(catName, atName)
            if (catName, atName) in self.__intEnumD:
                fD["ENUMS"] = sorted(self.__assignEnumTypes(self.__dApi.getEnumListPdbx(catName, atName), pType))
                logger.debug("Using internal enums for %s %s %d", catName, atName, len(fD["ENUMS"]))
                enumTupList = self.__dApi.getEnumListAltWithFullDetails(catName, atName)
            else:
                fD["ENUMS"] = sorted(self.__assignEnumTypes(self.__dApi.getEnumList(catName, atName), pType))
                enumTupList = self.__dApi.getEnumListWithFullDetails(catName, atName)
            #
            if self.__hasEnumDetails(enumTupList):
                #
                fD["ENUMS_ANNOTATED"] = []
                for eTup in self.__assignEnumTupTypes(enumTupList, pType):
                    teD = {"value": eTup[0]}
                    if eTup[1]:
                        teD["detail"] = eTup[1]
                    if eTup[2]:
                        teD["name"] = eTup[2]
                    if eTup[3]:
                        teD["units"] = eTup[3]
                    fD["ENUMS_ANNOTATED"].append(teD)
            # -----
            fD["EXAMPLES"] = self.__assignExampleTupTypes(catName, atName, self.__dApi.getExampleListPdbx(catName, atName), pType)
            fD["EXAMPLES"].extend(self.__assignExampleTupTypes(catName, atName, self.__dApi.getExampleList(catName, atName), pType))
            # -----
            scL = []
            for scTup in self.__dApi.getItemSubCategoryList(catName, atName):
                if scTup[1] is not None:
                    qD = {"id": scTup[0], "label": scTup[1]}
                else:
                    qD = {"id": scTup[0]}
                scL.append(qD)
            fD["SUB_CATEGORIES"] = scL
            if len(scL) > 1:
                logger.debug("Multiple subcategories for %r %r %r", catName, atName, scL)
            #
            # bList = self.__dApi.getBoundaryListAlt(catName, atName, fallBack=True)
            bdList = self.__dApi.getBoundaryList(catName, atName)
            if bdList:
                minD = {}
                maxD = {}
                for (minV, maxV) in bdList:
                    if minV == maxV:
                        continue
                    if minV not in [".", "?"]:
                        minD[minV] = False
                    if maxV not in [".", "?"]:
                        maxD[maxV] = False
                for (minV, maxV) in bdList:
                    if minV == maxV and minV in minD:
                        minD[minV] = True
                    if minV == maxV and maxV in maxD:
                        maxD[maxV] = True
                for ky in minD:
                    if "." in ky:
                        kyV = float(ky)
                    else:
                        kyV = int(ky)
                    if minD[ky]:
                        fD["MIN_VALUE"] = kyV
                    else:
                        fD["MIN_VALUE_EXCLUSIVE"] = kyV
                for ky in maxD:
                    if "." in ky:
                        kyV = float(ky)
                    else:
                        kyV = int(ky)
                    if maxD[ky]:
                        fD["MAX_VALUE"] = kyV
                    else:
                        fD["MAX_VALUE_EXCLUSIVE"] = kyV
            #
            aD[atName] = fD
        #
        return aD

    def __getCategoryFeatures(self, catName, unitCardinalityList, subCategoryD):
        cD = {"KEY_ATTRIBUTES": []}
        # cD['KEY_ATTRIBUTES'] = [CifName.attributePart(keyItem) for keyItem in self.__dApi.getCategoryKeyList(catName)]
        cD["KEY_ATTRIBUTES"] = [CifName.attributePart(keyItem) for keyItem in self.__getCategoryKeysWithReplacement(catName)]
        cD["UNIT_CARDINALITY"] = catName in unitCardinalityList
        cD["CONTENT_CLASSES"] = self.__getContentClasses(catName)
        #
        # Exclude all categories beginning with "ma_" from being mandatory
        # (temporarily hardcoded here until new configuration file section added to achieve same effect)
        cD["IS_MANDATORY"] = True if str(self.__dApi.getCategoryMandatoryCode(catName)).lower() == "yes" and not catName.startswith("ma_") else False
        # cD["IS_MANDATORY"] = True if str(self.__dApi.getCategoryMandatoryCode(catName)).lower() == "yes" else False
        #
        cD["SUB_CATEGORIES"] = subCategoryD[catName] if catName in subCategoryD else []
        #
        return cD

    def __getUnitCardinalityCategories(self, parentDList):
        """Assign categories with unit cardinality relative to the input list of parent key items.

        parentDList (dict):  [{'CATEGORY_NAME':xxx 'ATTRIBUTE_NAME': xxxx}]

        Return: category name list
        """
        numParents = len(parentDList)
        logger.debug("Parent slice count %d def %r", numParents, parentDList)
        ucL = []
        #
        #  Find the common set of child categories for the input parent items
        comCatList = []
        for pD in parentDList:
            catList = [pD["CATEGORY_NAME"]]
            for childItem in self.__dApi.getFullChildList(pD["CATEGORY_NAME"], pD["ATTRIBUTE_NAME"]):
                childCategoryName = CifName.categoryPart(childItem)
                primaryKeyItemList = self.__dApi.getCategoryKeyList(childCategoryName)
                logger.debug("child category %r primary key items  %r", childCategoryName, primaryKeyItemList)
                # child must be part of the primary key to be a candidate
                if childItem in primaryKeyItemList:
                    catList.append(childCategoryName)
            if comCatList:
                comCatList = list(set(catList) & set(comCatList))
            else:
                comCatList.extend(catList)
        logger.debug("Common category list %r", comCatList)
        for cat in comCatList:
            primaryKeyItemList = self.__dApi.getCategoryKeyList(cat)
            if len(primaryKeyItemList) == numParents:
                ucL.append(cat)
        #
        logger.debug("Slice unit cardinality categories from parent-child relationships %r", ucL)
        return sorted(ucL)

    #

    def __getCategoryKeysWithReplacement(self, categoryName):
        if categoryName in self.__keyReplaceCategoryD:
            keyItems = [CifName.itemName(categoryName, atName) for atName in self.__keyReplaceCategoryD[categoryName]]
        else:
            keyItems = self.__dApi.getCategoryKeyList(categoryName)
        return sorted(keyItems)

    def __indexContexts(self, dictSchema):
        """Extract the category an item level dictionary contexts."""
        catIndex = {}
        atIndex = {}
        for catName in dictSchema:
            for cT in self.__dApi.getCategoryContextList(catName):
                catIndex.setdefault(cT, []).append(catName)

            for atName in dictSchema[catName]:
                for cT in self.__dApi.getContextList(catName, atName):
                    atIndex.setdefault(cT, []).append((catName, atName))
        return catIndex, atIndex
