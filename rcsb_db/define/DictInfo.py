##
# File:    DictInfo.py
# Author:  J. Westbrook
# Date:    8-May-2018
# Version: 0.001 Initial version
#
# Updates:
#  23-May-2018 jdw add method to return vanilla data Type dictionary/revise api -
#   5-Jun-2018 jdw replace IoAdapter() with MarshalUtil() -
#  10-Jun-2018 jdw move the base table definitions in terms of content classes
#  16-Jun-2018 jdw standardize method name for getAttributeDataTypeD()
#
#
##
"""
Assemble dictionary metadata required to construct and load a schema defintions.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.api.PdbxContainers import CifName

from rcsb_db.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class DictInfo(object):
    """ Assemble dictionary metadata required to build/load schema definition...

    """

    def __init__(self, dictLocators, dictHelper=None, **kwargs):
        """
        Args:
            dictLocators (list string): dictionary locator list

            dictHelper (class, optional): a subclass of of DictInfoHelperBase.
            dictSubset (string, optional): name of dictionary subset ('chem_comp', 'bird', 'bird_family', 'pdbx')

        """
        self.__setup(dictLocators, dictHelper, **kwargs)
        #

    def __setup(self, dictLocators, dictHelper, **kwargs):

        dictSubset = kwargs.get('dictSubset', None)
        mU = MarshalUtil()
        containerList = []
        for dictLocator in dictLocators:
            containerList.extend(mU.doImport(dictLocator, format="mmcif-dict"))
        #
        #  Not used in any public methods -
        #
        self.__dApi = DictionaryApi(containerList=containerList, consolidate=True, verbose=True)
        #
        iTypeCodes = []
        iQueryStrings = []
        unitCardinalityList = []
        dataSelectFilterD = {}
        itemTransformD = {}
        categoryContentClasses = {}
        attributeContentClasses = {}
        iterableD = {}
        #
        self.__categoryList = self.__dApi.getCategoryList()

        self.__dictSchema = {catName: self.__dApi.getAttributeNameList(catName) for catName in self.__categoryList}
        ##
        categoryContextIndex, attributeContextIndex = self.__indexContexts(self.__dictSchema)
        self.__keyReplaceItems = attributeContextIndex['RCSB_CATEGORY_KEY_SUBSTITION'] if 'RCSB_CATEGORY_KEY_SUBSTITION' in attributeContextIndex else []
        self.__keyReplaceCategoryD = {}
        for catName, atName in self.__keyReplaceItems:
            self.__keyReplaceCategoryD.setdefault(catName, []).append(atName)
        #
        logger.debug("Primary key replacements: %r" % self.__keyReplaceItems)
        logger.debug("Primary key replacement category index: %r" % self.__keyReplaceCategoryD)
        #
        if dictHelper and dictSubset:
            cardD = dictHelper.getCardinalityKeyItem(dictSubset)
            logger.debug("Cardinality attribute %r" % cardD.items())
            unitCardinalityList = self.__getUnitCardinalityCategories(cardD['CATEGORY_NAME'], cardD['ATTRIBUTE_NAME'])
            unitCardinalityList.extend(dictHelper.getCardinalityCategoryExtras())
            logger.debug("Cardinality categories %r" % unitCardinalityList)
            #
            categoryContentClasses = dictHelper.getCategoryContentClasses(dictSubset)
            logger.debug("categoryContentClasses %r" % categoryContentClasses)

            attributeContentClasses = dictHelper.getAttributeContentClasses(dictSubset)
            logger.debug("attributeContentClasses %r" % attributeContentClasses)
            #
            self.__categoryFeatures = {catName: self.__getCategoryFeatures(catName, unitCardinalityList, categoryContentClasses) for catName in self.__categoryList}
            iTypeCodes = dictHelper.getTypeCodes('iterable')
            iQueryStrings = dictHelper.getQueryStrings('iterable')
            iterableD = self.__getIterables(iTypeCodes, iQueryStrings, dictHelper)
            dataSelectFilterD = dictHelper.getSelectionFiltersBySubset(dictSubset)
            itemTransformD = dictHelper.getItemTransformD()
        else:
            logger.debug("Missing dictionary helper method")

        #
        self.__methodD = self.__getMethodInfo()
        self.__attributeFeatures = {
            catName: self.__getAttributeFeatures(
                catName,
                iterableD,
                itemTransformD,
                self.__methodD,
                attributeContentClasses) for catName in self.__categoryList}
        self.__attributeDataTypeD = {catName: self.__getAttributeTypeD(catName) for catName in self.__categoryList}
        self.__dataSelectFilterD = dataSelectFilterD

        #
        #

    def __getIterables(self, iTypeCodes, iQueryStrings, dictHelper):
        itD = {}
        for catName in self.__categoryList:
            for atName in self.__dictSchema[catName]:
                typeCode = self.__dApi.getTypeCode(catName, atName)
                typeCodeAlt = self.__dApi.getTypeCodeAlt(catName, atName)
                if typeCode in iTypeCodes or typeCodeAlt in iTypeCodes:
                    itD[(catName, atName)] = dictHelper.getDelimiter(catName, atName)
                    continue
                description = self.__dApi.getDescription(catName, atName)
                for qs in iQueryStrings:
                    if qs in description:
                        itD[(catName, atName)] = dictHelper.getDelimiter(catName, atName)
        return itD

    def __getMethodInfo(self):
        methodD = {}
        methodIndex = self.__dApi.getMethodIndex()
        for item, mrL in methodIndex.items():
            for mr in mrL:
                mId = mr.getId()
                catName = mr.getCategoryName()
                atName = mr.getAttributeName()
                mType = mr.getType()
                if (catName, atName) not in methodD:
                    methodD[(catName, atName)] = []
                methDef = self.__dApi.getMethod(mId)
                mLang = methDef.getLanguage()
                mCode = methDef.getCode()
                mImplement = methDef.getInline()
                d = {'METHOD_LANGUAGE': mLang, 'METHOD_IMPLEMENT': mImplement, 'METHOD_TYPE': mType, 'METHOD_CODE': mCode}
                methodD[(catName, atName)].append(d)
        ##
        logger.debug("Method dictionary %r" % methodD)
        return methodD

    def getMethodImplementation(self, catName, atName, methodCode="calculate_on_load"):
        try:
            if (catName, atName) in self.__methodD:
                for mD in self.__methodD[(catName, atName)]:
                    if mD['METHOD_CODE'].lower() in ['calculate_on_load']:
                        return mD['METHOD_IMPLEMENT']
            return None

        except Exception as e:
            logger.exception("Failing catName %s atName %s with %s" % (catName, atName, str(e)))
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

    def getNameSchema(self):
        return self.__dictSchema

    def getCategoryAttributes(self, catName):
        """

           Returns: list of attributes in the input category
        """
        return self.__dictSchema[catName]

    def getCategoryFeatures(self, catName):
        try:
            return self.__categoryFeatures[catName]
        except Exception as e:
            logger.error("Missing category %s %s" % (catName, str(e)))
        return {}

    def getAttributeFeatures(self, catName):
        try:
            return self.__attributeFeatures[catName]
        except Exception as e:
            logger.error("Missing category %s %s" % (catName, str(e)))
        return {}

    def getAttributeDataTypeD(self):
        return self.__attributeDataTypeD

    def __getAttributeTypeD(self, catName):
        aD = {}
        for atName in self.__dictSchema[catName]:
            aD[atName] = self.__dApi.getTypeCode(catName, atName)
        return aD

    def __getAttributeFeatures(self, catName, iterableD, itemTransformD, methodD, attributeContentClasses):
        """
        Args:
            catName (string): Description
            iTypeCodes (tuple, optional): iterable dictionary type codes
            iQueryStrings (list, optional): search strings applied to item descriptions to identify iterable candidates
            itemTransformD (dict): dictionary of data transform filters  itd[(catName,atName)] = [f1,f2,...]

        Returns:
            dict: attribure features


             cL = self.getCategoryContextList(catName)
        """
        aD = {}
        #
        # keyAtNames = [CifName.attributePart(kyItem) for kyItem in self.__dApi.getCategoryKeyList(catName)]
        keyAtNames = [CifName.attributePart(kyItem) for kyItem in self.__getCategoryKeysWithReplacement(catName)]
        for atName in self.__dictSchema[catName]:
            fD = {
                'TYPE_CODE': None,
                'TYPE_CODE_ALT': None,
                'IS_MANDATORY': False,
                "CHILD_ITEMS": [],
                'DESCRIPTION': None,
                'IS_KEY': False,
                "ITERABLE_DELIMITER": None,
                'FILTER_TYPES': [],
                'IS_CHAR_TYPE': False,
                'METHODS': [],
                'CONTENT_CLASSES': []}
            fD['TYPE_CODE'] = self.__dApi.getTypeCode(catName, atName)
            fD['TYPE_CODE_ALT'] = self.__dApi.getTypeCodeAlt(catName, atName)
            fD['IS_MANDATORY'] = True if str(self.__dApi.getMandatoryCode(catName, atName)).lower() in ['y', 'yes'] else False
            fD['DESCRIPTION'] = self.__dApi.getDescription(catName, atName)
            fD['CHILD_ITEMS'] = self.__dApi.getFullChildList(catName, atName)
            fD['IS_KEY'] = atName in keyAtNames
            fD['IS_CHAR_TYPE'] = str(self.__dApi.getTypePrimitive(catName, atName)).lower() in ['char', 'uchar']
            #
            fD['ITERABLE_DELIMITER'] = iterableD[(catName, atName)] if (catName, atName) in iterableD else None
            fD['FILTER_TYPES'] = itemTransformD[(catName, atName)] if (catName, atName) in itemTransformD else []
            #
            fD['METHODS'] = methodD[(catName, atName)] if (catName, atName) in methodD else []
            fD['CONTENT_CLASSES'] = attributeContentClasses[(catName, atName)] if (catName, atName) in attributeContentClasses else []
            aD[atName] = fD
        #
        return aD

    def __getCategoryFeatures(self, catName, unitCardinalityList, categoryContentClasses):
        cD = {'KEY_ATTRIBUTES': []}
        # cD['KEY_ATTRIBUTES'] = [CifName.attributePart(keyItem) for keyItem in self.__dApi.getCategoryKeyList(catName)]
        cD['KEY_ATTRIBUTES'] = [CifName.attributePart(keyItem) for keyItem in self.__getCategoryKeysWithReplacement(catName)]
        cD['UNIT_CARDINALITY'] = catName in unitCardinalityList
        cD['CONTENT_CLASSES'] = categoryContentClasses[catName] if catName in categoryContentClasses else []
        #
        return cD

    def __getUnitCardinalityCategories(self, categoryName, attributeName):
        """ Assign categories with unit cardinality related to the input parent key item.

            Return: category list
        """
        ucL = []
        #
        # The input category is unit cardinality by definition
        #
        ucL.append(categoryName)
        #
        for childItem in self.__dApi.getFullChildList(categoryName, attributeName):
            # logger.debug("Category %s attribute %s child %r" % (categoryName, attributeName, childItem))
            #
            childCategoryName = CifName.categoryPart(childItem)
            pKyL = self.__dApi.getCategoryKeyList(childCategoryName)
            # logger.debug("Category %s attribute %s key items %r" % (categoryName, attributeName, pKyL))
            if len(pKyL) == 1 and childItem in pKyL:
                ucL.append(CifName.categoryPart(pKyL[0]))
        return ucL
    #

    def __getCategoryKeysWithReplacement(self, categoryName):
        if categoryName in self.__keyReplaceCategoryD:
            keyItems = [CifName.itemName(categoryName, atName) for atName in self.__keyReplaceCategoryD[categoryName]]
        else:
            keyItems = self.__dApi.getCategoryKeyList(categoryName)
        return keyItems

    def __indexContexts(self, dictSchema):
        """  Extract the category an item level dictionary contexts.
        """
        catIndex = {}
        atIndex = {}
        for catName in dictSchema:
            for c in self.__dApi.getCategoryContextList(catName):
                catIndex.setdefault(c, []).append(catName)

            for atName in dictSchema[catName]:
                for c in self.__dApi.getContextList(catName, atName):
                    atIndex.setdefault(c, []).append((catName, atName))
        return catIndex, atIndex
