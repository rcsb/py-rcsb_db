##
# File:    SchemaDefDictInfo.py
# Author:  J. Westbrook
# Date:    8-May-2018
# Version: 0.001 Initial version
#
# Updates:
#  23-May-2018 jdw add method to return vanilla data Type dictionary/revise api -
#
#
##
"""
Extract dictionary metadata required to construct schema defintions.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
logger = logging.getLogger(__name__)

from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.api.PdbxContainers import CifName

from rcsb_db.utils.IoUtil import IoUtil


class SchemaDefDictInfo(object):
    """ Assemble dictionary metadata required to build schema definition...

    """

    def __init__(self, dictPath, dictSubset=None, dictHelper=None):
        """
        Args:
            dictPath (string): dictionary path
            dictSubset (string, optional): name of dictionary subset ('chem_comp', 'bird', 'bird_family', 'pdbx')
            dictHelper (class, optional): a subclass of of DictInfoHelperBase.

        """
        self.__setup(dictPath, dictSubset, dictHelper)
        #

    def __setup(self, dictPath, dictSubset, dictHelper):
        myIo = IoAdapter(raiseExceptions=True)
        containerList = myIo.readFile(inputFilePath=dictPath)
        self.__dApi = DictionaryApi(containerList=containerList, consolidate=True, verbose=True)
        #
        iTypeCodes = []
        iQueryStrings = []
        unitCardinalityList = []
        #
        self.__categoryList = self.getCategories()
        #
        if dictHelper and dictSubset:
            (pCategoryName, pAttributeName) = dictHelper.getCardinalityKeyItem(dictSubset)
            unitCardinalityList = self.__getUnitCardinalityCategories(pCategoryName, pAttributeName)
            self.__categoryFeatures = {catName: self.__getCategoryFeatures(catName, unitCardinalityList) for catName in self.__categoryList}
            iTypeCodes = dictHelper.getTypeCodes('iterable')
            iQueryStrings = dictHelper.getQueryStrings('iterable')

        #
        self.__dictSchema = {catName: self.getCategoryAttributes(catName) for catName in self.__categoryList}
        self.__attributeFeatures = {catName: self.__getAttributeFeatures(catName, iTypeCodes, iQueryStrings) for catName in self.__categoryList}
        self.__dataTypeD = {catName: self.__getAttributeTypeD(catName) for catName in self.__categoryList}

    def getCategories(self):
        """
            Returns:  list of dictionary categories
        """
        #
        return self.__dApi.getCategoryList()

    def getNameSchema(self):
        return self.__dictSchema

    def getCategoryAttributes(self, catName):
        """

           Returns: list of attributes in the input category
        """
        return self.__dApi.getAttributeNameList(catName)

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

    def getDataTypeD(self):
        return self.__dataTypeD

    def __getAttributeTypeD(self, catName):
        aD = {}
        for atName in self.__dictSchema[catName]:
            aD[atName] = self.__dApi.getTypeCode(catName, atName)
        return aD

    def __getAttributeFeatures(self, catName, iTypeCodes, iQueryStrings):
        """
        Args:
            catName (string): Description
            iTypeCodes (tuple, optional): iterable dictionary type codes
            iQueryStrings (list, optional): search strings applied to item descriptions to identify iterable candidates

        Returns:
            dict: attribure features
        """
        aD = {}
        #
        keyAtNames = [CifName.attributePart(kyItem) for kyItem in self.__dApi.getCategoryKeyList(catName)]
        for atName in self.__dictSchema[catName]:
            fD = {'TYPE_CODE': None, 'TYPE_CODE_ALT': None, 'IS_MANDATORY': False, "CHILD_ITEMS": [], 'DESCRIPTION': None, 'IS_KEY': False, "IS_ITERABLE": False}
            fD['TYPE_CODE'] = self.__dApi.getTypeCode(catName, atName)
            fD['TYPE_CODE_ALT'] = self.__dApi.getTypeCodeAlt(catName, atName)
            fD['IS_MANDATORY'] = True if self.__dApi.getMandatoryCodeAlt(catName, atName) in ['Y', 'y'] else False
            fD['DESCRIPTION'] = self.__dApi.getDescription(catName, atName)
            fD['CHILD_ITEMS'] = self.__dApi.getFullChildList(catName, atName)
            fD['IS_KEY'] = atName in keyAtNames
            #
            fD['IS_ITERABLE'] = False
            if fD['TYPE_CODE'] in iTypeCodes or fD['TYPE_CODE_ALT'] in iTypeCodes:
                fD['IS_ITERABLE'] = True
            else:
                for qs in iQueryStrings:
                    if qs in fD['DESCRIPTION']:
                        fD['IS_ITERABLE'] = True

            aD[atName] = fD
        #
        return aD

    def __getCategoryFeatures(self, catName, unitCardinalityList):
        cD = {'KEY_ATTRIBUTES': []}
        cD['KEY_ATTRIBUTES'] = [CifName.attributePart(keyItem) for keyItem in self.__dApi.getCategoryKeyList(catName)]
        cD['UNIT_CARDINALITY'] = catName in unitCardinalityList
        #
        return cD

    def __getUnitCardinalityCategories(self, categoryName, attributeName):
        """ Assign categories with unit cardinality related to the input parent key item.

            Return: category list
        """
        ucL = []
        for childItem in self.__dApi.getFullChildList(categoryName, attributeName):
            childCategoryName = CifName.categoryPart(childItem)
            pKyL = self.__dApi.getCategoryKeyList(childCategoryName)
            if len(pKyL) == 1:
                ucL.append(CifName.categoryPart(pKyL[0]))
        return ucL
    #
