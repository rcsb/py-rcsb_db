##
# File:    DictInfoHelper.py
# Author:  J. Westbrook
# Date:    18-May-2018
# Version: 0.001 Initial version
#
# Updates:
#  23-May-2018  jdw revise dependencies for helper function.  Change method api's
#  27-May-2018  jdw add attribrute level filters
#   1-Jun-2018  jdw add bridging class DataTransformInfo for attribute filters
#  13-Jun-2018  jdw add content classes to cover former base table feature
#  15-Jun-2018  jdw add support for alternative iterable delimiters
#  24-Jul-2018  jdw fix logic in processing _itemTransformers data
#   6-Aug-2018  jdw add slice parent item definitions in terms of parent items
#   8-Aug-2018  jdw add slice parent conditional filter definitions that could be applied to the parent data category,
#  10-Aug-2018  jdw add slice category and cardinality extras
#  18-Aug-2018  jdw add schema pdbx_core analogous to pdbx removing the block attribute.
#   7-Sep-2018  jdw add generated content classes for core schemas
#  10-Sep-2018  jdw add iterable details for semicolon separated text data
#  11-Sep-2018  jdw adjust slice cardinality constraints for entity and assembly identifier categories.
#  30-Sep-2018  jdw add source and host organism categories
#   2-Oct-2018  jdw add repository_holdings and sequence_cluster content types and associated category content.
#  12-Feb-2019  jdw add wildCardAtName argument on __getItemTransformD()
##
"""
This helper class supplements dictionary information as required for schema production.
I other words, the additional content conferred here would best be incorporated as part
of standard data definitions at some future point.

This is the single source of additional dictionary specific semantic content and is
design to avoid scattering dictionary semantic content haphazardly throughout the
code base.

All data accessors and structures here refer to dictionary category and attribute names.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.helpers.DictInfoHelperBase import DictInfoHelperBase
from rcsb.db.processors.DataTransformFactory import DataTransformInfo

logger = logging.getLogger(__name__)


class DictInfoHelper(DictInfoHelperBase):
    """ Supplements dictionary information as required for schema production.

    """

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (below)
            dictPath (str, optional): path to the current dictioonary text
            dictSubset (str, optional): name of dictionary content subset - alias for schema name


            Add - Include exclude filters on dictionary content -


        """
        super(DictInfoHelper, self).__init__(**kwargs)
        # ----
        #
        self.__cfgOb = kwargs.get('cfgOb', None)
        sectionName = kwargs.get('config_section', 'dictionary_helper')
        self.__cfgD = self.__cfgOb.exportConfig(sectionName=sectionName)
        #
        # ----
        self._dictSubset = kwargs.get('dictSubset', None)
        self._dictPath = kwargs.get("dictPath", None)
        #
        self.__dti = DataTransformInfo()
        self.__itD = self.__getItemTransformD()
        #
        self.__categoryClasses = self.__getCategoryContentClasses()
        self.__attributeClasses = self.__getAttributeContentClasses()

    def getDelimiter(self, categoryName, attributeName, default=','):
        for tD in self.__cfgD['iterable_delimiters']:
            if tD['CATEGORY_NAME'] == categoryName and tD['ATTRIBUTE_NAME'] == attributeName:
                return tD['DELIMITER']
        return default

    def getCategoryContentClasses(self, dictSubset):
        try:
            rD = {}
            for catName, cDL in self.__categoryClasses.items():
                for cD in cDL:
                    if cD['DICT_SUBSET'] == dictSubset:
                        if catName not in rD:
                            rD[catName] = []
                        rD[catName].append(cD['CONTENT_CLASS'])

        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return rD

    def getAttributeContentClasses(self, dictSubset):
        try:
            rD = {}
            for (catName, atName), cDL in self.__attributeClasses.items():
                for cD in cDL:
                    if cD['DICT_SUBSET'] == dictSubset:
                        if (catName, atName) not in rD:
                            rD[(catName, atName)] = []
                        rD[(catName, atName)].append(cD['CONTENT_CLASS'])

        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return rD

    def __getCategoryContentClasses(self):
        classD = {}
        try:
            for cTup, cDL in self.__cfgD['content_classes'].items():
                for cD in cDL:
                    if cD['CATEGORY_NAME'] not in classD:
                        classD[cD['CATEGORY_NAME']] = []
                    classD[cD['CATEGORY_NAME']].append({'CONTENT_CLASS': cTup[0], 'DICT_SUBSET': cTup[1]})
        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return classD

    def __getAttributeContentClasses(self, wildCardAtName='__all__'):
        classD = {}
        try:
            for cTup, cDL in self.__cfgD['content_classes'].items():
                for cD in cDL:
                    catName = cD['CATEGORY_NAME']
                    # if now optional 'ATTRIBUTE_NAME_LIST' is absent insert wildcard attribute
                    if 'ATTRIBUTE_NAME_LIST' in cD:
                        for atName in cD['ATTRIBUTE_NAME_LIST']:
                            if (catName, atName) not in classD:
                                classD[(catName, atName)] = []
                            classD[(catName, atName)].append({'CONTENT_CLASS': cTup[0], 'DICT_SUBSET': cTup[1]})
                    else:
                        if (catName, wildCardAtName) not in classD:
                            classD[(catName, wildCardAtName)] = []
                        classD[(catName, wildCardAtName)].append({'CONTENT_CLASS': cTup[0], 'DICT_SUBSET': cTup[1]})
        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return classD

    def __getItemTransformD(self, wildCardAtName='__all__'):
        itD = {}
        for f, dL in self.__cfgD['item_transformers'].items():
            logger.debug("Verify transform method %r" % f)
            if self.__dti.isImplemented(f):
                for d in dL:
                    atN = d['ATTRIBUTE_NAME'] if 'ATTRIBUTE_NAME' in d else wildCardAtName
                    itD.setdefault((d['CATEGORY_NAME'], atN), []).append(f)
                    # if (d['CATEGORY_NAME'], d['ATTRIBUTE_NAME']) not in itD:
                    #     itD[(d['CATEGORY_NAME'], d['ATTRIBUTE_NAME'])] = []
                    # itD[(d['CATEGORY_NAME'], d['ATTRIBUTE_NAME'])].append(f)

        return itD

    def getDictPath(self):
        return self._dictPath

    def getDictSubSet(self):
        return self._dictSubset

    def getTransformItems(self, transformName):
        """ Return the list of items subject to the input attribute filter.

            _itemTransformers{<filterName> : [{'CATEGORY_NAME':..., 'ATTRIBUTE_NAME': ... },{}]
        """
        try:
            if self.__dti.isImplemented(transformName):
                return self.__cfgD['item_transformers'][transformName]
        except Exception:
            return []

    def getItemTransforms(self, categoryName, attributeName):
        """ Return the list of transforms to be applied to the input item (categoryName, attributeName).
        """
        try:
            return self.__itD[(categoryName, attributeName)]
        except Exception:
            return []

    def getItemTransformD(self):
        """ Return the dictionary of transforms to be applied to the input item (categoryName, attributeName).
        """
        try:
            return self.__itD
        except Exception:
            return {}

    def getCardinalityCategoryExtras(self):
        return self.__cfgD['cardinality_category_extras']

    def getCardinalityKeyItem(self, dictSubset):
        """ Identify the parent item for the dictionary subset that can be used to
            identify child categories with unity cardinality.   That is, logically containing
            a single data row in any instance.

        """
        try:
            return self.__cfgD['cardinality_parent_items'][dictSubset]
        except Exception:
            pass
        return {'CATEGORY_NAME': None, 'ATTRIBUTE_NAME': None}

    def getTypeCodes(self, kind):
        """ Get the list of CIF type codes of a particular kind.

           returns (dict) [{'TYPE_CODE': <type> ... other feature of the type}]
        """
        try:
            return self.__cfgD['type_code_classes'][kind]
        except Exception as e:
            logger.exception("Failing for kind %r with %s" % (kind, str(e)))
            pass
        return []

    def getQueryStrings(self, kind):
        try:
            return self.__cfgD['query_string_selectors'][kind]
        except Exception:
            pass

        return []

    def getSelectionFilter(self, dictSubset, kind):
        """  Interim api for selection filters defined in terms of dictionary category and attributes name and their values.

        """
        try:
            return self.__cfgD['selection_filters'][(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getSelectionFiltersBySubset(self, dictSubset):
        """  Interim api for selection filters for a particular dictionary subset.

        """
        try:
            return {kind: v for (kind, dS), v in self.__cfgD['selection_filters'].items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getContentClass(self, dictSubset, kind):
        """  Interim api for special category classes.

        """
        try:
            return self.__cfgD['special_content'][(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getContentClassBySubset(self, dictSubset):
        """  Interim api for special category classes.

        """
        try:
            return {kind: v for (kind, dS), v in self.__cfgD['special_content'].items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getSliceParentItems(self, dictSubset, kind):
        """  Interim api for slice parent itens defined in terms of dictionary category and attributes name and their values.

        """
        try:
            return self.__cfgD['slice_parent_items'][(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getSliceParentsBySubset(self, dictSubset):
        """  Interim api for slice parent items for a particular dictionary subset.

        """
        try:
            return {kind: v for (kind, dS), v in self.__cfgD['slice_parent_items'].items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getSliceParentFilters(self, dictSubset, kind):
        """  Interim api for slice parent condition filters defined in terms of dictionary category and attributes name and their values.

        """
        try:
            return self.__cfgD['slice_parent_filters'][(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getSliceParentFiltersBySubset(self, dictSubset):
        """  Interim api for slice parent condition filters for a particular dictionary subset.

        """
        try:
            return {kind: v for (kind, dS), v in self.__cfgD['slice_parent_filters'].items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getSliceCardinalityCategoryExtras(self, dictSubset, kind):
        try:
            return self.__cfgD['slice_cardinality_category_extras'][(kind, dictSubset)]
        except Exception:
            return []

    def getSliceCategoryExtras(self, dictSubset, kind):
        try:
            return self.__cfgD['slice_category_extras'][(kind, dictSubset)]
        except Exception:
            return []
