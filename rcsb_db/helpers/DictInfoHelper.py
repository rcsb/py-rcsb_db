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

from rcsb_db.helpers.DictInfoHelperBase import DictInfoHelperBase
from rcsb_db.loaders.DataTransformFactory import DataTransformInfo

logger = logging.getLogger(__name__)


class DictInfoHelper(DictInfoHelperBase):
    """ Supplements dictionary information as required for schema production.

    """
    # Data items requiring a particular data transformation -
    _itemTransformers = {
        'STRIP_WS': [
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_c_terminal_seq_one_letter_code'},
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_n_terminal_seq_one_letter_code'},
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_seq_one_letter_code'},
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_seq_one_letter_code_can'},
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_seq_one_letter_code_sample'},
            {'CATEGORY_NAME': 'struct_ref', 'ATTRIBUTE_NAME': 'pdbx_seq_one_letter_code'}
        ]
    }

    _cardinalityItems = {
        'bird': {'CATEGORY_NAME': 'pdbx_reference_molecule', 'ATTRIBUTE_NAME': 'prd_id'},
        'bird_family': {'CATEGORY_NAME': 'pdbx_reference_molecule_family', 'ATTRIBUTE_NAME': 'family_prd_id'},
        'chem_comp': {'CATEGORY_NAME': 'chem_comp', 'ATTRIBUTE_NAME': 'id'},
        'bird_chem_comp': {'CATEGORY_NAME': 'chem_comp', 'ATTRIBUTE_NAME': 'id'},
        'pdbx': {'CATEGORY_NAME': 'entry', 'ATTRIBUTE_NAME': 'id'}
    }
    _cardinalityCategoryExtras = ['rcsb_load_status']
    #
    _selectionFilters = {('PUBLIC_RELEASE', 'pdbx'): [{'CATEGORY_NAME': 'pdbx_database_status', 'ATTRIBUTE_NAME': 'status_code', 'VALUES': ['REL']}],
                         ('PUBLIC_RELEASE', 'chem_comp'): [{'CATEGORY_NAME': 'pdbx_database_status', 'ATTRIBUTE_NAME': 'status_code', 'VALUES': ['REL']}],
                         ('PUBLIC_RELEASE', 'bird_chem_comp'): [{'CATEGORY_NAME': 'pdbx_database_status', 'ATTRIBUTE_NAME': 'status_code', 'VALUES': ['REL']}],
                         ('PUBLIC_RELEASE', 'bird'): [{'CATEGORY_NAME': 'pdbx_reference_molecule', 'ATTRIBUTE_NAME': 'release_status', 'VALUES': ['REL', 'OBS']}],
                         ('PUBLIC_RELEASE', 'bird_family'): [{'CATEGORY_NAME': 'pdbx_reference_molecule_family', 'ATTRIBUTE_NAME': 'release_status', 'VALUES': ['REL', 'OBS']}]
                         }

    _typeCodeClasses = {'iterable': ['ucode-alphanum-csv', 'id_list']}
    _queryStringSelectors = {'iterable': ['comma separate']}
    # Put the non default iterable delimiter cases here -
    _iterableDelimiters = [{'CATEGORY_NAME': 'chem_comp', 'ATTRIBUTE_NAME': 'pdbx_synonyms', 'DELIMITER': ';'}]
    #

    _contentClasses = {('ADMIN_CATEGORY', 'pdbx'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']},
                                                    {'CATEGORY_NAME': 'pdbx_struct_assembly_gen', 'ATTRIBUTE_NAME_LIST': ['ordinal']}],
                       ('ADMIN_CATEGORY', 'chem_comp'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']}],
                       ('ADMIN_CATEGORY', 'bird_chem_comp'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']}],
                       ('ADMIN_CATEGORY', 'bird'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']}],
                       ('ADMIN_CATEGORY', 'bird_family'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']}],
                       }

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (below)
            dictPath (str, optional): path to the current dictioonary text
            dictSubset (str, optional): name of dictionary content subset


            Add - Include exclude filters on dictionary content -


        """
        super(DictInfoHelper, self).__init__(**kwargs)
        self._dictSubset = kwargs.get('dictSubset', None)
        self._dictPath = kwargs.get("dictPath", None)
        #
        self.__dti = DataTransformInfo()
        self.__itD = self.__getItemTransformD()
        #
        self.__categoryClasses = self.__getCategoryContentClasses()
        self.__attributeClasses = self.__getAttributeContentClasses()

    def getDelimiter(self, categoryName, attributeName, default=','):
        for tD in DictInfoHelper._iterableDelimiters:
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
            for cTup, cDL in DictInfoHelper._contentClasses.items():
                for cD in cDL:
                    if cD['CATEGORY_NAME'] not in classD:
                        classD[cD['CATEGORY_NAME']] = []
                    classD[cD['CATEGORY_NAME']].append({'CONTENT_CLASS': cTup[0], 'DICT_SUBSET': cTup[1]})
        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return classD

    def __getAttributeContentClasses(self):
        classD = {}
        try:
            for cTup, cDL in DictInfoHelper._contentClasses.items():
                for cD in cDL:
                    catName = cD['CATEGORY_NAME']
                    for atName in cD['ATTRIBUTE_NAME_LIST']:
                        if (catName, atName) not in classD:
                            classD[(catName, atName)] = []
                        classD[(catName, atName)].append({'CONTENT_CLASS': cTup[0], 'DICT_SUBSET': cTup[1]})
        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return classD

    def __getItemTransformD(self):
        itD = {}
        for f, dL in DictInfoHelper._itemTransformers.items():
            if not self.__dti.isImplemented(f):
                continue
            for d in dL:
                if (d['CATEGORY_NAME'], d['ATTRIBUTE_NAME']) in itD:
                    itD[(d['CATEGORY_NAME'], d['ATTRIBUTE_NAME'])] = []
                itD[(d['CATEGORY_NAME'], d['ATTRIBUTE_NAME'])].append(f)
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
                return DictInfoHelper._itemTransformers[transformName]
        except Exception:
            return []

    def getItemTransforms(self, categoryName, attributeName):
        """ Return the list of transforms to be applied to the input item (categoryName, attributeName).
        """
        try:
            self.__itD[(categoryName, attributeName)]
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
        return DictInfoHelper._cardinalityCategoryExtras

    def getCardinalityKeyItem(self, dictSubset):
        """ Identify the parent item for the dictionary subset that can be used to
            identify child categories with unity cardinality.   That is, logically containing
            a single data row in any instance.

        """
        try:
            return DictInfoHelper._cardinalityItems[dictSubset]
        except Exception:
            pass
        return {'CATEGORY_NAME': None, 'ATTRIBUTE_NAME': None}

    def getTypeCodes(self, kind):
        """
        """
        try:
            DictInfoHelper._typeCodeClasses[kind]
        except Exception:
            pass
        return []

    def getQueryStrings(self, kind):
        try:
            DictInfoHelper._queryStringSelectors[kind]
        except Exception:
            pass

        return []

    def getSelectionFilter(self, dictSubset, kind):
        """  Interim api for selection filters defined in terms of dictionary category and attributes name and their values.

        """
        try:
            return DictInfoHelper._selectionFilters[(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getSelectionFiltersBySubset(self, dictSubset):
        """  Interim api for selection filters for a particular dictionary subset.

        """
        try:
            return {kind: v for (kind, dS), v in DictInfoHelper._selectionFilters.items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getContentClass(self, dictSubset, kind):
        """  Interim api for special category classes.

        """
        try:
            return DictInfoHelper._specialContent[(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getContentClassBySubset(self, dictSubset):
        """  Interim api for special category classes.

        """
        try:
            return {kind: v for (kind, dS), v in DictInfoHelper._specialContent.items() if dS == dictSubset}
        except Exception:
            pass
        return {}
