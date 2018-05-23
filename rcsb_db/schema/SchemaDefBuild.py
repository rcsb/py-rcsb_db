##
# File:    SchemaDefBuild.py
# Author:  J. Westbrook
# Date:    1-May-2018
# Version: 0.001 Initial version
#
# Updates:
#
#  9-May-2018 jdw integrate dictionary and file based (type/coverage) data.
##
"""
Integrate dictionary metadata and file based(type/coverage) into a schema defintions.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import re
import logging
logger = logging.getLogger(__name__)

from rcsb_db.schema.DataTypeInfo import DataTypeInfo
from rcsb_db.schema.SchemaDefDictInfo import SchemaDefDictInfo


class SchemaApplicationInfo(object):
    """ Assemble application metadata required to build schema definition...

    """

    def __init__(self, applicationName='ANY', filePath=None):
        self.__filePath = filePath
        self.__applicationName = applicationName
        self.__filePath = filePath

        self.__setup(self.__filePath, self.__applicationName)

    def __setup(self, filePath, applicationName):
        ddt = DataTypeInfo(filePath=filePath, applicationName=applicationName)
        if filePath:
            self.__dtmD = ddt.readDefaultDataTypeMap(filePath, applicationName='ANY')
        else:
            self.__dtmD = ddt.getDefaultDataTypeMap(applicationName='ANY')

    def __appInfo(self, catName, atName):
        """
            data_rcsb_schema_info

            Data set item and category schema info -

                _pdbx_item_schema_info.data_set_id
                _pdbx_item_schema_info.item_name
                _pdbx_item_schema_info.populated_count
                _pdbx_item_schema_info.max_field_width
                _pdbx_item_schema_info.min_field_width
                _pdbx_item_schema_info.min_precision
                _pdbx_item_schema_info.max_precision

                _pdbx_category_schema_info.data_set_id
                _pdbx_category_schema_info.category_id
                _pdbx_category_schema_info.min_row_count
                _pdbx_category_schema_info.max_row_count

            Data set item and category schema summary info -

                _pdbx_item_schema_summary_info.item_name
                _pdbx_item_schema_summary_info.data_set_populated_count
                _pdbx_item_schema_summary_info.data_set_total_count
                _pdbx_item_schema_summary_info.max_field_width
                _pdbx_item_schema_summary_info.min_field_width
                _pdbx_item_schema_summary_info.min_precision
                _pdbx_item_schema_summary_info.max_precision
                _pdbx_item_schema_summary_info.update_date

                _pdbx_category_schema_summary_info.category_id
                _pdbx_category_schema_summary_info.data_set_populated_count
                _pdbx_category_schema_summary_info.data_set_total_count
                _pdbx_category_schema_summary_info.min_row_count
                _pdbx_category_schema_summary_info.max_row_count
                _pdbx_category_schema_summary_info.update_date


            Application data type map -

                _pdbx_application_data_type_map.application_name
                _pdbx_application_data_type_map.type_code
                _pdbx_application_data_type_map.default_type_code
                _pdbx_application_data_type_map.default_precision
                _pdbx_application_data_type_map.default_width


            Application index list -

                _pdbx_application_index_list.application_name
                _pdbx_application_index_list.index_name
                _pdbx_application_index_list.category_id
                _pdbx_application_index_list.index_type
                _pdbx_application_index_list.attribute_name

        """


class SchemaDefBuild(object):

    """ Build schema definitions from dictionary and auxilary content.
        {
        'ATOM_SITES_ALT': {
                    'ATTRIBUTES': {'DETAILS': 'details', 'ID': 'id', 'STRUCTURE_ID': 'Structure_ID'},
                    'ATTRIBUTE_INFO': {'DETAILS': {'NULLABLE': True,
                                                   'ORDER': 2,
                                                   'PRECISION': 0,
                                                   'PRIMARY_KEY': False,
                                                   'APP_TYPE': 'VARCHAR',
                                                   'WIDTH': 200},
                                       'ID': {'NULLABLE': False,
                                              'ORDER': 3,
                                              'PRECISION': 0,
                                              'PRIMARY_KEY': True,
                                              'APP_TYPE': 'VARCHAR',
                                              'WIDTH': 10},
                                       'STRUCTURE_ID': {'NULLABLE': False,
                                                        'ORDER': 1,
                                                        'PRECISION': 0,
                                                        'PRIMARY_KEY': True,
                                                        'APP_TYPE': 'VARCHAR',
                                                        'WIDTH': 10}},
                    'ATTRIBUTE_MAP': {'DETAILS': ('atom_sites_alt', 'details', None, None),
                                      'ID': ('atom_sites_alt', 'id', None, None),
                                      'STRUCTURE_ID': (None, None, 'datablockid()', None)},
                    'INDICES': {'p1': {'ATTRIBUTES': ('STRUCTURE_ID', 'ID'), 'TYPE': 'UNIQUE'},
                                's1': {'ATTRIBUTES': ('STRUCTURE_ID',), 'TYPE': 'SEARCH'}},
                    'MAP_MERGE_INDICES': {'atom_sites_alt': {'ATTRIBUTES': ('id',), 'TYPE': 'EQUI-JOIN'}},
                    'SCHEMA_DELETE_ATTRIBUT': 'STRUCTURE_ID',
                    'SCHEMA_ID': 'ATOM_SITES_ALT',
                    'SCHEMA_NAME': 'atom_sites_alt',
                    'SCHEMA_TYPE': 'transactional'}
        }
    """

    def __init__(self, dictPath, cardinalityKeyItem=None, convertNameFunc=None):
        self.__dictPath = dictPath
        #
        self.__sdi = SchemaDefDictInfo(dictPath=dictPath, cardinalityKeyItem=cardinalityKeyItem, iTypeCodes=('ucode-alphanum-csv', 'id_list'), iQueryStrings=['comma separate'])
        #
        self.__convertName = convertNameFunc if convertNameFunc else self.__convertNameDefault
        #
        self.__re0 = re.compile('(database|cell|order|partition|group)$', flags=re.IGNORECASE)
        self.__re1 = re.compile('[-/%[]')
        self.__re2 = re.compile('[\]]')
        #
        self.__dtInfo = DataTypeInfo(filePath=None, applicationName='ANY')

    def create(self, applicationName, blockAttributeName, categoryExcludeList=None, categoryIncludeList=None):
        """
        Some validations -

        MySQL limits len(indexList) > 16)
                     Max character row record length 65535

        """
        categoryExcludeList = categoryExcludeList if categoryExcludeList else []
        categoryIncludeList = categoryIncludeList if categoryIncludeList else []
        #
        dictSchema = self.__sdi.getNameSchema()
        #
        rD = {}
        for catName, atNameList in dictSchema.items():
            if catName in categoryExcludeList:
                continue
            if categoryIncludeList and catName not in categoryIncludeList:
                continue
            cfD = self.__sdi.getCategoryFeatures(catName)
            aD = self.__sdi.getAttributeFeatures(catName)
            #
            sName = self.__convertName(catName)
            sId = sName.upper()
            d = {}
            d['SCHEMA_ID'] = sId
            d['SCHEMA_NAME'] = sName
            d['SCHEMA_TYPE'] = 'transactional'
            d['SCHEMA_UNIT_CARDINALITY'] = cfD['UNIT_CARDINALITY']
            #
            d['ATTRIBUTES'] = {self.__convertName(blockAttributeName).upper(): self.__convertName(blockAttributeName)} if blockAttributeName else {}
            d['ATTRIBUTES'].update({(self.__convertName(at)).upper(): self.__convertName(at) for at in atNameList})
            #
            d['ATTRIBUTE_MAP'] = {(self.__convertName(blockAttributeName)).upper(): {'CATEGORY': None, 'ATTRIBUTE': None,
                                                                                     'METHOD_NAME': 'datablockid()', 'ARGUMENTS': None}} if blockAttributeName else {}
            d['ATTRIBUTE_MAP'].update({(self.__convertName(at)).upper(): {'CATEGORY': catName, 'ATTRIBUTE': at, 'METHOD_NAME': None, 'ARGUMENTS': None} for at in atNameList})
            #

            d['ATTRIBUTE_INFO'] = {}
            atIdIndexList = []
            atNameIndexList = []
            iOrder = 1
            if blockAttributeName:
                td = {'ORDER': iOrder, 'NULLABLE': False, 'PRECISION': 0, 'PRIMARY_KEY': True, 'APP_TYPE': 'VARCHAR', 'WIDTH': 12, 'ITERABLE': False}
                iOrder += 1
                atId = (self.__convertName(blockAttributeName)).upper()
                atIdIndexList.append(atId)
                atNameIndexList.append(blockAttributeName)
                d['ATTRIBUTE_INFO'][atId] = td
            #
            for atName in sorted(atNameList):
                fD = aD[atName]
                if fD['IS_KEY']:
                    appType = self.__dtInfo.getAppTypeName(fD['TYPE_CODE'])
                    appPrecision = self.__dtInfo.getAppTypePrecision(fD['TYPE_CODE'])
                    appWidth = self.__dtInfo.getAppTypeWidth(fD['TYPE_CODE'])
                    td = {'ORDER': iOrder,
                          'NULLABLE': fD['IS_MANDATORY'],
                          'PRECISION': appPrecision,
                          'PRIMARY_KEY': fD['IS_KEY'],
                          'APP_TYPE': appType,
                          'WIDTH': appWidth,
                          'ITERABLE': False}
                    atId = (self.__convertName(atName)).upper()
                    d['ATTRIBUTE_INFO'][atId] = td
                    atIdIndexList.append(atId)
                    atNameIndexList.append(atName)
                    iOrder += 1
            for atName in sorted(atNameList):
                fD = aD[atName]
                if not fD['IS_KEY']:
                    appType = self.__dtInfo.getAppTypeName(fD['TYPE_CODE'])
                    appPrecision = self.__dtInfo.getAppTypePrecision(fD['TYPE_CODE'])
                    appWidth = self.__dtInfo.getAppTypeWidth(fD['TYPE_CODE'])
                    td = {'ORDER': iOrder,
                          'NULLABLE': fD['IS_MANDATORY'],
                          'PRECISION': appPrecision,
                          'PRIMARY_KEY': fD['IS_KEY'],
                          'APP_TYPE': appType,
                          'WIDTH': appWidth,
                          'ITERABLE': False}
                    atId = (self.__convertName(atName)).upper()
                    d['ATTRIBUTE_INFO'][atId] = td
                    iOrder += 1
            #
            atIdDeleteList = [self.__convertName(blockAttributeName).upper()] if blockAttributeName else atIdIndexList
            d['SCHEMA_DELETE_ATTRIBUTES'] = atIdDeleteList

            d['INDICES'] = {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": tuple(atIdIndexList)}}
            if atIdDeleteList != atIdIndexList:
                d['INDICES']["s1"] = {"TYPE": "SEARCH", "ATTRIBUTES": tuple(atIdDeleteList)}
            #
            d['MAP_MERGE_INDICES'] = {catName: {'ATTRIBUTES': tuple(atNameIndexList), 'TYPE': 'EQUI-JOIN'}}
            #
            rD[sId] = d
        #
        return rD

    def __convertNameDefault(self, name):
        """ Default schema name converter -
        """
        #self.__re0 = re.compile('(database|cell|order|partition|group)$', flags=re.IGNORECASE)
        #self.__re1 = re.compile('[-/%[]')
        #self.__re2 = re.compile('[\]]')

        if self.__re0.match(name):
            name = 'the_' + name
        return self.__re1.sub('_', self.__re2.sub('', name))

# -------------------------- ------------- ------------- ------------- ------------- ------------- -------------
