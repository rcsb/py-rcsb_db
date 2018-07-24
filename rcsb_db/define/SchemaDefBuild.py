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
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging

from rcsb_db.define.DataTypeApplicationInfo import DataTypeApplicationInfo
from rcsb_db.define.DataTypeInstanceInfo import DataTypeInstanceInfo
#
from rcsb_db.define.DictInfo import DictInfo

logger = logging.getLogger(__name__)


class SchemaDefBuild(object):
    """ Build schema definitions from dictionary and auxilary content.

    """

    def __init__(self, schemaName, dictLocators, instDataTypeFilePath, appDataTypeFilePath,
                 dictHelper=None, schemaDefHelper=None, documentDefHelper=None, applicationName='ANY', includeContentClasses=None):
        """
        """
        #
        self.__schemaName = schemaName
        self.__dictLocators = dictLocators
        self.__instDataTypeFilePath = instDataTypeFilePath
        self.__appDataTypeFilePath = appDataTypeFilePath
        self.__dictHelper = dictHelper
        self.__schemaDefHelper = schemaDefHelper
        self.__documentDefHelper = documentDefHelper
        self.__applicationName = applicationName
        self.__includeContentClasses = includeContentClasses if includeContentClasses else []

    def build(self):
        rD = self.__build(schemaName=self.__schemaName,
                          applicationName=self.__applicationName,
                          dictLocators=self.__dictLocators,
                          instDataTypeFilePath=self.__instDataTypeFilePath,
                          appDataTypeFilePath=self.__appDataTypeFilePath,
                          dictHelper=self.__dictHelper,
                          schemaDefHelper=self.__schemaDefHelper,
                          documentDefHelper=self.__documentDefHelper,
                          includeContentClasses=self.__includeContentClasses
                          )
        return rD

    def __build(self, schemaName, applicationName,
                dictLocators, instDataTypeFilePath, appDataTypeFilePath,
                dictHelper, schemaDefHelper, documentDefHelper, includeContentClasses):
        """
        """
        databaseName = self.__schemaDefHelper.getDatabaseName(schemaName) if self.__schemaDefHelper else ''
        databaseVersion = self.__schemaDefHelper.getDatabaseVersion(schemaName) if self.__schemaDefHelper else ''
        #
        schemaDef = {'NAME': schemaName, 'APP_NAME': applicationName, 'DATABASE_NAME': databaseName, 'DATABASE_VERSION': databaseVersion}
        #
        schemaDef['SELECTION_FILTERS'] = dictHelper.getSelectionFiltersBySubset(schemaName)
        schemaDef['SCHEMA_DICT'] = self.__createSchemaDict(schemaName, applicationName, dictLocators, instDataTypeFilePath,
                                                           appDataTypeFilePath, dictHelper, schemaDefHelper, includeContentClasses)
        schemaDef['DOCUMENT_DICT'] = self.__createDocumentDict(schemaName, documentDefHelper)
        return schemaDef

    def __createDocumentDict(self, schemaName, documentDefHelper, applicationName='ANY'):
        """Internal method to assign document-level details to the schema definition,


        Args:
            schemaName (string): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            documentDefHelper (class instance):  Class instance providing additional document-level metadata
            applicationName (string, optional): Application name ANY|SQL|...

        Returns:
            dict: dictionary of document-level metadata


        """
        rD = {'CONTENT_TYPE_COLLECTION_MAP': {}, 'COLLECTION_DOCUMENT_ATTRIBUTE_NAME': {}, 'COLLECTION_CONTENT': {}}
        #
        dH = documentDefHelper
        if dH:
            cL = dH.getCollections(schemaName)
            rD['CONTENT_TYPE_COLLECTION_MAP'][schemaName] = cL
            for c in cL:
                rD['COLLECTION_CONTENT'][c] = {'INCLUDE': dH.getIncluded(c), 'EXCLUDE': dH.getExcluded(c)}
                rD['COLLECTION_DOCUMENT_ATTRIBUTE_NAME'][c] = dH.getDocumentKeyAttributeName(c)
        #
        return rD

    def __testContentClasses(self, includeContentClasses, assignedContentClasses):
        """ Return True if any of the include content classes are assigned.

        """
        # logger.debug("includeContentClasses %r assignedContentClasses %r" % (includeContentClasses, assignedContentClasses))
        for cc in includeContentClasses:
            if cc in assignedContentClasses:
                return True
        return False

    def __createSchemaDict(self, schemaName, applicationName, dictLocators, instDataTypeFilePath, appDataTypeFilePath, dictHelper, schemaDefHelper, includeContentClasses=None):
        """Internal method to integrate dictionary and instance metadata into a common schema description data structure.

        Args:
            schemaName (string): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            applicationName (string): ANY|SQL
            dictLocators (list): Locators for contributing CIF dictionaries.
            instDataTypeFilePath (string): Path to data instance type and coverage
            appDataTypeFilePath (string): Path to resource file mapping cif data types to application data types
            dictHelper (class instance): Class instance providing additional dictionary level metadata
            schemaDefHelper (class instance): Class instance providing additional schema details
            includeContentClasses (list, optional): list of content class to be included (e.g. ADMIN_CATEGORY)

        Returns:
            dict: definitions for each schema object

        """
        contentClasses = includeContentClasses if includeContentClasses else []
        logger.debug("Including additional category classes %r" % contentClasses)
        dictInfo = DictInfo(dictLocators=dictLocators, dictHelper=dictHelper, dictSubset=schemaName)
        #
        dtInstInfo = DataTypeInstanceInfo(instDataTypeFilePath)
        dtAppInfo = DataTypeApplicationInfo(appDataTypeFilePath, applicationName=applicationName)
        #
        # Supplied by the schemaDefHelper
        #
        includeList = self.__schemaDefHelper.getIncluded(schemaName) if self.__schemaDefHelper else []
        excludeList = self.__schemaDefHelper.getExcluded(schemaName) if self.__schemaDefHelper else []
        #
        # Optional synthetic attribute added to each category with value linked to data block identifier (or other function)
        blockAttributeName = self.__schemaDefHelper.getBlockAttributeName(schemaName) if self.__schemaDefHelper else None
        blockAttributeCifType = self.__schemaDefHelper.getBlockAttributeCifType(schemaName) if self.__schemaDefHelper else None
        blockAttributeAppType = dtAppInfo.getAppTypeName(blockAttributeCifType)
        blockAttributeWidth = self.__schemaDefHelper.getBlockAttributeMaxWidth(schemaName) if self.__schemaDefHelper else 0
        blockAttributeMethod = self.__schemaDefHelper.getBlockAttributeMethod(schemaName) if self.__schemaDefHelper else None
        #
        # Function to perform category and attribute name conversion.
        # convertNameF = self.__schemaDefHelper.convertNameDefault if self.__schemaDefHelper else self.__convertNameDefault
        #
        if applicationName in ['ANY', 'SQL', 'DOCUMENT', 'SOLR']:
            nameConvention = applicationName
        else:
            nameConvention = 'DEFAULT'
        convertNameF = self.__schemaDefHelper.getConvertNameMethod(nameConvention) if self.__schemaDefHelper else self.__convertNameDefault
        #
        dictSchema = dictInfo.getNameSchema()
        #
        rD = {}
        for catName, atNameList in dictSchema.items():
            cfD = dictInfo.getCategoryFeatures(catName)
            logger.debug("catName %s contentClasses %r cfD %r" % (catName, contentClasses, cfD))

            if not dtInstInfo.exists(catName) and not self.__testContentClasses(contentClasses, cfD['CONTENT_CLASSES']):
                continue
            sName = convertNameF(catName)
            sId = sName.upper()
            #
            if excludeList and sId in excludeList:
                continue
            if includeList and sId not in includeList:
                continue
            #
            aD = dictInfo.getAttributeFeatures(catName)
            #

            d = {}
            d['SCHEMA_ID'] = sId
            d['SCHEMA_NAME'] = sName
            d['SCHEMA_TYPE'] = 'transactional'
            d['SCHEMA_UNIT_CARDINALITY'] = cfD['UNIT_CARDINALITY'] if 'UNIT_CARDINALITY' in cfD else False
            d['SCHEMA_CONTENT_CLASSES'] = cfD['CONTENT_CLASSES'] if 'CONTENT_CLASSES' in cfD else []
            #
            d['ATTRIBUTES'] = {convertNameF(blockAttributeName).upper(): convertNameF(blockAttributeName)} if blockAttributeName else {}
            d['ATTRIBUTES'].update({(convertNameF(at)).upper(): convertNameF(at) for at in atNameList})
            #
            #
            d['ATTRIBUTE_MAP'] = {(convertNameF(blockAttributeName)).upper(): {'CATEGORY': None, 'ATTRIBUTE': None,
                                                                               'METHOD_NAME': blockAttributeMethod, 'ARGUMENTS': None}} if blockAttributeName else {}

            d['ATTRIBUTE_INFO'] = {}
            atIdIndexList = []
            atNameIndexList = []
            iOrder = 1
            if blockAttributeName:
                td = {
                    'ORDER': iOrder,
                    'NULLABLE': False,
                    'PRECISION': 0,
                    'PRIMARY_KEY': True,
                    'APP_TYPE': blockAttributeAppType,
                    'WIDTH': blockAttributeWidth,
                    'ITERABLE_DELIMITER': None,
                    'FILTER_TYPES': [],
                    'IS_CHAR_TYPE': True,
                    'CONTENT_CLASSES': ['BLOCK_ATTRIBUTE']}
                iOrder += 1
                atId = (convertNameF(blockAttributeName)).upper()
                atIdIndexList.append(atId)
                atNameIndexList.append(blockAttributeName)
                d['ATTRIBUTE_INFO'][atId] = td
            #
            for atName in sorted(atNameList):
                fD = aD[atName]
                if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD['CONTENT_CLASSES']):
                    continue
                if fD['IS_KEY']:
                    appType = dtAppInfo.getAppTypeName(fD['TYPE_CODE'])
                    appWidth = dtAppInfo.getAppTypeDefaultWidth(fD['TYPE_CODE'])
                    instWidth = dtInstInfo.getMaxWidth(catName, atName)
                    #
                    revAppType, revAppWidth = dtAppInfo.updateCharType(fD['IS_KEY'], appType, instWidth, appWidth, bufferPercent=20.0)
                    if applicationName in ['SQL', 'ANY']:
                        logger.debug("catName %s atName %s cifType %s appType %s appWidth %r instWidth %r --> revAppType %r revAppWidth %r " %
                                     (catName, atName, fD['TYPE_CODE'], appType, appWidth, instWidth, revAppType, revAppWidth))
                    #
                    appPrecision = dtAppInfo.getAppTypeDefaultPrecision(fD['TYPE_CODE'])
                    td = {'ORDER': iOrder,
                          'NULLABLE': not fD['IS_MANDATORY'],
                          'PRECISION': appPrecision,
                          'PRIMARY_KEY': fD['IS_KEY'],
                          'APP_TYPE': revAppType,
                          'WIDTH': revAppWidth,
                          'ITERABLE_DELIMITER': None,
                          'FILTER_TYPES': fD['FILTER_TYPES'],
                          'IS_CHAR_TYPE': fD['IS_CHAR_TYPE'],
                          'CONTENT_CLASSES': fD['CONTENT_CLASSES']}
                    atId = (convertNameF(atName)).upper()
                    d['ATTRIBUTE_INFO'][atId] = td
                    atIdIndexList.append(atId)
                    atNameIndexList.append(atName)
                    #
                    mI = dictInfo.getMethodImplementation(catName, atName, methodCode="calculate_on_load")
                    if mI:
                        d['ATTRIBUTE_MAP'].update({(convertNameF(atName)).upper(): {'CATEGORY': None, 'ATTRIBUTE': None, 'METHOD_NAME': mI, 'ARGUMENTS': None}})
                    else:
                        d['ATTRIBUTE_MAP'].update({(convertNameF(atName)).upper(): {'CATEGORY': catName, 'ATTRIBUTE': atName, 'METHOD_NAME': None, 'ARGUMENTS': None}})
                    iOrder += 1
            for atName in sorted(atNameList):
                fD = aD[atName]
                if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD['CONTENT_CLASSES']):
                    continue

                if not fD['IS_KEY']:
                    appType = dtAppInfo.getAppTypeName(fD['TYPE_CODE'])
                    appWidth = dtAppInfo.getAppTypeDefaultWidth(fD['TYPE_CODE'])
                    instWidth = dtInstInfo.getMaxWidth(catName, atName)
                    revAppType, revAppWidth = dtAppInfo.updateCharType(fD['IS_KEY'], appType, instWidth, appWidth, bufferPercent=20.0)
                    if applicationName in ['SQL', 'ANY']:
                        logger.debug("catName %s atName %s cifType %s appType %s appWidth %r instWidth %r --> revAppType %r revAppWidth %r " %
                                     (catName, atName, fD['TYPE_CODE'], appType, appWidth, instWidth, revAppType, revAppWidth))

                    #
                    appPrecision = dtAppInfo.getAppTypeDefaultPrecision(fD['TYPE_CODE'])
                    td = {'ORDER': iOrder,
                          'NULLABLE': not fD['IS_MANDATORY'],
                          'PRECISION': appPrecision,
                          'PRIMARY_KEY': fD['IS_KEY'],
                          'APP_TYPE': revAppType,
                          'WIDTH': revAppWidth,
                          'ITERABLE_DELIMITER': fD['ITERABLE_DELIMITER'],
                          'FILTER_TYPES': fD['FILTER_TYPES'],
                          'IS_CHAR_TYPE': fD['IS_CHAR_TYPE'],
                          'CONTENT_CLASSES': fD['CONTENT_CLASSES']}
                    atId = (convertNameF(atName)).upper()
                    d['ATTRIBUTE_INFO'][atId] = td
                    mI = dictInfo.getMethodImplementation(catName, atName, methodCode="calculate_on_load")
                    if mI:
                        d['ATTRIBUTE_MAP'].update({(convertNameF(atName)).upper(): {'CATEGORY': None, 'ATTRIBUTE': None, 'METHOD_NAME': mI, 'ARGUMENTS': None}})
                    else:
                        d['ATTRIBUTE_MAP'].update({(convertNameF(atName)).upper(): {'CATEGORY': catName, 'ATTRIBUTE': atName, 'METHOD_NAME': None, 'ARGUMENTS': None}})
                    iOrder += 1
            #
            atIdDelete = convertNameF(blockAttributeName).upper() if blockAttributeName else None
            d['SCHEMA_DELETE_ATTRIBUTE'] = atIdDelete

            d['INDICES'] = {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": tuple(atIdIndexList)}}
            if len(atIdIndexList) > 1:
                d['INDICES']["s1"] = {"TYPE": "SEARCH", "ATTRIBUTES": tuple([atIdDelete])}
            #
            d['MAP_MERGE_INDICES'] = {catName: {'ATTRIBUTES': tuple(atNameIndexList), 'TYPE': 'EQUI-JOIN'}}
            #
            rD[sId] = d
        #
        return rD

    def __convertNameDefault(self, name):
        """ Default schema name converter -
        """
        return name
# -------------------------- ------------- ------------- ------------- ------------- ------------- -------------
