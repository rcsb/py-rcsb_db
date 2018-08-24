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
##
"""
Integrate dictionary metadata and file based(type/coverage) into schema defintions.
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

import copy
import logging

from rcsb.db.define.DataTypeApplicationInfo import DataTypeApplicationInfo
from rcsb.db.define.DataTypeInstanceInfo import DataTypeInstanceInfo
#
from rcsb.db.define.DictInfo import DictInfo

logger = logging.getLogger(__name__)


class SchemaDefBuild(object):
    """ Build schema definitions from dictionary and auxilary content.

    """

    def __init__(self, schemaName, dictLocators, instDataTypeFilePath, appDataTypeFilePath,
                 dictHelper=None, schemaDefHelper=None, documentDefHelper=None, applicationName='ANY',
                 includeContentClasses=['DYNAMIC_CONTENT']):
        """
        """
        #
        self.__schemaName = schemaName
        self.__dictLocators = dictLocators
        self.__instDataTypeFilePath = instDataTypeFilePath
        self.__appDataTypeFilePath = appDataTypeFilePath
        self.__schemaDefHelper = schemaDefHelper
        self.__documentDefHelper = documentDefHelper
        self.__applicationName = applicationName
        self.__includeContentClasses = includeContentClasses

        self.__dictInfo = DictInfo(dictLocators=dictLocators, dictHelper=dictHelper, dictSubset=schemaName)
        #

    def build(self, collectionName=None, schemaType='rcsb'):
        if schemaType.lower() == 'rcsb':
            rD = self.__build(schemaName=self.__schemaName,
                              applicationName=self.__applicationName,
                              instDataTypeFilePath=self.__instDataTypeFilePath,
                              appDataTypeFilePath=self.__appDataTypeFilePath,
                              schemaDefHelper=self.__schemaDefHelper,
                              documentDefHelper=self.__documentDefHelper,
                              includeContentClasses=self.__includeContentClasses
                              )
        elif schemaType.lower() == 'json':
            rD = self.__createJsonSchema(schemaName=self.__schemaName,
                                         collectionName=collectionName,
                                         instDataTypeFilePath=self.__instDataTypeFilePath,
                                         appDataTypeFilePath=self.__appDataTypeFilePath,
                                         schemaDefHelper=self.__schemaDefHelper,
                                         documentDefHelper=self.__documentDefHelper,
                                         includeContentClasses=self.__includeContentClasses
                                         )

        return rD

    def __build(self, schemaName, applicationName, instDataTypeFilePath, appDataTypeFilePath,
                schemaDefHelper, documentDefHelper, includeContentClasses):
        """
        """
        databaseName = self.__schemaDefHelper.getDatabaseName(schemaName) if self.__schemaDefHelper else ''
        databaseVersion = self.__schemaDefHelper.getDatabaseVersion(schemaName) if self.__schemaDefHelper else ''
        #
        schemaDef = {'NAME': schemaName, 'APP_NAME': applicationName, 'DATABASE_NAME': databaseName, 'DATABASE_VERSION': databaseVersion}
        #
        schemaDef['SELECTION_FILTERS'] = self.__dictInfo.getSelectionFiltersForSubset()

        schemaDef['SCHEMA_DICT'] = self.__createSchemaDict(schemaName, applicationName, instDataTypeFilePath,
                                                           appDataTypeFilePath, schemaDefHelper, includeContentClasses)
        schemaDef['DOCUMENT_DICT'] = self.__createDocumentDict(schemaName, documentDefHelper)
        schemaDef['SLICE_PARENT_ITEMS'] = self.__convertSliceParentItemNames(schemaName, applicationName)
        schemaDef['SLICE_PARENT_FILTERS'] = self.__convertSliceParentFilterNames(schemaName, applicationName)
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
        rD = {'CONTENT_TYPE_COLLECTION_MAP': {}, 'COLLECTION_DOCUMENT_ATTRIBUTE_NAMES': {}, 'COLLECTION_CONTENT': {}}
        #
        dH = documentDefHelper
        if dH:
            cL = dH.getCollections(schemaName)
            rD['CONTENT_TYPE_COLLECTION_MAP'][schemaName] = cL
            for c in cL:
                rD['COLLECTION_CONTENT'][c] = {'INCLUDE': dH.getIncluded(c), 'EXCLUDE': dH.getExcluded(c), 'SLICE_FILTER': dH.getSliceFilter(c)}
                rD['COLLECTION_DOCUMENT_ATTRIBUTE_NAMES'][c] = dH.getDocumentKeyAttributeNames(c)
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

    def __getConvertNameMethod(self, applicationName):
        # Function to perform category and attribute name conversion.
        # convertNameF = self.__schemaDefHelper.convertNameDefault if self.__schemaDefHelper else self.__convertNameDefault
        #
        try:
            if applicationName in ['ANY', 'SQL', 'DOCUMENT', 'SOLR', 'JSON']:
                nameConvention = applicationName
            else:
                nameConvention = 'DEFAULT'
            return self.__schemaDefHelper.getConvertNameMethod(nameConvention) if self.__schemaDefHelper else self.__convertNameDefault
        except Exception:
            pass

        return self.__convertNameDefault

    def __createSchemaDict(self, schemaName, applicationName, instDataTypeFilePath, appDataTypeFilePath, schemaDefHelper, includeContentClasses=None):
        """Internal method to integrate dictionary and instance metadata into a common schema description data structure.

        Args:
            schemaName (string): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            applicationName (string): ANY|SQL
            instDataTypeFilePath (string): Path to data instance type and coverage
            appDataTypeFilePath (string): Path to resource file mapping cif data types to application data types
            schemaDefHelper (class instance): Class instance providing additional schema details
            includeContentClasses (list, optional): list of additional content classes to be included (e.g. DYNAMIC_CONTENT)

        Returns:
            dict: definitions for each schema object


        """
        verbose = False
        contentClasses = includeContentClasses if includeContentClasses else []
        logger.debug("Including additional category classes %r" % contentClasses)
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
        #
        blockAttributeName = self.__schemaDefHelper.getBlockAttributeName(schemaName) if self.__schemaDefHelper else None
        blockAttributeCifType = self.__schemaDefHelper.getBlockAttributeCifType(schemaName) if self.__schemaDefHelper else None
        blockAttributeAppType = dtAppInfo.getAppTypeName(blockAttributeCifType)
        blockAttributeWidth = self.__schemaDefHelper.getBlockAttributeMaxWidth(schemaName) if self.__schemaDefHelper else 0
        blockAttributeMethod = self.__schemaDefHelper.getBlockAttributeMethod(schemaName) if self.__schemaDefHelper else None
        #
        convertNameF = self.__getConvertNameMethod(applicationName)
        #
        dictSchema = self.__dictInfo.getNameSchema()
        #
        rD = {}
        for catName, atNameList in dictSchema.items():
            cfD = self.__dictInfo.getCategoryFeatures(catName)
            #logger.debug("catName %s contentClasses %r cfD %r" % (catName, contentClasses, cfD))

            if not dtInstInfo.exists(catName) and not self.__testContentClasses(contentClasses, cfD['CONTENT_CLASSES']):
                logger.debug("Schema %r Skipping category %s content classes %r" % (schemaName, catName, cfD['CONTENT_CLASSES']))
                continue
            sName = convertNameF(catName)
            sId = sName.upper()
            #
            if excludeList and sId in excludeList:
                continue
            if includeList and sId not in includeList:
                continue
            #
            aD = self.__dictInfo.getAttributeFeatures(catName)
            #
            sliceNames = self.__dictInfo.getSliceNames()
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
                    if verbose and applicationName in ['SQL', 'ANY']:
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
                    mI = self.__dictInfo.getMethodImplementation(catName, atName, methodCodes=["calculate_on_load"])
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
                    if verbose and applicationName in ['SQL', 'ANY']:
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
                    mI = self.__dictInfo.getMethodImplementation(catName, atName, methodCodes=["calculate_on_load"])
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
            # JDW -  Need to review attribute names here -
            d['MAP_MERGE_INDICES'] = {catName: {'ATTRIBUTES': tuple(atNameIndexList), 'TYPE': 'EQUI-JOIN'}}
            # ----
            tD = {}
            logger.debug("Slice names %r" % sliceNames)
            for sliceName in sliceNames:
                sL = self.__dictInfo.getSliceAttributes(sliceName, catName)
                logger.debug("Slice attributes %r" % sL)
                if sL:
                    # Convert names to IDs --
                    tL = []
                    for s in sL:
                        pD = {'PARENT_CATEGORY': convertNameF(s['PARENT_CATEGORY_NAME']).upper(),
                              'PARENT_ATTRIBUTE': convertNameF(s['PARENT_ATTRIBUTE_NAME']).upper(),
                              'CHILD_ATTRIBUTE': convertNameF(s['CHILD_ATTRIBUTE_NAME']).upper()}
                        tL.append(pD)
                    tD[sliceName] = tL
            d['SLICE_ATTRIBUTES'] = tD
            #
            # ---- slice cardinality
            #
            d['SLICE_UNIT_CARDINALITY'] = {}
            sliceCardD = self.__dictInfo.getSliceUnitCardinalityForSubset()
            logger.debug("Slice card dict %r" % sliceCardD.items())
            for sliceName, catL in sliceCardD.items():
                if catName in catL:
                    d['SLICE_UNIT_CARDINALITY'][sliceName] = True
                else:
                    d['SLICE_UNIT_CARDINALITY'][sliceName] = False
            #
            d['SLICE_CATEGORY_EXTRAS'] = {}
            sliceCatD = self.__dictInfo.getSliceCategoryExtrasForSubset()
            logger.debug("Slice category extra dict %r" % sliceCatD.items())
            for sliceName, catL in sliceCatD.items():
                if catName in catL:
                    d['SLICE_CATEGORY_EXTRAS'][sliceName] = True
                else:
                    d['SLICE_CATEGORY_EXTRAS'][sliceName] = False
            #
            rD[sId] = d
        #
        return rD

    def __convertSliceParentItemNames(self, schemaName, applicationName):
        sliceD = {}
        try:
            convertNameF = self.__getConvertNameMethod(applicationName)
            # [{'CATEGORY_NAME': 'entity', 'ATTRIBUTE_NAME': 'id'}
            spD = self.__dictInfo.getSliceParentItemsForSubset()
            for ky in spD:
                rL = []
                for aL in spD[ky]:
                    d = {'CATEGORY': convertNameF(aL['CATEGORY_NAME']).upper(), 'ATTRIBUTE': convertNameF(aL['ATTRIBUTE_NAME']).upper()}
                    rL.append(d)
                sliceD[ky] = rL
            #
            return sliceD
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return sliceD

    def __convertSliceParentFilterNames(self, schemaName, applicationName):
        sliceD = {}
        try:
            convertNameF = self.__getConvertNameMethod(applicationName)
            # [{'CATEGORY_NAME': 'entity', 'ATTRIBUTE_NAME': 'id'}
            spD = self.__dictInfo.getSliceParentFiltersForSubset()
            for ky in spD:
                rL = []
                for aL in spD[ky]:
                    d = {'CATEGORY': convertNameF(aL['CATEGORY_NAME']).upper(), 'ATTRIBUTE': convertNameF(aL['ATTRIBUTE_NAME']).upper(), 'VALUES': aL['VALUES']}
                    rL.append(d)
                sliceD[ky] = rL
            #
            return sliceD
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return sliceD

    def __convertNameDefault(self, name):
        """ Default schema name converter -
        """
        return name

    # -------------------------- ------------- ------------- ------------- ------------- ------------- -------------

    def __createJsonSchema(self, schemaName, collectionName, instDataTypeFilePath, appDataTypeFilePath, schemaDefHelper, documentDefHelper, includeContentClasses=None):
        """Internal method to integrate dictionary and instance metadata into a common json schema description data structure.

           Working for schema style: rowwise_by_name_with_cardinality

        Args:
            schemaName (str): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            collectionName (str): Collection defined within a schema/content type
            instDataTypeFilePath (str): Path to data instance type and coverage
            appDataTypeFilePath (str): Path to resource file mapping cif data types to application data types
            schemaDefHelper (class instance): Class instance providing additional schema details
            documentDefHelper (class instance): Class instance providing additional document schema details
            includeContentClasses (list, optional): list of additional content classes to be included (e.g. DYNAMIC_CONTENT)

        Returns:
            dict: representation of JSON schema -

        """
        applicationName = 'JSON'
        convertNameF = self.__getConvertNameMethod(applicationName)
        #
        addBlockAttribute = False
        contentClasses = includeContentClasses if includeContentClasses else []
        logger.debug("Including additional category classes %r" % contentClasses)
        #
        dtInstInfo = DataTypeInstanceInfo(instDataTypeFilePath)
        dtAppInfo = DataTypeApplicationInfo(appDataTypeFilePath, applicationName=applicationName)
        #
        #      Supplied by the schemaDefHelper for the content type (SchemaIds)
        #
        includeList = self.__schemaDefHelper.getIncluded(schemaName) if self.__schemaDefHelper else []
        excludeList = self.__schemaDefHelper.getExcluded(schemaName) if self.__schemaDefHelper else []
        #
        #      Supplied by the documentDefHelp for the collection (SchemaIds)
        #
        docIncludeList = documentDefHelper.getIncluded(collectionName)
        docExcludeList = documentDefHelper.getExcluded(collectionName)

        sliceFilter = documentDefHelper.getSliceFilter(collectionName)
        sliceCategories = self.__dictInfo.getSliceCategories(sliceFilter) if sliceFilter else []
        sliceCategoryExtrasD = self.__dictInfo.getSliceCategoryExtrasForSubset() if sliceFilter else {}
        if sliceFilter in sliceCategoryExtrasD:
            sliceCategories.extend(sliceCategoryExtrasD[sliceFilter])
        sliceCardD = self.__dictInfo.getSliceUnitCardinalityForSubset() if sliceFilter else {}
        #
        if addBlockAttribute:
            # Optional synthetic attribute added to each category with value linked to data block identifier (or other function)
            blockAttributeName = self.__schemaDefHelper.getBlockAttributeName(schemaName) if self.__schemaDefHelper else None
            blockAttributeCifType = self.__schemaDefHelper.getBlockAttributeCifType(schemaName) if self.__schemaDefHelper else None
            blockAttributeAppType = dtAppInfo.getAppTypeName(blockAttributeCifType)
            blockAttributeWidth = self.__schemaDefHelper.getBlockAttributeMaxWidth(schemaName) if self.__schemaDefHelper else 0
            # blockAttributeMethod = self.__schemaDefHelper.getBlockAttributeMethod(schemaName) if self.__schemaDefHelper else None
        #

        #
        dictSchema = self.__dictInfo.getNameSchema()
        #
        schemaPropD = {}
        for catName, atNameList in dictSchema.items():
            cfD = self.__dictInfo.getCategoryFeatures(catName)
            logger.debug("catName %s contentClasses %r cfD %r" % (catName, contentClasses, cfD))

            #
            #  Skip categories that are uniformly unpopulated --
            #
            if not dtInstInfo.exists(catName) and not self.__testContentClasses(contentClasses, cfD['CONTENT_CLASSES']):
                logger.debug("Schema %r Skipping category %s content classes %r" % (schemaName, catName, cfD['CONTENT_CLASSES']))
                continue
            #
            # -> Create a schema id  for catName <-
            sName = convertNameF(catName)
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
            aD = self.__dictInfo.getAttributeFeatures(catName)
            #
            isUnitCard = True if 'UNIT_CARDINALITY' in cfD else False
            if sliceFilter and sliceFilter in sliceCardD:
                isUnitCard = catName in sliceCardD[sliceFilter]
            #
            pD = {'type': "object", 'properties': {}, 'required': []}
            #
            if isUnitCard:
                catPropD = pD
            else:
                catPropD = {'type': "array", 'items': [pD], 'minItems': 1, 'uniqueItems': True}
            #
            if addBlockAttribute:
                schemaAttributeName = convertNameF(blockAttributeName)
                atPropD = {'type': blockAttributeAppType, 'maxWidth': blockAttributeWidth}
                pD['required'].append(schemaAttributeName)
                pD['properties'][schemaAttributeName] = atPropD

            for atName in sorted(atNameList):
                fD = aD[atName]
                if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD['CONTENT_CLASSES']):
                    continue
                #
                schemaAttributeName = convertNameF(atName)
                appType = dtAppInfo.getAppTypeName(fD['TYPE_CODE'])
                # appWidth = dtAppInfo.getAppTypeDefaultWidth(fD['TYPE_CODE'])
                # instWidth = dtInstInfo.getMaxWidth(catName, atName)
                isRequired = fD['IS_KEY'] or fD['IS_MANDATORY']
                if isRequired:
                    pD['required'].append(schemaAttributeName)
                #
                if appType in ['string']:
                    # atPropD = {'type': appType, 'maxWidth': instWidth}
                    atPropD = {'type': appType}

                elif appType in ['number', 'integer']:
                    atPropD = {'type': appType}
                    if 'MIN_VALUE' in fD:
                        atPropD['minimum'] = fD['MIN_VALUE']
                    elif 'MIN_VALUE_EXCLUSIVE' in fD:
                        atPropD['exclusiveMinimum'] = fD['MIN_VALUE_EXCLUSIVE']
                    if 'MAX_VALUE' in fD:
                        atPropD['maximum'] = fD['MAX_VALUE']
                    elif 'MAX_VALUE_EXCLUSIVE' in fD:
                        atPropD['exclusiveMaximum'] = fD['MAX_VALUE_EXCLUSIVE']
                else:
                    pass
                #
                if fD['ENUMS']:
                    atPropD['enum'] = fD['ENUMS']
                try:
                    if fD['EXAMPLES']:
                        atPropD['examples'] = [str(t1).strip() for t1, t2 in fD['EXAMPLES']]
                except Exception as e:
                    logger.exception("Failing for %r with %s" % (fD['EXAMPLES'], str(e)))
                if fD['DESCRIPTION']:
                    atPropD['description'] = fD['DESCRIPTION']
                #
                delimiter = fD['ITERABLE_DELIMITER']
                if delimiter:
                    pD['properties'][schemaAttributeName] = {'type': 'array', 'items': atPropD, 'uniqueItems': True}
                else:
                    pD['properties'][schemaAttributeName] = atPropD

                #

            #
            schemaPropD[sName] = copy.deepcopy(catPropD)

        return {"$id": "https://github.com/rcsb/py-rcsb.db/tree/master/rcsb.db/data/json-schema/",
                "$schema": "http://json-schema.org/draft-07/schema#", 'title': 'Schema for content type %s collection %s'
                % (schemaName, collectionName), 'type': 'object', 'properties': schemaPropD, 'required': []}
