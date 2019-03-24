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
##
"""
Integrate dictionary metadata and file based(type/coverage) into internal and JSON/BSON schema defintions.


"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import copy
import logging
import os

from rcsb.db.define.DataTypeApplicationInfo import DataTypeApplicationInfo
from rcsb.db.define.DataTypeInstanceInfo import DataTypeInstanceInfo
from rcsb.db.define.DictInfo import DictInfo

logger = logging.getLogger(__name__)


class SchemaDefBuild(object):
    """ Integrate dictionary metadata and file based(type/coverage) into internal and JSON/BSON schema defintions.

    """

    def __init__(self, schemaName, cfgOb, includeContentClasses=['GENERATED_CONTENT', 'EVOLVING_CONTENT', 'CONSOLIDATED_BIRD_CONTENT', 'INTEGRATED_CONTENT']):
        """

        """
        configName = 'site_info'
        self.__cfgOb = cfgOb
        self.__schemaName = schemaName
        self.__includeContentClasses = includeContentClasses
        #
        pathPdbxDictionaryFile = self.__cfgOb.getPath('PDBX_DICT_LOCATOR', sectionName=configName)
        pathRcsbDictionaryFile = self.__cfgOb.getPath('RCSB_DICT_LOCATOR', sectionName=configName)
        #
        pathVrptDictionaryFile = self.__cfgOb.getPath('VRPT_DICT_LOCATOR', sectionName=configName)
        #
        dictLocators = [pathPdbxDictionaryFile, pathRcsbDictionaryFile, pathVrptDictionaryFile]
        if schemaName.startswith('ihm'):
            pathIhmDictionaryFile = self.__cfgOb.getPath('IHMDEV_DICT_LOCATOR', sectionName=configName)
            pathFlrDictionaryFile = self.__cfgOb.getPath('FLR_DICT_LOCATOR', sectionName=configName)
            dictLocators.append(pathIhmDictionaryFile)
            dictLocators.append(pathFlrDictionaryFile)
        #
        self.__schemaDefHelper = self.__cfgOb.getHelper('SCHEMADEF_HELPER_MODULE', sectionName=configName, cfgOb=self.__cfgOb)
        self.__documentDefHelper = self.__cfgOb.getHelper('DOCUMENT_HELPER_MODULE', sectionName=configName, cfgOb=self.__cfgOb)
        #
        ###
        dataTypeInstanceFile = self.__schemaDefHelper.getDataTypeInstanceFile(schemaName) if self.__schemaDefHelper else '.'
        #
        pth = self.__cfgOb.getPath('INSTANCE_DATA_TYPE_INFO_LOCATOR_PATH', sectionName=configName)
        self.__instDataTypeFilePath = os.path.join(pth, dataTypeInstanceFile) if dataTypeInstanceFile and pth else None
        ##
        self.__appDataTypeFilePath = self.__cfgOb.getPath('APP_DATA_TYPE_INFO_LOCATOR', sectionName=configName)
        dictHelper = self.__cfgOb.getHelper('DICT_HELPER_MODULE', sectionName=configName, cfgOb=self.__cfgOb)
        #
        self.__dictInfo = DictInfo(dictLocators=dictLocators, dictHelper=dictHelper, dictSubset=schemaName)
        #

    def build(self, collectionName=None, dataTyping='ANY', schemaType='rcsb', enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums",
              suppressSingleton=True):
        rD = {}
        if schemaType.lower() == 'rcsb':
            rD = self.__build(schemaName=self.__schemaName,
                              dataTyping=dataTyping,
                              instDataTypeFilePath=self.__instDataTypeFilePath,
                              appDataTypeFilePath=self.__appDataTypeFilePath,
                              schemaDefHelper=self.__schemaDefHelper,
                              documentDefHelper=self.__documentDefHelper,
                              includeContentClasses=self.__includeContentClasses
                              )
        elif schemaType.lower() in ['json', 'bson']:
            rD = self.__createJsonLikeSchema(schemaName=self.__schemaName,
                                             collectionName=collectionName,
                                             dataTyping=dataTyping.upper(),
                                             instDataTypeFilePath=self.__instDataTypeFilePath,
                                             appDataTypeFilePath=self.__appDataTypeFilePath,
                                             schemaDefHelper=self.__schemaDefHelper,
                                             documentDefHelper=self.__documentDefHelper,
                                             includeContentClasses=self.__includeContentClasses,
                                             enforceOpts=enforceOpts)
        return rD

    def __build(self, schemaName, dataTyping, instDataTypeFilePath, appDataTypeFilePath,
                schemaDefHelper, documentDefHelper, includeContentClasses):
        """
        """
        databaseName = self.__schemaDefHelper.getDatabaseName(schemaName) if self.__schemaDefHelper else ''
        databaseVersion = self.__schemaDefHelper.getDatabaseVersion(schemaName) if self.__schemaDefHelper else ''

        #
        schemaDef = {'NAME': schemaName, 'APP_NAME': dataTyping, 'DATABASE_NAME': databaseName,
                     'DATABASE_VERSION': databaseVersion}
        #
        schemaDef['SELECTION_FILTERS'] = self.__dictInfo.getSelectionFiltersForSubset()

        schemaDef['SCHEMA_DICT'] = self.__createSchemaDict(schemaName, dataTyping, instDataTypeFilePath,
                                                           appDataTypeFilePath, schemaDefHelper, includeContentClasses)
        schemaDef['DOCUMENT_DICT'] = self.__createDocumentDict(schemaName, documentDefHelper)
        schemaDef['SLICE_PARENT_ITEMS'] = self.__convertSliceParentItemNames(schemaName, dataTyping)
        schemaDef['SLICE_PARENT_FILTERS'] = self.__convertSliceParentFilterNames(schemaName, dataTyping)
        return schemaDef

    def __createDocumentDict(self, schemaName, documentDefHelper, dataTyping='ANY'):
        """Internal method to assign document-level details to the schema definition,


        Args:
            schemaName (string): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            documentDefHelper (class instance):  Class instance providing additional document-level metadata
            dataTyping (string, optional): Application name ANY|SQL|...

        Returns:
            dict: dictionary of document-level metadata


        """
        rD = {'CONTENT_TYPE_COLLECTION_INFO': [],
              'COLLECTION_DOCUMENT_ATTRIBUTE_NAMES': {},
              'COLLECTION_DOCUMENT_REPLACE_ATTRIBUTE_NAMES': {},
              'COLLECTION_DOCUMENT_PRIVATE_KEYS': {},
              'COLLECTION_DOCUMENT_INDICES': {},
              'COLLECTION_CONTENT': {},
              'COLLECTION_SUB_CATEGORY_AGGREGATES': {}}
        #
        dH = documentDefHelper
        if dH:
            # cdL  = list of [{'NAME': , 'VERSION': xx }, ...]
            cdL = dH.getCollectionInfo(schemaName)
            rD['CONTENT_TYPE_COLLECTION_INFO'] = cdL
            for cd in cdL:
                c = cd['NAME']
                rD['COLLECTION_CONTENT'][c] = {'INCLUDE': dH.getIncluded(c), 'EXCLUDE': dH.getExcluded(c), 'SLICE_FILTER': dH.getSliceFilter(c)}
                rD['COLLECTION_DOCUMENT_ATTRIBUTE_NAMES'][c] = dH.getDocumentKeyAttributeNames(c)
                rD['COLLECTION_DOCUMENT_REPLACE_ATTRIBUTE_NAMES'][c] = dH.getDocumentReplaceAttributeNames(c)
                rD['COLLECTION_DOCUMENT_PRIVATE_KEYS'][c] = dH.getPrivateDocumentAttributes(c)
                rD['COLLECTION_DOCUMENT_INDICES'][c] = dH.getDocumentIndices(c)
                rD['COLLECTION_SUB_CATEGORY_AGGREGATES'][c] = dH.getSubCategoryAggregateFeatures(c)
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

    def __getConvertNameMethod(self, dataTyping):
        # Function to perform category and attribute name conversion.
        # convertNameF = self.__schemaDefHelper.convertNameDefault if self.__schemaDefHelper else self.__convertNameDefault
        #
        try:
            if dataTyping in ['ANY', 'SQL', 'DOCUMENT', 'SOLR', 'JSON', 'BSON']:
                nameConvention = dataTyping
            else:
                nameConvention = 'DEFAULT'
            return self.__schemaDefHelper.getConvertNameMethod(nameConvention) if self.__schemaDefHelper else self.__convertNameDefault
        except Exception:
            pass

        return self.__convertNameDefault

    def __createSchemaDict(self, schemaName, dataTyping, instDataTypeFilePath, appDataTypeFilePath, schemaDefHelper, includeContentClasses=None):
        """Internal method to integrate dictionary and instance metadata into a common schema description data structure.

        Args:
            schemaName (string): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            dataTyping (string): ANY|SQL
            instDataTypeFilePath (string): Path to data instance type and coverage
            appDataTypeFilePath (string): Path to resource file mapping cif data types to application data types
            schemaDefHelper (class instance): Class instance providing additional schema details
            includeContentClasses (list, optional): list of additional content classes to be included (e.g. GENERATED_CONTENT)

        Returns:
            dict: definitions for each schema object


        """
        verbose = False
        contentClasses = includeContentClasses if includeContentClasses else []
        #
        logger.debug("Including additional category classes %r" % contentClasses)
        #
        dtInstInfo = DataTypeInstanceInfo(instDataTypeFilePath)
        dtAppInfo = DataTypeApplicationInfo(appDataTypeFilePath, dataTyping=dataTyping)
        #
        # Supplied by the schemaDefHelper
        #
        includeList = self.__schemaDefHelper.getIncluded(schemaName) if self.__schemaDefHelper else []
        excludeList = self.__schemaDefHelper.getExcluded(schemaName) if self.__schemaDefHelper else []
        excludeAttributesD = self.__schemaDefHelper.getExcludedAttributes(schemaName) if self.__schemaDefHelper else {}
        #
        logger.debug("Schema include list length %d" % len(includeList))
        logger.debug("Schema exclude list length %d" % len(excludeList))
        #
        # Optional synthetic attribute added to each category with value linked to data block identifier (or other function)
        #
        blockAttributeName = self.__schemaDefHelper.getBlockAttributeName(schemaName) if self.__schemaDefHelper else None
        blockAttributeCifType = self.__schemaDefHelper.getBlockAttributeCifType(schemaName) if self.__schemaDefHelper else None
        blockAttributeAppType = dtAppInfo.getAppTypeName(blockAttributeCifType)
        blockAttributeWidth = self.__schemaDefHelper.getBlockAttributeMaxWidth(schemaName) if self.__schemaDefHelper else 0
        blockAttributeMethod = self.__schemaDefHelper.getBlockAttributeMethod(schemaName) if self.__schemaDefHelper else None
        #
        convertNameF = self.__getConvertNameMethod(dataTyping)
        #
        dictSchema = self.__dictInfo.getSchemaNames()
        logger.debug("Dictionary category length %d" % len(dictSchema))
        #
        rD = {}
        for catName, fullAtNameList in dictSchema.items():

            #
            atNameList = [at for at in fullAtNameList if (catName, at) not in excludeAttributesD]
            #
            cfD = self.__dictInfo.getCategoryFeatures(catName)
            #
            # logger.debug("catName %s contentClasses %r cfD %r" % (catName, contentClasses, cfD))

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
            # JDW
            if not cfD:
                logger.info("%s catName %s contentClasses %r cfD %r" % (schemaName, catName, contentClasses, cfD))
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
            d['SCHEMA_MANDATORY'] = cfD['IS_MANDATORY']
            d['SCHEMA_SUB_CATEGORIES'] = []
            #
            d['ATTRIBUTES'] = {convertNameF(blockAttributeName).upper(): convertNameF(blockAttributeName)} if blockAttributeName else {}
            # d['ATTRIBUTES'].update({(convertNameF(at)).upper(): convertNameF(at) for at in atNameList})
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
                    'ENUMERATION': {},
                    'IS_CHAR_TYPE': True,
                    'CONTENT_CLASSES': ['BLOCK_ATTRIBUTE'],
                    'SUB_CATEGORIES': []}
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
                    if verbose and dataTyping in ['SQL', 'ANY']:
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
                          'ENUMERATION': {str(ky).lower(): ky for ky in fD['ENUMS']},
                          'CONTENT_CLASSES': fD['CONTENT_CLASSES'],
                          'SUB_CATEGORIES': fD['SUB_CATEGORIES']}
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
            #
            for atName in sorted(atNameList):
                fD = aD[atName]
                if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD['CONTENT_CLASSES']):
                    continue
                if not fD['IS_KEY']:
                    appType = dtAppInfo.getAppTypeName(fD['TYPE_CODE'])
                    if not appType:
                        logger.error("Missing data type mapping for %s %s" % (catName, atName))
                    appWidth = dtAppInfo.getAppTypeDefaultWidth(fD['TYPE_CODE'])
                    instWidth = dtInstInfo.getMaxWidth(catName, atName)
                    revAppType, revAppWidth = dtAppInfo.updateCharType(fD['IS_KEY'], appType, instWidth, appWidth, bufferPercent=20.0)
                    if verbose and dataTyping in ['SQL', 'ANY']:
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
                          'ENUMERATION': {str(ky).lower(): ky for ky in fD['ENUMS']},
                          'CONTENT_CLASSES': fD['CONTENT_CLASSES'],
                          'SUB_CATEGORIES': fD['SUB_CATEGORIES']}
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
            scL = []
            for atId in d['ATTRIBUTE_INFO']:
                scL.extend(d['ATTRIBUTE_INFO'][atId]['SUB_CATEGORIES'])
            d['SCHEMA_SUB_CATEGORIES'] = list(set(scL))
            #
            # Make attributes dict consistent with map ...
            d['ATTRIBUTES'].update({atId: convertNameF(tD['ATTRIBUTE']) for atId, tD in d['ATTRIBUTE_MAP'].items() if atId not in d['ATTRIBUTES']})

            #
            rD[sId] = d
        #
        return rD

    def __convertSliceParentItemNames(self, schemaName, dataTyping):
        sliceD = {}
        try:
            convertNameF = self.__getConvertNameMethod(dataTyping)
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

    def __convertSliceParentFilterNames(self, schemaName, dataTyping):
        sliceD = {}
        try:
            convertNameF = self.__getConvertNameMethod(dataTyping)
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

    def __createJsonLikeSchema(self, schemaName, collectionName, dataTyping, instDataTypeFilePath, appDataTypeFilePath,
                               schemaDefHelper, documentDefHelper, includeContentClasses=None, jsonSpecDraft='4',
                               enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums", removeSubCategoryPrefix=True):
        """Internal method to integrate dictionary and instance metadata into a common json/bson schema description data structure.

           Working only for practical schema style: rowwise_by_name_with_cardinality

        Args:
            schemaName (str): A schema/content name: pdbx|chem_comp|bird|bird_family ...
            collectionName (str): Collection defined within a schema/content type
            dataTyping (str): Target data type convention for the schema (e.g. JSON, BSON, or a variant of these...)
            instDataTypeFilePath (str): Path to data instance type and coverage
            appDataTypeFilePath (str): Path to resource file mapping cif data types to application data types
            schemaDefHelper (class instance): Class instance providing additional schema details
            documentDefHelper (class instance): Class instance providing additional document schema details
            includeContentClasses (list, optional): list of additional content classes to be included (e.g. GENERATED_CONTENT)
            jsonSpecDraft (str, optional): The target draft schema specification '4|6'
            enforceOpts (str, optional): options for semantics are included in the schema (e.g. "mandatoryKeys|mandatoryAttributes|bounds|enums")

        Returns:
            dict: representation of JSON/BSON schema -


        """
        addBlockAttribute = True
        suppressSingleton = not documentDefHelper.getRetainSingletonObjects(collectionName)
        logger.debug("Collection %s suppress singleton %r" % (collectionName, suppressSingleton))
        subCategoryAggregates = documentDefHelper.getSubCategoryAggregates(collectionName)
        logger.debug("%s %s Sub_category aggregates %r" % (schemaName, collectionName, subCategoryAggregates))
        privDocKeyL = documentDefHelper.getPrivateDocumentAttributes(collectionName)
        # enforceOpts = "mandatoryKeys|mandatoryAttributes|bounds|enums"
        #
        # dataTyping = 'JSON'
        dataTypingU = dataTyping.upper()
        typeKey = 'bsonType' if dataTypingU == 'BSON' else 'type'
        convertNameF = self.__getConvertNameMethod(dataTypingU)
        #
        contentClasses = includeContentClasses if includeContentClasses else []
        logger.debug("Including additional category classes %r" % contentClasses)
        #
        dtInstInfo = DataTypeInstanceInfo(instDataTypeFilePath)
        dtAppInfo = DataTypeApplicationInfo(appDataTypeFilePath, dataTyping=dataTypingU)
        #
        #      Supplied by the schemaDefHelper for the content type (SchemaIds)
        #
        includeList = schemaDefHelper.getIncluded(schemaName) if self.__schemaDefHelper else []
        excludeList = schemaDefHelper.getExcluded(schemaName) if self.__schemaDefHelper else []
        excludeAttributesD = self.__schemaDefHelper.getExcludedAttributes(schemaName) if self.__schemaDefHelper else {}
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
            blockAttributeName = schemaDefHelper.getBlockAttributeName(schemaName) if self.__schemaDefHelper else None
            blockAttributeCifType = schemaDefHelper.getBlockAttributeCifType(schemaName) if self.__schemaDefHelper else None
            blockAttributeAppType = dtAppInfo.getAppTypeName(blockAttributeCifType)
            #blockAttributeWidth = schemaDefHelper.getBlockAttributeMaxWidth(schemaName) if schemaDefHelper else 0
            # blockAttributeMethod = schemaDefHelper.getBlockAttributeMethod(schemaName) if schemaDefHelper else None
        #
        dictSchema = self.__dictInfo.getSchemaNames()
        #
        schemaPropD = {}
        mandatoryCategoryL = []
        for catName, fullAtNameList in dictSchema.items():
            atNameList = [at for at in fullAtNameList if (catName, at) not in excludeAttributesD]
            cfD = self.__dictInfo.getCategoryFeatures(catName)
            # logger.debug("catName %s contentClasses %r cfD %r" % (catName, contentClasses, cfD))

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
            if cfD['IS_MANDATORY']:
                mandatoryCategoryL.append(catName)
            #
            isUnitCard = True if ('UNIT_CARDINALITY' in cfD and cfD['UNIT_CARDINALITY']) else False
            if sliceFilter and sliceFilter in sliceCardD:
                isUnitCard = catName in sliceCardD[sliceFilter]
            #
            pD = {typeKey: "object", 'properties': {}, 'required': [], "additionalProperties": False}
            #
            if isUnitCard:
                catPropD = pD
            else:
                if cfD['IS_MANDATORY']:
                    # catPropD = {typeKey: "array", 'items': [pD], 'minItems': 1, 'uniqueItems': True}
                    catPropD = {typeKey: "array", 'items': pD, 'minItems': 1, 'uniqueItems': True}
                else:
                    # JDW Adjusted minItems=1
                    catPropD = {typeKey: "array", 'items': pD, 'minItems': 1, 'uniqueItems': True}
            #
            if addBlockAttribute and blockAttributeName:
                schemaAttributeName = convertNameF(blockAttributeName)
                # atPropD = {typeKey: blockAttributeAppType, 'maxWidth': blockAttributeWidth}
                atPropD = {typeKey: blockAttributeAppType}
                pD['required'].append(schemaAttributeName)
                pD['properties'][schemaAttributeName] = atPropD

            #  First, filter any subcategory aggregates from the available list of a category attributes
            #

            subCatPropD = {}
            if subCategoryAggregates:
                logger.debug("%s %s %s subcategories %r" % (schemaName, collectionName, catName, cfD['SUB_CATEGORIES']))
                for subCategory in subCategoryAggregates:
                    if subCategory not in cfD['SUB_CATEGORIES']:
                        continue
                    logger.debug("%s %s %s processing subcategory %r" % (schemaName, collectionName, catName, subCategory))
                    reqL = []
                    scD = {typeKey: "object", 'properties': {}, "additionalProperties": False}
                    for atName in sorted(atNameList):
                        fD = aD[atName]
                        # Exclude primary data attributes with no instance coverage except if in a protected content class
                        # if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD['CONTENT_CLASSES']):
                        #    continue
                        if subCategory not in fD['SUB_CATEGORIES']:
                            continue
                        #
                        schemaAttributeName = convertNameF(atName)
                        if (removeSubCategoryPrefix):
                            schemaAttributeName = schemaAttributeName.replace(subCategory + '_', '')
                        #
                        isRequired = ('mandatoryAttributes' in enforceOpts and fD['IS_MANDATORY'])
                        if isRequired:
                            reqL.append(schemaAttributeName)
                        #
                        atPropD = self.__getJsonAttributeProperties(fD, dataTypingU, dtAppInfo, jsonSpecDraft, enforceOpts)
                        scD['properties'][schemaAttributeName] = atPropD
                    if reqL:
                        scD['required'] = reqL
                    subCatPropD[subCategory] = {typeKey: 'array', 'items': scD, 'uniqueItems': False}
            #
            if subCatPropD:
                logger.debug("%s %s %s processing subcategory properties %r" % (schemaName, collectionName, catName, subCatPropD.items()))
            #
            for atName in sorted(atNameList):
                fD = aD[atName]
                # Exclude primary data attributes with no instance coverage except if in a protected content class
                if not dtInstInfo.exists(catName, atName) and not self.__testContentClasses(contentClasses, fD['CONTENT_CLASSES']):
                    continue
                if subCategoryAggregates and self.__subCategoryTest(subCategoryAggregates, fD['SUB_CATEGORIES']):
                    continue
                #
                schemaAttributeName = convertNameF(atName)
                isRequired = (('mandatoryKeys' in enforceOpts and fD['IS_KEY']) or ('mandatoryAttributes' in enforceOpts and fD['IS_MANDATORY']))
                if isRequired:
                    pD['required'].append(schemaAttributeName)
                #
                atPropD = self.__getJsonAttributeProperties(fD, dataTypingU, dtAppInfo, jsonSpecDraft, enforceOpts)

                delimiter = fD['ITERABLE_DELIMITER']
                if delimiter:
                    pD['properties'][schemaAttributeName] = {typeKey: 'array', 'items': atPropD, 'uniqueItems': False}
                else:
                    pD['properties'][schemaAttributeName] = atPropD

            if subCatPropD:
                pD['properties'].update(copy.copy(subCatPropD))
            # pD['required'].extend(list(subCatPropD.keys()))
            #
            if 'required' in catPropD and len(catPropD['required']) < 1:
                logger.info("Category %s cfD %r" % (catName, cfD.items()))
            #
            schemaPropD[sName] = copy.deepcopy(catPropD)
        #
        # Add any private keys to the object schema - Fetch the metadata for the private keys
        #
        privKeyD = {}
        privMandatoryD = {}
        if privDocKeyL:
            for pdk in privDocKeyL:
                aD = self.__dictInfo.getAttributeFeatures(convertNameF(pdk['CATEGORY_NAME']))
                fD = aD[convertNameF(pdk['ATTRIBUTE_NAME'])]
                atPropD = self.__getJsonAttributeProperties(fD, dataTypingU, dtAppInfo, jsonSpecDraft, enforceOpts)
                privKeyD[pdk['PRIVATE_DOCUMENT_NAME']] = atPropD
                privMandatoryD[pdk['PRIVATE_DOCUMENT_NAME']] = pdk['MANDATORY']

        #
        # Suppress the category name for schemas with a single category -
        #
        if suppressSingleton and len(schemaPropD) == 1:
            logger.debug("%s %s suppressing category in singleton schema" % (schemaName, collectionName))
            # rD = copy.deepcopy(catPropD)
            for k, v in privKeyD.items():
                pD['properties'][k] = v
                # pD['required'] = k
                if privMandatoryD[k]:
                    pD['required'].append(k)
            rD = copy.deepcopy(pD)
            # if "additionalProperties" in rD:
            #    rD["additionalProperties"] = True
        else:
            for k, v in privKeyD.items():
                schemaPropD[k] = v
                if privMandatoryD[k]:
                    mandatoryCategoryL.append(k)
            #
            rD = {typeKey: 'object', 'properties': schemaPropD, "additionalProperties": False}
            if len(mandatoryCategoryL):
                rD['required'] = mandatoryCategoryL

        if dataTypingU == 'BSON':
            rD['properties']['_id'] = {'bsonType': 'objectId'}
            logger.debug("Adding mongo key %r" % rD['properties']['_id'])
        #
        if dataTypingU == 'JSON':
            sdType = dataTyping.lower()
            sLevel = 'full' if 'bounds' in enforceOpts else 'min'
            fn = "%s-schema-%s-%s.json" % (sdType, sLevel, collectionName)
            collectionVersion = documentDefHelper.getCollectionVersion(schemaName, collectionName)
            jsonSchemaUrl = "http://json-schema.org/draft-0%s/schema#" % jsonSpecDraft if jsonSpecDraft in ['3', '4', '6', '7'] else "http://json-schema.org/schema#"
            schemaRepo = 'https://github.com/rcsb/py-rcsb.db/tree/master/rcsb.db/data/json-schema/'
            desc1 = 'RCSB Exchange Database JSON schema derived from the %s content type schema. ' % schemaName
            desc2 = 'This schema supports collection %s version %s. ' % (collectionName, collectionVersion)
            desc3 = 'This schema is hosted in repository %s%s and follows JSON schema specification version %s' % (schemaRepo, fn, jsonSpecDraft)
            rD.update({"$id": "%s%s" % (schemaRepo, fn),
                       "$schema": jsonSchemaUrl,
                       'title': 'schema: %s collection: %s version: %s' % (schemaName, collectionName, collectionVersion),
                       'description': desc1 + desc2 + desc3,
                       '$comment': 'schema_version: %s' % collectionVersion
                       })

        return rD

    def __getJsonAttributeProperties(self, fD, dataTypingU, dtAppInfo, jsonSpecDraft, enforceOpts):
        #
        atPropD = {}
        try:
            # - assign data type attributes
            typeKey = 'bsonType' if dataTypingU == 'BSON' else 'type'
            appType = dtAppInfo.getAppTypeName(fD['TYPE_CODE'])
            #
            #
            if appType in ['string']:
                # atPropD = {typeKey: appType, 'maxWidth': instWidth}
                atPropD = {typeKey: appType}
            elif appType in ['date', 'datetime'] and dataTypingU == 'JSON':
                fmt = 'date' if appType == 'date' else 'date-time'
                atPropD = {typeKey: 'string', 'format': fmt}
            elif appType in ['date', 'datetime'] and dataTypingU == 'BSON':
                atPropD = {typeKey: 'date'}
            elif appType in ['number', 'integer', 'int', 'double']:
                atPropD = {typeKey: appType}
                #
                if 'bounds' in enforceOpts:
                    if jsonSpecDraft in ['3', '4']:
                        if 'MIN_VALUE' in fD:
                            atPropD['minimum'] = fD['MIN_VALUE']
                        elif 'MIN_VALUE_EXCLUSIVE' in fD:
                            atPropD['minimum'] = fD['MIN_VALUE_EXCLUSIVE']
                            atPropD['exclusiveMinimum'] = True
                        if 'MAX_VALUE' in fD:
                            atPropD['maximum'] = fD['MAX_VALUE']
                        elif 'MAX_VALUE_EXCLUSIVE' in fD:
                            atPropD['maximum'] = fD['MAX_VALUE_EXCLUSIVE']
                            atPropD['exclusiveMaximum'] = True
                    elif jsonSpecDraft in ['6', '7']:
                        if 'MIN_VALUE' in fD:
                            atPropD['minimum'] = fD['MIN_VALUE']
                        elif 'MIN_VALUE_EXCLUSIVE' in fD:
                            atPropD['exclusiveMinimum'] = fD['MIN_VALUE_EXCLUSIVE']
                        if 'MAX_VALUE' in fD:
                            atPropD['maximum'] = fD['MAX_VALUE']
                        elif 'MAX_VALUE_EXCLUSIVE' in fD:
                            atPropD['exclusiveMaximum'] = fD['MAX_VALUE_EXCLUSIVE']
            else:
                atPropD = {typeKey: appType}
            #
            if 'enums' in enforceOpts and fD['ENUMS']:
                atPropD['enum'] = fD['ENUMS']
            if dataTypingU not in ['BSON']:
                try:
                    if fD['EXAMPLES']:
                        atPropD['examples'] = [str(t1).strip() for t1, t2 in fD['EXAMPLES']]
                except Exception as e:
                    logger.exception("Failing for %r with %s" % (fD['EXAMPLES'], str(e)))
                if fD['DESCRIPTION']:
                    atPropD['description'] = fD['DESCRIPTION']
                #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        #
        return atPropD

    def __subCategoryTest(self, filterList, atSubCategoryList):
        """ Return true if any element of filter list in atSubCategoryList
        """
        if not filterList or not atSubCategoryList:
            return False
        for subCat in filterList:
            if subCat in atSubCategoryList:
                return True
        return False
