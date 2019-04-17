##
# File:    SchemaDefUtil.py
# Author:  J. Westbrook
# Date:    31-Mar-2018
# Version: 0.001
#
# Updates:
#   9-Apr-2018 jdw update to provide indices and include remaining schema
#  18-Jun-2018 jdw update to new schema generation protocol
#  22-Jun-2018 jdw change collection attribute specification to dot notation.
#  14-Aug-2018 jdw generalize the primaryIndex to the list of attributes returned by getDocumentKeyAttributeNames()
#  21-Aug-2018 jdw use getHelper() from the configuration class.
#   7-Sep-2018 jdw add method to return a stored JSON schema getJsonSchema()
#  11-Nov-2018 jdw add support for chem_comp_core and bird_chem_comp_core schemas
#   3-Dec-2018 jdw generalize the delivery of document indices
#   4-Jan-2019 jdw add prefix path options to api and json/bson schema paths
#                  add method getJsonSchemaLocator()
#   8-Jan-2019 jdw standardize argument names -
#   6-Feb-2019 jdw add option to merge content types to getPathList()
#
##
"""
 A collection of schema and repo path convenience methods.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os

from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.db.utils.HashableDict import HashableDict
from rcsb.db.utils.RepoPathUtil import RepoPathUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class SchemaDefUtil(object):
    """ A collection of schema and repository convenience methods.
    """

    def __init__(self, cfgOb=None, numProc=1, fileLimit=None, workPath=None, **kwargs):
        self.__cfgOb = cfgOb
        self.__fileLimit = fileLimit
        self.__numProc = numProc
        self.__workPath = workPath

    def getSchemaOptions(self, schemaLevel, extraOpts=None):
        opts = extraOpts + '|' if extraOpts else ""
        if schemaLevel == 'full':
            return opts + "mandatoryKeys|mandatoryAttributes|bounds|enums"
        elif schemaLevel in ['min', 'minimum']:
            return opts + "mandatoryKeys|enums"
        else:
            return opts

    def getLocatorObjList(self, contentType, inputPathList=None, mergeContentTypes=None):
        """Convenience method to get the data path list for the input repository content type.

        Args:
            contentType (str): Repository content type (e.g. pdbx, chem_comp, bird, ...)
            inputPathList (list, optional): path list that will be returned if provided.
            mergeContentTypes (list, optional): repository content types to combined with the
                              primary content type.

        Returns:
            Obj list: data file paths or tuple of file paths


        """
        inputPathList = inputPathList if inputPathList else []
        rpU = RepoPathUtil(self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath)
        locatorList = rpU.getLocatorList(contentType, inputPathList=inputPathList)
        #
        if mergeContentTypes and 'vrpt' in mergeContentTypes and contentType in ['pdbx', 'pdbx_core']:
            dictMapPath = self.__cfgOb.getPath('VRPT_DICT_MAPPING_LOCATOR', sectionName=self.__cfgOb.getDefaultSectionName())
            locObjL = []
            for locator in locatorList:
                if isinstance(locator, str):
                    kwD = HashableDict({})
                    oL = [HashableDict({'locator': locator, 'format': 'mmcif', 'kwargs': kwD})]
                    for mergeContentType in mergeContentTypes:
                        pth, fn = os.path.split(locator)
                        idCode = fn[:4] if fn and len(fn) >= 8 else None
                        mergeLocator = rpU.getLocator(mergeContentType, idCode) if idCode else None
                        if mergeLocator:
                            kwD = HashableDict({'dictMapPath': dictMapPath})
                            oL.append(HashableDict({'locator': mergeLocator, 'format': 'vrpt-xml-to-cif', 'kwargs': kwD}))
                    lObj = tuple(oL)
                else:
                    logger.error("Unexpected output locator type %r" % locator)
                    lObj = locator
                locObjL.append(lObj)
            #
            locatorList = locObjL

        # -
        return locatorList

    def getSchemaInfo(self, contentType, dataTyping='ANY', altDirPath=None):
        """Convenience method to return essential schema details for the input repository content type.

        Args:
            contentType (str): Repository content type (e.g. pdbx, bird, chem_comp, ...)
            dataTyping (str, optional): Application name for the target schema (e.g. ANY, SQL, ...)
            altDirPath (str, optional): alternative directory path for schema definition file

        Returns:
            tuple: SchemaDefAccess(object), target database name, target collection name list, primary index attribute list


        """
        sd = None
        dbName = None
        collectionNameList = []
        docIndexD = {}
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            schemaLocator = self.getSchemaDefLocator(contentType, dataTyping=dataTyping, altDirPath=altDirPath)
            logger.debug("ContentType %r dataTyping %r altDirPath %r schemaLocator %r " % (contentType, dataTyping, altDirPath, schemaLocator))
            schemaDef = mU.doImport(schemaLocator, format="json")
            if schemaDef:
                logger.debug("Using cached schema definition for %s application %s" % (contentType, dataTyping))
                sd = SchemaDefAccess(schemaDef)
                if sd:
                    dbName = sd.getDatabaseName()
                    collectionInfoList = sd.getCollectionInfo()
                    logger.debug("Schema %s database name %s collections %r" % (contentType, dbName, collectionInfoList))
                    for cd in collectionInfoList:
                        collectionName = cd['NAME']
                        collectionNameList.append(collectionName)
                        docIndexD[collectionName] = sd.getDocumentIndices(collectionName)

        except Exception as e:
            logger.exception("Retreiving schema %s for %s failing with %s" % (contentType, dataTyping, str(e)))

        return sd, dbName, collectionNameList, docIndexD

    def getSchemaDefLocator(self, contentType, dataTyping='ANY', altDirPath=None):
        """Return schema definition path for the input content type and application.

           Defines schema definition naming convention -

           Uses configuration details for directory path/locator details.

           site_info:
            'SCHEMA_DEF_LOCATOR_PATH': <locator prefix for schema file

           Args:
            contentType (str): Repository content type (e.g. pdbx, bird, chem_comp, ...)
            dataTyping (str, optional): Application name for the target schema (e.g. ANY, SQL, ...)
            altDirPath (str, optional): alternative directory path for schema definition file

            Returns:

             str: schema definition file locator

        """
        schemaLocator = None
        try:
            prefixName = 'SCHEMA_DEF_LOCATOR_PATH'
            pth = self.__cfgOb.getPath(prefixName, sectionName=self.__cfgOb.getDefaultSectionName())
            fn = 'schema_def-%s-%s.json' % (contentType, dataTyping.upper())
            #
            schemaLocator = os.path.join(altDirPath, fn) if altDirPath else os.path.join(pth, fn)
        except Exception as e:
            logger.exception("Retreiving schema definition path %s for %s failing with %s" % (contentType, dataTyping, str(e)))
        return schemaLocator

    def getJsonSchemaLocator(self, collectionName, schemaType='BSON', level='full', altDirPath=None):
        """Return JSON schema path for the input collection data type convention and level.

           Defines the JSON/BSON schema naming convention -

           site_info:
            'JSON_SCHEMA_LOCATOR_PATH': <locator prefix for schema file>

           Args:
            collectionName (str): Collection name in document store
            schemaType (str, optional): data type convention (BSON|JSON)
            level (str, optional): Completeness of the schema (e.g. min or full)
            altDirPath (str, optional): alternative directory path for json schema file

            Returns:

            str: schema file locator

        """
        schemaLocator = None
        try:
            prefixName = 'JSON_SCHEMA_LOCATOR_PATH'
            sdType = None
            sLevel = None
            schemaLocator = None
            if schemaType.upper() in ['JSON', 'BSON']:
                sdType = schemaType.lower()
            if level.lower() in ['min', 'minimun']:
                sLevel = 'min'
            elif level.lower() in ['full']:
                sLevel = level.lower()
            #
            if sdType and sLevel:
                pth = self.__cfgOb.getPath(prefixName, sectionName=self.__cfgOb.getDefaultSectionName())
                fn = "%s-schema-%s-%s.json" % (sdType, sLevel, collectionName)
                schemaLocator = os.path.join(altDirPath, fn) if altDirPath else os.path.join(pth, fn)
            else:
                logger.error("Unsupported schema options:  %s level %r type %r" % (collectionName, level, schemaType))
                schemaLocator = None
        except Exception as e:
            logger.debug("Retreiving JSON schema definition for %s type %s failing with %s" % (collectionName, schemaType, str(e)))
        #
        return schemaLocator

    def getJsonSchema(self, collectionName, schemaType='BSON', level='full', altDirPath=None):
        """Return JSON schema (w/ BSON types) object for the input collection and level.and

           Currenting using configuration information -



        Args:
            collectionName (str): Collection name in document store
            schemaType (str, optional): data type convention (BSON|JSON)
            level (str, optional): Completeness of the schema (e.g. min or full)
            altDirPath (str, optional): alternative directory path for json schema file

        Returns:
            dict: Schema object

        """
        sObj = None
        schemaLocator = self.getJsonSchemaLocator(collectionName, schemaType=schemaType, level=level, altDirPath=altDirPath)
        mU = MarshalUtil(workPath=self.__workPath)
        if schemaLocator and mU.exists(schemaLocator):
            mU = MarshalUtil(workPath=self.__workPath)
            sObj = mU.doImport(schemaLocator, format="json")
        else:
            logger.debug("Failed to read schema for %s %r" % (collectionName, level))
        return sObj

    def makeSchema(self, schemaName, collectionName, schemaType='BSON', level='full', saveSchema=False, altDirPath=None, extraOpts=None):
        try:
            smb = SchemaDefBuild(schemaName, self.__cfgOb)
            #
            cD = None
            stU = schemaType.upper()
            cD = smb.build(collectionName, dataTyping=stU, schemaType=stU, enforceOpts=self.getSchemaOptions(level, extraOpts=extraOpts))
            if cD and saveSchema:
                schemaLocator = self.getJsonSchemaLocator(collectionName, schemaType=schemaType, level=level, altDirPath=altDirPath)
                mU = MarshalUtil(workPath=self.__workPath)
                mU.doExport(schemaLocator, cD, format="json", indent=3)
        except Exception as e:
            logger.exception("Building schema %s collection %s failing with %s" % (schemaName, collectionName, str(e)))
        return cD

    def makeSchemaDef(self, schemaName, dataTyping='ANY', saveSchema=False, altDirPath=None):
        schemaDef = None
        try:
            smb = SchemaDefBuild(schemaName, self.__cfgOb)
            schemaDef = smb.build(dataTyping=dataTyping, schemaType='rcsb')
            if schemaDef and saveSchema:
                schemaLocator = self.getSchemaDefLocator(schemaName, dataTyping=dataTyping, altDirPath=altDirPath)
                mU = MarshalUtil(workPath=self.__workPath)
                mU.doExport(schemaLocator, schemaDef, format="json", indent=3)
        except Exception as e:
            logger.exception("Building schema %s failing with %s" % (schemaName, str(e)))
        return schemaDef
