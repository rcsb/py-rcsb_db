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
##
"""
 A collection of schema and repo path convenience methods.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging

from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
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

    def getPathList(self, contentType, inputPathList=None):
        """Convenience method to get the data path list for the input repository content type.

        Args:
            contentType (str): Repository content type (e.g. pdbx, chem_comp, bird, ...)
            inputPathList (list, optional): path list that will be returned if provided.

        Returns:
            list: data file file path list


        """
        inputPathList = inputPathList if inputPathList else []
        rpU = RepoPathUtil(self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath)
        outputPathList = rpU.getRepoPathList(contentType, inputPathList=inputPathList)
        return outputPathList

    def getSchemaInfo(self, contentType, applicationName='ANY', useCache=True, saveSchema=True):
        """Convenience method to return essential schema details for the input repository content type.

        Args:
            contentType (str): Repository content type (e.g. pdbx, bird, chem_comp, ...)
            applicationName (str, optional): Application name for the target schema (e.g. ANY, SQL, ...)
            useCache (bool, optional): Use a cached version of the target schema if available
            saveSchema (bool, optional): If a schema is generated preserve the generated schema

        Returns:
            tuple: SchemaDefAccess(object), target database name, target collection name list, primary index attribute list


        """
        sd = None
        dbName = None
        collectionNameList = []
        docIndexD = {}
        mU = MarshalUtil(workPath=self.__workPath)
        try:
            optName = 'SCHEMA_DEF_LOCATOR_%s' % applicationName.upper()
            schemaLocator = self.__cfgOb.getPath(optName, sectionName=contentType)
            schemaDef = mU.doImport(schemaLocator, format="json")
            if schemaDef and useCache:
                logger.debug("Using cached schema definition for %s application %s" % (contentType, applicationName))
                sd = SchemaDefAccess(schemaDef)
            else:
                schemaDef = self.__buildSchema(contentType, applicationName)
                if schemaDef and saveSchema:
                    optName = 'SCHEMA_DEF_LOCATOR_%s' % applicationName.upper()
                    schemaLocator = self.__cfgOb.getPath(optName, sectionName=contentType)
                    mU.doExport(schemaLocator, schemaDef, format="json")
                sd = SchemaDefAccess(schemaDef)

            if sd:
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(contentType)
                logger.debug("Schema %s database name %s collections %r" % (contentType, dbName, collectionNameList))
                for collectionName in collectionNameList:
                    docIndexD[collectionName] = sd.getDocumentIndices(collectionName)

        except Exception as e:
            logger.exception("Retreiving schema %s for %s failing with %s" % (contentType, applicationName, str(e)))

        return sd, dbName, collectionNameList, docIndexD

    def getJsonSchema(self, collectionName, level='full'):
        """Return JSON schema (w/ BSON types) for the input collection and level.and

           Currenting using configuration information -

            pdbx_core_entity_v5_0_2]
                SCHEMA_NAME=pdbx_core
                JSON_SCHEMA_FULL_LOCATOR=...
                JSON_SCHEMA_MIN_LOCATOR=...

        Args:
            collectionName (str): Collection name in document store
            level (str, optional): Completeness of the schema (e.g. min or full)

        Returns:
            dict: Schema object

        """
        sObj = None
        if level.lower() == 'full':
            schemaLocator = self.__cfgOb.getPath('BSON_SCHEMA_FULL_LOCATOR', sectionName=collectionName)
        elif level.lower() in ['min', 'minimum']:
            schemaLocator = self.__cfgOb.getPath('BSON_SCHEMA_MIN_LOCATOR', sectionName=collectionName)
        else:
            logger.error("Unsupported schema level %s %r" % (collectionName, level))
            schemaLocator = None

        if schemaLocator:
            #
            mU = MarshalUtil(workPath=self.__workPath)
            sObj = mU.doImport(schemaLocator, format="json")
        else:
            logger.error("Failed to read schema for %s %r" % (collectionName, level))
        return sObj

    def __buildSchema(self, contentType, applicationName):
        """Internal method to create a schema definition from dictionary and supported metadata.


        Args:
            contentType (str): Repository content type (e.g. pdbx, bird, chem_comp, ...)
            applicationName (str, optional): Application name for the target schema (e.g. ANY, SQL, ...)

        Returns:
            dict: schema definition


        """
        try:
            #
            logger.debug("Building schema definition for %s application %s" % (contentType, applicationName))
            smb = SchemaDefBuild(contentType, self.__cfgOb.getConfigPath(), mockTopPath=self.__cfgOb.getMockTopPath())
            #
            sD = smb.build(applicationName=applicationName, schemaType='rcsb')
            logger.info("Schema %s dictionary category length %d" % (contentType, len(sD['SCHEMA_DICT'])))
            return sD

        except Exception as e:
            logger.exception("Building schema %s failing with %s" % (contentType, str(e)))

        return {}
