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

    def __init__(self, cfgOb=None, numProc=1, fileLimit=None, workPath=None, **kwargs):
        self.__cfgOb = cfgOb
        self.__fileLimit = fileLimit
        self.__numProc = numProc
        self.__workPath = workPath

    def getPathList(self, schemaName, inputPathList=None):
        outputPathList = []
        inputPathList = inputPathList if inputPathList else []
        rpU = RepoPathUtil(self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit)
        try:
            if schemaName == "bird":
                outputPathList = inputPathList if inputPathList else rpU.getBirdPathList()
            elif schemaName == "bird_family":
                outputPathList = inputPathList if inputPathList else rpU.getBirdFamilyPathList()
            elif schemaName == 'chem_comp':
                outputPathList = inputPathList if inputPathList else rpU.getChemCompPathList()
            elif schemaName == 'bird_chem_comp':
                outputPathList = inputPathList if inputPathList else rpU.getBirdChemCompPathList()
            elif schemaName in ['pdbx', 'pdbx_core']:
                outputPathList = inputPathList if inputPathList else rpU.getEntryPathList()
            elif schemaName in ['pdb_distro', 'da_internal', 'status_history']:
                outputPathList = inputPathList if inputPathList else []
            else:
                logger.warning("Unsupported schemaName %s" % schemaName)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        if self.__fileLimit:
            outputPathList = outputPathList[:self.__fileLimit]

        return outputPathList

    def getSchemaInfo(self, schemaName, applicationName='ANY', saveSchema=True):
        sd = None
        dbName = None
        collectionNameList = []
        primaryIndexD = {}
        mU = MarshalUtil(workPath=self.__workPath)
        try:
            optName = 'SCHEMA_DEF_LOCATOR_%s' % applicationName.upper()
            schemaLocator = self.__cfgOb.getPath(optName, sectionName=schemaName)
            schemaDef = mU.doImport(schemaLocator, format="json")
            if schemaDef:
                logger.debug("Using cached schema definition for %s application %s" % (schemaName, applicationName))
                sd = SchemaDefAccess(schemaDef)
            else:
                schemaDef = self.__buildSchema(schemaName, applicationName)
                if schemaDef and saveSchema:
                    optName = 'SCHEMA_DEF_LOCATOR_%s' % applicationName.upper()
                    schemaLocator = self.__cfgOb.getPath(optName, sectionName=schemaName)
                    mU.doExport(schemaLocator, schemaDef, format="json")
                sd = SchemaDefAccess(schemaDef)

            if sd:
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(schemaName)
                logger.info("Schema %s database name %s collections %r" % (schemaName, dbName, collectionNameList))
                primaryIndexD = {}
                for collectionName in collectionNameList:
                    primaryIndexD[collectionName] = sd.getDocumentKeyAttributeNames(collectionName)

        except Exception as e:
            logger.exception("Retreiving schema %s for %s failing with %s" % (schemaName, applicationName, str(e)))

        return sd, dbName, collectionNameList, primaryIndexD

    def __buildSchema(self, schemaName, applicationName):
        try:
            #
            logger.debug("Building schema definition for %s application %s" % (schemaName, applicationName))
            locPdbxDictionaryFile = self.__cfgOb.getPath('PDBX_DICT_LOCATOR', sectionName=schemaName)
            locRcsbDictionaryFile = self.__cfgOb.getPath('RCSB_DICT_LOCATOR', default=None, sectionName=schemaName)
            dictLocators = [locPdbxDictionaryFile, locRcsbDictionaryFile] if locRcsbDictionaryFile else [locPdbxDictionaryFile]
            #
            instDataTypeFilePath = self.__cfgOb.getPath('INSTANCE_DATA_TYPE_INFO_LOCATOR', sectionName=schemaName)
            appDataTypeFilePath = self.__cfgOb.getPath('APP_DATA_TYPE_INFO_LOCATOR', sectionName=schemaName)
            #
            dictInfoHelper = self.__cfgOb.getHelper('DICT_HELPER_MODULE', sectionName=schemaName)
            defHelper = self.__cfgOb.getHelper('SCHEMADEF_HELPER_MODULE', sectionName=schemaName)
            docHelper = self.__cfgOb.getHelper('DOCUMENT_HELPER_MODULE', sectionName=schemaName)
            smb = SchemaDefBuild(schemaName,
                                 dictLocators=dictLocators,
                                 instDataTypeFilePath=instDataTypeFilePath,
                                 appDataTypeFilePath=appDataTypeFilePath,
                                 dictHelper=dictInfoHelper,
                                 schemaDefHelper=defHelper,
                                 documentDefHelper=docHelper,
                                 applicationName=applicationName)
            #
            sD = smb.build()
            logger.info("Schema %s dictionary category length %d" % (schemaName, len(sD['SCHEMA_DICT'])))
            return sD

        except Exception as e:
            logger.exception("Building schema %s failing with %s" % (schemaName, str(e)))
            self.fail()
        return {}
