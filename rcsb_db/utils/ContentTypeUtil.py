##
# File:    ContentTypeUtil.py
# Author:  J. Westbrook
# Date:    31-Mar-2018
# Version: 0.001
#
# Updates:
#  9-Apr-2018 jdw update to provide indices and include remaining schema
##
"""
 A collection of schema and repo path convenience methods.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import os

from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef
from rcsb_db.schema.ChemCompSchemaDef import ChemCompSchemaDef
from rcsb_db.schema.PdbxSchemaDef import PdbxSchemaDef
from rcsb_db.schema.PdbDistroSchemaDef import PdbDistroSchemaDef
from rcsb_db.schema.StatusHistorySchemaDef import StatusHistorySchemaDef
from rcsb_db.schema.DaInternalSchemaDef import DaInternalSchemaDef

from rcsb_db.utils.RepoPathUtil import RepoPathUtil


import logging
logger = logging.getLogger(__name__)


class ContentTypeUtil(object):

    def __init__(self, cfgOb=None, numProc=1, fileLimit=None, mockTopPath=None):
        self.__cfgOb = cfgOb
        self.__fileLimit = fileLimit
        self.__mockTopPath = mockTopPath
        self.__numProc = numProc

    def getPathList(self, contentType, inputPathList=None):
        outputPathList = []
        inputPathList = inputPathList if inputPathList else []
        rpU = RepoPathUtil(self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
        try:
            if contentType == "bird":
                outputPathList = inputPathList if inputPathList else rpU.getBirdPathList()
            elif contentType == "bird_family":
                outputPathList = inputPathList if inputPathList else rpU.getBirdFamilyPathList()
            elif contentType == 'chem_comp':
                outputPathList = inputPathList if inputPathList else rpU.getChemCompPathList()
            elif contentType == 'bird_chem_comp':
                outputPathList = inputPathList if inputPathList else rpU.getBirdChemCompPathList()
            elif contentType == 'pdbx':
                outputPathList = inputPathList if inputPathList else rpU.getEntryPathList()
            elif contentType in ['pdb_distro', 'da_internal', 'status_history']:
                outputPathList = inputPathList if inputPathList else []
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        if self.__fileLimit:
            outputPathList = outputPathList[:self.__fileLimit]

        return outputPathList

    def xgetSchemaInfo(self, contentType):
        sd = None
        dbName = None
        collectionNameList = []
        try:
            if contentType == "bird":
                sd = BirdSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(contentType)
            elif contentType == "bird_family":
                sd = BirdSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(contentType)
            elif contentType == 'chem_comp':
                sd = ChemCompSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(contentType)
            elif contentType == 'bird_chem_comp':
                sd = ChemCompSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(contentType)
            elif contentType == 'pdbx':
                sd = PdbxSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(contentType)
            elif contentType == 'pdb_distro':
                sd = PdbDistroSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(contentType)
            elif contentType == 'status_history':
                sd = StatusHistorySchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionNameList = sd.getContentTypeCollections(contentType)
            else:
                logger.warning("Unsupported contentType %s" % contentType)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return sd, dbName, collectionNameList


    def getSchemaInfo(self, contentType):
        sd = None
        dbName = None
        collectionNameList = []
        primaryIndexD = {}
        try:
            if contentType == "bird":
                sd = BirdSchemaDef(convertNames=True)
            elif contentType == "bird_family":
                sd = BirdSchemaDef(convertNames=True)
            elif contentType == 'chem_comp':
                sd = ChemCompSchemaDef(convertNames=True)
            elif contentType == 'bird_chem_comp':
                sd = ChemCompSchemaDef(convertNames=True)
            elif contentType == 'pdbx':
                sd = PdbxSchemaDef(convertNames=True)
            elif contentType == 'pdb_distro':
                sd = PdbDistroSchemaDef(convertNames=True)
            elif contentType == 'status_history':
                sd = StatusHistorySchemaDef(convertNames=True)
            elif contentType == 'da_internal':
                sd = DaInternalSchemaDef(convertNames=True)
            else:
                logger.warning("Unsupported contentType %s" % contentType)

            dbName = sd.getDatabaseName()
            collectionNameList = sd.getContentTypeCollections(contentType)
            primaryIndexD = {}
            for collectionName in collectionNameList:
                (tn, an) = sd.getDocumentKeyAttributeName(collectionName)
                primaryIndexD[collectionName] = tn + '.' + an

        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return sd, dbName, collectionNameList, primaryIndexD
