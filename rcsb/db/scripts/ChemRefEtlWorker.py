##
# File: ChemRefEtlWorker.py
# Date: 2-Jul-2018  jdw
#
# ETL utilities for processing chemical reference data and related data integration.
#
# Updates:
#  9-Dec-2018  jdw add validation methods
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.mongo.ChemRefExtractor import ChemRefExtractor
from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.chemref.ChemRefDataPrep import ChemRefDataPrep

logger = logging.getLogger(__name__)


class ChemRefEtlWorker(object):
    """ Prepare and load chemical reference data collections.
    """

    def __init__(self, cfgOb, workPath=None, numProc=2, chunkSize=10, readBackCheck=False, documentLimit=None, mockTopPath=None, verbose=False):
        self.__cfgOb = cfgOb
        self.__workPath = workPath
        self.__readBackCheck = readBackCheck
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        self.__documentLimit = documentLimit
        self.__mockTopPath = mockTopPath
        #
        self.__resourceName = "MONGO_DB"
        self.__filterType = "assign-dates"
        self.__verbose = verbose
        self.__statusList = []
        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__documentLimit, workPath=self.__workPath)
        #

    def __updateStatus(self, updateId, databaseName, collectionName, status, startTimestamp):
        try:
            sFlag = 'Y' if status else 'N'
            desp = DataExchangeStatus()
            desp.setStartTime(tS=startTimestamp)
            desp.setObject(databaseName, collectionName)
            desp.setStatus(updateId=updateId, successFlag=sFlag)
            desp.setEndTime()
            self.__statusList.append(desp.getStatus())
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def load(self, updateId, extResource, loadType='full'):
        """ Load chemical reference integrated data for the input external resource-

        Relevant configuration options:

        site_info:
            DRUGBANK_MAPPING_LOCATOR: DrugBank/drugbank_pdb_mapping.json
            DRUGBANK_DATA_LOCATOR: DrugBank/full_database.xml.gz

        """
        try:
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            #
            crdp = ChemRefDataPrep(self.__cfgOb)
            crExt = ChemRefExtractor(self.__cfgOb)

            idD = crExt.getChemCompAccesionMapping(extResource)
            dList = crdp.getDocuments(extResource, idD)
            #
            logger.info("Resource %r mapped document length %d" % (extResource, len(dList)))
            logger.debug("Objects %r" % dList[:2])

            schemaName = 'drugbank_core'
            sD, databaseName, collectionList, _ = self.__schU.getSchemaInfo(schemaName)
            collectionName = collectionList[0] if len(collectionList) > 0 else 'unassigned'
            indexL = sD.getDocumentIndex(collectionName, 'primary')
            logger.info("Database %r collection %r index attributes %r " % (databaseName, collectionName, indexL))
            #
            collectionVersion = sD.getCollectionVersion(collectionName)
            addValues = {'_schema_version': collectionVersion}
            #
            dl = DocumentLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                documentLimit=self.__documentLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            # JDW --
            # databaseName = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionName) + '_' + self.__cfgOb.get('DATABASE_VERSION_STRING', sectionName=sectionName)
            # collectionVersion = self.__cfgOb.get('COLLECTION_VERSION_STRING', sectionName=sectionName)
            # collectionName = self.__cfgOb.get('COLLECTION_DRUGBANK_CORE', sectionName=sectionName) + '_' + collectionVersion
            #
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList,
                         indexAttributeList=indexL, keyNames=None, addValues=addValues)

            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def getLoadStatus(self):
        return self.__statusList
