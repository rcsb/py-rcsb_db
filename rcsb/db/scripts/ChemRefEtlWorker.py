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

from jsonschema import Draft4Validator, FormatChecker

from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.chemref.ChemRefDataPrep import ChemRefDataPrep
from rcsb.utils.io.IoUtil import IoUtil

logger = logging.getLogger(__name__)


class ChemRefEtlWorker(object):
    """ Prepare and load repository holdings and repository update data.
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

    def getChemCompAccesionMapping(self, extResource, **kwargs):
        """ Get the accession code mapping between chemical component identifiers and identifier(s) for the
            input external resource.

        Args:
            resourceName (str):  resource name (e.g. DrugBank, CCDC)
            **kwargs: unused

        Returns:
            dict: {dbExtId: True, dbExtId: True, ...  }

        """
        idD = {}
        try:
            docList = []
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists("chem_comp_v5", "chem_comp_core_v5_0_2"):
                    logger.info("Document count is %d" % mg.count("chem_comp_v5", "chem_comp_core_v5_0_2"))
                    qD = {'rcsb_chem_comp_related.resource_name': extResource}
                    selectL = ['rcsb_chem_comp_related', '__comp_id']
                    tL = mg.fetch("chem_comp_v5", "chem_comp_core_v5_0_2", selectL, queryD=qD)
                    logger.info("CC mapping count %d" % len(tL))
                    docList.extend(tL)

                if mg.collectionExists("chem_comp_v5", "bird_chem_comp_core_v5_0_2"):
                    qD = {'rcsb_chem_comp_related.resource_name': extResource}
                    selectL = ['rcsb_chem_comp_related', '__prd_id']
                    tL = mg.fetch("chem_comp_v5", "bird_chem_comp_core_v5_0_2", selectL, queryD=qD)
                    logger.info("BIRD mapping count %d" % len(tL))
                    docList.extend(tL)
                #
                for doc in docList:
                    dL = doc['rcsb_chem_comp_related'] if 'rcsb_chem_comp_related' in doc else []
                    for d in dL:
                        if d['resource_name'] == extResource and 'resource_accession_code' in d:
                            idD[d['resource_accession_code']] = True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return idD

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
        """ Load chemical reference integrated data -

        Relevant configuration options:

        site_info:
            DRUGBANK_MAPPING_LOCATOR: DrugBank/drugbank_pdb_mapping.json
            DRUGBANK_DATA_LOCATOR: DrugBank/full_database.xml.gz

        drugbank_core:
            DATABASE_NAME: chem_comp_core
            DATABASE_VERSION_STRING: v5
            COLLECTION_DRUGBANK_CORE: drugbank_core
            COLLECTION_VERSION_STRING: v0_1
            SCHEMA_DEF_LOCATOR_SQL: schema/schema_def-drugbank_core-SQL.json
            SCHEMA_DEF_LOCATOR_ANY: schema/schema_def-drugbank_core-ANY.json
            INSTANCE_DATA_TYPE_INFO_LOCATOR: data_type_info/scan-drugbank_core-type-map.json

        """
        try:
            self.__statusList
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            #
            crdp = ChemRefDataPrep(self.__cfgOb)
            idD = self.getChemCompAccesionMapping(extResource)
            dList = crdp.getDocuments(extResource, idD)
            #
            logger.info("Resource %r mapped document length %d" % (extResource, len(dList)))
            logger.debug("Objects %r" % dList[:2])

            sectionName = 'drugbank_core'
            #
            dl = DocumentLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                documentLimit=self.__documentLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            #
            databaseName = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionName) + '_' + self.__cfgOb.get('DATABASE_VERSION_STRING', sectionName=sectionName)
            collectionVersion = self.__cfgOb.get('COLLECTION_VERSION_STRING', sectionName=sectionName)
            collectionName = self.__cfgOb.get('COLLECTION_DRUGBANK_CORE', sectionName=sectionName) + '_' + collectionVersion
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList,
                         indexAttributeList=['update_id'], keyNames=None)

            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def validate(self, extResource, schemaPath=None, dataPath=None):
        eCount = 0
        if extResource == "DrugBank":
            schemaName = 'drugbank_core'
            collectionNames = ['drugbank_core_v0_1']
            crdp = ChemRefDataPrep(self.__cfgOb)
            idD = self.getChemCompAccesionMapping(extResource)
            dList = crdp.getDocuments(extResource, idD)
            if dataPath:
                ioU = IoUtil()
                ioU.serialize(dataPath, dList[0], format='json', indent=3)
            eCount = self.__validate(schemaPath, schemaName, collectionNames, dList, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums")

        return eCount

    def __validate(self, schemaPath, schemaName, collectionNames, dList, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):

        eCount = 0
        for collectionName in collectionNames:
            cD = self.__buildJsonSchema(schemaPath, schemaName, collectionName, enforceOpts=enforceOpts)
            # Raises exceptions for schema compliance.
            Draft4Validator.check_schema(cD)
            #
            v = Draft4Validator(cD, format_checker=FormatChecker())
            for ii, d in enumerate(dList):
                logger.debug("Schema %s collection %s document %d" % (schemaName, collectionName, ii))
                try:
                    cCount = 0
                    for error in sorted(v.iter_errors(d), key=str):
                        logger.info("schema %s collection %s path %s error: %s" % (schemaName, collectionName, error.path, error.message))
                        logger.info(">>> failing object is %r" % d)
                        eCount += 1
                        cCount += 1
                    #
                    logger.debug("schema %s collection %s count %d" % (schemaName, collectionName, cCount))
                except Exception as e:
                    logger.exception("Validation error %s" % str(e))

        return eCount

    def __buildJsonSchema(self, schemaPath, schemaName, collectionName, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):
        try:
            smb = SchemaDefBuild(schemaName, self.__cfgOb.getConfigPath(), mockTopPath=self.__mockTopPath)
            cD = smb.build(collectionName, applicationName='json', schemaType='json', enforceOpts=enforceOpts)
            #
            logger.debug("Schema dictionary category length %d" % len(cD['properties']))
            #
            if schemaPath:
                ioU = IoUtil()
                ioU.serialize(schemaPath, cD, format='json', indent=3)
            return cD

        except Exception as e:
            logger.exception("Failing with %s" % str(e))

    def getLoadStatus(self):
        return self.__statusList
