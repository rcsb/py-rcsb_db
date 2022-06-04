##
# File: SequenceClustersEtlWorker.py
# Date: 2-Jul-2018  jdw
#
# ETL utilities for sequence cluster data
#
# Updates:
#  15-Jul-2018 jdw split out to separate module and add status tracking
#  28-Oct-2018 jdw adjustments for new configuration organization
#   4-Jan-2019 jdw differentiate site and application config sections for provenance.
#   1-Jun-2022 dwp Add clusterFileNameTemplate input argument
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.ClusterDataPrep import ClusterDataPrep
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.utils.ProvenanceProvider import ProvenanceProvider

logger = logging.getLogger(__name__)


class SequenceClustersEtlWorker(object):
    """Prepare and load sequence cluster data.

     Note: relevant configuration options -

    site_info:
     'RCSB_SEQUENCE_CLUSTER_DATA_PATH': ...

    entity_sequence_clusters:
     DATABASE_NAME: sequence_clusters
     DATABASE_VERSION_STRING: v5
     COLLECTION_ENTITY_MEMBERS: entity_members
     COLLECTION_ENTITY_MEMBERS_INDEX: data_set_id,entry_id,entity_id
     COLLECTION_CLUSTER_MEMBERS: cluster_members
     COLLECTION_CLUSTER_MEMBERS_INDEX: data_set_id,identity,cluster_id
     COLLECTION_VERSION_STRING: v0_1
     ENTITY_SCHEMA_NAME: rcsb_entity_sequence_cluster_entity_list
     CLUSTER_SCHEMA_NAME: rcsb_entity_sequence_cluster_identifer_list
     SEQUENCE_IDENTITY_LEVELS: 100,95,90,70,50,30
     COLLECTION_CLUSTER_PROVENANCE: cluster_provenance
     PROVENANCE_KEY_NAME: rcsb_entity_sequence_cluster_prov
     PROVENANCE_INFO_LOCATOR: provenance/rcsb_extend_provenance_info.json


    """

    def __init__(self, cfgOb, workPath=None, numProc=2, chunkSize=10, readBackCheck=False, documentLimit=None, verbose=False, clusterFileNameTemplate=None):
        self.__cfgOb = cfgOb
        self.__cachePath = workPath
        self.__readBackCheck = readBackCheck
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        self.__documentLimit = documentLimit
        self.__resourceName = "MONGO_DB"
        self.__verbose = verbose
        self.__clusterFileNameTemplate = clusterFileNameTemplate
        #
        self.__sectionCluster = "entity_sequence_clusters_configuration"
        self.__clusterDataPath = self.__cfgOb.getPath("RCSB_SEQUENCE_CLUSTER_DATA_PATH", sectionName=self.__cfgOb.getDefaultSectionName())
        self.__databaseName = self.__cfgOb.get("DATABASE_NAME", sectionName=self.__sectionCluster, default="sequence_clusters")
        self.__databaseVersion = self.__cfgOb.get("DATABASE_VERSION_STRING", sectionName=self.__sectionCluster, default="v5")
        #
        self.__entityMemberCollection = self.__cfgOb.get("COLLECTION_ENTITY_MEMBERS", sectionName=self.__sectionCluster, default="entity_members")
        self.__clusterMembersCollection = self.__cfgOb.get("COLLECTION_CLUSTER_MEMBERS", sectionName=self.__sectionCluster, default="cluster_members")
        self.__clusterProvenanceCollection = self.__cfgOb.get("COLLECTION_CLUSTER_PROVENANCE", sectionName=self.__sectionCluster, default="cluster_provenance")
        # self.__collectionVersion = self.__cfgOb.get("COLLECTION_VERSION_STRING", sectionName=self.__sectionCluster, default="v0_1")
        #
        self.__entitySchemaName = self.__cfgOb.get("ENTITY_SCHEMA_NAME", sectionName=self.__sectionCluster, default="rcsb_entity_sequence_cluster_entity_list")
        self.__clusterSchemaName = self.__cfgOb.get("CLUSTER_SCHEMA_NAME", sectionName=self.__sectionCluster, default="rcsb_entity_sequence_cluster_identifer_list")
        #
        tS = self.__cfgOb.get("COLLECTION_ENTITY_MEMBERS_INDEX", sectionName=self.__sectionCluster, default=None)
        self.__entityMemberCollectionIndexL = tS.split(",") if tS else None
        tS = self.__cfgOb.get("COLLECTION_CLUSTER_MEMBERS_INDEX", sectionName=self.__sectionCluster, default=None)
        self.__clusterMembersCollectionIndexL = tS.split(",") if tS else None
        # sample data set
        #
        tS = self.__cfgOb.get("SEQUENCE_IDENTITY_LEVELS", sectionName=self.__sectionCluster, default=None)
        self.__identityLevels = tS.split(",") if tS else ["100", "95", "90", "70", "50", "30"]
        #
        self.__statusList = []
        #

    def __updateStatus(self, updateId, databaseName, collectionName, status, startTimestamp):
        try:
            sFlag = "Y" if status else "N"
            desp = DataExchangeStatus()
            desp.setStartTime(tS=startTimestamp)
            desp.setObject(databaseName, collectionName)
            desp.setStatus(updateId=updateId, successFlag=sFlag)
            desp.setEndTime()
            self.__statusList.append(desp.getStatus())
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __extract(self, dataSetId, dataLocator, levels):
        """Extract sequence cluster data set  (mmseq2 or blastclust organization)"""
        try:
            cdp = ClusterDataPrep(
                workPath=self.__cachePath,
                entitySchemaName=self.__entitySchemaName,
                clusterSchemaName=self.__clusterSchemaName,
                clusterFileNameTemplate=self.__clusterFileNameTemplate,
            )
            _, docBySequenceD, docByClusterD = cdp.extract(dataSetId, clusterSetLocator=dataLocator, levels=levels, clusterType="entity")
            return docBySequenceD, docByClusterD
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return {}, {}

    def __fetchProvenance(self):
        """Fetching a provenance dictionary content."""
        try:
            provKeyName = self.__cfgOb.get("PROVENANCE_KEY_NAME", sectionName=self.__sectionCluster, default="rcsb_entity_sequence_cluster_prov")
            provU = ProvenanceProvider(self.__cfgOb, self.__cachePath, useCache=True)
            pD = provU.fetch()
            return pD[provKeyName] if provKeyName in pD else {}
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return {}

    def etl(self, dataSetId, dataLocator=None, loadType="full"):
        """Prepare and load sequence cluster data by entity and by cluster identifer."""
        try:
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            #
            docBySequenceD, docByClusterD = self.__extract(dataSetId=dataSetId, dataLocator=dataLocator, levels=self.__identityLevels)
            #
            dl = DocumentLoader(
                self.__cfgOb,
                self.__cachePath,
                self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                documentLimit=self.__documentLimit,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
            )
            #
            databaseName = self.__databaseName
            # addValues = {"_schema_version": self.__collectionVersion}
            addValues = None
            #
            collectionName = self.__entityMemberCollection
            dList = docBySequenceD[self.__entitySchemaName]
            ok1 = dl.load(
                databaseName, collectionName, loadType=loadType, documentList=dList, indexAttributeList=self.__entityMemberCollectionIndexL, keyNames=None, addValues=addValues
            )
            self.__updateStatus(dataSetId, databaseName, collectionName, ok1, statusStartTimestamp)
            #
            collectionName = self.__clusterMembersCollection
            dList = docByClusterD[self.__clusterSchemaName]
            ok2 = dl.load(
                databaseName, collectionName, loadType=loadType, documentList=dList, indexAttributeList=self.__clusterMembersCollectionIndexL, keyNames=None, addValues=addValues
            )
            self.__updateStatus(dataSetId, databaseName, collectionName, ok2, statusStartTimestamp)
            #
            pD = self.__fetchProvenance()
            collectionName = self.__clusterProvenanceCollection
            ok3 = dl.load(databaseName, collectionName, loadType=loadType, documentList=[pD], indexAttributeList=None, keyNames=None, addValues=addValues)
            self.__updateStatus(dataSetId, databaseName, collectionName, ok3, statusStartTimestamp)
            #
            return ok1 and ok2 and ok3
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def getLoadStatus(self):
        return self.__statusList
