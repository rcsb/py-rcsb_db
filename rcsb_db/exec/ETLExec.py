##
# File: ETLExec.py
# Date: 2-Jul-2018  jdw
#
#  Execution wrapper  --  ETL utilities for derived and remote data sources -
#
#  Updates:
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
import logging
import os
import sys

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.io.MarshalUtil import MarshalUtil
from rcsb_db.loaders.ClusterDataPrep import ClusterDataPrep
from rcsb_db.mongo.DocumentLoader import DocumentLoader
from rcsb_db.utils.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


class ClusterEtlWorker(object):
    """
        Note: relevant configuration options -
        [entity_sequence_clusters]
        DATABASE_NAME=sequence_clusters
        DATABASE_VERSION_STRING=v5
        COLLECTION_ENTITY_MEMBERS=entity_members
        COLLECTION_CLUSTER_MEMBERS=cluster_members
        COLLECTION_VERSION_STRING=v0_1
        ENTITY_SCHEMA_NAME=rcsb_sequence_cluster_entity_list
        CLUSTER_SCHEMA_NAME=rcsb_sequence_cluster_identifer_list
        COLLECTION_ENTITY_MEMBERS_INDEX=data_set_id,entry_id,entity_id
        COLLECTION_CLUSTER_MEMBERS_INDEX=data_set_id,identity,cluster_id
        SEQUENCE_IDENTITY_LEVELS=100,95,90,70,50,30

        """

    def __init__(self, cfgOb, workPath=None, numProc=2, chunkSize=10, readBackCheck=False, documentLimit=None, verbose=False):
        self.__cfgOb = cfgOb
        self.__workPath = workPath
        self.__readBackCheck = readBackCheck
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        self.__documentLimit = documentLimit
        self.__resourceName = "MONGO_DB"
        self.__verbose = verbose
        #
        sectionCluster = 'entity_sequence_clusters'
        self.__databaseName = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionCluster, default='sequence_clusters')
        self.__databaseVersion = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionCluster, default='v5')
        self.__entityMemberCollection = self.__cfgOb.get('COLLECTION_ENTITY_MEMBERS', sectionName=sectionCluster, default='entity_members')
        self.__clusterMembersCollection = self.__cfgOb.get('COLLECTION_CLUSTER_MEMBERS', sectionName=sectionCluster, default='cluster_members')
        self.__collectionVersion = self.__cfgOb.get('DATABASE_NAME', sectionName=sectionCluster, default='v0_1')
#
        self.__entitySchemaName = self.__cfgOb.get('ENTITY_SCHEMA_NAME', sectionName=sectionCluster, default='rcsb_sequence_cluster_entity_list')
        self.__clusterSchemaName = self.__cfgOb.get('CLUSTER_SCHEMA_NAME', sectionName=sectionCluster, default='rcsb_sequence_cluster_identifer_list')
        #
        tS = self.__cfgOb.get('COLLECTION_ENTITY_MEMBERS_INDEX', sectionName=sectionCluster, default=None)
        self.__entityMemberCollectionIndexL = tS.split(',') if tS else None
        tS = self.__cfgOb.get('COLLECTION_CLUSTER_MEMBERS_INDEX', sectionName=sectionCluster, default=None)
        self.__clusterMembersCollectionIndexL = tS.split(',') if tS else None
        # sample data set
        #
        tS = self.__cfgOb.get('SEQUENCE_IDENTITY_LEVELS', sectionName=sectionCluster, default=None)
        self.__identityLevels = tS.split(',') if tS else ['100', '95', '90', '70', '50', '30']
        #

    def __extract(self, dataSetId, dataLocator, levels):
        """ Extract sequence cluster data set  (mmseq2 or blastclust organization)
        """
        try:
            cdp = ClusterDataPrep(workPath=self.__workPath, entitySchemaName=self.__entitySchemaName, clusterSchemaName=self.__clusterSchemaName)
            cifD, docBySequenceD, docByClusterD = cdp.extract(dataSetId, clusterSetLocator=dataLocator, levels=levels, clusterType='entity')
            return docBySequenceD, docByClusterD
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return {}, {}

    def etl(self, dataSetId, dataLocator):
        """ Load sequence cluster data in documents
        """
        try:
            docBySequenceD, docByClusterD = self.__extract(dataSetId=dataSetId, dataLocator=dataLocator, levels=self.__identityLevels)
            #
            dl = DocumentLoader(self.__cfgOb, self.__resourceName, numProc=self.__numProc, chunkSize=self.__chunkSize,
                                documentLimit=self.__documentLimit, verbose=self.__verbose, readBackCheck=self.__readBackCheck)
            #
            dbName = self.__databaseName + '_' + self.__databaseVersion
            cName = self.__entityMemberCollection + '_' + self.__collectionVersion
            dList = docBySequenceD[self.__entitySchemaName]
            ok1 = dl.load(dbName, cName, loadType='full', documentList=dList, indexAttributeList=self.__entityMemberCollectionIndexL, keyName=None)

            cName = self.__entityMemberCollection + '_' + self.__collectionVersion
            dList = docByClusterD[self.__clusterSchemaName]
            ok2 = dl.load(dbName, cName, loadType='full', documentList=dList, indexAttributeList=self.__clusterMembersCollectionIndexL, keyName=None)
            #
            return ok1 and ok2
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False


def main():
    parser = argparse.ArgumentParser()
    #
    #
    parser.add_argument("--full", default=True, action='store_true', help="Fresh full load in a new tables/collections (Default)")
    #parser.add_argument("--replace", default=False, action='store_true', help="Load with replacement in an existing table/collection (default)")
    #
    parser.add_argument("--etl_entity_sequence_clusters", default=False, action='store_true', help="ETL entity sequence clusters")

    parser.add_argument("--data_set_id", default=None, help="Data set identifier (e.g., 2018_14)")
    parser.add_argument("--sequence_cluster_data_path", default=None, help="Sequence cluster data path")

    #
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default="DEFAULT", help="Configuration section name")

    parser.add_argument("--db_type", default="mongo", help="Database server type (default=mongo)")

    # parser.add_argument("--document_style", default="rowwise_by_name_with_cardinality",
    #                    help="Document organization (rowwise_by_name_with_cardinality|rowwise_by_name|columnwise_by_name|rowwise_by_id|rowwise_no_name")
    parser.add_argument("--read_back_check", default=False, action='store_true', help="Perform read back check on all documents")
    #
    parser.add_argument("--num_proc", default=2, help="Number of processes to execute (default=2)")
    parser.add_argument("--chunk_size", default=10, help="Number of files loaded per process")
    parser.add_argument("--document_limit", default=None, help="Load document limit for testing")
    parser.add_argument("--prune_document_size", default=None, help="Prune large documents to this size limit (MB)")
    parser.add_argument("--debug", default=False, action='store_true', help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action='store_true', help="Use MOCK repository configuration for testing")
    parser.add_argument("--working_path", default=None, help="Working path for temporary files")
    args = parser.parse_args()
    #
    debugFlag = args.debug
    if debugFlag:
        logger.setLevel(logging.DEBUG)
        logger.debug("Using software version %s" % __version__)
    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    #                                       Configuration Details
    configPath = args.config_path
    configName = args.config_name
    if not configPath:
        configPath = os.getenv('DBLOAD_CONFIG_PATH', None)
    try:
        if os.access(configPath, os.R_OK):
            os.environ['DBLOAD_CONFIG_PATH'] = configPath
            logger.info("Using configuation path %s (%s)" % (configPath, configName))
        else:
            logger.error("Missing or access issue with config file %r" % configPath)
            exit(1)
        mockTopPath = os.path.join(TOPDIR, "rcsb_db", "data") if args.mock else None
        cfgOb = ConfigUtil(configPath=configPath, sectionName=configName, mockTopPath=mockTopPath)
    except Exception as e:
        logger.error("Missing or access issue with config file %r" % configPath)
        exit(1)

    #
    try:
        readBackCheck = args.read_back_check

        dataSetId = args.data_set_id
        seqDataLocator = args.sequence_cluster_data_path

        numProc = int(args.num_proc)
        chunkSize = int(args.chunk_size)
        documentLimit = int(args.document_limit) if args.document_limit else None

        loadType = 'full' if args.full else 'replace'
        loadType = 'replace' if args.replace else 'full'

        workPath = args.working_path if args.working_path else '.'

        # if args.document_style not in ['rowwise_by_name', 'rowwise_by_name_with_cardinality', 'columnwise_by_name', 'rowwise_by_id', 'rowwise_no_name']:
        #    logger.error("Unsupported document style %s" % args.document_style)

        if args.db_type != "mongo":
            logger.error("Unsupported database server type %s" % args.db_type)
    except Exception as e:
        logger.exception("Argument processing problem %s" % str(e))
        parser.print_help(sys.stderr)
        exit(1)
    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    ##
    if args.db_type == "mongo":
        if args.etl_entity_sequence_clusters:
            cw = ClusterEtlWorker(
                cfgOb,
                numProc=numProc,
                chunkSize=chunkSize,
                documentLimit=documentLimit,
                verbose=debugFlag,
                readBackCheck=readBackCheck,
                workPath=workPath)
            ok = cw.etl(dataSetId, seqDataLocator)

        logger.info("Operation completed with status %r " % ok)


if __name__ == '__main__':
    main()
