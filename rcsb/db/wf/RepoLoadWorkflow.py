##
# File: RepoLoadWorkflow.py
# Date: 15-Dec-2019  jdw
#
#  Workflow wrapper  --  repository database loading utilities --
#
#  Updates:
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.db.cli.RepoHoldingsEtlWorker import RepoHoldingsEtlWorker
from rcsb.db.cli.SequenceClustersEtlWorker import SequenceClustersEtlWorker
from rcsb.db.helpers.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.mongo.PdbxLoader import PdbxLoader
from rcsb.db.utils.TimeUtil import TimeUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class RepoLoadWorkflow(object):
    def __init__(self, **kwargs):
        #  Configuration Details
        configPath = kwargs.get("configPath", "exdb-config-example.yml")
        self.__configName = kwargs.get("configName", "site_info_configuration")
        mockTopPath = kwargs.get("mockTopPath", None)
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=mockTopPath)
        #
        self.__cachePath = kwargs.get("cachePath", ".")
        self.__cachePath = os.path.abspath(self.__cachePath)
        self.__debugFlag = kwargs.get("debugFlag", False)
        if self.__debugFlag:
            logger.setLevel(logging.DEBUG)
        #
        #  Rebuild or check resource cache
        rebuildCache = kwargs.get("rebuildCache", False)
        self.__cacheStatus = self.buildResourceCache(rebuildCache=rebuildCache)
        logger.info("Cache status if %r", self.__cacheStatus)
        #

    def load(self, op, **kwargs):
        if not self.__cacheStatus:
            logger.error("Cache update failed")
            return False
        # argument processing
        if op not in ["pdbx-loader", "etl-repository-holdings", "etl-entity-sequence-clusters"]:
            return False
        try:
            readBackCheck = kwargs.get("readBackCheck", False)
            numProc = int(kwargs.get("numPro", 1))
            chunkSize = int(kwargs.get("chunkSize", 10))
            fileLimit = int(kwargs.get("fileLimit")) if "fileLimit" in kwargs else None
            documentLimit = int(kwargs.get("documentLimit")) if "documentLimit" in kwargs else None
            failedFilePath = kwargs.get("failFileListPath", None)
            loadFileListPath = kwargs.get("loadFileListPath", None)
            saveInputFileListPath = kwargs.get("saveFilListPath", None)
            schemaLevel = kwargs.get("schemaLevel", "min") if kwargs.get("schemaLevel") in ["min", "full"] else "min"
            loadType = kwargs.get("loadType", "full")  # or replace
            pruneDocumentSize = float(kwargs.get("pruneDocumentSize")) if "pruneDocumentSize" in kwargs else None

            # "Document organization (rowwise_by_name_with_cardinality|rowwise_by_name|columnwise_by_name|rowwise_by_id|rowwise_no_name",
            documentStyle = kwargs.get("documentStyle", "rowwise_by_name_with_cardinality")
            dbType = kwargs.get("dbType", "mongo")
            #
            databaseName = kwargs.get("databaseName", None)
            databaseNameList = self.__cfgOb.get("DATABASE_NAMES_ALL", sectionName="database_catalog_configuration").split(",")
            #
            collectionNameList = kwargs.get("collectionNameList", None)
            mergeValidationReports = kwargs.get("mergeValidationReports", True)
            #
            tU = TimeUtil()
            dataSetId = kwargs.get("dataSetId") if "dataSetId" in kwargs else tU.getCurrentWeekSignature()
            seqDataLocator = self.__cfgOb.getPath("RCSB_SEQUENCE_CLUSTER_DATA_PATH", sectionName=self.__configName)
            sandboxPath = self.__cfgOb.getPath("RCSB_EXCHANGE_SANDBOX_PATH", sectionName=self.__configName)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            return False

        #

        if op == "pdbx-loader" and dbType == "mongo" and databaseName in databaseNameList:
            okS = True
            try:
                inputPathList = None
                if loadFileListPath:
                    mu = MarshalUtil(workPath=self.__cachePath)
                    inputPathList = mu.doImport(loadFileListPath, fmt="list")
                    if not inputPathList:
                        logger.error("Missing or empty input file path list %s", loadFileListPath)
                        return False
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                return False
            #
            try:
                mw = PdbxLoader(
                    self.__cfgOb,
                    self.__cachePath,
                    resourceName="MONGO_DB",
                    numProc=numProc,
                    chunkSize=chunkSize,
                    fileLimit=fileLimit,
                    verbose=self.__debugFlag,
                    readBackCheck=readBackCheck,
                )
                ok = mw.load(
                    databaseName,
                    collectionLoadList=collectionNameList,
                    loadType=loadType,
                    inputPathList=inputPathList,
                    styleType=documentStyle,
                    dataSelectors=["PUBLIC_RELEASE"],
                    failedFilePath=failedFilePath,
                    saveInputFileListPath=saveInputFileListPath,
                    pruneDocumentSize=pruneDocumentSize,
                    validationLevel=schemaLevel,
                    mergeContentTypes=["vrpt"] if mergeValidationReports else None,
                )
                okS = self.loadStatus(mw.getLoadStatus(), readBackCheck=readBackCheck)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
        elif op == "etl-entity-sequence-clusters" and dbType == "mongo":
            cw = SequenceClustersEtlWorker(
                self.__cfgOb, numProc=numProc, chunkSize=chunkSize, documentLimit=documentLimit, verbose=self.__debugFlag, readBackCheck=readBackCheck, workPath=self.__cachePath
            )
            ok = cw.etl(dataSetId, seqDataLocator, loadType=loadType)
            okS = self.loadStatus(cw.getLoadStatus(), readBackCheck=readBackCheck)
        elif op == "etl-repository-holdings" and dbType == "mongo":
            rhw = RepoHoldingsEtlWorker(
                self.__cfgOb,
                sandboxPath,
                self.__cachePath,
                numProc=numProc,
                chunkSize=chunkSize,
                documentLimit=documentLimit,
                verbose=self.__debugFlag,
                readBackCheck=readBackCheck,
            )
            ok = rhw.load(dataSetId, loadType=loadType)
            okS = self.loadStatus(rhw.getLoadStatus(), readBackCheck=readBackCheck)

        logger.info("Operations completed with status %r " % ok and okS)

        return ok and okS

    def loadStatus(self, statusList, readBackCheck=True):
        ret = False
        try:
            dl = DocumentLoader(self.__cfgOb, self.__cachePath, "MONGO_DB", numProc=1, chunkSize=2, documentLimit=None, verbose=False, readBackCheck=readBackCheck)
            #
            sectionName = "data_exchange_configuration"
            databaseName = self.__cfgOb.get("DATABASE_NAME", sectionName=sectionName)
            collectionName = self.__cfgOb.get("COLLECTION_UPDATE_STATUS", sectionName=sectionName)
            ret = dl.load(databaseName, collectionName, loadType="append", documentList=statusList, indexAttributeList=["update_id", "database_name", "object_name"], keyNames=None)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ret

    def buildResourceCache(self, rebuildCache=False):
        """Generate and cache resource dependencies.
        """
        ret = False
        try:
            rp = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, cachePath=self.__cachePath)
            ret = rp.cacheResources(useCache=not rebuildCache)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ret