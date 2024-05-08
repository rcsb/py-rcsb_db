##
# File: RepoLoadWorkflow.py
# Date: 15-Dec-2019  jdw
#
#  Workflow wrapper  --  repository database loading utilities --
#
#  Updates:
#   1-Jun-2022 dwp Add clusterFileNameTemplate to load method kwargs
#   2-Feb-2023 dwp Add removeAndRecreateDbCollections method for wiping a database without involving any data loading;
#                  Add support for inputIdCodeList
#  26-Apr-2023 dwp Add regexPurge flag to control running regexp purge step during document load (with default set to False)
#   7-Nov-2023 dwp Add maxStepLength parameter
#  26-Mar-2024 dwp Add arguments and methods to support CLI usage from weekly-update workflow
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import random
import math

from rcsb.db.cli.RepoHoldingsEtlWorker import RepoHoldingsEtlWorker
from rcsb.db.cli.SequenceClustersEtlWorker import SequenceClustersEtlWorker
from rcsb.utils.dictionary.DictMethodResourceProvider import DictMethodResourceProvider
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
        self.__configName = kwargs.get("configName", "site_info_remote_configuration")
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
        # rebuildCache = kwargs.get("rebuildCache", False)
        # self.__cacheStatus = self.buildResourceCache(rebuildCache=rebuildCache)
        # logger.debug("Cache status if %r", self.__cacheStatus)
        #

    def load(self, op, **kwargs):
        # if not self.__cacheStatus:
        #    logger.error("Resource cache test or rebuild has failed - exiting")
        #    return False
        # argument processing
        if op not in ["pdbx_loader", "etl_repository_holdings", "etl_entity_sequence_clusters"]:
            logger.error("Unsupported operation %r - exiting", op)
            return False
        try:
            databaseName = kwargs.get("databaseName", None)
            databaseNameList = self.__cfgOb.get("DATABASE_NAMES_ALL", sectionName="database_catalog_configuration").split(",")
            collectionNameList = kwargs.get("collectionNameList", None)
            loadType = kwargs.get("loadType", "replace")  # or "full"
            dbType = kwargs.get("dbType", "mongo")
            #
            numProc = int(kwargs.get("numProc", 1))
            chunkSize = int(kwargs.get("chunkSize", 10))
            maxStepLength = int(kwargs.get("maxStepLength", 500))
            fileLimit = kwargs.get("fileLimit", None)
            fileLimit = int(fileLimit) if fileLimit else None
            readBackCheck = kwargs.get("readBackCheck", True)
            rebuildSchemaFlag = kwargs.get("rebuildSchemaFlag", False)
            documentLimit = kwargs.get("documentLimit", None)
            documentLimit = int(documentLimit) if documentLimit else None
            failedFilePath = kwargs.get("failedFilePath", None)
            loadIdListPath = kwargs.get("loadIdListPath", None)
            # inputIdCodeList = kwargs.get("inputIdCodeList", None)
            loadFileListPath = kwargs.get("loadFileListPath", None)
            saveInputFileListPath = kwargs.get("saveInputFileListPath", None)
            schemaLevel = kwargs.get("schemaLevel", "min") if kwargs.get("schemaLevel") in ["min", "full"] else "min"
            updateSchemaOnReplace = kwargs.get("updateSchemaOnReplace", True)
            pruneDocumentSize = kwargs.get("pruneDocumentSize", None)
            pruneDocumentSize = float(pruneDocumentSize) if pruneDocumentSize else None
            regexPurge = kwargs.get("regexPurge", False)
            providerTypeExclude = kwargs.get("providerTypeExclude", None)
            clusterFileNameTemplate = kwargs.get("clusterFileNameTemplate", None)
            #
            # "Document organization (rowwise_by_name_with_cardinality|rowwise_by_name|columnwise_by_name|rowwise_by_id|rowwise_no_name",
            documentStyle = kwargs.get("documentStyle", "rowwise_by_name_with_cardinality")
            dataSelectors = kwargs.get("dataSelectors", ["PUBLIC_RELEASE"])
            #
            mergeValidationReports = kwargs.get("mergeValidationReports", True)
            mergeContentTypes = ["vrpt"] if mergeValidationReports else None
            #
            rebuildCache = kwargs.get("rebuildCache", False)
            forceReload = kwargs.get("forceReload", False)
            #
            tU = TimeUtil()
            dataSetId = kwargs.get("dataSetId") if "dataSetId" in kwargs else tU.getCurrentWeekSignature()
            seqDataLocator = self.__cfgOb.getPath("RCSB_SEQUENCE_CLUSTER_DATA_PATH", sectionName=self.__configName)
            sandboxPath = self.__cfgOb.getPath("RCSB_EXCHANGE_SANDBOX_PATH", sectionName=self.__configName)

        except Exception as e:
            logger.exception("Argument and configuration processing failing with %s", str(e))
            return False
        #

        if op == "pdbx_loader" and dbType == "mongo" and databaseName in databaseNameList:
            okS = True
            try:
                inputPathList, inputIdCodeList = None, None
                if loadIdListPath:
                    mu = MarshalUtil(workPath=self.__cachePath)
                    inputIdCodeList = mu.doImport(loadIdListPath, fmt="list")
                    if not inputIdCodeList:
                        logger.error("Operation %r missing or empty input file path list %s - exiting", op, loadIdListPath)
                        return False
                elif loadFileListPath:
                    mu = MarshalUtil(workPath=self.__cachePath)
                    inputPathList = mu.doImport(loadFileListPath, fmt="list")
                    if not inputPathList:
                        logger.error("Operation %r missing or empty input file path list %s - exiting", op, loadFileListPath)
                        return False
            except Exception as e:
                logger.exception("Operation %r processing input path list failing with %s", op, str(e))
                return False
            #
            try:
                mw = PdbxLoader(
                    self.__cfgOb,
                    self.__cachePath,
                    resourceName="MONGO_DB",
                    numProc=numProc,
                    chunkSize=chunkSize,
                    maxStepLength=maxStepLength,
                    fileLimit=fileLimit,
                    verbose=self.__debugFlag,
                    readBackCheck=readBackCheck,
                    rebuildSchemaFlag=rebuildSchemaFlag,
                )
                ok = mw.load(
                    databaseName,
                    collectionLoadList=collectionNameList,
                    loadType=loadType,
                    inputPathList=inputPathList,
                    inputIdCodeList=inputIdCodeList,
                    styleType=documentStyle,
                    dataSelectors=dataSelectors,
                    failedFilePath=failedFilePath,
                    saveInputFileListPath=saveInputFileListPath,
                    pruneDocumentSize=pruneDocumentSize,
                    regexPurge=regexPurge,
                    validationLevel=schemaLevel,
                    mergeContentTypes=mergeContentTypes,
                    providerTypeExclude=providerTypeExclude,
                    updateSchemaOnReplace=updateSchemaOnReplace,
                    rebuildCache=rebuildCache,
                    forceReload=forceReload,
                )
                okS = self.loadStatus(mw.getLoadStatus(), readBackCheck=readBackCheck)
            except Exception as e:
                logger.exception("Operation %r database %r failing with %s", op, databaseName, str(e))
        elif op == "etl_entity_sequence_clusters" and dbType == "mongo":
            cw = SequenceClustersEtlWorker(
                self.__cfgOb,
                numProc=numProc,
                chunkSize=chunkSize,
                maxStepLength=maxStepLength,
                documentLimit=documentLimit,
                verbose=self.__debugFlag,
                readBackCheck=readBackCheck,
                workPath=self.__cachePath,
                clusterFileNameTemplate=clusterFileNameTemplate,
            )
            ok = cw.etl(dataSetId, seqDataLocator, loadType=loadType)
            okS = self.loadStatus(cw.getLoadStatus(), readBackCheck=readBackCheck)
        elif op == "etl_repository_holdings" and dbType == "mongo":
            rhw = RepoHoldingsEtlWorker(
                self.__cfgOb,
                sandboxPath,
                self.__cachePath,
                numProc=numProc,
                chunkSize=chunkSize,
                maxStepLength=maxStepLength,
                documentLimit=documentLimit,
                verbose=self.__debugFlag,
                readBackCheck=readBackCheck,
            )
            ok = rhw.load(dataSetId, loadType=loadType)
            okS = self.loadStatus(rhw.getLoadStatus(), readBackCheck=readBackCheck)

        logger.info("Completed operation %r with status %r", op, ok and okS)

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

    def buildResourceCache(self, rebuildCache=False, providerTypeExclude=None, restoreUseStash=True, restoreUseGit=True):
        """Generate and cache resource dependencies."""
        ret = False
        try:
            # First make sure the CACHE directory exists
            if not os.path.isdir(self.__cachePath):
                logger.info("Cache directory %s doesn't exist. Creating it", self.__cachePath)
                os.makedirs(self.__cachePath)
            else:
                logger.info("Cache directory %s already exists.", self.__cachePath)

            # Now build the cache
            useCache = not rebuildCache
            rP = DictMethodResourceProvider(
                self.__cfgOb,
                configName=self.__configName,
                cachePath=self.__cachePath,
                restoreUseStash=restoreUseStash,
                restoreUseGit=restoreUseGit,
                providerTypeExclude=providerTypeExclude,
            )
            ret = rP.cacheResources(useCache=useCache, doBackup=False, useStash=False, useGit=False)
            logger.info("useCache %r cache reload status (%r)", useCache, ret)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ret

    def removeAndRecreateDbCollections(self, op, **kwargs):
        if op not in ["pdbx_db_wiper"]:
            logger.error("Unsupported operation %r - exiting", op)
            return False
        try:
            schemaLevel = kwargs.get("schemaLevel", "min") if kwargs.get("schemaLevel") in ["min", "full"] else "min"
            dbType = kwargs.get("dbType", "mongo")
            #
            databaseName = kwargs.get("databaseName", None)
            databaseNameList = self.__cfgOb.get("DATABASE_NAMES_ALL", sectionName="database_catalog_configuration").split(",")
            collectionNameList = kwargs.get("collectionNameList", None)
            #
        except Exception as e:
            logger.exception("Argument and configuration processing failing with %s", str(e))
            return False
        #
        if dbType == "mongo" and databaseName in databaseNameList:
            try:
                mw = PdbxLoader(
                    self.__cfgOb,
                    self.__cachePath,
                    resourceName="MONGO_DB",
                    verbose=self.__debugFlag,
                )
                ok = mw.removeAndRecreateDbCollections(
                    databaseName,
                    collectionLoadList=collectionNameList,
                    validationLevel=schemaLevel,
                )
            except Exception as e:
                logger.exception("Operation %r database %r failing with %s", op, databaseName, str(e))

        logger.info("Completed operation %r with status %r", op, ok)

        return ok

    def splitIdList(self, op, **kwargs):
        if op not in ["pdbx_id_list_splitter"]:
            logger.error("Unsupported operation %r - exiting", op)
            return False

        databaseName = kwargs.get("databaseName")
        holdingsFilePath = kwargs.get("holdingsFilePath", None)  # For CSMs: http://computed-models-internal-%s.rcsb.org/staging/holdings/computed-models-holdings-list.json
        loadFileListDir = kwargs.get("loadFileListDir")  # ExchangeDbConfig().loadFileListsDir
        loadFileListPrefix = databaseName + "_ids"  # pdbx_core_ids or pdbx_comp_model_core_ids
        numSublistFiles = kwargs.get("numSublistFiles")  # ExchangeDbConfig().pdbxCoreNumberSublistFiles
        #
        mU = MarshalUtil(workPath=self.__cachePath)
        #
        if databaseName == "pdbx_core":
            # Get list of ALL entries to be loaded for the current update cycle
            if not holdingsFilePath:
                holdingsFilePath = os.path.join(self.__cfgOb.getPath("PDB_REPO_URL", sectionName=self.__configName), "pdb/holdings/released_structures_last_modified_dates.json.gz")
            holdingsFileD = mU.doImport(holdingsFilePath, fmt="json")
            idL = [k.upper() for k in holdingsFileD]
            logger.info("Total number of entries to load: %d (obtained from file: %s)", len(idL), holdingsFilePath)
            random.shuffle(idL)  # randomize the order to reduce the chance of consecutive large structures occurring (which may cause memory spikes)
            filePathMappingD = self.splitIdListAndWriteToFiles(idL, numSublistFiles, loadFileListDir, loadFileListPrefix, holdingsFilePath)

        elif databaseName == "pdbx_comp_model_core":
            filePathMappingD = {}
            if holdingsFilePath:
                holdingsFileBaseDir = os.path.dirname(os.path.dirname(holdingsFilePath))
            else:
                holdingsFilePath = self.__cfgOb.getPath("PDBX_COMP_MODEL_HOLDINGS_LIST_PATH", sectionName=self.__configName)
                holdingsFileBaseDir = self.__cfgOb.getPath("PDBX_COMP_MODEL_REPO_PATH", sectionName=self.__configName)
            holdingsFileD = mU.doImport(holdingsFilePath, fmt="json")
            #
            if len(holdingsFileD) == 1:
                # Split up single holdings file into multiple sub-lists
                holdingsFile = os.path.join(holdingsFileBaseDir, list(holdingsFileD.keys())[0])
                hD = mU.doImport(holdingsFile, fmt="json")
                idL = [k.upper() for k in hD]
                logger.info("Total number of entries to load for holdingsFile %s: %d", holdingsFile, len(idL))
                filePathMappingD = self.splitIdListAndWriteToFiles(idL, numSublistFiles, loadFileListDir, loadFileListPrefix, holdingsFile)
            #
            elif len(holdingsFileD) > 1:
                # Create one sub-list for each holdings file
                mU = MarshalUtil()
                index = 1
                for hF, count in holdingsFileD.items():
                    holdingsFile = os.path.join(holdingsFileBaseDir, hF)
                    hD = mU.doImport(holdingsFile, fmt="json")
                    idL = [k.upper() for k in hD]
                    logger.info("Total number of entries to load for holdingsFile %s: %d", holdingsFile, len(idL))
                    #
                    fPath = os.path.join(loadFileListDir, f"{loadFileListPrefix}-{index}.txt")
                    ok = mU.doExport(fPath, idL, fmt="list")
                    if not ok:
                        raise ValueError("Failed to export id list %r" % fPath)
                    filePathMappingD.update({str(index): {"filePath": fPath, "numModels": count, "sourceFile": holdingsFile}})
                    index += 1
                #
                mappingFilePath = os.path.join(loadFileListDir, loadFileListPrefix + "_mapping.json")
                ok = mU.doExport(mappingFilePath, filePathMappingD, fmt="json", indent=4)

            else:
                logger.error("Unsupported database for ID list splitting %s", databaseName)
                return False

        # Do one last santity check
        ok = filePathMappingD is not None
        outfilePathL = [v["filePath"] for k, v in filePathMappingD.items()]
        for oPath in outfilePathL:
            ok = (os.path.exists(oPath) and os.path.isfile(oPath)) and ok
            if not ok:
                logger.error("Entry ID loading sublist file does not exist: %r", oPath)

        return ok

    def splitIdListAndWriteToFiles(self, inputList, nFiles, outfileDir, outfilePrefix, sourceFile):
        """Split input ID list into equally distributed sublists of size nFiles.

        Write files to the given outfileDir and outfilePrefix.

        Returns:
            list: list of output file paths
        """
        sublistSize = math.ceil(len(inputList) / nFiles)

        # Split the input list into n sublists
        sublists = [inputList[i: i + sublistSize] for i in range(0, len(inputList), sublistSize)]

        # Write each sublist to a separate file
        filePathMappingD = {}
        for idx, sublist in enumerate(sublists):
            index = idx + 1
            filePath = os.path.join(outfileDir, f"{outfilePrefix}-{index}.txt")
            with open(filePath, "w", encoding="utf-8") as file:
                # Write each string on its own line in the file
                for string in sublist:
                    file.write(f"{string}\n")
            filePathMappingD.update({str(index): {"filePath": filePath, "numModels": len(sublist), "sourceFile": sourceFile}})

        mappingFilePath = os.path.join(outfileDir, outfilePrefix + "_mapping.json")
        mU = MarshalUtil()
        ok = mU.doExport(mappingFilePath, filePathMappingD, fmt="json", indent=4)
        if not ok:
            raise ValueError("Failed to export mappingFilePath %r" % mappingFilePath)

        return filePathMappingD

    def loadCompleteCheck(self, op, **kwargs):
        if op not in ["pdbx_loader_check"]:
            logger.error("Unsupported operation %r - exiting", op)
            return False
        try:
            databaseName = kwargs.get("databaseName", None)
            holdingsFilePath = kwargs.get("holdingsFilePath", None)
            completeIdCodeList, completeIdCodeCount = self.__getCompleteIdListCount(databaseName, holdingsFilePath)
            if not (completeIdCodeList or completeIdCodeCount):
                logger.error("Failed to get completeIdCodeList and completeIdCodeCount for database %r", databaseName)
                return False
            #
        except Exception as e:
            logger.exception("Argument and configuration processing failing with %s", str(e))
            return False
        #
        try:
            mw = PdbxLoader(
                self.__cfgOb,
                self.__cachePath,
                resourceName="MONGO_DB",
                verbose=self.__debugFlag,
            )
            ok = mw.loadCompleteCheck(
                databaseName,
                completeIdCodeList=completeIdCodeList,
                completeIdCodeCount=completeIdCodeCount,
            )
        except Exception as e:
            logger.exception("Operation %r database %r failing with %s", op, databaseName, str(e))

        logger.info("Completed operation %r with status %r", op, ok)

        return ok

    def __getCompleteIdListCount(self, databaseName, holdingsFilePath):
        mU = MarshalUtil(workPath=self.__cachePath)
        #
        if databaseName == "pdbx_core":
            # Get list of ALL entries to be loaded for the current update cycle
            if not holdingsFilePath:
                holdingsFilePath = os.path.join(self.__cfgOb.getPath("PDB_REPO_URL", sectionName=self.__configName), "pdb/holdings/released_structures_last_modified_dates.json.gz")
            holdingsFileD = mU.doImport(holdingsFilePath, fmt="json")
            idL = [k.upper() for k in holdingsFileD]
            logger.info("Total number of entries to load: %d (obtained from file: %s)", len(idL), holdingsFilePath)
            return idL, len(idL)

        elif databaseName == "pdbx_comp_model_core":
            if not holdingsFilePath:
                holdingsFilePath = self.__cfgOb.getPath("PDBX_COMP_MODEL_HOLDINGS_LIST_PATH", sectionName=self.__configName)
            holdingsFileD = mU.doImport(holdingsFilePath, fmt="json")
            # Don't return the actual CSM file list since it will be unmanageable upon scaling
            return None, sum(holdingsFileD.values())

        else:
            logger.error("Unsupported database for completed load checking %s", databaseName)

        return None, None
