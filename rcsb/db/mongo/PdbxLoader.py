##
# File:    PdbxLoader.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#     20-Mar-2018 jdw  adding prdcc within chemical component collection
#     21-Mar-2018 jdw  content filtering options added from documents
#     25-Mar-2018 jdw  add support for loading multiple collections for a content type.
#     26-Mar-2018 jdw  improve how successful loads are tracked accross collections -
#      9-Apr-2018 jdw  folding in improved schema path utility apis
#     19-Jun-2018 jdw  integrate with dynamic schema / status object must be unit cardinality.
#     22-Jun-2018 jdw  change collection attribute specification to dot notation
#     22-Jun-2018 jdw  separate cases where the loading success can be easily mapped to the source data object.
#     24-Jun-2018 jdw  rename and specialize function
#     14-Jul-2018 jdw  add methods to return data exchange status objects for load operations
#     25-Jul-2018 jdw  fixed bazaar bad function references blocking failure handling
#     25-Jul-2018 jdw  restore pruning operations
#     14-Aug-2018 jdw  primaryIndexD from self.__schU.getSchemaInfo(schemaName) updated to list and i
#                      in __createCollection(self, databaseName, collectionName, indexAttributeNames=None) make indexAttributeNames a list
#     10-Sep-2018 jdw  Adjust error handling and reporting across multiple collections
#     24-Oct-2018 jdw  update for new configuration organization
#     11-Nov-2018 jdw  add DrugBank and CCDC mapping path details.
#     21-Nov-2018 jdw  add addDocumentPrivateAttributes(dList, collectionName) to inject private document keys
#      3-Dec-2018 jdw  generalize the creation of collection indices.
#     16-Feb-2019 jdw  Add argument mergeContentTypes to load() method. Generalize the handling of path lists to
#                      support locator object lists.
#      6-Aug-2019 jdw  Add schema generation option and move dictionary API instantiation into load() method.
#     18-May-2020 jdw  Add brute force document purging for loadType=replace
#     10-Jan-2022 dwp  Add support for loading id code lists for mongo PdbxLoader() (preliminary)
#     29-Apr-2022 dwp  Add support for handling and making use of internal computed-model identifiers
#     29-Jun-2022 dwp  Remove uneeded custom-support for computed-model identifiers (will now use the internally-modified entry.id)
#      2-Feb-2023 dwp  Add removeAndRecreateDbCollections method for wiping a database without involving any data loading
#
##
"""
Worker methods for loading primary data content following mapping conventions in external schema definitions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

# pylint: disable=too-many-lines

import logging
import operator
import os
import sys
import time

import bson

from jsonschema import Draft4Validator
from jsonschema import FormatChecker
from mmcif.api.DictMethodRunner import DictMethodRunner
from rcsb.utils.dictionary.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
from rcsb.utils.dictionary.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.processors.DataTransformFactory import DataTransformFactory
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.utils.repository.RepositoryProvider import RepositoryProvider
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

logger = logging.getLogger(__name__)


class PdbxLoaderWorker(object):
    def __init__(self, cfgOb, repositoryProviderObj, dictionaryMethodRunnerObj, resourceName):
        self.__verbose = False
        self.__cfgOb = cfgOb
        self.__rpP = repositoryProviderObj
        self.__dmh = dictionaryMethodRunnerObj
        self.__resourceName = resourceName

    def loadWorker(self, dataList, procName, optionsD, workingDir):
        """Multi-proc worker method for MongoDb loading -

        successList, resultList, diagList=workerFunc(runList=nextList,procName, optionsD, workingDir)

        locatorObjList -> containerList -> docList  ->|LOAD|<-  .... return success locatorObjList

        """
        try:
            startTime = self.__begin(message=procName)
            # Recover common options
            styleType = optionsD["styleType"]
            filterType = optionsD["filterType"]
            readBackCheck = optionsD["readBackCheck"]
            logSize = "logSize" in optionsD and optionsD["logSize"]
            dataSelectors = optionsD["dataSelectors"]
            loadType = optionsD["loadType"]
            databaseName = optionsD["databaseName"]
            pruneDocumentSize = optionsD["pruneDocumentSize"]
            sd = optionsD["schemaDefAccess"]
            dtf = optionsD["dataTransformFactory"]
            collectionNameList = optionsD["collectionNameList"]
            useNameFlag = optionsD["useNameFlag"]
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=workingDir, verbose=self.__verbose)
            # -------------------------------------------
            # -- Create map of  cIdD{ container identifier} =  locatorObj
            #
            collectionName = None
            cIdD = {}
            cNameL = []
            containerList = []
            for locatorObj in dataList:
                # JDW
                cL = self.__rpP.getContainerList([locatorObj])
                if cL:
                    cNameL.append(cL[0].getName().upper().strip())
                    cId = cL[0].getName() if useNameFlag else cL[0].getProp("uid")
                    cIdD[cId] = locatorObj
                    containerList.extend(cL)
            # -- Apply methods to each container -
            for container in containerList:
                if self.__dmh:
                    self.__dmh.apply(container)
                else:
                    logger.debug("%s No dynamic method handler for ", procName)
            # -----
            if loadType != "full":
                for collectionName in collectionNameList:
                    ok = self.__purgeDocuments(databaseName, collectionName, cNameL)
                    logger.debug("%s %s - loadType %r cNameL %r (%r)", databaseName, collectionName, loadType, cNameL, ok)
            # --
            failContainerIdS = set()
            rejectContainerIdS = set()
            # -----
            for collectionName in collectionNameList:
                ok = True
                # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
                docIdL = sd.getDocumentKeyAttributeNames(collectionName)
                replaceIdL = sd.getDocumentReplaceAttributeNames(collectionName)
                #
                tableIdExcludeList = sd.getCollectionExcluded(collectionName)
                tableIdIncludeList = sd.getCollectionSelected(collectionName)
                sliceFilter = sd.getCollectionSliceFilter(collectionName)
                sdp.setSchemaIdExcludeList(tableIdExcludeList)
                sdp.setSchemaIdIncludeList(tableIdIncludeList)
                #
                logger.debug("%s databaseName %s collectionName %s slice filter %s", procName, databaseName, collectionName, sliceFilter)
                logger.debug("%s databaseName %s include list %r", procName, databaseName, tableIdIncludeList)
                logger.debug("%s databaseName %s exclude list %r", procName, databaseName, tableIdExcludeList)
                #
                dList, containerIdList, rejectIdList = sdp.processDocuments(
                    containerList,
                    styleType=styleType,
                    filterType=filterType,
                    dataSelectors=dataSelectors,
                    sliceFilter=sliceFilter,
                    useNameFlag=useNameFlag,
                    collectionName=collectionName,
                )
                #
                # ------
                # Collect the container identifiers for the rejected containers (paths for logging only)
                # Note that rejections are NOT treated as failures!
                #
                rejectPathList = []
                for cId in rejectIdList:
                    rejectContainerIdS.add(cId)
                    locObj = cIdD[cId]
                    rejectPathList.extend(self.__rpP.getLocatorPaths([locObj], locatorIndex=0))
                rejectPathList = list(set(rejectPathList))
                #
                if logSize:
                    self.__logDocumentSize(procName, dList, docIdL)

                dList = sdp.addDocumentPrivateAttributes(dList, collectionName)
                dList = sdp.addDocumentSubCategoryAggregates(dList, collectionName)
                #
                # --- And after adjustments create index
                #     to map dList -> containerNamList  using dList(uniqId) -> containterName
                #
                indexDoc = {}
                try:
                    for dD, cId in zip(dList, containerIdList):
                        dIdTup = self.__getKeyValues(dD, docIdL)
                        indexDoc[dIdTup] = cId
                except Exception as e:
                    logger.exception("Failing cN %r  dD %r with %s", cId, dD, str(e))

                #
                if dList:
                    ok, _, failDocIdS = self.__loadDocuments(
                        databaseName, collectionName, dList, docIdL, replaceIdL=replaceIdL, loadType=loadType, readBackCheck=readBackCheck, pruneDocumentSize=pruneDocumentSize
                    )
                # ------
                # Collect the container identifiers for the successful loads (paths for logging only)
                #
                failPathList = []
                for dId in failDocIdS:
                    cId = indexDoc[dId]
                    failContainerIdS.add(cId)
                    locObj = cIdD[cId]
                    failPathList.extend(self.__rpP.getLocatorPaths([locObj], locatorIndex=0))
                failPathList = list(set(failPathList))
                #
                if failPathList:
                    logger.debug("%s %s/%s worker load failures %r", procName, databaseName, collectionName, [os.path.basename(pth) for pth in failPathList if pth is not None])
                if rejectPathList:
                    logger.debug("%s %s/%s worker load rejected %r", procName, databaseName, collectionName, [os.path.basename(pth) for pth in rejectPathList])
            #
            # -------------------------
            #  failContainerIdS = set()
            #  rejectContainerIdS = set()
            #
            #  cIdD[cId] = locatorObj
            # ----
            retList = [locatorObj for cId, locatorObj in cIdD.items() if cId not in failContainerIdS]
            logger.debug("%s %s load worker returns  successes %d rejects %d failures %d", procName, databaseName, len(retList), len(rejectContainerIdS), len(failContainerIdS))

            ok = len(failContainerIdS) == 0
            self.__end(startTime, procName + " with status " + str(ok))
            return retList, [], []

        except Exception as e:
            # logger.error("Failing for dataList %r" % dataList)
            logger.exception("Failing with %s", str(e))

        return [], [], []

    # -------------- -------------- -------------- -------------- -------------- -------------- --------------
    #                                        ---  Supporting code follows ---
    #
    def __logDocumentSize(self, procName, dList, docIdL):
        maxDocumentMegaBytes = -1
        thresholdMB = 15.8
        # thresholdMB = 5.0
        for tD in dList:
            cN = self.__getKeyValues(tD, docIdL)
            # documentMegaBytes = float(sys.getsizeof(pickle.dumps(tD, protocol=0))) / 1000000.0
            documentMegaBytes = float(sys.getsizeof(bson.BSON.encode(tD))) / 1000000.0
            logger.debug("%s Document %s %.4f MB", procName, cN, documentMegaBytes)
            maxDocumentMegaBytes = max(maxDocumentMegaBytes, documentMegaBytes)
            if documentMegaBytes > thresholdMB:
                logger.info("Large document %r  %.4f MB", cN, documentMegaBytes)
                for ky in tD:
                    try:
                        sMB = float(sys.getsizeof(bson.BSON.encode({"t": tD[ky]}))) / 1000000.0
                    except Exception:
                        sMB = -1
                    logger.info("Sub-document length %s sizeMB %.4f  %8d", ky, sMB, len(tD[ky]))
                #
        logger.info("%s maximum document size loaded %.4f MB", procName, maxDocumentMegaBytes)
        return True

    def __purgeDocuments(self, databaseName, collectionName, cardinalIdL):
        """Purge documents from collection within database with cardinal identifiers in cardinalIdL."""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                for cardId in cardinalIdL:
                    # selectD = {"rcsb_id": "/%s/" % cardId} # this filter did not work
                    selectD = {"rcsb_id": {"$regex": "^%s" % cardId.upper(), "$options": "i"}}  # case-insensitive
                    dCount = mg.delete(databaseName, collectionName, selectD)
                    logger.debug("Remove %d objects in database %s collection %s selection %r", dCount, databaseName, collectionName, selectD)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    #

    def __writePathList(self, filePath, pathList):
        try:
            with open(filePath, "w", encoding="utf-8") as ofh:
                for pth in pathList:
                    ofh.write("%s\n" % pth)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __begin(self, message=""):
        startTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting %s at %s", message, ts)
        return startTime

    def __end(self, startTime, message=""):
        endTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        delta = endTime - startTime
        logger.debug("Completed %s at %s (%.4f seconds)", message, ts, delta)

    def __createCollection(self, databaseName, collectionName, indexDL=None, bsonSchema=None):
        """Create database and collection and optionally a set of indices -"""
        try:
            logger.debug("Create database %s collection %s", databaseName, collectionName)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                ok1 = mg.createCollection(databaseName, collectionName, bsonSchema=bsonSchema)
                ok2 = mg.databaseExists(databaseName)
                ok3 = mg.collectionExists(databaseName, collectionName)
                okI = True
                if indexDL:
                    for indexD in indexDL:
                        okI = mg.createIndex(databaseName, collectionName, indexD["ATTRIBUTE_NAMES"], indexName=indexD["INDEX_NAME"], indexType="DESCENDING", uniqueFlag=False)

            return ok1 and ok2 and ok3 and okI
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __removeCollection(self, databaseName, collectionName):
        """Drop collection within database"""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                logger.debug("Remove collection database %s collection %s", databaseName, collectionName)
                logger.debug("Starting databases = %r", mg.getDatabaseNames())
                logger.debug("Starting collections = %r", mg.getCollectionNames(databaseName))
                ok = mg.dropCollection(databaseName, collectionName)
                logger.debug("Databases = %r", mg.getDatabaseNames())
                logger.debug("Post drop collections = %r", mg.getCollectionNames(databaseName))
                ok = mg.collectionExists(databaseName, collectionName)
                logger.debug("Post drop collections = %r", mg.getCollectionNames(databaseName))
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __pruneBySize(self, dList, limitMB=15.9):
        """For the input list of objects (dictionaries).objects
        Return a pruned list satisfying the input total object size limit -

        """
        oL = []
        try:
            for dD in dList:
                sD = {}
                sumMB = 0.0
                for ky in dD:
                    dMB = 0.0
                    try:
                        dMB = float(sys.getsizeof(bson.BSON.encode({ky: dD[ky]}))) / 1000000.0
                    except Exception as e:
                        logger.error("ky %r d[ky] %r with %s", ky, dD[ky], str(e))

                    sumMB += dMB
                    sD[ky] = dMB
                if sumMB < limitMB:
                    oL.append(dD)
                    continue
                #
                sortedSd = sorted(sD.items(), key=operator.itemgetter(1))
                prunedSum = 0.0
                for ky, sMB in sortedSd:
                    prunedSum += sMB
                    if prunedSum > limitMB:
                        dD.pop(ky, None)
                        logger.debug("Pruning ky %s size(MB) %.2f", ky, sMB)
                oL.append(dD)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            #
        logger.debug("Pruning returns document list length %d", len(dList))
        return oL

    def __loadDocuments(self, databaseName, collectionName, dList, docIdL, replaceIdL=None, loadType="full", readBackCheck=False, pruneDocumentSize=None):
        #
        # Load database/collection with input document list -
        #
        rIdL = []

        logger.debug("databaseName %s collectionName %s docIdL %r", databaseName, collectionName, docIdL)
        inputDocIdS = {self.__getKeyValues(dD, docIdL) for dD in dList}
        failDocIdS = set()
        successDocIdS = set()

        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                if loadType == "replace" and replaceIdL:
                    deleteTupL = mg.deleteList(databaseName, collectionName, dList, replaceIdL)
                    logger.debug("Deleted document status %r", deleteTupL)
                if pruneDocumentSize:
                    dList = self.__pruneBySize(dList, limitMB=pruneDocumentSize)
                #
                rIdL.extend(mg.insertList(databaseName, collectionName, dList, keyNames=docIdL, salvage=True))
                # ---
                #  If there is a failure then determine the specific successes and failures -
                #
                successDocIdS = inputDocIdS
                if len(rIdL) != len(dList):
                    sIdS = set()
                    try:
                        for rId in rIdL:
                            rObj = mg.fetchOne(databaseName, collectionName, "_id", rId)
                            dIdTup = self.__getKeyValues(rObj, docIdL)
                            sIdS.add(dIdTup)
                    except Exception as e:
                        logger.exception("Failing with %s", str(e))
                    successDocIdS = sIdS
                # enumerate the failures
                failDocIdS = inputDocIdS - successDocIdS
                #
                if readBackCheck:
                    # build an index of the input document list
                    indD = {}
                    try:
                        for ii, dD in enumerate(dList):
                            dIdTup = self.__getKeyValues(dD, docIdL)
                            indD[dIdTup] = ii
                    except Exception as e:
                        logger.exception("Failing ii %d d %r with %s", ii, dD, str(e))
                    #
                    # Note that objects in dList are mutated by the insert operation with the additional key '_id',
                    # hence, it is possible to compare the fetched object with the input object.
                    #
                    rbStatus = True
                    for ii, rId in enumerate(rIdL):
                        rObj = mg.fetchOne(databaseName, collectionName, "_id", rId)
                        dIdTup = self.__getKeyValues(rObj, docIdL)
                        jj = indD[dIdTup]
                        if rObj != dList[jj]:
                            rbStatus = False
                            break
                #
                if readBackCheck and not rbStatus:
                    return False, successDocIdS, failDocIdS
                #
            return len(rIdL) == len(dList), successDocIdS, failDocIdS
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return False, [], inputDocIdS

    def __getKeyValues(self, dct, keyNames):
        """Return the tuple of values of corresponding to the input dictionary key names expressed in dot notation.

        Args:
            dct (dict): source dictionary object (nested)
            keyNames (list): list of dictionary keys in dot notatoin

        Returns:
            tuple: tuple of values corresponding to the input key names

        """
        rL = []
        try:
            for keyName in keyNames:
                rL.append(self.__getKeyValue(dct, keyName))
        except Exception as e:
            logger.exception("Failing for key names %r with %s", keyNames, str(e))

        return tuple(rL)

    def __getKeyValue(self, dct, keyName):
        """Return the value of the corresponding key expressed in dot notation in the input dictionary object (nested)."""
        try:
            kys = keyName.split(".")
            for key in kys:
                try:
                    dct = dct[key]
                except Exception:
                    logger.warning("Missing document key %r in %r", key, list(dct.keys()))
                    return None
            return dct
        except Exception as e:
            logger.error("Failing for key %r with %s", keyName, str(e))

        return None


class PdbxLoader(object):
    def __init__(
        self,
        cfgOb,
        cachePath=None,
        resourceName="MONGO_DB",
        numProc=4,
        chunkSize=15,
        fileLimit=None,
        verbose=False,
        readBackCheck=False,
        maxStepLength=2000,
        useSchemaCache=True,
        rebuildSchemaFlag=False,
    ):
        """Worker methods for loading primary data content following mapping conventions in external schema definitions.

        Args:
            cfgOb (object): ConfigInfo() instance
            cachePath (str, optional): path to cache directories
            resourceName (str, optional): server resource name
            numProc (int, optional): number of processes to launch (MultiProcUtil)
            chunkSize (int, optional): partition list of load paths into chunks of this size
            fileLimit (int, optional): maximum number of files to process (no limit = None)
            verbose (bool, optional): Description
            readBackCheck (bool, optional): read back and check each loaded object
            maxStepLength (int, optional): limit multiprocessing steps

        """
        self.__verbose = verbose
        #
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__fileLimit = fileLimit
        self.__maxStepLength = maxStepLength
        #
        # Controls for multiprocessing execution -
        self.__numProc = max(numProc, 1)
        self.__chunkSize = max(chunkSize, 1)
        #
        self.__cfgOb = cfgOb
        self.__cfgSectionName = self.__cfgOb.getDefaultSectionName()
        self.__resourceName = resourceName
        #
        self.__readBackCheck = readBackCheck
        self.__cachePath = cachePath
        self.__useSchemaCache = useSchemaCache
        self.__rebuildSchemaFlag = rebuildSchemaFlag
        self.__mpFormat = "[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s"
        #
        #
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=self.__useSchemaCache, rebuildFlag=self.__rebuildSchemaFlag)
        self.__rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, cachePath=self.__cachePath)
        #
        self.__statusList = []
        #

        self.__dmh = None
        #

    def load(
        self,
        databaseName,
        collectionLoadList=None,
        loadType="full",
        inputPathList=None,
        inputIdCodeList=None,
        styleType="rowwise_by_name",
        dataSelectors=None,
        failedFilePath=None,
        saveInputFileListPath=None,
        pruneDocumentSize=None,
        logSize=False,
        validationLevel="min",
        mergeContentTypes=None,
        useNameFlag=True,
        updateSchemaOnReplace=True,
        validateFailures=True,
        reloadPartial=True,
        providerTypeExclude=None,
        restoreUseGit=True,
        restoreUseStash=True,
    ):
        """Driver method for loading PDBx/mmCIF content into the Mongo document store.

        Args:
            databaseName (str): A content datbase schema (e.g. 'bird','bird_family','bird_chem_comp', chem_comp', 'pdbx', 'pdbx_core')
            collectionLoadList (list, optional): list of collection names in this schema to load (default is load all collections)
            loadType (str, optional): mode of loading 'full' (bulk delete then bulk insert) or 'replace'
            inputPathList (list, optional): Data file path list (if not provided the full repository will be scanned)
            inputIdCodeList (list, optional): ID Code list (remote discovery mode) (if not provided the full repository will be scanned)
            styleType (str, optional): one of 'rowwise_by_name', 'columnwise_by_name', 'rowwise_no_name', 'rowwise_by_name_with_cardinality'
            dataSelectors (list, optional): selector names defined for this schema (e.g. PUBLIC_RELEASE)
            failedFilePath (str, optional): Path to hold file paths for load failures
            saveInputFileListPath (list, optional): List of files
            pruneDocumentSize (bool, optional): iteratively remove large elements from a collection to satisfy size limits
            logSize (bool, optional): Compute and log bson serialized object size
            validationLevel (str, optional): Completeness of json/bson metadata schema bound to each collection (e.g. 'min', 'full' or None)
            useNameFlag (bool, optional): Use container name as unique identifier otherwise use UID property.
            updateSchemaOnReplace (bool, optional): Update validation schema for loadType == 'replace'
            validateFailures (bool, optional): output validation report on load failures
            reloadPartial (bool, optional): on load failures attempt reload of partial objects.
            providerTypeExclude (str, optional): exclude dictionary method provider by type name. Defaults to None.
            restoreUseStash (bool, optional): restore cache resources using stash storage.  Defaults to True.
            restoreUseGit (bool, optional): restore cache resources using git storage.  Defaults to True.
        Returns:
            bool: True on success or False otherwise

        """
        try:
            #
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            #
            logger.info("Beginning load operation (%r) for database %s", loadType, databaseName)
            startTime = self.__begin(message="loading operation")
            #
            modulePathMap = self.__cfgOb.get("DICT_METHOD_HELPER_MODULE_PATH_MAP", sectionName=self.__cfgSectionName)
            dP = DictionaryApiProviderWrapper(self.__cachePath, cfgOb=self.__cfgOb, useCache=True)
            dictApi = dP.getApiByName(databaseName)
            # ---
            dmrP = DictMethodResourceProvider(
                self.__cfgOb, cachePath=self.__cachePath, restoreUseStash=restoreUseStash, restoreUseGit=restoreUseGit, providerTypeExclude=providerTypeExclude
            )
            # Cache dependencies in serial mode.
            ok = dmrP.cacheResources(useCache=True)
            if not ok:
                logger.error("Checking cached resource dependencies failed - %s load (%r) aborted", databaseName, loadType)
                return ok
            # ---
            self.__dmh = DictMethodRunner(dictApi, modulePathMap=modulePathMap, resourceProvider=dmrP)
            locatorObjList = self.__rpP.getLocatorObjList(contentType=databaseName, inputPathList=inputPathList, inputIdCodeList=inputIdCodeList, mergeContentTypes=mergeContentTypes)
            logger.info("Loading database %s (%r) with path length %d", databaseName, loadType, len(locatorObjList))
            #
            if saveInputFileListPath:
                self.__writePathList(saveInputFileListPath, self.__rpP.getLocatorPaths(locatorObjList))
                logger.info("Saving %d paths in %s", len(locatorObjList), saveInputFileListPath)
            #
            filterType = "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates|convert-iterables|normalize-enums|translateXMLCharRefs"
            if styleType in ["columnwise_by_name", "rowwise_no_name"]:
                filterType = "drop-empty-tables|skip-max-width|assign-dates|convert-iterables|normalize-enums|translateXMLCharRefs"
            #
            optD = {}
            optD["databaseName"] = databaseName
            optD["styleType"] = styleType
            optD["filterType"] = filterType
            optD["readBackCheck"] = self.__readBackCheck
            optD["dataSelectors"] = dataSelectors
            optD["loadType"] = loadType
            optD["logSize"] = logSize
            optD["pruneDocumentSize"] = pruneDocumentSize
            optD["useNameFlag"] = useNameFlag
            optD["validationLevel"] = validationLevel
            optD["validateFailures"] = validateFailures
            optD["reloadPartial"] = reloadPartial
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #

            numProc = self.__numProc
            chunkSize = self.__chunkSize if locatorObjList and self.__chunkSize < len(locatorObjList) else 0
            #
            sd, _, fullCollectionNameList, docIndexD = self.__schP.getSchemaInfo(databaseName, dataTyping="ANY")
            collectionNameList = collectionLoadList if collectionLoadList else fullCollectionNameList

            # Move "entry" collection to the end of the list so that if it fails midload, we can determine which entities/assemblies/etc. need reloading based on entry collection
            colL = [c for c in collectionNameList]
            for col in colL:
                if "core_entry" in col.lower():
                    collectionNameList.append(collectionNameList.pop(collectionNameList.index(col)))
            logger.info("COLLECTION NAME LIST: %r", collectionNameList)

            for collectionName in collectionNameList:
                if loadType == "full":
                    self.__removeCollection(databaseName, collectionName)
                    indexDL = docIndexD[collectionName] if collectionName in docIndexD else []
                    bsonSchema = None
                    if validationLevel and validationLevel in ["min", "full"]:
                        bsonSchema = self.__schP.getJsonSchema(databaseName, collectionName, encodingType="BSON", level=validationLevel)
                    ok = self.__createCollection(databaseName, collectionName, indexDL=indexDL, bsonSchema=bsonSchema)
                    logger.debug("Collection create return status %r", ok)
                elif loadType == "replace" and updateSchemaOnReplace:
                    bsonSchema = None
                    if validationLevel and validationLevel in ["min", "full"]:
                        bsonSchema = self.__schP.getJsonSchema(databaseName, collectionName, encodingType="BSON", level=validationLevel)
                    if bsonSchema:
                        ok = self.__updateCollectionSchema(databaseName, collectionName, bsonSchema=bsonSchema)
                        if not ok:
                            logger.info("Schema update failing for %s (%s)", databaseName, collectionName)
            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=filterType)
            optD["schemaDefAccess"] = sd
            optD["dataTransformFactory"] = dtf
            optD["collectionNameList"] = collectionNameList

            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            numPaths = len(locatorObjList)
            logger.debug("Processing %d total paths", numPaths)
            numProc = min(numProc, numPaths)
            maxStepLength = self.__maxStepLength
            if numPaths > maxStepLength:
                numLists = int(numPaths / maxStepLength)
                # JDW always fill numProc
                numLists = max(numLists, numProc)
                subLists = [locatorObjList[i::numLists] for i in range(numLists)]
            else:
                subLists = [locatorObjList]
            #
            if subLists:
                logger.info(
                    "Starting load of %s (%r) using %d processor for total count %d outer subtask count %d subtask length %d",
                    databaseName,
                    loadType,
                    numProc,
                    numPaths,
                    len(subLists),
                    len(subLists[0]),
                )
            else:
                logger.error("Path partitioning fails for %s (%r) using numProc %d", databaseName, loadType, numProc)
            #
            failList = []
            for ii, subList in enumerate(subLists):
                logger.info("Starting outer subtask %d of %d length %d", ii + 1, len(subLists), len(subList))
                #
                # pdbxLoaderWorker = PdbxLoaderWorker(self.__cfgOb, self.__rpP, self.__dmh, self.__resourceName)
                mpu = MultiProcUtil(verbose=True)
                mpu.setWorkingDir(self.__cachePath)
                mpu.setOptions(optionsD=optD)
                mpu.set(workerObj=self, workerMethod="loadWorker")
                ok, failListT, _, _ = mpu.runMulti(dataList=subList, numProc=numProc, numResults=1, chunkSize=chunkSize)
                logger.info("Completed outer subtask %d of %d (status=%r) length %d failures (%d) %r", ii + 1, len(subLists), ok, len(subList), len(failListT), failListT)
                failList.extend(failListT)
            failList = list(set(failList))
            if failList:
                logger.info("Full failed path list %r", failList)
            #
            failedPathList = self.__rpP.getLocatorPaths(failList, locatorIndex=0)
            if failedFilePath and failedPathList:
                wOk = self.__writePathList(failedFilePath, failedPathList)
                logger.info("Writing failure path %s length %d status %r", failedFilePath, len(failList), wOk)
            #
            ok = len(failList) == 0
            self.__end(startTime, "Loading operation completed with status " + str(ok))
            #
            # Create the status objects for the current operations
            # ----
            sFlag = "Y" if ok else "N"
            for collectionName in collectionNameList:
                desp.setStartTime(tS=statusStartTimestamp)
                desp.setObject(databaseName, collectionName)
                desp.setStatus(updateId=None, successFlag=sFlag)
                desp.setEndTime()
                self.__statusList.append(desp.getStatus())
            #
            if ok:
                logger.info("Completed loading %s with status %r loaded %d paths", databaseName, ok, numPaths)
            else:
                logger.info(
                    "Completed loading %s with status %r failure count %d of %d paths: %r",
                    databaseName,
                    ok,
                    len(failList),
                    numPaths,
                    [os.path.basename(pth) for pth in failedPathList],
                )
            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return False

    def getLoadStatus(self):
        return self.__statusList

    def loadWorker(self, dataList, procName, optionsD, workingDir):
        """Multi-proc worker method for MongoDb loading -

        successList, resultList, diagList=workerFunc(runList=nextList,procName, optionsD, workingDir)

        locatorObjList -> containerList -> docList  ->|LOAD|<-  .... return success locatorObjList

        """
        try:
            startTime = self.__begin(message=procName)
            # Recover common options
            styleType = optionsD["styleType"]
            filterType = optionsD["filterType"]
            readBackCheck = optionsD["readBackCheck"]
            logSize = "logSize" in optionsD and optionsD["logSize"]
            dataSelectors = optionsD["dataSelectors"]
            loadType = optionsD["loadType"]
            databaseName = optionsD["databaseName"]
            pruneDocumentSize = optionsD["pruneDocumentSize"]
            sd = optionsD["schemaDefAccess"]
            dtf = optionsD["dataTransformFactory"]
            collectionNameList = optionsD["collectionNameList"]
            useNameFlag = optionsD["useNameFlag"]
            validationLevel = optionsD["validationLevel"]
            validateFailures = optionsD["validateFailures"]
            reloadPartial = optionsD["reloadPartial"]
            #
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=workingDir, verbose=self.__verbose)
            # -------------------------------------------
            # -- Create map of  cIdD{ container identifier} =  locatorObj
            #
            collectionName = None
            cIdD = {}
            cNameL = []
            containerList = []
            for locatorObj in dataList:
                # JDW
                cL = self.__rpP.getContainerList([locatorObj])
                if cL:
                    cNameL.append(cL[0].getName().upper().strip())
                    cId = cL[0].getName() if useNameFlag else cL[0].getProp("uid")
                    cIdD[cId] = locatorObj
                    containerList.extend(cL)
            #
            # -- Apply methods to each container
            for container in containerList:
                if self.__dmh:
                    self.__dmh.apply(container)
                else:
                    logger.debug("%s No dynamic method handler for ", procName)
            # -----
            if loadType != "full":
                for collectionName in collectionNameList:
                    logger.debug("Purging objects from %s for %d containers", collectionName, len(cNameL))
                    ok = self.__purgeDocuments(databaseName, collectionName, cNameL)
                    logger.debug("%s %s - loadType %r cNameL %r (%r)", databaseName, collectionName, loadType, cNameL, ok)
                    # --
            # -----
            failContainerIdS = set()
            rejectContainerIdS = set()
            cardinalIdFailS = set()
            # -----
            for collectionName in collectionNameList:
                ok = True
                # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
                docIdL = sd.getDocumentKeyAttributeNames(collectionName)
                replaceIdL = sd.getDocumentReplaceAttributeNames(collectionName)
                #
                tableIdExcludeList = sd.getCollectionExcluded(collectionName)
                tableIdIncludeList = sd.getCollectionSelected(collectionName)
                sliceFilter = sd.getCollectionSliceFilter(collectionName)
                sdp.setSchemaIdExcludeList(tableIdExcludeList)
                sdp.setSchemaIdIncludeList(tableIdIncludeList)
                #
                logger.debug("%s databaseName %s collectionName %s slice filter %s", procName, databaseName, collectionName, sliceFilter)
                logger.debug("%s databaseName %s include list %r", procName, databaseName, tableIdIncludeList)
                logger.debug("%s databaseName %s exclude list %r", procName, databaseName, tableIdExcludeList)
                #
                dList, containerIdList, rejectIdList = sdp.processDocuments(
                    containerList,
                    styleType=styleType,
                    filterType=filterType,
                    dataSelectors=dataSelectors,
                    sliceFilter=sliceFilter,
                    useNameFlag=useNameFlag,
                    collectionName=collectionName,
                )
                #
                # -- JDWJDW
                # logger.info("loadType %r collectionName %r replaceIdL %r idList %r", loadType, collectionName, replaceIdL, containerIdList)
                # --
                # ------
                # Collect the container identifiers for the rejected containers (paths for logging only)
                # Note that rejections are NOT treated as failures!
                #
                rejectPathList = []
                for cId in rejectIdList:
                    rejectContainerIdS.add(cId)
                    locObj = cIdD[cId]
                    rejectPathList.extend(self.__rpP.getLocatorPaths([locObj], locatorIndex=0))
                rejectPathList = list(set(rejectPathList))
                #
                if logSize:
                    self.__logDocumentSize(procName, dList, docIdL)

                dList = sdp.addDocumentPrivateAttributes(dList, collectionName)
                dList = sdp.addDocumentSubCategoryAggregates(dList, collectionName)
                #
                # --- And after adjustments create index
                #     to map dList -> containerNamList  using dList(uniqId) -> containterName
                #
                indexDoc = {}
                try:
                    for dD, cId in zip(dList, containerIdList):
                        dIdTup = self.__getKeyValues(dD, docIdL)
                        indexDoc[dIdTup] = cId
                except Exception as e:
                    logger.exception("Failing cN %r  dD %r with %s", cId, dD, str(e))

                #
                if dList:
                    ok, _, failDocIdS = self.__loadDocuments(
                        databaseName, collectionName, dList, docIdL, replaceIdL=replaceIdL, loadType=loadType, readBackCheck=readBackCheck, pruneDocumentSize=pruneDocumentSize
                    )
                #
                if failDocIdS:

                    logger.info("Initial load failures: %r", failDocIdS)
                    fList = []
                    for dD in dList:
                        tId = self.__getKeyValues(dD, docIdL)
                        if tId in failDocIdS:
                            fList.append(dD)
                            if validateFailures:
                                logger.info("Validating document %r", tId)
                                self.__validateDocuments(databaseName, collectionName, [dD], docIdL, schemaLevel=validationLevel)
                    #
                    #  -- Try and repair failDocIdS --
                    #
                    if reloadPartial:
                        logger.debug("Attempting corrections on documents %r", failDocIdS)
                        fList = self.__validateAndFix(databaseName, collectionName, fList, docIdL, schemaLevel=validationLevel)

                        fOk, _, failDocIdS = self.__loadDocuments(
                            databaseName, collectionName, fList, docIdL, replaceIdL=replaceIdL, loadType=loadType, readBackCheck=readBackCheck, pruneDocumentSize=pruneDocumentSize
                        )
                        logger.info("Final load (%r) failures: %r", fOk, failDocIdS)

                # ------
                # Collect the container identifiers for the successful loads (paths for logging only)
                #
                failPathList = []
                for dId in failDocIdS:
                    cId = indexDoc[dId]
                    cardinalIdFailS.add(dId[0])
                    failContainerIdS.add(cId)
                    locObj = cIdD[cId]
                    failPathList.extend(self.__rpP.getLocatorPaths([locObj], locatorIndex=0))
                failPathList = list(set(failPathList))
                #
                if failPathList:
                    logger.debug("%s %s/%s worker load failures %r", procName, databaseName, collectionName, [os.path.basename(pth) for pth in failPathList if pth is not None])
                if rejectPathList:
                    logger.debug("%s %s/%s worker load rejected %r", procName, databaseName, collectionName, [os.path.basename(pth) for pth in rejectPathList])
            #
            # -------------------------
            #  failContainerIdS = set()
            #  rejectContainerIdS = set()
            #
            #  cIdD[cId] = locatorObj
            # ----
            retList = [locatorObj for cId, locatorObj in cIdD.items() if cId not in failContainerIdS]
            logger.debug("%s %s load worker returns  successes %d rejects %d failures %d", procName, databaseName, len(retList), len(rejectContainerIdS), len(failContainerIdS))
            #
            if cardinalIdFailS:
                # remove all collection objects related to a load failure
                for collectionName in collectionNameList:
                    logger.info("Purging all objects from %s for failed ids: %r", collectionName, cardinalIdFailS)
                    ok = self.__purgeDocuments(databaseName, collectionName, list(cardinalIdFailS))
            #
            ok = len(failContainerIdS) == 0
            self.__end(startTime, procName + " with status " + str(ok))
            return retList, [], []

        except Exception as e:
            # logger.error("Failing for dataList %r" % dataList)
            logger.exception("Failing with %s", str(e))

        return [], [], []

    # -------------- -------------- -------------- -------------- -------------- -------------- --------------
    #                                        ---  Supporting code follows ---
    #
    def __validateAndFix(self, databaseName, collectionName, dList, docIdL, schemaLevel="full"):
        """[summary]

        Args:
            databaseName ([type]): [description]
            collectionName ([type]): [description]
            dList ([type]): [description]
            docIdL ([type]): [description]
            schemaLevel (str, optional): [description]. Defaults to "full".

        Returns:
            (list):  updated document list (remediated for validation issues)
        """
        #
        rList = []
        logger.info("Validating and fixing objects in databaseName %s collectionName %s numObject %d docIdL %r", databaseName, collectionName, len(dList), docIdL)
        cD = self.__schP.getJsonSchema(databaseName, collectionName, encodingType="JSON", level=schemaLevel)
        # --
        try:
            Draft4Validator.check_schema(cD)
        except Exception as e:
            logger.error("%s %s schema validation fails with %s", databaseName, collectionName, str(e))
        # --
        filterArtifactErrors = True
        valInfo = Draft4Validator(cD, format_checker=FormatChecker())
        for ii, dD in enumerate(dList):
            cN = self.__getKeyValues(dD, docIdL)
            logger.info("Checking %r with schema %s collection %s document (%d)", cN, databaseName, collectionName, ii + 1)
            updL = []
            try:
                for error in sorted(valInfo.iter_errors(dD), key=str):
                    #
                    # Filter and cleanup artifacts -
                    #
                    if filterArtifactErrors and "properties are not allowed ('_id' was unexpected)" in error.message:
                        dD.pop("_id")
                        continue
                    if filterArtifactErrors and "datetime.datetime" in error.message and "is not of type 'string'" in error.message:
                        continue
                    #
                    logger.info("Document issues with schema %s collection %s (%s) path %s error: %s", databaseName, collectionName, cN, error.path, error.message)
                    logger.debug("Failing document %d : %r", ii + 1, list(dD.items()))
                    #
                    pLen = len(error.path)
                    if pLen >= 1:
                        subObjName = str(error.path[0])
                        if subObjName in dD:
                            logger.info("Found subobject %r", subObjName)
                            logger.info("subObject %r", dD[subObjName])
                            dD.pop(subObjName)
                            updL.append(cN)
                    #
            except Exception as e:
                logger.exception("Validation updating processing error %s", str(e))
            #
            if updL:
                rList.append(dD)
        #
        logger.info("Corrected document count (%d) %r", len(updL), updL)
        return rList

    #
    def __validateDocuments(self, databaseName, collectionName, dList, docIdL, schemaLevel="full"):
        #
        logger.info("Validating databaseName %s collectionName %s numObject %d docIdL %r", databaseName, collectionName, len(dList), docIdL)
        eCount = 0
        cD = self.__schP.getJsonSchema(databaseName, collectionName, encodingType="JSON", level=schemaLevel)
        # cD = self.__schP.makeSchema(databaseName, collectionName, encodingType="JSON", level=schemaLevel, saveSchema=True, extraOpts=self.__extraOpts)
        # Raises exceptions for schema compliance.
        try:
            Draft4Validator.check_schema(cD)
        except Exception as e:
            logger.error("%s %s schema validation fails with %s", databaseName, collectionName, str(e))
        #
        filterErrors = True
        valInfo = Draft4Validator(cD, format_checker=FormatChecker())
        logger.info("Validating %d documents from %s %s", len(dList), databaseName, collectionName)
        for ii, dD in enumerate(dList):
            cN = self.__getKeyValues(dD, docIdL)
            logger.info("Checking with schema %s collection %s document (%d) %r", databaseName, collectionName, ii + 1, cN)
            try:
                cCount = 0
                for error in sorted(valInfo.iter_errors(dD), key=str):
                    # filter artifacts -
                    #
                    if filterErrors and "properties are not allowed ('_id' was unexpected)" in error.message:
                        continue
                    if filterErrors and "datetime.datetime" in error.message and "is not of type 'string'" in error.message:
                        continue
                    logger.info("Document issues with schema %s collection %s (%s) path %s error: %s", databaseName, collectionName, cN, error.path, error.message)
                    logger.debug("Failing document %d : %r", ii + 1, list(dD.items()))
                    eCount += 1
                    cCount += 1
                if cCount > 0:
                    logger.info("For schema %s collection %s container %s error count %d", databaseName, collectionName, cN, cCount)
            except Exception as e:
                logger.exception("Validation processing error %s", str(e))
        return eCount

    def __logDocumentSize(self, procName, dList, docIdL):
        maxDocumentMegaBytes = -1
        thresholdMB = 15.8
        # thresholdMB = 5.0
        for tD in dList:
            cN = self.__getKeyValues(tD, docIdL)
            # documentMegaBytes = float(sys.getsizeof(pickle.dumps(tD, protocol=0))) / 1000000.0
            documentMegaBytes = float(sys.getsizeof(bson.BSON.encode(tD))) / 1000000.0
            logger.debug("%s Document %s %.4f MB", procName, cN, documentMegaBytes)
            maxDocumentMegaBytes = max(maxDocumentMegaBytes, documentMegaBytes)
            if documentMegaBytes > thresholdMB:
                logger.info("Large document %r  %.4f MB", cN, documentMegaBytes)
                for ky in tD:
                    try:
                        sMB = float(sys.getsizeof(bson.BSON.encode({"t": tD[ky]}))) / 1000000.0
                    except Exception:
                        sMB = -1
                    logger.info("Sub-document length %s sizeMB %.4f  %8d", ky, sMB, len(tD[ky]))
                #
        logger.info("%s maximum document size loaded %.4f MB", procName, maxDocumentMegaBytes)
        return True

    #

    def __writePathList(self, filePath, pathList):
        try:
            with open(filePath, "w", encoding="utf-8") as ofh:
                for pth in pathList:
                    ofh.write("%s\n" % pth)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __begin(self, message=""):
        startTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting %s at %s", message, ts)
        return startTime

    def __end(self, startTime, message=""):
        endTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        delta = endTime - startTime
        logger.debug("Completed %s at %s (%.4f seconds)", message, ts, delta)

    def __createCollection(self, databaseName, collectionName, indexDL=None, bsonSchema=None):
        """Create database and collection and optionally a set of indices -"""
        try:
            logger.debug("Create database %s collection %s", databaseName, collectionName)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                ok1 = mg.createCollection(databaseName, collectionName, bsonSchema=bsonSchema)
                ok2 = mg.databaseExists(databaseName)
                ok3 = mg.collectionExists(databaseName, collectionName)
                okI = True
                if indexDL:
                    for indexD in indexDL:
                        okI = mg.createIndex(databaseName, collectionName, indexD["ATTRIBUTE_NAMES"], indexName=indexD["INDEX_NAME"], indexType="DESCENDING", uniqueFlag=False)

            return ok1 and ok2 and ok3 and okI
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __updateCollectionSchema(self, databaseName, collectionName, bsonSchema=None, validationLevel="strict", validationAction="error"):
        """Update validation schema for the input collection -"""
        try:
            logger.debug("Updating validatio for schema database %s collection %s", databaseName, collectionName)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                ok1 = mg.databaseExists(databaseName)
                ok2 = mg.collectionExists(databaseName, collectionName)
                if ok1 and ok2:
                    ok3 = mg.updateCollection(databaseName, collectionName, bsonSchema=bsonSchema, validationLevel=validationLevel, validationAction=validationAction)
                    logger.info("Updated %r %r validation schema", databaseName, collectionName)
            return ok1 and ok2 and ok3
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __removeCollection(self, databaseName, collectionName):
        """Drop collection within database"""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                logger.debug("Remove collection database %s collection %s", databaseName, collectionName)
                logger.debug("Starting databases = %r", mg.getDatabaseNames())
                logger.debug("Starting collections = %r", mg.getCollectionNames(databaseName))
                ok = mg.dropCollection(databaseName, collectionName)
                logger.debug("Databases = %r", mg.getDatabaseNames())
                logger.debug("Post drop collections = %r", mg.getCollectionNames(databaseName))
                ok = mg.collectionExists(databaseName, collectionName)
                logger.debug("Post drop collections = %r", mg.getCollectionNames(databaseName))
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __purgeDocuments(self, databaseName, collectionName, cardinalIdL):
        """Purge documents from collection within database with cardinal identifiers in cardinalIdL."""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                for cardId in cardinalIdL:
                    selectD = {"rcsb_id": {"$regex": "^%s" % cardId.upper(), "$options": "i"}}  # case-insensitive
                    dCount = mg.delete(databaseName, collectionName, selectD)
                    logger.debug("Remove %d objects in database %s collection %s selection %r", dCount, databaseName, collectionName, selectD)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __pruneBySize(self, dList, limitMB=15.9):
        """For the input list of objects (dictionaries).objects
        Return a pruned list satisfying the input total object size limit -

        """
        oL = []
        try:
            for dD in dList:
                sD = {}
                sumMB = 0.0
                for ky in dD:
                    dMB = 0.0
                    try:
                        dMB = float(sys.getsizeof(bson.BSON.encode({ky: dD[ky]}))) / 1000000.0
                    except Exception as e:
                        logger.error("ky %r d[ky] %r with %s", ky, dD[ky], str(e))

                    sumMB += dMB
                    sD[ky] = dMB
                if sumMB < limitMB:
                    oL.append(dD)
                    continue
                #
                sortedSd = sorted(sD.items(), key=operator.itemgetter(1))
                prunedSum = 0.0
                for ky, sMB in sortedSd:
                    prunedSum += sMB
                    if prunedSum > limitMB:
                        dD.pop(ky, None)
                        logger.debug("Pruning ky %s size(MB) %.2f", ky, sMB)
                oL.append(dD)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            #
        logger.debug("Pruning returns document list length %d", len(dList))
        return oL

    def __loadDocuments(self, databaseName, collectionName, dList, docIdL, replaceIdL=None, loadType="full", readBackCheck=False, pruneDocumentSize=None):
        #
        # Load database/collection with input document list -
        #
        rIdL = []

        logger.debug("databaseName %s collectionName %s docIdL %r", databaseName, collectionName, docIdL)
        inputDocIdS = {self.__getKeyValues(dD, docIdL) for dD in dList}
        failDocIdS = set()
        successDocIdS = set()

        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                if loadType == "replace" and replaceIdL:
                    deleteTupL = mg.deleteList(databaseName, collectionName, dList, replaceIdL)
                    logger.debug("Deleted document status %r", deleteTupL)
                if pruneDocumentSize:
                    dList = self.__pruneBySize(dList, limitMB=pruneDocumentSize)
                #
                rIdL.extend(mg.insertList(databaseName, collectionName, dList, keyNames=docIdL, salvage=True))
                # ---
                #  If there is a failure then determine the specific successes and failures -
                #
                successDocIdS = inputDocIdS
                if len(rIdL) != len(dList):
                    sIdS = set()
                    try:
                        for rId in rIdL:
                            rObj = mg.fetchOne(databaseName, collectionName, "_id", rId)
                            dIdTup = self.__getKeyValues(rObj, docIdL)
                            sIdS.add(dIdTup)
                    except Exception as e:
                        logger.exception("Failing with %s", str(e))
                    successDocIdS = sIdS
                # enumerate the failures
                failDocIdS = inputDocIdS - successDocIdS
                #

                #
                if readBackCheck:
                    # build an index of the input document list
                    indD = {}
                    try:
                        for ii, dD in enumerate(dList):
                            dIdTup = self.__getKeyValues(dD, docIdL)
                            indD[dIdTup] = ii
                    except Exception as e:
                        logger.exception("Failing ii %d d %r with %s", ii, dD, str(e))
                    #
                    # Note that objects in dList are mutated by the insert operation with the additional key '_id',
                    # hence, it is possible to compare the fetched object with the input object.
                    #
                    rbStatus = True
                    for ii, rId in enumerate(rIdL):
                        rObj = mg.fetchOne(databaseName, collectionName, "_id", rId)
                        dIdTup = self.__getKeyValues(rObj, docIdL)
                        jj = indD[dIdTup]
                        if rObj != dList[jj]:
                            rbStatus = False
                            break
                #
                if readBackCheck and not rbStatus:
                    return False, successDocIdS, failDocIdS
                #
            return len(rIdL) == len(dList), successDocIdS, failDocIdS
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return False, [], inputDocIdS

    def __getKeyValues(self, dct, keyNames):
        """Return the tuple of values corresponding to the input dictionary of key names expressed in dot notation.

        Args:
            dct (dict): source dictionary object (nested)
            keyNames (list): list of dictionary keys in dot notation

        Returns:
            tuple: tuple of values corresponding to the input key names

        """
        rL = []
        try:
            for keyName in keyNames:
                rL.append(self.__getKeyValue(dct, keyName))
        except Exception as e:
            logger.exception("Failing for key names %r with %s", keyNames, str(e))

        return tuple(rL)

    def __getKeyValue(self, dct, keyName):
        """Return the value of the corresponding key expressed in dot notation in the input dictionary object (nested)."""
        try:
            kys = keyName.split(".")
            for key in kys:
                try:
                    dct = dct[key]
                except Exception:
                    logger.warning("Missing document key %r in %r", key, list(dct.keys()))
                    return None
            return dct
        except Exception as e:
            logger.error("Failing for key %r with %s", keyName, str(e))

        return None

    def removeAndRecreateDbCollections(self, databaseName, collectionLoadList=None, validationLevel="min"):
        """Remove and recreate collections for input database.

        Args:
            databaseName (str): A content datbase schema (e.g. 'bird','bird_family','bird_chem_comp', chem_comp', 'pdbx', 'pdbx_core')
            collectionLoadList (list, optional): list of collection names in this schema to load (default is load all collections)
            validationLevel (str, optional): Completeness of json/bson metadata schema bound to each collection (e.g. 'min', 'full' or None)
        Returns:
            bool: True on success or False otherwise

        """
        try:
            logger.info("Beginning wiping of database %s", databaseName)

            _, _, fullCollectionNameList, docIndexD = self.__schP.getSchemaInfo(databaseName, dataTyping="ANY")
            collectionNameList = collectionLoadList if collectionLoadList else fullCollectionNameList

            for collectionName in collectionNameList:
                logger.info("Removing and recreating database %s collection %s", databaseName, collectionName)
                self.__removeCollection(databaseName, collectionName)
                indexDL = docIndexD[collectionName] if collectionName in docIndexD else []
                bsonSchema = None
                if validationLevel and validationLevel in ["min", "full"]:
                    bsonSchema = self.__schP.getJsonSchema(databaseName, collectionName, encodingType="BSON", level=validationLevel)
                ok = self.__createCollection(databaseName, collectionName, indexDL=indexDL, bsonSchema=bsonSchema)
                logger.debug("Collection create return status %r", ok)
            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return False
