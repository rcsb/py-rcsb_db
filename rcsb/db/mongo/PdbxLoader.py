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
#                      in __createCollection(self, dbName, collectionName, indexAttributeNames=None) make indexAttributeNames a list
#
##
"""
Worker methods for loading PDBx resident data sets (e.g., BIRD, CCD and PDBx/mmCIF data files)
following mapping conventions in external schema definitions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import operator
import sys
import time

import bson

from rcsb.db.define.DictMethodRunner import DictMethodRunner
from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.processors.DataTransformFactory import DataTransformFactory
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

logger = logging.getLogger(__name__)


class PdbxLoader(object):

    def __init__(self, cfgOb, resourceName="MONGO_DB", numProc=4, chunkSize=15, fileLimit=None, verbose=False, readBackCheck=False, maxStepLength=2000, workPath=None):
        self.__verbose = verbose
        #
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__fileLimit = fileLimit
        self.__maxStepLength = maxStepLength
        #
        # Controls for multiprocessing execution -
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        #
        self.__cfgOb = cfgOb
        self.__resourceName = resourceName
        #
        self.__readBackCheck = readBackCheck
        self.__workPath = workPath
        self.__mpFormat = '[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s'
        #
        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit)
        #
        self.__statusList = []
        #
        # PDBX_DICT_LOCATOR=dictionaries/mmcif_pdbx_v5_next.dic
        # RCSB_DICT_LOCATOR=dictionaries/rcsb_mmcif_ext_v1.dic
        # DICT_METHOD_HELPER_MODULE=rcsb.db.helpers.DictMethodRunnerHelper
        #
        sectionName = 'DEFAULT'
        pathPdbxDictionaryFile = self.__cfgOb.getPath('PDBX_DICT_LOCATOR', sectionName=sectionName)
        pathRcsbDictionaryFile = self.__cfgOb.getPath('RCSB_DICT_LOCATOR', sectionName=sectionName)
        dH = self.__cfgOb.getHelper('DICT_METHOD_HELPER_MODULE', sectionName=sectionName)
        self.__dmh = DictMethodRunner(dictLocators=[pathPdbxDictionaryFile, pathRcsbDictionaryFile], methodHelper=dH)

    def load(self, schemaName, collectionLoadList=None, loadType='full', inputPathList=None, styleType='rowwise_by_name', dataSelectors=None,
             failedFilePath=None, saveInputFileListPath=None, pruneDocumentSize=None, locatorKey='rcsb_load_status.locator'):
        """Driver method for loading PDBx/mmCIF content into document store.

        Args:
            schemaName (str): A content schema (e.g. 'bird','bird_family','bird_chem_comp', chem_comp', 'pdbx', 'pdbx_core')
            collectionNameList (list, optional):  list of collection names in this schema to load (default is load all collections)
            loadType (str, optional): mode of loading 'full' (bulk delete then bulk insert) or 'replace'
            inputPathList (list, optional): Data file path list (if not provided the full repository will be scanned)
            styleType (str, optional): one of 'rowwise_by_name', 'columnwise_by_name', 'rowwise_no_name', 'rowwise_by_name_with_cardinality'
            dataSelectors (list, optional): selector names defined for this schema (e.g. PUBLIC_RELEASE)
            failedFilePath (str, optional): Path to hold file paths for load failures
            saveInputFileListPath (list, optional): List of files
            pruneDocumentSize (bool, optional): iteratively remove large elements from a collection to satisfy size limits
            locatorKey (str, optional): Key identifier in document content storing the data file path (url)

        Returns:
            bool: True on success or False otherwise

        """
        try:
            #
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            #
            startTime = self.__begin(message="loading operation")
            #
            pathList = self.__schU.getPathList(schemaName=schemaName, inputPathList=inputPathList)
            logger.debug("Path list length %d" % len(pathList))
            #
            if saveInputFileListPath:
                self.__writePathList(saveInputFileListPath, pathList)
                logger.info("Saving %d paths in %s" % (len(pathList), saveInputFileListPath))
            #
            filterType = "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates|convert-iterables"
            if styleType in ["columnwise_by_name", "rowwise_no_name"]:
                filterType = "drop-empty-tables|skip-max-width|assign-dates|convert-iterables"
            #
            optD = {}
            optD['schemaName'] = schemaName
            optD['styleType'] = styleType
            optD['filterType'] = filterType
            optD['readBackCheck'] = self.__readBackCheck
            optD['logSize'] = self.__verbose
            optD['dataSelectors'] = dataSelectors
            optD['loadType'] = loadType
            optD['pruneDocumentSize'] = pruneDocumentSize
            optD['locatorKey'] = locatorKey
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #

            numProc = self.__numProc
            chunkSize = self.__chunkSize if pathList and self.__chunkSize < len(pathList) else 0
            #
            sd, dbName, fullCollectionNameList, primaryIndexD = self.__schU.getSchemaInfo(schemaName)

            collectionNameList = collectionLoadList if collectionLoadList else fullCollectionNameList

            for collectionName in collectionNameList:
                if loadType == 'full':
                    self.__removeCollection(dbName, collectionName)
                    indAtL = primaryIndexD[collectionName] if collectionName in primaryIndexD else None
                    ok = self.__createCollection(dbName, collectionName, indAtL)
                    logger.debug("Collection create status %r" % ok)

            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=filterType)
            optD['schemaDefAccess'] = sd
            optD['dataTransformFactory'] = dtf
            optD['collectionNameList'] = collectionNameList
            optD['dbName'] = dbName
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            numPaths = len(pathList)
            logger.debug("Processing %d total paths" % numPaths)
            numProc = min(numProc, numPaths)
            maxStepLength = self.__maxStepLength
            if numPaths > maxStepLength:
                numLists = int(numPaths / maxStepLength)
                subLists = [pathList[i::numLists] for i in range(numLists)]
            else:
                subLists = [pathList]
            #
            if subLists and len(subLists) > 0:
                logger.info("Starting loadType %s with numProc %d outer subtask count %d subtask length ~ %d" % (loadType, numProc, len(subLists), len(subLists[0])))
            #
            failList = []
            retLists = []
            diagList = []
            for ii, subList in enumerate(subLists):
                logger.info("Running outer subtask %d of %d length %d" % (ii + 1, len(subLists), len(subList)))
                #
                mpu = MultiProcUtil(verbose=True)
                mpu.setWorkingDir(self.__workPath)
                mpu.setOptions(optionsD=optD)
                mpu.set(workerObj=self, workerMethod="loadWorker")
                ok, failListT, retListsT, diagListT = mpu.runMulti(dataList=subList, numProc=numProc, numResults=1, chunkSize=chunkSize)
                failList.extend(failListT)
                retLists.extend(retListsT)
                diagList.extend(diagListT)
            logger.debug("Failing path list %r" % failList)
            logger.info("Load count success %d - failed count %d" % (len(pathList), len(failList)))
            #
            if failedFilePath and len(failList) > 0:
                wOk = self.__writePathList(failedFilePath, failList)
                logger.info("Writing failure path %s status %r" % (failedFilePath, wOk))
            #
            ok = len(failList) == 0
            self.__end(startTime, "Loading operation with status " + str(ok))
            #
            # Create the status objects for the current operations
            # ----
            sFlag = 'Y' if ok else 'N'
            for collectionName in collectionNameList:
                desp.setStartTime(tS=statusStartTimestamp)
                desp.setObject(dbName, collectionName)
                desp.setStatus(updateId=None, successFlag=sFlag)
                desp.setEndTime()
                self.__statusList.append(desp.getStatus())
            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def getLoadStatus(self):
        return self.__statusList

    def loadWorker(self, dataList, procName, optionsD, workingDir):
        """ Multi-proc worker method for MongoDb loading -
        """
        try:
            startTime = self.__begin(message=procName)
            # Recover common options
            styleType = optionsD['styleType']
            filterType = optionsD['filterType']
            readBackCheck = optionsD['readBackCheck']
            logSize = 'logSize' in optionsD and optionsD['logSize']
            dataSelectors = optionsD['dataSelectors']
            loadType = optionsD['loadType']
            schemaName = optionsD['schemaName']
            pruneDocumentSize = optionsD['pruneDocumentSize']
            locatorKey = optionsD['locatorKey']
            #
            sd = optionsD['schemaDefAccess']
            dtf = optionsD['dataTransformFactory']
            collectionNameList = optionsD['collectionNameList']
            dbName = optionsD['dbName']
            #
            fType = "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates|convert-iterables"
            if styleType in ["columnwise_by_name", "rowwise_no_name"]:
                fType = "drop-empty-tables|skip-max-width|assign-dates|convert-iterables"
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=workingDir, verbose=self.__verbose)
            containerList = sdp.getContainerList(dataList, filterType=filterType)
            #
            # Apply dynamic methods here -
            #
            for container in containerList:
                self.__dmh.apply(container)

            logger.debug("%s container count %d" % (procName, len(containerList)))
            #
            logger.debug("%s schemaName %s dbName %s collectionNameList %s pathlist %d containerList %d" %
                         (procName, schemaName, dbName, collectionNameList, len(dataList), len(containerList)))
            fullSuccessPathList = []
            fullFailedPathList = []
            for collectionName in collectionNameList:
                successPathList = []
                failedPathList = []
                ok = True
                # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
                tableIdExcludeList = sd.getCollectionExcluded(collectionName)
                tableIdIncludeList = sd.getCollectionSelected(collectionName)
                sliceFilter = sd.getCollectionSliceFilter(collectionName)
                sdp.setSchemaIdExcludeList(tableIdExcludeList)
                sdp.setSchemaIdIncludeList(tableIdIncludeList)
                #
                logger.debug("%s schemaName %s dbName %s collectionName %s slice filter %s" % (procName, schemaName, dbName, collectionName, sliceFilter))
                logger.debug("%s schemaName %s include list %r" % (procName, schemaName, tableIdIncludeList))
                logger.debug("%s schemaName %s exclude list %r" % (procName, schemaName, tableIdExcludeList))

                #
                tableDataDictList, containerNameList, rejectPathList = sdp.processDocuments(containerList, styleType=styleType, filterType=fType,
                                                                                            dataSelectors=dataSelectors, sliceFilter=sliceFilter)
                #
                # Get the unique paths for the rejected  container list -
                #
                rejectPathList = list(set(rejectPathList))
                #
                logger.debug("%s schemaName %s dbName %s collectionName %s slice filter %s num containers %d len data %d num rejects %d" %
                             (procName, schemaName, dbName, collectionName, sliceFilter, len(containerNameList), len(tableDataDictList), len(rejectPathList)))
                #
                if logSize:
                    maxDocumentMegaBytes = -1
                    for tD, cN in zip(tableDataDictList, containerNameList):
                        # documentMegaBytes = float(sys.getsizeof(pickle.dumps(tD, protocol=0))) / 1000000.0
                        documentMegaBytes = float(sys.getsizeof(bson.BSON.encode(tD))) / 1000000.0
                        logger.debug("%s Document %s  %.4f MB" % (procName, cN, documentMegaBytes))
                        maxDocumentMegaBytes = max(maxDocumentMegaBytes, documentMegaBytes)
                        if documentMegaBytes > 15.8:
                            logger.info("Large document %s  %.4f MB" % (cN, documentMegaBytes))
                    logger.debug("%s maximum document size loaded %.4f MB" % (procName, maxDocumentMegaBytes))
                #
                #  Get the [scbemaId.atId,...] holding the natural document Id
                #
                docIdL = sd.getDocumentKeyAttributeNames(collectionName)
                logger.debug("%s docIdL %r collectionName %r" % (procName, docIdL, collectionName))
                #
                if tableDataDictList:
                    ok, successPathList, failedPathList = self.__loadDocuments(dbName, collectionName, tableDataDictList, docIdL,
                                                                               loadType=loadType, locatorKey=locatorKey,
                                                                               readBackCheck=readBackCheck, pruneDocumentSize=pruneDocumentSize)
                #
                logger.debug("%s %s/%s inputList %d successes %d  failed %d rejected %d" %
                             (procName, dbName, collectionName, len(tableDataDictList), len(successPathList), len(failedPathList), len(rejectPathList)))
                #
                successPathList.extend(rejectPathList)
                fullSuccessPathList.extend(successPathList)
                fullFailedPathList.extend(failedPathList)
            #
            logger.debug("fullSuccessPathList %r" % fullSuccessPathList)
            logger.debug("fullFailedPathList  %r" % fullFailedPathList)
            retList = list(set(fullSuccessPathList) - set(fullFailedPathList))
            logger.debug("%s %s %r full success %s full fails %d " % (procName, dbName, collectionNameList,
                                                                      len(set(fullSuccessPathList)), len(set(fullFailedPathList))))
            self.__end(startTime, procName + " with status " + str(ok))
            return retList, [], []

        except Exception as e:
            #logger.error("Failing for dataList %r" % dataList)
            logger.exception("Failing with %s" % str(e))

        return [], [], []

    # -------------- -------------- -------------- -------------- -------------- -------------- --------------
    #                                        ---  Supporting code follows ---
    #
    def __writePathList(self, filePath, pathList):
        try:
            with open(filePath, 'w') as ofh:
                for pth in pathList:
                    ofh.write("%s\n" % pth)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __begin(self, message=""):
        startTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting %s at %s" % (message, ts))
        return startTime

    def __end(self, startTime, message=""):
        endTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        delta = endTime - startTime
        logger.debug("Completed %s at %s (%.4f seconds)" % (message, ts, delta))

    def __createCollection(self, dbName, collectionName, indexAttributeNames=None):
        """Create database and collection and optionally a primary index -
        """
        try:
            logger.debug("Create database %s collection %s" % (dbName, collectionName))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                ok1 = mg.createCollection(dbName, collectionName)
                ok2 = mg.databaseExists(dbName)
                ok3 = mg.collectionExists(dbName, collectionName)
                okI = True
                if indexAttributeNames:
                    okI = mg.createIndex(dbName, collectionName, indexAttributeNames, indexName="primary", indexType="DESCENDING", uniqueFlag=False)

            return ok1 and ok2 and ok3 and okI
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __removeCollection(self, dbName, collectionName):
        """Drop collection within database

        """
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                logger.debug("Remove collection database %s collection %s" % (dbName, collectionName))
                logger.debug("Starting databases = %r" % mg.getDatabaseNames())
                logger.debug("Starting collections = %r" % mg.getCollectionNames(dbName))
                ok = mg.dropCollection(dbName, collectionName)
                logger.debug("Databases = %r" % mg.getDatabaseNames())
                logger.debug("Post drop collections = %r" % mg.getCollectionNames(dbName))
                ok = mg.collectionExists(dbName, collectionName)
                logger.debug("Post drop collections = %r" % mg.getCollectionNames(dbName))
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __pruneBySize(self, dList, limitMB=15.9):
        """ For the input list of objects (dictionaries).objects
            Return a pruned list satisfying the input total object size limit -

        """
        oL = []
        try:
            for d in dList:
                sD = {}
                sumMB = 0.0
                for ky in d:
                    dMB = float(sys.getsizeof(bson.BSON.encode({ky: d[ky]}))) / 1000000.0
                    sumMB += dMB
                    sD[ky] = dMB
                if sumMB < limitMB:
                    oL.append(d)
                    continue
                #
                sorted_sD = sorted(sD.items(), key=operator.itemgetter(1))
                prunedSum = 0.0
                for ky, sMB in sorted_sD:
                    prunedSum += sMB
                    if prunedSum > limitMB:
                        d.pop(ky, None)
                        logger.debug("Pruning ky %s size(MB) %.2f" % (ky, sMB))
                oL.append(d)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            #
        logger.debug("Pruning returns document list length %d" % len(dList))
        return oL

    def __loadDocuments(self, dbName, collectionName, dList, docIdL, loadType='full', locatorKey=None, readBackCheck=False, pruneDocumentSize=None):
        #
        # Load database/collection with input document list -
        #
        rIdL = []
        indD = {}
        failList = []
        fullSuccessValueList = [self.__getKeyValue(d, locatorKey) for d in dList]
        logger.debug("fullSuccessValueList length %d" % len(fullSuccessValueList))

        logger.debug("dbName %s collectionName %s docIdL %r locatorKey %r" % (dbName, collectionName, docIdL, locatorKey))
        #
        try:
            for ii, d in enumerate(dList):
                dIdTup = self.__getKeyValues(d, docIdL)
                indD[dIdTup] = ii
        except Exception as e:
            logger.exception("Failing ii %d d %r with %s" % (ii, d, str(e)))

        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                keyNames = docIdL
                if loadType == 'replace':
                    dTupL = mg.deleteList(dbName, collectionName, dList, keyNames)
                    logger.debug("Deleted document status %r" % dTupL)
                if pruneDocumentSize:
                    dList = self.__pruneBySize(dList, limitMB=pruneDocumentSize)
                #
                rIdL.extend(mg.insertList(dbName, collectionName, dList, keyNames=keyNames, salvage=True))
                logger.debug("-- InsertList returns rIdL length %r or %r" % (len(rIdL), len(dList)))
                # ---
                #  If there is a failure then determine the specific successes and failures -
                #
                successList = fullSuccessValueList
                if len(rIdL) != len(dList):
                    try:
                        sList = []
                        for rId in rIdL:
                            #
                            rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                            dIdTup = self.__getKeyValues(rObj, docIdL)
                            jj = indD[dIdTup]
                            sList.append(self.__getKeyValue(dList[jj], locatorKey))
                        #
                        failList = list(set(fullSuccessValueList) - set(sList))
                        successList = list(set(sList))
                    except Exception as e:
                        logger.exception("Failing with %s" % str(e))
                #
                if readBackCheck:
                    #
                    # Note that objects in dList are mutated by the insert operation with the additional key '_id',
                    # hence, it is possible to compare the fetched object with the input object.
                    #
                    rbStatus = True
                    for ii, rId in enumerate(rIdL):
                        rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                        dIdTup = self.__getKeyValues(rObj, docIdL)
                        jj = indD[dIdTup]
                        if (rObj != dList[jj]):
                            rbStatus = False
                            break
                #
                if readBackCheck and not rbStatus:
                    return False, successList, failList
                #
            return len(rIdL) == len(dList), successList, failList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False, [], fullSuccessValueList

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
            logger.exception("Failing for key names %r with %s" % (keyNames, str(e)))

        return tuple(rL)

    def __getKeyValue(self, dct, keyName):
        """  Return the value of the corresponding key expressed in dot notation in the input dictionary object (nested).
        """
        try:
            kys = keyName.split('.')
            for key in kys:
                try:
                    dct = dct[key]
                except KeyError:
                    return None
            return dct
        except Exception as e:
            logger.exception("Failing for key %r with %s" % (keyName, str(e)))

        return None
