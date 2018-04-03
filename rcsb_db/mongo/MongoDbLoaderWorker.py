##
# File:    MongoDbLoaderWorker.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#     20-Mar-2018 jdw  adding prdcc within chemical component collection
#     21-Mar-2018 jdw  content filtering options added from documents
#     25-Mar-2018 jdw  add support for loading multiple collections for a content type.
#     26-Mar-2018 jdw  improve how successful loads are tracked accross collections -
##
"""
Worker methods for loading MongoDb using BIRD, CCD and PDBx/mmCIF data files
and following external schema definitions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import time
import bson

import logging
logger = logging.getLogger(__name__)


from rcsb_db.loaders.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef
from rcsb_db.schema.ChemCompSchemaDef import ChemCompSchemaDef
from rcsb_db.schema.PdbxSchemaDef import PdbxSchemaDef
from rcsb_db.utils.MultiProcUtil import MultiProcUtil
from rcsb_db.utils.RepoPathUtil import RepoPathUtil

from rcsb_db.mongo.Connection import Connection
from rcsb_db.mongo.MongoDbUtil import MongoDbUtil


class MongoDbLoaderWorker(object):

    def __init__(self, cfgOb, resourceName="MONGO_DB", numProc=4, chunkSize=15, fileLimit=None, mockTopPath=None, verbose=False, readBackCheck=False):
        self.__verbose = verbose
        #
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__fileLimit = fileLimit
        #
        # Controls for multiprocessing execution -
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        #
        self.__cfgOb = cfgOb
        self.__resourceName = resourceName
        #
        self.__readBackCheck = readBackCheck
        self.__mockTopPath = mockTopPath

    def loadContentType(self, contentType, loadType='full', inputPathList=None, styleType='rowwise_by_name', documentSelectors=None,
                        failedFilePath=None, saveInputFileListPath=None):
        """  Driver method for loading MongoDb content -

            contentType:  one of 'bird','bird_family','bird_chem_comp', chem_comp','pdbx'
            #
            loadType:     "full" or "replace"
            styleType:    one of 'rowwise_by_name', 'columnwise_by_name', 'rowwise_no_name', 'rowwise_by_name_with_cardinality'

        """
        try:
            startTime = self.__begin(message="loading operation")
            #
            pathList = self.__getPathInfo(contentType, inputPathList=inputPathList)
            #
            if saveInputFileListPath:
                self.__writePathList(saveInputFileListPath, pathList)
                logger.info("Saving %d paths in %s" % (len(pathList), saveInputFileListPath))
            #
            optD = {}
            optD['contentType'] = contentType
            optD['styleType'] = styleType
            optD['readBackCheck'] = self.__readBackCheck
            optD['logSize'] = self.__verbose
            optD['documentSelectors'] = documentSelectors
            optD['loadType'] = loadType
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #

            numProc = self.__numProc
            chunkSize = self.__chunkSize if inputPathList and self.__chunkSize < len(inputPathList) else 0
            #
            _, dbName, collectionNameList, primaryIndexD = self.__getSchemaInfo(contentType)
            for collectionName in collectionNameList:
                if loadType == 'full':
                    self.__removeCollection(dbName, collectionName)
                    indAt = primaryIndexD[collectionName] if collectionName in primaryIndexD else None
                    self.__createCollection(dbName, collectionName, indAt)

            #
            mpu = MultiProcUtil(verbose=True)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="loadWorker")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=pathList, numProc=numProc, numResults=1, chunkSize=chunkSize)
            logger.debug("Failing path list %r" % failList)
            logger.info("Load path list length %d failed load list length %d" % (len(pathList), len(failList)))
            #
            if failedFilePath and len(failList) > 0:
                wOk = self.__writePathList(failedFilePath, failList)
                logger.info("Writing load failure path list to %s status %r" % (failedFilePath, wOk))
            #
            self.__end(startTime, "loading operation with status " + str(ok))

            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def loadWorker(self, dataList, procName, optionsD, workingDir):
        """ Multi-proc worker method for MongoDb loading -
        """
        try:
            startTime = self.__begin(message=procName)
            # Recover common options
            styleType = optionsD['styleType']
            readBackCheck = optionsD['readBackCheck']
            logSize = 'logSize' in optionsD and optionsD['logSize']
            documentSelectors = optionsD['documentSelectors']
            loadType = optionsD['loadType']
            contentType = optionsD['contentType']
            #
            fType = "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates|convert-iterables"
            if styleType in ["columnwise_by_name", "rowwise_no_name"]:
                fType = "drop-empty-tables|skip-max-width|assign-dates|convert-iterables"
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #
            sd, dbName, collectionNameList, _ = self.__getSchemaInfo(contentType)
            sdp = SchemaDefDataPrep(schemaDefObj=sd, verbose=self.__verbose)
            containerList = sdp.getContainerList(dataList, filterType=fType)
            #
            logger.debug("%s contentType %s dbName %s collectionNameList %s pathlist length %d containerList length %d" %
                         (procName, contentType, dbName, collectionNameList, len(dataList), len(containerList)))
            fullSuccessPathList = []
            fullFailedPathList = []
            for collectionName in collectionNameList:
                # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
                tableIdExcludeList = sd.getCollectionExcludedTables(collectionName)
                tableIdIncludeList = sd.getCollectionSelectedTables(collectionName)
                sdp.setTableIdExcludeList(tableIdExcludeList)
                sdp.setTableIdIncludeList(tableIdIncludeList)
                #
                logger.debug("%s contentType %s dbName %s collectionName %s" % (procName, contentType, dbName, collectionName))
                logger.debug("%s contentType %s include List %r" % (procName, contentType, tableIdIncludeList))
                logger.debug("%s contentType %s exclude List %r" % (procName, contentType, tableIdExcludeList))
                #
                tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType=styleType, filterType=fType, documentSelectors=documentSelectors)
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
                    logger.info("%s maximum document size loaded %.4f MB" % (procName, maxDocumentMegaBytes))
                #
                #  Get the tableId.attId holding the natural document Id
                docIdD = {}
                docIdD['tableName'], docIdD['attributeName'] = sd.getDocumentKeyAttributeName(collectionName)
                logger.debug("%s docIdD %r collectionName %r" % (procName, docIdD, collectionName))
                #
                ok, successPathList, failedPathList = self.__loadDocuments(dbName, collectionName, tableDataDictList, docIdD,
                                                                           loadType=loadType, successKey='__load_status__.load_file_path', readBackCheck=readBackCheck)
                #
                logger.info("%s database %s collection %s successList length = %d  failed %d rejected %d" %
                            (procName, dbName, collectionName, len(successPathList), len(failedPathList), len(rejectList)))
                #
                successPathList.extend(rejectList)
                fullSuccessPathList.extend(successPathList)
                fullFailedPathList.extend(failedPathList)
            #
            retList = list(set(fullSuccessPathList) - set(fullFailedPathList))
            self.__end(startTime, procName + " with status " + str(ok))
            return retList, [], []

        except Exception as e:
            logger.error("Failing with dataList %r" % dataList)
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
        logger.info("Starting %s at %s" % (message, ts))
        return startTime

    def __end(self, startTime, message=""):
        endTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        delta = endTime - startTime
        logger.info("Completed %s at %s (%.4f seconds)" % (message, ts, delta))

    def __createCollection(self, dbName, collectionName, indexAttributeName=None):
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
                if indexAttributeName:
                    okI = mg.createIndex(dbName, collectionName, [indexAttributeName], indexName="primary", indexType="DESCENDING", uniqueFlag=False)

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

    def __loadDocuments(self, dbName, collectionName, dList, docIdD, loadType='full', successKey=None, readBackCheck=False):
        #
        # Load database/collection with input document list -
        #
        indD = {}
        failList = []
        fullSuccessValueList = [self.__dictGet(d, successKey) for d in dList]
        logger.debug("fullSuccessValueList length %d" % len(fullSuccessValueList))
        #
        try:
            for ii, d in enumerate(dList):
                tn = docIdD['tableName']
                an = docIdD['attributeName']
                # logger.debug("++++++++++ ---------- tn %s an %s" % (tn, an))
                dId = d[tn][an]
                indD[dId] = ii
        except Exception as e:
            logger.exception("Failing ii %d d %r with %s" % (ii, d, str(e)))

        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                keyName = docIdD['tableName'] + '.' + docIdD['attributeName']
                if loadType == 'replace':
                    dTupL = mg.deleteList(dbName, collectionName, dList, keyName)
                    logger.debug("Deleted document status %r" % dTupL)

                rIdL = mg.insertList(dbName, collectionName, dList, keyName)
                logger.debug("Insert returns rIdL length %r" % len(rIdL))

                # ---
                #  If there is a failure then determine the specific successes and failures -
                #
                successList = fullSuccessValueList
                if len(rIdL) != len(dList):
                    sList = []
                    for rId in rIdL:
                        rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                        docId = rObj[docIdD['tableName']][docIdD['attributeName']]
                        jj = indD[docId]
                        sList.append(self.__dictGet(dList[jj], successKey))
                    #
                    failList = list(set(fullSuccessValueList) - set(sList))
                    successList = list(set(sList))
                #
                if readBackCheck:
                    #
                    # Note that objects in dList are mutated by additional key '_id' that is added on insert -
                    #
                    rbStatus = True
                    for ii, rId in enumerate(rIdL):
                        rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                        docId = rObj[docIdD['tableName']][docIdD['attributeName']]
                        jj = indD[docId]
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

    def __getPathInfo(self, contentType, inputPathList=None):
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
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        if self.__fileLimit:
            inputPathList = inputPathList[:self.__fileLimit]

        return outputPathList

    def __getSchemaInfo(self, contentType):
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

    def __dictGet(self, dct, dotNotation):
        """  Convert input dictionary key (dot notation) to divided Python format and return appropriate dictionary value.
        """
        key = None
        try:
            kys = dotNotation.split('.')
            for key in kys:
                try:
                    dct = dct[key]
                except KeyError:
                    return None
            return dct
        except Exception as e:
            logger.exception("Failing dotNotation %s key %r with %s" % (dotNotation, key, str(e)))

        return None
