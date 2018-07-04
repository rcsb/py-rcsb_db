##
# File:    DocumentLoader.py
# Author:  J. Westbrook
# Date:    24-Jun-2018
# Version: 0.001
#
# Updates:
#
##
"""
Worker methods for loading document sets into MongoDb.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import time

from rcsb_db.mongo.Connection import Connection
from rcsb_db.mongo.MongoDbUtil import MongoDbUtil
from rcsb_db.utils.MultiProcUtil import MultiProcUtil

logger = logging.getLogger(__name__)


class DocumentLoader(object):

    def __init__(self, cfgOb, resourceName="MONGO_DB", numProc=4, chunkSize=15, documentLimit=None, verbose=False, readBackCheck=False, maxStepLength=2000):
        self.__verbose = verbose
        #
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__documentLimit = documentLimit
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
        self.__mpFormat = '[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s'
        #
        #

    def load(self, databaseName, collectionName, loadType='full', documentList=None, indexAttributeList=None, keyName=None):
        """  Driver method for loading MongoDb content -


            loadType:     "full" or "replace"

        """
        try:
            startTime = self.__begin(message="loading operation")
            #

            #
            optionsD = {}
            optionsD['collectionName'] = collectionName
            optionsD['databaseName'] = databaseName
            optionsD['readBackCheck'] = self.__readBackCheck
            optionsD['loadType'] = loadType
            optionsD['keyName'] = keyName
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #
            docList = documentList[:self.__documentLimit] if self.__documentLimit else documentList
            logger.debug("Full document list length %d limit %r" % (len(documentList), self.__documentLimit))
            numProc = self.__numProc
            chunkSize = self.__chunkSize if docList and self.__chunkSize < len(docList) else 0
            #
            if loadType == 'full':
                self.__removeCollection(databaseName, collectionName)
                indAtList = indexAttributeList if indexAttributeList else []
                ok = self.__createCollection(databaseName, collectionName, indAtList)
                logger.debug("Collection create status %r" % ok)

            #
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            numDocs = len(docList)
            logger.debug("Processing %d total documents" % numDocs)
            numProc = min(numProc, numDocs)
            maxStepLength = self.__maxStepLength
            if numDocs > maxStepLength:
                numLists = int(numDocs / maxStepLength)
                subLists = [docList[i::numLists] for i in range(numLists)]
            else:
                subLists = [docList]
            #
            if subLists and len(subLists) > 0:
                logger.info("Starting with numProc %d outer subtask count %d subtask length ~ %d" % (numProc, len(subLists), len(subLists[0])))
            #
            failList = []
            for ii, subList in enumerate(subLists):
                logger.info("Running outer subtask %d of %d length %d" % (ii + 1, len(subLists), len(subList)))
                #
                mpu = MultiProcUtil(verbose=True)
                mpu.setOptions(optionsD=optionsD)
                mpu.set(workerObj=self, workerMethod="loadWorker")
                ok, failListT, _, _ = mpu.runMulti(dataList=subList, numProc=numProc, numResults=1, chunkSize=chunkSize)
                failList.extend(failListT)
            logger.debug("Failing document list %r" % failList)
            logger.info("Document list length %d failed load list length %d" % (len(docList), len(failList)))
            #

            self.__end(startTime, "loading operation with status " + str(ok))

            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def loadWorker(self, dataList, procName, optionsD, workingDir):
        """ Multi-proc worker method for MongoDb document loading -
        """
        try:
            startTime = self.__begin(message=procName)
            readBackCheck = optionsD['readBackCheck']
            loadType = optionsD['loadType']

            collectionName = optionsD['collectionName']
            databaseName = optionsD['databaseName']
            keyName = optionsD['keyName']
            #
            logger.debug("%s databaseName %s collectionName %s" % (procName, databaseName, collectionName))
            #
            if dataList:
                ok, successList, failedList = self.__loadDocuments(databaseName, collectionName, dataList,
                                                                   loadType=loadType, readBackCheck=readBackCheck, keyName=keyName)
            #
            logger.debug("%s database %s collection %s inputList length %d successList length %d  failed %d" %
                         (procName, databaseName, collectionName, len(dataList), len(successList), len(failedList)))
            #

            self.__end(startTime, procName + " with status " + str(ok))
            return successList, [], []

        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return [], [], []

    # -------------- -------------- -------------- -------------- -------------- -------------- --------------
    #                                        ---  Supporting code follows ---
    #

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

    def __loadDocuments(self, dbName, collectionName, docList, loadType='full', readBackCheck=False, keyName=None):
        #
        # Load database/collection with input document list -
        #
        failList = []
        logger.debug("dbName %s collectionName %s document count %d" % (dbName, collectionName, len(docList)))
        if keyName:
            # map the document list to some document key if this is provided
            indD = {}
            indL = []
            try:
                for ii, d in enumerate(docList):
                    dId = self.__dictGet(d, keyName)
                    indD[dId] = ii
                indL = list(range(len(docList)))
            except Exception as e:
                logger.exception("Failing ii %d d %r with %s" % (ii, d, str(e)))
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                if loadType == 'replace' and keyName:
                    dTupL = mg.deleteList(dbName, collectionName, docList, keyName)
                    logger.debug("Deleted document status %r" % dTupL)
                #
                rIdL = mg.insertList(dbName, collectionName, docList, keyName=keyName)
                logger.debug("Insert returns rIdL length %r" % len(rIdL))

                # ---
                #  If there is a failure then determine the specific successes and failures -
                #
                successList = docList
                failList = []
                if len(rIdL) != len(docList):
                    if keyName:
                        successIndList = []
                        for rId in rIdL:
                            rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                            dId = self.__getDict(rObj, keyName)
                            successIndList.append(indD[dId])
                        failIndList = list(set(indL) - set(successIndList))
                        failList = [docList[ii] for ii in failIndList]
                        successList = [docList[ii] for ii in successIndList]
                    else:
                        # fail the whole batch if we don't have visibility into each document
                        failList = docList
                        successList = []
                #
                if readBackCheck and keyName:
                    #
                    # Note that objects in docList are mutated by the insert operation with the additional key '_id',
                    # hence, it is possible to compare the fetched object with the input object.
                    #
                    rbStatus = True
                    for ii, rId in enumerate(rIdL):
                        rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                        dId = self.__getDict(rObj, keyName)
                        jj = indD[dId]
                        if (rObj != docList[jj]):
                            rbStatus = False
                            break
                #
                if readBackCheck and not rbStatus:
                    return False, successList, failList
                #
            return len(rIdL) == len(docList), successList, failList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False, [], docList

    def __dictGet(self, dct, dotNotation):
        """  Convert input dictionary key (dot notation) to divided Python format and return appropriate dictionary value.
        """
        key = None
        try:
            kys = dotNotation.split('.')
            for key in kys:
                try:
                    dct = dct[key]
                    logger.debug("dct %r " % dct)
                except KeyError:
                    return None
            return dct
        except Exception as e:
            logger.exception("Failing dotNotation %s key %r with %s" % (dotNotation, key, str(e)))

        return None
