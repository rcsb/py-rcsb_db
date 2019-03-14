##
# File:    DocumentLoader.py
# Author:  J. Westbrook
# Date:    24-Jun-2018
# Version: 0.001
#
# Updates:
#  13-July-2018 jdw add append mode
#  14-Aug-2018  jdw generalize key identifiers to lists
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

from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

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
        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__documentLimit)
        #
        self.__readBackCheck = readBackCheck
        self.__mpFormat = '[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s'
        #
        #

    def load(self, databaseName, collectionName, loadType='full', documentList=None, indexAttributeList=None, keyNames=None, schemaLevel='full', addValues=None):
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
            optionsD['keyNames'] = keyNames
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #
            docList = documentList[:self.__documentLimit] if self.__documentLimit else documentList
            logger.debug("Full document list length %d limit %r" % (len(documentList), self.__documentLimit))
            numProc = self.__numProc
            chunkSize = self.__chunkSize if docList and self.__chunkSize < len(docList) else 0
            #
            if addValues:
                try:
                    for doc in docList:
                        for k, v in addValues.items():
                            doc[k] = v
                except Exception as e:
                    logger.error("Add values %r fails with %s" % (addValues, str(e)))

            #
            indAtList = indexAttributeList if indexAttributeList else []
            bsonSchema = None
            if schemaLevel and schemaLevel in ['min', 'full']:
                bsonSchema = self.__schU.getJsonSchema(collectionName, schemaType='BSON', level=schemaLevel)
                logger.debug("Using schema validation for %r %r" % (collectionName, schemaLevel))

            if loadType == 'full':
                self.__removeCollection(databaseName, collectionName)
                ok = self.__createCollection(databaseName, collectionName, indAtList, bsonSchema=bsonSchema)
                logger.info("Collection %s create status %r" % (collectionName, ok))
            elif loadType == 'append':
                # create only if object does not exist -
                ok = self.__createCollection(databaseName, collectionName, indexAttributeNames=indAtList, checkExists=True, bsonSchema=bsonSchema)
                logger.debug("Collection %s create status %r" % (collectionName, ok))
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
                logger.debug("Starting with numProc %d outer subtask count %d subtask length ~ %d" % (numProc, len(subLists), len(subLists[0])))
            #
            failList = []
            for ii, subList in enumerate(subLists):
                logger.debug("Running outer subtask %d of %d length %d" % (ii + 1, len(subLists), len(subList)))
                #
                mpu = MultiProcUtil(verbose=True)
                mpu.setOptions(optionsD=optionsD)
                mpu.set(workerObj=self, workerMethod="loadWorker")
                ok, failListT, _, _ = mpu.runMulti(dataList=subList, numProc=numProc, numResults=1, chunkSize=chunkSize)
                failList.extend(failListT)
            logger.debug("Completed load with failing document list %r" % failList)
            logger.debug("Document list length %d failed load list length %d" % (len(docList), len(failList)))
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
            keyNames = optionsD['keyNames']
            #
            logger.debug("%s databaseName %s collectionName %s" % (procName, databaseName, collectionName))
            #
            if dataList:
                ok, successList, failedList = self.__loadDocuments(databaseName, collectionName, dataList,
                                                                   loadType=loadType, readBackCheck=readBackCheck,
                                                                   keyNames=keyNames)
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

    def __createCollection(self, dbName, collectionName, indexAttributeNames=None, checkExists=False, bsonSchema=None):
        """Create database and collection and optionally a primary index -
        """
        try:
            logger.debug("Create database %s collection %s" % (dbName, collectionName))
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if checkExists and mg.databaseExists(dbName) and mg.collectionExists(dbName, collectionName):
                    ok1 = True
                else:
                    ok1 = mg.createCollection(dbName, collectionName, bsonSchema=bsonSchema)
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

    def __loadDocuments(self, dbName, collectionName, docList, loadType='full', readBackCheck=False, keyNames=None):
        #
        # Load database/collection with input document list -
        #
        failList = []
        rIdL = []
        successList = []
        logger.debug("Loading dbName %s collectionName %s with document count %d keynames %r " %
                     (dbName, collectionName, len(docList), keyNames))
        if keyNames:
            # map the document list to some document key if this is provided
            indD = {}
            indL = []
            try:
                for ii, d in enumerate(docList):
                    dIdTup = self.__getKeyValues(d, keyNames)
                    indD[dIdTup] = ii
                indL = list(range(len(docList)))
            except Exception as e:
                logger.exception("Failing ii %d d %r with %s" % (ii, d, str(e)))
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                if loadType == 'replace' and keyNames:
                    dTupL = mg.deleteList(dbName, collectionName, docList, keyNames)
                    logger.debug("Deleted document status %r" % (dTupL,))
                #
                rIdL = mg.insertList(dbName, collectionName, docList, keyNames=keyNames)
                logger.debug("Insert returns rIdL length %r" % len(rIdL))

                # ---
                #  If there is a failure then determine the specific successes and failures -
                #
                successList = docList
                failList = []
                if len(rIdL) != len(docList):
                    if keyNames:
                        successIndList = []
                        for rId in rIdL:
                            rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                            dIdTup = self.__getKeyValues(rObj, keyNames)
                            successIndList.append(indD[dIdTup])
                        failIndList = list(set(indL) - set(successIndList))
                        failList = [docList[ii] for ii in failIndList]
                        successList = [docList[ii] for ii in successIndList]
                    else:
                        # fail the whole batch if we don't have visibility into each document
                        failList = docList
                        successList = []
                #
                rbStatus = True
                if readBackCheck and keyNames:
                    #
                    # Note that objects in docList are mutated by the insert operation with the additional key '_id',
                    # hence, it is possible to compare the fetched object with the input object.
                    #
                    for ii, rId in enumerate(rIdL):
                        rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                        dIdTup = self.__getKeyValues(rObj, keyNames)
                        jj = indD[dIdTup]
                        if (rObj != docList[jj]):
                            rbStatus = False
                            break
                #
                if readBackCheck and not rbStatus:
                    return False, successList, failList
                #
            return len(rIdL) == len(docList), successList, failList
        except Exception as e:
            logger.exception("Failing %r %r (len=%d) %s with %s" % (dbName, collectionName, len(docList), keyNames, str(e)))
        return False, [], docList

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
