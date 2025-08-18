##
# File:    DocumentLoader.py
# Author:  J. Westbrook
# Date:    24-Jun-2018
# Version: 0.001
#
# Updates:
#  13-July-2018 jdw add append mode
#  14-Aug-2018  jdw generalize key identifiers to lists
#  15-Jul-2025  dwp add ability to provide a dictionary of fields to index and their desired corresponding names
#  30-Jul-2025  dwp consolidate redundant methods with those previously in PdbxLoader and make them public methods
#                   to allow for re-use by PdbxLoader (createCollection(), removeCollection(), getKeyValues())
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
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

logger = logging.getLogger(__name__)


class DocumentLoader(object):
    def __init__(
        self,
        cfgOb,
        cachePath,
        resourceName="MONGO_DB",
        numProc=4,
        chunkSize=15,
        documentLimit=None,
        verbose=False,
        readBackCheck=False,
        maxStepLength=2000,
        schemaRebuildFlag=False,
    ):
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
        self.__cachePath = cachePath if cachePath else "."
        self.__schP = SchemaProvider(cfgOb, cachePath, useCache=True, rebuildFlag=schemaRebuildFlag)
        #
        self.__readBackCheck = readBackCheck
        self.__mpFormat = "[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s"
        #
        #

    def load(self, databaseName, collectionName, loadType="full", documentList=None, indexAttributeList=None, keyNames=None, schemaLevel="full", addValues=None, indexDL=None):
        """Driver method for loading MongoDb content -
        """
        try:
            startTime = self.__begin(message="loading operation")
            #
            optionsD = {}
            optionsD["collectionName"] = collectionName
            optionsD["databaseName"] = databaseName
            # optionsD["databaseName"] = databaseNameMongo
            optionsD["readBackCheck"] = self.__readBackCheck
            optionsD["loadType"] = loadType
            optionsD["keyNames"] = keyNames
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #
            docList = documentList[: self.__documentLimit] if self.__documentLimit else documentList
            logger.debug("Full document list length %d limit %r", len(documentList), self.__documentLimit)
            logger.info("Loading documents with numProc %r chunkSize %r maxStepLength %r", self.__numProc, self.__chunkSize, self.__maxStepLength)
            numProc = self.__numProc
            chunkSize = self.__chunkSize if docList and self.__chunkSize < len(docList) else 0
            #
            if addValues:
                try:
                    for doc in docList:
                        for k, v in addValues.items():
                            doc[k] = v
                except Exception as e:
                    logger.error("Add values %r fails with %s", addValues, str(e))
            #
            indAtList = indexAttributeList if indexAttributeList else []
            indAtDictList = indexDL if indexDL else []
            bsonSchema = None
            if schemaLevel and schemaLevel in ["min", "full"]:
                bsonSchema = self.__schP.getJsonSchema(databaseName, collectionName, encodingType="BSON", level=schemaLevel)
                logger.debug("Using schema validation for %r %r %r", databaseName, collectionName, schemaLevel)
            #
            if loadType == "full":
                self.removeCollection(databaseName, collectionName)
                ok = self.createCollection(databaseName, collectionName, indexAttributeNames=indAtList, bsonSchema=bsonSchema, indexDL=indAtDictList)
                logger.info("Collection %s create status %r", collectionName, ok)
            elif loadType == "append":
                # create only if object does not exist -
                ok = self.createCollection(databaseName, collectionName, indexAttributeNames=indAtList, checkExists=True, bsonSchema=bsonSchema, indexDL=indAtDictList)
                logger.debug("Collection %s create status %r", collectionName, ok)
                # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            numDocs = len(docList)
            logger.info("Processing %d total documents", numDocs)
            numProc = min(numProc, numDocs)
            maxStepLength = self.__maxStepLength
            if numDocs > maxStepLength:
                numLists = int(numDocs / maxStepLength)
                subLists = [docList[i::numLists] for i in range(numLists)]
            else:
                subLists = [docList]
            #
            if subLists:
                logger.debug("Starting with numProc %d outer subtask count %d subtask length ~ %d", numProc, len(subLists), len(subLists[0]))
            #
            failList = []
            for ii, subList in enumerate(subLists):
                logger.debug("Running outer subtask %d of %d length %d", ii + 1, len(subLists), len(subList))
                #
                mpu = MultiProcUtil(verbose=True)
                mpu.setOptions(optionsD=optionsD)
                mpu.set(workerObj=self, workerMethod="loadWorker")
                ok, failListT, _, _ = mpu.runMulti(dataList=subList, numProc=numProc, numResults=1, chunkSize=chunkSize)
                failList.extend(failListT)
            logger.info("Completed load with failing document list %r", failList)
            logger.info("Document list length %d failed load list length %d", len(docList), len(failList))
            #
            self.__end(startTime, "loading operation with status " + str(ok))
            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return False

    def loadWorker(self, dataList, procName, optionsD, workingDir):
        """Multi-proc worker method for MongoDb document loading -"""
        try:
            startTime = self.__begin(message=procName)
            readBackCheck = optionsD["readBackCheck"]
            loadType = optionsD["loadType"]

            collectionName = optionsD["collectionName"]
            databaseName = optionsD["databaseName"]
            keyNames = optionsD["keyNames"]
            #
            logger.debug("%s databaseName %s collectionName %s workingDir %s", procName, databaseName, collectionName, workingDir)
            #
            ok = False
            successList, failedList = [], []
            if dataList:
                ok, successList, failedList = self.__loadDocuments(databaseName, collectionName, dataList, loadType=loadType, readBackCheck=readBackCheck, keyNames=keyNames)
            #
            logger.debug(
                "%s database %s collection %s inputList length %d successList length %d  failed %d",
                procName,
                databaseName,
                collectionName,
                len(dataList),
                len(successList),
                len(failedList),
            )
            #
            self.__end(startTime, procName + " with status " + str(ok))
            return successList, [], []

        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return [], [], []

    # -------------- -------------- -------------- -------------- -------------- -------------- --------------
    #                                        ---  Supporting code follows ---
    #

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

    def createCollection(self, dbName, collectionName, indexDL=None, indexAttributeNames=None, checkExists=False, bsonSchema=None, indexType="DESCENDING"):
        """Create database and collection and optionally a primary index

        Args:
            dbName (str): Database name
            collectionName (str): Collection name
            indexAttributeNames (list, optional): List of attributes/fields to create a COMPOUND index on with name "primary". Defaults to None.
            checkExists (bool, optional): _description_. Defaults to False.
            bsonSchema (_type_, optional): _description_. Defaults to None.
            indexDL (list, optional): List of dictionaries containing attributes/fields to index and desired index name. Use this INSTEAD OF indexAttributeNames. Defaults to None.
                                      Structure looks like: [{"ATTRIBUTE_NAMES": ["rcsb_id"], "INDEX_NAME": "index_1"}], where ATTRIBUTE_NAMES can be a list > 1 for a compound index.

        Returns:
            bool: True if success; False otherwise
        """
        try:
            logger.debug("Create database %s collection %s", dbName, collectionName)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if checkExists and mg.databaseExists(dbName) and mg.collectionExists(dbName, collectionName):
                    ok1 = True
                else:
                    ok1 = mg.createCollection(dbName, collectionName, bsonSchema=bsonSchema)
                ok2 = mg.databaseExists(dbName)
                ok3 = mg.collectionExists(dbName, collectionName)
                okI = True
                if indexAttributeNames and indexDL:
                    raise ValueError("Cannot provide both indexAttributeNames and indexDL in collection creation - must provide one or the other")
                if indexAttributeNames:
                    okI = mg.createIndex(dbName, collectionName, indexAttributeNames, indexName="primary", indexType="DESCENDING", uniqueFlag=False)
                if indexDL:
                    for indexD in indexDL:
                        uniqueFlag = indexD.get("UNIQUE", False)
                        okI = mg.createIndex(dbName, collectionName, indexD["ATTRIBUTE_NAMES"], indexName=indexD["INDEX_NAME"], indexType=indexType, uniqueFlag=uniqueFlag) and okI
            return ok1 and ok2 and ok3 and okI
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def removeCollection(self, dbName, collectionName):
        """Drop collection within database"""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                logger.debug("Remove collection database %s collection %s", dbName, collectionName)
                logger.debug("Starting databases = %r", mg.getDatabaseNames())
                logger.debug("Starting collections = %r", mg.getCollectionNames(dbName))
                ok = mg.dropCollection(dbName, collectionName)
                logger.debug("Databases = %r", mg.getDatabaseNames())
                logger.debug("Post drop collections = %r", mg.getCollectionNames(dbName))
                ok = mg.collectionExists(dbName, collectionName)
                logger.debug("Post drop collections = %r", mg.getCollectionNames(dbName))
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __loadDocuments(self, dbName, collectionName, docList, loadType="full", readBackCheck=False, keyNames=None):
        #
        # Load database/collection with input document list -
        #
        failList = []
        rIdL = []
        successList = []
        logger.debug("Loading dbName %s collectionName %s with document count %d keynames %r", dbName, collectionName, len(docList), keyNames)
        if keyNames:
            # map the document list to some document key if this is provided
            indD = {}
            indL = []
            try:
                for ii, doc in enumerate(docList):
                    dIdTup = self.getKeyValues(doc, keyNames)
                    indD[dIdTup] = ii
                indL = list(range(len(docList)))
            except Exception as e:
                logger.exception("Failing ii %d d %r with %s", ii, doc, str(e))
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                #
                if loadType == "replace" and keyNames:
                    dTupL = mg.deleteList(dbName, collectionName, docList, keyNames)
                    logger.debug("Deleted document status %r", (dTupL,))
                #
                rIdL = mg.insertList(dbName, collectionName, docList, keyNames=keyNames)
                logger.debug("Insert returns rIdL length %r", len(rIdL))

                # ---
                #  If there is a failure then determine the specific successes and failures -
                #
                successList = docList
                failList = []
                if len(rIdL) != len(docList):
                    if keyNames:
                        successIndList = []
                        for rId in rIdL:
                            rObj = mg.fetchOne(dbName, collectionName, "_id", rId)
                            dIdTup = self.getKeyValues(rObj, keyNames)
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
                        rObj = mg.fetchOne(dbName, collectionName, "_id", rId)
                        dIdTup = self.getKeyValues(rObj, keyNames)
                        jj = indD[dIdTup]
                        if rObj != docList[jj]:
                            rbStatus = False
                            break
                #
                if readBackCheck and not rbStatus:
                    return False, successList, failList
                #
            return len(rIdL) == len(docList), successList, failList
        except Exception as e:
            logger.exception("Failing %r %r (len=%d) %s with %s", dbName, collectionName, len(docList), keyNames, str(e))
        return False, [], docList

    def getKeyValues(self, dct, keyNames):
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
                except KeyError:
                    logger.warning("Missing document key %r in %r", key, list(dct.keys()))
                    return None
            return dct
        except Exception as e:
            logger.error("Failing for key %r with %s", keyName, str(e))

        return None
