##
# File:  MongoDbUtil.py
# Date:  12-Mar-2018 J. Westbrook
#
# Update:
#      17-Mar-2018  jdw add replace and index ops
#      19-Mar-2018  jdw reorganize error handling for bulk insert
#      24-Mar-2018  jdw add salvage path for bulk insert
#      25-Jul-2018  jdw more adjustments to exception handling and salvage processing.
#      14-Aug-2018  jdw generalize document identifier to a complex key
#       6-Sep-2018  jdw method to invoke general database command
#       7-Sep-2018  jdw add schema binding to createCollection method.createCollection. Change the default option to bypassValidation=False
#                       for method insertList()
#       8-Jan-2021  jdw add distinct() method
#      13-Aug-2024  dwp update reindex method for pymongo 4.x support
#      15-Jul-2025  dwp add getCollectionIndexes method
#       7-May-2026  dwp add pre-check to each method to make sure self.__mgObj is not None;
#                       remove try/except handling from deleteList to force failure
#      15-Sep-2026  dwp add fetchBatched() to page large result sets over successive short-lived
#                       cursors (bounded per-batch timeout + retry), bound count() with a timeout,
#                       and add estimatedCount() for informational collection counts
##
"""
Base class for simple essential database operations for MongoDb.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import time
from collections import OrderedDict

import pymongo

logger = logging.getLogger(__name__)

#
# TODO: MOVE THESE TO CONFIG FILE AFTER VERIFYING THESE CHANGES WORK
#
# Default controls for batched (paged) document retrieval.
#
# A single find() cursor over a very large collection must complete -- including every getMore
# round trip -- within the client 'timeoutMS' budget (CSOT applies to the entire life of a
# cursor). On a slow or congested link that budget is exhausted mid-iteration and the whole
# fetch is lost (pymongo.errors.ExecutionTimeout "operation exceeded time limit" during
# getMore). Paging the result set with a range query over an indexed sort key gives every
# batch its own short-lived cursor and its own timeout budget, so a stalled batch fails (and
# is retried) in minutes instead of taking down an hour-long fetch.
#
DEFAULT_FETCH_BATCH_SIZE = 10000
DEFAULT_FETCH_BATCH_TIMEOUT_SECONDS = 600
DEFAULT_FETCH_MAX_RETRIES = 3
DEFAULT_FETCH_RETRY_DELAY_SECONDS = 10
DEFAULT_COUNT_TIMEOUT_SECONDS = 300
#
# Transient server/network conditions worth retrying on a fresh cursor. Everything else
# (bad query, auth failure, ...) is raised immediately.
#
RETRYABLE_FETCH_ERRORS = (
    pymongo.errors.AutoReconnect,
    pymongo.errors.ConnectionFailure,
    pymongo.errors.ExecutionTimeout,
    pymongo.errors.NetworkTimeout,
    pymongo.errors.NotPrimaryError,
    pymongo.errors.ServerSelectionTimeoutError,
    pymongo.errors.WaitQueueTimeoutError,
)


class MongoDbUtil(object):
    def __init__(self, mongoClientObj, verbose=False):
        self.__verbose = verbose
        self.__mgObj = mongoClientObj
        self.__mongoIndexTypes = {"DESCENDING": pymongo.DESCENDING, "ASCENDING": pymongo.ASCENDING, "TEXT": pymongo.TEXT}

    def testMongoObj(self):
        """Sometimes self.__mgObj is randomly None and causes a failure. Not sure why...timeout?

        Raises:
            ValueError: if self.__mgObj is None
        """
        if self.__mgObj is None:
            raise ValueError("Mongo object self.__mgObj is None")

    def databaseExists(self, databaseName):
        self.testMongoObj()
        try:
            dbNameList = self.__mgObj.list_database_names()
            if databaseName in dbNameList:
                return True
            else:
                return False
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def getDatabaseNames(self):
        self.testMongoObj()
        return self.__mgObj.list_database_names()

    def createDatabase(self, databaseName, overWrite=True):
        self.testMongoObj()
        try:
            if overWrite and self.databaseExists(databaseName):
                logger.debug("Dropping existing database %s", databaseName)
                self.__mgObj.drop_database(databaseName)
            db = self.__mgObj[databaseName]
            logger.debug("Creating database %s %r", databaseName, db)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return False

    def dropDatabase(self, databaseName):
        self.testMongoObj()
        try:
            self.__mgObj.drop_database(databaseName)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def databaseCommand(self, databaseName, command):
        self.testMongoObj()
        try:
            rs = self.__mgObj[databaseName].command(command)
            logger.debug("Database %s Command %r returns %r", databaseName, command, rs)
            return True
        except Exception as e:
            logger.exception("Database %s command %s failing with %s", databaseName, command, str(e))
        return False

    def collectionExists(self, databaseName, collectionName):
        self.testMongoObj()
        try:
            if self.databaseExists(databaseName) and (collectionName in self.__mgObj[databaseName].list_collection_names()):
                return True
            else:
                return False
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def getCollectionNames(self, databaseName):
        self.testMongoObj()
        return self.__mgObj[databaseName].list_collection_names()

    def createCollection(self, databaseName, collectionName, overWrite=True, bsonSchema=None, validationLevel="strict", validationAction="error"):
        """
        Args:
            databaseName (str): Description
            collectionName (str): Description
            overWrite (bool, optional): Drop any existing collection before creation
            bsonSchema (dict, optional): JSON Schema (MongoDb flavor ~Draft 4 semantics w/ BSON types)
            validationLevel (str, optional): Apply to all inserts (strict) of but not for updates to existing documents (moderate)
            validationAction (str, optional): Reject inserts with error (error) or allow inserts with logged warning (warn)
                                              Warnings are recorded in the MongoDB system log and these are not conveniently accessible
                                              via the Python API.

        Returns:
            bool: True for success or False otherwise
        """
        self.testMongoObj()
        try:
            if overWrite and self.collectionExists(databaseName, collectionName):
                self.__mgObj[databaseName].drop_collection(collectionName)
            #
            ok = self.__mgObj[databaseName].create_collection(collectionName)
            logger.debug("Return from create collection %r", ok)
            if bsonSchema:
                self.updateCollection(databaseName, collectionName, bsonSchema=bsonSchema, validationLevel=validationLevel, validationAction=validationAction)
            return True
        except Exception as e:
            logger.exception("Failing for databaseName %s collectionName %s with %s", databaseName, collectionName, str(e))
        return False

    def updateCollection(self, databaseName, collectionName, bsonSchema=None, validationLevel="strict", validationAction="error"):
        """Update the validation schema and validation settings for the input collection
        Args:
            databaseName (str): Description
            collectionName (str): Description
            bsonSchema (dict, optional): JSON Schema (MongoDb flavor ~Draft 4 semantics w/ BSON types)
            validationLevel (str, optional): Apply to all inserts (strict) of but not for updates to existing documents (moderate)
            validationAction (str, optional): Reject inserts with error (error) or allow inserts with logged warning (warn)
                                              Warnings are recorded in the MongoDB system log and these are not conveniently accessible
                                              via the Python API.

        Returns:
            bool: True for success or False otherwise
        """
        self.testMongoObj()
        try:
            if bsonSchema:
                # bsonSchema.update({'additionalProperties': False})
                sD = {"$jsonSchema": bsonSchema}
                cmdD = OrderedDict([("collMod", collectionName), ("validator", sD), ("validationLevel", validationLevel), ("validationAction", validationAction)])
                self.__mgObj[databaseName].command(cmdD)
            return True
        except Exception as e:
            logger.exception("Failing for databaseName %s collectionName %s with %s", databaseName, collectionName, str(e))
        return False

    def dropCollection(self, databaseName, collectionName):
        self.testMongoObj()
        try:
            ok = self.__mgObj[databaseName].drop_collection(collectionName)
            logger.debug("Return from drop collection %r", ok)
            return True
        except Exception as e:
            logger.error("Failing drop collection for databaseName %s collectionName %s with %s", databaseName, collectionName, str(e))
        return False

    def insert(self, databaseName, collectionName, dObj, documentKey=None):
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            rV = clt.insert_one(dObj)
            try:
                rId = rV.inserted_id
                return rId
            except Exception as e:
                logger.debug("Failing with %s", str(e))
                return None
        except Exception as e:
            if documentKey:
                logger.error("Failing %r with %s", documentKey, str(e))
            else:
                logger.error("Failing with %s", str(e))
        return None

    def insertList(self, databaseName, collectionName, dList, ordered=False, bypassValidation=False, keyNames=None, salvage=False):
        """Insert the input list of documents (dList) into the input database/collection.


        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            dList (list): document list
            ordered (bool, optional): insert in input order
            bypassValidation (bool, optional): skip internal validation processing
            keyNames (list, optional): list of key names required to uniquely identify the object (dot notation)
            salvage (bool, optional): perform serial salvage operation for a batch insert failure

        Returns:
            list: List of MongoDB document identifiers for inserted objects


        """
        rIdL = []
        rV = None
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            rV = clt.insert_many(dList, ordered=ordered, bypass_document_validation=bypassValidation)
        except Exception as e:
            # If above insert_many fails, nothing *should* have been loaded and rV should be None
            # But, in case so, salvaging (below) will pre-delete all potential docs that may have been partially loaded
            logger.error("Bulk insert failing for document length %d with %s", len(dList), str(e)[:100])

            # for ii, dd in enumerate(dList):
            #    logger.error(" %d error doc %r" % (ii, list(dd.keys())))
        #
        try:
            rIdL = rV.inserted_ids if rV is not None else []
        except Exception as e:
            logger.error("Bulk insert list processing fails for document length %d with %s", len(dList), str(e))

        if salvage and keyNames and (len(rIdL) < len(dList)):
            logger.info("Bulk insert document recovery starting for %d documents", len(dList))
            rIdL = self.__salvageinsertList(databaseName, collectionName, dList, keyNames)
            logger.info("Bulk insert document recovery returns %d of %d", len(rIdL), len(dList))

        return rIdL

    def insertListSerial(self, databaseName, collectionName, dList, keyNames):
        """Insert the input list of documents (dList) into the input database/collection in serial mode.

        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            dList (list): document list
            keyNames (list, optional): list of key names required to uniquely identify the object (dot notation)

        Returns:
            list: List of MongoDB document identifiers for inserted objects

        """
        rIdL = []
        try:
            for dD in dList:
                kyVals = self.__getKeyValues(dD, keyNames)
                rId = self.insert(databaseName, collectionName, dD, documentKey=kyVals)
                if rId:
                    rIdL.append(rId)
                    logger.debug("Insert succeeds for document %s", (kyVals,))
                else:
                    logger.debug("Loading document %r failed", (kyVals,))
        except Exception as e:
            logger.exception("Failing %s and %s keyName %r with %s", databaseName, collectionName, keyNames, str(e))
        #
        return rIdL

    def __salvageinsertList(self, databaseName, collectionName, dList, keyNames):
        """Delete and serially insert the input document list.   Return the list list of documents ids successfully loaded.

        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            dList (list): document list
            keyNames (list, optional): list of key names required to uniquely identify the object (dot notation)

        Returns:
            list: List of MongoDB document identifiers for inserted objects

        """
        logger.info("Salvaging %s %s document list length %d", databaseName, collectionName, len(dList))
        dTupL = self.deleteList(databaseName, collectionName, dList, keyNames)
        logger.info("Salvage bulk insert - deleting %d documents", len(dTupL))
        rIdL = self.insertListSerial(databaseName, collectionName, dList, keyNames)
        logger.info("Salvage bulk insert - salvaged document length %d", len(rIdL))
        return rIdL

    def fetchOne(self, databaseName, collectionName, ky, val):
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            dObj = clt.find_one({ky: val})
            return dObj
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None

    def update(self, databaseName, collectionName, dObj, selectD, upsertFlag=False):
        """Update documents satisfying the selection details with the content of dObj.

        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            dObj (dict): document data (dotted notation for sub-objects applied with '$set')
            selectD (dict): dictionary of key/values for the selction/filter query

        Returns:
            int: update document count

            update_many(filter, update, upsert=False, array_filters=None)
        """
        self.testMongoObj()
        try:
            numModified = 0
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            rV = clt.update_many(selectD, {"$set": dObj}, upsert=upsertFlag, array_filters=None)
            try:
                numMatched = rV.matched_count
                numModified = rV.modified_count
                logger.debug("Replacement matched %d modified %d", numMatched, numModified)
                return numModified
            except Exception as e:
                logger.error("Failing update %s and %s selectD %r with %s", databaseName, collectionName, selectD, str(e))
                return None
        except Exception as e:
            logger.exception("Failing update %s and %s selectD %r with %s", databaseName, collectionName, selectD, str(e))
        return numModified

    def replace(self, databaseName, collectionName, dObj, selectD, upsertFlag=True):
        """Replace the input document based on a selection query in the input selection dictionary (k,v).

        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            dList (list): document list
            selectD (dict, optional): dictionary of key/values for the selction query
            upsertFlag (bool, optional): set MongoDB 'upsert' option

        Returns:
            str: MongoDB document identifier for the replaced object

        """
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            rV = clt.replace_one(selectD, dObj, upsert=upsertFlag)
            logger.debug("Replace returns  %r", rV)
            try:
                rId = rV.upserted_id
                numMatched = rV.matched_count
                numModified = rV.modified_count
                logger.debug("Replacement matched %d modified %d or upserted with _id %s", numMatched, numModified, rId)
                return numMatched or numModified
            except Exception as e:
                logger.error("Failing %s and %s selectD %r with %s", databaseName, collectionName, selectD, str(e))
                return None
        except Exception as e:
            logger.error("Failing %s and %s selectD %r with %s", databaseName, collectionName, selectD, str(e))
        return None

    def replaceList(self, databaseName, collectionName, dList, keyNames, upsertFlag=True):
        """Replace the list of input documents based on a selection query by keyNames -

        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            dList (list): document list
            keyNames (list, optional): list of key names required to uniquely identify the object (dot notation)
            upsertFlag (bool, optional): set MongoDB 'upsert' option

        Returns:
            list: List of MongoDB document identifiers for replaced objects

        """
        rIdL = []
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            for dD in dList:
                kyVals = self.__getKeyValues(dD, keyNames)
                selectD = {ky: val for ky, val in zip(keyNames, kyVals)}
                rV = clt.replace_one(selectD, dD, upsert=upsertFlag)
                try:
                    rIdL.append(rV.upserted_id)
                except Exception as e:
                    logger.error("Failing for %s and %s selectD %r with %s", databaseName, collectionName, selectD.items(), str(e))
        except Exception as e:
            logger.error("Failing %s and %s selectD %r with %s", databaseName, collectionName, selectD.items(), str(e))
        #
        return rIdL

    def deleteList(self, databaseName, collectionName, dList, keyNames):
        """Delete the list of input documents based on a selection query by keyNames.


        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            dList (list): document list
            keyNames (list, optional): list of key names required to uniquely identify the object (dot notation)

        Returns:
            list: (value tuple of key names, deletion count)

        """
        cD = {}
        delTupL = []
        selectD = {}
        #
        self.testMongoObj()
        clt = self.__mgObj[databaseName].get_collection(collectionName)
        #
        # NOTE: May want to look into grouping these into one single delete query instead of multiple individual ones,
        #       and evaluating how much faster (if at all) that lets the workflow run. May be significant for CSMs.
        #       If doing so, make sure that you only do so when the selectD has just one Key (e.g., not a paired key)
        #       and make sure the delete query doesn't timeout too easily with this approach.
        for dD in dList:  # each dD is an entire Mongo doc for an entry/entity/...
            kyVals = self.__getKeyValues(dD, keyNames)
            selectD = {ky: val for ky, val in zip(keyNames, kyVals)}
            # Example 'selectD': {'rcsb_assembly_container_identifiers.entry_id': 'AF_AFQ06159F1'}
            tt = tuple(selectD.items())
            if tt in cD:
                continue
            cD[tt] = True
            rV = clt.delete_many(selectD)
            try:
                # delTupL.append((kyVals, r.deleted_count))
                delTupL.append((selectD, rV.deleted_count))
            except Exception as e:
                logger.error("Failing %s and %s selectD %r with %s", databaseName, collectionName, selectD.items(), str(e))
        logger.debug("%s %s deleted status %r", databaseName, collectionName, delTupL)
        #
        return delTupL

    def delete(self, databaseName, collectionName, selectD):
        """Delete objects from the input collection based on the input selection query.


        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            selectD (dict): selection query

        Returns:
            (int): deletion count

        """
        delCount = 0
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            rV = clt.delete_many(selectD)
            delCount = rV.deleted_count
            logger.debug("%s %s deleted %d", databaseName, collectionName, delCount)
        except Exception as e:
            logger.error("Failing %s and %s selectD %r with %s", databaseName, collectionName, selectD.items(), str(e))
        #
        return delCount

    def createIndex(self, databaseName, collectionName, keyList, indexName="primary", indexType="DESCENDING", uniqueFlag=False):
        self.testMongoObj()
        try:
            iTupL = [(ky, self.__mongoIndexTypes[indexType]) for ky in keyList]
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            clt.create_index(iTupL, name=indexName, background=True, unique=uniqueFlag)
            logger.debug("Current indexes for %s %s : %r", databaseName, collectionName, clt.list_indexes())
            return True
        except Exception as e:
            logger.error("Failing %s and %s keyList %r with %s", databaseName, collectionName, keyList, str(e))
        return False

    def dropIndex(self, databaseName, collectionName, indexName="primary"):
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            clt.drop_index(indexName)
            logger.debug("Current indexes for %s %s : %r", databaseName, collectionName, clt.list_indexes())
            return True
        except Exception as e:
            logger.error("Failing %s and %s with %s", databaseName, collectionName, str(e))
        return False

    def reIndex(self, databaseName, collectionName):
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            logger.debug("Current indexes for %s %s : %r", databaseName, collectionName, clt.list_indexes())
            self.__mgObj[databaseName].command("reIndex", collectionName)
            return True
        except Exception as e:
            logger.exception("Failing %s and %s with %s", databaseName, collectionName, str(e))
        return False

    def getCollectionIndexes(self, databaseName, collectionName):
        """
        Return a list of index information dictionaries for the given collection.
        """
        indexList = []
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            indexList = list(clt.list_indexes())
            if self.__verbose:
                for idx in indexList:
                    logger.debug("Index on %s.%s: %r", databaseName, collectionName, idx)
            return indexList
            # Return list looks like:
            # [
            #     SON([('v', 2), ('key', SON([('_id', 1)])), ('name', '_id_')]),
            #     SON([('v', 2), ('key', SON([('id', -1)])), ('name', 'index_1'), ('background', True)]),
            #     SON([('v', 2), ('key', SON([('parents', -1)])), ('name', 'index_2'), ('background', True)])
            # ]
        except Exception as e:
            logger.exception("Failed to retrieve indexes for %s.%s with error: %s", databaseName, collectionName, str(e))
        return []

    def fetch(self, databaseName, collectionName, selectL, queryD=None, suppressId=False, **kwargs):
        """Fetch selections (selectL) from documents satisfying input query constraints.

        The result set is retrieved in batches (see fetchBatched()) and accumulated here.
        Callers that do not need the whole result set resident in memory at once should
        iterate fetchBatched() directly.

        Args:
            databaseName (str): database name
            collectionName (str): collection name
            selectL (list): list of attribute names to return (empty/None returns whole documents)
            queryD (dict, optional): selection query. Defaults to None.
            suppressId (bool, optional): exclude '_id' from the returned documents. Defaults to False.
            batchSize (int, optional): documents per batch (0 disables batching). Defaults to DEFAULT_FETCH_BATCH_SIZE.
            sortKey (str, optional): indexed attribute used to page the result set. Defaults to "_id".
            batchTimeoutSeconds (int, optional): per-batch timeout. Defaults to DEFAULT_FETCH_BATCH_TIMEOUT_SECONDS.
            maxRetries (int, optional): attempts per batch. Defaults to DEFAULT_FETCH_MAX_RETRIES.
            retryDelaySeconds (int, optional): base retry backoff. Defaults to DEFAULT_FETCH_RETRY_DELAY_SECONDS.

        Returns:
            list: list of documents -- an EMPTY list means the query matched no documents;
                  None means the fetch itself FAILED. Callers must distinguish the two.
        """
        dList = []
        self.testMongoObj()
        try:
            for dL in self.fetchBatched(databaseName, collectionName, selectL, queryD=queryD, suppressId=suppressId, **kwargs):
                dList.extend(dL)
            return dList
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None

    def fetchBatched(
        self,
        databaseName,
        collectionName,
        selectL,
        queryD=None,
        suppressId=False,
        batchSize=None,
        sortKey="_id",
        batchTimeoutSeconds=DEFAULT_FETCH_BATCH_TIMEOUT_SECONDS,
        maxRetries=DEFAULT_FETCH_MAX_RETRIES,
        retryDelaySeconds=DEFAULT_FETCH_RETRY_DELAY_SECONDS,
    ):
        """Generator yielding successive batches of documents satisfying the input query constraints.

        The result set is paged with a range query over 'sortKey' ('{sortKey: {"$gt": <last value
        of the previous batch>}}' with an ascending sort and a limit), so each batch is served by
        its own short-lived cursor. This bounds the amount of work that is lost -- and retried --
        when a single round trip stalls, and keeps peak memory proportional to the batch size
        rather than to the full result set.

        Note:
            'sortKey' must be indexed (the default, '_id', always is). Paging over an unindexed
            attribute forces a server-side in-memory sort of the whole collection per batch.

        Args:
            (see fetch())

        Yields:
            list: a non-empty list of documents.

        Raises:
            pymongo.errors.PyMongoError: if a batch cannot be retrieved after 'maxRetries' attempts.
        """
        self.testMongoObj()
        batchSize = DEFAULT_FETCH_BATCH_SIZE if batchSize is None else int(batchSize)
        maxRetries = max(1, int(maxRetries))
        projectionD, stripSortKey = self.__makeFetchProjection(selectL, suppressId, sortKey)
        clt = self.__mgObj[databaseName].get_collection(collectionName)
        #
        if batchSize <= 0:
            # Batching explicitly disabled -- single cursor over the full result set
            logger.debug("Batching disabled for %s %s - using a single cursor", databaseName, collectionName)
            dL = self.__fetchOneBatch(clt, queryD if queryD else None, projectionD, None, 0, batchTimeoutSeconds, maxRetries, retryDelaySeconds)
            if dL:
                yield dL
            return
        #
        startTime = time.time()
        lastKeyValue = None
        totalCount = 0
        batchNum = 0
        while True:
            batchNum += 1
            bqD = self.__addRangeConstraint(queryD, sortKey, lastKeyValue)
            dL = self.__fetchOneBatch(clt, bqD, projectionD, sortKey, batchSize, batchTimeoutSeconds, maxRetries, retryDelaySeconds)
            if not dL:
                break
            if sortKey not in dL[-1]:
                raise ValueError("Sort key %r absent from fetched documents of %s %s - cannot page result set" % (sortKey, databaseName, collectionName))
            lastKeyValue = dL[-1][sortKey]
            totalCount += len(dL)
            numFetched = len(dL)
            if stripSortKey:
                for dD in dL:
                    dD.pop(sortKey, None)
            logger.debug("%s %s batch %d fetched %d documents (total %d)", databaseName, collectionName, batchNum, numFetched, totalCount)
            yield dL
            if numFetched < batchSize:
                break
        logger.info(
            "Fetched %d documents from %s %s in %d batch(es) of %d (%.4f seconds)",
            totalCount,
            databaseName,
            collectionName,
            batchNum,
            batchSize,
            time.time() - startTime,
        )

    def __fetchOneBatch(self, clt, queryD, projectionD, sortKey, batchSize, batchTimeoutSeconds, maxRetries, retryDelaySeconds):
        """Run a single batch query, retrying on transient server/network errors.

        Each attempt uses a new cursor, and -- when 'batchTimeoutSeconds' is set -- its own
        timeout budget (overriding any client-level 'timeoutMS' for the duration of the call).
        """
        lastErr = None
        for attempt in range(1, maxRetries + 1):
            try:
                if batchTimeoutSeconds and float(batchTimeoutSeconds) > 0:
                    with pymongo.timeout(float(batchTimeoutSeconds)):
                        return self.__runFetchCursor(clt, queryD, projectionD, sortKey, batchSize)
                return self.__runFetchCursor(clt, queryD, projectionD, sortKey, batchSize)
            except RETRYABLE_FETCH_ERRORS as e:
                lastErr = e
                if attempt >= maxRetries:
                    break
                delaySeconds = float(retryDelaySeconds) * attempt
                logger.warning(
                    "Batch fetch on %s attempt %d of %d failed (%s) - retrying in %.1f seconds",
                    clt.full_name,
                    attempt,
                    maxRetries,
                    str(e),
                    delaySeconds,
                )
                time.sleep(delaySeconds)
        logger.error("Batch fetch on %s failed after %d attempt(s) with %s", clt.full_name, maxRetries, str(lastErr))
        raise lastErr

    @staticmethod
    def __runFetchCursor(clt, queryD, projectionD, sortKey, batchSize):
        cursor = clt.find(filter=queryD if queryD else None, projection=projectionD)
        if sortKey:
            cursor = cursor.sort(sortKey, pymongo.ASCENDING).limit(int(batchSize))
        return list(cursor)

    @staticmethod
    def __makeFetchProjection(selectL, suppressId, sortKey):
        """Build the projection for a batched fetch.

        Returns:
            (dict|None, bool): projection document (None selects whole documents), and a flag
            indicating whether 'sortKey' was added solely to support paging and so must be
            removed from the documents handed back to the caller.
        """
        if selectL:
            sD = {k: 1 for k in selectL}
            if suppressId:
                sD["_id"] = 0
        elif suppressId:
            sD = {"_id": 0}
        else:
            # Whole documents - '_id' and every other attribute are already present
            return None, False
        #
        stripSortKey = False
        if sortKey == "_id":
            if sD.get("_id", None) == 0:
                sD["_id"] = 1
                stripSortKey = True
        elif sortKey not in sD:
            sD[sortKey] = 1
            stripSortKey = True
        return sD, stripSortKey

    @staticmethod
    def __addRangeConstraint(queryD, sortKey, lastKeyValue):
        """Add the '{sortKey: {"$gt": lastKeyValue}}' paging constraint to the selection query."""
        if lastKeyValue is None:
            return dict(queryD) if queryD else {}
        rangeD = {sortKey: {"$gt": lastKeyValue}}
        if not queryD:
            return rangeD
        if sortKey in queryD or "$and" in queryD:
            # Don't clobber an existing constraint on the sort key
            return {"$and": [dict(queryD), rangeD]}
        qD = dict(queryD)
        qD.update(rangeD)
        return qD

    def count(self, databaseName, collectionName, countFilter=None, timeoutSeconds=DEFAULT_COUNT_TIMEOUT_SECONDS):
        """Return the number of documents matching the (optional) filter.

        Note:
            An unfiltered count_documents() is a full collection scan on the server and can be
            very slow on large collections - use estimatedCount() when an exact value is not
            required. 'timeoutSeconds' bounds the call so a stalled count cannot consume the
            whole client timeout budget.
        """
        self.testMongoObj()
        try:
            tF = countFilter if countFilter else {}
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            logger.debug("Current indexes for %s %s : %r", databaseName, collectionName, clt.list_indexes())
            if timeoutSeconds and float(timeoutSeconds) > 0:
                with pymongo.timeout(float(timeoutSeconds)):
                    return clt.count_documents(tF)
            return clt.count_documents(tF)
        except Exception as e:
            logger.exception("Failing for %s and %s with %s", databaseName, collectionName, str(e))
        return 0

    def estimatedCount(self, databaseName, collectionName, timeoutSeconds=DEFAULT_COUNT_TIMEOUT_SECONDS):
        """Return the estimated document count for a collection (from collection metadata).

        This is O(1) on the server, unlike the collection scan performed by count(), and is
        intended for informational/logging use where an exact count is not required.

        Returns:
            int: estimated document count, or -1 if the count could not be obtained.
        """
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            if timeoutSeconds and float(timeoutSeconds) > 0:
                with pymongo.timeout(float(timeoutSeconds)):
                    return clt.estimated_document_count()
            return clt.estimated_document_count()
        except Exception as e:
            logger.error("Failing for %s and %s with %s", databaseName, collectionName, str(e))
        return -1

    def distinct(self, databaseName, collectionName, ky):
        """Return a list of distinct values for the input key in the collection."""
        rL = []
        self.testMongoObj()
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            rL = clt.distinct(ky)
        except Exception as e:
            logger.exception("Failing for %s and %s (%s) with %s", databaseName, collectionName, ky, str(e))
        return rL

    def __getKeyValues(self, dct, keyNames):
        """Return the tuple of values of corresponding to the input dictionary key names expressed in dot notation.

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
                except KeyError:
                    return None
            return dct
        except Exception as e:
            logger.exception("Failing for key %r with %s", keyName, str(e))

        return None
