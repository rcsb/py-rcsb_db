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
##
"""
Base class for simple essential database operations for MongoDb.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
from collections import OrderedDict

import pymongo

logger = logging.getLogger(__name__)


class MongoDbUtil(object):
    def __init__(self, mongoClientObj, verbose=False):
        self.__verbose = verbose
        self.__mgObj = mongoClientObj
        self.__mongoIndexTypes = {"DESCENDING": pymongo.DESCENDING, "ASCENDING": pymongo.ASCENDING, "TEXT": pymongo.TEXT}

    def databaseExists(self, databaseName):
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
        return self.__mgObj.list_database_names()

    def createDatabase(self, databaseName, overWrite=True):
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
        try:
            self.__mgObj.drop_database(databaseName)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def databaseCommand(self, databaseName, command):
        try:
            rs = self.__mgObj[databaseName].command(command)
            logger.debug("Database %s Command %r returns %r", databaseName, command, rs)
            return True
        except Exception as e:
            logger.exception("Database %s command %s failing with %s", databaseName, command, str(e))
        return False

    def collectionExists(self, databaseName, collectionName):
        try:
            if self.databaseExists(databaseName) and (collectionName in self.__mgObj[databaseName].list_collection_names()):
                return True
            else:
                return False
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def getCollectionNames(self, databaseName):
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
        try:
            ok = self.__mgObj[databaseName].drop_collection(collectionName)
            logger.debug("Return from drop collection %r", ok)
            return True
        except Exception as e:
            logger.error("Failing drop collection for databaseName %s collectionName %s with %s", databaseName, collectionName, str(e))
        return False

    def insert(self, databaseName, collectionName, dObj, documentKey=None):
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
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            rV = clt.insert_many(dList, ordered=ordered, bypass_document_validation=bypassValidation)
        except Exception as e:
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
        try:
            rIdL = []
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
        try:
            cD = {}
            delTupL = []
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            for dD in dList:
                kyVals = self.__getKeyValues(dD, keyNames)
                selectD = {ky: val for ky, val in zip(keyNames, kyVals)}
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
            return delTupL
        except Exception as e:
            logger.error("Failing %s and %s selectD %r with %s", databaseName, collectionName, selectD.items(), str(e))
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
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            clt.drop_index(indexName)
            logger.debug("Current indexes for %s %s : %r", databaseName, collectionName, clt.list_indexes())
            return True
        except Exception as e:
            logger.error("Failing %s and %s with %s", databaseName, collectionName, str(e))
        return False

    def reIndex(self, databaseName, collectionName):
        try:
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            logger.debug("Current indexes for %s %s : %r", databaseName, collectionName, clt.list_indexes())
            clt.reindex()
            return True
        except Exception as e:
            logger.exception("Failing %s and %s with %s", databaseName, collectionName, str(e))
        return False

    def fetch(self, databaseName, collectionName, selectL, queryD=None, suppressId=False):
        """Fetch selections (selectL) from documents satisfying input
        query constraints.
        """
        dList = []
        sD = {}
        try:
            qD = queryD if queryD is not None else None
            if selectL:
                sD = {k: 1 for k in selectL}
                if suppressId:
                    sD["_id"] = 0
            else:
                if suppressId:
                    sD["_id"] = 0
                else:
                    sD = None
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            # logger.debug("Got collection object %r" % c)
            for dD in clt.find(filter=qD, projection=sD):
                # logger.debug("Got doc %r" % d)
                dList.append(dD)
            return dList
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None

    def count(self, databaseName, collectionName, countFilter=None):
        try:
            tF = countFilter if countFilter else {}
            clt = self.__mgObj[databaseName].get_collection(collectionName)
            logger.debug("Current indexes for %s %s : %r", databaseName, collectionName, clt.list_indexes())
            return clt.count_documents(tF)
        except Exception as e:
            logger.exception("Failing for %s and %s with %s", databaseName, collectionName, str(e))
        return 0

    def distinct(self, databaseName, collectionName, ky):
        """Return a list of distinct values for the input key in the collection."""
        rL = []
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
