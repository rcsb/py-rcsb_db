##
# File:  MongoDbUtil.py
# Date:  12-Mar-2018 J. Westbrook
#
# Update:
#      17-Mar-2018  jdw add replace and index ops
#      19-Mar-2018  jdw reorganize error handling for bulk insert
#      24-Mar-2018  jdw add salvage path for bulk insert
##
"""
Base class for simple essential database operations for MongoDb.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
logger = logging.getLogger(__name__)
#
import pymongo
#


class MongoDbUtil(object):

    def __init__(self, mongoClientObj, verbose=False):
        self.__verbose = verbose
        self.__mgObj = mongoClientObj
        self.__mongoIndexTypes = {'DESCENDING': pymongo.DESCENDING, 'ASCENDING': pymongo.ASCENDING, 'TEXT': pymongo.TEXT}

    def databaseExists(self, databaseName):
        try:
            dbNameList = self.__mgObj.list_database_names()
            if databaseName in dbNameList:
                return True
            else:
                return False
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def getDatabaseNames(self):
        return self.__mgObj.list_database_names()

    def createDatabase(self, databaseName, overWrite=True):
        try:
            if overWrite and self.databaseExists(databaseName):
                logger.debug("Dropping existing database %s" % databaseName)
                self.__mgObj.drop_database(databaseName)
            db = self.__mgObj[databaseName]
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def dropDatabase(self, databaseName):
        try:
            self.__mgObj.drop_database(databaseName)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def collectionExists(self, databaseName, collectionName):
        try:
            if self.databaseExists(databaseName) and (collectionName in self.__mgObj[databaseName].collection_names()):
                return True
            else:
                return False
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def getCollectionNames(self, databaseName):
        return self.__mgObj[databaseName].collection_names()

    def createCollection(self, databaseName, collectionName, overWrite=True):
        try:
            if overWrite and self.collectionExists(databaseName, collectionName):
                self.__mgObj[databaseName].drop_collection(collectionName)
            #
            ok = self.__mgObj[databaseName].create_collection(collectionName)
            logger.debug("Return from create collection %r " % ok)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def dropCollection(self, databaseName, collectionName):
        try:
            ok = self.__mgObj[databaseName].drop_collection(collectionName)
            logger.debug("Return from drop collection %r " % ok)
            return True
        except Exception as e:
            logger.error("Failing with %s" % str(e))
        return False

    def insert(self, databaseName, collectionName, dObj):
        try:
            c = self.__mgObj[databaseName].get_collection(collectionName)
            r = c.insert_one(dObj)
            try:
                rId = r.inserted_id
                return rId
            except Exception as e:
                logger.debug("Failing with %s" % str(e))
                return None
        except Exception as e:
            logger.error("Failing with %s" % str(e))
        return None

    def insertList(self, databaseName, collectionName, dList, keyName, ordered=False, bypassValidation=True):
        rIdL = []
        try:
            c = self.__mgObj[databaseName].get_collection(collectionName)
            r = c.insert_many(dList, ordered=ordered, bypass_document_validation=bypassValidation)
        except Exception as e:
            logger.error("Bulk insert operation failing with %s" % str(e))
        #
        try:
            rIdL = r.inserted_ids
            return rIdL
        except Exception as e:
            logger.debug("Bulk insert document Id recovery failing with %s" % str(e))
            return self.___salvageInsertList(databaseName, collectionName, dList, keyName)

        return rIdL

    def insertListSerial(self, databaseName, collectionName, dList, keyName):
        rIdL = []
        try:
            for d in dList:
                kyVal = self.__dictGet(d, keyName)
                rId = self.insert(databaseName, collectionName, d)
                if rId:
                    rIdL.append(rId)
                else:
                    logger.error("Loading document %r failed" % kyVal)
        except Exception as e:
            logger.exception("Failing %s and %s selectD %r with %s" % (databaseName, collectionName, keyName, str(e)))
        #
        return rIdL

    def ___salvageInsertList(self, databaseName, collectionName, dList, keyName):
        ''' Delete and serially insert the input document list.   Return the list list
            of documents ids successfully loaded.
        '''
        dTupL = self.deleteList(databaseName, collectionName, dList, keyName)
        logger.debug("Salvage bulk insert - deleting %d documents" % len(dTupL))
        return self.insertListSerial(databaseName, collectionName, dList, keyName)

    def fetchOne(self, databaseName, collectionName, ky, val):
        try:
            c = self.__mgObj[databaseName].get_collection(collectionName)
            dObj = c.find_one({ky: val})
            return dObj
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return None

    def replace(self, databaseName, collectionName, dObj, selectD, upsertFlag=True):
        try:
            c = self.__mgObj[databaseName].get_collection(collectionName)
            r = c.replace_one(selectD, dObj, upsert=upsertFlag)
            logger.debug("Replace returns  %r" % r)
            try:
                rId = r.upserted_id
                numMatched = r.matched_count
                numModified = r.modified_count
                logger.debug("Replacement mathed %d modified %d with _id %s" % (numMatched, numModified, rId))
                return rId
            except Exception as e:
                logger.error("Failing %s and %s selectD %r with %s" % (databaseName, collectionName, selectD, str(e)))
                return None
        except Exception as e:
            logger.exception("Failing %s and %s selectD %r with %s" % (databaseName, collectionName, selectD, str(e)))
        return None

    def replaceList(self, databaseName, collectionName, dList, keyName, upsertFlag=True):
        """ Replace the list of input documents based on a selection query by keyName -
            Note: splitting keyName (dot notation for Mongo) to divided keys for Python
        """
        try:
            rIdL = []
            c = self.__mgObj[databaseName].get_collection(collectionName)
            for d in dList:
                kyVal = self.__dictGet(d, keyName)
                selectD = {keyName: kyVal}
                r = c.replace_one(selectD, d, upsert=upsertFlag)
                try:
                    rIdL.append(r.upserted_id)
                except Exception as e:
                    logger.error("Failing %s and %s selectD %r with %s" % (databaseName, collectionName, keyName, str(e)))
        except Exception as e:
            logger.error("Failing %s and %s selectD %r with %s" % (databaseName, collectionName, keyName, str(e)))
        #
        return rIdL

    def deleteList(self, databaseName, collectionName, dList, keyName):
        """ Delete the list of input documents based on a selection query by keyName -

            Note: splitting keyName (dot notation for Mongo) to divided keys for Python
        """
        try:
            delTupL = []
            c = self.__mgObj[databaseName].get_collection(collectionName)
            for d in dList:
                kyVal = self.__dictGet(d, keyName)
                selectD = {keyName: kyVal}
                r = c.delete_many(selectD)
                try:
                    delTupL.append((kyVal, r.deleted_count))
                except Exception as e:
                    logger.error("Failing %s and %s selectD %r with %s" % (databaseName, collectionName, keyName, str(e)))
                logger.debug("Deleted status %r" % delTupL)
            return delTupL
        except Exception as e:
            logger.error("Failing %s and %s selectD %r with %s" % (databaseName, collectionName, keyName, str(e)))
        #
        return delTupL

    def __dictGet(self, dct, dotNotation):
        """  Convert input dictionary key (dot notation) to divided Python format and return appropriate dictionary value.
        """
        try:
            kys = dotNotation.split('.')
            for key in kys:
                try:
                    dct = dct[key]
                except KeyError:
                    return None
            return dct
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return None

    def createIndex(self, databaseName, collectionName, keyList, indexName="primary", indexType="DESCENDING", uniqueFlag=False):

        try:
            iTupL = [(ky, self.__mongoIndexTypes[indexType]) for ky in keyList]
            c = self.__mgObj[databaseName].get_collection(collectionName)
            c.create_index(iTupL, name=indexName, background=True, unique=uniqueFlag)
            logger.debug("Current indexes for %s %s : %r" % (databaseName, collectionName, c.list_indexes()))
            return True
        except Exception as e:
            logger.error("Failing %s and %s keyList %r with %s" % (databaseName, collectionName, keyList, str(e)))
        return False

    def dropIndex(self, databaseName, collectionName, indexName="primary"):
        try:
            c = self.__mgObj[databaseName].get_collection(collectionName)
            c.drop_index(indexName)
            logger.debug("Current indexes for %s %s : %r" % (databaseName, collectionName, c.list_indexes()))
            return True
        except Exception as e:
            logger.error("Failing %s and %s with %s" % (databaseName, collectionName, str(e)))
        return False

    def reIndex(self, databaseName, collectionName):
        try:
            c = self.__mgObj[databaseName].get_collection(collectionName)
            logger.debug("Current indexes for %s %s : %r" % (databaseName, collectionName, c.list_indexes()))
            c.reindex()
            return True
        except Exception as e:
            logger.exception("Failing %s and %s with %s" % (databaseName, collectionName, str(e)))
        return False

    def fetch(self, databaseName, collectionName, selectL, queryD=None):
        """ Fetch selections (selectL) from documents satisfying input
            query constraints.
        """
        try:
            qD = queryD if queryD is not None else {}
            sD = {k: 1 for k in selectL}
            c = self.__mgObj[databaseName].get_collection(collectionName)
            dList = c.find(qD, sD)
            return dList
        except Exception as e:
            logger.error("Failing with %s" % str(e))
        return None
