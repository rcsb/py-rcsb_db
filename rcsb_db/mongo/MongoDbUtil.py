##
# File:  MongoDbUtil.py
# Date:  12-Mar-2018 J. Westbrook
#
# Update:
#
##
"""
Base class for simple essential database operations for MongoDb.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import copy

import logging
logger = logging.getLogger(__name__)
#
#
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class MongoDbUtil(object):

    def __init__(self, mongoClientObj, verbose=False):
        self.__verbose = verbose
        self.__mgObj = mongoClientObj

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
            logger.exception("Failing with %s" % str(e))
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
            logger.exception("Failing with %s" % str(e))
        return None

    def insertList(self, databaseName, collectionName, dList):
        try:
            c = self.__mgObj[databaseName].get_collection(collectionName)
            r = c.insert_many(dList)
            try:
                rIdL = r.inserted_ids
                return rIdL
            except Exception as e:
                logger.debug("Failing with %s" % str(e))
                return None
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return None

    def fetchOne(self, databaseName, collectionName, ky, val):
        try:
            c = self.__mgObj[databaseName].get_collection(collectionName)
            dObj = c.find_one({ky: val})
            return dObj
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return None

