##
# File:  Connection.py
# Date:  25-Mar-2018 J. Westbrook
#
# Update:
#
##
"""
Derived class for managing database connection which handles application specific authentication.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import copy
import logging
logger = logging.getLogger(__name__)

from rcsb_db.mongo.ConnectionBase import ConnectionBase


class Connection(ConnectionBase):

    def __init__(self, cfgOb=None, verbose=False):
        super(Connection, self).__init__(verbose=verbose)
        self.__verbose = verbose
        self.__cfgOb = cfgOb
        #
        self.__defaultPort = 27017
        self.__defaultHost = 'localhost'
        self.__dbServer = 'mongo'

    def assignResource(self, resourceName=None):
        #
        self._assignResource(resourceName)
        infoD = {}
        if not self.__cfgOb:
            return infoD
        #
        if (resourceName == "EXCHANGE_DB"):
            infoD["DB_NAME"] = self.__cfgOb.get("EXCHANGE_DB_NAME")
            infoD["DB_HOST"] = self.__cfgOb.get("EXCHANGE_DB_HOST")
            infoD["DB_SOCKET"] = self.__cfgOb.get("EXCHANGE_DB_SOCKET", defaultValue=None)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("EXCHANGE_DB_PORT", defaultValue=self.__defaultPort)))
            infoD["DB_USER"] = self.__cfgOb.get("EXCHANGE_DB_USER_NAME")
            infoD["DB_PW"] = self.__cfgOb.get("EXCHANGE_DB_PASSWORD")
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("EXCHANGE_DB_ADMIN_DB_NAME", defaultValue='admin')
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("EXCHANGE_DB_WRITE_CONCERN", defaultValue="majority")
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("EXCHANGE_DB_READ_CONCERN", defaultValue="majority")
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("EXCHANGE_DB_READ_PREFERENCE", defaultValue="nearest")
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("EXCHANGE_DB_WRITE_TO_JOURNAL", defaultValue=True)
        elif (resourceName == "MONGO_DB"):
            infoD["DB_NAME"] = self.__cfgOb.get("MONGO_DB_NAME")
            infoD["DB_HOST"] = self.__cfgOb.get("MONGO_DB_HOST", defaultValue=self.__defaultHost)
            infoD["DB_SOCKET"] = self.__cfgOb.get("MONGO_DB_SOCKET", defaultValue=None)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("MONGO_DB_PORT", defaultValue=self.__defaultPort)))
            infoD["DB_USER"] = self.__cfgOb.get("MONGO_DB_USER_NAME")
            infoD["DB_PW"] = self.__cfgOb.get("MONGO_DB_PASSWORD")
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("MONGO_DB_ADMIN_DB_NAME", defaultValue='admin')
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("MONGO_DB_WRITE_CONCERN", defaultValue="majority")
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("MONGO_DB_READ_CONCERN", defaultValue="majority")
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("MONGO_DB_READ_PREFERENCE", defaultValue="nearest")
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("MONGO_DB_WRITE_TO_JOURNAL", defaultValue=True)
        else:
            infoD["DB_NAME"] = self.__cfgOb.get("DB_NAME")
            infoD["DB_HOST"] = self.__cfgOb.get("DB_HOST", defaultValue=self.__defaultHost)
            infoD["DB_SOCKET"] = self.__cfgOb.get("DB_SOCKET", defaultValue=None)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("DB_PORT", defaultValue=self.__defaultPort)))
            infoD["DB_USER"] = self.__cfgOb.get("DB_USER_NAME")
            infoD["DB_PW"] = self.__cfgOb.get("DB_PASSWORD")
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("DB_ADMIN_DB_NAME", defaultValue='admin')
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("DB_WRITE_CONCERN", defaultValue="majority")
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("DB_READ_CONCERN", defaultValue="majority")
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("DB_READ_PREFERENCE", defaultValue="nearest")
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("DB_WRITE_TO_JOURNAL", defaultValue=True)
        #
        self.setPreferences(infoD)
        #
        return copy.deepcopy(infoD)
        #
