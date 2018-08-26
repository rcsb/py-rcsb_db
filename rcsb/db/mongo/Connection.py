##
# File:  Connection.py
# Date:  25-Mar-2018 J. Westbrook
#
# Update:
#   1-Apr-2018 jdw add context methods
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

from rcsb.db.mongo.ConnectionBase import ConnectionBase

logger = logging.getLogger(__name__)


class Connection(ConnectionBase):

    def __init__(self, cfgOb=None, infoD=None, resourceName=None, verbose=False):
        super(Connection, self).__init__(verbose=verbose)
        #
        self.__cfgOb = cfgOb
        #
        if infoD:
            self.setPreferences(infoD)
        #
        if resourceName:
            self.assignResource(resourceName)
        #

    def assignResource(self, resourceName=None):
        #
        defaultPort = 27017
        defaultHost = 'localhost'
        dbServer = 'mongo'

        self._assignResource(resourceName)
        infoD = {}
        if not self.__cfgOb:
            return infoD
        #
        if (resourceName == "EXCHANGE_DB"):
            infoD["DB_NAME"] = self.__cfgOb.get("EXCHANGE_DB_NAME")
            infoD["DB_HOST"] = self.__cfgOb.get("EXCHANGE_DB_HOST")
            infoD["DB_SOCKET"] = self.__cfgOb.get("EXCHANGE_DB_SOCKET", default=None)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("EXCHANGE_DB_PORT", default=defaultPort)))
            infoD["DB_USER"] = self.__cfgOb.get("EXCHANGE_DB_USER_NAME")
            infoD["DB_PW"] = self.__cfgOb.get("EXCHANGE_DB_PASSWORD")
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("EXCHANGE_DB_ADMIN_DB_NAME", default='admin')
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("EXCHANGE_DB_WRITE_CONCERN", default="majority")
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("EXCHANGE_DB_READ_CONCERN", default="majority")
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("EXCHANGE_DB_READ_PREFERENCE", default="nearest")
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("EXCHANGE_DB_WRITE_TO_JOURNAL", default=True)
        elif (resourceName == "MONGO_DB"):
            infoD["DB_NAME"] = self.__cfgOb.get("MONGO_DB_NAME")
            infoD["DB_HOST"] = self.__cfgOb.get("MONGO_DB_HOST", default=defaultHost)
            infoD["DB_SOCKET"] = self.__cfgOb.get("MONGO_DB_SOCKET", default=None)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("MONGO_DB_PORT", default=defaultPort)))
            infoD["DB_USER"] = self.__cfgOb.get("MONGO_DB_USER_NAME")
            infoD["DB_PW"] = self.__cfgOb.get("MONGO_DB_PASSWORD")
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("MONGO_DB_ADMIN_DB_NAME", default='admin')
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("MONGO_DB_WRITE_CONCERN", default="majority")
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("MONGO_DB_READ_CONCERN", default="majority")
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("MONGO_DB_READ_PREFERENCE", default="nearest")
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("MONGO_DB_WRITE_TO_JOURNAL", default=True)
        else:
            infoD["DB_NAME"] = self.__cfgOb.get("DB_NAME")
            infoD["DB_HOST"] = self.__cfgOb.get("DB_HOST", default=defaultHost)
            infoD["DB_SOCKET"] = self.__cfgOb.get("DB_SOCKET", default=None)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("DB_PORT", default=defaultPort)))
            infoD["DB_USER"] = self.__cfgOb.get("DB_USER_NAME")
            infoD["DB_PW"] = self.__cfgOb.get("DB_PASSWORD")
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("DB_ADMIN_DB_NAME", default='admin')
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("DB_WRITE_CONCERN", default="majority")
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("DB_READ_CONCERN", default="majority")
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("DB_READ_PREFERENCE", default="nearest")
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("DB_WRITE_TO_JOURNAL", default=True)
        #
        infoD['DB_SERVER'] = dbServer
        self.setPreferences(infoD)
        #
        return copy.deepcopy(infoD)
        #

    def __enter__(self):
        self.openConnection()
        return self.getClientConnection()

    def __exit__(self, *args):
        return self.closeConnection()
