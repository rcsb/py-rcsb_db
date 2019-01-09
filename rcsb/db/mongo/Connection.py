##
# File:  Connection.py
# Date:  25-Mar-2018 J. Westbrook
#
# Update:
#   1-Apr-2018 jdw add context methods
#  23-Oct-2018 jdw add section name config access methods and make this a constructor argument
#   5-Dec-2018 jdw pass on exceptions from the context manager __exit__() method
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

    def __init__(self, cfgOb=None, infoD=None, resourceName=None, sectionName='site_info', verbose=False):
        super(Connection, self).__init__(verbose=verbose)
        #
        self.__cfgOb = cfgOb

        #
        if infoD:
            self.setPreferences(infoD)
        #
        if resourceName:
            self.assignResource(resourceName, sectionName)
        #

    def assignResource(self, resourceName=None, sectionName=None):
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
            infoD["DB_NAME"] = self.__cfgOb.get("EXCHANGE_DB_NAME", sectionName=sectionName)
            infoD["DB_HOST"] = self.__cfgOb.get("EXCHANGE_DB_HOST", sectionName=sectionName)
            infoD["DB_SOCKET"] = self.__cfgOb.get("EXCHANGE_DB_SOCKET", default=None, sectionName=sectionName)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("EXCHANGE_DB_PORT", default=defaultPort, sectionName=sectionName)))
            infoD["DB_USER"] = self.__cfgOb.get("EXCHANGE_DB_USER_NAME", sectionName=sectionName)
            infoD["DB_PW"] = self.__cfgOb.get("EXCHANGE_DB_PASSWORD", sectionName=sectionName)
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("EXCHANGE_DB_ADMIN_DB_NAME", default='admin', sectionName=sectionName)
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("EXCHANGE_DB_WRITE_CONCERN", default="majority", sectionName=sectionName)
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("EXCHANGE_DB_READ_CONCERN", default="majority", sectionName=sectionName)
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("EXCHANGE_DB_READ_PREFERENCE", default="nearest", sectionName=sectionName)
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("EXCHANGE_DB_WRITE_TO_JOURNAL", default=True, sectionName=sectionName)
        elif (resourceName == "MONGO_DB"):
            infoD["DB_NAME"] = self.__cfgOb.get("MONGO_DB_NAME", sectionName=sectionName)
            infoD["DB_HOST"] = self.__cfgOb.get("MONGO_DB_HOST", default=defaultHost, sectionName=sectionName)
            infoD["DB_SOCKET"] = self.__cfgOb.get("MONGO_DB_SOCKET", default=None, sectionName=sectionName)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("MONGO_DB_PORT", default=defaultPort, sectionName=sectionName)))
            infoD["DB_USER"] = self.__cfgOb.get("MONGO_DB_USER_NAME", sectionName=sectionName)
            infoD["DB_PW"] = self.__cfgOb.get("MONGO_DB_PASSWORD", sectionName=sectionName)
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("MONGO_DB_ADMIN_DB_NAME", default='admin', sectionName=sectionName)
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("MONGO_DB_WRITE_CONCERN", default="majority", sectionName=sectionName)
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("MONGO_DB_READ_CONCERN", default="majority", sectionName=sectionName)
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("MONGO_DB_READ_PREFERENCE", default="nearest", sectionName=sectionName)
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("MONGO_DB_WRITE_TO_JOURNAL", default=True, sectionName=sectionName)
        else:
            infoD["DB_NAME"] = self.__cfgOb.get("DB_NAME", sectionName=sectionName)
            infoD["DB_HOST"] = self.__cfgOb.get("DB_HOST", default=defaultHost, sectionName=sectionName)
            infoD["DB_SOCKET"] = self.__cfgOb.get("DB_SOCKET", default=None, sectionName=sectionName)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("DB_PORT", default=defaultPort, sectionName=sectionName)))
            infoD["DB_USER"] = self.__cfgOb.get("DB_USER_NAME", sectionName=sectionName)
            infoD["DB_PW"] = self.__cfgOb.get("DB_PASSWORD", sectionName=sectionName)
            infoD["DB_ADMIN_DB_NAME"] = self.__cfgOb.get("DB_ADMIN_DB_NAME", default='admin', sectionName=sectionName)
            infoD["DB_WRITE_CONCERN"] = self.__cfgOb.get("DB_WRITE_CONCERN", default="majority", sectionName=sectionName)
            infoD["DB_READ_CONCERN"] = self.__cfgOb.get("DB_READ_CONCERN", default="majority", sectionName=sectionName)
            infoD["DB_READ_PREFERENCE"] = self.__cfgOb.get("DB_READ_PREFERENCE", default="nearest", sectionName=sectionName)
            infoD["DB_WRITE_TO_JOURNAL"] = self.__cfgOb.get("DB_WRITE_TO_JOURNAL", default=True, sectionName=sectionName)
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
        if args[0]:
            raise
        return self.closeConnection()
