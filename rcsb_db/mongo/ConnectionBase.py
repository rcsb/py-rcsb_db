##
# File:  ConnectionBase.py
# Date:  12-Mar-2018 J. Westbrook
#
# Update:
#    17-Mar-2018 jdw  add r/w sync controls - generalize auth to prefs
##
"""
Base class for managing database connection which handles application specific authentication.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import platform
import copy

try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus

import logging
logger = logging.getLogger(__name__)
#
#
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

if platform.system() == "Linux":
    try:
        pass
    except Exception as e:
        logger.exception("Loading Linux feature failing")


class ConnectionBase(object):

    def __init__(self, siteId=None, verbose=False):
        self.__verbose = verbose
        #
        self.__siteId = siteId
        self.__db = None
        self.__dbClient = None
        self.__prefD = {}
        self.__databaseName = None
        self.__dbHost = None
        self.__dbUser = None
        self.__dbPw = None
        self.__dbSocket = None
        self.__dbPort = None
        self.__dbAdminDb = None
        self.__dbPort = 27017
        self.__dbServer = 'mongo'

    def setResource(self, resourceName=None, realm='RCSB'):
        #
        if (resourceName == "EXCHANGE_DB"):
            self.__databaseName = self._cI.get("SITE_EXCHANGE_DB_NAME")
            self.__dbHost = self._cI.get("SITE_EXCHANGE_DB_HOST_NAME")
            self.__dbSocket = self._cI.get("SITE_EXCHANGE_DB_SOCKET")
            self.__dbPort = self._cI.get("SITE_EXCHANGE_DB_PORT_NUMBER")
            self.__dbUser = self._cI.get("SITE_EXCHANGE_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_EXCHANGE_DB_PASSWORD")
            self.__dbAdminDb = self._cI.get("SITE_EXCHANGE_ADMIN_DB_NAME")
        else:
            pass

        if self.__dbSocket is None or len(self.__dbSocket) < 2:
            self.__dbSocket = None

        if self.__dbPort is None:
            self.__dbPort = 27017
        else:
            self.__dbPort = int(str(self.__dbPort))

        logger.debug("+ConnectionBase(setResource) %s resource name %s server %s dns %s host %s user %s socket %s port %r admindb %s" %
                     (self.__siteId, resourceName, self.__dbServer, self.__databaseName, self.__dbHost, self.__dbUser, self.__dbSocket, self.__dbPort, self.__dbAdminDb))
        #
        self.__prefD["DB_NAME"] = self.__databaseName
        self.__prefD["DB_HOST"] = self.__dbHost
        self.__prefD["DB_USER"] = self.__dbUser
        self.__prefD["DB_PW"] = self.__dbPw
        self.__prefD["DB_SOCKET"] = self.__dbSocket
        self.__prefD["DB_PORT"] = int(str(self.__dbPort))
        self.__prefD["DB_SERVER"] = self.__dbServer
        self.__prefD["DB_ADMIN_DB_NAME"] = self.__dbAdminDb
        #

    def getPreferences(self):
        return self.__prefD

    def setPreferences(self, prefD):
        try:
            self.__prefD = copy.deepcopy(prefD)
            self.__databaseName = self.__prefD.get("DB_NAME", None)
            self.__dbHost = self.__prefD.get("DB_HOST", 'localhost')
            self.__dbUser = self.__prefD.get("DB_USER", None)
            self.__dbPw = self.__prefD.get("DB_PW", None)
            self.__dbSocket = self.__prefD.get("DB_SOCKET", None)
            self.__dbServer = self.__prefD.get("DB_SERVER", "mongo")
            self.__dbAdminDb = self.__prefD.get("DB_ADMIN_DB_NAME", "admin")
            self.__writeConcern = self.__prefD.get("DB_WRITE_CONCERN", "majority")
            self.__readConcern = self.__prefD.get("DB_READ_CONCERN", "majority")
            self.__readPreference = self.__prefD.get("DB_READ_PREFERENCE", "nearest")
            self.__writeJournalOpt = self.__prefD.get("DB_WRITE_TO_JOURNAL", True)
            #
            port = self.__prefD.get("DB_PORT", 27017)
            if port and len(str(port)) > 0:
                self.__dbPort = int(str(port))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

    def openConnection(self):
        """ Create a database connection and store a connection object.

            Returns True for success or False otherwise
        """
        #
        if self.__dbClient is not None:
            # Close an open connection -
            logger.info("+MyDbConnect.connect() WARNING Closing an existing connection.")
            self.closeConnection()

        try:
            if self.__dbUser and (len(self.__dbUser) > 0) and self.__dbPw and (len(self.__dbPw) > 0):
                uri = "mongodb://%s:%s@%s/%s" % (quote_plus(self.__dbUser), quote_plus(self.__dbPw), self.__dbHost, self.__dbAdminDb)
            else:
                uri = "mongodb://%s:%d" % (self.__dbHost, self.__dbPort)

            kw = {}
            kw['w'] = self.__writeConcern
            kw['j'] = True
            kw['appname'] = 'dbloader'
            kw['readConcernLevel'] = self.__readConcern
            kw['readPreference'] = self.__readPreference
            #
            logger.debug("URI is %s" % uri)
            self.__dbClient = MongoClient(uri, **kw)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        try:
            # The ismaster command is cheap and does not require auth.
            d = self.__dbClient.admin.command('ismaster')
            logger.debug("Server status: %r " % d)
            return True
        except ConnectionFailure:
            logger.exception("Connection error to server %s host %s dsn %s user %s pw %s socket %s port %d \n" %
                             (self.__dbServer, self.__dbHost, self.__databaseName, self.__dbUser, self.__dbPw, self.__dbSocket, self.__dbPort))
        self.__dbClient = None

        return False

    def getClientConnection(self):
        """ Return an instance of a connected client.
        """
        return self.__dbClient

    def closeConnection(self):
        """ Close db session
        """
        if self.__dbClient is not None:
            self.__dbClient.close()
            self.__dbClient = None
            return True
        else:
            return False
