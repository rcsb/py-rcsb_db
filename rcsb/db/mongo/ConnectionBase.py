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


import copy
import logging
import platform

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus

logger = logging.getLogger(__name__)


if platform.system() == "Linux":
    try:
        pass
    except Exception:
        logger.exception("Loading Linux feature failing")


class ConnectionBase(object):

    def __init__(self, verbose=False):
        self.__verbose = verbose
        #
        self.__infoD = {}
        self.__dbClient = None

        self.__databaseName = None
        self.__dbHost = None
        self.__dbUser = None
        self.__dbPw = None
        self.__dbSocket = None
        self.__dbPort = None
        self.__dbAdminDb = None
        self.__dbPort = None
        self.__defaultPort = 27017
        self.__dbServer = 'mongo'
        self.__resourceName = None

    def assignResource(self, resourceName=None):
        # implement in the derived class
        self._assignResource(resourceName)

    def _assignResource(self, resourceName):
        self.__resourceName = resourceName

    def getPreferences(self):
        return self.__infoD

    def setPreferences(self, infoD):
        try:
            self.__infoD = copy.deepcopy(infoD)
            self.__databaseName = self.__infoD.get("DB_NAME", None)
            self.__dbHost = self.__infoD.get("DB_HOST", 'localhost')
            self.__dbUser = self.__infoD.get("DB_USER", None)
            self.__dbPw = self.__infoD.get("DB_PW", None)
            self.__dbSocket = self.__infoD.get("DB_SOCKET", None)
            self.__dbServer = self.__infoD.get("DB_SERVER", "mongo")
            self.__dbAdminDb = self.__infoD.get("DB_ADMIN_DB_NAME", "admin")
            self.__writeConcern = self.__infoD.get("DB_WRITE_CONCERN", "majority")
            self.__readConcern = self.__infoD.get("DB_READ_CONCERN", "majority")
            self.__readPreference = self.__infoD.get("DB_READ_PREFERENCE", "nearest")
            self.__writeJournalOpt = self.__infoD.get("DB_WRITE_TO_JOURNAL", True)
            #
            port = self.__infoD.get("DB_PORT", self.__defaultPort)
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
            logger.warning("Closing an existing open connection.")
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
            # logger.debug("URI is %s" % uri)
            self.__dbClient = MongoClient(uri, **kw)
        except Exception as e:
            logger.error("Connection to resource %s failing with %s" % (self.__resourceName, str(e)))

        try:
            # The ismaster command is cheap and does not require auth.
            d = self.__dbClient.admin.command('ismaster')
            logger.debug("Server status: %r " % d)
            return True
        except ConnectionFailure:
            logger.exception("Connection failing to resource %s " % self.__resourceName)

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
