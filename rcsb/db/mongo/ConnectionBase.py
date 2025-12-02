##
# File:  ConnectionBase.py
# Date:  12-Mar-2018 J. Westbrook
#
# Update:
#    17-Mar-2018 jdw  add r/w sync controls - generalize auth to prefs
#    13-Aug-2024 dwp  update keywords for pymongo 4.x support
#    13-Aug-2025 dwp  make use of configured port number in URI string
#    13-Nov-2025 mjt  set URI with DB_URI instead of building it, if available
#     2-Dec-2025 dwp  adjust mongo option priority to first use explicit settings, else use URI-provided options
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

from urllib.parse import quote_plus, urlparse, parse_qs

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

        self.__dbUri = None
        self.__databaseName = None
        self.__dbHost = None
        self.__dbUser = None
        self.__dbPw = None
        self.__dbSocket = None
        self.__dbPort = None
        self.__dbAdminDb = None
        self.__dbPort = None
        self.__defaultPort = 27017
        self.__dbServer = "mongo"
        self.__resourceName = None
        self.__sectionName = None
        self.__writeConcern = None
        self.__readConcern = None
        self.__readPreference = None
        self.__writeJournalOpt = None
        self.__connectTimeoutMS = None
        self.__socketTimeoutMS = None
        self.__appname = None

    def assignResource(self, resourceName=None, sectionName=None):
        # implement in the derived class
        self._assignResource(resourceName, sectionName)

    def _assignResource(self, resourceName, sectionName):
        self.__resourceName = resourceName
        self.__sectionName = sectionName

    def getPreferences(self):
        return self.__infoD

    def setPreferences(self, infoD):
        try:
            self.__infoD = copy.deepcopy(infoD)
            self.__dbUri = self.__infoD.get("DB_URI", None)
            #
            # Extract out URI options into a dictionary
            uriKwargD = {k: v[0] for k, v in parse_qs(urlparse(self.__dbUri).query).items()} if self.__dbUri else {}
            # Priority of settings usage is:
            # (1) Explicit config settings take precedence if provided (e.g., "DB_READ_PREFERENCE", ...)
            # (2) If not explicitly defined, use URI options if provided
            # (3) Else fallback to defaults defined below
            #
            self.__databaseName = self.__infoD.get("DB_NAME", None)
            self.__dbHost = self.__infoD.get("DB_HOST", "localhost")
            self.__dbUser = self.__infoD.get("DB_USER", None)
            self.__dbPw = self.__infoD.get("DB_PW", None)
            self.__dbSocket = self.__infoD.get("DB_SOCKET", None)
            self.__dbServer = self.__infoD.get("DB_SERVER", "mongo")
            self.__dbAdminDb = self.__infoD.get("DB_ADMIN_DB_NAME") if self.__infoD.get("DB_ADMIN_DB_NAME") is not None else uriKwargD.get("authSource", "admin")
            self.__writeConcern = self.__infoD.get("DB_WRITE_CONCERN") if self.__infoD.get("DB_WRITE_CONCERN") is not None else uriKwargD.get("w", "majority")
            self.__readConcern = self.__infoD.get("DB_READ_CONCERN") if self.__infoD.get("DB_READ_CONCERN") is not None else uriKwargD.get("readConcernLevel", "majority")
            self.__readPreference = self.__infoD.get("DB_READ_PREFERENCE") if self.__infoD.get("DB_READ_PREFERENCE") is not None else uriKwargD.get("readPreference", "nearest")
            self.__writeJournalOpt = self.__infoD.get("DB_WRITE_TO_JOURNAL") if self.__infoD.get("DB_WRITE_TO_JOURNAL") is not None else uriKwargD.get("journal", True)
            self.__connectTimeoutMS = (
                self.__infoD.get("DB_CONNECTION_TIMEOUT_MS")
                if self.__infoD.get("DB_CONNECTION_TIMEOUT_MS") is not None
                else uriKwargD.get("connectTimeoutMS", 60000)
            )
            self.__socketTimeoutMS = self.__infoD.get("DB_SOCKET_TIMEOUT_MS") if self.__infoD.get("DB_SOCKET_TIMEOUT_MS") is not None else uriKwargD.get("socketTimeoutMS", None)
            self.__appname = self.__infoD.get("DB_APP_NAME") if self.__infoD.get("DB_APP_NAME") is not None else uriKwargD.get("appname", "dbloader")
            #
            port = self.__infoD.get("DB_PORT", self.__defaultPort)
            if port and str(port):
                self.__dbPort = int(str(port))
        except Exception as e:
            logger.exception("Failing with %s", str(e))

    def openConnection(self):
        """Create a database connection and store a connection object.

        Returns True for success or False otherwise
        """
        #
        if self.__dbClient is not None:
            # Close an open connection -
            logger.warning("Closing an existing open connection.")
            self.closeConnection()

        try:
            if self.__dbUri:
                # expects complete URI, with any additional options. Ex: mongodb://<username>:<password>@<ip>:<port>
                uri = self.__dbUri
            elif self.__dbUser and self.__dbPw and self.__dbPort:
                uri = "mongodb://%s:%s@%s:%d/%s" % (quote_plus(self.__dbUser), quote_plus(self.__dbPw), self.__dbHost, self.__dbPort, self.__dbAdminDb)
            elif self.__dbUser and self.__dbPw:
                uri = "mongodb://%s:%s@%s/%s" % (quote_plus(self.__dbUser), quote_plus(self.__dbPw), self.__dbHost, self.__dbAdminDb)
            else:
                uri = "mongodb://%s:%d" % (self.__dbHost, self.__dbPort)

            kw = {}
            kw["w"] = self.__writeConcern
            kw["journal"] = self.__writeJournalOpt
            kw["appname"] = self.__appname
            kw["readConcernLevel"] = self.__readConcern
            kw["readPreference"] = self.__readPreference
            kw["connectTimeoutMS"] = self.__connectTimeoutMS
            kw["socketTimeoutMS"] = self.__socketTimeoutMS
            #
            # logger.debug("URI is %s" % uri)
            self.__dbClient = MongoClient(uri, **kw)
        except Exception as e:
            logger.error("Connection to resource %s failing with %s", self.__resourceName, str(e))
        dD = {}
        try:
            dD = self.__dbClient.admin.command("hello")
            # logger.debug("Server status: %r", dD)
            return True
        except ConnectionFailure:
            logger.exception("Connection %r failing to resource %s", dD, self.__resourceName)

        self.__dbClient = None

        return False

    def getClientConnection(self):
        """Return an instance of a connected client."""
        return self.__dbClient

    def closeConnection(self):
        """Close db session"""
        if self.__dbClient is not None:
            self.__dbClient.close()
            self.__dbClient = None
            return True
        else:
            return False
