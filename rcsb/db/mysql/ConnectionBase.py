##
# File:  ConnectionBase.py
# Date:  25-Mar-2018 J. Westbrook
#
# Update:
##
"""
Base class for managing database connection for MySQL.  Application credentials are
handled by the derived class.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import copy
import logging
import platform

import MySQLdb

logger = logging.getLogger(__name__)
#
#
if platform.system() == "Linux":
    try:
        import sqlalchemy.pool as pool  # pylint: disable=import-error

        MySQLdb = pool.manage(MySQLdb, pool_size=12, max_overflow=12, timeout=30, echo=False, use_threadlocal=False)
    except Exception as e:
        logger.exception("Creating MYSQL connection pool failing with %s", str(e))


class ConnectionBase(object):
    def __init__(self, siteId=None, verbose=False):
        self.__verbose = verbose
        #
        self.__siteId = siteId

        self.__db = None
        self._dbCon = None

        self.__infoD = {}
        self.__databaseName = None
        self.__dbHost = None
        self.__dbUser = None
        self.__dbPw = None
        self.__dbSocket = None
        self.__dbPort = None
        self.__dbAdminDb = None
        self.__dbPort = None
        self.__defaultPort = 3306
        self.__dbServer = "mysql"
        self.__resourceName = None
        self.__sectionName = None

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
            self.__databaseName = self.__infoD.get("DB_NAME", None)
            self.__dbHost = self.__infoD.get("DB_HOST", "localhost")
            self.__dbUser = self.__infoD.get("DB_USER", None)
            self.__dbPw = self.__infoD.get("DB_PW", None)
            self.__dbSocket = self.__infoD.get("DB_SOCKET", None)
            self.__dbServer = self.__infoD.get("DB_SERVER", "mysql")
            #
            port = self.__infoD.get("DB_PORT", self.__defaultPort)
            if port is not None:
                self.__dbPort = int(str(port))
        except Exception as e:
            logger.exception("Failing with %s", str(e))

    def openConnection(self):
        """Create a database connection and return a connection object.

        Returns None on failure
        """
        #
        if self._dbCon is not None:
            # Close an open connection -
            logger.info("+MyDbConnect.connect() WARNING Closing an existing connection.")
            self.closeConnection()

        try:
            if self.__dbSocket is None:
                dbcon = MySQLdb.connect(
                    db="%s" % self.__databaseName, user="%s" % self.__dbUser, passwd="%s" % self.__dbPw, host="%s" % self.__dbHost, port=self.__dbPort, local_infile=1
                )
            else:
                dbcon = MySQLdb.connect(
                    db="%s" % self.__databaseName,
                    user="%s" % self.__dbUser,
                    passwd="%s" % self.__dbPw,
                    host="%s" % self.__dbHost,
                    port=self.__dbPort,
                    unix_socket="%s" % self.__dbSocket,
                    local_infile=1,
                )

            self._dbCon = dbcon
            return True
        except Exception as e:
            logger.exception("Connection error to resource %s with %s", self.__resourceName, str(e))
            self._dbCon = None

        return False

    def getClientConnection(self):
        return self._dbCon

    def closeConnection(self):
        """Close db session"""
        if self._dbCon is not None:
            self._dbCon.close()
            self._dbCon = None
            return True
        else:
            return False

    def getCursor(self):
        try:
            return self._dbCon.cursor()
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return None
