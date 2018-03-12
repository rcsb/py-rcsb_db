##
# File:  MyConnectionBase.py
# Date:  25-Jan-2013 J. Westbrook
#
# Update:
#  4-Feb-2013 jdw include resource for chemical components data.
# 13-Jul-2014 jdw add config for da_internal database
#  3-Mar-2016 jdw add support for non-standard port on connection -
# 30-Jan-2017 jdw all authentication now taken from configuration file -
# 16-Feb-2017 jdw add resource 'status'
# 20-Apr-2017 jdw adjusting pooling configuration
##
"""
Base class for managing database connection which handles application specific authentication.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"



import platform
import MySQLdb

import logging
logger = logging.getLogger(__name__)
#
#
if platform.system() == "Linux":
    try:
        import sqlalchemy.pool as pool
        MySQLdb = pool.manage(MySQLdb, pool_size=12, max_overflow=12, timeout=30, echo=False, use_threadlocal=False)
    except Exception as e:
        logger.exception("Creating MYSQL connection pool failing")


from wwpdb.api.facade.ConfigInfo import ConfigInfo


class MyConnectionBase(object):

    def __init__(self, siteId=None, verbose=False):
        self.__verbose = verbose
        #
        self.__siteId = siteId
        self._cI = ConfigInfo(self.__siteId)
        self.__db = None
        self._dbCon = None
        self.__authD = {}
        self.__databaseName = None
        self.__dbHost = None
        self.__dbUser = None
        self.__dbPw = None
        self.__dbSocket = None
        self.__dbPort = None
        self.__dbPort = 3306
        self.__dbServer = 'mysql'

    def setResource(self, resourceName=None):
        #
        if (resourceName == "PRD"):
            self.__databaseName = self._cI.get("SITE_REFDATA_PRD_DB_NAME")
            self.__dbHost = self._cI.get("SITE_REFDATA_DB_HOST_NAME")
            self.__dbSocket = self._cI.get("SITE_REFDATA_DB_SOCKET")
            self.__dbPort = self._cI.get("SITE_REFDATA_DB_PORT_NUMBER")

            self.__dbUser = self._cI.get("SITE_REFDATA_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_REFDATA_DB_PASSWORD")

        elif (resourceName == "CC"):
            self.__databaseName = self._cI.get("SITE_REFDATA_CC_DB_NAME")
            self.__dbHost = self._cI.get("SITE_REFDATA_DB_HOST_NAME")
            self.__dbSocket = self._cI.get("SITE_REFDATA_DB_SOCKET")
            self.__dbPort = self._cI.get("SITE_REFDATA_DB_PORT_NUMBER")

            self.__dbUser = self._cI.get("SITE_REFDATA_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_REFDATA_DB_PASSWORD")

        elif (resourceName == "RCSB_INSTANCE"):
            self.__databaseName = self._cI.get("SITE_INSTANCE_DB_NAME")
            self.__dbHost = self._cI.get("SITE_INSTANCE_DB_HOST_NAME")
            self.__dbSocket = self._cI.get("SITE_INSTANCE_DB_SOCKET")
            self.__dbPort = self._cI.get("SITE_INSTANCE_DB_PORT_NUMBER")

            self.__dbUser = self._cI.get("SITE_INSTANCE_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_INSTANCE_DB_PASSWORD")

        elif (resourceName == "DA_INTERNAL"):
            self.__databaseName = self._cI.get("SITE_DA_INTERNAL_DB_NAME")
            self.__dbHost = self._cI.get("SITE_DA_INTERNAL_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_DA_INTERNAL_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_DA_INTERNAL_DB_SOCKET")

            self.__dbUser = self._cI.get("SITE_DA_INTERNAL_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_DA_INTERNAL_DB_PASSWORD")

        elif (resourceName == "DA_INTERNAL_COMBINE"):
            self.__databaseName = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_NAME")
            self.__dbHost = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_SOCKET")

            self.__dbUser = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_PASSWORD")
        elif (resourceName == "DISTRO"):
            self.__databaseName = self._cI.get("SITE_DISTRO_DB_NAME")
            self.__dbHost = self._cI.get("SITE_DISTRO_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_DISTRO_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_DISTRO_DB_SOCKET")

            self.__dbUser = self._cI.get("SITE_DISTRO_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_DISTRO_DB_PASSWORD")

        elif (resourceName == "STATUS"):
            self.__databaseName = self._cI.get("SITE_DB_DATABASE_NAME")
            self.__dbHost = self._cI.get("SITE_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_DB_SOCKET")

            self.__dbUser = self._cI.get("SITE_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_DB_PASSWORD")
        else:
            pass

        if self.__dbSocket is None or len(self.__dbSocket) < 2:
            self.__dbSocket = None

        if self.__dbPort is None:
            self.__dbPort = 3306
        else:
            self.__dbPort = int(str(self.__dbPort))

        logger.info("+MyConnectionBase(setResource) %s resource name %s server %s dns %s host %s user %s socket %s port %r" %
                    (self.__siteId, resourceName, self.__dbServer, self.__databaseName, self.__dbHost, self.__dbUser, self.__dbSocket, self.__dbPort))
        #
        self.__authD["DB_NAME"] = self.__databaseName
        self.__authD["DB_HOST"] = self.__dbHost
        self.__authD["DB_USER"] = self.__dbUser
        self.__authD["DB_PW"] = self.__dbPw
        self.__authD["DB_SOCKET"] = self.__dbSocket
        self.__authD["DB_PORT"] = int(str(self.__dbPort))
        self.__authD["DB_SERVER"] = self.__dbServer
        #

    def getAuth(self):
        return self.__authD

    def setAuth(self, authD):
        try:
            self.__databaseName = self.__authD["DB_NAME"]
            self.__dbHost = self.__authD["DB_HOST"]
            self.__dbUser = self.__authD["DB_USER"]
            self.__dbPw = self.__authD["DB_PW"]
            self.__dbSocket = self.__authD["DB_SOCKET"]
            if 'DB_PORT' in self.__authD:
                self.__dbPort = int(str(self.__authD["DB_PORT"]))
            else:
                self.__dbPort = 3306
            self.__dbServer = self.__authD["DB_SERVER"]
        except Exception as e:
            pass

    def openConnection(self):
        """ Create a database connection and return a connection object.

            Returns None on failure
        """
        #
        if self._dbCon is not None:
            # Close an open connection -
            logger.info("+MyDbConnect.connect() WARNING Closing an existing connection.")
            self.closeConnection()

        try:
            if self.__dbSocket is None:
                dbcon = MySQLdb.connect(db="%s" % self.__databaseName,
                                        user="%s" % self.__dbUser,
                                        passwd="%s" % self.__dbPw,
                                        host="%s" % self.__dbHost,
                                        port=self.__dbPort,
                                        local_infile=1)
            else:
                dbcon = MySQLdb.connect(db="%s" % self.__databaseName,
                                        user="%s" % self.__dbUser,
                                        passwd="%s" % self.__dbPw,
                                        host="%s" % self.__dbHost,
                                        port=self.__dbPort,
                                        unix_socket="%s" % self.__dbSocket,
                                        local_infile=1)

            self._dbCon = dbcon
            return True
        except Exception as e:
            logger.exception("+MyDbConnect.connect() Connection error to server %s host %s dsn %s user %s pw %s socket %s port %d \n" %
                             (self.__dbServer, self.__dbHost, self.__databaseName, self.__dbUser, self.__dbPw, self.__dbSocket, self.__dbPort))
            self._dbCon = None

        return False

    def getConnection(self):
        return self._dbCon

    def closeConnection(self):
        """ Close db session
        """
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
            logger.exception("+MyConnectionBase(getCursor) failing.\n")

        return None
