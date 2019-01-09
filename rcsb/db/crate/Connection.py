##
# File:    Connection.py
# Author:  J. Westbrook
# Date:    1-Apr-2018
#
#  Connection methods for  Crate DB.
#
# Updates:
#
##
"""
Connection methods for  Crate DB.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import copy
import logging

from crate import client
# from crate.client.exceptions import (DatabaseError, OperationalError, ProgrammingError, Warning)

logger = logging.getLogger(__name__)


class Connection(object):

    """ Class to encapsulate Crate RDBMS DBI connection.
    """

    def __init__(self, cfgOb=None, infoD=None, resourceName=None, sectionName='site_info', verbose=False):
        self.__verbose = verbose

        self.__db = None
        self.__dbcon = None

        self.__infoD = infoD
        self.__dbName = None

        self.__dbHost = None
        self.__dbUser = None
        self.__dbPw = None
        self.__dbSocket = None
        self.__dbPort = None
        self.__dbAdminDb = None
        self.__dbPort = None
        #
        self.__defaultPort = 4200
        self.__dbServer = 'crate'
        self.__resourceName = resourceName

        self.__cfgOb = cfgOb
        #
        if infoD:
            self.setPreferences(infoD)
        #
        if resourceName:
            self.assignResource(resourceName, sectionName)

    def getPreferences(self):
        return self.__infoD

    def setPreferences(self, infoD):
        try:
            self.__infoD = copy.deepcopy(infoD)
            self.__dbName = self.__infoD.get("DB_NAME", None)
            self.__dbHost = self.__infoD.get("DB_HOST", 'localhost')
            self.__dbUser = self.__infoD.get("DB_USER", None)
            self.__dbPw = self.__infoD.get("DB_PW", None)
            self.__dbSocket = self.__infoD.get("DB_SOCKET", None)
            self.__dbServer = self.__infoD.get("DB_SERVER", "crate")
            #
            port = self.__infoD.get("DB_PORT", self.__defaultPort)
            if port and len(str(port)) > 0:
                self.__dbPort = int(str(port))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

    def assignResource(self, resourceName=None, sectionName=None):
        #
        defaultPort = 4200
        defaultHost = 'localhost'
        dbServer = 'crate'

        self.__resourceName = resourceName
        infoD = {}
        if not self.__cfgOb:
            return infoD
        #
        if (resourceName == "CRATE_DB"):
            infoD["DB_NAME"] = self.__cfgOb.get("CRATE_DB_NAME", sectionName=sectionName)
            infoD["DB_HOST"] = self.__cfgOb.get("CRATE_DB_HOST", sectionName=sectionName)
            infoD["DB_SOCKET"] = self.__cfgOb.get("CRATE_DB_SOCKET", default=None, sectionName=sectionName)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("CRATE_DB_PORT", default=defaultPort, sectionName=sectionName)))
            infoD["DB_USER"] = self.__cfgOb.get("CRATE_DB_USER_NAME", sectionName=sectionName)
            infoD["DB_PW"] = self.__cfgOb.get("CRATE_DB_PASSWORD", sectionName=sectionName)

        else:
            infoD["DB_NAME"] = self.__cfgOb.get("DB_NAME", sectionName=sectionName)
            infoD["DB_HOST"] = self.__cfgOb.get("DB_HOST", default=defaultHost, sectionName=sectionName)
            infoD["DB_SOCKET"] = self.__cfgOb.get("DB_SOCKET", default=None, sectionName=sectionName)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("DB_PORT", default=defaultPort, sectionName=sectionName)))
            infoD["DB_USER"] = self.__cfgOb.get("DB_USER_NAME", sectionName=sectionName)
            infoD["DB_PW"] = self.__cfgOb.get("DB_PASSWORD", sectionName=sectionName)
        #
        infoD['DB_SERVER'] = dbServer
        self.setPreferences(infoD)
        #
        return copy.deepcopy(infoD)
        #

    def connect(self):
        """ Create a database connection and return a connection object.

            Returns None on failure
        """
        #
        crate_host = "{host}:{port}".format(host=self.__dbHost, port=self.__dbPort)
        crate_uri = "http://%s" % crate_host
        logger.debug("Connection using uri %s" % crate_uri)
        #
        dbcon = client.connect(crate_uri)
        #
        if self.__dbcon is not None:
            # Close an open connection -
            logger.info("Closing an existing connection.\n")
            self.close()
        try:
            dbcon = self.__dbcon = dbcon
        except Exception as e:
            logger.exception("Connection error to server %s host %s port %d %s" %
                             (self.__dbServer, self.__dbHost, self.__dbPort, str(e)))
            self.__dbcon = None

        return self.__dbcon

    def close(self):
        """ Close any open database connection.
        """
        if self.__dbcon is not None:
            try:
                self.__dbcon.close()
                self.__dbcon = None
                return True
            except Exception as e:
                logger.exception("Connection close error %s" % str(e))
        return False

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args):
        return self.close()
