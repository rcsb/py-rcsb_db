##
# File:    Connection.py
# Author:  J. Westbrook
# Date:    1-Apr-2018
# Version: 0.001 Initial version
#
# Updates:
#
##
"""
Utility classes to create connections and process SQL commands with CockroachDb using
the PostgreSQL DB API 2 compatible driver.

 pip install  psycopg2
 or
 pip install  psycopg2-binary
"""
from __future__ import generators

import copy
import logging

import psycopg2
import psycopg2.extras

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

#
logger = logging.getLogger(__name__)


class Connection(object):
    """Class to encapsulate connection semantics for PostgresSQL DBI connection for CockroachDB."""

    def __init__(self, cfgOb=None, infoD=None, resourceName=None, sectionName="site_info_configuration", verbose=False):
        self.__verbose = verbose

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
        self.__defaultPort = 26257
        self.__dbServer = "cockroach"
        self.__resourceName = resourceName
        #
        self.__cfgOb = cfgOb
        #
        if infoD:
            self.setPreferences(infoD)
        #
        if resourceName:
            self.assignResource(resourceName, sectionName=sectionName)

    def getPreferences(self):
        return self.__infoD

    def setPreferences(self, infoD):
        try:
            self.__infoD = copy.deepcopy(infoD)
            self.__dbName = self.__infoD.get("DB_NAME")
            self.__dbHost = self.__infoD.get("DB_HOST", "localhost")
            self.__dbUser = self.__infoD.get("DB_USER", None)
            self.__dbPw = self.__infoD.get("DB_PW", None)
            self.__dbSocket = self.__infoD.get("DB_SOCKET", None)
            self.__dbServer = self.__infoD.get("DB_SERVER", "cockroach")
            #
            port = self.__infoD.get("DB_PORT", self.__defaultPort)
            if port and str(port):
                self.__dbPort = int(str(port))
        except Exception as e:
            logger.exception("Failing with %s", str(e))

    def assignResource(self, resourceName=None, sectionName=None):
        #
        defaultPort = 26257
        defaultHost = "localhost"
        dbServer = "cockroach"
        defaultDbName = "system"

        self.__resourceName = resourceName
        infoD = {}
        if not self.__cfgOb:
            return infoD
        #
        if resourceName == "COCKROACH_DB":
            infoD["DB_NAME"] = self.__cfgOb.get("COCKROACH_DB_NAME", default=defaultDbName, sectionName=sectionName)
            infoD["DB_HOST"] = self.__cfgOb.get("COCKROACH_DB_HOST", sectionName=sectionName)
            infoD["DB_SOCKET"] = self.__cfgOb.get("COCKROACH_DB_SOCKET", default=None, sectionName=sectionName)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("COCKROACH_DB_PORT", default=defaultPort, sectionName=sectionName)))
            infoD["DB_USER"] = self.__cfgOb.get("COCKROACH_DB_USER_NAME", sectionName=sectionName)
            infoD["DB_PW"] = self.__cfgOb.get("COCKROACH_DB_PASSWORD", sectionName=sectionName)

        else:
            infoD["DB_NAME"] = self.__cfgOb.get("DB_NAME", default=defaultDbName, sectionName=sectionName)
            infoD["DB_HOST"] = self.__cfgOb.get("DB_HOST", default=defaultHost, sectionName=sectionName)
            infoD["DB_SOCKET"] = self.__cfgOb.get("DB_SOCKET", default=None, sectionName=sectionName)
            infoD["DB_PORT"] = int(str(self.__cfgOb.get("DB_PORT", default=defaultPort, sectionName=sectionName)))
            infoD["DB_USER"] = self.__cfgOb.get("DB_USER_NAME", sectionName=sectionName)
            infoD["DB_PW"] = self.__cfgOb.get("DB_PASSWORD", sectionName=sectionName)
        #
        infoD["DB_SERVER"] = dbServer
        self.setPreferences(infoD)
        #
        return copy.deepcopy(infoD)
        #

    def connect(self):
        """Create a database connection and return a connection object.

        Returns None on failure
        """
        #
        if self.__dbcon is not None:
            # Close an open connection -
            logger.info("Closing an existing connection.")
            self.close()

        try:
            if self.__dbPw:
                dbcon = psycopg2.connect(database="%s" % self.__dbName, user="%s" % self.__dbUser, password="%s" % self.__dbPw, host="%s" % self.__dbHost, port=self.__dbPort)
            else:
                dbcon = psycopg2.connect(database="%s" % self.__dbName, user="%s" % self.__dbUser, host="%s" % self.__dbHost, port=self.__dbPort)

            dbcon.set_session(autocommit=True)
            self.__dbcon = dbcon
        except Exception as e:
            logger.error("Failing with %s", str(e))
            self.__dbcon = None

        return self.__dbcon

    def close(self):
        """Close any open database connection."""
        if self.__dbcon is not None:
            try:
                self.__dbcon.close()
                self.__dbcon = None
                return True
            except Exception as e:
                logger.exception("Connection close error %s", str(e))

        return False

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args):
        return self.close()
