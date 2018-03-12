##
# File:    MyDdUtil.py
# Author:  J. Westbrook
# Date:    27-Jan-2012
# Version: 0.001 Initial version
#
# Updates:
# 27-Jan-2012 Jdw  Refactored and consolidated MySQL utilities from various sources
# 31-Jan-2012 Jdw  Move SQL generators to a separate class -
#  9-Jan-2013 jdw  add parameters to connection method to permit batch file loading.
# 11-Jan-2013 jdw  make mysql warnings generate exceptions.
# 21-Jan-2013 jdw  adjust the dbapi command order for processing sql command lists -
#                  tested with batch loading using truncate/load & delete from /load
# 11-Jul-2013 jdw  add optional parameter for database socket -
# 11-Nov-2014 jdw  add authentication via dictionary object -
#  3-Mar-2016 jdw  add port parameter option to connect method -
# 11-Aug-2016 jdw  add connection pool wrapper
# 11-Aug-2016 jdw  add chunked fetch method
#
##
"""
Utility classes to create connections and process SQL commands with a MySQL RDBMS.

"""
from __future__ import generators

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

#

import MySQLdb
# import _mysql_exceptions

import sys
import os
import platform
import warnings
import logging
logger = logging.getLogger(__name__)

#
#
if platform.system() == "Linux":
    try:
        import sqlalchemy.pool as pool
        MySQLdb = pool.manage(MySQLdb, pool_size=12, max_overflow=12, timeout=30, echo=True, use_threadlocal=True)
    except Exception as e:
        pass


class MyDbConnect(object):

    """ Class to encapsulate RDBMS DBI connection.
    """

    def __init__(self, dbServer='mysql', dbHost='localhost', dbName=None, dbUser=None, dbPw=None, dbSocket=None, dbPort=None, verbose=False):
        self.__verbose = verbose

        if (dbName is None):
            self.__dbName = os.getenv("MYSQL_DB_NAME")
        else:
            self.__dbName = dbName

        if (dbUser is None):
            self.__dbUser = os.getenv("MYSQL_DB_USER")
        else:
            self.__dbUser = dbUser

        if (dbPw is None):
            self.__dbPw = os.getenv("MYSQL_DB_PW")
        else:
            self.__dbPw = dbPw

        if (dbHost is None):
            self.__dbHost = os.getenv("MYSQL_DB_HOST")
        else:
            self.__dbHost = dbHost

        if dbSocket is None:
            # try from the environment -
            tS = os.getenv("MYSQL_DB_SOCKET")
            if (tS is not None):
                self.__dbSocket = tS
            else:
                self.__dbSocket = None
        else:
            self.__dbSocket = dbSocket

        if dbPort is None:
            # try from the environment -
            tS = os.getenv("MYSQL_DB_PORT")
            if (tS is not None):
                self.__dbPort = int(tS)
            else:
                self.__dbPort = 3306
        else:
            self.__dbPort = dbPort
        #
        self.__dbServer = dbServer

        if (dbServer != 'mysql'):
            logger.info("+MyDbConnect. Unsupported server %s\n" % dbServer)
            sys.exit(1)

        self.__dbcon = None

    def setAuth(self, authD):
        try:
            self.__dbName = authD["DB_NAME"]
            self.__dbHost = authD["DB_HOST"]
            self.__dbUser = authD["DB_USER"]
            self.__dbPw = authD["DB_PW"]
            self.__dbSocket = authD["DB_SOCKET"]
            self.__dbServer = authD["DB_SERVER"]
            # treat port as optional with default of 3306
            if 'DB_PORT' in authD:
                self.__dbPort = authD["DB_PORT"]
            else:
                self.__dbPort = 3306
        except Exception as e:
            logger.info("+MyDbConnect.setAuth failing  %r\n" % authD.items())
            logger.exception("Failing with %s" % str(e))

    def connect(self):
        """ Create a database connection and return a connection object.

            Returns None on failure
        """
        #
        if self.__dbcon is not None:
            # Close an open connection -
            logger.info("+MyDbConnect.connect() WARNING Closing an existing connection.\n")
            self.close()

        # logger.info("+MyDbConnect.connect() Connection to server %s host %s dsn %s user %s pw %s socket %s port %d \n" %
        #                    (self.__dbServer, self.__dbHost, self.__dbName, self.__dbUser, self.__dbPw, self.__dbSocket, self.__dbPort))
        try:
            if self.__dbSocket is None:
                dbcon = MySQLdb.connect(db="%s" % self.__dbName,
                                        user="%s" % self.__dbUser,
                                        passwd="%s" % self.__dbPw,
                                        host="%s" % self.__dbHost,
                                        port=self.__dbPort,
                                        local_infile=1)
            else:
                dbcon = MySQLdb.connect(db="%s" % self.__dbName,
                                        user="%s" % self.__dbUser,
                                        passwd="%s" % self.__dbPw,
                                        host="%s" % self.__dbHost,
                                        port=self.__dbPort,
                                        unix_socket="%s" % self.__dbSocket,
                                        local_infile=1)

            self.__dbcon = dbcon
        except Exception as e:
            logger.info("+MyDbConnect.connect() Connection error to server %s host %s dsn %s user %s pw %s socket %s port %d \n" %
                        (self.__dbServer, self.__dbHost, self.__dbName, self.__dbUser, self.__dbPw, self.__dbSocket, self.__dbPort))
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
                pass
        return False


class MyDbQuery(object):

    """ Parameterized SQL queries using Python DBI protocol...
    """

    def __init__(self, dbcon, verbose=True):
        self.__dbcon = dbcon
        self.__verbose = verbose
        self.__ops = ['EQ', 'GE', 'GT', 'LT', 'LE', 'LIKE', 'NOT LIKE']
        self.__opDict = {'EQ': '=',
                         'GE': '>=',
                         'GT': '>',
                         'LT': '<',
                         'LE': '<=',
                         'LIKE': 'LIKE',
                         'NOT LIKE': 'NOT LIKE'
                         }
        self.__logOps = ['AND', 'OR', 'NOT']
        self.__grpOps = ['BEGIN', 'END']
        self.__warningAction = 'default'

    def sqlBatchTemplateCommand(self, templateValueList, prependSqlList=None):
        """  Execute a batch sql commands followed by a single commit. Commands are
             are describe in a template with an associated list of values.

             prependSqlList = Optional list of SQL commands to be executed prior to any
                              batch template commands.

             Errors and warnings that generate exceptions are caught by this method.
        """
        #warnings.simplefilter("error", MySQLdb.Warning)
        self.__setWarningHandler()
        try:
            t = ''
            v = []
            curs = self.__dbcon.cursor()
            if ((prependSqlList is not None) and (len(prependSqlList) > 0)):
                sqlCommand = '\n'.join(prependSqlList)
                curs.execute(sqlCommand)

            for t, v in templateValueList:
                curs.execute(t, v)
            self.__dbcon.commit()
            curs.close()
            return True
        except MySQLdb.Error as e:
            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand MySQL error message is:\n%s\n" % e)
                logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % (t % tuple(v)))
            self.__dbcon.rollback()
            curs.close()
        except MySQLdb.Warning as e:

            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand MySQL warning message is:\n%s\n" % e)
                logger.info("MyDbQuery.sqlCommand generated warnings for command:\n%s\n" % (t % tuple(v)))
            self.__dbcon.rollback()
            curs.close()
        except Exception as e:

            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand generated exception for command:\n%s\n" % (t % tuple(v)))
                logger.exception("Failing with %s" % str(e))
            self.__dbcon.rollback()
            curs.close()
        return False

    def sqlTemplateCommand(self, sqlTemplate=None, valueList=[]):
        """  Execute sql template command with associated value list.

             Errors and warnings that generate exceptions are caught by this method.
        """
        #warnings.simplefilter("error", MySQLdb.Warning)
        self.__setWarningHandler()
        try:
            curs = self.__dbcon.cursor()
            curs.execute(sqlTemplate, valueList)
            self.__dbcon.commit()
            curs.close()
            return True
        except MySQLdb.Error as e:
            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand MySQL message is:\n%s\n" % e)
                logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % (sqlTemplate % tuple(valueList)))
            self.__dbcon.rollback()
            curs.close()
        except MySQLdb.Warning as e:

            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand MySQL message is:\n%s\n" % e)
                logger.info("MyDbQuery.sqlCommand generated warnings for command:\n%s\n" % (sqlTemplate % tuple(valueList)))
            self.__dbcon.rollback()
            curs.close()
        except Exception as e:
            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand generated warnings for command:\n%s\n" % (sqlTemplate % tuple(valueList)))
                logger.exception("Failing with %s" % str(e))
            self.__dbcon.rollback()
            curs.close()
        return False

    def setWarning(self, action):
        if action in ['error', 'ignore', 'default']:
            self.__warningAction = action
            return True
        else:
            self.__warningAction = 'default'
            return False

    def __setWarningHandler(self):
        if self.__warningAction == 'error':
            warnings.simplefilter("error", MySQLdb.Warning)
        elif self.__warningAction in ['ignore', 'default']:
            warnings.simplefilter(self.__warningAction)
        else:
            warnings.simplefilter('default')

    def sqlCommand(self, sqlCommandList):
        """  Execute the input list of SQL commands catching exceptions from the server.

        The treatment of warning is controlled by a prior setting of self.setWarnings("error"|"ignore"|"default")
        """

        # warnings.simplefilter("error", MySQLdb.Warning)
        self.__setWarningHandler()
        try:
            sqlCommand = ''
            curs = self.__dbcon.cursor()
            for sqlCommand in sqlCommandList:
                curs.execute(sqlCommand)
            #
            self.__dbcon.commit()
            curs.close()
            return True
        except MySQLdb.Error as e:
            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % sqlCommand)
                logger.info("MyDbQuery.sqlCommand MySQL warning is message is:\n%s\n" % e)
            # self.__dbcon.rollback()
            curs.close()
        except MySQLdb.Warning as e:
            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand MySQL message is:\n%s\n" % e)
                logger.info("MyDbQuery.sqlCommand generated warnings for command:\n%s\n" % sqlCommand)
                logger.exception("Failing with %s" % str(e))
            # self.__dbcon.rollback()
            curs.close()
        except Exception as e:
            if (self.__verbose):
                logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % sqlCommand)
                logger.exception("Failing with %s" % str(e))
            # self.__dbcon.rollback()
            curs.close()

        return False

    def sqlCommand2(self, queryString):
        """   Execute SQL command catching exceptions returning no data from the server.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            #warnings.simplefilter('error', MySQLdb.Warning)
            #warnings.simplefilter('error', _mysql_exceptions.Warning)
            try:
                curs = self.__dbcon.cursor()
                curs.execute(queryString)
                curs.close()
                return True
            except MySQLdb.ProgrammingError as e:
                if (self.__verbose):
                    logger.info("MyDbQuery.sqlCommand MySQL warning is message is:\n%s\n" % e)
                curs.close()
            except MySQLdb.OperationalError as e:
                if (self.__verbose):
                    logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % queryString)
                    logger.info("MyDbQuery.sqlCommand MySQL warning is message is:\n%s\n" % e)
                curs.close()
            except MySQLdb.Error as e:
                if (self.__verbose):
                    logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % queryString)
                    logger.info("MyDbQuery.sqlCommand MySQL warning is message is:\n%s\n" % e)
                curs.close()
            except Exception as e:
                if (self.__verbose):
                    logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % queryString)
                curs.close()
                logger.exception("Failing with %s" % str(e))
        return []

    def __fetchIter(self, cursor, rowSize=1000):
        """ Chunked iterator to manage results fetches to mysql server
        """
        while True:
            results = cursor.fetchmany(rowSize)
            if not results:
                break
            for result in results:
                yield result

    def selectRows(self, queryString):
        """ Execute SQL command and return list of lists for the result set.
        """
        rowList = []
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            # warnings.simplefilter('error', MySQLdb.Warning)
            # warnings.simplefilter('error', _mysql_exceptions.Warning)
            try:
                curs = self.__dbcon.cursor()
                curs.execute(queryString)
                while True:
                    result = curs.fetchone()
                    if (result is not None):
                        rowList.append(result)
                    else:
                        break
                curs.close()
                return rowList
            except MySQLdb.ProgrammingError as e:
                if (self.__verbose):
                    logger.info("MyDbQuery.sqlCommand MySQL warning is message is:\n%s\n" % e)
                curs.close()
            except MySQLdb.OperationalError as e:
                if (self.__verbose):
                    logger.info("MyDbQuery.sqlCommand MySQL warning is message is:\n%s\n" % e)
                    logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % queryString)
                curs.close()
            except MySQLdb.Error as e:
                if (self.__verbose):
                    logger.info("MyDbQuery.sqlCommand MySQL warning is message is:\n%s\n" % e)
                    logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % queryString)
                curs.close()
            except Exception as e:
                if (self.__verbose):
                    logger.info("MyDbQuery.sqlCommand SQL command failed for:\n%s\n" % queryString)
                    logger.exception("Failing with %s" % str(e))
                curs.close()

        return []

    def simpleQuery(self, selectList=[], fromList=[], condition='',
                    orderList=[], returnObj=[]):
        """
        """
        #
        colsCsv = ",".join(["%s" % k for k in selectList])
        tablesCsv = ",".join(["%s" % k for k in fromList])

        order = ""
        if (len(orderList) > 0):
            (a, t) = orderList[0]
            order = " ORDER BY CAST(%s AS %s) " % (a, t)
            for (a, t) in orderList[1:]:
                order += ", CAST(%s AS %s) " % (a, t)

        #
        query = "SELECT " + colsCsv + " FROM " + tablesCsv + condition + order
        if (self.__verbose):
            logger.info("Query: %s\n" % query)
        curs = self.__dbcon.cursor()
        curs.execute(query)
        while True:
            result = curs.fetchone()
            if (result is not None):
                returnObj.append(result)
            else:
                break
        curs.close()
        return returnObj

    def testSelectQuery(self, count):
        tSQL = "select %d" % count
        #
        try:
            rowL = self.selectRows(queryString=tSQL)
            tup = rowL[0]
            return int(str(tup[0])) == count
        except Exception as e:
            return False
