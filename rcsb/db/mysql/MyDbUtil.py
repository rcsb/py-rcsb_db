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
# 10-Mar-2018 jdw  Py2->Py3 compatibility using driver fork described at https://mysqlclient.readthedocs.io/user_guide.html#
# 25-Mar-2018 jdw  Connection class moved Connection/ConnectionBase
# 30-Mar-2018 jdw  adjust the exception handing -- and control of warnings
#
##
"""
Utility classes to create connections and process SQL commands with a MySQL RDBMS.

"""

from __future__ import generators

import logging
import warnings

# pylint: disable=no-member
import MySQLdb

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

logger = logging.getLogger(__name__)

#
#  Pooling seems to be broken for Py3 on MACos -
# if platform.system() == "Linux":
#    try:
#        import sqlalchemy.pool as pool
#        MySQLdb = pool.manage(MySQLdb, pool_size=12, max_overflow=12, timeout=30, echo=True, use_threadlocal=True)
#    except Exception as e:
#        pass


class MyDbQuery(object):

    """Parameterized SQL queries using Python DBI protocol..."""

    def __init__(self, dbcon, verbose=True):
        self.__dbcon = dbcon
        self.__verbose = verbose
        self.__ops = ["EQ", "GE", "GT", "LT", "LE", "LIKE", "NOT LIKE"]
        self.__opDict = {"EQ": "=", "GE": ">=", "GT": ">", "LT": "<", "LE": "<=", "LIKE": "LIKE", "NOT LIKE": "NOT LIKE"}
        self.__logOps = ["AND", "OR", "NOT"]
        self.__grpOps = ["BEGIN", "END"]
        self.__warningAction = "default"

    def sqlBatchTemplateCommand(self, templateValueList, prependSqlList=None):
        """Execute a batch sql commands followed by a single commit. Commands are
        are describe in a template with an associated list of values.

        prependSqlList = Optional list of SQL commands to be executed prior to any
                         batch template commands.

        Errors and warnings that generate exceptions are caught by this method.
        """
        with warnings.catch_warnings():
            self.__setWarningHandler()
            try:
                tpl = ""
                vL = []
                curs = self.__dbcon.cursor()
                if (prependSqlList is not None) and prependSqlList:
                    sqlCommand = "\n".join(prependSqlList)
                    curs.execute(sqlCommand)

                for tpl, vL in templateValueList:
                    curs.execute(tpl, vL)
                self.__dbcon.commit()
                curs.close()
                return True
            except MySQLdb.Error as e:
                logger.info("MySQL error message is:\n%s\n", str(e))
                logger.error("SQL command failed for:\n%s\n", (tpl % tuple(vL)))
                self.__dbcon.rollback()
                curs.close()
            except MySQLdb.Warning as e:
                logger.info("MySQL warning message is:\n%s\n", str(e))
                logger.info("SQL Command generated warnings for command:\n%s\n", (tpl % tuple(vL)))
                self.__dbcon.rollback()
                curs.close()
            except Exception as e:
                logger.info("SQL Command generated exception for command:\n%s\n", (tpl % tuple(vL)))
                logger.exception("Failing with %s", str(e))
                self.__dbcon.rollback()
                curs.close()
            return False

    def sqlTemplateCommand(self, sqlTemplate=None, valueList=None):
        """Execute sql template command with associated value list.

        Errors and warnings that generate exceptions are caught by this method.
        """
        vList = valueList if valueList else []
        with warnings.catch_warnings():
            self.__setWarningHandler()
            try:
                curs = self.__dbcon.cursor()
                curs.execute(sqlTemplate, vList)
                self.__dbcon.commit()
                curs.close()
                return True
            except MySQLdb.Error as e:
                logger.info("SQL command failed for:\n%s\n", (sqlTemplate % tuple(vList)))
                logger.error("MySQL error message is:\n%s\n", str(e))
                self.__dbcon.rollback()
                curs.close()
            except MySQLdb.Warning as e:
                logger.info("MYSQL warnings for command:\n%s\n", (sqlTemplate % tuple(vList)))
                logger.warning("MySQL warning message is:\n%s\n", str(e))
                self.__dbcon.rollback()
                curs.close()
            except Exception as e:
                logger.info("SQL Command generated warnings command:\n%s\n", (sqlTemplate % tuple(vList)))
                logger.exception("Failing with %s", str(e))
                self.__dbcon.rollback()
                curs.close()
            return False

    def setWarning(self, action):
        if action in ["error", "ignore", "default"]:
            self.__warningAction = action
            return True
        else:
            self.__warningAction = "default"
            return False

    def __setWarningHandler(self):
        """'error' will map all MySQL warnings to exceptions -

        'ignore' will completely suppress warnings

        other settings may print warning directly to stderr
        """
        if self.__warningAction == "error":
            warnings.simplefilter("error", category=MySQLdb.Warning)
        elif self.__warningAction in ["ignore", "default"]:
            warnings.simplefilter(self.__warningAction)
        else:
            warnings.simplefilter("default")

    def sqlCommand(self, sqlCommandList):
        """Execute the input list of SQL commands catching exceptions from the server.

        The treatment of warning is controlled by a prior setting of self.setWarnings("error"|"ignore"|"default")

        category=MySQLdb.Warning

        """
        with warnings.catch_warnings():
            self.__setWarningHandler()
            curs = None
            try:
                sqlCommand = ""
                curs = self.__dbcon.cursor()
                for sqlCommand in sqlCommandList:
                    curs.execute(sqlCommand)
                #
                self.__dbcon.commit()
                curs.close()
                return True
            except MySQLdb.Error as e:
                logger.info("SQL command failed for:\n%s\n", sqlCommand)
                logger.error("MySQL error is message is:\n%s\n", str(e))
                # self.__dbcon.rollback()
                if curs:
                    curs.close()
            except MySQLdb.Warning as e:
                logger.info("SQL generated warnings for command:\n%s\n", sqlCommand)
                logger.warning("MySQL warning message is:\n%s\n", str(e))
                # self.__dbcon.rollback()
                if curs:
                    curs.close()
                return True
            except Exception as e:
                logger.info("SQL command failed for:\n%s\n", sqlCommand)
                logger.exception("Failing with %s", str(e))
                # self.__dbcon.rollback()
                if curs:
                    curs.close()

        return False

    def sqlCommand2(self, queryString):
        """Execute SQL command catching exceptions returning no data from the server."""
        curs = None
        with warnings.catch_warnings():
            self.__setWarningHandler()
            try:
                curs = self.__dbcon.cursor()
                curs.execute(queryString)
                curs.close()
                return True
            except MySQLdb.ProgrammingError as e:
                logger.error("MySQL warning is message is:\n%s\n", str(e))
                if curs:
                    curs.close()
            except MySQLdb.OperationalError as e:
                logger.info("SQL command failed for:\n%s\n", queryString)
                logger.info("MySQL warning is message is:\n%s\n", str(e))
                if curs:
                    curs.close()
            except MySQLdb.Error as e:
                logger.info("SQL command failed for:\n%s\n", queryString)
                logger.info("MySQL warning is message is:\n%s\n", str(e))
                if curs:
                    curs.close()
            except Exception as e:
                logger.info("SQL command failed for:\n%s\n", queryString)
                if curs:
                    curs.close()
                logger.exception("Failing with %s", str(e))
        return []

    def __fetchIter(self, cursor, rowSize=1000):
        """Chunked iterator to manage results fetches to mysql server"""
        while True:
            results = cursor.fetchmany(rowSize)
            if not results:
                break
            for result in results:
                yield result

    def selectRows(self, queryString):
        """Execute SQL command and return list of lists for the result set."""
        rowList = []
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                curs = self.__dbcon.cursor()
                curs.execute(queryString)
                while True:
                    result = curs.fetchone()
                    if result is not None:
                        rowList.append(result)
                    else:
                        break
                curs.close()
                return rowList
            except MySQLdb.ProgrammingError as e:
                logger.warning("MySQL warning is message is:\n%s\n", str(e))
                curs.close()
            except MySQLdb.OperationalError as e:
                logger.info("SQL command failed for:\n%s\n", queryString)
                logger.warning("MySQL warning is message is:\n%s\n", str(e))
                curs.close()
            except MySQLdb.Error as e:
                logger.info("SQL command failed for:\n%s\n", queryString)
                logger.error("MySQL warning is message is:\n%s\n", str(e))
                curs.close()
            except Exception as e:
                logger.info("SQL command failed for:\n%s\n", queryString)
                logger.exception("Failing with %s", str(e))
                curs.close()

        return []

    def simpleQuery(self, selectList, fromList, condition="", orderList=None, returnObj=None):
        """ """
        #
        oL = orderList if orderList else []
        retObj = returnObj if returnObj else []
        #
        colsCsv = ",".join(["%s" % k for k in selectList])
        tablesCsv = ",".join(["%s" % k for k in fromList])

        order = ""
        if oL:
            (sV, tV) = oL[0]
            order = " ORDER BY CAST(%s AS %s) " % (sV, tV)
            for (sV, tV) in oL[1:]:
                order += ", CAST(%s AS %s) " % (sV, tV)

        #
        query = "SELECT " + colsCsv + " FROM " + tablesCsv + condition + order
        logger.debug("Query: %s\n", query)
        curs = self.__dbcon.cursor()
        curs.execute(query)
        while True:
            result = curs.fetchone()
            if result is not None:
                retObj.append(result)
            else:
                break
        curs.close()
        return retObj

    def testSelectQuery(self, count):
        tSQL = "select %d" % count
        #
        try:
            rowL = self.selectRows(queryString=tSQL)
            tup = rowL[0]
            return int(str(tup[0])) == count
        except Exception:
            return False
