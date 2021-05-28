##
# File:    CrateDbUtil.py
# Author:  J. Westbrook
# Date:    21-Dec-2017
# Version: 0.001 Initial version
#
# Updates:
#
##
"""
Utility classes to create connections and process SQL commands with CrateDb.

"""
from __future__ import generators

import logging

from crate.client.exceptions import DatabaseError, OperationalError, ProgrammingError, Warning  # pylint: disable=redefined-builtin

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

logger = logging.getLogger(__name__)


class CrateDbQuery(object):

    """Parameterized SQL queries using Python DBI protocol..."""

    def __init__(self, dbcon, verbose=True):
        self.__dbcon = dbcon
        self.__verbose = verbose
        self.__ops = ["EQ", "GE", "GT", "LT", "LE", "LIKE", "NOT LIKE"]
        self.__opDict = {"EQ": "=", "GE": ">=", "GT": ">", "LT": "<", "LE": "<=", "LIKE": "LIKE", "NOT LIKE": "NOT LIKE"}
        self.__logOps = ["AND", "OR", "NOT"]
        self.__grpOps = ["BEGIN", "END"]
        self.__warningAction = "default"

    def sqlTemplateCommandMany(self, sqlTemplate, valueLists=None):
        """Execute a batch sql commands followed by a single commit. Commands are
         are describe in a template with an associated list of values.

        cursor.executemany("INSERT INTO locations (name, date, kind, position) VALUES (?, ?, ?, ?)",
            ...                [('Cloverleaf', '2007-03-11', 'Quasar', 7),
            ...                 ('Old Faithful', '2007-03-11', 'Quasar', 7)])
            [{u'rowcount': 1}, {u'rowcount': 1}]

        """
        lenR = 0
        ret = []
        iFail = 0
        try:
            lenT = len(valueLists)
            curs = self.__dbcon.cursor()
            ret = curs.executemany(sqlTemplate, valueLists)
            lenR = len(ret)
            logger.debug("Return len %d", len(ret))
            for i, _ in enumerate(ret):
                if ret[i]["rowcount"] != 1:
                    iFail += 1
                    logger.info("Insert fails on row %d of %d with values: %r", i, lenT, valueLists[i])
            curs.close()
            return lenR - iFail
        except DatabaseError as e:
            logger.info("sqlTemplate %s", sqlTemplate)
            logger.info("return list %r", ret)
            logger.error("error is:\n%s", str(e))
            curs.close()
        except Warning as e:
            logger.warning("warning is:\n%s", str(e))
            curs.close()
        except Exception as e:
            logger.exception("Exception is:\n%s", str(e))
            curs.close()
        #
        lenR = len(ret)
        return lenR - iFail

    def sqlTemplateCommand(self, sqlTemplate=None, valueList=None):
        """Execute sql template command with associated value list.

        Insert one row -

        Errors and warnings that generate exceptions are caught by this method.
        """
        valueList = valueList if valueList else []
        try:
            curs = self.__dbcon.cursor()
            curs.execute(sqlTemplate, valueList)
            curs.close()
            return True
        except DatabaseError as e:
            logger.info(" error is:\n%s\n", str(e))
            curs.close()
        except Warning as e:
            logger.info(" warning is:\n%s\n", str(e))
            curs.close()
        except Exception as e:
            logger.info(" exception is:\n%s\n", str(e))
            curs.close()
        return False

    def sqlTemplateCommandList(self, sqlTemplateValueList=None):
        """Execute sql template command with associated value list.

        Input -

        sqlTemplateValueList [(sqlTemplate,vList), (sqlTemplate, vlist), ... ]

        Insert on row -

        Errors and warnings that generate exceptions are caught by this method.
        """
        vL = []
        iFail = 0
        try:
            curs = self.__dbcon.cursor()
            #
            lenT = len(sqlTemplateValueList)
            for ii in range(lenT):
                tV, vL = sqlTemplateValueList[ii]
                try:
                    curs.execute(tV, vL)
                except Exception as e:
                    iFail += 1
                    logger.info(" Error is: %s", str(e))
                    # logger.info(" Template for record %d of %d : %s" % (ii, lenT, t))
                    logger.info(" Record %d of %d value list: %s", ii, lenT, vL)
            #
            curs.close()
            logger.debug(" Inserted %d of %d values", ii - iFail, lenT)
            return ii - iFail + 1
        except DatabaseError as e:
            logger.exception(" error is: %s", str(e))
            logger.info(" Record %d of %d value list: %s", ii, lenT, vL)
            curs.close()
        except Warning as e:
            logger.info(" Warning is: %s", str(e))
            logger.info(" Record %d of %d value list: %s", ii, lenT, vL)
            curs.close()
        except Exception as e:
            logger.info(" Exception is: %s", str(e))
            logger.info(" Record %d of %d value list: %s", ii, lenT, vL)
            curs.close()
        return ii - iFail + 1

    def sqlCommandList(self, sqlCommandList):
        """Execute the input list of SQL commands catching exceptions from the server.

        The treatment of warning is controlled by a prior setting of self.setWarnings("error"|"ignore"|"default")
        """

        try:
            sqlCommand = ""
            curs = self.__dbcon.cursor()
            for sqlCommand in sqlCommandList:
                curs.execute(sqlCommand)
            #
            curs.close()
            return True
        except DatabaseError as e:
            logger.info(" SQL command failed for:\n%s", sqlCommand)
            logger.info(" database error is message is:\n%s", str(e))
            curs.close()
        except Warning as e:
            logger.info(" warning message is:\n%s", str(e))
            logger.info(" generated warnings for command:\n%s", sqlCommand)
            curs.close()
        except Exception as e:
            logger.info(" exception message is:\n%s\n", str(e))
            logger.exception(" SQL command failed for:\n%s\n", sqlCommand)
            curs.close()

        return False

    def sqlCommand(self, queryString):
        """Execute SQL command catching exceptions returning no data from the server."""
        try:
            curs = self.__dbcon.cursor()
            curs.execute(queryString)
            curs.close()
            return True
        except OperationalError as e:
            logger.info(" SQL command failed for:\n%s", queryString)
            logger.info(" warning is message is:\n%s", str(e))
            curs.close()
        except DatabaseError as e:
            logger.info(" SQL command failed for:\n%s\n", queryString)
            logger.info(" MySQL warning is message is:\n%s\n", str(e))
            curs.close()
        except Exception as e:
            logger.exception(" SQL command failed for:\n%s\n with %s", queryString, str(e))
            curs.close()
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
        except ProgrammingError as e:
            logger.info(" MySQL warning is message is:\n%s\n", str(e))
            curs.close()
        except OperationalError as e:
            logger.info(" MySQL warning is message is:\n%s\n", str(e))
            logger.info(" SQL command failed for:\n%s\n", queryString)
            curs.close()
        except DatabaseError as e:
            logger.info(" MySQL warning is message is:\n%s\n", str(e))
            logger.info(" SQL command failed for:\n%s\n", queryString)
            curs.close()
        except Exception as e:
            logger.exception(" SQL command failed for:\n%s\n with %s", queryString, str(e))
            curs.close()

        return []

    def simpleQuery(self, selectList=None, fromList=None, condition="", orderList=None, returnObj=None):
        """ """
        #
        selectList = selectList if selectList else []
        fromList = fromList if fromList else []
        orderList = orderList if orderList else []
        returnObj = returnObj if returnObj else []
        colsCsv = ",".join(["%s" % k for k in selectList])
        tablesCsv = ",".join(["%s" % k for k in fromList])

        order = ""
        if orderList:
            (aV, tV) = orderList[0]
            order = " ORDER BY CAST(%s AS %s) " % (aV, tV)
            for (aV, tV) in orderList[1:]:
                order += ", CAST(%s AS %s) " % (aV, tV)

        #
        query = "SELECT " + colsCsv + " FROM " + tablesCsv + condition + order
        if self.__verbose:
            logger.info("Query: %s\n", query)
        curs = self.__dbcon.cursor()
        curs.execute(query)
        while True:
            result = curs.fetchone()
            if result is not None:
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
        except Exception:
            return False
