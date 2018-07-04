##
# File:    CockroachDbUtil.py
# Author:  J. Westbrook
# Date:    10-Feb-2018
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

import logging

import psycopg2
import psycopg2.extras

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

logger = logging.getLogger(__name__)


class CockroachDbQuery(object):

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

    def sqlTemplateCommandMany(self, sqlTemplate, valueLists=None, pageSize=100):
        """  Execute a batch sql commands followed by a single commit. Commands are
             are describe in a template with an associated list of values.

                psycopg2.extras.execute_batch(cur, sql, argslist, page_size=100)

        """
        try:

            curs = self.__dbcon.cursor()
            # curs.executemany(sqlTemplate, valueLists)
            psycopg2.extras.execute_batch(curs, sqlTemplate, valueLists, page_size=pageSize)
            curs.close()
            return True
        except psycopg2.DatabaseError as e:
            logger.info("sqlTemplate: %s" % sqlTemplate)
            logger.debug("valueLists:  %r" % valueLists)
            logger.error("Database error is:\n%s" % str(e))
            curs.close()
        except Warning as e:
            logger.warning("Warning is:\n%s" % str(e))
            curs.close()
        except Exception as e:
            logger.exception("Exception is:\n%s" % str(e))
            curs.close()
        #
        return False

    def sqlTemplateCommand(self, sqlTemplate=None, valueList=[]):
        """  Execute sql template command with associated value list.

             Insert one row -

             Errors and warnings that generate exceptions are caught by this method.
        """
        try:
            curs = self.__dbcon.cursor()
            curs.execute(sqlTemplate, valueList)
            curs.close()
            return True
        except psycopg2.DatabaseError as e:
            logger.info(" error is:\n%s\n" % str(e))
            curs.close()
        except Warning as e:
            logger.info(" warning is:\n%s\n" % str(e))
            curs.close()
        except Exception as e:
            logger.info(" exception is:\n%s\n" % str(e))
            curs.close()
        return False

    def sqlTemplateCommandList(self, sqlTemplateValueList=None):
        """  Execute sql template command with associated value list.

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
                t, vL = sqlTemplateValueList[ii]
                try:
                    curs.execute(t, vL)
                except Exception as e:
                    iFail += 1
                    logger.info(" Error is: %s" % str(e))
                    # logger.info(" Template for record %d of %d : %s" % (ii, lenT, t))
                    logger.info(" Record %d of %d value list: %s" % (ii, lenT, vL))
            #
            curs.close()
            logger.debug(" Inserted %d of %d values" % (ii - iFail, lenT))
            return ii - iFail + 1
        except psycopg2.DatabaseError as e:
            logger.exception(" error is: %s" % str(e))
            logger.info(" Record %d of %d value list: %s" % (ii, lenT, vL))
            curs.close()
        except Warning as e:
            logger.info(" Warning is: %s" % str(e))
            logger.info(" Record %d of %d value list: %s" % (ii, lenT, vL))
            curs.close()
        except Exception as e:
            logger.info(" Exception is: %s" % str(e))
            logger.info(" Record %d of %d value list: %s" % (ii, lenT, vL))
            curs.close()
        return ii - iFail + 1

    def sqlCommandList(self, sqlCommandList):
        """  Execute the input list of SQL commands catching exceptions from the server.


        The treatment of warning is controlled by a prior setting of self.setWarnings("error"|"ignore"|"default")
        """

        try:
            sqlCommand = ''
            curs = self.__dbcon.cursor()
            for sqlCommand in sqlCommandList:
                curs.execute(sqlCommand)
            #
            curs.close()
            return True
        except psycopg2.DatabaseError as e:
            logger.info(" SQL command failed for:\n%s" % sqlCommand)
            logger.info(" database error is message is:\n%s" % str(e))
            curs.close()
        except Warning as e:
            logger.info(" warning message is:\n%s" % str(e))
            logger.info(" generated warnings for command:\n%s" % sqlCommand)
            curs.close()
        except Exception as e:
            logger.info(" exception message is:\n%s\n" % str(e))
            logger.exception(" SQL command failed for:\n%s\n" % sqlCommand)
            curs.close()

        return False

    def sqlCommand(self, queryString):
        """   Execute SQL command catching exceptions returning no data from the server.
        """
        try:
            curs = self.__dbcon.cursor()
            curs.execute(queryString)
            curs.close()
            return True
        except psycopg2.OperationalError as e:
            logger.info(" SQL command failed for:\n%s" % queryString)
            logger.info(" Warning is message is:\n%s" % str(e))
            curs.close()
        except psycopg2.DatabaseError as e:
            logger.info(" SQL command failed for:\n%s" % queryString)
            logger.info(" Warning is message is:\n%s" % str(e))
            curs.close()
        except Exception as e:
            logger.info(" SQL command failed for:\n%s" % queryString)
            logger.info(" Warning is message is:\n%s" % str(e))
            curs.close()
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
        except psycopg2.ProgrammingError as e:
            logger.info(" Warning is message is:\n%s" % str(e))
            curs.close()
        except psycopg2.OperationalError as e:
            logger.info(" Warning is message is:\n%s" % str(e))
            logger.info(" SQL command failed for:\n%s" % queryString)
            curs.close()
        except psycopg2.DatabaseError as e:
            logger.info(" Warning is message is:\n%s" % str(e))
            logger.info(" SQL command failed for:\n%s" % queryString)
            curs.close()
        except Exception as e:
            logger.info(" Error message is:\n%s" % str(e))
            logger.exception(" SQL command failed for:\n%s" % queryString)
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
        except Exception:
            return False
