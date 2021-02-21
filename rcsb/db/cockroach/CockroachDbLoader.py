##
# File:    CockroachDbLoader.py
# Author:  J. Westbrook
# Date:    1-Apr-2018
# Version: 0.001 Initial version
#
# Loader variant to support stripped down support for Cockroach DB.
#
# Updates:
#
# 31-Mar-2019 jdw add more speific tests for null value suggested by
#                 issue = MySQL SchemaDefLoader skip zero values #19
##
"""
Generic mapper of PDBx/mmCIF instance data to SQL loadable data files based on external
schema definition defined in class SchemaDefBase().
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import time

from rcsb.db.cockroach.CockroachDbUtil import CockroachDbQuery
from rcsb.db.processors.DataTransformFactory import DataTransformFactory
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.db.sql.SqlGen import SqlGenAdmin

logger = logging.getLogger(__name__)


class CockroachDbLoader(object):

    """Map PDBx/mmCIF instance data to SQL loadable data using external schema definition."""

    def __init__(self, schemaDefObj, ioObj=None, dbCon=None, workPath=".", cleanUp=False, warnings="default", verbose=True):
        """Map PDBx/mmCIF instance data to SQL loadable data using external schema definition.

        Args:
            schemaDefObj (object): Description
            ioObj (None, optional): Description
            dbCon (None, optional): Description
            workPath (str, optional): Description
            cleanUp (bool, optional): Description
            warnings (str, optional): Description
            verbose (bool, optional): Description
        """
        self.__verbose = verbose
        self.__debug = False
        self.__sD = schemaDefObj
        self.__ioObj = ioObj
        #
        self.__dbCon = dbCon
        self.__workingPath = workPath
        self.__pathList = []
        self.__cleanUp = cleanUp
        #
        # self.__sdp = SchemaDefDataPrep(schemaDefObj=schemaDefObj, ioObj=IoAdapter(), verbose=True)
        #
        self.__warningAction = warnings
        self.__fTypeRow = "skip-max-width"
        dtf = DataTransformFactory(schemaDefAccessObj=self.__sD, filterType=self.__fTypeRow)
        self.__sdp = SchemaDefDataPrep(schemaDefAccessObj=self.__sD, dtObj=dtf, workPath=self.__workingPath, verbose=self.__verbose)
        #

    def load(self, inputPathList=None, containerList=None, loadType="batch-file", deleteOpt=None, tableIdSkipD=None):
        """Load data for each table defined in the current schema definition object.
        Data are extracted from the input file or container list.

        Data source options:
              inputPathList = [<full path of target input file>, ....]

            or

              containerList = [ data container, ...]


            loadType  =  ['cockroack-insert' | 'cockroach-insert-many']
            deleteOpt = 'selected' | 'all'

            tableIdSkipD - searchable container with tableIds to be skipped on loading -

            Loading is performed using the current database server connection.

            Intermediate data files for 'batch-file' loading are created in the current working path.

            Returns True for success or False otherwise.

        Args:
            inputPathList (list, optional): Description
            containerList (list, optional): Description
            loadType (str, optional): Description
            deleteOpt (None, optional): Description
            tableIdSkipD (None, optional): Description

        Returns:
            TYPE: Description

        """
        tableIdSkipD = tableIdSkipD if tableIdSkipD is not None else {}
        if inputPathList is not None:
            tableDataDict, containerNameList = self.__sdp.fetch(inputPathList)
        elif containerList is not None:
            tableDataDict, containerNameList = self.__sdp.process(containerList)
        #
        #

        if loadType in ["cockroach-insert", "cockroach-insert-many"]:
            sqlMode = "single"
            if loadType in ["cockroach-insert-many"]:
                sqlMode = "many"
            for tableId, rowList in tableDataDict.items():
                if tableId in tableIdSkipD:
                    continue
                if deleteOpt in ["all", "truncate", "selected"] or rowList:
                    self.__cockroachInsertImport(tableId, rowList=rowList, containerNameList=containerNameList, deleteOpt=deleteOpt, sqlMode=sqlMode)
            return True
        else:
            pass

        return False

    def __cockroachInsertImport(self, tableId, rowList=None, containerNameList=None, deleteOpt="selected", sqlMode="many"):
        """Load the input table using sql cockroach templated inserts of the input rowlist of dictionaries (i.e. d[attributeId]=value).

        The containerNameList corresponding to the data within loadable data in rowList can be provided
        if 'selected' deletions are to performed prior to the the batch data inserts.

        deleteOpt = ['selected','all'] where 'selected' deletes rows corresponding to the input container
                    list before insert.   The 'all' options truncates the table prior to insert.

                    Deletions are performed in the absence of loadable data.

        Args:
            tableId (TYPE): Description
            rowList (None, optional): Description
            containerNameList (None, optional): Description
            deleteOpt (str, optional): Description
            sqlMode (str, optional): Description

        Returns:
            TYPE: Description

        """
        startTime = time.time()
        crQ = CockroachDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
        sqlGen = SqlGenAdmin(self.__verbose)
        #
        databaseName = self.__sD.getVersionedDatabaseName()
        tableDefObj = self.__sD.getTable(tableId)
        tableName = tableDefObj.getName()
        tableAttributeIdList = tableDefObj.getAttributeIdList()
        tableAttributeNameList = tableDefObj.getAttributeNameList()
        #
        sqlDeleteList = None
        if deleteOpt in ["selected", "delete"] and containerNameList is not None:
            deleteAttributeName = tableDefObj.getDeleteAttributeName()
            logger.debug("tableName %s delete attribute %s", tableName, deleteAttributeName)
            sqlDeleteList = sqlGen.deleteFromListSQL(databaseName, tableName, deleteAttributeName, containerNameList, chunkSize=10)
            # logger.debug("Delete SQL for %s : %r" % (tableId, sqlDeleteList))
        elif deleteOpt in ["all", "truncate"]:
            sqlDeleteList = [sqlGen.truncateTableSQL(databaseName, tableName)]
        #
        lenC = len(rowList)
        logger.debug("Deleting from table %s length %d", tableName, lenC)
        crQ.sqlCommandList(sqlDeleteList)
        endTime1 = time.time()
        logger.debug("Deleting succeeds for table %s %d rows at %s (%.3f seconds)", tableName, lenC, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime1 - startTime)
        logger.debug("Delete commands %s", sqlDeleteList)

        if not rowList:
            logger.debug("Skipping insert for table %s length %d", tableName, len(containerNameList))
            return True
        #
        logger.debug("Insert begins for table %s with row length %d", tableName, len(rowList))
        sqlInsertList = []
        tupL = list(zip(tableAttributeIdList, tableAttributeNameList))
        if sqlMode == "many":
            aList = []
            for tId, nm in tupL:
                aList.append(tId)
            #
            vLists = []
            for row in rowList:
                vList = []
                for tId, nm in tupL:
                    if row[tId] and row[tId] != r"\N":
                        vList.append(row[tId])
                    else:
                        vList.append(None)
                vLists.append(vList)
            #
            ret = crQ.sqlTemplateCommandMany(sqlTemplate=sqlGen.idInsertTemplateSQL(databaseName, tableDefObj, aList), valueLists=vLists)
            endTime = time.time()
            if ret:
                logger.debug("Insert succeeds for table %s %d rows at %s (%.3f seconds)", tableName, lenC, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - endTime1)
            else:
                logger.error("Insert fails for table %s %d rows at %s (%.3f seconds)", tableName, lenC, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - endTime1)
        else:
            lenT = -1
            lenR = -1
            aList = []
            for tId, nm in tupL:
                aList.append(nm)
            #
            for row in rowList:
                vList = []
                for tId, nm in tupL:
                    if row[tId] is not None and row[tId] != r"\N":
                        vList.append(row[tId])
                    else:
                        vList.append(None)
                sqlInsertList.append((sqlGen.insertTemplateSQL(databaseName, tableName, aList), vList))
            #
            lenT = len(sqlInsertList)
            lenR = crQ.sqlTemplateCommandList(sqlInsertList)
            #
            ret = lenR == lenT
            endTime = time.time()
            if ret:
                logger.debug(
                    "Insert succeeds for table %s %d of %d rows at %s (%.3f seconds)",
                    tableName,
                    lenR,
                    lenT,
                    time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                    endTime - endTime1,
                )
            else:
                logger.error(
                    "Insert fails for table %s %d of %d rows at %s (%.3f seconds)", tableName, lenR, lenT, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - endTime1
                )

        return ret
