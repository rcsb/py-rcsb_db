##
# File:    CrateDbLoader.py
# Author:  J. Westbrook
# Date:    1-Apr-2018
#
#  Loader variant to support stripped down support for Crate DB.
#
# Updates:
#
# 31-Mar-2019 jdw add more speific tests for null value suggested by
#                 issue = MySQL SchemaDefLoader skip zero values #19
##
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

from rcsb.db.crate.CrateDbUtil import CrateDbQuery
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.db.sql.SqlGen import SqlGenAdmin

#
try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter

logger = logging.getLogger(__name__)


class CrateDbLoader(object):

    """ Map PDBx/mmCIF instance data to SQL loadable data using external schema definition.
    """

    def __init__(self, schemaDefObj, ioObj=None, dbCon=None, workPath='.', cleanUp=False, warnings='default', verbose=True):
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
        self.__sdp = SchemaDefDataPrep(schemaDefAccessObj=schemaDefObj, ioObj=IoAdapter(), verbose=True)

    def load(self, inputPathList=None, containerList=None, loadType='batch-file', deleteOpt=None, tableIdSkipD=None):
        """ Load data for each table defined in the current schema definition object.
            Data are extracted from the input path or container list.

            Data source options:

              inputPathList = [<full path of target input file>, ....]

            or

              containerList = [ data container, ...]


            loadType  =  ['crate-insert' | 'crate-insert-many']
            deleteOpt = 'selected' | 'all'

            tableIdSkipD - searchable container with tableIds to be skipped on loading -

            Loading is performed using the current database server connection.

            Intermediate data files for 'batch-file' loading are created in the current working path.

            Returns True for success or False otherwise.

        """
        tableIdSkipD = tableIdSkipD if tableIdSkipD is not None else {}
        if inputPathList is not None:
            tableDataDict, containerNameList = self.__sdp.fetch(inputPathList)
        elif containerList is not None:
            tableDataDict, containerNameList = self.__sdp.process(containerList)
        #

        #
        if loadType in ['crate-insert', 'crate-insert-many']:
            sqlMode = 'single'
            if loadType in ['crate-insert-many']:
                sqlMode = 'many'
            for tableId, rowList in tableDataDict.items():
                if tableId in tableIdSkipD:
                    continue
                if deleteOpt in ['all', 'selected'] or len(rowList) > 0:
                    self.__crateInsertImport(tableId, rowList=rowList, containerNameList=containerNameList, deleteOpt=deleteOpt, sqlMode=sqlMode)
            return True
        else:
            pass

        return False

    def __crateInsertImport(self, tableId, rowList=None, containerNameList=None, deleteOpt='selected', sqlMode='many', refresh=True):
        """ Load the input table using sql crate templated inserts of the input rowlist of dictionaries (i.e. d[attributeId]=value).

            The containerNameList corresponding to the data within loadable data in rowList can be provided
            if 'selected' deletions are to performed prior to the the batch data inserts.

            deleteOpt = ['selected','all'] where 'selected' deletes rows corresponding to the input container
                        list before insert.   The 'all' options truncates the table prior to insert.

                        Deletions are performed in the absence of loadable data.

        """
        startTime = time.time()
        sqlRefresh = None
        crQ = CrateDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
        sqlGen = SqlGenAdmin(self.__verbose)
        #
        databaseName = self.__sD.getVersionedDatabaseName()
        tableDefObj = self.__sD.getSchemaObject(tableId)
        tableName = tableDefObj.getName()
        tableAttributeIdList = tableDefObj.getAttributeIdList()
        tableAttributeNameList = tableDefObj.getAttributeNameList()
        #
        sqlDeleteList = None
        if deleteOpt in ['selected', 'delete'] and containerNameList is not None:
            deleteAttributeName = tableDefObj.getDeleteAttributeName()
            sqlDeleteList = sqlGen.deleteFromListSQL(databaseName, tableName, deleteAttributeName, containerNameList, chunkSize=10)
            logger.debug("Delete SQL for %s : %r" % (tableId, sqlDeleteList))
        elif deleteOpt in ['all', 'truncate']:
            sqlDeleteList = [sqlGen.truncateTableSQL(databaseName, tableName)]
        #
        logger.debug("Deleting from table %s length %d" % (tableName, len(containerNameList)))
        crQ.sqlCommandList(sqlDeleteList)
        logger.debug("Delete commands %s" % sqlDeleteList)
        if len(rowList) == 0:
            return True
        if refresh:
            sqlRefresh = sqlGen.refreshTableSQLCrate(databaseName, tableName)
            crQ.sqlCommand(sqlRefresh)
        #
        logger.info("Insert begins for table %s with row length %d" % (tableName, len(rowList)))
        sqlInsertList = []
        tupL = list(zip(tableAttributeIdList, tableAttributeNameList))
        if sqlMode == 'many':
            aList = []
            for id, nm in tupL:
                aList.append(nm)
            #
            vLists = []
            for row in rowList:
                vList = []
                for id, nm in tupL:
                    if row[id] and row[id] != r'\N':
                        vList.append(row[id])
                    else:
                        vList.append(None)
                vLists.append(vList)
            #
            lenT = len(vLists)
            lenR = crQ.sqlTemplateCommandMany(sqlTemplate=sqlGen.insertTemplateSQLCrate(databaseName, tableName, aList), valueLists=vLists)
            ret = (lenR == len(vLists))
        else:
            aList = []
            for id, nm in tupL:
                aList.append(nm)
            #
            for row in rowList:
                vList = []
                for id, nm in tupL:
                    if row[id] is not None and row[id] != r'\N':
                        vList.append(row[id])
                    else:
                        vList.append(None)
                sqlInsertList.append((sqlGen.insertTemplateSQLCrate(databaseName, tableName, aList), vList))
            #
            lenT = len(sqlInsertList)
            lenR = crQ.sqlTemplateCommandList(sqlInsertList)
            ret = (lenR == lenT)
        if refresh:
            sqlRefresh = sqlGen.refreshTableSQLCrate(databaseName, tableName)
            crQ.sqlCommand(sqlRefresh)
        #
        endTime = time.time()
        if (ret):
            logger.info("Insert succeeds for table %s %d of %d rows at %s (%.3f seconds)" %
                        (tableName, lenR, lenT, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))
        else:
            logger.info("Insert fails for table %s %d of %d rows at %s (%.3f seconds)" %
                        (tableName, lenR, lenT, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

        return ret


if __name__ == '__main__':
    pass
