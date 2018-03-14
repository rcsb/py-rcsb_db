##
# File:    SchemaDefCrateDbLoader.py
# Author:  J. Westbrook
# Date:    18-Jan-2018
#
#  Variant to support stripped down support for Crate DB.
#
# Updates:
#  9-Jan-2013 jdw add merging index support for loading tables from multiple
#                 instance categories.
# 10-Jan-2013 jdw add null value filter and maximum string width checks.
# 13-Jan-2013 jdw provide batch file and batch insert loading modes.
# 15-Jan-2013 jdw add pre-load delete options
# 19-Jan-2012 jdw add IoAdapter
# 20-Jan-2013 jdw add append options for batch file loading
# 20-Jan-2013 jdw provide methods for loading container lists
#  2-Oct-2017 jdw escape null string '\N' and suppress print statements
# 20-Dec-2017 jdw set to use python adapter -
# 30-Dec-2017 jdw add crate server support - 'crate-insert', 'crate-insert-many'
#  4-Jan-2018 jdw add table skipping filters
##
"""
Generic mapper of PDBx/mmCIF instance data to SQL loadable data files based on external
schema definition defined in class SchemaDefBase().

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import os
import time

from rcsb_db.sql.MyDbSqlGen import MyDbAdminSqlGen
from rcsb_db.crate.CrateDbUtil import CrateDbQuery
from rcsb_db.loaders.SchemaDefDataPrep import SchemaDefDataPrep
#
#

import logging
logger = logging.getLogger(__name__)
#


class SchemaDefLoader(object):

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
        self.__colSep = '&##&\t'
        self.__rowSep = '$##$\n'
        #
        self.__warningAction = warnings
        self.__overWrite = {}
        self.__sdp = SchemaDefDataPrep(schemaDefObj=schemaDefObj, ioObj=IoAdapter(), verbose=True)

    def setWarning(self, action):
        if action in ['error', 'ignore', 'default']:
            self.__warningAction = action
            return True
        else:
            self.__warningAction = 'default'
            return False

    def setDelimiters(self, colSep=None, rowSep=None):
        """  Set column and row delimiters for intermediate data files used for
             batch-file loading operations.
        """
        self.__colSep = colSep if colSep is not None else '&##&\t'
        self.__rowSep = rowSep if rowSep is not None else '$##$\n'
        return True

    def load(self, inputPathList=None, containerList=None, loadType='batch-file', deleteOpt=None, tableIdSkipD=None):
        """ Load data for each table defined in the current schema definition object.
            Data are extracted from the input file list.

            Data source options:

              inputPathList = [<full path of target input file>, ....]

            or

              containerList = [ data container, ...]


            loadType  =  ['batch-file' | 'batch-insert']
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


    def __export(self, tableDict, colSep='&##&\t', rowSep='$##$\n', append=False, partName='1'):
        modeOpt = 'a' if append else 'w'

        exportList = []
        for tableId, rowList in tableDict.items():
            tObj = self.__sD.getTable(tableId)
            schemaAttributeIdList = tObj.getAttributeIdList()
            #
            if len(rowList) > 0:
                fn = os.path.join(self.__workingPath, tableId + "-loadable-" + partName + ".tdd")
                ofh = open(fn, modeOpt)
                for rD in rowList:
                    ofh.write("%s%s" % (colSep.join([rD[aId] for aId in schemaAttributeIdList]), rowSep))
                ofh.close()
                exportList.append((tableId, fn))
        return exportList

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
        sqlGen = MyDbAdminSqlGen(self.__verbose)
        #
        databaseName = self.__sD.getVersionedDatabaseName()
        tableDefObj = self.__sD.getTable(tableId)
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
                    if len(row[id]) > 0 and row[id] != r'\N':
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
                    if len(row[id]) > 0 and row[id] != r'\N':
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
