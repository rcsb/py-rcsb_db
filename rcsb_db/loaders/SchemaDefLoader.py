##
# File:    SchemaDefLoader.py
# Author:  J. Westbrook
# Date:    7-Jan-2013
# Version: 0.001 Initial version
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
#  4-Feb-2018 jdw add cockroach server support - 'cockroach-insert', 'cockroach-insert-many'
# 13-Mar-2018 jdw split data loading and data processing operations.
##
"""
Generic mapper of PDBx/mmCIF instance data to SQL loadable data files based on external
schema definition defined in class SchemaDefBase().

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import csv
import os
import time

from rcsb_db.sql.MyDbSqlGen import MyDbAdminSqlGen
from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.crate.CrateDbUtil import CrateDbQuery
from rcsb_db.cockroach.CockroachDbUtil import CockroachDbQuery
try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
#
#
from rcsb_db.loaders.SchemaDefDataPrep import SchemaDefDataPrep
#
import logging
logger = logging.getLogger(__name__)
#


class SchemaDefLoader(object):

    """ Map PDBx/mmCIF instance data to SQL loadable data using external schema definition.
    """

    def __init__(self, schemaDefObj, ioObj=IoAdapter(), dbCon=None, workPath='.', cleanUp=False, warnings='default', verbose=True):
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
        if loadType in ['batch-file', 'batch-file-append']:
            append = True if loadType == 'batch-file-append' else False
            exportList = self.__exportTdd(tableDataDict, colSep=self.__colSep, rowSep=self.__rowSep, append=append)
            for tableId, loadPath in exportList:
                if tableId in tableIdSkipD:
                    continue
                self.__batchFileImport(tableId, loadPath, sqlFilePath=None, containerNameList=containerNameList, deleteOpt=deleteOpt)
                if self.__cleanUp:
                    self.__cleanUpFile(loadPath)
            return True
        elif loadType == 'batch-insert':
            for tableId, rowList in tableDataDict.items():
                if tableId in tableIdSkipD:
                    continue
                if deleteOpt in ['all', 'selected'] or len(rowList) > 0:
                    self.__batchInsertImport(tableId, rowList=rowList, containerNameList=containerNameList, deleteOpt=deleteOpt)
            return True
        elif loadType in ['crate-insert', 'crate-insert-many']:
            sqlMode = 'single'
            if loadType in ['crate-insert-many']:
                sqlMode = 'many'
            for tableId, rowList in tableDataDict.items():
                if tableId in tableIdSkipD:
                    continue
                if deleteOpt in ['all', 'selected'] or len(rowList) > 0:
                    self.__crateInsertImport(tableId, rowList=rowList, containerNameList=containerNameList, deleteOpt=deleteOpt, sqlMode=sqlMode)
            return True
        elif loadType in ['cockroach-insert', 'cockroach-insert-many']:
            sqlMode = 'single'
            if loadType in ['cockroach-insert-many']:
                sqlMode = 'many'
            for tableId, rowList in tableDataDict.items():
                if tableId in tableIdSkipD:
                    continue
                if deleteOpt in ['all', 'truncate', 'selected'] or len(rowList) > 0:
                    self.__cockroachInsertImport(tableId, rowList=rowList, containerNameList=containerNameList, deleteOpt=deleteOpt, sqlMode=sqlMode)
            return True
        else:
            pass

        return False

    def __cleanUpFile(self, filePath):
        try:
            os.remove(filePath)
        except Exception as e:
            pass

    def makeLoadFilesMulti(self, dataList, procName, optionsD, workingDir):
        """ Create a loadable data file for each table defined in the current schema
            definition object.   Data is extracted from the input file list.

            Load files are creating in the current working path.

            Return the containerNames for the input path list, and path list for load files that are created.

        """
        try:
            pn = procName.split('-')[-1]
        except Exception:
            pn = procName

        exportFormat = optionsD['exportFormat'] if 'exportFormat' in optionsD else 'tdd'
        r1, r2 = self.makeLoadFiles(inputPathList=dataList, partName=pn, exportFormat=exportFormat)
        return dataList, r1, r2, []

    def makeLoadFiles(self, inputPathList, append=False, partName='1', exportFormat='tdd'):
        """ Create a loadable data file for each table defined in the current schema
            definition object.   Data is extracted from the input file list.

            Load files are created in the current working path.

            Return the containerNames for the input path list, and path list for load files that are created.


        """
        if exportFormat == 'tdd':
            tableDataDict, containerNameList = self.__fetch(inputPathList)
            return containerNameList, self.__exportTdd(tableDataDict, colSep=self.__colSep, rowSep=self.__rowSep, append=append, partName=partName)
        elif exportFormat == 'csv':
            tableDataDict, containerNameList = self.__fetch(inputPathList)
            return containerNameList, self.__exportCsv(tableDataDict, append=append, partName=partName)
        else:
            return [], []

    def __exportCsv(self, tableDict, append=False, partName='1'):
        """

        """
        modeOpt = 'a' if append else 'w'

        exportList = []
        for tableId, rowList in tableDict.items():
            if len(rowList) == 0:
                continue
            tObj = self.__sD.getTable(tableId)
            schemaAttributeIdList = tObj.getAttributeIdList()
            attributeNameList = tObj.getAttributeNameList()
            #
            fn = os.path.join(self.__workingPath, tableId + "-" + partName + ".csv")
            with open(fn, modeOpt, newline='') as ofh:
                csvWriter = csv.writer(ofh)
                csvWriter.writerow(attributeNameList)
                for rD in rowList:
                    csvWriter.writerow([rD[aId] for aId in schemaAttributeIdList])

            exportList.append((tableId, fn))
        return exportList

    def __exportTdd(self, tableDict, colSep='&##&\t', rowSep='$##$\n', append=False, partName='1'):
        modeOpt = 'a' if append else 'w'

        exportList = []
        for tableId, rowList in tableDict.items():
            tObj = self.__sD.getTable(tableId)
            schemaAttributeIdList = tObj.getAttributeIdList()
            #
            if len(rowList) > 0:
                fn = os.path.join(self.__workingPath, tableId + "-" + partName + ".tdd")
                ofh = open(fn, modeOpt)
                for rD in rowList:
                    ofh.write("%s%s" % (colSep.join([rD[aId] for aId in schemaAttributeIdList]), rowSep))
                ofh.close()
                exportList.append((tableId, fn))
        return exportList

    def loadBatchFiles(self, loadList=None, containerNameList=None, deleteOpt=None):
        """ Load data for each table defined in the current schema definition object using

            Data source options:

              loadList = [(tableId, <full path of load file), ....]
              containerNameList = [ data namecontainer, ...]

            deleteOpt = 'selected' | 'all','truncate'

            Loading is performed using the current database server connection.

            Returns True for success or False otherwise.

        """
        #
        startTime = time.time()
        for tableId, loadPath in loadList:
            ok = self.__batchFileImport(tableId, loadPath, sqlFilePath=None, containerNameList=containerNameList, deleteOpt=deleteOpt)
            if not ok:
                break
            if self.__cleanUp:
                self.__cleanUpFile(loadPath)
        #
        endTime = time.time()
        if self.__verbose:
            logger.debug("+SchemaDefLoader(loadBatchFiles) completed with status %r at %s (%.3f seconds)\n" %
                         (ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))
        return ok

    def delete(self, tableId, containerNameList=None, deleteOpt='all'):
        #
        startTime = time.time()
        sqlCommandList = self.__getSqlDeleteList(tableId, containerNameList=None, deleteOpt=deleteOpt)

        myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
        myQ.setWarning(self.__warningAction)
        ret = myQ.sqlCommand(sqlCommandList=sqlCommandList)
        #
        #
        endTime = time.time()
        if (self.__verbose):
            logger.debug("+SchemaDefLoader(delete) table %s server returns %r\n" % (tableId, ret))
            logger.debug("+SchemaDefLoader(delete) completed at %s (%.3f seconds)\n" %
                         (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))
            return ret
        else:
            if self.__verbose:
                logger.error("+SchemaDefLoader(delete) fails for %s\n" % tableId)
            return False

    def __getSqlDeleteList(self, tableId, containerNameList=None, deleteOpt='all'):
        """ Return the SQL delete commands for the input table and container name list.

        """
        databaseName = self.__sD.getDatabaseName()
        sqlGen = MyDbAdminSqlGen(self.__verbose)

        databaseName = self.__sD.getDatabaseName()
        tableDefObj = self.__sD.getTable(tableId)
        tableName = tableDefObj.getName()

        sqlDeleteList = []
        if deleteOpt in ['selected', 'delete'] and containerNameList is not None:
            deleteAttributeName = tableDefObj.getDeleteAttributeName()
            sqlDeleteList = sqlGen.deleteFromListSQL(databaseName, tableName, deleteAttributeName, containerNameList, chunkSize=50)
        elif deleteOpt in ['all', 'truncate']:
            sqlDeleteList = [sqlGen.truncateTableSQL(databaseName, tableName)]

        if (self.__verbose):
            logger.debug("+SchemaDefLoader(__getSqlDeleteList) delete SQL for %s : %r\n" % (tableId, sqlDeleteList))
        return sqlDeleteList

    def __batchFileImport(self, tableId, tableLoadPath, sqlFilePath=None, containerNameList=None, deleteOpt='all'):
        """ Batch load the input table using data in the input loadable data file.

            if sqlFilePath is provided then any generated SQL commands are preserved in this file.

            deleteOpt None|'selected'| 'all' or 'truncate'
        """
        startTime = time.time()
        databaseName = self.__sD.getDatabaseName()
        sqlGen = MyDbAdminSqlGen(self.__verbose)

        databaseName = self.__sD.getDatabaseName()
        tableDefObj = self.__sD.getTable(tableId)
        tableName = tableDefObj.getName()

        #
        if deleteOpt:
            sqlCommandList = self.__getSqlDeleteList(tableId, containerNameList=None, deleteOpt=deleteOpt)
        else:
            sqlCommandList = []

        if os.access(tableLoadPath, os.R_OK):
            tableDefObj = self.__sD.getTable(tableId)

            sqlCommandList.append(sqlGen.importTable(databaseName, tableDefObj, importPath=tableLoadPath))

            if (self.__verbose):
                logger.debug("+SchemaDefLoader(__batchFileImport) SQL import command\n%s\n" % sqlCommandList)
            #

        if sqlFilePath is not None:
            try:
                ofh = open(sqlFilePath, 'w')
                ofh.write("%s" % '\n'.join(sqlCommandList))
                ofh.close()
            except Exception as e:
                pass
        #
        myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
        myQ.setWarning(self.__warningAction)
        ret = myQ.sqlCommand(sqlCommandList=sqlCommandList)
        #
        #
        endTime = time.time()
        if (self.__verbose):
            logger.debug("+SchemaDefLoader(__batchFileImport) table %s server returns %r\n" % (tableId, ret))
            logger.debug("+SchemaDefLoader(__batchFileImport) completed at %s (%.3f seconds)\n" %
                         (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))
        return ret

    def loadBatchData(self, tableId, rowList=None, containerNameList=None, deleteOpt='selected'):
        return self.__batchInsertImport(tableId, rowList=rowList, containerNameList=containerNameList, deleteOpt=deleteOpt)

    def __batchInsertImport(self, tableId, rowList=None, containerNameList=None, deleteOpt='selected'):
        """ Load the input table using batch inserts of the input list of dictionaries (i.e. d[attributeId]=value).

            The containerNameList corresponding to the data within loadable data in rowList can be provided
            if 'selected' deletions are to performed prior to the the batch data inserts.

            deleteOpt = ['selected','all'] where 'selected' deletes rows corresponding to the input container
                        list before insert.   The 'all' options truncates the table prior to insert.

                        Deletions are performed in the absence of loadable data.

        """
        startTime = time.time()

        myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
        myQ.setWarning(self.__warningAction)
        sqlGen = MyDbAdminSqlGen(self.__verbose)
        #
        databaseName = self.__sD.getDatabaseName()
        tableDefObj = self.__sD.getTable(tableId)
        tableName = tableDefObj.getName()
        tableAttributeIdList = tableDefObj.getAttributeIdList()
        tableAttributeNameList = tableDefObj.getAttributeNameList()
        #
        sqlDeleteList = None
        if deleteOpt in ['selected', 'delete'] and containerNameList is not None:
            deleteAttributeName = tableDefObj.getDeleteAttributeName()
            sqlDeleteList = sqlGen.deleteFromListSQL(databaseName, tableName, deleteAttributeName, containerNameList, chunkSize=10)
            if (self.__verbose):
                logger.debug("+SchemaDefLoader(batchInsertImport) delete SQL for %s : %r\n" % (tableId, sqlDeleteList))
        elif deleteOpt in ['all', 'truncate']:
            sqlDeleteList = [sqlGen.truncateTableSQL(databaseName, tableName)]

        sqlInsertList = []
        for row in rowList:
            vList = []
            aList = []
            for id, nm in zip(tableAttributeIdList, tableAttributeNameList):
                if len(row[id]) > 0 and row[id] != r'\N':
                    vList.append(row[id])
                    aList.append(nm)
            sqlInsertList.append((sqlGen.insertTemplateSQL(databaseName, tableName, aList), vList))

        ret = myQ.sqlBatchTemplateCommand(sqlInsertList, prependSqlList=sqlDeleteList)
        if (self.__verbose):
            if (ret):
                logger.debug("+SchemaDefLoader(__batchInsertImport) batch insert completed for table %s rows %d\n" % (tableName, len(sqlInsertList)))
            else:
                logger.error("+SchemaDefLoader(__batchInsertImport) batch insert fails for table %s length %d\n" % (tableName, len(sqlInsertList)))

        endTime = time.time()
        if (self.__verbose):
            logger.debug("+SchemaDefLoader(__batchInsertImport) completed at %s (%.3f seconds)\n" %
                         (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

        return ret

    def __deleteFromTable(self, tableIdList, deleteValue):
        """  Delete data from the input table list where the schema table delete attribute
             has the input value "deleteValue".

        """
        sqlList = []
        sqlGen = MyDbAdminSqlGen(self.__verbose)
        for tableId in tableIdList:
            tableName = self.__sD.getTableName(tableId)
            tableDefObj = self.__sD.getTable(tableId)
            atName = tableDefObj.getDeleteAttributeName()
            sqlTemp = sqlGen.deleteTemplate(tableName, [atName])
            sqlList.append(sqlTemp % keyId)
        #
        return sqlList

    def __crateInsertImport(self, tableId, rowList=None, containerNameList=None, deleteOpt='selected', sqlMode='many', refresh=False):
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
        logger.debug("Insert begins for table %s with row length %d" % (tableName, len(rowList)))
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
            logger.debug("Insert succeeds for table %s %d of %d rows at %s (%.3f seconds)" %
                         (tableName, lenR, lenT, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))
        else:
            logger.error("Insert fails for table %s %d of %d rows at %s (%.3f seconds)" %
                         (tableName, lenR, lenT, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

        return ret

    def __cockroachInsertImport(self, tableId, rowList=None, containerNameList=None, deleteOpt='selected', sqlMode='many'):
        """ Load the input table using sql crate templated inserts of the input rowlist of dictionaries (i.e. d[attributeId]=value).

            The containerNameList corresponding to the data within loadable data in rowList can be provided
            if 'selected' deletions are to performed prior to the the batch data inserts.

            deleteOpt = ['selected','all'] where 'selected' deletes rows corresponding to the input container
                        list before insert.   The 'all' options truncates the table prior to insert.

                        Deletions are performed in the absence of loadable data.

        """
        startTime = time.time()
        crQ = CockroachDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
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
            # logger.debug("Delete SQL for %s : %r" % (tableId, sqlDeleteList))
        elif deleteOpt in ['all', 'truncate']:
            sqlDeleteList = [sqlGen.truncateTableSQL(databaseName, tableName)]
        #
        lenC = len(rowList)
        logger.debug("Deleting from table %s length %d" % (tableName, lenC))
        crQ.sqlCommandList(sqlDeleteList)
        endTime1 = time.time()
        logger.debug("Deleting succeeds for table %s %d rows at %s (%.3f seconds)" %
                     (tableName, lenC, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime1 - startTime))
        logger.debug("Delete commands %s" % sqlDeleteList)

        if len(rowList) == 0:
            logger.debug("Skipping insert for table %s length %d" % (tableName, len(containerNameList)))
            return True
        #
        logger.debug("Insert begins for table %s with row length %d" % (tableName, len(rowList)))
        sqlInsertList = []
        tupL = list(zip(tableAttributeIdList, tableAttributeNameList))
        if sqlMode == 'many':
            aList = []
            for id, nm in tupL:
                aList.append(id)
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
            ret = crQ.sqlTemplateCommandMany(sqlTemplate=sqlGen.idInsertTemplateSQL(databaseName, tableDefObj, aList), valueLists=vLists)
            endTime = time.time()
            if (ret):
                logger.debug("Insert succeeds for table %s %d rows at %s (%.3f seconds)" %
                             (tableName, lenC, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - endTime1))
            else:
                logger.error("Insert fails for table %s %d rows at %s (%.3f seconds)" %
                             (tableName, lenC, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - endTime1))
        else:
            lenT = -1
            lenR = -1
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
                sqlInsertList.append((sqlGen.insertTemplateSQL(databaseName, tableName, aList), vList))
            #
            lenT = len(sqlInsertList)
            lenR = crQ.sqlTemplateCommandList(sqlInsertList)
            #
            ret = (lenR == lenT)
            endTime = time.time()
            if (ret):
                logger.debug("Insert succeeds for table %s %d of %d rows at %s (%.3f seconds)" %
                             (tableName, lenR, lenT, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - endTime1))
            else:
                logger.error("Insert fails for table %s %d of %d rows at %s (%.3f seconds)" %
                             (tableName, lenR, lenT, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - endTime1))
        #

        return ret


if __name__ == '__main__':
    pass
