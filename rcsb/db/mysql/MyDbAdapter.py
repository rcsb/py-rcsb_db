##
# File:  MyDdAdapter.py
# Date:  10-April-2014  J.Westbrook
#
# Updates:
#
# 11-April-2014  jdw Generalized from WFTaskRequestDBAdapter.py
# 13-April-2014  jdw working with workflow schema WFTaskRequest() -
# 19-Feb  -2015  jdw various fixes
# 10-July -2015  jdw Change method/class names from MySqlGen
# 10-March-2018  jdw  Py2->Py3 compatibility using driver fork described at https://mysqlclient.readthedocs.io/user_guide.html#
# 29-March-2018  jdw remove dependency on wwPDB configuration  -   Use generic configuratio object in constructor -
#  9-July -2018  jdw flip back to time.time()
#
###
##
"""
Database adapter for managing simple access and persistance queries using a MySQL relational database store.
"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import copy
import logging
import sys
import time

from rcsb.db.mysql.Connection import Connection
from rcsb.db.mysql.MyDbUtil import MyDbQuery
from rcsb.db.mysql.SqlGen import SqlGenAdmin, SqlGenCondition, SqlGenQuery

logger = logging.getLogger(__name__)


class MyDbAdapter(object):

    """ Database adapter for managing simple access and persistance queries using a MySQL relational database store.
    """

    def __init__(self, schemaDefObj, cfgOb=None, verbose=False):
        self.__verbose = verbose
        self.__debug = False
        #
        self.__sd = schemaDefObj
        self.__databaseName = self.__sd.getDatabaseName()
        self.__dbCon = None
        self.__cObj = None
        self.__defaultD = {}
        self.__attributeParameterMap = {}
        self.__attributeConstraintParameterMap = {}

    def __open(self, infoD):
        cObj = Connection()
        cObj.setPreferences(infoD)
        ok = cObj.openConnection()
        if ok:
            return cObj
        else:
            return None

    def __close(self, cObj):
        if cObj is not None:
            cObj.closeConnection()
            return True
        else:
            return False

    def __getClientConnection(self, cObj):
        return cObj.getClientConnection()

    def _open(self, dbServer=None, dbHost=None, dbName=None, dbUser=None, dbPw=None, dbSocket=None, dbPort=None):
        """  Open a connection to the data base server hosting WF status and tracking data -

             Internal configuration details will be used if these are not externally supplied.
        """
        infoD = {}
        infoD["DB_HOST"] = dbHost if dbHost is not None else self.__cI.get('SITE_DB_HOST_NAME')
        infoD["DB_PORT"] = dbPort if dbPort is not None else self.__cI.get('SITE_DB_PORT_NUMBER')
        infoD["DB_NAME"] = dbName if dbName is not None else self.__cI.get('SITE_DB_DATABASE_NAME')
        infoD["DB_USER"] = dbUser if dbUser is not None else self.__cI.get('SITE_DB_USER_NAME')
        infoD["DB_PW"] = dbPw if dbPw is not None else self.__cI.get('SITE_DB_PASSWORD')
        infoD["DB_SERVER"] = dbServer if dbServer is not None else self.__cI.get('SITE_DB_SERVER')
        infoD["DB_SOCKET"] = dbSocket if dbSocket is not None else self.__cI.get('SITE_DB_SOCKET')
        #
        self.__cObj = self.__open(infoD)
        self.__dbCon = self.__getClientConnection(self.__cObj)
        return self.__dbCon is not None

    def _close(self):
        """  Close connection to the data base server hosting WF status and tracking data -
        """
        if self.__dbCon is not None:
            self.__close(self.__cObj)
            self.__dbCon = None
            self.__cObj = None

    def _setDebug(self, flag=True):
        self.__debug = flag

    def _setDataStore(self, dataStoreName):
        """  Set/reassign the database for all subsequent transactions.
        """
        self.__databaseName = dataStoreName

    def _getParameterDefaultValues(self, contextId):
        if contextId is not None and contextId in self.__defaultD:
            return self.__defaultD[contextId]
        else:
            return {}

    def _setParameterDefaultValues(self, contextId, valueD):
        """  Set the optional lookup dictionary of default values for unspecified parameters...

             valueD = { 'paramName1': <default value1>,  'paramName2' : <default value2>, ...  }
        """
        self.__defaultD[contextId] = copy.deepcopy(valueD)
        return True

    def _setAttributeParameterMap(self, tableId, mapL):
        """  Set list of correspondences between method parameters and table attribute IDs.

             These correspondences are used to map key-value parameter pairs to their associated table attribute values.

             mapL=[ (atId1,paramName1),(atId2,paramName2),... ]
        """
        self.__attributeParameterMap[tableId] = mapL
        return True

    def _getDefaultAttributeParameterMap(self, tableId):
        """ Return default attributeId parameter name mappings for the input tableId.

             mapL=[ (atId1,paramName1),(atId2,paramName2),... ]
        """
        return self.__sd.getDefaultAttributeParameterMap(tableId)

    def _getAttributeParameterMap(self, tableId):
        """
           For the input table return the method keyword argument name to table attribute mapping -
        """
        if tableId is not None and tableId in self.__attributeParameterMap:
            return self.__attributeParameterMap[tableId]
        else:
            return []

    def _getConstraintParameterMap(self, tableId):
        """
           For the input table return the method keyword argument name to table attribute mapping for
           those attributes that serve as constraints for update transactions -

        """
        if tableId is not None and tableId in self.__attributeConstraintParameterMap:
            return self.__attributeConstraintParameterMap[tableId]
        else:
            return []

    def _setConstraintParameterMap(self, tableId, mapL):
        """  Set list of correspondences between method parameters and table attribute IDs to be used as
             contraints in update operations.

             These correspondences are used to map key-value paramter pairs to their associated table attribute values.

             mapL=[ (atId1,paramName1),(atId2,paramName2),... ]
        """
        self.__attributeConstraintParameterMap[tableId] = mapL
        return True

    def _createSchema(self):
        """ Create table schema using the current class schema definition
        """
        if (self.__debug):
            startTime = time.time()
            logger.info("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                      sys._getframe().f_code.co_name,
                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        ret = False
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True
            #
            tableIdList = self.__sD.getSchemaIdList()
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myAd = SqlGenAdmin(self.__verbose)

            for tableId in tableIdList:
                sqlL = []
                tableDefObj = self.__sD.getSchemaObject(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=self.__databaseName, tableDefObj=tableDefObj))

                ret = myQ.sqlCommand(sqlCommandList=sqlL)
                if (self.__verbose):
                    logger.info("+%s.%s for tableId %s server returns: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, tableId, ret))
                if (self.__debug):
                    logger.info("+%s.%s SQL: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, '\n'.join(sqlL)))
            if iOpened:
                self._close()
        except Exception as e:
            status = " table create error " + str(e)
            logger.info("+%s.%s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, status))
            if (self.__verbose):
                logger.exception("Failing with %s" % str(e))

        if (self.__debug):
            endTime = time.time()
            logger.info("\nCompleted %s %s at %s (%.3f seconds)\n" % (self.__class__.__name__,
                                                                      sys._getframe().f_code.co_name,
                                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                      endTime - startTime))
        return ret

    def _getSecondsSinceEpoch(self):
        """  Return number of seconds since the epoch at the precision of the local installation.
             Typically a floating point value with microsecond precision.

             This is used as the default time reference (e.g. timestamp) for monitoring task requests.
        """
        return time.time()

    def _insertRequest(self, tableId, contextId, **kwargs):
        """ Insert into the input table using the keyword value pairs provided as input arguments -

            The contextId controls the handling default values for unspecified parameters.
        """
        startTime = time.time()
        if self.__debug:
            logger.info("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                      sys._getframe().f_code.co_name,
                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        ret = False
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True
            #
            tableDefObj = self.__sD.getSchemaObject(tableId)
            #
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myAd = SqlGenAdmin(self.__verbose)
            defaultValD = self._getParameterDefaultValues(contextId=contextId)
            #
            # Create the attribute and value list for template --
            #
            vList = []
            aList = []
            for atId, kwId in self._getAttributeParameterMap(tableId=tableId):
                if kwId in kwargs and kwargs[kwId] is not None:
                    vList.append(kwargs[kwId])
                    aList.append(atId)
                else:
                    # use the default values if these exist
                    if kwId in defaultValD and defaultValD[kwId] is not None:
                        vList.append(defaultValD[kwId])
                        aList.append(atId)
                    else:
                        # appropriate null handling -- all fields must be assigned on insert --
                        vList.append(tableDefObj.getAppNullValue(atId))
                        aList.append(atId)

            sqlT = myAd.idInsertTemplateSQL(self.__databaseName, tableDefObj, insertAttributeIdList=aList)
            if self.__debug:
                logger.info("+%s.%s  aList %d vList %d\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, len(aList), len(vList)))
                logger.info("+%s.%s insert template sql=\n%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sqlT))
                logger.info("+%s.%s insert values vList=\n%r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, vList))
                # sqlC = sqlT % vList
                # logger.info("+%s.%s insert sql command =\n%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sqlC))
            ret = myQ.sqlTemplateCommand(sqlTemplate=sqlT, valueList=vList)
            if iOpened:
                self._close()

        except Exception as e:
            status = " insert operation error " + str(e)
            logger.info("+%s.%s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, status))
            if (self.__verbose):
                logger.exception("Failing with %s" % str(e))
        if self.__debug:
            endTime = time.time()
            logger.info("\nCompleted %s %s at %s (%.3f seconds)\n" % (self.__class__.__name__,
                                                                      sys._getframe().f_code.co_name,
                                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                      endTime - startTime))

        return ret

    def _updateRequest(self, tableId, contextId, **kwargs):
        """  Update the input table using the keyword value pairs provided as input arguments -

             The contextId controls the handling default values for unspecified parameters.

        """
        startTime = time.time()
        if self.__debug:
            logger.info("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                      sys._getframe().f_code.co_name,
                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        ret = False
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True
            #
            tableDefObj = self.__sD.getSchemaObject(tableId)
            #
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myAd = SqlGenAdmin(self.__verbose)
            defaultValD = self._getParameterDefaultValues(contextId=contextId)
            cIdList = self._getConstraintParameterMap(tableId)

            #
            # create the value list for template --
            #
            vList = []
            aList = []
            cList = []
            for atId, kwId in self._getAttributeParameterMap(tableId):
                if (atId, kwId) in cIdList:
                    continue
                if kwId in kwargs and kwargs[kwId] is not None:
                    vList.append(kwargs[kwId])
                    aList.append(atId)
                else:
                    if kwId in defaultValD and defaultValD[kwId] is not None:
                        vList.append(defaultValD[kwId])
                        aList.append(atId)

            for atId, kwId in cIdList:
                if kwId in kwargs and kwargs[kwId] is not None:
                    vList.append(kwargs[kwId])
                    cList.append(atId)

            sqlT = myAd.idUpdateTemplateSQL(self.__databaseName, tableDefObj, updateAttributeIdList=aList, conditionAttributeIdList=cList)
            if (self.__debug):
                logger.info("+%s.%s update sql: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sqlT))
                logger.info("+%s.%s update values: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, vList))
            ret = myQ.sqlTemplateCommand(sqlTemplate=sqlT, valueList=vList)
            if iOpened:
                self._close()

        except Exception as e:
            status = " update operation error " + str(e)
            logger.info("+%s.%s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, status))
            if (self.__verbose):
                logger.exception("Failing with %s" % str(e))
        if self.__debug:
            endTime = time.time()
            logger.info("\nCompleted %s %s at %s (%.3f seconds)\n" % (self.__class__.__name__,
                                                                      sys._getframe().f_code.co_name,
                                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                      endTime - startTime))
        return ret

    def _select(self, tableId, **kwargs):
        """ Construct a selection query for input table and optional constraints provided as keyword value pairs in the
            input arguments.  Return a list of dictionaries of these query details including all table attributes.
        """
        startTime = time.time()
        if self.__debug:
            logger.info("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                      sys._getframe().f_code.co_name,
                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        rdList = []
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True
            #
            tableDefObj = self.__sD.getSchemaObject(tableId)
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            sqlGen = SqlGenQuery(schemaDefObj=self.__sd, verbose=self.__verbose)
            sqlGen.setDatabase(databaseName=self.__databaseName)
            sqlConstraint = SqlGenCondition(schemaDefObj=self.__sd, verbose=self.__verbose)
            #
            atMapL = self._getAttributeParameterMap(tableId=tableId)
            for kwArg, kwVal in kwargs.items():
                for atId, kwId in atMapL:
                    if kwId == kwArg:
                        if tableDefObj.isAttributeStringType(atId):
                            cTup = ((tableId, atId), 'EQ', (kwargs[kwId], 'CHAR'))
                        else:
                            cTup = ((tableId, atId), 'EQ', (kwargs[kwId], 'OTHER'))
                        sqlConstraint.addValueCondition(cTup[0], cTup[1], cTup[2])
                        break
            #
            # Add optional constraints OR ordering by primary key attributes
            if len(sqlConstraint.get()) > 0:
                sqlGen.setCondition(sqlConstraint)
            else:
                for atId in tableDefObj.getPrimaryKeyAttributeIdList():
                    sqlGen.addOrderByAttributeId(attributeTuple=(tableId, atId))

            atIdList = self.__sd.getAttributeIdList(tableId)
            for atId in atIdList:
                sqlGen.addSelectAttributeId(attributeTuple=(tableId, atId))
            #
            sqlS = sqlGen.getSql()
            if (self.__debug):
                logger.info("+%s.%s selection sql: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sqlS))

            rowList = myQ.selectRows(queryString=sqlS)
            sqlGen.clear()
            #
            # return the result set as a list of dictionaries
            #
            for iRow, row in enumerate(rowList):
                rD = {}
                for colVal, atId in zip(row, atIdList):
                    rD[atId] = colVal
                if (self.__debug):
                    logger.info("+%s.%s result set row %d dictionary %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, iRow, rD.items()))
                rdList.append(rD)
            if iOpened:
                self._close()
        except Exception as e:
            status = " operation error " + str(e)
            logger.info("+%s.%s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, status))
            if (self.__verbose):
                logger.exception("Failing with %s" % str(e))

        if self.__debug:
            endTime = time.time()
            logger.info("\nCompleted %s %s at %s (%.3f seconds)\n" % (self.__class__.__name__,
                                                                      sys._getframe().f_code.co_name,
                                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                      endTime - startTime))
        return rdList

    def _deleteRequest(self, tableId, **kwargs):
        """ Delete from input table records identified by the keyword value pairs provided as input arguments -
        """
        startTime = time.time()
        if self.__debug:
            logger.info("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                      sys._getframe().f_code.co_name,
                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        ret = False
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True

            tableDefObj = self.__sD.getSchemaObject(tableId)
            #
            #
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose)
            myAd = SqlGenAdmin(self.__verbose)
            #
            # Create the attribute and value list for template --
            #
            vList = []
            aList = []
            for atId, kwId in self._getAttributeParameterMap(tableId):
                if kwId in kwargs and kwargs[kwId] is not None:
                    vList.append(kwargs[kwId])
                    aList.append(atId)

            sqlT = myAd.idDeleteTemplateSQL(self.__databaseName, tableDefObj, conditionAttributeIdList=aList)
            if (self.__debug):
                logger.info("+%s.%s delete sql: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sqlT))
                logger.info("+%s.%s delete values: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, vList))
            ret = myQ.sqlTemplateCommand(sqlTemplate=sqlT, valueList=vList)

            if iOpened:
                self._close()

        except Exception as e:
            status = " delete operation error " + str(e)
            logger.info("+%s.%s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, status))
            if (self.__verbose):
                logger.exception("Failing with %s" % str(e))

        if self.__debug:
            endTime = time.time()
            logger.info("\nCompleted %s %s at %s (%.3f seconds)\n" % (self.__class__.__name__,
                                                                      sys._getframe().f_code.co_name,
                                                                      time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                      endTime - startTime))
        return ret
