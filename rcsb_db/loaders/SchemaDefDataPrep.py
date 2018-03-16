##
# File:    SchemaDefDataPrep.py
# Author:  J. Westbrook
# Date:    13-Mar-2018
#
#
# Updates:
#      13-Mar-2018  jdw extracted data processing methods from SchemaDefLoader class
#      14-Mar-2018  jdw Add organization options for output loadable data -
#      14-Mar-2018. jdw Add document oriented extractors, add table exclusion list
#      15-Mar-2018  jdw Add filtering options for missing values  -
#      16-Mar-2018  jdw add styleType = rowwise_by_name_with_cardinality
#
##
"""
Generic mapper of PDBx/mmCIF instance data to a data organization consistent
with an external schema definition defined in class SchemaDefBase().

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import time
import pickle
import dateutil.parser
from operator import itemgetter

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
#

import logging
logger = logging.getLogger(__name__)
#


class SchemaDefDataPrep(object):

    """Generic mapper of PDBx/mmCIF instance data to a data organization consistent
        with an external schema definition defined in class SchemaDefBase().
    """

    def __init__(self, schemaDefObj, ioObj=IoAdapter(), verbose=True):
        self.__verbose = verbose
        self.__debug = False
        self.__sD = schemaDefObj
        self.__ioObj = ioObj
        #
        self.__overWrite = {}
        #
        self.__tableIdExcludeD = {}
        #

    def setTableIdExcludeList(self, tableIdList):
        """ Set list of table Ids to be excluded from any data extraction operations.
        """
        try:
            self.__tableIdExcludeD = {tId: True for tId in tableIdList}
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def fetch(self, inputPathList, styleType="rowwise_by_id", filterType="none"):
        """ Return a dictionary of loadable data for each table defined in the current schema
            definition object.   Data are extracted from all files in the input file list,
            and this is added in single schema instance such that data from multiple files are appended to a
            one collection of tables.     The organization of the loadable data is controlled
            by the style preference:

            Returns: tableDataDict, containerNameList

                 For styleType settings:

                     rowwise_by_id:      dict[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                     rowwise_by_name:    dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}

                filterTypes: "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"

        """
        tableDataDictById, containerNameList = self.__fetch(inputPathList, filterType)
        tableDataDict = self.__transformTableData(tableDataDictById, styleType=styleType)
        return tableDataDict, containerNameList

    def fetchDocuments(self, inputPathList, styleType="rowwise_by_id", filterType="none"):
        """ Return a dictionary of loadable data for each table defined in the current schema
            definition object.   Data are extracted from the each input file, and each data
            set is stored in a separate schema instance (document).  The organization
            of the loadable data is controlled by the style preference:

            Returns: tableDataDictList, containerNameList

                 For styleType settings:

                     rowwise_by_id:      dict[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                     rowwise_by_name:    dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}

                 filterTypes: "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"

        """
        tableDataDictList = []
        containerNameList = []
        for inputPath in inputPathList:
            tableDataDictById, cnList = self.__fetch([inputPath], filterType)
            tableDataDict = self.__transformTableData(tableDataDictById, styleType=styleType)
            tableDataDictList.append(tableDataDict)
            containerNameList.extend(cnList)
        #
        return tableDataDictList, containerNameList

    def process(self, containerList, styleType="rowwise_by_id", filterType="none"):
        """ Return a dictionary of loadable data for each table defined in the current schema
            definition object.   Data are extracted from all files in the input container list,
            and this is added in single schema instance such that data from multiple files are appended to a
            one collection of tables.  The organization of the loadable data is controlled by the style preference:

            Returns: tableDataDict, containerNameList

                 For styleType settings:

                     rowwise_by_id:      dict[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                     rowwise_by_name:    dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}

                filterTypes: "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"


        """
        tableDataDictById, containerNameList = self.__process(containerList, filterType)
        tableDataDict = self.__transformTableData(tableDataDictById, styleType=styleType)

        return tableDataDict, containerNameList

    def processDocuments(self, containerList, styleType="rowwise_by_id", filterType="none"):
        """ Return a dictionary of loadable data for each table defined in the current schema
            definition object.   Data are extracted from the each input container, and each data
            set is stored in a separate schema instance (document).  The organization of the loadable
            data is controlled by the style preference:

            Returns: tableDataDictList, containerNameList

                 For styleType settings:

                     rowwise_by_id:      dict[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                     rowwise_by_name:    dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}

            filterTypes:  "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"
        """
        tableDataDictList = []
        containerNameList = []
        for container in containerList:
            tableDataDictById, cnList = self.__process([container], filterType)
            #
            tableDataDict = self.__transformTableData(tableDataDictById, styleType=styleType)
            tableDataDictList.append(tableDataDict)
            containerNameList.extend(cnList)

        return tableDataDictList, containerNameList

    def __fetch(self, loadPathList, filterType):
        """ Internal method to create loadable data corresponding to the table schema definition
            from the input list of data files.

            Returns: dicitonary d[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                                and
                     container name list. []

        """
        startTime = time.time()
        #
        containerNameList = []
        tableDataDict = {}
        tableIdList = self.__sD.getTableIdList()
        for lPath in loadPathList:
            myContainerList = self.__ioObj.readFile(lPath)
            self.__mapData(myContainerList, tableIdList, tableDataDict, filterType)
            containerNameList.extend([myC.getName() for myC in myContainerList])
        #
        tableDataDictF = {}
        if 'drop-empty-tables' in filterType:
            for k, v in tableDataDict.items():
                if len(v) > 0:
                    tableDataDictF[k] = v
        else:
            tableDataDictF = tableDataDict
        #

        endTime = time.time()
        logger.debug("completed at %s (%.3f seconds)" %
                     (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

        return tableDataDictF, containerNameList

    def __process(self, containerList, filterType):
        """ Internal method to create loadable data corresponding to the table schema definition
            from the input container list.

            Returns: dicitonary d[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                                and
                     container name list. []
        """
        startTime = time.time()
        #
        containerNameList = []
        tableDataDict = {}
        tableIdList = self.__sD.getTableIdList()
        self.__mapData(containerList, tableIdList, tableDataDict, filterType)
        containerNameList.extend([myC.getName() for myC in containerList])
        #
        #
        tableDataDictF = {}
        if 'drop-empty-tables' in filterType:
            for k, v in tableDataDict.items():
                if len(v) > 0:
                    tableDataDictF[k] = v
        else:
            tableDataDictF = tableDataDict
        #
        logger.debug("Container name list: %r\n" % containerNameList)
        #
        endTime = time.time()
        logger.debug("completed at %s (%.3f seconds)\n" %
                     (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

        return tableDataDictF, containerNameList

    def __transformTableData(self, tableDataDictById, styleType="rowwise_by_name"):
        """  Reorganize and rename input table data object according to the input style preference:

             Input: tableDataDictById  (styleType="rowwise_by_id")
                                         dict[<tableId>]   = [ row1asDict[attributeId]=value,  row2asDict[attribute]=value, .. ]

             Output: rowwise_by_name:     dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}
      rowwise_by_name_with_cardinality:  same as rowwise_byName with special handing of tables with unit cardinality
                                                          dict[<tableName>] = row1Dict[attributeName]=value (singleton row)
        """
        rD = {}

        try:
            if styleType == "rowwise_by_name":
                for tableId in tableDataDictById:
                    tableDef = self.__sD.getTable(tableId)
                    tableName = self.__sD.getTableName(tableId)
                    logger.debug("Transforming table id %s to name %s" % (tableId, tableName))
                    iRowDList = tableDataDictById[tableId]
                    oRowDList = []
                    for iRowD in iRowDList:
                        oRowD = {}
                        for atId in iRowD:
                            oRowD[tableDef.getAttributeName(atId)] = iRowD[atId]
                        oRowDList.append(oRowD)
                    rD[tableName] = oRowDList
            elif styleType == "rowwise_by_name_with_cardinality":
                tupL = []
                sum = 0.0
                for tableId in tableDataDictById:
                    tableDef = self.__sD.getTable(tableId)
                    tableName = self.__sD.getTableName(tableId)
                    unitCard = self.__sD.hasUnitCardinality(tableId)
                    iRowDList = tableDataDictById[tableId]
                    #
                    megaBytes = float(len(pickle.dumps(iRowDList))) / 1000000.0
                    sum += megaBytes
                    tupL.append((tableId, megaBytes))
                    if unitCard and len(iRowDList) == 1:
                        iRowD = iRowDList[0]
                        oRowD = {}
                        for atId in iRowD:
                            oRowD[tableDef.getAttributeName(atId)] = iRowD[atId]
                        rD[tableName] = oRowD
                    else:
                        oRowDList = []
                        for iRowD in iRowDList:
                            oRowD = {}
                            for atId in iRowD:
                                oRowD[tableDef.getAttributeName(atId)] = iRowD[atId]
                            oRowDList.append(oRowD)
                        rD[tableName] = oRowDList
                #
                thisId = '?'
                if 'ENTRY' in tableDataDictById and 'STRUCTURE_ID' in tableDataDictById['ENTRY']:
                    thisId = tableDataDictById['ENTRY']['STRUCTURE_ID']
                sTupL = sorted(tupL, key=itemgetter(1), reverse=True)
                for tup in sTupL[:8]:
                    logger.debug("Transforming %s table id %s size %.4f (of %.4f) MB" % (thisId, tup[0], tup[1], sum))
                    #
            elif styleType == "columnwise_by_name":
                for tableId in tableDataDictById:
                    tableDef = self.__sD.getTable(tableId)
                    tableName = self.__sD.getTableName(tableId)
                    iRowDList = tableDataDictById[tableId]
                    colD = {}
                    for iRowD in iRowDList:
                        for atId in iRowD:
                            atName = tableDef.getAttributeName(atId)
                            if atName not in colD:
                                colD[atName] = []
                            colD[atName].append(iRowD[atId])
                    rD[tableName] = colD
            elif styleType == "rowwise_no_name":
                for tableId in tableDataDictById:
                    tableDef = self.__sD.getTable(tableId)
                    tableName = self.__sD.getTableName(tableId)
                    atIdList = self.__sD.getAttributeIdList(tableId)
                    atNameList = self.__sD.getAttributeNameList(tableId)
                    #
                    iRowDList = tableDataDictById[tableId]
                    oRowList = []
                    for iRowD in iRowDList:
                        oRowL = []
                        for atId in atIdList:
                            oRowL.append(iRowD[atId])
                        oRowList.append(oRowL)
                    #
                    rD[tableName] = {'attributes': atNameList, 'data': oRowList}
            elif styleType == "rowwise_by_id":
                rD = tableDataDictById
            else:
                rD = tableDataDictById
                logger.warning("Unsupported style type %s" % styleType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            rD = tableDataDictById

        return rD

    def __showOverwrite(self):
        #
        if (self.__verbose):
            if len(self.__overWrite) > 0:
                for k, v in self.__overWrite.items():
                    logger.debug("+SchemaDefLoader(load) %r maximum width %r" % (k, v))

    def __evalMapFunction(self, dataContainer, rowList, attributeId, functionName, functionArgs=None):
        if (functionName == "datablockid()"):
            val = dataContainer.getName()
            for rowD in rowList:
                rowD[attributeId] = val
            return True
        else:
            return False

    def __mapData(self, containerList, tableIdList, tableDataDict, filterType="none"):
        """
           Process instance data in the input container list and map these data to the
           table schema definitions in the input table list.

           Returns: mapped data as a list of dictionaries with attribute Id key for
                    each schema table.  Data are appended to any existing table in
                    the input dictionary.


        """
        for myContainer in containerList:
            for tableId in tableIdList:
                if tableId in self.__tableIdExcludeD:
                    logger.debug("Skipping excluded table %s" % tableId)
                    continue
                if tableId not in tableDataDict:
                    tableDataDict[tableId] = []
                tObj = self.__sD.getTable(tableId)
                #
                # Instance categories that are mapped to the current table -
                #
                mapCategoryNameList = tObj.getMapInstanceCategoryList()
                numMapCategories = len(mapCategoryNameList)
                #
                # Attribute Ids that are not directly mapped to the schema (e.g. functions)
                #
                otherAttributeIdList = tObj.getMapOtherAttributeIdList()

                if numMapCategories == 1:
                    rowList = self.__mapInstanceCategory(tObj, mapCategoryNameList[0], myContainer, filterType)
                elif numMapCategories >= 1:
                    rowList = self.__mapInstanceCategoryList(tObj, mapCategoryNameList, myContainer, filterType)

                for atId in otherAttributeIdList:
                    fName = tObj.getMapAttributeFunction(atId)
                    fArgs = tObj.getMapAttributeFunctionArgs(atId)
                    self.__evalMapFunction(dataContainer=myContainer, rowList=rowList, attributeId=atId, functionName=fName, functionArgs=fArgs)

                tableDataDict[tableId].extend(rowList)

        return tableDataDict

    def __mapInstanceCategory(self, tObj, categoryName, myContainer, filterType):
        """ Extract data from the input instance category and map these data to the organization
            in the input table schema definition object.

            No merging is performed by this method.

            Return a list of dictionaries with schema attribute Id keys containing data
            mapped from the input instance category.
        """
        #
        dropEmptyFlag = False
        if 'drop-empty-attributes' in filterType:
            dropEmptyFlag = True
        skipMaxWidthFlag = False
        if 'skip-max-width' in filterType:
            skipMaxWidthFlag = True
        assignDateFlag = False
        if 'assign-dates' in filterType:
            assignDateFlag = True

        retList = []
        catObj = myContainer.getObj(categoryName)
        if catObj is None:
            return retList

        attributeIndexDict = catObj.getAttributeIndexDict()
        schemaTableId = tObj.getId()
        schemaAttributeMapDict = tObj.getMapAttributeDict()
        schemaAttributeIdList = tObj.getAttributeIdList()
        nullValueDict = tObj.getSqlNullValueDict()
        maxWidthDict = tObj.getStringWidthDict()
        curAttributeIdList = tObj.getMapInstanceAttributeIdList(categoryName)

        for row in catObj.getRowList():
            d = {}
            if not dropEmptyFlag:
                for atId in schemaAttributeIdList:
                    d[atId] = nullValueDict[atId]

            for atId in curAttributeIdList:
                try:
                    atName = schemaAttributeMapDict[atId]
                    if atName not in attributeIndexDict:
                        continue
                    val = row[attributeIndexDict[atName]]
                    if skipMaxWidthFlag:
                        maxW = 0
                    else:
                        maxW = maxWidthDict[atId]

                    lenVal = len(val)
                    if dropEmptyFlag and ((lenVal == 0) or (val == '?') or (val == '.')):
                        continue
                    if assignDateFlag and tObj.isAttributeDateType(atId) and not ((lenVal == 0) or (val == '?') or (val == '.')):
                        d[atId] = self.__assignDateType(atId, val)
                        continue
                    if maxW > 0:
                        if lenVal > maxW:
                            tup = (schemaTableId, atId)
                            if tup in self.__overWrite:
                                self.__overWrite[tup] = max(self.__overWrite[tup], lenVal)
                            else:
                                self.__overWrite[tup] = lenVal

                        d[atId] = val[:maxW] if ((val != '?') and (val != '.')) else nullValueDict[atId]
                    else:
                        d[atId] = val if ((val != '?') and (val != '.')) else nullValueDict[atId]
                except Exception as e:
                    if (self.__verbose):
                        logger.error("\n+ERROR - processing table %s attribute %s row %r\n" % (schemaTableId, atId, row))
                        logger.exception("Failing with %s" % str(e))

            retList.append(d)

        return retList

    def __mapInstanceCategoryList(self, tObj, categoryNameList, myContainer, filterType):
        """ Extract data from the input instance categories and map these data to the organization
            in the input table schema definition object.

            Data from contributing categories is merged using attributes specified in
            the merging index for the input table.

            Return a list of dictionaries with schema attribute Id keys containing data
            mapped from the input instance category.
        """
        #
        dropEmptyFlag = False
        if 'drop-empty-attributes' in filterType:
            dropEmptyFlag = True
        skipMaxWidthFlag = False
        if 'skip-max-width' in filterType:
            skipMaxWidthFlag = True
        assignDateFlag = False
        if 'assign-dates' in filterType:
            assignDateFlag = True
        #
        mD = {}
        for categoryName in categoryNameList:
            catObj = myContainer.getObj(categoryName)
            if catObj is None:
                continue

            attributeIndexDict = catObj.getAttributeIndexDict()
            schemaTableId = tObj.getId()
            schemaAttributeMapDict = tObj.getMapAttributeDict()
            schemaAttributeIdList = tObj.getAttributeIdList()
            nullValueDict = tObj.getSqlNullValueDict()
            maxWidthDict = tObj.getStringWidthDict()
            curAttributeIdList = tObj.getMapInstanceAttributeIdList(categoryName)
            #
            # dictionary of merging indices for each attribute in this category -
            #
            indL = tObj.getMapMergeIndexAttributes(categoryName)

            for row in catObj.getRowList():
                # initialize full table row --
                d = {}
                if not dropEmptyFlag:
                    for atId in schemaAttributeIdList:
                        d[atId] = nullValueDict[atId]

                # assign merge index
                mK = []
                for atName in indL:
                    try:
                        mK.append(row[attributeIndexDict[atName]])
                    except Exception as e:
                        # would reflect a serious issue of missing key-
                        if (self.__debug):
                            logger.exception("Failing with %s" % str(e))

                for atId in curAttributeIdList:
                    try:
                        atName = schemaAttributeMapDict[atId]
                        val = row[attributeIndexDict[atName]]
                        if skipMaxWidthFlag:
                            maxW = 0
                        else:
                            maxW = maxWidthDict[atId]
                        lenVal = len(val)
                        if dropEmptyFlag and ((lenVal == 0) or (val == '?') or (val == '.')):
                            continue
                        if assignDateFlag and tObj.isAttributeDateType(atId) and not ((lenVal == 0) or (val == '?') or (val == '.')):
                            d[atId] = self.__assignDateType(atId, val)
                            continue
                        if maxW > 0:
                            if lenVal > maxW:
                                logger.error("+ERROR - Table %s attribute %s length %d exceeds %d\n" % (schemaTableId, atId, lenVal, maxW))
                            d[atId] = val[:maxW] if ((val != '?') and (val != '.')) else nullValueDict[atId]
                        else:
                            d[atId] = val if ((val != '?') and (val != '.')) else nullValueDict[atId]
                    except Exception as e:
                        # only for testing -
                        if (self.__debug):
                            logger.exception("Failing with %s" % str(e))
                #
                # Update this row using exact matching of the merging key --
                # jdw  - will later add more complex comparisons
                #
                tk = tuple(mK)
                if tk not in mD:
                    mD[tk] = {}

                mD[tk].update(d)

        return mD.values()

    def __assignDateType(self, atId, value):
        """   yyyy-mm-dd  or yyyy-mm-dd:hh:mm:ss
        """
        try:
            value = value.replace(":", " ", 1)
            return dateutil.parser.parse(value)
        except Exception as e:
            logger.exception("Attribute processing error %s %s : %s" % (atId, value, str(e)))
        return value

if __name__ == '__main__':
    pass
