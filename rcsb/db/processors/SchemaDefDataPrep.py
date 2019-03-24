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
#      19-Mar-2018  jdw add container name or input file path as a hidden document field
#      22-Mar-2018  jdw add tableInclude details to limit the content scope
#      22-Mar-2018  jdw change contentSelectors to documentSelectors ...
#      25-Mar-2018  jdw improve handling of selected / excluded tables -
#       9-Apr-2018  jdw add attribute level filtering
#      11-Apr-2018  jdw integrate DataTransformFactory()
#      15-Jun-2018  jdw rename documentSelectors to dataSelectors as these filters are
#                       applied to filter in coming data sets.
#      18-Jun-2018  jdw Handle all IO using MarshalUtil(), eliminate adhoc status table,
#                       add new dynamic methods -
#      19-Jun-2018  jdw Change file paths to locator lists -
#       6-Aug-2018  jdw Move properties stored locally in __loadInfo to the base container
#                       deprecating __loadInfo in this class
#      20-Nov-2018  jdw add addDocumentPrivateAttributes() to inject private document attributes,
#                       This method is collection dependent which is awkward in this class.
#       4-Dec-2018  jdw make insertion of private keys optional
#       8-Dec-2018  jdw add check to return False if a selection category does not exist.
#      10-Jan-2019  jdw pass down container name to processRecord() to allow for better diagnostics.
#       5-Feb-2019  jdw generalize locatorList to locatorObjList and associated dependent changes,
#                       and add __mergeContainers() -
#
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


import datetime
import logging
import time

from rcsb.db.processors.SchemaDefReShape import SchemaDefReShape
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class SchemaDefDataPrep(object):

    """Generic mapper of PDBx/mmCIF instance data to a data organization consistent
       with an external schema definition defined in class SchemaDefBase().
    """

    def __init__(self, schemaDefAccessObj, dtObj=None, workPath=None, verbose=True):
        self.__verbose = verbose
        self.__workPath = workPath
        self.__debug = False
        self.__sD = schemaDefAccessObj
        self.__mU = MarshalUtil(workPath=workPath)
        self.__dtObj = dtObj
        #
        self.__overWrite = {}
        #
        self.__schemaIdExcludeD = {}
        self.__schemaIdIncludeD = {}
        #
        self.__reShape = SchemaDefReShape(schemaDefAccessObj, workPath=workPath, verbose=verbose)
        #

    def setSchemaIdExcludeList(self, schemaIdList):
        """ Set list of schema Ids to be excluded from any data extraction operations.
        """
        try:
            self.__schemaIdExcludeD = {sId: True for sId in schemaIdList}
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def setSchemaIdIncludeList(self, schemaIdList):
        """ Set list of schema Ids for inclusion in any data extraction operations. (subject to exclusion).

            This list will limit the candidate tables selected from the current schema and exclusions if
            specified will still be applied.
        """
        try:
            self.__schemaIdIncludeD = {sId: True for sId in schemaIdList}
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __getTimeStamp(self):
        utcnow = datetime.datetime.utcnow()
        ts = utcnow.strftime("%Y-%m-%d:%H:%M:%S")
        return ts

    def __mergeContainers(self, locatorObj, format="mmcif", mergeTarget=0):
        """ Consolidate content in auxiliary files locatorObj[1:] into
            locatorObj[0] container index 'mergeTarget'.

        """
        #
        cL = []
        try:
            if isinstance(locatorObj, str):
                cL = self.__mU.doImport(locatorObj, format=format)
                return cL if cL else []
            elif isinstance(locatorObj, (list, tuple)) and len(locatorObj) > 0:
                d = locatorObj[0]
                kw = d['kwargs']
                cL = self.__mU.doImport(d['locator'], format=d['format'], **kw)
                if cL and len(cL) > 0:
                    for d in locatorObj[1:]:
                        kw = d['kwargs']
                        rObj = self.__mU.doImport(d['locator'], format=d['format'], **kw)
                        mergeL = rObj if rObj else []
                        for mc in mergeL:
                            cL[mergeTarget].merge(mc)
                #
                return cL
            else:
                return []
        except Exception as e:
            logger.exception("Failing for %r with %s" % (locatorObj, str(e)))

        return cL

    def getContainerList(self, locatorObjList, filterType="none"):
        """ Return the data container list obtained by parsing the input locator list.
        """
        cL = []
        for locatorObj in locatorObjList:
            # myContainerList = self.__mU.doImport(locatorObj, format="mmcif")
            # logger.info("locatorObj is %r" % type(locatorObj))
            myContainerList = self.__mergeContainers(locatorObj, format="mmcif", mergeTarget=0)
            for c in myContainerList:
                cL.append(c)
        return cL

    def getLocatorsFromPaths(self, locatorObjList, pathList, locatorIndex=0):
        """ Return locator objects with paths (locatorObjIndex) matching the input pathList.

        """
        # index the input locatorObjList
        rL = []
        try:
            if locatorObjList and isinstance(locatorObjList[0], str):
                return pathList
            #
            locIdx = {}
            for ii, locatorObj in enumerate(locatorObjList):
                if 'locator' in locatorObj[locatorIndex]:
                    locIdx[locatorObj[locatorIndex]['locator']] = ii
            #
            for pth in pathList:
                jj = locIdx[pth] if pth in locIdx else None
                if jj is not None:
                    rL.append(locatorObjList[jj])
        except Exception as e:
            logger.exception("Faliing with %s" % str(e))
        #
        return rL

    def getLocatorPaths(self, locatorObjList, locatorIndex=0):
        try:
            if locatorObjList and isinstance(locatorObjList[0], str):
                return locatorObjList
            else:
                return [locatorObj[locatorIndex]['locator'] for locatorObj in locatorObjList]
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return []

    def fetch(self, locatorObjList, styleType="rowwise_by_id", filterType="none", dataSelectors=None):
        """ Return a dictionary of loadable data for each table defined in the current schema
            definition object.   Data are extracted from all files in the input file list,
            and this is added in single schema instance such that data from multiple files are appended to a
            one collection of tables.     The organization of the loadable data is controlled
            by the style preference:

            Returns: schemaDataDict, containerNameList

                 For styleType settings:

                     rowwise_by_id:      dict[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                     rowwise_by_name:    dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}

                filterTypes: "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"

        """
        schemaDataDictById, containerNameList, _ = self.__fetch(locatorObjList, filterType, dataSelectors=dataSelectors)
        schemaDataDict = self.__reShape.applyShape(schemaDataDictById, styleType=styleType)
        return schemaDataDict, containerNameList

    def fetchDocuments(self, locatorObjList, styleType="rowwise_by_id", filterType="none", logSize=False, dataSelectors=None, sliceFilter=None):
        """ Return a list of dictionaries of loadable data for each table defined in the current schema
            definition object.   Data are extracted from the each input file, and each data
            set is stored in a separate schema instance (document).  The organization
            of the loadable data is controlled by the style preference:

            Returns: schemaDataDictList, containerNameList

                 For styleType settings:

                     rowwise_by_id:      dict[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                     rowwise_by_name:    dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}

                 filterTypes: "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"

                 sliceFilter: name of slice filter
        """
        schemaDataDictList = []
        containerNameList = []
        rejectLocatorObjList = []
        for locator in locatorObjList:
            schemaDataDictById, cnList, rL = self.__fetch([locator], filterType, dataSelectors=dataSelectors)
            rejectLocatorObjList.extend(rL)
            if not schemaDataDictById:
                continue
            sddL = self.__reShape.applySlicedShape(schemaDataDictById, styleType=styleType, sliceFilter=sliceFilter)
            schemaDataDictList.extend(sddL)
            containerNameList.extend(cnList)
        #
        return schemaDataDictList, containerNameList, rejectLocatorObjList

    def process(self, containerList, styleType="rowwise_by_id", filterType="none", dataSelectors=None):
        """ Return a dictionary of loadable data for each table defined in the current schema
            definition object.   Data are extracted from all files in the input container list,
            and this is added in single schema instance such that data from multiple files are appended to a
            one collection of tables.  The organization of the loadable data is controlled by the style preference:

            Returns: schemaDataDict, containerNameList

                 For styleType settings:

                     rowwise_by_id:      dict[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                     rowwise_by_name:    dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}

                filterTypes: "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"


        """
        schemaDataDictById, containerNameList, _ = self.__process(containerList, filterType, dataSelectors=dataSelectors)
        schemaDataDict = self.__reShape.applyShape(schemaDataDictById, styleType=styleType)

        return schemaDataDict, containerNameList

    def processDocuments(self, containerList, styleType="rowwise_by_id", filterType="none", logSize=False, dataSelectors=None, sliceFilter=None):
        """ Return a list of dictionaries of loadable data for each table defined in the current schema
            definition object.   Data are extracted from the each input container, and each data
            set is stored in a separate schema instance (document).  The organization of the loadable
            data is controlled by the style preference:

            Returns: schemaDataDictList, containerNameList

                 For styleType settings:

                     rowwise_by_id:      dict[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                     rowwise_by_name:    dict[<tableName>] = [ row1Dict[attributeName]=value,  row2dict[], .. ]
                     rowwise_no_name:    dict[<tableName>] = {'attributes': [atName1, atName2,... ], 'data' : [[val1, val2, .. ],[val1, val2,... ]]}
                     columnwise_by_name: dict[<tableName>] = {'atName': [val1, val2,... ], atName2: [val1,val2, ... ], ...}

            filterTypes:  "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"

            sliceFilter: name of slice filter
        """
        schemaDataDictList = []
        containerNameList = []
        rejectPathList = []
        for container in containerList:
            schemaDataDictById, _, rL = self.__process([container], filterType, dataSelectors=dataSelectors)
            rejectPathList.extend(rL)
            if not schemaDataDictById:
                continue
            #
            logger.debug("Reshape container %s using %s" % (container.getName(), sliceFilter))
            sddL = self.__reShape.applySlicedShape(schemaDataDictById, styleType=styleType, sliceFilter=sliceFilter)
            if len(sddL) == 0:
                logger.info("No result on reshaping container %s using %s" % (container.getName(), sliceFilter))
            else:
                schemaDataDictList.extend(sddL)
                #
                # Match the container name to the generated reshaped objects
                cName = container.getName()
                cnList = [cName for i in range(len(sddL))]
                containerNameList.extend(cnList)

        rejectPathList = list(set(rejectPathList))
        #
        return schemaDataDictList, containerNameList, rejectPathList

    def DEPaddDocumentPrivateAttributes(self, docList, collectionName, styleType="rowwise_by_name"):
        """ For the input collection, add private document attributes to the input document list.
        """
        if styleType not in ['rowwise_by_name', 'rowwise_by_name_with_cardinality']:
            logger.error("Unsupported document style %s" % styleType)
            return docList
        try:
            d = {}
            privDocKeyL = self.__sD.getPrivateDocumentAttributes(collectionName)
            if privDocKeyL:
                for pdk in privDocKeyL:
                    catName = pdk['CATEGORY_NAME']
                    atName = pdk['ATTRIBUTE_NAME']
                    pName = pdk['PRIVATE_DOCUMENT_NAME']
                    isMandatory = pdk['MANDATORY']
                    for d in docList:
                        if catName in d and atName in d[catName] and d[catName][atName] and d[catName][atName] not in ['.', '?']:
                            d[pName] = d[catName][atName]
                        else:
                            if isMandatory:
                                logger.info("Skipping private key for %s %s %r %r" % (catName, atName, pdk, list(d.items())[:5]))
        except Exception as e:
            logger.exception("Failing with %s : %r" % (str(e), list(d.items())[:5]))
        #
        return docList

    def addDocumentPrivateAttributes(self, docList, collectionName, styleType="rowwise_by_name"):
        """ For the input collection, add private document attributes to the input document list.
        """
        if styleType not in ['rowwise_by_name', 'rowwise_by_name_with_cardinality']:
            logger.error("Unsupported document style %s" % styleType)
            return docList
        try:
            d = {}
            privDocKeyL = self.__sD.getPrivateDocumentAttributes(collectionName)
            version = self.__sD.getCollectionVersion(collectionName)
            #
            if privDocKeyL:
                for doc in docList:
                    for pdk in privDocKeyL:
                        pName = pdk['PRIVATE_DOCUMENT_NAME']
                        isMandatory = pdk['MANDATORY']
                        if pdk['UPDATE_ON_LOAD']:
                            if pdk['NAME'] == 'rcsb_schema_container_identifiers.collection_schema_version':
                                doc[pName] = version

                        else:
                            catName = pdk['CATEGORY_NAME']
                            atName = pdk['ATTRIBUTE_NAME']
                            #
                            if catName in doc and atName in doc[catName] and doc[catName][atName] and doc[catName][atName] not in ['.', '?']:
                                doc[pName] = doc[catName][atName]
                            else:
                                if isMandatory:
                                    logger.info("Skipping private key for %s %s %r %r" % (catName, atName, pdk, list(d.items())[:5]))
        except Exception as e:
            logger.exception("Failing with %s : %r" % (str(e), list(d.items())[:5]))
        #
        return docList

    def addDocumentSubCategoryAggregates(self, docList, collectionName, styleType="rowwise_by_name", removeSubCategoryPrefix=True):
        """ For the input collection, add subcategory aggregates to the input document list.
        """
        if styleType not in ['rowwise_by_name', 'rowwise_by_name_with_cardinality']:
            logger.error("Unsupported document style %s" % styleType)
            return docList
        try:
            d = {}
            scAgL = self.__sD.getSubCategoryAggregates(collectionName)
            if scAgL:
                scAgD = {}
                logger.debug("%s processing subcategory aggregates %r" % (collectionName, scAgL))
                for scAg in scAgL:
                    scD = {}
                    sIdL = self.__sD.getSubCategorySchemaIdList(scAg)
                    for sId in sIdL:
                        atIdL = self.__sD.getSubCategoryAttributeIdList(sId, scAg)
                        scD[self.__sD.getSchemaName(sId)] = [self.__sD.getAttributeName(sId, atId) for atId in atIdL]
                    scAgD[scAg] = scD
                logger.debug("%s subcategory aggregate name dictionary %r" % (collectionName, scAgD))
                #
                for doc in docList:
                    for scAg, scD in scAgD.items():
                        for sName in scD:
                            if sName not in doc:
                                continue
                            #
                            hasUnitCard = self.__sD.getSubCategoryAggregatesUnitCardinality(collectionName, scAg)
                            logger.debug("%s agg %s unit cardinalioty %r obj %s type %r" % (collectionName, hasUnitCard, scAg, sName, type(doc[sName])))
                            atNameL = scD[sName]
                            if isinstance(doc[sName], list):
                                for rowD in doc[sName]:
                                    if set(atNameL).issubset(set(rowD.keys())):
                                        atLenL = [len(rowD[atName]) for atName in atNameL]
                                        atLen = min(atLenL)
                                        logger.debug("%s %s %r Candidate list row is %r" % (scAg, sName, atNameL, rowD))
                                        # copy all of the data to the new aggregate object
                                        rL = []
                                        for ii in range(atLen):
                                            d = {}
                                            for atName in atNameL:
                                                cAtName = atName.replace(scAg + '_', '') if removeSubCategoryPrefix else atName
                                                d[cAtName] = rowD[atName][ii]
                                            rL.append(d)
                                        rowD[scAg] = rL
                                        for atName in atNameL:
                                            del rowD[atName]
                                        logger.debug("Processed list row is %r" % rowD)
                            elif isinstance(doc[sName], dict):
                                if set(atNameL).issubset(set(doc[sName].keys())):
                                    logger.debug("Candidate dict row is %r" % doc[sName])
                                    atLenL = [len(doc[sName][atName]) for atName in atNameL]
                                    atLen = min(atLenL)
                                    # copy all of the data to the new aggregate object
                                    rL = []
                                    for ii in range(atLen):
                                        d = {}
                                        for atName in atNameL:
                                            cAtName = atName.replace(scAg + '_', '') if removeSubCategoryPrefix else atName
                                            d[cAtName] = doc[sName][atName][ii]
                                        rL.append(d)
                                    doc[sName][scAg] = rL
                                    for atName in atNameL:
                                        del doc[sName][atName]
                                    logger.debug("Processed dict row is %r" % doc[sName])
                            else:
                                logger.error("%s unanticipated document data type for sName %s" % (collectionName, sName))

        except Exception as e:
            logger.exception("Failing with %s : %r" % (str(e), list(d.items())[:5]))
        #
        return docList

    def __fetch(self, locatorObjList, filterType, dataSelectors=None):
        """ Internal method to create loadable data corresponding to the table schema definition
            from the input list of data files.

            Returns: dicitonary d[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                                and
                     container name list. []

        """
        startTime = time.time()
        #
        rejectLocatorObjList = []
        containerNameList = []
        schemaDataDict = {}
        for locatorObj in locatorObjList:
            # myContainerList = self.__mU.doImport(locatorObj, format="mmcif")
            myContainerList = self.__mergeContainers(locatorObj, format="mmcif", mergeTarget=0)
            cL = []
            for c in myContainerList:
                if self.__testdataSelectors(c, dataSelectors):
                    cL.append(c)
                    # self.__loadInfo[c.getName()] = {'load_date': self.__getTimeStamp(), 'locator': locatorObj}
                else:
                    rejectLocatorObjList.append(locatorObj)
            self.__mapData(cL, schemaDataDict, filterType)
            containerNameList.extend([myC.getName() for myC in myContainerList])
        #
        schemaDataDictF = {}
        if 'drop-empty-tables' in filterType:
            for k, v in schemaDataDict.items():
                if len(v) > 0:
                    schemaDataDictF[k] = v
        else:
            schemaDataDictF = schemaDataDict
        #
        rejectLocatorObjList = list(set(rejectLocatorObjList))
        #
        #
        #
        endTime = time.time()
        logger.debug("completed at %s (%.3f seconds)" %
                     (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

        return schemaDataDictF, containerNameList, rejectLocatorObjList

    def __process(self, containerList, filterType, dataSelectors=None):
        """ Internal method to create loadable data corresponding to the table schema definition
            from the input container list.

            Returns: dicitonary d[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                                and
                     processed container name list. []
                     list of paths or names of rejected containers. []

        """
        startTime = time.time()
        #
        rejectPathList = []
        containerNameList = []
        schemaDataDict = {}
        cL = []
        for c in containerList:
            if self.__testdataSelectors(c, dataSelectors):
                cL.append(c)
            else:
                try:
                    # rejectList.append(self.__loadInfo[c.getName()]['locator'])
                    rejectPathList.append(c.getProp['locator'])
                except Exception:
                    rejectPathList.append(c.getName())
        #
        self.__mapData(cL, schemaDataDict, filterType)
        containerNameList.extend([myC.getName() for myC in containerList])
        #
        #
        schemaDataDictF = {}
        if 'drop-empty-tables' in filterType:
            for k, v in schemaDataDict.items():
                if len(v) > 0:
                    schemaDataDictF[k] = v
        else:
            schemaDataDictF = schemaDataDict
        #
        #
        endTime = time.time()
        logger.debug("completed at %s (%.3f seconds)\n" %
                     (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

        return schemaDataDictF, containerNameList, rejectPathList

    def __testdataSelectors(self, container, dataSelectors):
        """ Test the if the input container satisfies the input content/data selectors.

            Selection content must exist in the input container with the specified value.

            Return:  True fo sucess or False otherwise
        """
        logger.debug("Applying selectors: %r" % dataSelectors)
        if not dataSelectors:
            return True
        try:
            for cs in dataSelectors:
                csDL = self.__sD.getDataSelectors(cs)
                for csD in csDL:
                    tn = csD['CATEGORY_NAME']
                    an = csD['ATTRIBUTE_NAME']
                    vals = csD['VALUES']
                    logger.debug("Applying selector %s: tn %s an %s vals %r" % (cs, tn, an, vals))
                    if not container.exists(tn):
                        return False
                    catObj = container.getObj(tn)
                    numRows = catObj.getRowCount()
                    if numRows:
                        for ii in range(numRows):
                            logger.debug("Testing %s type %r row %d of %d" % (an, type(an), ii, numRows))
                            v = catObj.getValue(attributeName=an, rowIndex=ii)
                            if v not in vals:
                                logger.debug("Selector %s rejects : tn %s an %s value %r" % (cs, tn, an, v))
                                return False
                    else:
                        logger.debug("Selector %s rejects container with missing category %s" % (cs, tn))
                        return False
            #
            # all selectors satisfied
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def __showOverwrite(self):
        #
        if (self.__verbose):
            if len(self.__overWrite) > 0:
                for k, v in self.__overWrite.items():
                    logger.debug("+SchemaDefLoader(load) %r maximum width %r" % (k, v))

    def __evalMapFunction(self, dataContainer, rowDList, attributeId, functionName, functionArgs=None):
        """ Evaluate dynamic schema methods on the input data container.

        """
        # logger.debug("Evaluating function %s on attribute %s" % (functionName, attributeId))
        fn = functionName.lower()
        cName = dataContainer.getName()
        if (fn in "datablockid()"):
            val = cName
            for rowD in rowDList:
                rowD[attributeId] = val
        elif (fn == "getdatetime()"):
            # val = self.__loadInfo[cName]['load_date'] if cName in self.__loadInfo else self.__getTimeStamp()
            v = dataContainer.getProp('load_date')
            val = v if v else self.__getTimeStamp()
            for rowD in rowDList:
                rowD[attributeId] = val
        elif (fn == "getlocator()"):
            # val = self.__loadInfo[cName]['locator'] if cName in self.__loadInfo else 'unknown'
            v = dataContainer.getProp('locator')
            val = v if v else 'unknown'
            for rowD in rowDList:
                rowD[attributeId] = val
        elif (fn == "rowindex()"):
            for ii, rowD in enumerate(rowDList, 1):
                rowD[attributeId] = ii
        else:
            logger.error("Unsupported dynamic method %s for attribute %s" % (functionName, attributeId))
            return False

        return True

    def __mapData(self, containerList, schemaDataDict, filterType="none"):
        """
           Process instance data in the input container list and map these data to the
           table schema definitions to the current selected table list.

           Returns: mapped data as a list of dictionaries with attribute Id key for
                    each schema table.  Data are appended to any existing table in
                    the input dictionary.


        """
        # Respect any input selection otherwise use all schema defined tables -
        if self.__schemaIdIncludeD:
            selectedTableIdList = list(self.__schemaIdIncludeD.keys())
        else:
            selectedTableIdList = self.__sD.getSchemaIdList()
        #
        for myContainer in containerList:
            for tableId in selectedTableIdList:
                if not self.__sD.hasSchemaObject(tableId):
                    # logger.debug("Skipping undefined table %s" % tableId)
                    continue
                if tableId in self.__schemaIdExcludeD:
                    # logger.debug("Skipping excluded table %s" % tableId)
                    continue
                if tableId not in schemaDataDict:
                    schemaDataDict[tableId] = []
                tObj = self.__sD.getSchemaObject(tableId)
                #
                # Instance categories that are mapped to the current table -
                #
                mapCategoryNameList = tObj.getMapInstanceCategoryList()
                numMapCategories = len(mapCategoryNameList)
                #
                # Attribute Ids that are not directly mapped to the schema (e.g. functions)
                #
                otherAttributeIdList = tObj.getMapOtherAttributeIdList()
                #

                if numMapCategories == 1:
                    rowDList = self.__mapInstanceCategory(tObj, mapCategoryNameList[0], myContainer, filterType)
                elif numMapCategories == 0:
                    # For a purely synthetic category with only method mappings,  create a placeholder row dictionary.
                    rowDList = [{k: None for k in otherAttributeIdList}]
                elif numMapCategories >= 1:
                    rowDList = self.__mapInstanceCategoryList(tObj, mapCategoryNameList, myContainer, filterType)

                for atId in otherAttributeIdList:
                    fName = tObj.getMapAttributeFunction(atId)
                    fArgs = tObj.getMapAttributeFunctionArgs(atId)
                    self.__evalMapFunction(dataContainer=myContainer, rowDList=rowDList, attributeId=atId, functionName=fName, functionArgs=fArgs)

                schemaDataDict[tableId].extend(rowDList)

        return schemaDataDict

    def __mapInstanceCategory(self, tObj, categoryName, myContainer, filterType):
        """ Extract data from the input instance category and map these data to the organization
            in the input table schema definition object.

            No merging is performed by this method.

            Return a list of dictionaries with schema attribute Id keys containing data
            mapped from the input instance category.
        """
        #
        retList = []
        catObj = myContainer.getObj(categoryName)
        if catObj is None:
            return retList
        attributeNameList = catObj.getAttributeList()
        #
        for row in catObj.getRowList():
            d = self.__dtObj.processRecord(tObj.getId(), row, attributeNameList, containerName=myContainer.getName())
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
        # Consider mD as orderdict
        mD = {}
        for categoryName in categoryNameList:
            catObj = myContainer.getObj(categoryName)
            if catObj is None:
                continue
            attributeNameList = catObj.getAttributeList()
            attributeIndexDict = catObj.getAttributeIndexDict()
            #
            # dictionary of merging indices for each attribute in this category -
            #
            indL = tObj.getMapMergeIndexAttributes(categoryName)

            for row in catObj.getRowList():
                # assign merge index
                mK = []
                for atName in indL:
                    try:
                        mK.append(row[attributeIndexDict[atName]])
                    except Exception as e:
                        # would reflect a serious issue of missing key-
                        if (self.__debug):
                            logger.exception("Failing with %s" % str(e))
                #

                d = self.__dtObj.processRecord(tObj.getId(), row, attributeNameList, containerName=myContainer.getName())

                #
                # Update this row using exact matching of the merging key --
                # jdw  - will later add more complex comparisons
                #
                tk = tuple(mK)
                if tk not in mD:
                    mD[tk] = {}

                mD[tk].update(d)

        return mD.values()


if __name__ == '__main__':
    pass
