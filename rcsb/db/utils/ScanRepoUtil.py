##
# File:    ScanRepoUtil.py
# Author:  J. Westbrook
# Date:    30-Apr-2018
# Version: 0.001
#
# Updates:
#  9-May-2018 jdw implement incremental update of scanned data
# 18-May-2018 jdw move IO to IoUtils.py
# 23-May-2018 jdw remove proxy to ContentTypeUtil(), go directly to RepoPathUtil() for paths
#  5-Jun-2018 jdw update prototypes for IoUtil() methods
# 16-Jun-2018 jdw update data type prototype.
# 18-Jun-2018 jdw move mocking to configuration level
# 28-Jun-2018 jdw remove IoUtil() and add working path constructor argument
##
"""
Tools for for scanning repositories and collecting coverage and type data information.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import collections
import datetime
import logging
import time

from mmcif.api.DataCategory import DataCategory

from rcsb.db.utils.RepoPathUtil import RepoPathUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

logger = logging.getLogger(__name__)

ScanValue = collections.namedtuple('ScanValue', 'containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec')
ScanSummary = collections.namedtuple('ScanSummary', 'containerId, fromPath, scanDate, scanCategoryDict')


class ScanRepoUtil(object):
    """Tools for for scanning repositories and collecting coverage and type data information.

    """

    def __init__(self, cfgOb, attributeDataTypeD=None, numProc=4, chunkSize=15, fileLimit=None, maxStepLength=2000, workPath=None):
        """
        Args:
            cfgOb (object): Configuration object (ConfigUtil)

            attributeDataTypeD
            dictPath (str): Path to supporting data dictionary

            numProc (int, optional): Number of parallel worker processes used.
            chunkSize (int, optional): Size of files processed in a single multi-proc process
            fileLimit (int, optional): maximum file scanned or None for no limit
            mockTopPath (str, optional): Path to directory containing mock repositories or None
            maxStepLength (int, optional): maximum number of multi-proc runs to perform
        """
        #
        self.__attributeDataTypeD = attributeDataTypeD if attributeDataTypeD else {}
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__fileLimit = fileLimit
        self.__maxStepLength = maxStepLength
        #
        # Controls for multiprocessing execution -
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        #
        self.__cfgOb = cfgOb
        #
        self.__mpFormat = '[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s'

        self.__workPath = workPath
        self.__mU = MarshalUtil(workPath=self.__workPath)

    def scanContentType(self, contentType, scanType='full', inputPathList=None, scanDataFilePath=None, failedFilePath=None, saveInputFileListPath=None):
        """Driver method for scan operation

        Args:
            contentType (str):  one of 'bird','bird_family','bird_chem_comp', chem_comp','pdbx'
            scanType (str, optional): 'full' [or 'incr' to be supported]
            inputPathList (list, optional):  list of input file paths to scan
            scanDataFilePath (str, optional): file path for serialized scan data (Pickle format)
            failedFilePath (str, optional): file path for list of files that fail scanning operation
            saveInputFileListPath str, optional): Path to store file path list that is scanned

        Returns:
            bool: True for success or False otherwise

        """
        try:
            startTime = self.__begin(message="scanning operation")
            #
            rpU = RepoPathUtil(self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit)
            pathList = rpU.getLocatorList(contentType=contentType, inputPathList=inputPathList)
            #
            if saveInputFileListPath:
                self.__mU.doExport(saveInputFileListPath, pathList, format="list")
                logger.debug("Saving %d paths in %s" % (len(pathList), saveInputFileListPath))
            #

            optD = {}
            optD['contentType'] = contentType
            optD['logSize'] = True
            optD['scanType'] = scanType
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            #

            numProc = self.__numProc
            chunkSize = self.__chunkSize if inputPathList and self.__chunkSize < len(inputPathList) else 0
            #
            # ---------------- - ---------------- - ---------------- - ---------------- - ---------------- -
            numPaths = len(pathList)
            logger.debug("Processing %d total paths" % numPaths)
            numProc = min(numProc, numPaths)
            maxStepLength = self.__maxStepLength
            if numPaths > maxStepLength:
                numLists = int(numPaths / maxStepLength)
                subLists = [pathList[i::numLists] for i in range(numLists)]
            else:
                subLists = [pathList]
            #
            if subLists and len(subLists) > 0:
                logger.debug("Starting with numProc %d outer subtask count %d subtask length ~ %d" % (numProc, len(subLists), len(subLists[0])))
            #
            numResults = 1
            failList = []
            retLists = [[] for ii in range(numResults)]
            diagList = []
            for ii, subList in enumerate(subLists):
                logger.debug("Running outer subtask %d or %d length %d" % (ii + 1, len(subLists), len(subList)))
                #
                mpu = MultiProcUtil(verbose=True)
                mpu.setOptions(optionsD=optD)
                mpu.set(workerObj=self, workerMethod="scanWorker")
                ok, failListT, retListsT, diagListT = mpu.runMulti(dataList=subList, numProc=numProc, numResults=numResults, chunkSize=chunkSize)
                failList.extend(failListT)
                # retLists is a list of lists -
                for ii in range(numResults):
                    retLists[ii].extend(retListsT[ii])
                diagList.extend(diagListT)
            logger.debug("Scan failed path list %r" % failList)
            logger.debug("Scan path list success length %d load list failed length %d" % (len(pathList), len(failList)))
            logger.debug("Returned metadata length %r" % len(retLists[0]))
            #
            if failedFilePath and len(failList):
                wOk = self.__mU.doExport(failedFilePath, failList, format="list")
                logger.debug("Writing scan failure path list to %s status %r" % (failedFilePath, wOk))
            #
            if scanType == 'incr':
                scanDataD = self.__mU.doImport(scanDataFilePath, format='pickle', default=None)
                logger.info("Imported scan data with keys %r" % list(scanDataD.keys()))
            else:
                scanDataD = {}
            #
            if scanDataFilePath and len(retLists[0]):
                for ssTup in retLists[0]:
                    cId = ssTup.containerId
                    if scanType == 'full' and cId in scanDataD:
                        logger.error("Duplicate container id %s in %r and %r" % (cId, ssTup.fromPath, scanDataD[cId].fromPath))
                    #
                    scanDataD[cId] = ssTup

                ok = self.__mU.doExport(scanDataFilePath, scanDataD, format='pickle')
                tscanDataD = self.__mU.doImport(scanDataFilePath, format='pickle')
                ok = tscanDataD == scanDataD

            self.__end(startTime, "scanning operation with status " + str(ok))

            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def evalScan(self, scanDataFilePath, evalJsonFilePath, evalType='data_type'):

        scanDataD = self.__mU.doImport(scanDataFilePath, format='pickle')
        if evalType in ['data_type']:
            rD = self.__evalScanDataType(scanDataD)
        elif evalType in ['data_coverage']:
            rD = self.__evalScanDataCoverage(scanDataD)
        else:
            logger.debug("Unknown evalType %r " % evalType)
        ok = self.__mU.doExport(evalJsonFilePath, rD, format='json')

        return ok

    def __evalScanDataType(self, scanDataD):
        """
        ScanValue = collections.namedtuple('ScanValue', 'containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec')
        ScanSummary = collections.namedtuple('ScanSummary', 'containerId, fromPath, scanDate, scanCategoryDict')

        """
        # for populated sD[category] -> d[atName]->{minWidth: , maxWidth:, minPrec:, maxPrec: , count}
        sD = {}
        for cId in scanDataD:
            ssTup = scanDataD[cId]
            d = ssTup.scanCategoryDict
            for catName in d:
                if catName not in sD:
                    sD[catName] = {}
                for svTup in d[catName]:
                    if svTup.atName not in sD[catName]:
                        sD[catName][svTup.atName] = {'minWidth': svTup.minWidth, 'maxWidth': svTup.maxWidth, 'minPrec': svTup.minPrec, 'maxPrec': svTup.maxPrec, 'count': 1}
                        continue
                    sD[catName][svTup.atName]['minWidth'] = min(sD[catName][svTup.atName]['minWidth'], svTup.minWidth)
                    sD[catName][svTup.atName]['maxWidth'] = max(sD[catName][svTup.atName]['maxWidth'], svTup.maxWidth)
                    sD[catName][svTup.atName]['minPrec'] = min(sD[catName][svTup.atName]['minPrec'], svTup.minPrec)
                    sD[catName][svTup.atName]['maxPrec'] = max(sD[catName][svTup.atName]['maxPrec'], svTup.maxPrec)
                    sD[catName][svTup.atName]['count'] += 1
        return sD

    def __evalScanDataCoverage(self, scanDataD):
        """
        ScanValue = collections.namedtuple('ScanValue', 'containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec')
        ScanSummary = collections.namedtuple('ScanSummary', 'containerId, fromPath, scanDate, scanCategoryDict')

        """

        # for populated sD[category] -> d[atName]->{count: #, instances: [id,id,id]}
        sD = {}
        for cId in scanDataD:
            ssTup = scanDataD[cId]
            d = ssTup.scanCategoryDict
            for catName in d:
                if catName not in sD:
                    sD[catName] = {}
                for svTup in d[catName]:
                    if svTup.atName not in sD[catName]:
                        sD[catName][svTup.atName] = {'count': 0, 'instances': []}
                    sD[catName][svTup.atName]['instances'].append(svTup.containerId)
                    sD[catName][svTup.atName]['count'] += 1
        return sD

    def scanWorker(self, dataList, procName, optionsD, workingDir):
        """ Multi-proc worker method for scanning repository data files-


        """
        try:
            startTime = self.__begin(message=procName)
            # Recover common options

            scanType = optionsD['scanType']
            contentType = optionsD['contentType']
            #
            successList = []
            retList = []

            containerList = self.__getContainerList(dataList)
            for container in containerList:
                ret = self.__scanContainer(container)
                successList.append(ret.fromPath)
                retList.append(ret)
            #

            logger.debug("%s scanType %s contentType %spathlist length %d containerList length %d" % (procName, scanType, contentType, len(dataList), len(containerList)))

            ok = len(successList) == len(dataList)
            #
            self.__end(startTime, procName + " with status " + str(ok))
            return successList, retList, []

        except Exception as e:
            logger.error("Failing with dataList %r" % dataList)
            logger.exception("Failing with %s" % str(e))

        return [], [], []

    def __getContainerList(self, inputPathList):
        """
        """
        utcnow = datetime.datetime.utcnow()
        ts = utcnow.strftime("%Y-%m-%d:%H:%M:%S")

        cL = []
        for lPath in inputPathList:
            myContainerList = self.__mU.doImport(lPath, format="mmcif")

            for c in myContainerList:
                dc = DataCategory('rcsb_load_status', ['name', 'load_date', 'locator'], [[c.getName(), ts, lPath]])
                logger.debug("data category %r" % dc)
                c.append(dc)
                cL.append(c)
        return cL

    def __scanContainer(self, container):
        """ Scan the input container for

          Get the file name -
        """
        cName = container.getName()
        loadStatusObj = container.getObj('rcsb_load_status')
        lName = loadStatusObj.getValue(attributeName='name', rowIndex=0)
        lFilePath = loadStatusObj.getValue(attributeName='locator', rowIndex=0)
        lDate = loadStatusObj.getValue(attributeName='load_date', rowIndex=0)
        #
        oD = {}
        for objName in container.getObjNameList():
            if objName == 'rcsb_load_status':
                continue
            obj = container.getObj(objName)
            afD = self.__attributeDataTypeD[objName] if objName in self.__attributeDataTypeD else {}
            atNameList = obj.getAttributeList()
            wMin = {atName: 100000 for atName in atNameList}
            wMax = {atName: -1 for atName in atNameList}
            pMin = {atName: 100000 for atName in atNameList}
            pMax = {atName: -1 for atName in atNameList}
            for row in obj.getRowList():
                for ii, val in enumerate(row):
                    valLen = len(val)
                    if ((valLen == 0) or (val == '?') or (val == '.')):
                        continue
                    atName = atNameList[ii]
                    wMin[atName] = min(wMin[atName], valLen)
                    wMax[atName] = max(wMax[atName], valLen)
                    if atName in afD and afD[atName] == 'float':
                        vPrec = 0
                        try:
                            fields = val.split('.')
                            vPrec = len(fields[1])
                            pMin[atName] = min(pMin[atName], vPrec)
                            pMax[atName] = max(pMax[atName], vPrec)
                        except Exception as e:
                            logger.debug("Failed to process float %s %r %r %s" % (atName, val, vPrec, str(e)))
                            pMin[atName] = 0
                            pMax[atName] = 0
                        logger.debug("Got float for %s %r %r" % (atName, val, vPrec))
                    else:
                        pMin[atName] = 0
                        pMax[atName] = 0

            # ScanValue - containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec
            oD[objName] = [ScanValue(cName, objName, atN, wMin[atN], wMax[atN], pMin[atN], pMax[atN]) for atN in wMax if wMax[atN] != -1]
        # ScanSummary containerId, fromPath, scanCategoryDict
        #
        ret = ScanSummary(lName, lFilePath, lDate, oD)
        #
        return ret

    def __begin(self, message=""):
        startTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting %s at %s" % (message, ts))
        return startTime

    def __end(self, startTime, message=""):
        endTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        delta = endTime - startTime
        logger.debug("Completed %s at %s (%.4f seconds)" % (message, ts, delta))
