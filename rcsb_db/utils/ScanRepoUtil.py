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
##
"""
Tools for for scanning repositories and collecting coverage and type data information.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import time
import datetime


import collections
import logging
logger = logging.getLogger(__name__)

ScanValue = collections.namedtuple('ScanValue', 'containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec')
ScanSummary = collections.namedtuple('ScanSummary', 'containerId, fromPath, scanDate, scanCategoryDict')

from rcsb_db.utils.MultiProcUtil import MultiProcUtil
from rcsb_db.utils.IoUtil import IoUtil
from rcsb_db.utils.RepoPathUtil import RepoPathUtil
from mmcif.api.DataCategory import DataCategory


try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
#


class ScanRepoUtil(object):
    """Tools for for scanning repositories and collecting coverage and type data information.

    """

    def __init__(self, cfgOb, dataTypeD=None, numProc=4, chunkSize=15, fileLimit=None, mockTopPath=None, maxStepLength=2000):
        """
        Args:
            cfgOb (object): Configuration object (ConfigUtil)
            dictPath (str): Path to supporting data dictionary
            numProc (int, optional): Number of parallel worker processes used.
            chunkSize (int, optional): Size of files processed in a single multi-proc process
            fileLimit (int, optional): maximum file scanned or None for no limit
            mockTopPath (str, optional): Path to directory containing mock repositories or None
            maxStepLength (int, optional): maximum number of multi-proc runs to perform
            convertNameFunc (func, optional): alternative method to standardize/simplify dictionary item names
        """
        #
        self.__dataTypeD = dataTypeD if dataTypeD else {}
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
        self.__mockTopPath = mockTopPath
        self.__mpFormat = '[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s'

        self.__ioObj = IoAdapter()
        self.__ioU = IoUtil()

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
            rpU = RepoPathUtil(self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
            pathList = rpU.getRepoPathList(contentType=contentType, inputPathList=inputPathList)
            #

            if saveInputFileListPath:
                self.__writePathList(saveInputFileListPath, pathList)
                logger.info("Saving %d paths in %s" % (len(pathList), saveInputFileListPath))
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
            logger.info("Processing %d total paths" % numPaths)
            numProc = min(numProc, numPaths)
            maxStepLength = self.__maxStepLength
            if numPaths > maxStepLength:
                numLists = int(numPaths / maxStepLength)
                subLists = [pathList[i::numLists] for i in range(numLists)]
            else:
                subLists = [pathList]
            #
            if subLists and len(subLists) > 0:
                logger.info("Starting with numProc %d outer subtask count %d subtask length ~ %d" % (numProc, len(subLists), len(subLists[0])))
            #
            numResults = 1
            failList = []
            retLists = [[] for ii in range(numResults)]
            diagList = []
            for ii, subList in enumerate(subLists):
                logger.info("Running outer subtask %d or %d length %d" % (ii + 1, len(subLists), len(subList)))
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
            logger.info("Scan path list success length %d load list failed length %d" % (len(pathList), len(failList)))
            logger.info("Returned metadata length %r" % len(retLists[0]))
            #
            if failedFilePath and len(failList):
                wOk = self.__writePathList(failedFilePath, failList)
                logger.info("Writing scan failure path list to %s status %r" % (failedFilePath, wOk))
            #
            if scanType == 'incr':
                scanDataD = self.__ioU.deserialize(scanDataFilePath, default=None)
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

                ok = self.__ioU.serialize(scanDataFilePath, dObj=scanDataD, pickleProtocol=0)
                tscanDataD = self.__ioU.deserialize(scanDataFilePath)
                ok = tscanDataD == scanDataD

            self.__end(startTime, "scanning operation with status " + str(ok))

            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def evalScan(self, scanDataFilePath, evalJsonFilePath, evalType='data_type'):

        scanDataD = self.__ioU.deserialize(scanDataFilePath)
        if evalType in ['data_type']:
            rD = self.__evalScanDataType(scanDataD)
        elif evalType in ['data_coverage']:
            rD = self.__evalScanDataCoverage(scanDataD)
        else:
            logger.info("Unknown evalType %r " % evalType)
        ok = self.__ioU.serializeJson(evalJsonFilePath, rD)

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
            myContainerList = self.__ioObj.readFile(lPath)
            for c in myContainerList:
                dc = DataCategory('__load_status__', ['name', 'load_date', 'load_file_path'], [[c.getName(), ts, lPath]])
                logger.debug("data category %r" % dc)
                c.append(dc)
                cL.append(c)
        return cL

    def __scanContainer(self, container):
        """ Scan the input container for

          Get the file name -
        """
        cName = container.getName()
        loadStatusObj = container.getObj('__load_status__')
        lName = loadStatusObj.getValue(attributeName='name', rowIndex=0)
        lFilePath = loadStatusObj.getValue(attributeName='load_file_path', rowIndex=0)
        lDate = loadStatusObj.getValue(attributeName='load_date', rowIndex=0)
        #
        oD = {}
        for objName in container.getObjNameList():
            if objName == '__load_status__':
                continue
            obj = container.getObj(objName)
            afD = self.__dataTypeD[objName] if objName in self.__dataTypeD else {}
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
        logger.info("Starting %s at %s" % (message, ts))
        return startTime

    def __end(self, startTime, message=""):
        endTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        delta = endTime - startTime
        logger.info("Completed %s at %s (%.4f seconds)" % (message, ts, delta))
