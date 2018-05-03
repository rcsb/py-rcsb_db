##
# File:    ScanRepoUtil.py
# Author:  J. Westbrook
# Date:    30-Apr-2018
# Version: 0.001
#
# Updates:

##
"""
Tools for for scanning repositories and collecting coverage and type data for
schema construction.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import time
import re
import datetime

import json
import pickle
import pprint

import collections
import logging
logger = logging.getLogger(__name__)

ScanValue = collections.namedtuple('ScanValue', 'containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec')
ScanSummary = collections.namedtuple('ScanSummary', 'containerId, fromPath, scanDate, scanDictcatName')

from rcsb_db.schema.SchemaDefBuild import SchemaDictInfo
from rcsb_db.utils.ContentTypeUtil import ContentTypeUtil
from rcsb_db.utils.MultiProcUtil import MultiProcUtil
from mmcif.api.DataCategory import DataCategory

try:
    from mmcif.io.IoAdapterCore import IoAdapterCore as IoAdapter
except Exception as e:
    from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
#


class ScanRepoUtil(object):
    """Tools for for scanning repositories and collecting coverage and type data for
       schema construction.

    """

    def __init__(self, cfgOb, dictPath, cardinalityKeyItem='_entry.id', numProc=4, chunkSize=15, fileLimit=None, mockTopPath=None, maxStepLength=2000, convertNameFunc=None):
        #
        self.__sdi = SchemaDictInfo(dictPath=dictPath, cardinalityKeyItem=cardinalityKeyItem, iTypeCodes=('ucode-alphanum-csv', 'id_list'), iQueryStrings=['comma separate'])
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
        self.__convertName = convertNameFunc if convertNameFunc else self.__convertNameDefault
        self.__re0 = re.compile('(database|cell|order|partition|group)$', flags=re.IGNORECASE)
        self.__re1 = re.compile('[-/%[]')
        self.__re2 = re.compile('[\]]')
        #
        #
        self.__mockTopPath = mockTopPath
        self.__mpFormat = '[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s'
        #
        self.__ctU = ContentTypeUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, mockTopPath=self.__mockTopPath)
        #
        self.__ioObj = IoAdapter()

    def scanContentType(self, contentType, scanType='full', inputPathList=None, outputFilePath=None, failedFilePath=None, saveInputFileListPath=None):
        """  Driver method for loading MongoDb content -

            contentType:  one of 'bird','bird_family','bird_chem_comp', chem_comp','pdbx'
            #
            scanType:     "full" or "replace"

        """
        try:
            startTime = self.__begin(message="scanning operation")
            #
            pathList = self.__ctU.getPathList(contentType=contentType, inputPathList=inputPathList)
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
            failList = []
            retLists = []
            diagList = []
            for ii, subList in enumerate(subLists):
                logger.info("Running outer subtask %d or %d length %d" % (ii + 1, len(subLists), len(subList)))
                #
                mpu = MultiProcUtil(verbose=True)
                mpu.setOptions(optionsD=optD)
                mpu.set(workerObj=self, workerMethod="scanWorker")
                ok, failListT, retListsT, diagListT = mpu.runMulti(dataList=subList, numProc=numProc, numResults=1, chunkSize=chunkSize)
                failList.extend(failListT)
                retLists.extend(retListsT)
                diagList.extend(diagListT)
            logger.debug("Scan failed path list %r" % failList)
            logger.info("Scan path list length %d failed load list length %d" % (len(pathList), len(failList)))
            logger.info("Returned metadata length %r" % len(retLists[0]))
            #
            if failedFilePath and len(failList):
                wOk = self.__writePathList(failedFilePath, failList)
                logger.info("Writing scan failure path list to %s status %r" % (failedFilePath, wOk))
            #
            if outputFilePath and len(retLists[0]):
                ok = self.__serialize(outputFilePath, dObj=retLists[0], pickleProtocol=0)
                tLists = self.__deserialize(outputFilePath)
                ok = tLists == retLists[0]

            self.__end(startTime, "scanning operation with status " + str(ok))

            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def evalScan(self, scanDataFilePath, evalJsonFilePath, evalType='data_type'):

        ssTupL = self.__deserialize(scanDataFilePath)
        if evalType in ['data_type']:
            rD = self.__evalScanDataType(ssTupL)
        elif evalType in ['data_coverage']:
            rD = self.__evalScanDataCoverage(ssTupL)
        else:
            logger.info("Unknown evalType %r " % evalType)
        ok = self.__saveData(evalJsonFilePath, rD)

        return ok

    def __evalScanDataType(self, ssTupL):
        """
        ScanValue = collections.namedtuple('ScanValue', 'containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec')
        ScanSummary = collections.namedtuple('ScanSummary', 'containerId, fromPath, scanDate, scanDictcatName')

        """
        # for populated sD[category] -> d[atName]->{minWidth: , maxWidth:, minPrec:, maxPrec: , count}
        sD = {}
        for ssTup in ssTupL:
            d = ssTup.scanDictcatName
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

    def __evalScanDataCoverage(self, ssTupL):
        """
        ScanValue = collections.namedtuple('ScanValue', 'containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec')
        ScanSummary = collections.namedtuple('ScanSummary', 'containerId, fromPath, scanDate, scanDictcatName')

        """

        # for populated sD[category] -> d[atName]->{count: #, instances: [id,id,id]}
        sD = {}
        for ssTup in ssTupL:
            d = ssTup.scanDictcatName
            for catName in d:
                if catName not in sD:
                    sD[catName] = {}
                for svTup in d[catName]:
                    if svTup.atName not in sD[catName]:
                        sD[catName][svTup.atName] = {'count': 0, 'instances': []}
                    sD[catName][svTup.atName]['instances'].append(svTup.containerId)
                    sD[catName][svTup.atName]['count'] += 1
        return sD

    def __saveData(self, savePath, sdObj, format="json"):
        """Persist the schema map  data structure -
        """
        try:
            if format == "json":
                sOut = json.dumps(sdObj, sort_keys=True, indent=3)
            else:
                sOut = pprint.pformat(sdObj, indent=1, width=120)
            with open(savePath, 'w') as ofh:
                ofh.write("\n%s\n" % sOut)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __serialize(self, filePath='scan_data.pic', dObj=None, pickleProtocol=pickle.HIGHEST_PROTOCOL):
        try:
            with open(filePath, 'wb') as ofh:
                pickle.dump(dObj, ofh, pickleProtocol)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __deserialize(self, filePath='scan_data.pic', default=None):
        try:
            with open(filePath, 'rb') as ifh:
                dObj = pickle.load(ifh)
            return dObj
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return default

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
            afD = self.__sdi.getAttributeFeatures(objName)
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
                    if atName in afD and afD[atName]['TYPE_CODE'] == 'float':
                        vPrec = 0
                        try:
                            fields = val.split('.')
                            vPrec = len(fields[1])
                            pMin[atName] = min(pMin[atName], vPrec)
                            pMax[atName] = max(pMax[atName], vPrec)
                        except Exception as e:
                            pMin[atName] = 0
                            pMax[atName] = 0
                        logger.debug("Got float for %s %r %r" % (atName, val, vPrec))
                    else:
                        pMin[atName] = 0
                        pMax[atName] = 0

            # ScanValue - containerId, catName, atName, minWidth, maxWidth, minPrec, maxPrec
            oD[objName] = [ScanValue(cName, objName, atN, wMin[atN], wMax[atN], pMin[atN], pMax[atN]) for atN in wMax if wMax[atN] != -1]
        # ScanSummary containerId, fromPath, scanDictcatName
        #
        ret = ScanSummary(lName, lFilePath, lDate, oD)
        #
        return ret

    def __convertNameDefault(self, name):
        """ Default schema name converter -
        """
        if self.__re0.match(name):
            name = 'the_' + name
        return self.__re1.sub('_', self.__re2.sub('', name))
    #
    # -------------- -------------- -------------- -------------- -------------- -------------- --------------
    #                                        ---  Supporting code follows ---
    #

    def __writePathList(self, filePath, pathList):
        try:
            with open(filePath, 'w') as ofh:
                for pth in pathList:
                    ofh.write("%s\n" % pth)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

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
