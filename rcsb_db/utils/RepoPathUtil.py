##
# File:    RepoPathUtil.py
# Author:  J. Westbrook
# Date:    21-Mar-2018
# Version: 0.001
#
# Updates:
#   22-Mar-2018  jdw add support for all repositories -
#   26-Mar-2018  jdw internalize the use of externally provided configuration object -
#   27-Mar-2018  jdw add path to support mock repositories for testing.
##
"""
 Utilites for scanning common data repository file systems.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import os
import time
try:
    import os.scandir as scandir
except Exception as e:
    import scandir

import logging
logger = logging.getLogger(__name__)

from rcsb_db.utils.MultiProcUtil import MultiProcUtil
from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo
from mmcif_utils.bird.PdbxPrdCcIo import PdbxPrdCcIo
from mmcif_utils.bird.PdbxFamilyIo import PdbxFamilyIo
# from mmcif_utils.chemcomp.PdbxChemCompIo import PdbxChemCompIo


class RepoPathUtil(object):

    def __init__(self, cfgOb, numProc=8, fileLimit=None, mockTopPath=None, verbose=False):
        self.__fileLimit = fileLimit
        self.__numProc = numProc
        self.__verbose = verbose
        self.__cfgOb = cfgOb
        self.__mockTopPath = mockTopPath

    def __cfgWrapper(self, ky):
        return os.path.join(self.__mockTopPath, self.__cfgOb.get(ky)) if self.__mockTopPath else self.__cfgOb.get(ky)

    def _chemCompPathWorker(self, dataList, procName, optionsD, workingDir):
        """ Return the list of chemical component definition file paths in the current repository.
        """
        topRepoPath = optionsD['topRepoPath']
        pathList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, dirs, files in scandir.walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) <= 7:
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def getChemCompPathList(self):
        return self.__getChemCompPathList(self.__cfgWrapper('CHEM_COMP_REPO_PATH'), numProc=self.__numProc)

    def __getChemCompPathList(self, topRepoPath, numProc=8):
        """Get the path list for the chemical component definition repository
        """
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting at %s" % ts)
        startTime = time.time()
        pathList = []
        try:
            dataS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
            dataList = [a for a in dataS]
            optD = {}
            optD['topRepoPath'] = topRepoPath
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_chemCompPathWorker")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Path list length %d  in %.4f seconds\n" % (len(pathList), endTime0 - startTime))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return self.__applyFileLimit(pathList)

    def _entryPathWorker(self, dataList, procName, optionsD, workingDir):
        """ Return the list of chemical component definition file paths in the current repository.
        """
        topRepoPath = optionsD['topRepoPath']
        pathList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, dirs, files in scandir.walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) == 8:
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def getEntryPathList(self):
        return self.__getEntryPathList(self.__cfgWrapper('RCSB_PDBX_SANBOX_PATH'), numProc=self.__numProc)

    def __getEntryPathList(self, topRepoPath, numProc=8):
        """Get the path list for the chemical component definition repository
        """
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting at %s" % ts)
        startTime = time.time()
        pathList = []
        try:
            dataList = []
            anL = 'abcdefghijklmnopqrstuvwxyz0123456789'
            for a1 in anL:
                for a2 in anL:
                    hc = a1 + a2
                    dataList.append(hc)
                    hc = a2 + a1
                    dataList.append(hc)
            dataList = list(set(dataList))
            #
            optD = {}
            optD['topRepoPath'] = topRepoPath
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_entryPathWorker")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Path list length %d  in %.4f seconds\n" % (len(pathList), endTime0 - startTime))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return self.__applyFileLimit(pathList)

    def getBirdPathList(self):
        return self.__getBirdPathList(self.__cfgWrapper('BIRD_REPO_PATH'))

    def __getBirdPathList(self, topRepoPath):
        """Get the path list of BIRD definitions in the repository.
        """
        pathList = []
        try:
            refIo = PdbxPrdIo(verbose=self.__verbose)
            refIo.setCachePath(topRepoPath)
            pathList = refIo.makeDefinitionPathList()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return self.__applyFileLimit(pathList)

    def getBirdFamilyPathList(self):
        return self.__getBirdFamilyPathList(self.__cfgWrapper('BIRD_FAMILY_REPO_PATH'))

    def __getBirdFamilyPathList(self, topRepoPath):
        """Get the path list of BIRD Family definitions in the repository.
        """
        pathList = []
        try:
            refIo = PdbxFamilyIo(verbose=self.__verbose)
            refIo.setCachePath(topRepoPath)
            pathList = refIo.makeDefinitionPathList()

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return self.__applyFileLimit(pathList)

    def getBirdChemCompPathList(self):
        return self.__getBirdChemCompPathList(self.__cfgWrapper('BIRD_CHEM_COMP_REPO_PATH'))

    def __getBirdChemCompPathList(self, topRepoPath):
        """Get the path list of BIRD chemical component definitions in the repository.
        """
        pathList = []
        try:
            refIo = PdbxPrdCcIo(verbose=self.__verbose)
            refIo.setCachePath(topRepoPath)
            pathList = refIo.makeDefinitionPathList()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return self.__applyFileLimit(pathList)

    def __applyFileLimit(self, pathList):
        logger.debug("Length of file path list %d (limit %r)" % (len(pathList), self.__fileLimit))
        if self.__fileLimit:
            return pathList[:self.__fileLimit]
        else:
            return pathList
