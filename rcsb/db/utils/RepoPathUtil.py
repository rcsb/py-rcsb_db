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
#   23-May-2018  jdw add getRepoPathList() convenience method
#   18-Jun-2018  jdw move mock support to the configuration module
#   12-Jul-2018  jdw correct config for PDBX_REPO_PATH
#   13-Aug-2018  jdw add support for gz compressed entry files
##
"""
 Utilites for scanning common data repository file systems.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time

from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

try:
    from os import walk
except ImportError:
    from scandir import walk


logger = logging.getLogger(__name__)


class RepoPathUtil(object):

    def __init__(self, cfgOb, numProc=8, fileLimit=None, verbose=False):
        self.__fileLimit = fileLimit
        self.__numProc = numProc
        self.__verbose = verbose
        self.__cfgOb = cfgOb
        self.__mpFormat = '[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s'

    def getRepoPathList(self, contentType, inputPathList=None):
        """ Convenience method to return repository path list by content type:
        """
        outputPathList = []
        inputPathList = inputPathList if inputPathList else []
        try:
            if contentType == "bird":
                outputPathList = inputPathList if inputPathList else self.getBirdPathList()
            elif contentType == "bird_family":
                outputPathList = inputPathList if inputPathList else self.getBirdFamilyPathList()
            elif contentType == 'chem_comp':
                outputPathList = inputPathList if inputPathList else self.getChemCompPathList()
            elif contentType == 'bird_chem_comp':
                outputPathList = inputPathList if inputPathList else self.getBirdChemCompPathList()
            elif contentType == 'pdbx':
                outputPathList = inputPathList if inputPathList else self.getEntryPathList()
            elif contentType in ['pdb_distro', 'da_internal', 'status_history']:
                outputPathList = inputPathList if inputPathList else []
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        if self.__fileLimit:
            outputPathList = outputPathList[:self.__fileLimit]

        return outputPathList

    def _chemCompPathWorker(self, dataList, procName, optionsD, workingDir):
        """ Return the list of chemical component definition file paths in the current repository.
        """
        topRepoPath = optionsD['topRepoPath']
        pathList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, dirs, files in walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) <= 7:
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def getChemCompPathList(self):
        return self.__getChemCompPathList(self.__cfgOb.getPath('CHEM_COMP_REPO_PATH'), numProc=self.__numProc)

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
        """ Return the list of entry file paths in the current repository.
        """
        topRepoPath = optionsD['topRepoPath']
        pathList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, dirs, files in walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if ((name.endswith(".cif.gz") and len(name) == 11) or (name.endswith(".cif") and len(name) == 8)):
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def getEntryPathList(self):
        return self.__getEntryPathList(self.__cfgOb.getPath('PDBX_REPO_PATH'), numProc=self.__numProc)

    def __getEntryPathList(self, topRepoPath, numProc=8):
        """Get the path list for structure entries in the input repository
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
        return self.__getBirdPathList(self.__cfgOb.getPath('BIRD_REPO_PATH'))

    def __getBirdPathList(self, topRepoPath):
        """ Return the list of definition file paths in the current repository.

            List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, dirs, files in os.walk(topRepoPath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.startswith("PRD_") and name.endswith(".cif") and len(name) <= 14:
                        pth = os.path.join(root, name)
                        sd[int(name[4:-4])] = pth
            #
            for k in sorted(sd.keys()):
                pathList.append(sd[k])
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        #
        return self.__applyFileLimit(pathList)

    def getBirdFamilyPathList(self):
        return self.__getBirdFamilyPathList(self.__cfgOb.getPath('BIRD_FAMILY_REPO_PATH'))

    def __getBirdFamilyPathList(self, topRepoPath):
        """ Return the list of definition file paths in the current repository.

            List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, dirs, files in os.walk(topRepoPath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.startswith("FAM_") and name.endswith(".cif") and len(name) <= 14:
                        pth = os.path.join(root, name)
                        sd[int(name[4:-4])] = pth
            #
            for k in sorted(sd.keys()):
                pathList.append(sd[k])
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        #
        return self.__applyFileLimit(pathList)

    def getBirdChemCompPathList(self):
        return self.__getBirdChemCompPathList(self.__cfgOb.getPath('BIRD_CHEM_COMP_REPO_PATH'))

    def __getBirdChemCompPathList(self, topRepoPath):
        """ Return the list of definition file paths in the current repository.

            List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, dirs, files in os.walk(topRepoPath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.startswith("PRDCC_") and name.endswith(".cif") and len(name) <= 16:
                        pth = os.path.join(root, name)
                        sd[int(name[6:-4])] = pth
            #
            for k in sorted(sd.keys()):
                pathList.append(sd[k])
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        #
        return self.__applyFileLimit(pathList)

    def __applyFileLimit(self, pathList):
        logger.debug("Length of file path list %d (limit %r)" % (len(pathList), self.__fileLimit))
        if self.__fileLimit:
            return pathList[:self.__fileLimit]
        else:
            return pathList
