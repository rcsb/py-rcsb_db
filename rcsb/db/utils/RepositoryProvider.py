##
# File:    RepositoryProvider.py
# Author:  J. Westbrook
# Date:    21-Mar-2018
#
# Updates:
#   22-Mar-2018  jdw add support for all repositories -
#   26-Mar-2018  jdw internalize the use of externally provided configuration object -
#   27-Mar-2018  jdw add path to support mock repositories for testing.
#   23-May-2018  jdw add getRepoPathList() convenience method
#   18-Jun-2018  jdw move mock support to the configuration module
#   12-Jul-2018  jdw correct config for PDBX_REPO_PATH
#   13-Aug-2018  jdw add support for gz compressed entry files
#   24-Oct-2018  jdw update for new configuration organization
#   28-Nov-2018  jdw add mergeBirdRefData()
#   13-Dec-2018  jdw add preliminary I/HM repository support
#    5-Feb-2019  jdw add just method naming conventions, add getLocator() method,
#                    consolidate deliver of path configuration details in __getRepoTopPath().
#   14-Mar-2019  jdw add VRPT_REPO_PATH_ENV as an override for the validation report repo path.
#   27-Aug-2019  jdw filter missing validation reports
#   16-Sep-2019  jdw consolidate chem_comp_core with bird_chem_comp_core
#
#
##
"""
 Utilites for scanning and accessing data in common repository file systems.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time

from rcsb.utils.io.HashableDict import HashableDict
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil
from rcsb.utils.validation.ValidationReportProvider import ValidationReportProvider

try:
    from os import walk  # pylint: disable=ungrouped-imports
except ImportError:
    from scandir import walk


logger = logging.getLogger(__name__)


def toCifWrapper(xrt):
    dirPath = os.environ.get("_RP_DICT_PATH_")
    vpr = ValidationReportProvider(dirPath=dirPath, useCache=True, cleaCache=False)
    vrd = vpr.getReader()
    return vrd.toCif(xrt)


class RepositoryProvider(object):
    def __init__(self, cfgOb, cachePath=None, numProc=8, fileLimit=None, verbose=False):
        self.__fileLimit = fileLimit
        self.__numProc = numProc
        self.__verbose = verbose
        self.__cfgOb = cfgOb
        self.__configName = self.__cfgOb.getDefaultSectionName()
        cpth = cachePath if cachePath else "."
        self.__cachePath = os.path.join(cpth, self.__cfgOb.get("REPO_UTIL_CACHE_DIR", sectionName=self.__configName))
        #
        self.__mU = MarshalUtil(workPath=self.__cachePath)
        #
        self.__ccPathD = None
        #
        self.__mpFormat = "[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s"

    def getLocatorObjList(self, contentType, inputPathList=None, mergeContentTypes=None):
        """Convenience method to get the data path list for the input repository content type.

        Args:
            contentType (str): Repository content type (e.g. pdbx, chem_comp, bird, ...)
            inputPathList (list, optional): path list that will be returned if provided.
            mergeContentTypes (list, optional): repository content types to combined with the
                                primary content type.

        Returns:
            Obj list: data file paths or tuple of file paths

        """
        inputPathList = inputPathList if inputPathList else []
        if inputPathList:
            return self.getLocatorObjListWithInput(contentType, inputPathList=inputPathList, mergeContentTypes=mergeContentTypes)
        #
        if mergeContentTypes and "vrpt" in mergeContentTypes and contentType in ["pdbx", "pdbx_core"]:
            dictPath = os.path.join(self.__cachePath, self.__cfgOb.get("DICTIONARY_CACHE_DIR", sectionName=self.__cfgOb.getDefaultSectionName()))
            os.environ["_RP_DICT_PATH_"] = os.path.join(dictPath, "vprt")
            locatorList = self.getEntryLocatorObjList(mergeContentTypes=mergeContentTypes)
        else:
            locatorList = self.__getLocatorList(contentType, inputPathList=inputPathList)
        return locatorList

    def getLocatorObjListWithInput(self, contentType, inputPathList=None, mergeContentTypes=None):
        """Convenience method to get the data path list for the input repository content type.

        Args:
            contentType (str): Repository content type (e.g. pdbx, chem_comp, bird, ...)
            inputPathList (list, optional): path list that will be returned if provided.
            mergeContentTypes (list, optional): repository content types to combined with the
                                primary content type.

        Returns:
            Obj list: data file paths or tuple of file paths

        """
        inputPathList = inputPathList if inputPathList else []
        locatorList = self.__getLocatorList(contentType, inputPathList=inputPathList)
        # JDW move the following to config
        if mergeContentTypes and "vrpt" in mergeContentTypes and contentType in ["pdbx", "pdbx_core"]:
            dictPath = os.path.join(self.__cachePath, self.__cfgOb.get("DICTIONARY_CACHE_DIR", sectionName=self.__cfgOb.getDefaultSectionName()))
            os.environ["_RP_DICT_PATH_"] = os.path.join(dictPath, "vprt")
            #
            locObjL = []
            for locator in locatorList:
                if isinstance(locator, str):
                    kwD = HashableDict({})
                    oL = [HashableDict({"locator": locator, "fmt": "mmcif", "kwargs": kwD})]
                    for mergeContentType in mergeContentTypes:
                        _, fn = os.path.split(locator)
                        idCode = fn[:4] if fn and len(fn) >= 8 else None
                        mergeLocator = self.__getLocator(mergeContentType, idCode, checkExists=True) if idCode else None
                        if mergeLocator:
                            # kwD = HashableDict({"marshalHelper": vrd.toCif})
                            kwD = HashableDict({"marshalHelper": toCifWrapper})
                            oL.append(HashableDict({"locator": mergeLocator, "fmt": "xml", "kwargs": kwD}))
                    lObj = tuple(oL)
                else:
                    logger.error("Unexpected output locator type %r", locator)
                    lObj = locator
                locObjL.append(lObj)
            #
            locatorList = locObjL
        # -
        return locatorList

    def getContainerList(self, locatorObjList):
        """ Return the data container list obtained by parsing the input locator object list.
        """
        cL = []
        for locatorObj in locatorObjList:
            myContainerList = self.__mergeContainers(locatorObj, fmt="mmcif", mergeTarget=0)
            for cA in myContainerList:
                cL.append(cA)
        return cL

    def __mergeContainers(self, locatorObj, fmt="mmcif", mergeTarget=0):
        """ Consolidate content in auxiliary files locatorObj[1:] into
            locatorObj[0] container index 'mergeTarget'.

        """
        #
        cL = []
        try:
            if isinstance(locatorObj, str):
                cL = self.__mU.doImport(locatorObj, fmt=fmt)
                return cL if cL else []
            elif isinstance(locatorObj, (list, tuple)) and locatorObj:
                dD = locatorObj[0]
                kw = dD["kwargs"]
                cL = self.__mU.doImport(dD["locator"], fmt=dD["fmt"], **kw)
                if cL:
                    for dD in locatorObj[1:]:
                        kw = dD["kwargs"]
                        rObj = self.__mU.doImport(dD["locator"], fmt=dD["fmt"], **kw)
                        mergeL = rObj if rObj else []
                        for mc in mergeL:
                            cL[mergeTarget].merge(mc)
                #
                return cL
            else:
                return []
        except Exception as e:
            logger.exception("Failing for %r with %s", locatorObj, str(e))

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
                if "locator" in locatorObj[locatorIndex]:
                    locIdx[locatorObj[locatorIndex]["locator"]] = ii
            #
            for pth in pathList:
                jj = locIdx[pth] if pth in locIdx else None
                if jj is not None:
                    rL.append(locatorObjList[jj])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return rL

    def getLocatorPaths(self, locatorObjList, locatorIndex=0):
        try:
            if locatorObjList and isinstance(locatorObjList[0], str):
                return locatorObjList
            else:
                return [locatorObj[locatorIndex]["locator"] for locatorObj in locatorObjList]
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return []

    def __getLocatorList(self, contentType, inputPathList=None):
        """ Internal convenience method to return repository path list by content type:
        """
        outputPathList = []
        inputPathList = inputPathList if inputPathList else []
        try:
            if contentType in ["bird", "bird_core"]:
                outputPathList = inputPathList if inputPathList else self.getBirdPathList()
            elif contentType == "bird_family":
                outputPathList = inputPathList if inputPathList else self.getBirdFamilyPathList()
            elif contentType in ["chem_comp"]:
                outputPathList = inputPathList if inputPathList else self.getChemCompPathList()
            elif contentType in ["bird_chem_comp"]:
                outputPathList = inputPathList if inputPathList else self.getBirdChemCompPathList()
            elif contentType in ["pdbx", "pdbx_core"]:
                outputPathList = inputPathList if inputPathList else self.getEntryPathList()
            elif contentType in ["chem_comp_core", "bird_consolidated", "bird_chem_comp_core"]:
                outputPathList = inputPathList if inputPathList else self.mergeBirdAndChemCompRefData()
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:
                outputPathList = inputPathList if inputPathList else self.getIhmDevPathList()
            elif contentType in ["pdb_distro", "da_internal", "status_history"]:
                outputPathList = inputPathList if inputPathList else []
            else:
                logger.warning("Unsupported contentType %s", contentType)
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        if self.__fileLimit:
            outputPathList = outputPathList[: self.__fileLimit]

        return sorted(outputPathList)

    def __getLocator(self, contentType, idCode, version="v1-0", checkExists=False):
        """ Convenience method to return repository path for a content type and cardinal identifier.
        """
        pth = None
        try:
            idCodel = idCode.lower()
            if contentType == "bird":
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode[-1], idCode + ".cif")
            elif contentType == "bird_family":
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode[-1], idCode + ".cif")
            elif contentType in ["chem_comp", "chem_comp_core"]:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode[0], idCode, idCode + ".cif")
            elif contentType in ["bird_chem_comp"]:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode[-1], idCode + ".cif")
            elif contentType in ["pdbx", "pdbx_core"]:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCodel[1:3], idCodel, idCodel + ".cif.gz")
            elif contentType in ["bird_consolidated", "bird_chem_comp_core"]:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode + ".cif")
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode, idCode + "_model_%s.cif.gz" % version)
            elif contentType in ["pdb_distro", "da_internal", "status_history"]:
                pass
            elif contentType in ["vrpt"]:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCodel[1:3], idCodel, idCodel + "_validation.xml.gz")
            else:
                logger.warning("Unsupported contentType %s", contentType)
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        if checkExists:
            pth = pth if self.__mU.exists(pth) else None
        return pth

    def __getRepoTopPath(self, contentType):
        """ Convenience method to return repository top path from configuration data.
        """
        pth = None
        try:
            if contentType == "bird":
                pth = self.__cfgOb.getPath("BIRD_REPO_PATH", sectionName=self.__configName)
            elif contentType == "bird_family":
                pth = self.__cfgOb.getPath("BIRD_FAMILY_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["chem_comp", "chem_comp_core"]:
                pth = self.__cfgOb.getPath("CHEM_COMP_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["bird_chem_comp"]:
                pth = self.__cfgOb.getPath("BIRD_CHEM_COMP_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["pdbx", "pdbx_core"]:
                pth = self.__cfgOb.getPath("PDBX_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["bird_consolidated", "bird_chem_comp_core"]:
                pth = self.__cachePath
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:
                pth = self.__cfgOb.getPath("IHM_DEV_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["pdb_distro", "da_internal", "status_history"]:
                pass
            elif contentType in ["vrpt"]:
                pth = self.__cfgOb.getEnvValue("VRPT_REPO_PATH_ENV", sectionName=self.__configName, default=None)
                if pth is None:
                    pth = self.__cfgOb.getPath("VRPT_REPO_PATH", sectionName=self.__configName)
                else:
                    logger.debug("Using validation report path from environment assignment %s", pth)
            else:
                logger.warning("Unsupported contentType %s", contentType)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return pth

    def _chemCompPathWorker(self, dataList, procName, optionsD, workingDir):
        """ Return the list of chemical component definition file paths in the current repository.
        """
        _ = procName
        _ = workingDir
        topRepoPath = optionsD["topRepoPath"]
        pathList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, _, files in walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) <= 7:
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def getChemCompPathList(self):
        return self.__getChemCompPathList(self.__getRepoTopPath("chem_comp"), numProc=self.__numProc)

    def __getChemCompPathList(self, topRepoPath, numProc=8):
        """Get the path list for the chemical component definition repository
        """
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting at %s", ts)
        startTime = time.time()
        pathList = []
        try:
            dataS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            dataList = [a for a in dataS]
            optD = {}
            optD["topRepoPath"] = topRepoPath
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_chemCompPathWorker")
            _, _, retLists, _ = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Path list length %d  in %.4f seconds", len(pathList), endTime0 - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyFileLimit(pathList)

    def _entryLocatorObjWithMergeWorker(self, dataList, procName, optionsD, workingDir):
        """ Return the list of entry locator objects including merge content in the current repository.
        """
        _ = procName
        _ = workingDir
        topRepoPath = optionsD["topRepoPath"]
        mergeContentTypes = optionsD["mergeContentTypes"]
        locatorObjList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, _, files in walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for fn in files:
                    if (fn.endswith(".cif.gz") and len(fn) == 11) or (fn.endswith(".cif") and len(fn) == 8):
                        locator = os.path.join(root, fn)
                        kwD = HashableDict({})
                        oL = [HashableDict({"locator": locator, "fmt": "mmcif", "kwargs": kwD})]
                        for mergeContentType in mergeContentTypes:
                            idCode = fn[:4] if fn and len(fn) >= 8 else None
                            mergeLocator = self.__getLocator(mergeContentType, idCode, checkExists=True) if idCode else None
                            if mergeLocator:
                                kwD = HashableDict({"marshalHelper": toCifWrapper})
                                oL.append(HashableDict({"locator": mergeLocator, "fmt": "xml", "kwargs": kwD}))
                        lObj = tuple(oL)
                        locatorObjList.append(lObj)
        return dataList, locatorObjList, []

    def getEntryLocatorObjList(self, mergeContentTypes=None):
        return self.__getEntryLocatorObjList(self.__getRepoTopPath("pdbx"), numProc=self.__numProc, mergeContentTypes=mergeContentTypes)

    def __getEntryLocatorObjList(self, topRepoPath, numProc=8, mergeContentTypes=None):
        """Get the path list for structure entries in the input repository
        """
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting at %s", ts)
        startTime = time.time()
        pathList = []
        try:
            dataList = []
            anL = "abcdefghijklmnopqrstuvwxyz0123456789"
            for a1 in anL:
                for a2 in anL:
                    hc = a1 + a2
                    dataList.append(hc)
                    hc = a2 + a1
                    dataList.append(hc)
            dataList = list(set(dataList))
            #
            optD = {}
            optD["topRepoPath"] = topRepoPath
            optD["mergeContentTypes"] = mergeContentTypes
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_entryLocatorObjWithMergeWorker")
            _, _, retLists, _ = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Locator object list length %d  in %.4f seconds", len(pathList), endTime0 - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyFileLimit(pathList)

    def _entryPathWorker(self, dataList, procName, optionsD, workingDir):
        """ Return the list of entry file paths in the current repository.
        """
        _ = procName
        _ = workingDir
        topRepoPath = optionsD["topRepoPath"]
        pathList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, _, files in walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if (name.endswith(".cif.gz") and len(name) == 11) or (name.endswith(".cif") and len(name) == 8):
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def getEntryPathList(self):
        return self.__getEntryPathList(self.__getRepoTopPath("pdbx"), numProc=self.__numProc)

    def __getEntryPathList(self, topRepoPath, numProc=8):
        """Get the path list for structure entries in the input repository
        """
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting at %s", ts)
        startTime = time.time()
        pathList = []
        try:
            dataList = []
            anL = "abcdefghijklmnopqrstuvwxyz0123456789"
            for a1 in anL:
                for a2 in anL:
                    hc = a1 + a2
                    dataList.append(hc)
                    hc = a2 + a1
                    dataList.append(hc)
            dataList = list(set(dataList))
            #
            optD = {}
            optD["topRepoPath"] = topRepoPath
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_entryPathWorker")
            _, _, retLists, _ = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Path list length %d  in %.4f seconds", len(pathList), endTime0 - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyFileLimit(pathList)

    def getBirdPathList(self):
        return self.__getBirdPathList(self.__getRepoTopPath("bird"))

    def __getBirdPathList(self, topRepoPath):
        """ Return the list of definition file paths in the current repository.

            List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, _, files in os.walk(topRepoPath, topdown=False):
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
            logger.exception("Failing with %s", str(e))
        #
        return self.__applyFileLimit(pathList)

    def getBirdFamilyPathList(self):
        return self.__getBirdFamilyPathList(self.__getRepoTopPath("bird_family"))

    def __getBirdFamilyPathList(self, topRepoPath):
        """ Return the list of definition file paths in the current repository.

            List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, _, files in os.walk(topRepoPath, topdown=False):
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
            logger.exception("Failing with %s", str(e))
        #
        return self.__applyFileLimit(pathList)

    def getBirdChemCompPathList(self):
        return self.__getBirdChemCompPathList(self.__getRepoTopPath("bird_chem_comp"))

    def __getBirdChemCompPathList(self, topRepoPath):
        """ Return the list of definition file paths in the current repository.

            List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, _, files in os.walk(topRepoPath, topdown=False):
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
            logger.exception("Failing with %s", str(e))
        #
        return self.__applyFileLimit(pathList)

    def __applyFileLimit(self, pathList):
        logger.debug("Length of file path list %d (limit %r)", len(pathList), self.__fileLimit)
        if self.__fileLimit:
            return pathList[: self.__fileLimit]
        else:
            return pathList

    def __buildFamilyIndex(self):
        """ Using information from the PRD family definition:
            #
            loop_
            _pdbx_reference_molecule_list.family_prd_id
            _pdbx_reference_molecule_list.prd_id
                FAM_000010 PRD_000041
                FAM_000010 PRD_000042
                FAM_000010 PRD_000043
                FAM_000010 PRD_000044
                FAM_000010 PRD_000048
                FAM_000010 PRD_000049
                FAM_000010 PRD_000051
            #
        """
        prdD = {}
        try:
            pthL = self.__getLocatorList("bird_family")
            for pth in pthL:
                containerL = self.__mU.doImport(pth, fmt="mmcif")
                for container in containerL:
                    catName = "pdbx_reference_molecule_list"
                    if container.exists(catName):
                        catObj = container.getObj(catName)
                        for ii in range(catObj.getRowCount()):
                            familyPrdId = catObj.getValue(attributeName="family_prd_id", rowIndex=ii)
                            prdId = catObj.getValue(attributeName="prd_id", rowIndex=ii)
                            if prdId in prdD:
                                logger.debug("duplicate prdId in family index %s %s", prdId, familyPrdId)
                            prdD[prdId] = {"familyPrdId": familyPrdId, "c": container}
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return prdD

    def __buildBirdCcIndex(self):
        """ Using information from the PRD pdbx_reference_molecule category to
        index the BIRDs corresponding small molecule correspondences

        """
        prdD = {}
        ccPathD = {}
        try:
            ccPathL = self.__getLocatorList("chem_comp")
            ccPathD = {}
            for ccPath in ccPathL:
                _, fn = os.path.split(ccPath)
                ccId, _ = os.path.splitext(fn)
                ccPathD[ccId] = ccPath
            logger.debug("ccPathD length %d", len(ccPathD))
            pthL = self.__getLocatorList("bird")
            for pth in pthL:
                containerL = self.__mU.doImport(pth, fmt="mmcif")
                for container in containerL:
                    catName = "pdbx_reference_molecule"
                    if container.exists(catName):
                        catObj = container.getObj(catName)
                        ii = 0
                        prdRepType = catObj.getValue(attributeName="represent_as", rowIndex=ii)
                        logger.debug("represent as %r", prdRepType)
                        if prdRepType in ["single molecule"]:
                            ccId = catObj.getValueOrDefault(attributeName="chem_comp_id", rowIndex=ii, defaultValue=None)
                            prdId = catObj.getValue(attributeName="prd_id", rowIndex=ii)
                            logger.debug("mapping prdId %r ccId %r", prdId, ccId)
                            if ccId and ccId in ccPathD:
                                prdD[prdId] = {"ccId": ccId, "ccPath": ccPathD[ccId]}
                                ccPathD[ccPathD[ccId]] = {"ccId": ccId, "prdId": prdId}
                            else:
                                logger.error("Bad ccId %r for BIRD %r", ccId, prdId)
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return prdD, ccPathD

    # -
    def mergeBirdAndChemCompRefData(self):
        prdSmallMolCcD, ccPathD = self.__buildBirdCcIndex()
        logger.info("PRD to CCD index length %d CCD map path length %d", len(prdSmallMolCcD), len(ccPathD))
        outputPathList = self.mergeBirdRefData(prdSmallMolCcD)
        ccOutputPathList = [pth for pth in self.getChemCompPathList() if pth not in ccPathD]
        outputPathList.extend(ccOutputPathList)
        return outputPathList

    def mergeBirdRefData(self, prdSmallMolCcD):
        """ Consolidate all of the bird reference data in a single container.

            If the BIRD is a 'small molecule' type then also merge with the associated CC definition.

            Store the merged data in the REPO_UTIL cache path and ...

            Return a path list for the consolidated data files -

        """
        outPathList = []
        try:
            birdPathList = self.__getLocatorList("bird")
            birdPathD = {}
            for birdPath in birdPathList:
                _, fn = os.path.split(birdPath)
                prdId, _ = os.path.splitext(fn)
                birdPathD[prdId] = birdPath
            #
            logger.debug("BIRD data length %d", len(birdPathD))
            logger.debug("BIRD keys %r", list(birdPathD.keys()))
            birdCcPathList = self.__getLocatorList("bird_chem_comp")
            birdCcPathD = {}
            for birdCcPath in birdCcPathList:
                _, fn = os.path.split(birdCcPath)
                prdCcId, _ = os.path.splitext(fn)
                prdId = "PRD_" + prdCcId[6:]
                birdCcPathD[prdId] = birdCcPath
            #
            logger.debug("BIRD CC data length %d", len(birdCcPathD))
            logger.debug("BIRD CC keys %r", list(birdCcPathD.keys()))
            fD = self.__buildFamilyIndex()
            logger.debug("Family index length %d", len(fD))
            logger.debug("Family index keys %r", list(fD.keys()))
            logger.debug("PRD to CCD small mol index length %d", len(prdSmallMolCcD))
            #
            for prdId in birdPathD:
                fp = os.path.join(self.__cachePath, prdId + ".cif")
                logger.debug("Export cache path is %r", fp)

                if prdId in birdPathD:
                    pth2 = birdPathD[prdId]
                    cL = self.__mU.doImport(pth2, fmt="mmcif")
                    cFull = cL[0]
                    logger.debug("Got Bird %r", cFull.getName())
                #
                #
                ccBird = None
                ccD = None
                if prdId in prdSmallMolCcD:
                    pthCc = prdSmallMolCcD[prdId]["ccPath"]
                    cL = self.__mU.doImport(pthCc, fmt="mmcif")
                    ccD = cL[0]
                    logger.debug("Got corresponding CCD %r", ccD.getName())
                elif prdId in birdCcPathD:
                    pth1 = birdCcPathD[prdId]
                    c1L = self.__mU.doImport(pth1, fmt="mmcif")
                    ccBird = c1L[0]
                    logger.debug("Got ccBird %r", ccBird.getName())
                    #
                cFam = None
                if prdId in fD:
                    cFam = fD[prdId]["c"]
                    logger.debug("Got cFam %r", cFam.getName())
                #
                if ccD:
                    for catName in ccD.getObjNameList():
                        cFull.append(ccD.getObj(catName))
                #
                if ccBird:
                    for catName in ccBird.getObjNameList():
                        cFull.append(ccBird.getObj(catName))
                if cFam:
                    for catName in cFam.getObjNameList():
                        cFull.append(cFam.getObj(catName))
                #
                self.__mU.doExport(fp, [cFull], fmt="mmcif")
                outPathList.append(fp)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return outPathList
        #

    def __exportConfig(self, container):
        """
                - CATEGORY_NAME: diffrn_detector
                  ATTRIBUTE_NAME_LIST:
                      - pdbx_frequency
                - CATEGORY_NAME: pdbx_serial_crystallography_measurement
                  ATTRIBUTE_NAME_LIST:
                      - diffrn_id
                      - pulse_energy
                      - pulse_duration
                      - xfel_pulse_repetition_rate
        """
        for catName in container.getObjNameList():
            cObj = container.getObj(catName)
            print("- CATEGORY_NAME: %s" % catName)
            print("  ATTRIBUTE_NAME_LIST:")
            for atName in cObj.getAttributeList():
                print("       - %s" % atName)
        return True

    def getIhmDevPathList(self):
        return self.__getIhmDevPathList(self.__getRepoTopPath("ihm_dev"))

    def __getIhmDevPathList(self, topRepoPath):
        """ Return the list of I/HM entries in the current repository.

            File name template is: PDBDEV_0000 0020_model_v1-0.cif.gz

            List is ordered in increasing PRDDEV numerical code.
        """
        pathList = []
        logger.debug("Searching path %r", topRepoPath)
        try:
            sd = {}
            for root, _, files in os.walk(topRepoPath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.startswith("PDBDEV_") and name.endswith(".cif.gz") and len(name) <= 50:
                        pth = os.path.join(root, name)
                        sd[int(name[7:15])] = pth
            #
            for k in sorted(sd.keys()):
                pathList.append(sd[k])
        except Exception as e:
            logger.exception("Failing search in %r with %s", topRepoPath, str(e))
        #
        return self.__applyFileLimit(pathList)
