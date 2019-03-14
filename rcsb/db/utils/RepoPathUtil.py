##
# File:    RepoPathUtil.py
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

from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

try:
    from os import walk
except ImportError:
    from scandir import walk


logger = logging.getLogger(__name__)


class RepoPathUtil(object):

    def __init__(self, cfgOb, cfgSectionName='site_info', numProc=8, fileLimit=None, workPath=None, verbose=False):
        self.__fileLimit = fileLimit
        self.__numProc = numProc
        self.__verbose = verbose
        self.__cfgOb = cfgOb
        self.__cfgSectionName = cfgSectionName
        self.__workPath = workPath if workPath else '.'
        #
        self.__mpFormat = '[%(levelname)s] %(asctime)s %(processName)s-%(module)s.%(funcName)s: %(message)s'

    def getLocatorList(self, contentType, inputPathList=None):
        """ Convenience method to return repository path list by content type:
        """
        outputPathList = []
        inputPathList = inputPathList if inputPathList else []
        try:
            if contentType == "bird":
                outputPathList = inputPathList if inputPathList else self.getBirdPathList()
            elif contentType == "bird_family":
                outputPathList = inputPathList if inputPathList else self.getBirdFamilyPathList()
            elif contentType in ['chem_comp', 'chem_comp_core']:
                outputPathList = inputPathList if inputPathList else self.getChemCompPathList()
            elif contentType in ['bird_chem_comp']:
                outputPathList = inputPathList if inputPathList else self.getBirdChemCompPathList()
            elif contentType in ['pdbx', 'pdbx_core']:
                outputPathList = inputPathList if inputPathList else self.getEntryPathList()
            elif contentType in ['bird_consolidated', 'bird_chem_comp_core']:
                outputPathList = inputPathList if inputPathList else self.mergeBirdRefData()
            elif contentType in ['ihm_dev', 'ihm_dev_core']:
                outputPathList = inputPathList if inputPathList else self.getIhmDevPathList()
            elif contentType in ['pdb_distro', 'da_internal', 'status_history']:
                outputPathList = inputPathList if inputPathList else []
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        if self.__fileLimit:
            outputPathList = outputPathList[:self.__fileLimit]

        return outputPathList

    def getLocator(self, contentType, idCode, version='v1-0'):
        """ Convenience method to return repository path for a content type and cardinal identifier.
        """
        pth = None
        try:
            idCodel = idCode.lower()
            if contentType == "bird":
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode[-1], idCode + '.cif')
            elif contentType == "bird_family":
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode[-1], idCode + '.cif')
            elif contentType in ['chem_comp', 'chem_comp_core']:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode[0], idCode, idCode + '.cif')
            elif contentType in ['bird_chem_comp']:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode[-1], idCode + '.cif')
            elif contentType in ['pdbx', 'pdbx_core']:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCodel[1:3], idCodel, idCodel + '.cif.gz')
            elif contentType in ['bird_consolidated', 'bird_chem_comp_core']:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode + '.cif')
            elif contentType in ['ihm_dev', 'ihm_dev_core']:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCode, idCode + '_model_%s.cif.gz' % version)
            elif contentType in ['pdb_distro', 'da_internal', 'status_history']:
                pass
            elif contentType in ['vrpt']:
                pth = os.path.join(self.__getRepoTopPath(contentType), idCodel[1:3], idCodel, idCodel + '_validation.xml.gz')
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return pth

    def __getRepoTopPath(self, contentType):
        """ Convenience method to return repository top path from configuration data.
        """
        pth = None
        try:
            if contentType == "bird":
                pth = self.__cfgOb.getPath('BIRD_REPO_PATH', sectionName=self.__cfgSectionName)
            elif contentType == "bird_family":
                pth = self.__cfgOb.getPath('BIRD_FAMILY_REPO_PATH', sectionName=self.__cfgSectionName)
            elif contentType in ['chem_comp', 'chem_comp_core']:
                pth = self.__cfgOb.getPath('CHEM_COMP_REPO_PATH', sectionName=self.__cfgSectionName)
            elif contentType in ['bird_chem_comp']:
                pth = self.__cfgOb.getPath('BIRD_CHEM_COMP_REPO_PATH', sectionName=self.__cfgSectionName)
            elif contentType in ['pdbx', 'pdbx_core']:
                pth = self.__cfgOb.getPath('PDBX_REPO_PATH', sectionName=self.__cfgSectionName)
            elif contentType in ['bird_consolidated', 'bird_chem_comp_core']:
                pth = self.__workPath
            elif contentType in ['ihm_dev', 'ihm_dev_core']:
                pth = self.__cfgOb.getPath('IHM_DEV_REPO_PATH', sectionName=self.__cfgSectionName)
            elif contentType in ['pdb_distro', 'da_internal', 'status_history']:
                pass
            elif contentType in ['vrpt']:
                pth = self.__cfgOb.getEnvValue('VRPT_REPO_PATH_ENV', sectionName=self.__cfgSectionName, default=None)
                if pth is None:
                    pth = self.__cfgOb.getPath('VRPT_REPO_PATH', sectionName=self.__cfgSectionName)
                else:
                    logger.debug("Using validation report path from environment assignment %s" % pth)
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return pth

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
        return self.__getChemCompPathList(self.__getRepoTopPath('chem_comp'), numProc=self.__numProc)

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
        return self.__getEntryPathList(self.__getRepoTopPath('pdbx'), numProc=self.__numProc)

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
        return self.__getBirdPathList(self.__getRepoTopPath('bird'))

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
        return self.__getBirdFamilyPathList(self.__getRepoTopPath('bird_family'))

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
        return self.__getBirdChemCompPathList(self.__getRepoTopPath('bird_chem_comp'))

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
            mU = MarshalUtil(workPath=self.__workPath)
            pthL = self.getLocatorList('bird_family')
            for pth in pthL:
                containerL = mU.doImport(pth, format="mmcif")
                for container in containerL:
                    catName = "pdbx_reference_molecule_list"
                    if container.exists(catName):
                        catObj = container.getObj(catName)
                        for ii in range(catObj.getRowCount()):
                            familyPrdId = catObj.getValue(attributeName='family_prd_id', rowIndex=ii)
                            prdId = catObj.getValue(attributeName='prd_id', rowIndex=ii)
                            if prdId in prdD:
                                logger.debug("duplicate prdId in family index %s %s " % (prdId, familyPrdId))
                            prdD[prdId] = {'familyPrdId': familyPrdId, 'c': container}
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return prdD
    #

    def mergeBirdRefData(self):
        """ Consolidate all of the bird reference data in a single container.

            Return a path list for the consolidated data files -

        """
        outPathList = []
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            birdPathList = self.getLocatorList('bird')
            birdPathD = {}
            for birdPath in birdPathList:
                _, fn = os.path.split(birdPath)
                prdId, _ = os.path.splitext(fn)
                birdPathD[prdId] = birdPath
            #
            logger.debug("BIRD data length %d" % len(birdPathD))
            logger.debug("BIRD keys %r" % list(birdPathD.keys()))
            birdCcPathList = self.getLocatorList('bird_chem_comp')
            birdCcPathD = {}
            for birdCcPath in birdCcPathList:
                _, fn = os.path.split(birdCcPath)
                prdCcId, _ = os.path.splitext(fn)
                prdId = 'PRD_' + prdCcId[6:]
                birdCcPathD[prdId] = birdCcPath
            #
            logger.debug("BIRD CC data length %d" % len(birdCcPathD))
            logger.debug("BIRD CC keys %r" % list(birdCcPathD.keys()))
            fD = self.__buildFamilyIndex()
            logger.debug("Family index length %d" % len(fD))
            logger.debug("Family index keys %r" % list(fD.keys()))
            #
            #
            for prdId in birdPathD:
                fp = os.path.join(self.__workPath, prdId + '.cif')
                logger.debug("Export path is %r" % fp)
                #
                if prdId in birdPathD:
                    pth2 = birdPathD[prdId]
                    c2L = mU.doImport(pth2, format="mmcif")
                    cFull = c2L[0]
                    logger.debug("Got cBird %r" % cFull.getName())
                #
                ccBird = None
                if prdId in birdCcPathD:
                    pth1 = birdCcPathD[prdId]
                    c1L = mU.doImport(pth1, format="mmcif")
                    ccBird = c1L[0]
                    logger.debug("Got cFull %r" % ccBird.getName())
                    #
                cFam = None
                if prdId in fD:
                    cFam = fD[prdId]['c']
                    logger.debug("Got cFam %r" % cFam.getName())
                #
                if cFam:
                    for catName in cFam.getObjNameList():
                        cFull.append(cFam.getObj(catName))
                #
                if ccBird:
                    for catName in ccBird.getObjNameList():
                        cFull.append(ccBird.getObj(catName))
                #
                mU.doExport(fp, [cFull], format='mmcif')
                outPathList.append(fp)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
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
        return self.__getIhmDevPathList(self.__getRepoTopPath('ihm_dev'))

    def __getIhmDevPathList(self, topRepoPath):
        """ Return the list of I/HM entries in the current repository.

            File name template is: PDBDEV_0000 0020_model_v1-0.cif.gz

            List is ordered in increasing PRDDEV numerical code.
        """
        pathList = []
        logger.debug("Searching path %r" % topRepoPath)
        try:
            sd = {}
            for root, dirs, files in os.walk(topRepoPath, topdown=False):
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
            logger.exception("Failing search in %r with %s" % (topRepoPath, str(e)))
        #
        return self.__applyFileLimit(pathList)
