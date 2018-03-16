##
# File:    MongoDbLoaderWorker.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#
##
"""
Worker methods for loading MongoDb using BIRD, CCD and PDBx/mmCIF data files
and following external schema definitions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import os
import time
import scandir
import pickle

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.loaders.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef
from rcsb_db.schema.ChemCompSchemaDef import ChemCompSchemaDef
from rcsb_db.schema.PdbxSchemaDef import PdbxSchemaDef
from rcsb_db.utils.MultiProcUtil import MultiProcUtil
from rcsb_db.utils.ConfigUtil import ConfigUtil

from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo
from mmcif_utils.bird.PdbxFamilyIo import PdbxFamilyIo
from mmcif_utils.chemcomp.PdbxChemCompIo import PdbxChemCompIo

from rcsb_db.mongo.ConnectionBase import ConnectionBase
from rcsb_db.mongo.MongoDbUtil import MongoDbUtil


class MongoDbLoaderWorker(object):

    def __init__(self, configPath, configName, numProc=4, chunkSize=15, fileLimit=None, verbose=False, readBackCheck=False):
        self.__verbose = verbose
        #
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__fileLimit = fileLimit
        #
        # Controls for multiprocessing execution -
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        #
        self.__cu = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__birdCachePath = self.__cu.get('BIRD_REPO_PATH')
        self.__birdFamilyCachePath = self.__cu.get('BIRD_FAMILY_REPO_PATH')
        self.__ccCachePath = self.__cu.get('CHEM_COMP_REPO_PATH')
        self.__pdbxFileCache = self.__cu.get('RCSB_PDBX_SANBOX_PATH')
        self.__pdbxLoadListPath = self.__cu.get('PDBX_LOAD_LIST_PATH')
        self.__pdbxTableIdExcludeList = str(self.__cu.get('PDBX_EXCLUDE_TABLES', defaultValue="")).split(',')
        self.__readBackCheck = readBackCheck
        #
        self.__authD = self.__assignCredentials(self.__cu)

    def loadContentType(self, contentType, styleType='rowwise_by_name'):
        """  Driver method for loading MongoDb content -

            contentType:  one of 'bird','bird-family','chem-comp','pdbx'
            styleType:    one of 'rowwise_by_name', 'columnwise_by_name', 'rowwise_no_name', 'rowwise_by_name_with_cardinality'

        """
        try:
            sd, dbName, collectionName, inputPathList, tableIdExcludeList = self.__getLoadInfo(contentType)
            #
            optD = {}
            optD['sd'] = sd
            optD['dbName'] = dbName
            optD['collectionName'] = collectionName
            optD['styleType'] = styleType
            optD['tableIdExcludeList'] = tableIdExcludeList
            optD['authD'] = self.__authD
            optD['readBackCheck'] = self.__readBackCheck
            #
            self.__removeCollection(dbName, collectionName, self.__authD)
            self.__createCollection(dbName, collectionName, self.__authD)
            #
            numProc = self.__numProc
            chunkSize = self.__chunkSize if self.__chunkSize < len(inputPathList) else 0
            #
            mpu = MultiProcUtil(verbose=True)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="loadWorker")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=inputPathList, numProc=numProc, numResults=1, chunkSize=chunkSize)
            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def loadWorker(self, dataList, procName, optionsD, workingDir):
        """ Multi-proc worker method for MongoDb loading -
        """
        try:
            sd = optionsD['sd']
            styleType = optionsD['styleType']
            tableIdExcludeList = optionsD['tableIdExcludeList']
            dbName = optionsD['dbName']
            collectionName = optionsD['collectionName']
            authD = optionsD['authD']
            readBackCheck = optionsD['readBackCheck']
            sdp = SchemaDefDataPrep(schemaDefObj=sd, verbose=self.__verbose)
            sdp.setTableIdExcludeList(tableIdExcludeList)
            fType = "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates"
            if styleType in ["columnwise_by_name", "rowwise_no_name"]:
                fType = "drop-empty-tables|skip-max-width|assign-dates"
            tableDataDictList, containerNameList = sdp.fetchDocuments(dataList, styleType=styleType, filterType=fType)

            maxDocumentBytes = -1
            for tD, cN in zip(tableDataDictList, containerNameList):
                documentBytes = sys.getsizeof(pickle.dumps(tD))
                maxDocumentBytes = max(maxDocumentBytes, documentBytes)
                megaBytes = float(documentBytes) / 1000000.0
                #logger.info("Document %r %s  %.5f MB" % (tD['entry'], cN, megaBytes))
                if megaBytes > 15.8:
                    logger.info("Large document %s  %.4f MB" % (cN, megaBytes))
            logger.info("Maximum document size loaded %.4f MB" % (float(maxDocumentBytes) / 1000000.0))

            ok = self.__loadDocuments(dbName, collectionName, authD, tableDataDictList, readBackCheck=readBackCheck)
            # all or nothing here
            if ok:
                return dataList, dataList, []
            else:
                return [], [], []
        except Exception as e:
            logger.info("Failing with dataList %r" % dataList)
            logger.exception("Failing with %s" % str(e))

        return [], [], []

    # -------------- -------------- -------------- -------------- -------------- -------------- --------------
    #                                        ---  Supporting code follows ---
    #

    def __assignCredentials(self, cfgObj, dbType="mongodb"):
        dbUserId = None
        dbHost = None
        dbUserPwd = None
        dbName = None
        dbAdminDb = None
        if dbType == 'mongodb':
            dbUserId = cfgObj.get("MONGO_DB_USER_NAME")
            dbUserPwd = cfgObj.get("MONGO_DB_PASSWORD")
            dbName = cfgObj.get("MONGO_DB_NAME")
            dbHost = cfgObj.get("MONGO_DB_HOST")
            dbPort = cfgObj.get("MONGO_DB_PORT")
            dbAdminDb = cfgObj.get("MONGO_DB_ADMIN_DB_NAME")
        else:
            pass

        authD = {"DB_HOST": dbHost, 'DB_USER': dbUserId, 'DB_PW': dbUserPwd, 'DB_NAME': dbName, "DB_PORT": dbPort, 'DB_ADMIN_DB_NAME': dbAdminDb}
        return authD

    def __begin(self):
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def __end(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def __open(self, authD):
        cObj = ConnectionBase()
        cObj.setAuth(authD)
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

    def __createCollection(self, dbName, collectionName, authD):
        """Create database and collection -
        """
        try:
            cObj = self.__open(authD)
            client = self.__getClientConnection(cObj)
            mg = MongoDbUtil(client)
            ok = mg.createCollection(dbName, collectionName)
            ok = mg.databaseExists(dbName)
            ok = mg.collectionExists(dbName, collectionName)
            ok = self.__close(cObj)
            return ok
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __removeCollection(self, dbName, collectionName, authD):
        """Drop collection within database

        """
        try:
            cObj = self.__open(authD)
            client = self.__getClientConnection(cObj)
            mg = MongoDbUtil(client)
            #
            logger.debug("Databases = %r" % mg.getDatabaseNames())
            logger.debug("Collections = %r" % mg.getCollectionNames(dbName))
            ok = mg.dropCollection(dbName, collectionName)
            logger.debug("Databases = %r" % mg.getDatabaseNames())
            logger.debug("Collections = %r" % mg.getCollectionNames(dbName))
            ok = mg.collectionExists(dbName, collectionName)
            logger.debug("Collections = %r" % mg.getCollectionNames(dbName))
            ok = self.__close(cObj)
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __loadDocuments(self, dbName, collectionName, authD, dList, readBackCheck=False):
        try:
            cObj = self.__open(authD)
            client = self.__getClientConnection(cObj)
            mg = MongoDbUtil(client)
            #

            rIdL = mg.insertList(dbName, collectionName, dList)
            #
            if readBackCheck:
                #
                # Note that objects in dList are mutated by additional key '_id' that is added on insert -
                #
                rbStatus = True
                for ii, rId in enumerate(rIdL):
                    rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                    if (rObj != dList[ii]):
                        rbStatus = False
                        break
            #
            ok = self.__close(cObj)
            if readBackCheck and not rbStatus:
                return False
            return len(rIdL) == len(dList)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __getLoadInfo(self, contentType):
        sd = None
        dbName = None
        collectionName = None
        inputPathList = []
        tableIdExcludeList = []
        try:
            if contentType == "bird":
                sd = BirdSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedDatabaseName()
                inputPathList = self.__getPrdPathList()
            elif contentType == "bird-family":
                sd = BirdSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = "family_v4_0_1"
                inputPathList = self.__getPrdFamilyPathList()
            elif contentType == 'chem-comp':
                sd = ChemCompSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedDatabaseName()
                inputPathList = self.__getChemCompPathList()
            elif contentType == 'pdbx':
                sd = PdbxSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedDatabaseName()
                # inputPathList = self.__makePdbxPathListInLine(cachePath=self.__pdbxFileCache)
                inputPathList = self.__getPdbxPathList(self.__pdbxLoadListPath, cachePath=self.__pdbxFileCache)
                tableIdExcludeList = self.__pdbxTableIdExcludeList
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        if self.__fileLimit:
            inputPathList = inputPathList[:self.__fileLimit]

        return sd, dbName, collectionName, inputPathList, tableIdExcludeList


# -------------

    def __getPrdPathList(self):
        """Test case -  get the path list of PRD definitions in the CVS repository.
        """
        try:
            refIo = PdbxPrdIo(verbose=self.__verbose)
            refIo.setCachePath(self.__birdCachePath)
            loadPathList = refIo.makeDefinitionPathList()
            logger.debug("Length of BIRD file path list %d (limit %r)" % (len(loadPathList), self.__fileLimit))
            if self.__fileLimit:
                return loadPathList[:self.__fileLimit]
            else:
                return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

    def __getPrdFamilyPathList(self):
        """Test case -  get the path list of PRD Family definitions in the CVS repository.
        """
        try:
            refIo = PdbxFamilyIo(verbose=self.__verbose)
            refIo.setCachePath(self.__birdFamilyCachePath)
            loadPathList = refIo.makeDefinitionPathList()
            logger.debug("Length of BIRD FAMILY file path list %d (limit %r)" % (len(loadPathList), self.__fileLimit))
            if self.__fileLimit:
                return loadPathList[:self.__fileLimit]
            else:
                return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

    def __getChemCompPathList(self):
        """Test case -  get the path list of definitions in the CVS repository.
        """
        try:
            refIo = PdbxChemCompIo(verbose=self.__verbose)
            refIo.setCachePath(self.__ccCachePath)
            loadPathList = refIo.makeComponentPathList()
            logger.debug("Length of CCD file path list %d (limit %r)" % (len(loadPathList), self.__fileLimit))
            if self.__fileLimit:
                return loadPathList[:self.__fileLimit]
            else:
                return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

    def __getPdbxPathList(self, fileListPath, cachePath):
        pathList = self.__readPathList(fileListPath)
        if len(pathList) < 1:
            ok = self.__makePdbxPathList(fileListPath, cachePath)
            if ok:
                pathList = self.__readPathList(fileListPath)
            else:
                pathList = []
        return pathList

    def __readPathList(self, fileListPath):
        pathList = []
        try:
            with open(fileListPath, 'r') as ifh:
                for line in ifh:
                    pth = str(line[:-1]).strip()
                    pathList.append(pth)
        except Exception as e:
            pass
        logger.info("Reading path list length %d" % len(pathList))
        return pathList

    def __makePdbxPathList(self, fileListPath, cachePath=None):
        """ Return the list of pdbx file paths in the current repository and store this

        """
        try:
            with open(fileListPath, 'w') as ofh:
                for root, dirs, files in scandir.walk(cachePath, topdown=False):
                    if "REMOVE" in root:
                        continue
                    for name in files:
                        if name.endswith(".cif") and len(name) == 8:
                            ofh.write("%s\n" % os.path.join(root, name))
                #
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def __makePdbxPathListInLine(self, cachePath=None):
        """ Return the list of pdbx file paths in the current repository.
        """
        pathList = []
        try:
            for root, dirs, files in scandir.walk(cachePath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) == 8:
                        pathList.append(os.path.join(root, name))

            logger.debug("\nFound %d files in %s\n" % (len(pathList), cachePath))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return pathList
