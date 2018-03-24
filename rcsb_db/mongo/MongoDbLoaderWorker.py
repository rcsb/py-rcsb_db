##
# File:    MongoDbLoaderWorker.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#     20-Mar-2018 jdw  adding prdcc within chemical component collection
#     21-Mar-2018 jdw  content filtering options added from documents
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
import pickle
import bson

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
from rcsb_db.utils.RepoPathUtil import RepoPathUtil

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
        self.__birdRepoPath = self.__cu.get('BIRD_REPO_PATH')
        self.__birdFamilyRepoPath = self.__cu.get('BIRD_FAMILY_REPO_PATH')
        self.__birdChemCompRepoPath = self.__cu.get('BIRD_CHEM_COMP_REPO_PATH')
        self.__chemCompRepoPath = self.__cu.get('CHEM_COMP_REPO_PATH')
        self.__pdbxFileRepo = self.__cu.get('RCSB_PDBX_SANBOX_PATH')
        #
        self.__readBackCheck = readBackCheck
        #
        self.__prefD = self.__assignPreferences(self.__cu)

    def loadContentType(self, contentType, loadType='full', inputPathList=None, styleType='rowwise_by_name', documentSelectors=None,
                        failedFilePath=None, saveInputFileListPath=None):
        """  Driver method for loading MongoDb content -

            contentType:  one of 'bird','bird-family','bird-chem-comp', chem-comp','pdbx', 'pdbx-ext'
            loadType:     "full" or "replace"
            styleType:    one of 'rowwise_by_name', 'columnwise_by_name', 'rowwise_no_name', 'rowwise_by_name_with_cardinality'

        """
        try:
            startTime = self.__begin(message="loading operation")
            sd, dbName, collectionName, pathList, tableIdIncludeList, tableIdExcludeList = self.__getLoadInfo(contentType, inputPathList=inputPathList)
            #
            if saveInputFileListPath:
                self.__writePathList(saveInputFileListPath, pathList)
                logger.info("Saving %d paths in %s" % (len(pathList), saveInputFileListPath))
            #
            logger.debug("contentType %s dbName %s collectionName %s" % (contentType, dbName, collectionName))
            logger.debug("contentType %s include List %r" % (contentType, tableIdIncludeList))
            logger.debug("contentType %s exclude List %r" % (contentType, tableIdExcludeList))

            #
            optD = {}
            optD['sd'] = sd
            optD['dbName'] = dbName
            optD['collectionName'] = collectionName
            optD['styleType'] = styleType
            optD['tableIdExcludeList'] = tableIdExcludeList
            optD['tableIdIncludeList'] = tableIdIncludeList
            optD['prefD'] = self.__prefD
            optD['readBackCheck'] = self.__readBackCheck
            optD['logSize'] = self.__verbose
            optD['documentSelectors'] = documentSelectors
            optD['loadType'] = loadType
            #
            if loadType == 'full':
                self.__removeCollection(dbName, collectionName, self.__prefD)
                self.__createCollection(dbName, collectionName, self.__prefD)
            #
            numProc = self.__numProc
            chunkSize = self.__chunkSize if inputPathList and self.__chunkSize < len(inputPathList) else 0
            #
            mpu = MultiProcUtil(verbose=True)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="loadWorker")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=pathList, numProc=numProc, numResults=1, chunkSize=chunkSize)
            logger.debug("Failing path list %r" % failList)
            logger.info("Load path list length %d failed load list length %d" % (len(pathList), len(failList)))
            #
            if failedFilePath and len(failList) > 0:
                wOk = self.__writePathList(failedFilePath, failList)
                logger.info("Writing load failure path list to %s status %r" % (failedFilePath, wOk))
            #
            self.__end(startTime, "loading operation with status " + str(ok))

            #
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def loadWorker(self, dataList, procName, optionsD, workingDir):
        """ Multi-proc worker method for MongoDb loading -
        """
        try:
            startTime = self.__begin(message=procName)
            sd = optionsD['sd']
            styleType = optionsD['styleType']
            tableIdExcludeList = optionsD['tableIdExcludeList']
            tableIdIncludeList = optionsD['tableIdIncludeList']
            dbName = optionsD['dbName']
            collectionName = optionsD['collectionName']
            prefD = optionsD['prefD']
            readBackCheck = optionsD['readBackCheck']
            logSize = 'logSize' in optionsD and optionsD['logSize']
            documentSelectors = optionsD['documentSelectors']
            loadType = optionsD['loadType']
            #
            sdp = SchemaDefDataPrep(schemaDefObj=sd, verbose=self.__verbose)
            sdp.setTableIdExcludeList(tableIdExcludeList)
            sdp.setTableIdIncludeList(tableIdIncludeList)
            fType = "drop-empty-attributes|drop-empty-tables|skip-max-width|assign-dates|convert-iterables"
            if styleType in ["columnwise_by_name", "rowwise_no_name"]:
                fType = "drop-empty-tables|skip-max-width|assign-dates|convert-iterables"
            tableDataDictList, containerNameList, rejectList = sdp.fetchDocuments(dataList, styleType=styleType, filterType=fType, documentSelectors=documentSelectors)
            #
            if logSize:
                maxDocumentMegaBytes = -1
                for tD, cN in zip(tableDataDictList, containerNameList):
                    # documentMegaBytes = float(sys.getsizeof(pickle.dumps(tD, protocol=0))) / 1000000.0
                    documentMegaBytes = float(sys.getsizeof(bson.BSON.encode(tD))) / 1000000.0
                    logger.debug("Document %s  %.4f MB" % (cN, documentMegaBytes))
                    maxDocumentMegaBytes = max(maxDocumentMegaBytes, documentMegaBytes)
                    if documentMegaBytes > 15.8:
                        logger.info("Large document %s  %.4f MB" % (cN, documentMegaBytes))
                logger.info("Maximum document size loaded %.4f MB" % maxDocumentMegaBytes)
            #
            #  Get the tableId.attId holding the natural document Id
            docIdD = {}
            docIdD['tableName'], docIdD['attributeName'] = sd.getDocumentKeyAttributeName(collectionName)
            logger.debug("docIdD %r collectionName %r" % (docIdD, collectionName))
            #
            ok, successPathList = self.__loadDocuments(dbName, collectionName, prefD, tableDataDictList, docIdD,
                                                       loadType=loadType, successKey='__INPUT_PATH__', readBackCheck=readBackCheck)
            #
            logger.info("%s SuccessList length = %d  rejected %d" % (procName, len(successPathList), len(rejectList)))
            successPathList.extend(rejectList)
            successPathList = list(set(successPathList))
            self.__end(startTime, procName + " with status " + str(ok))

            return successPathList, [], []

        except Exception as e:
            logger.error("Failing with dataList %r" % dataList)
            logger.exception("Failing with %s" % str(e))

        return [], [], []

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

    def __assignPreferences(self, cfgObj, dbType="mongodb"):
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

        prefD = {"DB_HOST": dbHost, 'DB_USER': dbUserId, 'DB_PW': dbUserPwd, 'DB_NAME': dbName, "DB_PORT": dbPort, 'DB_ADMIN_DB_NAME': dbAdminDb}
        return prefD

    def __begin(self, message=""):
        startTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Running application version %s" % __version__)
        logger.debug("Starting %s at %s" % (message, ts))
        return startTime

    def __end(self, startTime, message=""):
        endTime = time.time()
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        delta = endTime - startTime
        logger.info("Completed %s at %s (%.4f seconds)\n" % (message, ts, delta))

    def __open(self, prefD):
        cObj = ConnectionBase()
        cObj.setPreferences(prefD)
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

    def __createCollection(self, dbName, collectionName, prefD):
        """Create database and collection -
        """
        try:
            cObj = self.__open(prefD)
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

    def __removeCollection(self, dbName, collectionName, prefD):
        """Drop collection within database

        """
        try:
            cObj = self.__open(prefD)
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

    def __loadDocuments(self, dbName, collectionName, prefD, dList, docIdD, loadType='full', successKey=None, readBackCheck=False):
        #
        # Create index mapping documents in input list to the natural document identifier.
        indD = {}
        try:
            for ii, d in enumerate(dList):
                tn = docIdD['tableName']
                an = docIdD['attributeName']
                dId = d[tn][an]
                indD[dId] = ii
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        try:
            cObj = self.__open(prefD)
            client = self.__getClientConnection(cObj)
            mg = MongoDbUtil(client)
            #
            keyName = docIdD['tableName'] + '.' + docIdD['attributeName']
            if loadType == 'replace':
                dTupL = mg.deleteList(dbName, collectionName, dList, keyName)
                logger.debug("Deleted document status %r" % dTupL)

            rIdL = mg.insertList(dbName, collectionName, dList, keyName)
            #
            #  If there is a failure then determine the success list -
            #
            successList = [d[successKey] for d in dList]
            if len(rIdL) != len(dList):
                successList = []
                for rId in rIdL:
                    rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                    docId = rObj[docIdD['tableName']][docIdD['attributeName']]
                    jj = indD[docId]
                    successList.append(dList[jj][successKey])
            #
            if readBackCheck:
                #
                # Note that objects in dList are mutated by additional key '_id' that is added on insert -
                #
                rbStatus = True
                for ii, rId in enumerate(rIdL):
                    rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                    docId = rObj[docIdD['tableName']][docIdD['attributeName']]
                    jj = indD[docId]
                    if (rObj != dList[jj]):
                        rbStatus = False
                        break
            #
            ok = self.__close(cObj)
            if readBackCheck and not rbStatus:
                return False, successList
            #
            return len(rIdL) == len(dList), successList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False, []

    def __getLoadInfo(self, contentType, inputPathList=None):
        sd = None
        dbName = None
        collectionName = None
        inputPathList = inputPathList if inputPathList else []
        tableIdExcludeList = []
        tableIdIncludeList = []
        rpU = RepoPathUtil(fileLimit=self.__fileLimit)
        try:
            if contentType == "bird":
                sd = BirdSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedCollection("bird_v")
                outputPathList = inputPathList if inputPathList else rpU.getPrdPathList(self.__birdRepoPath)
            elif contentType == "bird-family":
                sd = BirdSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedCollection("family_v")
                outputPathList = inputPathList if inputPathList else rpU.getPrdFamilyPathList(self.__birdFamilyRepoPath)
            elif contentType == 'chem-comp':
                sd = ChemCompSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedCollection("chem_comp_v")
                outputPathList = inputPathList if inputPathList else rpU.getChemCompPathList(self.__chemCompRepoPath)
            elif contentType == 'bird-chem-comp':
                sd = ChemCompSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedCollection("bird_chem_comp_v")
                outputPathList = inputPathList if inputPathList else rpU.getPrdCCPathList(self.__birdChemCompRepoPath)
            elif contentType == 'pdbx':
                sd = PdbxSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedCollection("pdbx_v")
                outputPathList = inputPathList if inputPathList else rpU.getEntryPathList(self.__pdbxFileRepo)
                tableIdExcludeList = sd.getCollectionExcludedTables(collectionName)
                tableIdIncludeList = sd.getCollectionSelectedTables(collectionName)
            elif contentType == 'pdbx-ext':
                sd = PdbxSchemaDef(convertNames=True)
                dbName = sd.getDatabaseName()
                collectionName = sd.getVersionedCollection("pdbx_ext_v")
                outputPathList = inputPathList if inputPathList else rpU.getEntryPathList(self.__pdbxFileRepo)
                tableIdExcludeList = sd.getCollectionExcludedTables(collectionName)
                tableIdIncludeList = sd.getCollectionSelectedTables(collectionName)
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        if self.__fileLimit:
            inputPathList = inputPathList[:self.__fileLimit]

        return sd, dbName, collectionName, outputPathList, tableIdIncludeList, tableIdExcludeList
