##
# File:    SchemaDefLoaderMongoDbTests.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#
##
"""
Tests for creating and loading MongoDb using BIRD, CCD and PDBx/mmCIF data files
and following external schema definitions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import os
import time
import unittest
import scandir
import pprint

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
from rcsb_db.schema.DaInternalSchemaDef import DaInternalSchemaDef

from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo
from mmcif_utils.bird.PdbxFamilyIo import PdbxFamilyIo
from mmcif_utils.chemcomp.PdbxChemCompIo import PdbxChemCompIo

from rcsb_db.mongo.ConnectionBase import ConnectionBase
from rcsb_db.mongo.MongoDbUtil import MongoDbUtil



class SchemaDefLoaderMongoDbTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(SchemaDefLoaderMongoDbTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        # Limit the load length of each file type for testing  -  Set to None to remove -
        self.__fileLimit = None
        #
        # These are mocks of the repository/sandbox organizations for reference and structure data sets.
        #
        self.__birdCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_REPO")
        self.__birdFamilyCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_BIRD_FAMILY_REPO")
        self.__ccCachePath = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_CHEM_COMP_REPO")
        self.__pdbxFileCache = os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_PDBX_SANDBOX")
        #
        # Local model files for testing -
        #
        self.__pdbxPath = os.path.join(TOPDIR, "rcsb_db", "data")
        self.__pdbxFileList = ['1cbs.cif', '1o3q.cif', '1xbb.cif', '3of4.cif', '3oqp.cif', '3rer.cif', '3rij.cif', '5hoh.cif']
        #
        self.__tableIdExcludeList = ['ATOM_SITE','ATOM_SITE_ANISOTROP']

        dbUserId  = os.getenv("MONGO_DB_USER_NAME")
        dbUserPwd = os.getenv("MONGO_DB_PASSWORD")
        dbName = os.getenv("MONGO_DB_NAME")
        dbHost = os.getenv("MONGO_DB_HOST")
        dbPort = os.getenv("MONGO_DB_PORT")
        self.__myC = None
        ok = self.__open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName, dbPort=dbPort)
        self.assertTrue(ok)
        #
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        self.__close()
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testLoadChemCompReference(self):
        """ Test case -  Load chemical component reference data
        """
        try:
            ok = self.__loadContentType('chem-comp')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdReference(self):
        """ Test case -  Load Bird reference data
        """
        try:
            ok = self.__loadContentType('bird')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadBirdFamilyReference(self):
        """ Test case -  Load Bird family reference data
        """
        try:
            ok = self.__loadContentType('bird-family')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testLoadPdbxEntryData(self):
        """ Test case -  Load PDBx entry data
        """

        try:
            ok = self.__loadContentType('pdbx')
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    # -------------- -------------- -------------- -------------- -------------- -------------- --------------
    #                                        ---  Supporting code follows ---
    #
    def __open(self, dbUserId=None, dbUserPwd=None, dbHost=None, dbName=None, dbPort=None):
        authD = {"DB_HOST": dbHost, 'DB_USER': dbUserId, 'DB_PW': dbUserPwd, 'DB_NAME': dbName, "DB_PORT": dbPort}
        self.__myC = ConnectionBase()
        self.__myC.setAuth(authD)
        ok = self.__myC.openConnection()
        if ok:
            return True
        else:
            return False

    def __close(self):
        if self.__myC is not None:
            self.__myC.closeConnection()
            self.__myC = None
            return True
        else:
            return False

    def __getClientConnection(self):
        return self.__myC.getClientConnection()

    def __createCollection(self, dbName, collectionName):
        """Test case -  create database and collection -
        """
        try:
            client = self.__getClientConnection()
            mg = MongoDbUtil(client)
            ok = mg.createCollection(dbName, collectionName)
            self.assertTrue(ok)
            ok = mg.databaseExists(dbName)
            self.assertTrue(ok)
            ok = mg.collectionExists(dbName, collectionName)
            self.assertTrue(ok)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __removeCollection(self, dbName, collectionName):
        """drop collection within database

        """
        try:
            client = self.__getClientConnection()
            mg = MongoDbUtil(client)
            #
            logger.debug("Databases = %r" % mg.getDatabaseNames())
            logger.debug("Collections = %r" % mg.getCollectionNames(dbName))
            ok = mg.dropCollection(dbName, collectionName)
            self.assertTrue(ok)
            logger.debug("Databases = %r" % mg.getDatabaseNames())
            logger.debug("Collections = %r" % mg.getCollectionNames(dbName))
            #
            ok = mg.collectionExists(dbName, collectionName)
            self.assertFalse(ok)
            logger.debug("Collections = %r" % mg.getCollectionNames(dbName))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __loadDocuments(self, dbName, collectionName, dList):
        try:
            client = self.__getClientConnection()
            mg = MongoDbUtil(client)
            #
            rIdL = mg.insertList(dbName, collectionName, dList)
            self.assertTrue(len(rIdL), len(dList))
            #
            # Note that objects in dList are mutated by additional key '_id' that is added on insert -
            #
            for ii, rId in enumerate(rIdL):
                rObj = mg.fetchOne(dbName, collectionName, '_id', rId)
                # logger.debug("Return Object %s" % pprint.pformat(rObj, indent=3))
                self.assertEqual(len(dList[ii]), len(rObj))
                self.assertEqual(dList[ii], rObj)
                logger.debug("Object fetch compare success for %r" % rId)
            return True
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
                inputPathList = self.__makePdbxPathListInLine(cachePath=self.__pdbxFileCache)
                tableIdExcludeList = self.__tableIdExcludeList
            else:
                logger.warning("Unsupported contentType %s" % contentType)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return sd, dbName, collectionName, inputPathList, tableIdExcludeList

    def __loadContentType(self, contentType, styleType='rowwise_by_name'):

        try:
            sd, dbName, collectionName, inputPathList, tableIdExcludeList = self.__getLoadInfo(contentType)
            sdp = SchemaDefDataPrep(schemaDefObj=sd, verbose=self.__verbose)
            sdp.setTableIdExcludeList(tableIdExcludeList)
            tableDataDictList, containerNameList = sdp.fetchDocuments(inputPathList, styleType=styleType)
            self.__removeCollection(dbName, collectionName)
            self.__createCollection(dbName, collectionName)
            ok = self.__loadDocuments(dbName, collectionName, tableDataDictList)
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()
        return False

    def __getPdbxPathList(self):
        """Test case -  get the path list of PDBx instance example files -
        """

        try:
            loadPathList = [os.path.join(self.__pdbxPath, v) for v in self.__pdbxFileList]
            logger.debug("Length of PDBx file path list %d (limit %r)" % (len(loadPathList), self.__fileLimit))
            if self.__fileLimit:
                return loadPathList[:self.__fileLimit]
            else:
                return loadPathList
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

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
            self.fail()

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
            self.fail()

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
            self.fail()

    def __makePdbxPathList(self, fp, cachePath=os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_PDBX_SANDBOX")):
        """ Return the list of pdbx file paths in the current repository.
        """

        try:
            with open(fp, 'w') as ofh:
                for root, dirs, files in scandir.walk(cachePath, topdown=False):
                    if "REMOVE" in root:
                        continue
                    for name in files:
                        if name.endswith(".cif") and len(name) == 8:
                            ofh.write("%s\n" % os.path.join(root, name))
                #
                # logger.info("\nFound %d files in %s\n" % (len(pathList), cachePath))
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return False

    def __makePdbxPathListInLine(self, cachePath=os.path.join(TOPDIR, "rcsb_db", "data", "MOCK_PDBX_SANDBOX")):
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

def mongoLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderMongoDbTests("testLoadChemCompReference"))
    suiteSelect.addTest(SchemaDefLoaderMongoDbTests("testLoadBirdReference"))
    suiteSelect.addTest(SchemaDefLoaderMongoDbTests("testLoadBirdFamilyReference"))
    suiteSelect.addTest(SchemaDefLoaderMongoDbTests("testLoadPdbxEntryData"))
    return suiteSelect


if __name__ == '__main__':
    #
    if (True):
        mySuite = mongoLoadSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
