##
# File:    SchemaValidationListTests.py
# Author:  J. Westbrook
# Date:    9-Jun-2018
# Version: 0.001
#
# Update:
#  7-Sep-2018 jdw add multi-level (strict/min) validation tests
#
##
"""
Tests for utilities employed to construct local schema and json schema defintions from
dictionary metadata and user preference data, and to further apply these schema to
validate instance data.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import glob
import logging
import os
import time
import unittest

from jsonschema import Draft4Validator

from rcsb.db.define.DictMethodRunner import DictMethodRunner
from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.db.helpers.DictMethodRunnerHelper import DictMethodRunnerHelper
from rcsb.db.processors.DataTransformFactory import DataTransformFactory
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.IoUtil import IoUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaValidationListTests(unittest.TestCase):

    def setUp(self):
        self.__numProc = 2
        self.__fileLimit = None
        self.__mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        self.__workPath = os.path.join(HERE, 'test-output')
        self.__configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.yml')
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=self.__configPath, sectionName=configName, mockTopPath=self.__mockTopPath)
        self.__mU = MarshalUtil(workPath=self.__workPath)

        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath)
        self.__birdRepoPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'MOCK_BIRD_REPO')
        #
        self.__fTypeRow = "drop-empty-attributes|drop-empty-tables|skip-max-width|convert-iterables|normalize-enums"
        self.__fTypeCol = "drop-empty-tables|skip-max-width|convert-iterables|normalize-enums"
        self.__chemCompMockLen = 5
        self.__birdMockLen = 4
        self.__pdbxMockLen = 8
        self.__verbose = True
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'mmcif_pdbx_v5_next.dic')
        self.__pathRcsbDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'rcsb_mmcif_ext_v1.dic')
        #
        #
        self.__drugBankMappingFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'DrugBank', 'drugbank_pdb_mapping.json')
        self.__csdModelMappingFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'chem_comp_models', 'ccdc_pdb_mapping.json')
        #
        self.__pathTaxonomyMappingFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'NCBI', 'taxonomy_names.pic')

        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        self.__testDirPath = os.path.join(HERE, "test-output", 'pdbx-fails')
        # self.__testDirPath = os.path.join(HERE, "test-output", 'vfiles')

        # self.__testFileNames = ['2pjg.cif', '3ed8.cif', '3ryo.cif', '3s4r.cif', '3vzv.cif', '4jne.cif', '5hgq.cif', '5pgy.cif']
        # self.__testPathList = [os.path.join(self.__testFilePath, f) for f in self.__testFileNames]

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def specialTestValidateOptsStrict(self):
        enforceOpts = "mandatoryKeys|mandatoryAttributes|bounds|enums"
        schemaNameD = {'pdbx_core': ['pdbx_core_entity_v5_0_2', 'pdbx_core_entry_v5_0_2']}
        eCount = self.__testValidateOpts(schemaNameD, self.__testDirPath, enforceOpts=enforceOpts)
        logger.info("Total validation errors enforcing %s : %d" % (enforceOpts, eCount))
        self.assertEqual(eCount, 0)

    def specialTestValidateOptsMin(self):
        enforceOpts = "mandatoryKeys|enums"
        schemaNameD = {'pdbx_core': ['pdbx_core_entity_v5_0_2', 'pdbx_core_entry_v5_0_2']}
        eCount = self.__testValidateOpts(schemaNameD, self.__testDirPath, enforceOpts=enforceOpts)
        logger.info("Total validation errors enforcing %s : %d" % (enforceOpts, eCount))
        self.assertTrue(eCount <= 1)

    def __testValidateOpts(self, schemaNameD, testDirPath, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):
        #
        eCount = 0
        for schemaName in schemaNameD:
            for collectionName in schemaNameD[schemaName]:
                cD = self.__testBuildJson(schemaName, collectionName, enforceOpts=enforceOpts)
                dL, cnL = self.__testPrepDocumentsFromContainers(testDirPath, schemaName, collectionName, styleType="rowwise_by_name_with_cardinality")
                logger.info("Processed %d containers" % len(dL))
                # Raises exceptions for schema compliance.
                Draft4Validator.check_schema(cD)
                #
                v = Draft4Validator(cD)
                for ii, d in enumerate(dL):
                    logger.info("%s schema %s collection %s document %d" % (cnL[ii], schemaName, collectionName, ii))
                    try:
                        for error in sorted(v.iter_errors(d), key=str):
                            logger.info("schema %s collection %s (%s) path %s error: %s" % (schemaName, collectionName, cnL[ii], error.path, error.message))
                            eCount += 1
                        #
                    except Exception as e:
                        logger.exception("%s validation error %s" % (cnL[ii], str(e)))
        return eCount

    def __testBuildJson(self, schemaName, collectionName, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):
        try:
            pathSchemaDefJson1 = os.path.join(HERE, 'test-output', 'json-schema-%s.json' % (collectionName))
            #
            smb = SchemaDefBuild(schemaName, self.__configPath, mockTopPath=self.__mockTopPath)
            cD = smb.build(collectionName, applicationName='json', schemaType='json', enforceOpts=enforceOpts)
            #
            logger.debug("Schema dictionary category length %d" % len(cD['properties']))
            self.assertGreaterEqual(len(cD['properties']), 5)
            #
            ioU = IoUtil()
            ioU.serialize(pathSchemaDefJson1, cD, format='json', indent=3)
            return cD

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __testPrepDocumentsFromContainers(self, testDirPath, schemaName, collectionName, styleType="rowwise_by_name_with_cardinality"):
        """Test case -  create loadable PDBx data from repository files
        """
        try:

            inputPathList = glob.glob(testDirPath + "/*.cif")
            logger.info("Found %d files in test path %s" % (len(inputPathList), testDirPath))
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType=schemaName)
            #
            #
            dH = DictMethodRunnerHelper(drugBankMappingFilePath=self.__drugBankMappingFile, workPath=self.__workPath,
                                        csdModelMappingFilePath=self.__csdModelMappingFile,
                                        taxonomyMappingFilePath=self.__pathTaxonomyMappingFile)
            dmh = DictMethodRunner(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile], methodHelper=dH)

            #
            dtf = DataTransformFactory(schemaDefAccessObj=sd, filterType=self.__fTypeRow)
            sdp = SchemaDefDataPrep(schemaDefAccessObj=sd, dtObj=dtf, workPath=self.__workPath, verbose=self.__verbose)
            containerList = sdp.getContainerList(inputPathList)
            for container in containerList:
                cName = container.getName()
                logger.debug("Processing container %s" % cName)
                dmh.apply(container)
            #
            logger.debug("inputPathList %r" % inputPathList)
            tableIdExcludeList = sd.getCollectionExcluded(collectionName)
            tableIdIncludeList = sd.getCollectionSelected(collectionName)
            sliceFilter = sd.getCollectionSliceFilter(collectionName)
            sdp.setSchemaIdExcludeList(tableIdExcludeList)
            sdp.setSchemaIdIncludeList(tableIdIncludeList)
            #
            docList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType=styleType,
                                                                          filterType=self.__fTypeRow, dataSelectors=["PUBLIC_RELEASE"],
                                                                          sliceFilter=sliceFilter)

            docList = sdp.addDocumentPrivateAttributes(docList, collectionName)

            fp = os.path.join(HERE, "test-output", "export-%s-%s-prep-rowwise-by-name-with-cardinality.json" % (schemaName, collectionName))
            self.__mU.doExport(fp, docList, format="json", indent=3)
            logger.debug("Exported %r" % fp)
            return docList, containerNameList

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def schemaBuildJsonSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaValidationListTests("specialTestValidateOptsStrict"))
    suiteSelect.addTest(SchemaValidationListTests("specialTestValidateOptsMin"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = schemaBuildJsonSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
