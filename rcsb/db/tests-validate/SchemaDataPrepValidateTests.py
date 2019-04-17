##
# File:    SchemaDataPrepValidateTests.py
# Author:  J. Westbrook
# Date:    9-Jun-2018
# Version: 0.001
#
# Update:
#  7-Sep-2018 jdw add multi-level (strict/min) validation tests
# 29-Sep-2018 jdw add plugin for extended checks of JSON Schema formats.
# 31-Mar-2019 jdw add option to validate  'addParentRefs'
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

from jsonschema import Draft4Validator, FormatChecker

from rcsb.db.define.DictMethodRunner import DictMethodRunner
from rcsb.db.helpers.DictMethodRunnerHelper import DictMethodRunnerHelper
from rcsb.db.processors.DataTransformFactory import DataTransformFactory
from rcsb.db.processors.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaDataPrepValidateTests(unittest.TestCase):

    def setUp(self):
        self.__numProc = 2
        # self.__fileLimit = 200
        self.__fileLimit = None
        self.__mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        self.__workPath = os.path.join(HERE, 'test-output')
        self.__configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.yml')
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=self.__configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__mU = MarshalUtil(workPath=self.__workPath)

        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath)
        self.__birdRepoPath = self.__cfgOb.getPath('BIRD_REPO_PATH', sectionName=configName)
        #
        self.__fTypeRow = "drop-empty-attributes|drop-empty-tables|skip-max-width|convert-iterables|normalize-enums|translateXMLCharRefs"
        self.__fTypeCol = "drop-empty-tables|skip-max-width|convert-iterables|normalize-enums|translateXMLCharRefs"
        self.__chemCompMockLen = 8
        self.__birdMockLen = 4
        self.__pdbxMockLen = 8
        self.__verbose = True
        #
        self.__pathPdbxDictionaryFile = self.__cfgOb.getPath('PDBX_DICT_LOCATOR', sectionName=configName)
        self.__pathRcsbDictionaryFile = self.__cfgOb.getPath('RCSB_DICT_LOCATOR', sectionName=configName)
        self.__drugBankMappingFile = self.__cfgOb.getPath('DRUGBANK_MAPPING_LOCATOR', sectionName=configName)
        self.__csdModelMappingFile = self.__cfgOb.getPath('CCDC_MAPPING_LOCATOR', sectionName=configName)
        #
        # self.__pathTaxonomyMappingFile = self.__cfgOb.getPath('NCBI_TAXONOMY_LOCATOR', sectionName=configName)
        #
        self.__pathTaxonomyData = self.__cfgOb.getPath('NCBI_TAXONOMY_PATH', sectionName=configName)
        self.__pathEnzymeData = self.__cfgOb.getPath('ENZYME_CLASSIFICATION_DATA_PATH', sectionName=configName)
        self.__structDomainDataPath = self.__cfgOb.getPath('STRUCT_DOMAIN_CLASSIFICATION_DATA_PATH', sectionName=configName)
        #
        self.__testDirPath = os.path.join(HERE, "test-output", 'pdbx-fails')
        self.__testIhmDirPath = os.path.join(HERE, "test-output", 'ihm-files')
        self.__exportJson = True
        #
        # self.__extraOpts = 'addParentRefs'
        self.__extraOpts = None
        # The following for extended parent/child info -
        # self.__extraOpts = 'addParentRefs|addPrimaryKey'
        self.__allSchemaNameD = {'ihm_dev': ['ihm_dev'],
                                 'pdbx': ['pdbx', 'pdbx_ext'],
                                 'pdbx_core': ['pdbx_core_entity_monomer', 'pdbx_core_entity', 'pdbx_core_entry', 'pdbx_core_assembly', 'pdbx_core_entity_instance', ],
                                 'bird': ['bird'],
                                 'bird_family': ['family'],
                                 'chem_comp': ['chem_comp'],
                                 'chem_comp_core': ['chem_comp_core'],
                                 'bird_chem_comp': ['bird_chem_comp'],
                                 'bird_chem_comp_core': ['bird_chem_comp_core'],
                                 }
        self.__schemaNameD = {
            'pdbx_core': [
                'pdbx_core_entity',
                'pdbx_core_entry',
                'pdbx_core_assembly',
                'pdbx_core_entity_instance',
                'pdbx_core_entity_monomer',
                'pdbx_core_entity_instance_validation']}
        # self.__schemaNameD = {'pdbx_core': ['pdbx_core_entity_instance_validation']}
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testValidateOptsRepo(self):
        schemaLevel = 'min'
        inputPathList = None
        eCount = self.__testValidateOpts(schemaNameD=self.__schemaNameD, inputPathList=inputPathList, schemaLevel=schemaLevel, mergeContentTypes=['vrpt'])
        logger.info("Total validation errors schema level %s : %d" % (schemaLevel, eCount))
        # self.assertGreaterEqual(eCount, 20)

    def testValidateOptsList(self):
        schemaLevel = 'min'
        inputPathList = glob.glob(self.__testDirPath + "/*.cif")
        if not inputPathList:
            self.assertTrue(True)
            return True
        schemaNameD = {'pdbx_core': ['pdbx_core_entity', 'pdbx_core_entry']}
        eCount = self.__testValidateOpts(schemaNameD=schemaNameD, inputPathList=inputPathList, schemaLevel=schemaLevel, mergeContentTypes=['vrpt'])
        logger.info("Total validation errors schema level %s : %d" % (schemaLevel, eCount))
        # self.assertGreaterEqual(eCount, 20)

    def testValidateOptsIhmList(self):
        schemaLevel = 'full'
        inputPathList = glob.glob(self.__testIhmDirPath + "/*.cif")
        if not inputPathList:
            self.assertTrue(True)
            return True
        schemaNameD = {'ihm_dev_full': ['ihm_dev_full']}
        eCount = self.__testValidateOpts(schemaNameD=schemaNameD, inputPathList=inputPathList, schemaLevel=schemaLevel, mergeContentTypes=None)
        logger.info("Total validation errors schema level %s : %d" % (schemaLevel, eCount))
        # self.assertGreaterEqual(eCount, 20)
        #

    def __testValidateOpts(self, schemaNameD, inputPathList=None, schemaLevel='full', mergeContentTypes=None):
        #
        eCount = 0
        for schemaName in schemaNameD:
            _ = self.__schU.makeSchemaDef(schemaName, dataTyping='ANY', saveSchema=True, altDirPath=self.__workPath)
            pthList = inputPathList if inputPathList else self.__schU.getLocatorObjList(contentType=schemaName, mergeContentTypes=mergeContentTypes)
            for collectionName in schemaNameD[schemaName]:
                cD = self.__schU.makeSchema(
                    schemaName,
                    collectionName,
                    schemaType='JSON',
                    level=schemaLevel,
                    saveSchema=True,
                    altDirPath=self.__workPath,
                    extraOpts=self.__extraOpts)
                #
                dL, cnL = self.__testPrepDocumentsFromContainers(pthList, schemaName, collectionName, styleType="rowwise_by_name_with_cardinality")
                # Raises exceptions for schema compliance.
                try:
                    Draft4Validator.check_schema(cD)
                except Exception as e:
                    logger.error("%s %s schema validation fails with %s" % (schemaName, collectionName, str(e)))
                #
                v = Draft4Validator(cD, format_checker=FormatChecker())
                for ii, d in enumerate(dL):
                    logger.debug("Schema %s collection %s document %d" % (schemaName, collectionName, ii))
                    try:
                        cCount = 0
                        for error in sorted(v.iter_errors(d), key=str):
                            logger.info("schema %s collection %s (%s) path %s error: %s" % (schemaName, collectionName, cnL[ii], error.path, error.message))
                            logger.debug("Failing document %d : %r" % (ii, list(d.items())))
                            eCount += 1
                            cCount += 1
                        if cCount > 0:
                            logger.info("schema %s collection %s container %s error count %d" % (schemaName, collectionName, cnL[ii], cCount))
                    except Exception as e:
                        logger.exception("Validation processing error %s" % str(e))

        return eCount

    def __testPrepDocumentsFromContainers(self, inputPathList, schemaName, collectionName, styleType="rowwise_by_name_with_cardinality"):
        """Test case -  create loadable PDBx data from repository files
        """
        try:

            sd, _, _, _ = self.__schU.getSchemaInfo(contentType=schemaName, altDirPath=self.__workPath)
            #
            dH = DictMethodRunnerHelper(drugBankMappingFilePath=self.__drugBankMappingFile, workPath=self.__workPath,
                                        csdModelMappingFilePath=self.__csdModelMappingFile,
                                        enzymeDataPath=self.__pathEnzymeData,
                                        taxonomyDataPath=self.__pathTaxonomyData,
                                        structDomainDataPath=self.__structDomainDataPath)
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
            docList = sdp.addDocumentSubCategoryAggregates(docList, collectionName)
            #
            if self.__exportJson:
                for ii, doc in enumerate(docList):
                    cn = containerNameList[ii]
                    fp = os.path.join(HERE, "test-output", "%s-%s-%s-prep-rowwise-by-name-with-cardinality.json" % (cn, schemaName, collectionName))
                    self.__mU.doExport(fp, [doc], format="json", indent=3)
                    logger.debug("Exported %r" % fp)
            return docList, containerNameList

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def schemaValidateSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDataPrepValidateTests("testValidateOptsRepo"))
    #
    # suiteSelect.addTest(SchemaDataPrepValidateTests("testValidateOptsList"))
    return suiteSelect


def schemaIhmValidateSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDataPrepValidateTests("testValidateOptsIhmList"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = schemaValidateSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if False:
        mySuite = schemaIhmValidateSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
