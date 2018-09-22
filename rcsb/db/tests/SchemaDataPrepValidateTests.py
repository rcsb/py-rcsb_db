##
# File:    SchemaDataPrepValidateTests.py
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


import logging
import os
import time
import unittest

from jsonschema import Draft4Validator

from rcsb.db.define.DictMethodRunner import DictMethodRunner
from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.db.helpers.DictInfoHelper import DictInfoHelper
from rcsb.db.helpers.DictMethodRunnerHelper import DictMethodRunnerHelper
from rcsb.db.helpers.SchemaDefHelper import SchemaDefHelper
from rcsb.db.helpers.SchemaDocumentHelper import SchemaDocumentHelper
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


class SchemaDataPrepValidateTests(unittest.TestCase):

    def setUp(self):
        self.__numProc = 2
        self.__fileLimit = 200
        mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        self.__workPath = os.path.join(HERE, 'test-output')
        configPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'config', 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName, mockTopPath=mockTopPath)
        self.__mU = MarshalUtil(workPath=self.__workPath)

        self.__schU = SchemaDefUtil(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, workPath=self.__workPath)
        self.__birdRepoPath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'MOCK_BIRD_REPO')
        #
        self.__fTypeRow = "drop-empty-attributes|drop-empty-tables|skip-max-width|convert-iterables|normalize-enums"
        self.__fTypeCol = "drop-empty-tables|skip-max-width|convert-iterables|normalize-enums"
        self.__chemCompMockLen = 4
        self.__birdMockLen = 4
        self.__pdbxMockLen = 8
        self.__verbose = True
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'mmcif_pdbx_v5_next.dic')
        self.__pathRcsbDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'rcsb_mmcif_ext_v1.dic')
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testValidateOptsStrict(self):
        enforceOpts = "mandatoryKeys|mandatoryAttributes|bounds|enums"
        eCount = self.__testValidateOpts(enforceOpts=enforceOpts)
        logger.info("Total validation errors enforcing %s : %d" % (enforceOpts, eCount))
        self.assertGreaterEqual(eCount, 20)

    def testValidateOptsMin(self):
        enforceOpts = "mandatoryKeys|enums"
        eCount = self.__testValidateOpts(enforceOpts=enforceOpts)
        logger.info("Total validation errors enforcing %s : %d" % (enforceOpts, eCount))
        self.assertTrue(eCount <= 1)

    def __testValidateOpts(self, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):
        schemaNames = ['pdbx', 'pdbx_core', 'chem_comp', 'bird', 'bird_family']
        collectionNames = {'pdbx': ['pdbx_v5_0_2', 'pdbx_ext_v5_0_2'],
                           'pdbx_core': ['pdbx_core_entity_v5_0_2', 'pdbx_core_entry_v5_0_2', 'pdbx_core_assembly_v5_0_2'],
                           'bird': ['bird_v5_0_2'],
                           'bird_family': ['family_v5_0_2'],
                           'chem_comp': ['chem_comp_v5_0_2'],
                           'bird_chem_comp': ['bird_chem_comp_v5_0_2']}
        #
        eCount = 0
        for schemaName in schemaNames:
            for collectionName in collectionNames[schemaName]:
                cD = self.__testBuildJson(schemaName, collectionName, enforceOpts=enforceOpts)
                dL, cnL = self.__testPrepDocumentsFromContainers(schemaName, collectionName, styleType="rowwise_by_name_with_cardinality")
                # Raises exceptions for schema compliance.
                Draft4Validator.check_schema(cD)
                #
                v = Draft4Validator(cD)
                for ii, d in enumerate(dL):
                    logger.debug("Schema %s collection %s document %d" % (schemaName, collectionName, ii))
                    try:
                        cCount = 0
                        for error in sorted(v.iter_errors(d), key=str):
                            logger.debug("schema %s collection %s (%s) path %s error: %s" % (schemaName, collectionName, cnL[ii], error.path, error.message))
                            eCount += 1
                            cCount += 1
                        #
                        logger.debug("schema %s collection %s container %s count %d" % (schemaName, collectionName, cnL[ii], cCount))
                    except Exception as e:
                        logger.exception("Validation error %s" % str(e))

        return eCount

    def __testBuildJson(self, schemaName, collectionName, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):
        try:
            contentType = schemaName[:4] if schemaName.startswith('pdbx') else schemaName
            instDataTypeFilePath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'data_type_info', 'scan-%s-type-map.json' % contentType)
            appDataTypeFilePath = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'data_type_info', 'app_data_type_mapping.cif')
            #
            pathSchemaDefJson1 = os.path.join(HERE, 'test-output', 'json-schema-%s.json' % (collectionName))
            #
            dictInfoHelper = DictInfoHelper()
            defHelper = SchemaDefHelper()
            docHelper = SchemaDocumentHelper()
            #
            smb = SchemaDefBuild(schemaName,
                                 dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile],
                                 instDataTypeFilePath=instDataTypeFilePath,
                                 appDataTypeFilePath=appDataTypeFilePath,
                                 dictHelper=dictInfoHelper,
                                 schemaDefHelper=defHelper,
                                 documentDefHelper=docHelper)
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

    def __testPrepDocumentsFromContainers(self, schemaName, collectionName, styleType="rowwise_by_name_with_cardinality"):
        """Test case -  create loadable PDBx data from repository files
        """
        try:
            inputPathList = self.__schU.getPathList(contentType=schemaName)
            sd, _, _, _ = self.__schU.getSchemaInfo(contentType=schemaName)
            #
            dH = DictMethodRunnerHelper()
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
            tableDataDictList, containerNameList, rejectList = sdp.processDocuments(containerList, styleType=styleType,
                                                                                    filterType=self.__fTypeRow, dataSelectors=["PUBLIC_RELEASE"],
                                                                                    sliceFilter=sliceFilter)

            fp = os.path.join(HERE, "test-output", "export-%s-%s-prep-rowwise-by-name-with-cardinality.json" % (schemaName, collectionName))
            self.__mU.doExport(fp, tableDataDictList, format="json", indent=3)
            logger.debug("Exported %r" % fp)
            return tableDataDictList, containerNameList

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def schemaBuildJsonSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDataPrepValidateTests("testValidateOptsStrict"))
    suiteSelect.addTest(SchemaDataPrepValidateTests("testValidateOptsMin"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = schemaBuildJsonSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
