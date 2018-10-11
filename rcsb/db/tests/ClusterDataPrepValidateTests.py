# File:    ClusterDataPrepValidateTests.py
# Author:  J. Westbrook
# Date:    7-Oct-2018
# Version: 0.001
#
# Update:
#
##
"""
Tests for processing and validating sequence cluster and related provenance data.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from jsonschema import Draft4Validator, FormatChecker

from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.db.processors.ClusterDataPrep import ClusterDataPrep
from rcsb.db.utils.ProvenanceUtil import ProvenanceUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.IoUtil import IoUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ClusterDataPrepValidateTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        #
        self.__mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data')
        self.__pathConfig = os.path.join(self.__mockTopPath, 'config', 'dbload-setup-example.cfg')
        self.__workPath = os.path.join(HERE, 'test-output')
        self.__updateId = '2018_25'
        #
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, mockTopPath=self.__mockTopPath)
        self.__sandboxPath = self.__cfgOb.getPath('RCSB_EXCHANGE_SANDBOX_PATH', sectionName='DEFAULT')
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'mmcif_pdbx_v5_next.dic')
        self.__pathRcsbDictionaryFile = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'dictionaries', 'rcsb_mmcif_ext_v1.dic')
        #
        self.__dataSetId = '2018_23'
        self.__pathClusterData = os.path.join(TOPDIR, 'rcsb', 'mock-data', 'cluster_data', 'mmseqs-20180608')
        #self.__levels = ['100', '95', '90', '70', '50', '30']
        self.__levels = ['100']
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
        updateId = self.__updateId
        enforceOpts = "mandatoryKeys|mandatoryAttributes|bounds|enums"
        eCount = self.__testValidateOpts(updateId, enforceOpts=enforceOpts)
        logger.info("Total validation errors enforcing %s : %d" % (enforceOpts, eCount))
        self.assertTrue(eCount <= 1)

    def __testValidateOpts(self, updateId, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):
        schemaNames = ['entity_sequence_clusters']
        collectionNames = {'entity_sequence_clusters': ['cluster_provenance_v0_1', 'cluster_members_v0_1', 'entity_members_v0_1']}
        #
        eCount = 0
        for schemaName in schemaNames:
            for collectionName in collectionNames[schemaName]:
                cD = self.__testBuildJsonSchema(schemaName, collectionName, enforceOpts=enforceOpts)
                #
                dL = self.__getSequenceClusterData(collectionName, levels=self.__levels, dataSetId=self.__dataSetId, dataLocator=self.__pathClusterData)
                # Raises exceptions for schema compliance.
                Draft4Validator.check_schema(cD)
                #
                v = Draft4Validator(cD, format_checker=FormatChecker())
                for ii, d in enumerate(dL):
                    # logger.debug("Schema %s collection %s document %d" % (schemaName, collectionName, ii))
                    try:
                        cCount = 0
                        for error in sorted(v.iter_errors(d), key=str):
                            logger.info("schema %s collection %s path %s error: %s" % (schemaName, collectionName, error.path, error.message))
                            logger.info(">>> failing object is %r" % d)
                            eCount += 1
                            cCount += 1
                        #
                        logger.debug("schema %s collection %s count %d" % (schemaName, collectionName, cCount))
                    except Exception as e:
                        logger.exception("Validation error %s" % str(e))

        return eCount

    def __testBuildJsonSchema(self, schemaName, collectionName, enforceOpts="mandatoryKeys|mandatoryAttributes|bounds|enums"):
        try:
            pathSchemaDefJson1 = os.path.join(HERE, 'test-output', 'json-schema-%s.json' % (collectionName))
            #
            smb = SchemaDefBuild(schemaName, self.__pathConfig, mockTopPath=self.__mockTopPath)
            cD = smb.build(collectionName, applicationName='json', schemaType='json', enforceOpts=enforceOpts)
            #
            logger.debug("Schema dictionary category length %d" % len(cD))
            self.assertGreaterEqual(len(cD), 1)
            #
            ioU = IoUtil()
            ioU.serialize(pathSchemaDefJson1, cD, format='json', indent=3)
            return cD

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __fetchProvenance(self):
        """ Test case for fetching a provenance dictionary content.
        """
        try:
            provKeyName = 'rcsb_entity_sequence_cluster_prov'
            provU = ProvenanceUtil(cfgOb=self.__cfgOb, workPath=self.__workPath)
            pD = provU.fetch(schemaName='DEFAULT')
            return pD[provKeyName] if provKeyName in pD else {}
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getSequenceClusterData(self, collectionName, dataSetId=None, dataLocator=None, levels=None):
        """ Test extraction on an example sequence cluster data set.
        """
        try:
            #
            if collectionName == 'cluster_provenance_v0_1':
                return [self.__fetchProvenance()]
            #
            entitySchemaName = 'rcsb_entity_sequence_cluster_list'
            clusterSchemaName = 'rcsb_entity_sequence_cluster_identifer_list'
            # provKeyName = 'rcsb_entity_sequence_cluster_prov'
            cdp = ClusterDataPrep(workPath=self.__workPath, entitySchemaName=entitySchemaName,
                                  clusterSchemaName=clusterSchemaName)
            cifD, docBySequenceD, docByClusterD = cdp.extract(dataSetId, clusterSetLocator=dataLocator, levels=levels, clusterType='entity')
            self.assertEqual(len(cifD), 1)
            self.assertEqual(len(docBySequenceD), 1)
            self.assertEqual(len(docByClusterD), 1)
            if collectionName == 'entity_members_v0_1':
                return docBySequenceD[entitySchemaName]
            elif collectionName == 'cluster_members_v0_1':
                return docByClusterD[clusterSchemaName]

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()
        return None


def ClusterValidateSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ClusterDataPrepValidateTests("testValidateOptsStrict"))
    return suiteSelect


if __name__ == '__main__':
    #
    mySuite = ClusterValidateSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
