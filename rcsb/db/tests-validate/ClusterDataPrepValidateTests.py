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

from rcsb.db.processors.ClusterDataPrep import ClusterDataPrep
from rcsb.db.utils.ProvenanceUtil import ProvenanceUtil
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil

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
        self.__pathConfig = os.path.join(self.__mockTopPath, 'config', 'dbload-setup-example.yml')
        self.__workPath = os.path.join(HERE, 'test-output')
        self.__updateId = '2018_25'
        #
        configName = 'site_info'
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__sdu = SchemaDefUtil(cfgOb=self.__cfgOb)
        #
        self.__sandboxPath = self.__cfgOb.getPath('RCSB_EXCHANGE_SANDBOX_PATH', sectionName=configName)
        #
        self.__dataSetId = '2018_23'
        self.__pathClusterData = self.__cfgOb.getPath('RCSB_SEQUENCE_CLUSTER_DATA_PATH', sectionName=configName)
        # self.__levels = ['100', '95', '90', '70', '50', '30']
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
        schemaLevel = 'full'
        eCount = self.__testValidateOpts(updateId, schemaLevel=schemaLevel)
        logger.info("Total validation errors schema level %s : %d" % (schemaLevel, eCount))
        self.assertTrue(eCount <= 1)

    def __testValidateOpts(self, updateId, schemaLevel='full'):
        schemaNames = ['entity_sequence_clusters']
        collectionNames = {'entity_sequence_clusters': ['cluster_provenance', 'cluster_members', 'entity_members']}
        #
        eCount = 0
        for schemaName in schemaNames:
            for collectionName in collectionNames[schemaName]:
                cD = self.__sdu.makeSchema(schemaName, collectionName, schemaType='JSON', level=schemaLevel, saveSchema=True, altDirPath=self.__workPath)
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

    def __fetchProvenance(self):
        """ Test case for fetching a provenance dictionary content.
        """
        try:
            provKeyName = 'rcsb_entity_sequence_cluster_prov'
            provU = ProvenanceUtil(cfgOb=self.__cfgOb, workPath=self.__workPath)
            pD = provU.fetch(cfgSectionName='site_info')
            return pD[provKeyName] if provKeyName in pD else {}
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __getSequenceClusterData(self, collectionName, dataSetId=None, dataLocator=None, levels=None):
        """ Test extraction on an example sequence cluster data set.
        """
        try:
            #
            if collectionName == 'cluster_provenance':
                return [self.__fetchProvenance()]
            #
            entitySchemaName = 'rcsb_entity_sequence_cluster_list'
            clusterSchemaName = 'rcsb_entity_sequence_cluster_identifer_list'
            cdp = ClusterDataPrep(workPath=self.__workPath, entitySchemaName=entitySchemaName, clusterSchemaName=clusterSchemaName)
            cifD, docBySequenceD, docByClusterD = cdp.extract(dataSetId, clusterSetLocator=dataLocator, levels=levels, clusterType='entity')
            self.assertEqual(len(cifD), 1)
            self.assertEqual(len(docBySequenceD), 1)
            self.assertEqual(len(docByClusterD), 1)
            if collectionName == 'entity_members':
                return docBySequenceD[entitySchemaName]
            elif collectionName == 'cluster_members':
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
