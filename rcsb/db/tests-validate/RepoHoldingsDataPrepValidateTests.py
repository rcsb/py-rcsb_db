# File:    RepoHoldingsDataPrepValidateTests.py
# Author:  J. Westbrook
# Date:    7-Oct-2018
# Version: 0.001
#
# Update:
#
##
"""
Tests for processing and validating legacy repository holdings and status records.

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

from rcsb.db.processors.RepoHoldingsDataPrep import RepoHoldingsDataPrep
from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RepoHoldingsDataPrepValidateTests(unittest.TestCase):

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
        self.__sandboxPath = self.__cfgOb.getPath('RCSB_EXCHANGE_SANDBOX_PATH', sectionName=configName)
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

    def testValidateOptsMin(self):
        updateId = self.__updateId
        schemaLevel = 'min'
        eCount = self.__testValidateOpts(updateId, schemaLevel=schemaLevel)
        logger.info("Total validation errors schema level %s : %d" % (schemaLevel, eCount))
        self.assertTrue(eCount <= 1)

    def __testValidateOpts(self, updateId, schemaLevel='full'):
        schemaNames = ['repository_holdings']
        collectionNames = {'repository_holdings_min': ['repository_holdings_superseded'],
                           'repository_holdings': ['repository_holdings_update',
                                                   'repository_holdings_current',
                                                   'repository_holdings_unreleased',
                                                   'repository_holdings_prerelease',
                                                   'repository_holdings_removed',
                                                   'repository_holdings_removed_audit_authors',
                                                   'repository_holdings_superseded',
                                                   'repository_holdings_transferred',
                                                   'repository_holdings_insilico_models'
                                                   ],
                           'entity_sequence_clusters': ['cluster_members', 'cluster_provenance', 'entity_members']
                           }
        #
        eCount = 0
        for schemaName in schemaNames:
            for collectionName in collectionNames[schemaName]:
                cD = self.__sdu.makeSchema(schemaName, collectionName, schemaType='JSON', level=schemaLevel, saveSchema=True, altDirPath=self.__workPath)
                dL = self.__getRepositoryHoldingsDocuments(schemaName, collectionName, updateId)
                # Raises exceptions for schema compliance.
                Draft4Validator.check_schema(cD)
                #
                v = Draft4Validator(cD, format_checker=FormatChecker())
                for ii, d in enumerate(dL):
                    logger.debug("Schema %s collection %s document %d" % (schemaName, collectionName, ii))
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

    def __getRepositoryHoldingsDocuments(self, schemaName, collectionName, updateId):
        """ Test loading and processing operations for legacy holdings and status data.
        """
        rL = []
        try:
            rhdp = RepoHoldingsDataPrep(sandboxPath=self.__sandboxPath, workPath=self.__workPath)
            if collectionName == 'repository_holdings_update':
                rL = rhdp.getHoldingsUpdate(updateId=updateId)
                self.assertGreaterEqual(len(rL), 10)
                logger.debug("update data length %r" % len(rL))
            #
            elif collectionName == 'repository_holdings_current':
                rL = rhdp.getHoldingsCurrent(updateId=updateId)
                self.assertGreaterEqual(len(rL), 10)
                logger.debug("holdings data length %r" % len(rL))
            #
            elif collectionName == 'repository_holdings_unreleased':
                rL = rhdp.getHoldingsUnreleased(updateId=updateId)
                self.assertGreaterEqual(len(rL), 10)
                logger.debug("unreleased data length %r" % len(rL))
            #
            elif collectionName == 'repository_holdings_prerelease':
                rL = rhdp.getHoldingsPrerelease(updateId=updateId)
                self.assertGreaterEqual(len(rL), 10)
                logger.debug("unreleased data length %r" % len(rL))

            elif collectionName in ['repository_holdings_transferred', 'repository_holdings_insilico_models']:
                rL1, rL2 = rhdp.getHoldingsTransferred(updateId=updateId)
                if collectionName == 'repository_holdings_transferred':
                    self.assertGreaterEqual(len(rL1), 10)
                    logger.debug("transferred data length %r" % len(rL1))
                    rL = rL1
                elif collectionName == 'repository_holdings_insilico_models':
                    self.assertGreaterEqual(len(rL2), 10)
                    logger.debug("Insilico data length %r" % len(rL1))
                    rL = rL2
            elif collectionName in ['repository_holdings_removed', 'repository_holdings_removed_audit_authors', 'repository_holdings_superseded']:
                rL1, rL2, rL3 = rhdp.getHoldingsRemoved(updateId=updateId)
                if collectionName == 'repository_holdings_removed':
                    self.assertGreaterEqual(len(rL1), 10)
                    logger.debug("removed data length %r" % len(rL1))
                    rL = rL1
                elif collectionName == 'repository_holdings_removed_audit_authors':
                    self.assertGreaterEqual(len(rL2), 10)
                    logger.debug("removed author data length %r" % len(rL2))
                    rL = rL2
                elif collectionName == 'repository_holdings_superseded':
                    self.assertGreaterEqual(len(rL3), 10)
                    logger.debug("removed data length %r" % len(rL3))
                    rL = rL3
            #
        except Exception as e:
            logger.exception("%s %s failing with %s" % (schemaName, collectionName, str(e)))
            self.fail()

        return rL


def RepoHoldingsValidateSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepoHoldingsDataPrepValidateTests("testValidateOptsStrict"))
    suiteSelect.addTest(RepoHoldingsDataPrepValidateTests("testValidateOptsMin"))
    return suiteSelect


if __name__ == '__main__':
    #
    mySuite = RepoHoldingsValidateSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
