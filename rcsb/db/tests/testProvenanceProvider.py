# File:    ProvenanceProviderTests.py
# Author:  J. Westbrook
# Date:    24-Jun-2018
# Version: 0.001
#
# Update:
#   6-Jul-2018. jdw generalize test case.
#   9-Oct-2018  jdw only use the list cases and make pubmedid an int
#
##
"""
Tests for provenance management utilities.
"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.utils.ProvenanceProvider import ProvenanceProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ProvenanceProviderTests(unittest.TestCase):
    def setUp(self):
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        # Sanple provenance data -
        self.__provKeyName = "rcsb_entity_sequence_cluster_prov"
        self.__provInfo = {
            "software": {
                "pdbx_ordinal": 1,
                "name": "MMseq2",
                "version": "7d26617002d155353b375b47404621d4b07e196a",
                "date": "2017",
                "type": "package",
                "contact_author": "Martin Steinegger",
                "contact_author_email": "martin.steinegger@mpibpc.mpg.de",
                "classification": "bioinformatics",
                "location": "https://github.com/soedinglab/MMseqs2",
                "language": "C++",
                "citation_id": "mmseq2",
            },
            #
            "citation": {
                "id": "mmseq2",
                "title": "MMseqs2 enables sensitive protein sequence searching for the analysis of massive data sets.",
                "journal_abbrev": "Nat Biotechnol.",
                "journal_volume": "35",
                "page_first": "1026",
                "page_last": "1028",
                "year": 2017,
                "pdbx_database_id_PubMed": 29035372,
                "pdbx_database_id_DOI": "10.1038/nbt.3988",
            },
            #
            "citation_author": [{"citation_id": "mmseq2", "name": "Steinegger, M.", "ordinal": 1}, {"citation_id": "mmseq2", "name": "Soding, J.", "ordinal": 2}],
        }

        self.__provInfoL = {
            "software": [
                {
                    "pdbx_ordinal": 1,
                    "name": "MMseq2",
                    "version": "7d26617002d155353b375b47404621d4b07e196a",
                    "date": "2017",
                    "type": "package",
                    "contact_author": "Martin Steinegger",
                    "contact_author_email": "martin.steinegger@mpibpc.mpg.de",
                    "classification": "bioinformatics",
                    "location": "https://github.com/soedinglab/MMseqs2",
                    "language": "C++",
                    "citation_id": "mmseq2",
                }
            ],
            #
            "citation": [
                {
                    "id": "mmseq2",
                    "title": "MMseqs2 enables sensitive protein sequence searching for the analysis of massive data sets.",
                    "journal_abbrev": "Nat Biotechnol.",
                    "journal_volume": "35",
                    "page_first": "1026",
                    "page_last": "1028",
                    "year": 2017,
                    "pdbx_database_id_PubMed": 29035372,
                    "pdbx_database_id_DOI": "10.1038/nbt.3988",
                }
            ],
            #
            "citation_author": [{"citation_id": "mmseq2", "name": "Steinegger, M.", "ordinal": 1}, {"citation_id": "mmseq2", "name": "Soding, J.", "ordinal": 2}],
        }
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testPrefetchProvenance(self):
        """Test case for pre-fetching cached provenance dictionary content."""
        try:
            provU = ProvenanceProvider(self.__cfgOb, self.__cachePath)
            pD = provU.fetch()
            logger.debug("pD keys %r", list(pD.keys()))
            self.assertGreaterEqual(len(pD.keys()), 1)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testStore(self):
        """Test case for storing a provenance dictionary content."""
        try:
            provU = ProvenanceProvider(self.__cfgOb, self.__cachePath, useCache=False)
            pD = {self.__provKeyName: self.__provInfoL}
            ok = provU.store(pD)
            #
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testFetch(self):
        """Test case for fetching a provenance dictionary content."""
        try:
            provU = ProvenanceProvider(self.__cfgOb, self.__cachePath, useCache=False)
            pD = {self.__provKeyName: self.__provInfoL}
            ok = provU.store(pD)
            self.assertTrue(ok)
            #
            fD = provU.fetch()
            self.assertTrue(self.__provKeyName in fD)
            self.assertDictEqual(pD, fD)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testUpdate(self):
        """Test case for updating a provenance dictionary content."""
        try:
            provU = ProvenanceProvider(self.__cfgOb, self.__cachePath, useCache=False)
            pD = {self.__provKeyName: self.__provInfoL}
            ok = provU.store(pD)
            self.assertTrue(ok)
            #
            ok = provU.update(pD)
            self.assertTrue(ok)
            #
            fD = provU.fetch()
            self.assertTrue(self.__provKeyName in fD)
            self.assertDictEqual(pD, fD)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def provenanceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ProvenanceProviderTests("testStore"))
    suiteSelect.addTest(ProvenanceProviderTests("testFetch"))
    suiteSelect.addTest(ProvenanceProviderTests("testUpdate"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = provenanceSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
