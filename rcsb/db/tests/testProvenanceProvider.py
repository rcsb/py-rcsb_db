# File:    testProvenanceProvider.py
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
        self.__provInfoL = {
            "software": [
                {
                    "pdbx_ordinal": 1,
                    "name": "DIAMOND",
                    "version": "v2.1.13",
                    "date": "2025",
                    "type": "package",
                    "contact_author": "Benjamin Buchfink",
                    "contact_author_email": "buchfink@gmail.com",
                    "classification": "bioinformatics",
                    "location": "https://github.com/bbuchfink/diamond",
                    "language": "C++",
                    "citation_id": "diamond"
                }
            ],
            "citation": [
                {
                    "id": "primary",
                    "title": "Sensitive protein alignments at tree-of-life scale using DIAMOND",
                    "journal_abbrev": "Nat. Methods",
                    "journal_volume": "18",
                    "page_first": "366",
                    "page_last": "368",
                    "year": 2021,
                    "pdbx_database_id_PubMed": 33828273,
                    "pdbx_database_id_DOI": "10.1038/s41592-021-01101-x"
                },
                {
                    "id": "1",
                    "title": "Sensitive clustering of protein sequences at tree-of-life scale using DIAMOND DeepClust",
                    "journal_abbrev": "bioRxiv",
                    "year": 2023,
                    "pdbx_database_id_DOI": "10.1101/2023.01.24.525373"
                },
                {
                    "id": "2",
                    "title": "Fast and sensitive protein alignment using DIAMOND",
                    "journal_abbrev": "Nat. Methods",
                    "journal_volume": "12",
                    "page_first": "59",
                    "page_last": "60",
                    "year": 2015,
                    "pdbx_database_id_PubMed": 25402007,
                    "pdbx_database_id_DOI": "10.1038/nmeth.3176"
                }
            ],
            "citation_author": [
                {"citation_id": "primary", "name": "Buchfink, B.", "ordinal": 1},
                {"citation_id": "primary", "name": "Reuter, K.", "ordinal": 2},
                {"citation_id": "primary", "name": "Drost, H.G.", "ordinal": 3},
                {"citation_id": "1", "name": "Buchfink, B.", "ordinal": 4},
                {"citation_id": "1", "name": "Ashkenazy, H.", "ordinal": 5},
                {"citation_id": "1", "name": "Reuter, K.", "ordinal": 6},
                {"citation_id": "1", "name": "Kennedy, J.A.", "ordinal": 7},
                {"citation_id": "1", "name": "Drost, H.G.", "ordinal": 8},
                {"citation_id": "2", "name": "Buchfink, B.", "ordinal": 9},
                {"citation_id": "2", "name": "Xie, C.", "ordinal": 10},
                {"citation_id": "2", "name": "Huson, D.H.", "ordinal": 11}
            ]
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
