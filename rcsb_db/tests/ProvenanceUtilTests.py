# File:    ProvenanceUtilTests.py.py
# Author:  J. Westbrook
# Date:    24-Jun-2018
# Version: 0.001
#
# Update:

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
import sys
import time
import unittest

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.utils.ConfigUtil import ConfigUtil
from rcsb_db.utils.ProvenanceUtil import ProvenanceUtil

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ProvenanceUtilTests(unittest.TestCase):

    def setUp(self):
        self.__mockTopPath = os.path.join(TOPDIR, 'rcsb_db', 'data')
        self.__workPath = os.path.join(HERE, 'test-ouput')
        self.__pathConfig = os.path.join(self.__mockTopPath, 'dbload-setup-example.cfg')
        #
        self.__cfgOb = ConfigUtil(configPath=self.__pathConfig, mockTopPath=self.__mockTopPath)
        #
        # Sanple provenance data -
        self.__provInfo = {
            'software': [{'pdbx_ordinal': 1,
                          'name': 'MMseq2',
                          'version': '7d26617002d155353b375b47404621d4b07e196a',
                          'date': '2017',
                          'type': 'package',
                          'contact_author': 'Martin Steinegger',
                          'contact_author_email': 'martin.steinegger@mpibpc.mpg.de',
                          'classification': 'bioinformatics',
                          'location': 'https://github.com/soedinglab/MMseqs2',
                          'language': 'C++',
                          'citation_id': 'mmseq2'}],
            #
            'citation': [{'id': 'mmseq2',
                          'title': 'MMseqs2 enables sensitive protein sequence searching for the analysis of massive data sets.',
                          'journal_abbrev': 'Nat Biotechnol.',
                          'journal_volume': '35',
                          'page_first': '1026',
                          'page_last': '1028',
                          'year': 2017,
                          'pdbx_database_id_PubMed': '29035372',
                          'pdbx_database_id_DOI': '10.1038/nbt.3988'}],
            #
            'citation_author': [{'citation_id': 'mmseq2',
                                 'name': 'Steinegger, M.',
                                 'ordinal': 1},
                                {'citation_id': 'mmseq2',
                                 'name': 'Soding, J.',
                                 'ordinal': 2}],

        }
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testStore(self):
        """ Test case for storing a provenance dictionary content.
        """
        try:
            provU = ProvenanceUtil(cfgOb=self.__cfgOb, workPath=self.__workPath)
            pD = {'entity_sequence_clusters': self.__provInfo}
            ok = provU.store(pD, schemaName='DEFAULT')
            #
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testFetch(self):
        """ Test case for fetching a provenance dictionary content.
        """
        try:
            provU = ProvenanceUtil(cfgOb=self.__cfgOb, workPath=self.__workPath)
            pD = {'entity_sequence_clusters': self.__provInfo}
            ok = provU.store(pD, schemaName='DEFAULT')
            self.assertTrue(ok)
            #
            fD = provU.fetch(schemaName='DEFAULT')
            self.assertTrue('entity_sequence_clusters' in fD)
            self.assertDictEqual(pD, fD)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testUpdate(self):
        """ Test case for updating a provenance dictionary content.
        """
        try:
            provU = ProvenanceUtil(cfgOb=self.__cfgOb, workPath=self.__workPath)
            pD = {'entity_sequence_clusters': self.__provInfo}
            ok = provU.store(pD, schemaName='DEFAULT')
            self.assertTrue(ok)
            #
            ok = provU.update(pD, schemaName='DEFAULT')
            self.assertTrue(ok)
            #
            fD = provU.fetch(schemaName='DEFAULT')
            self.assertTrue('entity_sequence_clusters' in fD)
            self.assertDictEqual(pD, fD)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def provenanceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ProvenanceUtilTests("testStore"))
    suiteSelect.addTest(ProvenanceUtilTests("testFetch"))
    suiteSelect.addTest(ProvenanceUtilTests("testUpdate"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = provenanceSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
