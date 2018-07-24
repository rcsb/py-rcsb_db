##
# File:    SchemaDefBuildTests.py
# Author:  J. Westbrook
# Date:    9-Jun-2018
# Version: 0.001
#
# Update:
#
##
"""
Tests for utilities employed to construct SchemaMapDef data dependencies from
dictionary metadata and user preference data.

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

from rcsb_db.define.SchemaDefBuild import SchemaDefBuild
#
from rcsb_db.helpers.DictInfoHelper import DictInfoHelper
from rcsb_db.helpers.SchemaDefHelper import SchemaDefHelper
from rcsb_db.helpers.SchemaDocumentHelper import SchemaDocumentHelper
from rcsb_db.io.IoUtil import IoUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SchemaDefBuildTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb_db', 'data', 'dictionaries', 'mmcif_pdbx_v5_next.dic')
        self.__pathRcsbDictionaryFile = os.path.join(TOPDIR, 'rcsb_db', 'data', 'dictionaries', 'rcsb_mmcif_ext_v1.dic')
        #
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testBuild(self):
        schemaNames = ['pdbx', 'chem_comp', 'bird', 'bird_family', 'bird_chem_comp']
        applicationNames = ['ANY', 'SQL']
        for schemaName in schemaNames:
            for applicationName in applicationNames:
                self.__testBuild(schemaName, applicationName)

    def __testBuild(self, schemaName, applicationName):
        try:
            instDataTypeFilePath = os.path.join(TOPDIR, 'rcsb_db', 'data', 'data_type_info', 'scan-%s-type-map.json' % schemaName)
            appDataTypeFilePath = os.path.join(TOPDIR, 'rcsb_db', 'data', 'data_type_info', 'app_data_type_mapping.cif')
            #
            pathSchemaDefJson = os.path.join(HERE, 'test-output', 'schema_def-%s-%s.json' % (schemaName, applicationName))
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
                                 documentDefHelper=docHelper,
                                 applicationName=applicationName,
                                 includeContentClasses=['ADMIN_CATEGORY'])
            cD = smb.build()
            #
            logger.debug("Schema dictionary category length %d" % len(cD['SCHEMA_DICT']))
            self.assertGreaterEqual(len(cD['SCHEMA_DICT']), 5)
            #
            ioU = IoUtil()
            ioU.serialize(pathSchemaDefJson, cD, format='json', indent=3)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def schemaBuildSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefBuildTests("testBuild"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = schemaBuildSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
