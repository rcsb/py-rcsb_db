##
# File:    SchemaMapBuildTests.py.py
# Author:  J. Westbrook
# Date:    13-April-2013
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


import sys
import unittest
import pprint
import json
import os
import time

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.schema.SchemaDefBuild import SchemaDefBuild


class SchemaMapBuildTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        self.__pathPdbxDictionaryFile = os.path.join(TOPDIR, 'rcsb_db', 'data', 'mmcif_pdbx_v5_next.dic')
        self.__pathSaveDefJson = os.path.join(HERE, 'test-output', 'schema_map_def.json')
        self.__pathSaveDefText = os.path.join(HERE, 'test-output', 'schema_map_def.txt')
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)" % (self.id(),
                                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                            endTime - self.__startTime))

    def testCreate(self):
        try:
            smb = SchemaDefBuild(dictPath=self.__pathPdbxDictionaryFile, cardinalityKeyItem='_entry.id')
            cD = smb.create(applicationName="ANY", blockAttributeName="Structure_ID")
            logger.info("Dictionary category length %d" % len(cD))
            self.__saveSchemaMapDef(self.__pathSaveDefJson, cD)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __saveSchemaMapDef(self, savePath, sdObj, format="json"):
        """Persist the schema map  data structure -
        """
        try:
            if format == "json":
                sOut = json.dumps(sdObj, sort_keys=True, indent=3)
            else:
                sOut = pprint.pformat(sdObj, indent=1, width=120)
            with open(savePath, 'w') as ofh:
                ofh.write("\n%s\n" % sOut)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def schemaBuildSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaMapBuildTests("testCreate"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = schemaBuildSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
