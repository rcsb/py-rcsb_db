##
# File:    PdbxSchemaMapReaderTests.py
# Author:  J. Westbrook
# Date:    4-Jan-2013
# Version: 0.001
#
# Update:
#  27-Sep-2012  jdw add alternate instance attribute mapping.
#  11=Jan-2013  jdw add table and attribute abbreviation support.
#  12-Jan-2013  jdw add Chemical component and PDBx schema map examples
#  14-Jan-2013  jdw installed in wwpdb.utils.db/
#  28-Jun-2018  jdw repath schema map data files
##
"""
Tests for reader of RCSB schema map data files exporting the data structure used by the
wwpdb.utils.db.SchemaMapDef class hierarchy.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import pprint
import time
import unittest

from rcsb.db.utils.PdbxSchemaMapReader import PdbxSchemaMapReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PdbxSchemaMapReaderTests(unittest.TestCase):
    def setUp(self):
        self.__verbose = True
        self.__pathPrdSchemaMapFile = os.path.join(TOPDIR, "rcsb", "mock-data", "schema-maps", "schema_map_pdbx_prd_v5.cif")
        self.__pathPdbxSchemaMapFile = os.path.join(TOPDIR, "rcsb", "mock-data", "schema-maps", "schema_map_pdbx_v5_rc.cif")
        self.__pathCcSchemaMapFile = os.path.join(TOPDIR, "rcsb", "mock-data", "schema-maps", "schema_map_pdbx_cc.cif")
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        pass

    def testReadPrdMap(self):
        self.__readMap(self.__pathPrdSchemaMapFile, os.path.join(HERE, "test-output", "prd-def.out"))

    def testReadCcMap(self):
        self.__readMap(self.__pathCcSchemaMapFile, os.path.join(HERE, "test-output", "cc-def.out"))

    def testReadPdbxMap(self):
        self.__readMap(self.__pathPdbxSchemaMapFile, os.path.join(HERE, "test-output", "pdbx-def.out"))

    def __readMap(self, mapFilePath, defFilePath):
        """Test case -  read input schema map file and write python schema def data structure -"""
        try:
            smr = PdbxSchemaMapReader(verbose=self.__verbose)
            smr.read(mapFilePath)
            sd = smr.makeSchemaDef()
            # sOut=json.dumps(sd,sort_keys=True,indent=3)
            sOut = pprint.pformat(sd, indent=1, width=120)
            with open(defFilePath, "w", encoding="utf-8") as ofh:
                ofh.write("\n%s\n" % sOut)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def schemaSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadPrdMap"))
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadCcMap"))
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadPdbxMap"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = schemaSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
