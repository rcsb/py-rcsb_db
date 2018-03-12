##
#
# File:    PdbDistroSchemaReportTests.py
# Author:  J. Westbrook
# Date:    21-May-2015
# Version: 0.001
#
# Updates:
#           12-Mar-2018 jdw refactor for Python Packaging -
##
"""
Test cases for SQL select and report generation  using PDB Distro -

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import os
import sys
import unittest
import time

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.sql.MyDbSqlGen import MyDbQuerySqlGen, MyDbConditionSqlGen
from rcsb_db.schema.PdbDistroSchemaDef import PdbDistroSchemaDef


class PdbDistroSchemaReportTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def testSelect1(self):
        """Test case -  selection everything for a simple condition -
        """
        try:
            # selection list  -
            sList = [('PDB_ENTRY_TMP', 'PDB_ID'), ('REFINE', 'LS_D_RES_LOW'), ('REFINE', 'LS_R_FACTOR_R_WORK')]
            # condition value list -
            cList = [(('PDB_ENTRY_TMP', 'PDB_ID'), 'LIKE', ('x-ray', 'char')),
                     (('PDB_ENTRY_TMP', 'STATUS_CODE'), 'EQ', ('REL', 'char')),
                     (('PDB_ENTRY_TMP', 'METHOD'), 'NE', ('THEORETICAL_MODEL', 'char')),
                     (('PDBX_WEBSELECT', 'ENTRY_TYPE'), 'EQ', ('PROTEIN', 'char')),
                     (('PDBX_WEBSELECT', 'CRYSTAL_TWIN'), 'GT', (0, 'int')),
                     (('PDBX_WEBSELECT', 'REFINEMENT_SOFTWARE'), 'LIKE', ('REFMAC', 'char')),
                     (('PDBX_WEBSELECT', 'DATE_OF_RCSB_RELEASE'), 'GE', (1900, 'date')),
                     (('PDBX_WEBSELECT', 'DATE_OF_RCSB_RELEASE'), 'LE', (2014, 'date'))
                     ]
            #
            #
            gList = [('OR', ('PDBX_WEBSELECT', 'METHOD_TO_DETERMINE_STRUCT'), 'LIKE', ('MOLECULAR REPLACEMENT', 'char')),
                     ('OR', ('PDBX_WEBSELECT', 'METHOD_TO_DETERMINE_STRUCT'), 'LIKE', ('MR', 'char'))
                     ]
            # attribute ordering list
            oList = [('PDB_ENTRY_TMP', 'PDB_ID'), ('REFINE', 'LS_D_RES_LOW'), ('REFINE', 'LS_R_FACTOR_R_WORK')]

            sd = PdbDistroSchemaDef(verbose=self.__verbose)
            # tableIdList = sd.getTableIdList()
            # aIdList = sd.getAttributeIdList(tableId)
            sqlGen = MyDbQuerySqlGen(schemaDefObj=sd, verbose=self.__verbose)

            sTableIdList = []
            for sTup in sList:
                sqlGen.addSelectAttributeId(attributeTuple=(sTup[0], sTup[1]))
                sTableIdList.append(sTup[0])

            sqlCondition = MyDbConditionSqlGen(schemaDefObj=sd, verbose=self.__verbose)
            for cTup in cList:
                sqlCondition.addValueCondition(cTup[0], cTup[1], cTup[2])
            sqlCondition.addGroupValueConditionList(gList, preOp='AND')
            sqlCondition.addTables(sTableIdList)
            #
            sqlGen.setCondition(sqlCondition)
            for oTup in oList:
                sqlGen.addOrderByAttributeId(attributeTuple=oTup)
            sqlS = sqlGen.getSql()
            logger.debug("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % sqlS)
            self.assertGreaterEqual(len(sqlS), 1000)
            sqlGen.clear()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suiteSelect():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbDistroSchemaReportTests("testSelect1"))
    return suiteSelect


if __name__ == '__main__':
    mySuite = suiteSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
