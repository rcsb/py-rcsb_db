##
#
# File:    WorkflowSchemaReportTests.py
# Author:  J. Westbrook
# Date:    12-Feb-2015
# Version: 0.001
##
"""
Test cases for SQL select and report generation  using workflow schema -

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

from rcsb_db.schema.WorkflowSchemaDef import WorkflowSchemaDef
from rcsb_db.sql.SqlGen import SqlGenCondition, SqlGenQuery

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class WorkflowSchemaReportTests(unittest.TestCase):

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
            sd = WorkflowSchemaDef(verbose=self.__verbose)
            tableIdList = sd.getTableIdList()
            sqlGen = SqlGenQuery(schemaDefObj=sd, verbose=self.__verbose)

            for tableId in tableIdList:
                aIdList = sd.getAttributeIdList(tableId)
                for aId in aIdList:
                    sqlGen.addSelectAttributeId(attributeTuple=(tableId, aId))

                if 'DEP_SET_ID' in aIdList:
                    sqlCondition = SqlGenCondition(schemaDefObj=sd, verbose=self.__verbose)
                    sqlCondition.addValueCondition((tableId, "DEP_SET_ID"), 'EQ', ('D_1000000000', 'CHAR'))
                    sqlGen.setCondition(sqlCondition)
                if 'ORDINAL_ID' in aIdList:
                    sqlGen.addOrderByAttributeId(attributeTuple=(tableId, 'ORDINAL_ID'))
                sqlS = sqlGen.getSql()
                logger.debug("\n\n+SqlGenTests table creation SQL string\n %s\n\n" % sqlS)
                self.assertGreaterEqual(len(sqlS), 50)

                sqlGen.clear()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suiteSelect():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(WorkflowSchemaReportTests("testSelect1"))
    return suiteSelect


if __name__ == '__main__':
    mySuite = suiteSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
