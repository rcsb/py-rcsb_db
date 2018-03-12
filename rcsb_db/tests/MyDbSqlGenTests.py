##
#
# File:    MyDbSqlGenTests.py
# Author:  J. Westbrook
# Date:    31-Jan-2012
# Version: 0.001
#
# Updates:  20-Dec-2017 jdw py2/py3 working in compat23 branch
#           12-Mar-2018 jdw refactor for Python Packaging -
##
"""
Test cases for SQL command generation  --   no data connections required for these tests --

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

from rcsb_db.sql.MyDbSqlGen import MyDbAdminSqlGen, MyDbQuerySqlGen, MyDbConditionSqlGen
from rcsb_db.schema.MessageSchemaDef import MessageSchemaDef
from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef
from rcsb_db.schema.PdbDistroSchemaDef import PdbDistroSchemaDef


class MyDbSqlGenTests(unittest.TestCase):

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

    def testMessageSchemaCreate(self):
        """Test case -  create table schema using message schema definition as an example
        """

        try:
            msd = MessageSchemaDef(verbose=self.__verbose)
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=msd.getDatabaseName(), tableDefObj=tableDefObj))
                logger.debug("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            self.assertGreaterEqual(len(sqlL), 10)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testMessageImportExport(self):
        """Test case -  import and export commands --
        """

        try:
            msd = MessageSchemaDef(verbose=self.__verbose)
            databaseName = msd.getDatabaseName()
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose)
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                exportPath = os.path.join(HERE, "test-output", tableDefObj.getName() + ".tdd")
                sqlExport = myAd.exportTable(databaseName, tableDefObj, exportPath=exportPath)
                logger.debug("\n\n+MyDbSqlGenTests table export SQL string\n %s\n\n" % sqlExport)
                sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=exportPath)
                logger.debug("\n\n+MyDbSqlGenTests table import SQL string\n %s\n\n" % sqlImport)
                self.assertGreaterEqual(len(sqlImport), 100)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testBirdSchemaCreate(self):
        """Test case -  create table schema using message schema definition as an example
        """

        try:
            msd = BirdSchemaDef(verbose=self.__verbose)
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=msd.getDatabaseName(), tableDefObj=tableDefObj))
                logger.debug("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % '\n'.join(sqlL))
            self.assertGreaterEqual(len(sqlL), 90)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testBirdImportExport(self):
        """Test case -  import and export commands --
        """

        try:
            msd = BirdSchemaDef(verbose=self.__verbose)
            databaseName = msd.getDatabaseName()
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose)

            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                exportPath = tableDefObj.getName() + ".tdd"
                sqlExport = myAd.exportTable(databaseName, tableDefObj, exportPath=exportPath)
                logger.debug("\n\n+MyDbSqlGenTests table export SQL string\n %s\n\n" % sqlExport)
                sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=exportPath)
                logger.debug("\n\n+MyDbSqlGenTests table import SQL string\n %s\n\n" % sqlImport)
                self.assertGreaterEqual(len(sqlImport), 100)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSelect1(self):
        """Test case -  selection everything for a simple condition-
        """

        try:

            #
            msd = MessageSchemaDef(verbose=self.__verbose)
            tableIdList = msd.getTableIdList()
            sqlGen = MyDbQuerySqlGen(schemaDefObj=msd, verbose=self.__verbose)

            for tableId in tableIdList:
                sqlCondition = MyDbConditionSqlGen(schemaDefObj=msd, verbose=self.__verbose)
                sqlCondition.addValueCondition((tableId, "DEP_ID"), 'EQ', ('D000001', 'CHAR'))
                aIdList = msd.getAttributeIdList(tableId)
                for aId in aIdList:
                    sqlGen.addSelectAttributeId(attributeTuple=(tableId, aId))
                sqlGen.setCondition(sqlCondition)
                sqlGen.addOrderByAttributeId(attributeTuple=(tableId, 'MESSAGE_ID'))
                sqlS = sqlGen.getSql()
                logger.debug("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % sqlS)
                self.assertGreaterEqual(len(sqlS), 50)
                sqlGen.clear()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testSelectDistro(self):
        """Test case -  selection, condition and ordering methods using distro schema
        """

        try:
            # Celection list  -
            sList = [('PDB_ENTRY_TMP', 'PDB_ID'), ('REFINE', 'LS_D_RES_LOW'), ('REFINE', 'LS_R_FACTOR_R_WORK')]
            # Condition value list -
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
            self.assertGreaterEqual(len(sqlS), 100)
            sqlGen.clear()
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suite():
    return unittest.makeSuite(MyDbSqlGenTests, 'test')


def suiteMessageSchema():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbSqlGenTests("testMessageSchemaCreate"))
    suiteSelect.addTest(MyDbSqlGenTests("testMessageImportExport"))
    return suiteSelect


def suiteBirdSchema():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbSqlGenTests("testBirdSchemaCreate"))
    suiteSelect.addTest(MyDbSqlGenTests("testBirdImportExport"))
    return suiteSelect


def suiteSelect():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbSqlGenTests("testSelect1"))
    suiteSelect.addTest(MyDbSqlGenTests("testSelectDistro"))
    return suiteSelect


if __name__ == '__main__':
    # Run all tests --
    # unittest.main()
    #
    mySuite = suiteMessageSchema()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteBirdSchema()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
