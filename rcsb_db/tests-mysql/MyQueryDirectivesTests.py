##
#
# File:    MyQueryDirectivesTests.py
# Author:  J. Westbrook
# Date:    20-June-2015
# Version: 0.001
#
#  Updates:
#
#   09-Aug-2015  jdw add tests for multiple values dom references -
#   09-Aug-2015  jdw add status history tests
#   12-Mar-2018  jdw refactor for Python Packaging -
##
"""
Test cases for parsing query directives and producing SQL instructions.

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

from rcsb_db.mysql.MyDbUtil import MyDbQuery
from rcsb_db.mysql.Connection import Connection
from rcsb_db.schema.PdbDistroSchemaDef import PdbDistroSchemaDef
from rcsb_db.schema.DaInternalSchemaDef import DaInternalSchemaDef
from rcsb_db.sql.MyQueryDirectives import MyQueryDirectives
from rcsb_db.utils.ConfigUtil import ConfigUtil


class MyQueryDirectivesTests(unittest.TestCase):

    def setUp(self):
        self.__databaseName = 'stat'
        self.__verbose = True
        configPath = os.path.join(TOPDIR, "rcsb_db", "data", 'dbload-setup-example.cfg')
        configName = 'DEFAULT'
        self.__cfgOb = ConfigUtil(configPath=configPath, sectionName=configName)
        self.__resourceName = "MYSQL_DB"
        self.__connectD = self.__assignResource(self.__cfgOb, resourceName=self.__resourceName)

        self.__domD = {'solution': 'sad',
                       'spaceg': 'P 21 21 21',
                       'software': 'REFMAC',
                       'date1': '2000',
                       'date2': '2014',
                       'reso1': '1.0',
                       'reso2': '5.0',
                       'rfree1': '.1',
                       'rfree2': '.4',
                       'solvent1': '2',
                       'solvent2': '4',
                       'weight1': '100',
                       'weight2': '200',
                       'twin': '1',
                       'molecular_type': 'protein',
                       'molecular_type_list': ['protein', 'RNA'],
                       'xtype': 'refine.ls_d_res_high',
                       'ytype': 'refine.ls_d_res_low',
                       'source': 'human',
                       'multikey': 'pdbx_webselect.space_group_name_H_M|refine.ls_d_res_high|refine.ls_d_res_low',
                       'tax2': '9606|10090',
                       }

        self.__qdL = ["SELECT_ITEM:1:ITEM:DOM_REF:xtype",
                      "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
                      "VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.crystal_twin:COP:GT:VALUE:DOM_REF:twin",
                      "VALUE_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE:DOM_REF:molecular_type",
                      "VALUE_CONDITION:3:LOP:AND:ITEM:pdbx_webselect.space_group_name_H_M:COP:EQ:VALUE:DOM_REF:spaceg",
                      "VALUE_CONDITION:4:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software",
                      "VALUE_KEYED_CONDITION:15:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:solution",

                      "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MR%",
                      "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MOLECULAR REPLACEMENT%",
                      "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%SAD%",
                      "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MAD%",
                      "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MR%",
                      "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MOLECULAR REPLACEMENT%",
                      "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%SAD%",
                      "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MAD%",

                      "VALUE_CONDITION:5:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:GE:VALUE:DOM_REF:date1",
                      "VALUE_CONDITION:6:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:LE:VALUE:DOM_REF:date2",
                      "VALUE_CONDITION:7:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:GE:VALUE:DOM_REF:reso1",
                      "VALUE_CONDITION:8:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:LE:VALUE:DOM_REF:reso2",
                      "VALUE_CONDITION:9:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:GE:VALUE:DOM_REF:rfree1",
                      "VALUE_CONDITION:10:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:LE:VALUE:DOM_REF:rfree2",
                      "VALUE_CONDITION:11:LOP:AND:ITEM:pdbx_webselect.solvent_content:COP:GE:VALUE:DOM_REF:solvent1",
                      "VALUE_CONDITION:12:LOP:AND:ITEM:pdbx_webselect.solvent_content:COP:LE:VALUE:DOM_REF:solvent2",
                      "VALUE_CONDITION:13:LOP:AND:ITEM:pdbx_webselect.weight_in_ASU:COP:GE:VALUE:DOM_REF:weight1",
                      "VALUE_CONDITION:14:LOP:AND:ITEM:pdbx_webselect.weight_in_ASU:COP:LE:VALUE:DOM_REF:weight2",
                      "JOIN_CONDITION:20:LOP:AND:L_ITEM:pdbx_webselect.Structure_ID:COP:EQ:R_ITEM:refine.ls_d_res_low",
                      "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
                      "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING"]
        self.__startTime = time.time()
        logger.debug("Running tests on version %s" % __version__)
        logger.debug("Starting %s at %s" % (self.id(),
                                            time.strftime("%Y %m %d %H:%M:%S", time.localtime())))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n" % (self.id(),
                                                              time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                              endTime - self.__startTime))

    def __assignResource(self, cfgOb, resourceName="MYSQL_DB"):
        cn = Connection(cfgOb=cfgOb)
        return cn.assignResource(resourceName=resourceName)

    def open(self):
        return self.__open(self.__connectD)

    def __open(self, connectD):
        cObj = Connection()
        cObj.setPreferences(connectD)
        ok = cObj.openConnection()
        if ok:
            return cObj
        else:
            return None

    def close(self, cObj):
        if cObj is not None:
            cObj.closeConnection()
            self.__dbCon = None
            return True
        else:
            return False

    def getClientConnection(self, cObj):
        return cObj.getClientConnection()

    def testDirective1(self):
        """Test case -  selection everything for a simple condition -
        """

        try:
            sd = PdbDistroSchemaDef(verbose=self.__verbose)
            mqd = MyQueryDirectives(schemaDefObj=sd, verbose=self.__verbose)
            sqlS = mqd.build(queryDirL=self.__qdL, domD=self.__domD)
            logger.debug("\n\n+testDirective1 SQL\n %s\n\n" % sqlS)
            logger.debug("Length query string %d " % len(sqlS))
            self.assertGreater(len(sqlS), 500)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def testDirectiveWithQuery0(self):
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF:xtype",
               "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
               "VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.crystal_twin:COP:GT:VALUE:DOM_REF:twin",
               "VALUE_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE:DOM_REF:molecular_type",
               "VALUE_CONDITION:3:LOP:AND:ITEM:pdbx_webselect.space_group_name_H_M:COP:EQ:VALUE:DOM_REF:spaceg",
               "VALUE_CONDITION:4:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software",
               "VALUE_KEYED_CONDITION:15:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:solution",
               "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MR%",
               "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MOLECULAR REPLACEMENT%",
               "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%SAD%",
               "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MAD%",
               "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MR%",
               "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MOLECULAR REPLACEMENT%",
               "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%SAD%",
               "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MAD%",
               "VALUE_CONDITION:5:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:GE:VALUE:DOM_REF:date1",
               "VALUE_CONDITION:6:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:LE:VALUE:DOM_REF:date2",
               "VALUE_CONDITION:7:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:GE:VALUE:DOM_REF:reso1",
               "VALUE_CONDITION:8:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:LE:VALUE:DOM_REF:reso2",
               "VALUE_CONDITION:9:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:GE:VALUE:DOM_REF:rfree1",
               "VALUE_CONDITION:10:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:LE:VALUE:DOM_REF:rfree2",
               "VALUE_CONDITION:11:LOP:AND:ITEM:pdbx_webselect.solvent_content:COP:GE:VALUE:DOM_REF:solvent1",
               "VALUE_CONDITION:12:LOP:AND:ITEM:pdbx_webselect.solvent_content:COP:LE:VALUE:DOM_REF:solvent2",
               "VALUE_CONDITION:13:LOP:AND:ITEM:pdbx_webselect.weight_in_ASU:COP:GE:VALUE:DOM_REF:weight1",
               "VALUE_CONDITION:14:LOP:AND:ITEM:pdbx_webselect.weight_in_ASU:COP:LE:VALUE:DOM_REF:weight2",
               "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
               "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING"]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery1(self):
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF:xtype",
               "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
               "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
               "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING"]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery2(self):
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF:xtype",
               "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
               "JOIN_CONDITION:1:LOP:AND:L_ITEM:pdbx_webselect.Structure_ID:COP:EQ:R_ITEM:refine.Structure_ID",
               "VALUE_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE:DOM_REF:molecular_type",
               "VALUE_CONDITION:7:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:GE:VALUE:DOM_REF:reso1",
               "VALUE_CONDITION:8:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:LE:VALUE:DOM_REF:reso2",
               "VALUE_CONDITION:9:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:GE:VALUE:DOM_REF:rfree1",
               "VALUE_CONDITION:10:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:LE:VALUE:DOM_REF:rfree2",
               "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
               "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING"]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery3(self):
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF:xtype",
               "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
               "VALUE_CONDITION:4:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software",
               "VALUE_KEYED_CONDITION:15:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:solution",
               "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MR%",
               "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MOLECULAR REPLACEMENT%",
               "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%SAD%",
               "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MAD%",
               "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MR%",
               "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MOLECULAR REPLACEMENT%",
               "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%SAD%",
               "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MAD%",
               "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
               "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING"]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery4(self):
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF:xtype",
               "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
               "VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software",
               "VALUE_LIST_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE_LOP:AND:VALUE_LIST:DOM_REF:molecular_type_list",
               "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
               "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING"]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery5(self):
        # broken -- problem with automatic addition of equi-join conditions.
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF:xtype",
               "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
               "VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.space_group_name_H_M:COP:EQ:VALUE:DOM_REF:spaceg",
               "VALUE_KEYED_CONDITION:2:LOP:AND:CONDITION_LIST_ID:2:VALUE:DOM_REF:source",
               "CONDITION_LIST:2:KEY:human:LOP:OR:ITEM:entity_src_gen.pdbx_gene_src_ncbi_taxonomy_id:COP:EQ:VALUE:9606",
               "CONDITION_LIST:2:KEY:human:LOP:OR:ITEM:entity_src_nat.pdbx_ncbi_taxonomy_id:COP:EQ:VALUE:9606",
               "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
               "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING"]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery6(self):
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF_0:multikey",
               "SELECT_ITEM:2:ITEM:DOM_REF_1:multikey",
               "SELECT_ITEM:3:ITEM:DOM_REF_2:multikey",
               "ORDER_ITEM:1:ITEM:DOM_REF_1:multikey:SORT_ORDER:INCREASING",
               "ORDER_ITEM:2:ITEM:DOM_REF_2:multikey:SORT_ORDER:DECREASING"]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery7(self):
        """
        pdbx_database_status_history

         "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "ENTRY_ID": "entry_id",
                "PDB_ID": "pdb_id",
                "DATE_BEGIN": "date_begin",
                "DATE_END": "date_end",
                "STATUS_CODE_BEGIN": "status_code_begin",
                "STATUS_CODE_END": "status_code_end",
                "ANNOTATOR": "annotator",
                "DETAILS": "details",
                "DELTA_DAYS": "delta_days",
            },
        """
        myDomD = {'multiselect': 'pdbx_database_status_history.entry_id|pdbx_database_status_history.pdb_id|pdbx_database_status_history.status_code_begin|pdbx_database_status_history.status_code_end',
                  'endstat1': 'hpuborhold',
                  'beginstat1': 'auth'}
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF_0:multiselect",
               "SELECT_ITEM:2:ITEM:DOM_REF_1:multiselect",
               "SELECT_ITEM:3:ITEM:DOM_REF_2:multiselect",
               "SELECT_ITEM:4:ITEM:DOM_REF_3:multiselect",
               "VALUE_KEYED_CONDITION:5:LOP:AND:CONDITION_LIST_ID:2:VALUE:DOM_REF:beginstat1",
               "VALUE_KEYED_CONDITION:6:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:endstat1",

               "CONDITION_LIST:1:KEY:hpuborhold:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:HOLD",
               "CONDITION_LIST:1:KEY:hpuborhold:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:HPUB",
               "CONDITION_LIST:1:KEY:procorst1:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:PROC",
               "CONDITION_LIST:1:KEY:procorst1:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:PROC_ST_1",
               "CONDITION_LIST:1:KEY:auth:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:AUTH",
               "CONDITION_LIST:2:KEY:hpuborhold:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:HOLD",
               "CONDITION_LIST:2:KEY:hpuborhold:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:HPUB",
               "CONDITION_LIST:2:KEY:procorst1:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:PROC",
               "CONDITION_LIST:2:KEY:procorst1:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:PROC_ST_1",
               "CONDITION_LIST:2:KEY:auth:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:AUTH",

               "ORDER_ITEM:1:ITEM:DOM_REF_0:multiselect:SORT_ORDER:INCREASING",
               "ORDER_ITEM:2:ITEM:DOM_REF_1:multiselect:SORT_ORDER:INCREASING"]
        self.__testDirectiveWithHistoryQuery(qdL=qdL, domD=myDomD)

    def __testDirectiveWithDistroQuery(self, qdL, domD):
        """Test case -  selection everything for a simple condition - (Distro Schema)
        """

        try:
            self.__databaseName = 'stat'
            cObj = self.open()
            dbCon = self.getClientConnection(cObj)
            sd = PdbDistroSchemaDef(verbose=self.__verbose)
            mqd = MyQueryDirectives(schemaDefObj=sd, verbose=self.__verbose)
            sqlS = mqd.build(queryDirL=qdL, domD=domD, appendValueConditonsToSelect=True)
            self.assertGreater(len(sqlS), 100)
            logger.debug("\n\n+testDirectiveWithDistroQuery SQL\n %s\n\n" % sqlS)
            myQ = MyDbQuery(dbcon=dbCon, verbose=self.__verbose)
            rowList = myQ.selectRows(queryString=sqlS)
            logger.debug("length rowlist %d" % len(rowList))
            if (self.__verbose):
                logger.debug("\n+testDirectiveWithDistroQuery mysql server returns row length %d\n" % len(rowList))
                for ii, row in enumerate(rowList[:30]):
                    logger.debug("   %6d  %r\n" % (ii, row))
            self.close(cObj)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()

    def __testDirectiveWithHistoryQuery(self, qdL, domD):
        """Test case -  selection everything for a simple condition -
        """

        try:
            self.__databaseName = 'da_internal'
            cObj = self.open()
            dbCon = self.getClientConnection(cObj)
            sd = DaInternalSchemaDef(verbose=self.__verbose)
            mqd = MyQueryDirectives(schemaDefObj=sd, verbose=self.__verbose)
            sqlS = mqd.build(queryDirL=qdL, domD=domD, appendValueConditonsToSelect=True)
            self.assertGreater(len(sqlS), 100)
            if (self.__verbose):
                logger.debug("\n\n+testDirectiveWithHistoryQuery SQL\n %s\n\n" % sqlS)
            myQ = MyDbQuery(dbcon=dbCon, verbose=self.__verbose)
            rowList = myQ.selectRows(queryString=sqlS)
            logger.debug("length rowlist %d" % len(rowList))
            if (self.__verbose):
                logger.debug("\n+testDirectiveWithHistoryQuery mysql server returns row length %d\n" % len(rowList))
                for ii, row in enumerate(rowList[:30]):
                    logger.debug("   %6d  %r\n" % (ii, row))
            self.close(cObj)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def suiteSelect():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyQueryDirectivesTests("testDirective1"))
    return suiteSelect


def suiteSelectQuery():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery1"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery2"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery3"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery4"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery5"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery6"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery7"))
    return suiteSelect

if __name__ == '__main__':
    if (True):
        mySuite = suiteSelect()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

        mySuite = suiteSelectQuery()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
