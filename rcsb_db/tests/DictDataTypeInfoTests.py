# File:    DictDataTypeApplicationInfoTests.py
# Author:  J. Westbrook
# Date:    22-May-2013
# Version: 0.001
#
# Update:
# 5-Jun-2018  jdw update prototypes for IoUtil() methods
#
#
#
##
"""
Tests for managing access to application data type mapping information.

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

from rcsb_db.define.DataTypeApplicationInfo import DataTypeApplicationInfo
from rcsb_db.io.IoUtil import IoUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DataTypeApplicationInfoTests(unittest.TestCase):

    def setUp(self):
        self.__verbose = True
        self.maxDiff = None
        self.__ioU = IoUtil()
        #
        self.__pathSaveTypeMap = os.path.join(HERE, 'test-output', 'app_data_type_mapping.cif')
        self.__pathSaveTypeMapJson = os.path.join(HERE, 'test-output', 'app_data_type_mapping.json')
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

    def testDefaults(self):
        """ Verify default type assignments and read, write and update operations.
        """
        try:
            dtInfo = DataTypeApplicationInfo(locator=None, applicationName='ANY', workPath=None)
            mapD = dtInfo.getDefaultDataTypeMap()
            logger.debug("Default type map length %d" % len(mapD))
            ok = self.__ioU.serialize(self.__pathSaveTypeMapJson, mapD, format='json', indent=3)
            self.assertTrue(ok)
            ok = dtInfo.writeDefaultDataTypeMap(self.__pathSaveTypeMap, applicationName='ANY')
            #
            rMapD = dtInfo.readDefaultDataTypeMap(self.__pathSaveTypeMap, applicationName='ANY')
            self.assertEqual(len(mapD), len(rMapD))
            # Note treating all data as strings to facilitate differencing.
            rMapD['new_type'] = {'application_name': 'ANY', 'app_type_code': 'app_new_type', 'app_precision_default': '0', 'app_width_default': '80', 'type_code': 'new_type'}
            #
            ok = dtInfo.updateDefaultDataTypeMap(self.__pathSaveTypeMap, rMapD, applicationName='ANY')
            uMapD = dtInfo.readDefaultDataTypeMap(self.__pathSaveTypeMap, applicationName='ANY')
            self.assertEqual(len(uMapD), len(rMapD))
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
            self.fail()


def dictTypeInfoDefaultSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(DataTypeApplicationInfoTests("testDefaults"))
    return suiteSelect


if __name__ == '__main__':
    #
    if True:
        mySuite = dictTypeInfoDefaultSuite()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
