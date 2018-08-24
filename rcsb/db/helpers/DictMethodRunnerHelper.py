##
# File:    DictMethodRunnerHelper.py
# Author:  J. Westbrook
# Date:    18-Aug-2018
# Version: 0.001 Initial version
#
# Updates:
#
##
"""
This helper class implements external method references in the RCSB dictionary extension.

All data accessors and structures here refer to dictionary category and attribute names.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import datetime
import logging

from mmcif.api.DataCategory import DataCategory

from rcsb.db.helpers.DictMethodRunnerHelperBase import DictMethodRunnerHelperBase

logger = logging.getLogger(__name__)


class DictMethodRunnerHelper(DictMethodRunnerHelperBase):
    """ Helper class implements external method references in the RCSB dictionary extension.

    """

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (dict)  Placeholder for future key-value arguments

        """
        super(DictMethodRunnerHelper, self).__init__(**kwargs)
        self._thing = kwargs.get("thing", None)
        logger.info("Dictionary method helper init")
        #

    def echo(self, msg):
        logger.info(msg)

    def setDatablockId(self, dataContainer, catName, atName, **kwargs):
        try:
            val = dataContainer.getName()
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=[atName]))
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            rc = cObj.getRowCount()
            numRows = rc if rc else 1
            for ii in range(numRows):
                cObj.setValue(val, atName, ii)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def setLoadDateTime(self, dataContainer, catName, atName, **kwargs):
        try:
            val = dataContainer.getProp('load_date')
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=[atName]))
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            rc = cObj.getRowCount()
            numRows = rc if rc else 1
            for ii in range(numRows):
                cObj.setValue(val, atName, ii)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def setLocator(self, dataContainer, catName, atName, **kwargs):
        try:
            val = dataContainer.getProp('locator')
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=[atName]))
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            rc = cObj.getRowCount()
            numRows = rc if rc else 1
            for ii in range(numRows):
                cObj.setValue(val, atName, ii)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def setRowIndex(self, dataContainer, catName, atName, **kwargs):
        try:
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=[atName]))
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            rc = cObj.getRowCount()
            numRows = rc if rc else 1
            for ii, iRow in enumerate(range(numRows), 1):
                # Note - we set the integer value as a string  -
                cObj.setValue(str(ii), atName, iRow)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def __getTimeStamp(self):
        utcnow = datetime.datetime.utcnow()
        ts = utcnow.strftime("%Y-%m-%d:%H:%M:%S")
        return ts
