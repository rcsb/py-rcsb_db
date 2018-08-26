##
# File:    ProvenanceUtil.py
# Author:  J. Westbrook
# Date:    23-Jun-2018
# Version: 0.001
#
# Updates:
##
"""
 Utilities to access and update provenance details.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class ProvenanceUtil(object):

    def __init__(self, cfgOb=None, **kwargs):
        self.__cfgOb = cfgOb
        self.__workPath = kwargs.get("workPath", None)

    def fetch(self, schemaName='DEFAULT'):
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            provLocator = self.__cfgOb.getPath('PROVENANCE_INFO_LOCATOR', sectionName=schemaName)
            return mU.doImport(provLocator, format="json")
        except Exception as e:
            logger.exception("Failed retreiving provence for %s with %s" % (schemaName, str(e)))
        return {}

    def update(self, provD, schemaName='DEFAULT'):
        ok = False
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            provLocator = self.__cfgOb.getPath('PROVENANCE_INFO_LOCATOR', sectionName=schemaName)
            tD = mU.doImport(provLocator, format="json")
            tD.update(provD)
            ok = mU.doExport(provLocator, tD, format="json")
        except Exception as e:
            logger.exception("Failed storing provence for %s with %s" % (schemaName, str(e)))
        return ok

    def store(self, provD, schemaName='DEFAULT'):
        ok = False
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            provLocator = self.__cfgOb.getPath('PROVENANCE_INFO_LOCATOR', sectionName=schemaName)
            ok = mU.doExport(provLocator, provD, format="json")
        except Exception as e:
            logger.exception("Failed storing provence for %s with %s" % (schemaName, str(e)))
        return ok
