##
# File:    ProvenanceUtil.py
# Author:  J. Westbrook
# Date:    23-Jun-2018
# Version: 0.001
#
# Updates:
#  24-Oct-2018  jdw revise api for new config organization
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

    def fetch(self, cfgSectionName='site_info'):
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            provLocator = self.__cfgOb.getPath('PROVENANCE_INFO_LOCATOR', sectionName=cfgSectionName)
            return mU.doImport(provLocator, format="json")
        except Exception as e:
            logger.exception("Failed retreiving provence for %s with %s" % (cfgSectionName, str(e)))
        return {}

    def update(self, provD, cfgSectionName='site_info'):
        ok = False
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            provLocator = self.__cfgOb.getPath('PROVENANCE_INFO_LOCATOR', sectionName=cfgSectionName)
            tD = mU.doImport(provLocator, format="json")
            tD.update(provD)
            ok = mU.doExport(provLocator, tD, format="json")
        except Exception as e:
            logger.exception("Failed storing provence for %s with %s" % (cfgSectionName, str(e)))
        return ok

    def store(self, provD, cfgSectionName='site_info'):
        ok = False
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            provLocator = self.__cfgOb.getPath('PROVENANCE_INFO_LOCATOR', sectionName=cfgSectionName)
            ok = mU.doExport(provLocator, provD, format="json")
        except Exception as e:
            logger.exception("Failed storing provence for %s with %s" % (cfgSectionName, str(e)))
        return ok
