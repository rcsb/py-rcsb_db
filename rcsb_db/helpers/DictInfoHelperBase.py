##
# File:    DictInfoHelperBase.py
# Author:  J. Westbrook
# Date:    18-May-2018
# Version: 0.001 Initial version
#
# Updates:
#    6-Jun-2018 jdw review the used
##
"""
Supplements dictionary information as required for schema production.

Single source of additional dictionary semantic content.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

logger = logging.getLogger(__name__)


class DictInfoHelperBase(object):
    """ Supplements standard dictionary content as required for schema production.

    """

    def __init__(self, **kwargs):
        self._raiseExceptions = kwargs.get('raiseExceptions', False)

    def getCardinalityKeyItem(self):
        """ Identify the parent item for the dictionary type that can be used to
            identify child categories with unity cardinality.   That is, logically containing
            a single data row in any instance.

        """
        if self._raiseExceptions:
            raise NotImplementedError()
        return ('', '')

    def getTypeCodes(self, kind):
        """
        """
        if self._raiseExceptions:
            raise NotImplementedError()
        return []

    def getQueryStrings(self, kind):
        if self._raiseExceptions:
            raise NotImplementedError()
        return []
