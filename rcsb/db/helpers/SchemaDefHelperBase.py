##
# File:    SchemaDefHelperBase.py
# Author:  J. Westbrook
# Date:    6-Jun-2018
# Version: 0.001 Initial version
#
# Updates:
#  6-Jun-2018  jdw  separate table and document
##
"""
Inject additional information into a schema definition. Single source of additional
schema semantic content.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

logger = logging.getLogger(__name__)


class SchemaDefHelperBase(object):
    """ Inject additional information into a schema definition.

    """

    def __init__(self, **kwargs):
        self._raiseExceptions = kwargs.get('raiseExceptions', False)

    def getExcluded(self, schemaName):
        '''  For input schema definition, return the list of excluded schema identifiers.

        '''
        if self._raiseExceptions:
            raise NotImplementedError()
        return []

    def getIncluded(self, schemaName):
        '''  For input schema definition, return the list of included schema identifiers.

        '''
        if self._raiseExceptions:
            raise NotImplementedError()
        return []

    def getBlockAttributeId(self, schemaName):
        if self._raiseExceptions:
            raise NotImplementedError()
        return (None, None)
