##
# File:    SchemaDocumentHelperBase.py
# Author:  J. Westbrook
# Date:    6-Jun-2018
# Version: 0.001 Initial version
#
# Updates:
#  6-Jun-2018  jdw  separate table and document
# 14-Aug-2018  jdw  generalize document key attribute to attribute list
##
"""
Inject additional information into a schema document definition. Single source of additional document
schema semantic content.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

logger = logging.getLogger(__name__)


class SchemaDocumentHelperBase(object):
    """ Inject additional information into a schema document definition.

    """

    def __init__(self, **kwargs):
        self._raiseExceptions = kwargs.get('raiseExceptions', False)

    def getCollections(self, schemaName):
        if self._raiseExceptions:
            raise NotImplementedError()
        return []

    def getExcluded(self, collectionName):
        '''  For input collection, return the list of excluded schema identifiers.

        '''
        if self._raiseExceptions:
            raise NotImplementedError()
        return []

    def getIncluded(self, collectionName):
        '''  For input collection, return the list of included schema identifiers.

        '''
        if self._raiseExceptions:
            raise NotImplementedError()
        return []

    def getDocumentKeyAttributeNames(self, collectionName):
        if self._raiseExceptions:
            raise NotImplementedError()
        return []
