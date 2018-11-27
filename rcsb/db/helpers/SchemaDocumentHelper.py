##
# File:    SchemaDocumentHelper.py
# Author:  J. Westbrook
# Date:    7-Jun-2018
# Version: 0.001 Initial version
#
# Updates:
#  22-Jun-2018 jdw change collection attribute id specification to dot notation
#  14-Aug-2018 jdw generalize document key attribute to attribute list
#  20-Aug-2018 jdw slice details added to __schemaContentFilters
#   8-Oct-2018 jdw added getSubCategoryAggregates() method
#
##
"""
Inject additional document information into a schema definition.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.helpers.SchemaDocumentHelperBase import SchemaDocumentHelperBase

logger = logging.getLogger(__name__)


class SchemaDocumentHelper(SchemaDocumentHelperBase):
    """ Inject additional document information into a schema definition.

        Single source of additional document schema semantic content.
    """

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (below)

        """
        super(SchemaDocumentHelper, self).__init__(**kwargs)
        # ----
        #
        self.__cfgOb = kwargs.get('cfgOb', None)
        sectionName = kwargs.get('config_section', 'document_helper')
        self.__cfgD = self.__cfgOb.exportConfig(sectionName=sectionName)
        #
        # ----

    def getCollections(self, schemaName):
        cL = []
        try:
            cL = self.__cfgD['schema_collection_names'][schemaName]
        except Exception as e:
            logger.debug("Schema definitiona name %s failing with %s" % (schemaName, str(e)))
        return cL

    def getCollectionMap(self):
        try:
            return self.__cfgD['schema_collection_names']
        except Exception as e:
            logger.debug("Failing with %s" % (str(e)))
        return {}

    def getExcluded(self, collectionName):
        '''  For input collection, return the list of excluded schema identifiers.

        '''
        includeL = []
        try:
            includeL = [tS.upper() for tS in self.__cfgD['schema_content_filters'][collectionName]['EXCLUDE']]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return includeL

    def getIncluded(self, collectionName):
        '''  For input collection, return the list of included schema identifiers.

        '''
        excludeL = []
        try:
            excludeL = [tS.upper() for tS in self.__cfgD['schema_content_filters'][collectionName]['INCLUDE']]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return excludeL

    def getSliceFilter(self, collectionName):
        '''  For input collection, return an optional slice filter or None.

        '''
        sf = None
        try:
            sf = self.__cfgD['schema_content_filters'][collectionName]['SLICE']
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return sf

    def getDocumentKeyAttributeNames(self, collectionName):
        r = []
        try:
            return self.__cfgD['collection_attribute_names'][collectionName]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return r

    def getPrivateDocumentAttributes(self, collectionName):
        r = []
        try:
            return [d for d in self.__cfgD['collection_private_keys'][collectionName] if d['PRIVATE_DOCUMENT_NAME'] and len(d['PRIVATE_DOCUMENT_NAME']) > 0]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return r

    def getSubCategoryAggregates(self, collectionName):
        r = []
        try:
            return self.__cfgD['collection_subcategory_aggregates'][collectionName]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return r
