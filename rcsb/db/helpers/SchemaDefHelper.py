##
# File:    SchemaDefHelper.py
# Author:  J. Westbrook
# Date:    24-May-2018
# Version: 0.001 Initial version
#
# Updates:
#     24-July-2018  jdw Make the name conversion method convention specific.
##
"""
Inject additional semantic information into a schema definition applicable to all implementation types.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import re

from rcsb.db.helpers.SchemaDefHelperBase import SchemaDefHelperBase

logger = logging.getLogger(__name__)


class SchemaDefHelper(SchemaDefHelperBase):
    """ Inject additional information into a schema definition. Single source of additional schema semantic content.

    """
    # included or excluded schema identifiers
    #
    __schemaContentFilters = {'pdbx': {'INCLUDE': [],
                                       'EXCLUDE': ['ATOM_SITE',
                                                   'ATOM_SITE_ANISOTROP'
                                                   ]
                                       },
                              'pdbx_core': {'INCLUDE': [],
                                            'EXCLUDE': ['ATOM_SITE',
                                                        'ATOM_SITE_ANISOTROP'
                                                        ]
                                            },
                              }

    __block_attributes = {'pdbx': {'ATTRIBUTE_NAME': 'structure_id', 'CIF_TYPE_CODE': 'code', 'MAX_WIDTH': 12, 'METHOD': 'datablockid()'},
                          'bird': {'ATTRIBUTE_NAME': 'db_id', 'CIF_TYPE_CODE': 'code', 'MAX_WIDTH': 10, 'METHOD': 'datablockid()'},
                          'bird_family': {'ATTRIBUTE_NAME': 'db_id', 'CIF_TYPE_CODE': 'code', 'MAX_WIDTH': 10, 'METHOD': 'datablockid()'},
                          'chem_comp': {'ATTRIBUTE_NAME': 'component_id', 'CIF_TYPE_CODE': 'code', 'MAX_WIDTH': 10, 'METHOD': 'datablockid()'},
                          'bird_chem_comp': {'ATTRIBUTE_NAME': 'component_id', 'CIF_TYPE_CODE': 'code', 'MAX_WIDTH': 10, 'METHOD': 'datablockid()'},
                          'pdb_distro': {'ATTRIBUTE_NAME': 'structure_id', 'CIF_TYPE_CODE': 'code', 'MAX_WIDTH': 12, 'METHOD': 'datablockid()'},
                          }
    __databaseNames = {'pdbx': {'NAME': 'pdbx_v5', 'VERSION': '0_2'},
                       'pdbx_core': {'NAME': 'pdbx_v5', 'VERSION': '0_2'},
                       'bird': {'NAME': 'bird_v5', 'VERSION': '0_1'},
                       'bird_family': {'NAME': 'bird_v5', 'VERSION': '0_1'},
                       'chem_comp': {'NAME': 'chem_comp_v5', 'VERSION': '0_1'},
                       'bird_chem_comp': {'NAME': 'chem_comp_v5', 'VERSION': '0_1'},
                       'pdb_distro': {'NAME': 'stat', 'VERSION': '0_1'},
                       }

    #
    __re0 = re.compile('(database|cell|order|partition|group)$', flags=re.IGNORECASE)
    __re1 = re.compile('[-/%[]')
    __re2 = re.compile('[\]]')

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (below)  placeholer

        """
        super(SchemaDefHelper, self).__init__(**kwargs)

    def getConvertNameMethod(self, nameConvention):
        if nameConvention.upper() in ['SQL']:
            return self.__convertNameDefault
        elif nameConvention.upper() in ['ANY', 'DOCUMENT', 'SOLR', 'JSON']:
            return self.__convertNamePunc
        else:
            return self.__convertNameDefault

    def __convertNamePunc(self, name):
        """ Default schema name converter -
        """
        return SchemaDefHelper.__re1.sub('_', SchemaDefHelper.__re2.sub('', name))

    def __convertNameDefault(self, name):
        """ Default schema name converter -
        """
        if SchemaDefHelper.__re0.match(name) or name[0].isdigit():
            name = 'the_' + name
        return SchemaDefHelper.__re1.sub('_', SchemaDefHelper.__re2.sub('', name))

    # @classmethod
    # def xxconvertName(cls, name):
    #    """ Default schema name converter -
    #    """
    #    if cls.__re0.match(name):
    #        name = 'the_' + name
    #    return cls.__re1.sub('_', cls.__re2.sub('', name))

    def getExcluded(self, schemaName):
        '''  For input schema definition, return the list of excluded schema identifiers.

        '''
        includeL = []
        try:
            includeL = SchemaDefHelper.__schemaContentFilters[schemaName]['EXCLUDE']
        except Exception as e:
            logger.debug("Schema definition %s failing with %s" % (schemaName, str(e)))
        return includeL

    def getIncluded(self, schemaName):
        '''  For input schema definition, return the list of included schema identifiers.

        '''
        excludeL = []
        try:
            excludeL = SchemaDefHelper.__schemaContentFilters[schemaName]['INCLUDE']
        except Exception as e:
            logger.debug("Schema definition %s failing with %s" % (schemaName, str(e)))
        return excludeL

    def getBlockAttributeName(self, schemaName):
        r = None
        try:
            return SchemaDefHelper.__block_attributes[schemaName]['ATTRIBUTE_NAME']
        except Exception as e:
            logger.debug("Schema definition %s failing with %s" % (schemaName, str(e)))
        return r

    def getBlockAttributeCifType(self, schemaName):
        r = None
        try:
            return SchemaDefHelper.__block_attributes[schemaName]['CIF_TYPE_CODE']
        except Exception as e:
            logger.debug("Schema definition %s failing with %s" % (schemaName, str(e)))
        return r

    def getBlockAttributeMaxWidth(self, schemaName):
        r = None
        try:
            return SchemaDefHelper.__block_attributes[schemaName]['MAX_WIDTH']
        except Exception as e:
            logger.debug("Schema definition %s failing with %s" % (schemaName, str(e)))
        return r

    def getBlockAttributeMethod(self, schemaName):
        r = None
        try:
            return SchemaDefHelper.__block_attributes[schemaName]['METHOD']
        except Exception as e:
            logger.debug("Schema definition %s failing with %s" % (schemaName, str(e)))
        return r

    def getDatabaseName(self, schemaName):
        r = (None, None)
        try:
            return SchemaDefHelper.__databaseNames[schemaName]['NAME']
        except Exception as e:
            logger.debug("Schema definition %s failing with %s" % (schemaName, str(e)))
        return r

    def getDatabaseVersion(self, schemaName):
        r = (None, None)
        try:
            return SchemaDefHelper.__databaseNames[schemaName]['VERSION']
        except Exception as e:
            logger.debug("Schema definition %s failing with %s" % (schemaName, str(e)))
        return r
