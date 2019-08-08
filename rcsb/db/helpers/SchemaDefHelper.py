##
# File:    SchemaDefHelper.py
# Author:  J. Westbrook
# Date:    24-May-2018
# Version: 0.001 Initial version
#
# Updates:
#     24-Jul-2018  jdw Make the name conversion method convention specific.
#      4-Nov-2018  jdw add support for excluded attributes
#     31-Mar-2019  jdw add method getBlockAttributeRefParent()
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


logger = logging.getLogger(__name__)


class SchemaDefHelper(object):
    """ Inject additional information into a schema definition. Single source of additional schema semantic content.

    """

    #
    __rE0 = re.compile("(database|cell|order|partition|group)$", flags=re.IGNORECASE)
    __rE1 = re.compile(r"[-/%[]")
    __rE2 = re.compile(r"[\]]")

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (below)  placeholer

        """
        # ----
        #
        self.__cfgOb = kwargs.get("cfgOb", None)
        sectionName = kwargs.get("config_section", "schemadef_helper")
        self.__cfgD = self.__cfgOb.exportConfig(sectionName=sectionName)
        #
        # ----

    def getConvertNameMethod(self, nameConvention):
        if nameConvention.upper() in ["SQL"]:
            return self.__convertNameDefault
        elif nameConvention.upper() in ["ANY", "DOCUMENT", "SOLR", "JSON", "BSON"]:
            return self.__convertNamePunc
        else:
            return self.__convertNameDefault

    def __convertNamePunc(self, name):
        """ Default schema name converter -
        """
        return SchemaDefHelper.__rE1.sub("_", SchemaDefHelper.__rE2.sub("", name))

    def __convertNameDefault(self, name):
        """ Default schema name converter -
        """
        if SchemaDefHelper.__rE0.match(name) or name[0].isdigit():
            name = "the_" + name
        return SchemaDefHelper.__rE1.sub("_", SchemaDefHelper.__rE2.sub("", name))

    # @classmethod
    # def xxconvertName(cls, name):
    #    """ Default schema name converter -
    #    """
    #    if cls.__re0.match(name):
    #        name = 'the_' + name
    #    return cls.__re1.sub('_', cls.__re2.sub('', name))

    def getExcluded(self, schemaName):
        """  For input schema definition, return the list of excluded schema identifiers.

        """
        includeL = []
        try:
            includeL = [tS.upper() for tS in self.__cfgD["schema_content_filters"][schemaName]["EXCLUDE"]]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return includeL

    def getExcludedAttributes(self, schemaName):
        atExcludeD = {}
        for sn, dL in self.__cfgD["schema_exclude_attributes"].items():
            if sn == schemaName:
                for dD in dL:
                    atExcludeD[(dD["CATEGORY_NAME"], dD["ATTRIBUTE_NAME"])] = sn
        return atExcludeD

    def getIncluded(self, schemaName):
        """  For input schema definition, return the list of included schema identifiers.

        """
        excludeL = []
        try:
            excludeL = [tS.upper() for tS in self.__cfgD["schema_content_filters"][schemaName]["INCLUDE"]]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return excludeL

    def getBlockAttributeName(self, schemaName):
        ret = None
        try:
            return self.__cfgD["schema_block_attributes"][schemaName]["ATTRIBUTE_NAME"]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret

    def getBlockAttributeCifType(self, schemaName):
        ret = None
        try:
            return self.__cfgD["schema_block_attributes"][schemaName]["CIF_TYPE_CODE"]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret

    def getBlockAttributeMaxWidth(self, schemaName):
        ret = None
        try:
            return self.__cfgD["schema_block_attributes"][schemaName]["MAX_WIDTH"]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret

    def getBlockAttributeMethod(self, schemaName):
        ret = None
        try:
            return self.__cfgD["schema_block_attributes"][schemaName]["METHOD"]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret

    def getBlockAttributeRefParent(self, schemaName):
        ret = None
        try:
            pCatName = self.__cfgD["schema_block_attributes"][schemaName]["REF_PARENT_CATEGORY_NAME"]
            pAtName = self.__cfgD["schema_block_attributes"][schemaName]["REF_PARENT_ATTRIBUTE_NAME"]
            return {"CATEGORY_NAME": pCatName, "ATTRIBUTE_NAME": pAtName}
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret

    def getDatabaseName(self, schemaName):
        ret = None
        try:
            return self.__cfgD["schema_info"][schemaName]["DATABASE_NAME"]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret

    def getDatabaseVersion(self, schemaName):
        ret = None
        try:
            return self.__cfgD["schema_info"][schemaName]["VERSION"]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret

    def getDataTypeInstanceFile(self, schemaName):
        ret = None
        try:
            fn = self.__cfgD["schema_info"][schemaName]["INSTANCE_DATA_TYPE_INFO_FILENAME"]
            if str(fn).strip():
                return fn

        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret
