##
# File:    ContentDefinitionHelper.py
# Author:  J. Westbrook
# Date:    18-May-2018
# Version: 0.001 Initial version
#
# Updates:
#  23-May-2018  jdw revise dependencies for helper function.  Change method api's
#  27-May-2018  jdw add attribrute level filters
#   1-Jun-2018  jdw add bridging class DataTransformInfo for attribute filters
#  13-Jun-2018  jdw add content classes to cover former base table feature
#  15-Jun-2018  jdw add support for alternative iterable delimiters
#  24-Jul-2018  jdw fix logic in processing _itemTransformers data
#   6-Aug-2018  jdw add slice parent item definitions in terms of parent items
#   8-Aug-2018  jdw add slice parent conditional filter definitions that could be applied to the parent data category,
#  10-Aug-2018  jdw add slice category and cardinality extras
#  18-Aug-2018  jdw add schema pdbx_core analogous to pdbx removing the block attribute.
#   7-Sep-2018  jdw add generated content classes for core schemas
#  10-Sep-2018  jdw add iterable details for semicolon separated text data
#  11-Sep-2018  jdw adjust slice cardinality constraints for entity and assembly identifier categories.
#  30-Sep-2018  jdw add source and host organism categories
#   2-Oct-2018  jdw add repository_holdings and sequence_cluster content types and associated category content.
#  12-Feb-2019  jdw add wildCardAtName argument on __getItemTransformD()
#   6-Jun-2019  jdw remove dictSubset and dictPath keywords and methods
#  22-Aug-2019  jdw consolidate with ContentDefinitionHelper() as ContentDefinitionHelper, consolidate configInfo data sections,
#                   unify terminology dictSubset->databaseName
##
"""
This helper class supplements dictionary information as required for schema production.
The additional content conferred here would best be incorporated as part of standard data
definitions at some future point.

This is the single source of configuration of dictionary specific semantic content and is
designed to avoid scattering dictionary semantic content haphazardly throughout the
code base.

All data accessors and structures here refer to dictionary category and attribute names.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import re
from collections import OrderedDict

logger = logging.getLogger(__name__)


class ContentDefinitionHelper(object):
    """Supplements dictionary information with configuration as required for schema production."""

    #
    __rE0 = re.compile("(database|cell|order|partition|group)$", flags=re.IGNORECASE)
    __rE1 = re.compile(r"[-/%[]")
    __rE2 = re.compile(r"[\]]")

    def __init__(self, **kwargs):
        """
        Args:
            cfgOb (obj): instance of ConfigInfo()
            config_section (str): target configuration section name

            Add - Include exclude filters on dictionary content -


        """
        self.__cfgOb = kwargs.get("cfgOb", None)
        sectionName = kwargs.get("config_section", "content_info_helper_configuration")
        self.__cfgD = self.__cfgOb.exportConfig(sectionName=sectionName)
        #
        self.__itD = self.__getItemTransformD()
        #
        self.__categoryClasses = self.__getCategoryContentClasses()
        self.__attributeClasses = self.__getAttributeContentClasses()

    def getDelimiter(self, categoryName, attributeName, default=","):
        for tD in self.__cfgD["iterable_delimiters"]:
            if tD["CATEGORY_NAME"] == categoryName and tD["ATTRIBUTE_NAME"] == attributeName:
                return tD["DELIMITER"]
        return default

    def getCategoryContentClasses(self, databaseName):
        try:
            rD = OrderedDict()
            for catName, cDL in self.__categoryClasses.items():
                for cD in cDL:
                    if cD["DATABASE_NAME"] == databaseName:
                        if catName not in rD:
                            rD[catName] = []
                        rD[catName].append(cD["CONTENT_CLASS"])

        except Exception as e:
            logger.debug("Failing with %s", str(e))
        return rD

    def getAttributeContentClasses(self, databaseName):
        try:
            rD = OrderedDict()
            for (catName, atName), cDL in self.__attributeClasses.items():
                for cD in cDL:
                    if cD["DATABASE_NAME"] == databaseName:
                        if (catName, atName) not in rD:
                            rD[(catName, atName)] = []
                        rD[(catName, atName)].append(cD["CONTENT_CLASS"])

        except Exception as e:
            logger.debug("Failing with %s", str(e))
        return rD

    def __getCategoryContentClasses(self):
        classD = OrderedDict()
        try:
            for cTup, cDL in self.__cfgD["content_classes"].items():
                for cD in cDL:
                    if cD["CATEGORY_NAME"] not in classD:
                        classD[cD["CATEGORY_NAME"]] = []
                    classD[cD["CATEGORY_NAME"]].append({"CONTENT_CLASS": cTup[0], "DATABASE_NAME": cTup[1]})
        except Exception as e:
            logger.debug("Failing with %s", str(e))
        return classD

    def __getAttributeContentClasses(self, wildCardAtName="__all__"):
        classD = OrderedDict()
        try:
            for cTup, cDL in self.__cfgD["content_classes"].items():
                for cD in cDL:
                    catName = cD["CATEGORY_NAME"]
                    # if now optional 'ATTRIBUTE_NAME_LIST' is absent insert wildcard attribute
                    if "ATTRIBUTE_NAME_LIST" in cD:
                        for atName in cD["ATTRIBUTE_NAME_LIST"]:
                            if (catName, atName) not in classD:
                                classD[(catName, atName)] = []
                            classD[(catName, atName)].append({"CONTENT_CLASS": cTup[0], "DATABASE_NAME": cTup[1]})
                    else:
                        if (catName, wildCardAtName) not in classD:
                            classD[(catName, wildCardAtName)] = []
                        classD[(catName, wildCardAtName)].append({"CONTENT_CLASS": cTup[0], "DATABASE_NAME": cTup[1]})
        except Exception as e:
            logger.debug("Failing with %s", str(e))
        return classD

    def __getItemTransformD(self, wildCardAtName="__all__"):
        itD = OrderedDict()
        for iFilter, dL in self.__cfgD["item_transformers"].items():
            logger.debug("Verify transform method %r", iFilter)
            for dD in dL:
                atN = dD["ATTRIBUTE_NAME"] if "ATTRIBUTE_NAME" in dD else wildCardAtName
                itD.setdefault((dD["CATEGORY_NAME"], atN), []).append(iFilter)
        return itD

    def getTransformItems(self, transformName):
        """Return the list of items subject to the input attribute filter.

        _itemTransformers{<filterName> : [{'CATEGORY_NAME':..., 'ATTRIBUTE_NAME': ... },{}]
        """
        try:
            return self.__cfgD["item_transformers"][transformName]
        except Exception:
            return []

    def getItemTransforms(self, categoryName, attributeName):
        """Return the list of transforms to be applied to the input item (categoryName, attributeName)."""
        try:
            return self.__itD[(categoryName, attributeName)]
        except Exception:
            return []

    def getItemTransformD(self):
        """Return the dictionary of transforms to be applied to the input item (categoryName, attributeName)."""
        try:
            return self.__itD
        except Exception:
            return {}

    def getCardinalityCategoryExtras(self):
        return self.__cfgD["cardinality_category_extras"]

    def getCardinalityKeyItem(self, databaseName):
        """Identify the parent item for the dictionary subset that can be used to
        identify child categories with unity cardinality.   That is, logically containing
        a single data row in any instance.

        """
        try:
            return self.__cfgD["cardinality_parent_items"][databaseName]
        except Exception:
            pass
        return {"CATEGORY_NAME": None, "ATTRIBUTE_NAME": None}

    def getInternalEnumItems(self, databaseName):
        """Return the list of items in the input database that should use internal enumerations."""
        try:
            return self.__cfgD["internal_enumeration_items"][databaseName]
        except Exception:
            pass
        return []

    def getTypeCodes(self, kind):
        """Get the list of CIF type codes of a particular kind.

        returns (dict) [{'TYPE_CODE': <type> ... other feature of the type}]
        """
        try:
            return self.__cfgD["type_code_classes"][kind]
        except Exception as e:
            logger.exception("Failing for kind %r with %s", kind, str(e))
        return []

    def getQueryStrings(self, kind):
        try:
            return self.__cfgD["query_string_selectors"][kind]
        except Exception:
            pass

        return []

    def getSelectionFilter(self, databaseName, kind):
        """Interim api for selection filters defined in terms of dictionary category and attributes name and their values.
        JDW Warning -- check for yaml artifacts
        """
        try:
            return self.__cfgD["selection_filters"][(kind, databaseName)]
        except Exception:
            pass
        return []

    def getDatabaseSelectionFilters(self, databaseName):
        """Interim api for selection filters for a particular dictionary subset."""
        try:
            vD = OrderedDict()
            tD = {kind: v for (kind, dS), v in self.__cfgD["selection_filters"].items() if dS == databaseName}
            # cleanup the yaml artifacts --
            for kk, vvL in tD.items():
                vD[kk] = []
                for vv in vvL:
                    vD.setdefault(kk, []).append({k1: v1 for k1, v1 in vv.items()})
            return vD
        except Exception:
            pass
        return {}

    def getContentClass(self, databaseName, kind):
        """Interim api for special category classes."""
        try:
            return self.__cfgD["special_content"][(kind, databaseName)]
        except Exception:
            pass
        return []

    def getDatabaseContentClass(self, databaseName):
        """Interim api for special category classes."""
        try:
            return {kind: v for (kind, dS), v in self.__cfgD["special_content"].items() if dS == databaseName}
        except Exception:
            pass
        return {}

    def getSliceParentItems(self, databaseName, kind):
        """Interim api for slice parent itens defined in terms of dictionary category and attributes name and their values."""
        try:
            return self.__cfgD["slice_parent_items"][(kind, databaseName)]
        except Exception:
            pass
        return []

    def getDatabaseSliceParents(self, databaseName):
        """Interim api for slice parent items for a particular dictionary subset."""
        try:
            return {kind: v for (kind, dS), v in self.__cfgD["slice_parent_items"].items() if dS == databaseName}
        except Exception:
            pass
        return {}

    def getSliceParentFilters(self, databaseName, kind):
        """Interim api for slice parent condition filters defined in terms of dictionary category and attributes name and their values."""
        try:
            return self.__cfgD["slice_parent_filters"][(kind, databaseName)]
        except Exception:
            pass
        return []

    def getDatabaseSliceParentFilters(self, databaseName):
        """Interim api for slice parent condition filters for a particular dictionary subset."""
        try:
            return {kind: v for (kind, dS), v in self.__cfgD["slice_parent_filters"].items() if dS == databaseName}
        except Exception:
            pass
        return {}

    def getSliceCardinalityCategoryExtras(self, databaseName, kind):
        try:
            return self.__cfgD["slice_cardinality_category_extras"][(kind, databaseName)]
        except Exception:
            return []

    def getSliceCategoryExtras(self, databaseName, kind):
        try:
            return self.__cfgD["slice_category_extras"][(kind, databaseName)]
        except Exception:
            return []

    def getConvertNameMethod(self, nameConvention):
        if nameConvention.upper() in ["SQL"]:
            return self.__convertNameDefault
        elif nameConvention.upper() in ["ANY", "DOCUMENT", "SOLR", "JSON", "BSON"]:
            return self.__convertNamePunc
        else:
            return self.__convertNameDefault

    def __convertNamePunc(self, name):
        """Default schema name converter -"""
        return ContentDefinitionHelper.__rE1.sub("_", ContentDefinitionHelper.__rE2.sub("", name))

    def __convertNameDefault(self, name):
        """Default schema name converter -"""
        if ContentDefinitionHelper.__rE0.match(name) or name[0].isdigit():
            name = "the_" + name
        return ContentDefinitionHelper.__rE1.sub("_", ContentDefinitionHelper.__rE2.sub("", name))

    # @classmethod
    # def xxconvertName(cls, name):
    #    """ Default schema name converter -
    #    """
    #    if cls.__re0.match(name):
    #        name = 'the_' + name
    #    return cls.__re1.sub('_', cls.__re2.sub('', name))

    def getExcluded(self, schemaName):
        """For input schema definition, return the list of excluded schema identifiers."""
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
        """For input schema definition, return the list of included schema identifiers."""
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

    def getDatabaseVersion(self, schemaName):
        ret = None
        try:
            return self.__cfgD["database_names"][schemaName]["VERSION"]
        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret

    def getDataTypeInstanceFile(self, schemaName):
        ret = None
        try:
            fn = self.__cfgD["database_names"][schemaName]["INSTANCE_DATA_TYPE_INFO_FILENAME"]
            if str(fn).strip():
                return fn

        except Exception as e:
            logger.debug("Schema definition %s failing with %s", schemaName, str(e))
        return ret
