##
# File:    SchemaProviderTests.py
# Author:  J. Westbrook
# Date:    9-Dec-2019
# Version: 0.001
#
# Update:
##
"""
Tests for essential access features of SchemaProvider() module

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.db.define.ContentDefinition import ContentDefinition
from rcsb.utils.dictionary.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
from rcsb.db.helpers.ContentDefinitionHelper import ContentDefinitionHelper
from rcsb.db.helpers.DocumentDefinitionHelper import DocumentDefinitionHelper
from rcsb.utils.config.ConfigUtil import ConfigUtil

# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class SchemaSearchContextsTests(unittest.TestCase):
    skipFlag = True

    def setUp(self):
        self.__verbose = True
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        pathConfig = os.path.join(TOPDIR, "rcsb", "db", "config", "exdb-config-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=pathConfig, defaultSectionName=configName, mockTopPath=mockTopPath)
        self.__docHelper = DocumentDefinitionHelper(cfgOb=self.__cfgOb)
        #
        self.__pathPdbxDictionaryFile = self.__cfgOb.getPath("PDBX_DICT_LOCATOR", sectionName=configName)
        self.__pathRcsbDictionaryFile = self.__cfgOb.getPath("RCSB_DICT_LOCATOR", sectionName=configName)
        self.__pathVrptDictionaryFile = self.__cfgOb.getPath("VRPT_DICT_LOCATOR", sectionName=configName)

        # self.__mU = MarshalUtil()
        #
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__dP = DictionaryApiProviderWrapper(self.__cfgOb, self.__cachePath, useCache=True)

        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSearchGroups(self):
        ok = self.__docHelper.checkSearchGroups()
        self.assertTrue(ok)

    @unittest.skipIf(skipFlag, "Troubleshooting test")
    def testUnUsedIndexedItems(self):
        """Enumerate items that are indexed by have no search group assignments.

        collection_attribute_search_contexts
        """

        groupNameList = self.__docHelper.getSearchGroups()
        logger.info("Search groups (%d)", len(groupNameList))
        #
        nestedSearchableD = self.__assembleNestedCategorySearchables()
        nestedSearchableD.update(self.__assembleNestedSubCategorySearchables())
        #
        attribContextD = {}
        tD = self.__docHelper.getAllAttributeSearchContexts()
        for (catName, atName), contextL in tD.items():
            attribContextD.setdefault((catName, atName), []).extend([t[0] for t in contextL])
        logger.info("search context attribContextD %d", len(attribContextD))

        lookupD = {}
        # if (catName, atName) in nestedSearchableD:
        for groupName in groupNameList:
            # get attributes in group
            attributeTupList = self.__docHelper.getSearchGroupAttributes(groupName)
            # logger.info("")
            # logger.info("%s (%2d):", groupName, len(attributeTupList))
            for catName, atName in attributeTupList:
                lookupD.setdefault((catName, atName), []).append(groupName)
        #
        logger.info("Search group lookup len %d", len(lookupD))
        for (catName, atName), contextL in attribContextD.items():
            # logger.info("%s.%s contexL %r", catName, atName, contextL)

            if "full-text" in contextL:
                if (catName, atName) in lookupD or (catName, atName) in nestedSearchableD:
                    continue
                logger.info("%s.%s contexL %r", catName, atName, contextL)

        #

        return True

    @unittest.skipIf(skipFlag, "Troubleshooting test")
    def testExpandSearchGroups(self):
        """Expand search groups and metadata content as these would be display in RCSB search menu."""
        _, afD = self.__getContentFeatures()
        groupNameList = self.__docHelper.getSearchGroups()
        logger.info("Search groups (%d)", len(groupNameList))
        #
        nestedSearchableD = self.__assembleNestedCategorySearchables()
        nestedSearchableD.update(self.__assembleNestedSubCategorySearchables())
        #
        for groupName in groupNameList:
            # get attributes in group
            attributeTupList = self.__docHelper.getSearchGroupAttributes(groupName)
            logger.info("")
            logger.info("%s (%2d):", groupName, len(attributeTupList))
            # Get search context and brief descriptions -
            for catName, atName in attributeTupList:
                searchContextTupL = self.__docHelper.getSearchContexts(catName, atName)
                if not searchContextTupL:
                    logger.warning("Missing search context for %s.%s", catName, atName)
                descriptionText = self.__docHelper.getAttributeDescription(catName, atName, contextType="brief")
                if not descriptionText:
                    logger.warning("Missing brief description %s.%s", catName, atName)
                #
                fD = afD[catName][atName] if catName in afD and atName in afD[catName] else {}
                logger.debug("%s %s fD %r", catName, atName, fD)
                units = fD["UNITS"] if "UNITS" in fD else None
                #
                uS = ""
                if units:
                    uS = "(units=%s)" % units
                #
                nS = "(%s.%s)" % (catName, atName)
                if (catName, atName) in nestedSearchableD:
                    for dS in nestedSearchableD[(catName, atName)]:
                        logger.info("- %-55s: %s %s (%s)", dS, nS, uS, ",".join([tup[0] for tup in searchContextTupL]))
                else:
                    logger.info("- %-55s: %s %s (%s)", descriptionText, nS, uS, ",".join([tup[0] for tup in searchContextTupL]))

        return True

    def __assembleNestedCategorySearchables(self):
        """Assemble dictionary of searchable items in nested categories.

        Returns:
            (dict): {(category, atName): ["Materialized brief description", ... ]
        """
        # cfD, afD = self.__getContentFeatures()
        _, afD = self.__getContentFeatures()
        logger.info("")
        searchableCategoryD = {}
        groupNameList = self.__docHelper.getSearchGroups()
        logger.debug("Search group count (%d)", len(groupNameList))
        for groupName in groupNameList:
            # get attributes in group
            attributeTupList = self.__docHelper.getSearchGroupAttributes(groupName)
            for catName, atName in attributeTupList:
                searchableCategoryD.setdefault(catName, []).append(atName)
        logger.debug("Searchable category count (%d)", len(searchableCategoryD))
        #
        retD = {}
        for catName in searchableCategoryD:
            nestedContextDL = self.__docHelper.getNestedContexts(catName)
            if not nestedContextDL:
                # not nested skip
                continue
            elif len(nestedContextDL) > 1:
                logger.warning("Multiple nested contexts for category %s", catName)
            #
            for nestedContextD in nestedContextDL:
                contextPath = nestedContextD["FIRST_CONTEXT_PATH"] if "FIRST_CONTEXT_PATH" in nestedContextD else None
                if not contextPath:
                    logger.warning("Missing context path for nested category %s", catName)
                    continue
                #
                contextName = nestedContextD["CONTEXT_NAME"]

                #
                cpCatName = contextPath.split(".")[0]
                cpAtName = contextPath.split(".")[1]
                nestedPathSearchContext = self.__docHelper.getSearchContexts(cpCatName, cpAtName)
                logger.debug("Nested (%r) context path for %r %r", contextName, cpCatName, cpAtName)
                if not nestedPathSearchContext:
                    logger.warning("Missing nested (%r) search context for %r %r", contextName, cpCatName, cpAtName)
                #
                nfD = afD[cpCatName][cpAtName] if cpCatName in afD and cpAtName in afD[cpCatName] else {}
                logger.debug("FeatureD %r", nfD)
                # --
                enumMapD = {}
                enumDL = nfD["ENUMS_ANNOTATED"]
                if not enumDL:
                    logger.warning("Missing nested enums %s.%s", cpCatName, cpAtName)
                else:
                    logger.debug("All context enums count %d", len(enumDL))
                    for enumD in enumDL:
                        logger.info("%s.%s enumD %r", cpCatName, cpAtName, enumD)
                        if "name" not in enumD:
                            logger.warning("Missing nested enum (name) for %s.%s", cpCatName, cpAtName)
                    #
                    enumMapD = {enumD["value"]: enumD["name"] if "name" in enumD else enumD["detail"] for enumD in enumDL}
                # --
                nestedDescriptionText = self.__docHelper.getAttributeDescription(cpCatName, cpAtName, contextType="brief")
                if not nestedDescriptionText:
                    logger.warning("Missing brief nested description %s.%s", cpCatName, cpAtName)
                else:
                    logger.debug("Nested context description: %r", nestedDescriptionText)
                # --
                cvDL = nestedContextD["CONTEXT_ATTRIBUTE_VALUES"] if "CONTEXT_ATTRIBUTE_VALUES" in nestedContextD else []
                if not cvDL:
                    logger.warning("Missing context attribute values for %s", catName)
                    # if no context values defined then use: all enums x searchable attributes in this category
                    #
                    # Template:  enum detail + search attribute brief description text
                    for enumD in enumDL:
                        for atName in searchableCategoryD[catName]:
                            briefDescr = self.__docHelper.getAttributeDescription(catName, atName, contextType="brief")
                            # subCategories = nfD["SUB_CATEGORIES"] if "SUB_CATEGORIES" in nfD else None
                            tS = enumD["detail"] + " " + briefDescr
                            retD.setdefault((catName, atName), []).append(tS)
                else:
                    # Only use context values from the full enum list with specified search paths.
                    #
                    # Template:  context value (enum detail) + search path attribute (brief description text)
                    #  cVDL.append({"CONTEXT_VALUE": tD["CONTEXT_VALUE"], "SEARCH_PATHS": tD["SEARCH_PATHS"]})
                    #
                    for cvD in cvDL:
                        enumV = cvD["CONTEXT_VALUE"]
                        enumDetail = enumMapD[enumV] if enumV in enumMapD else None
                        if not enumDetail:
                            logger.warning("%s %s missing detail for enum value %s", catName, cpAtName, enumV)
                        for sp in cvD["SEARCH_PATHS"]:
                            if sp.count(".") > 1:
                                k = sp.rfind(".")
                                sp = sp[:k] + "_" + sp[k + 1 :]
                            cnS = sp.split(".")[0]
                            anS = sp.split(".")[1]
                            briefDescr = self.__docHelper.getAttributeDescription(cnS, anS, contextType="brief")
                            tS = enumDetail + " " + briefDescr
                            logger.debug("%s,%s tS %r", cnS, anS, tS)
                            retD.setdefault((cnS, anS), []).append(tS)
                        for aD in cvD["ATTRIBUTES"]:
                            sp = aD["PATH"]
                            if sp.count(".") > 1:
                                k = sp.rfind(".")
                                sp = sp[:k] + "_" + sp[k + 1 :]
                            cnS = sp.split(".")[0]
                            anS = sp.split(".")[1]
                            briefDescr = self.__docHelper.getAttributeDescription(cnS, anS, contextType="brief")
                            tS = enumDetail + " " + briefDescr
                            logger.debug("%s,%s tS %r", cnS, anS, tS)
                            retD.setdefault((cnS, anS), []).append(tS)
                            exL = aD["EXAMPLES"]
                            logger.info("%s,%s sp %r examplesL %r", cnS, anS, sp, exL)
        #
        for k, vL in retD.items():
            for v in vL:
                logger.debug("%s : %r", k, v)
        #
        return retD

    def __assembleNestedSubCategorySearchables(self):
        """Assemble dictionary of searchable items in nested subcategories.

        Returns:
            (dict): {(category, atName): ["Materialized brief description", ... ]
        """
        _, afD = self.__getContentFeatures()
        # logger.info("")
        searchableCategoryD = {}
        groupNameList = self.__docHelper.getSearchGroups()
        logger.debug("Search group count (%d)", len(groupNameList))
        for groupName in groupNameList:
            # get attributes in group
            attributeTupList = self.__docHelper.getSearchGroupAttributes(groupName)
            for catName, atName in attributeTupList:
                searchableCategoryD.setdefault(catName, []).append(atName)
        logger.debug("Searchable category count (%d)", len(searchableCategoryD))
        #
        subcatNestedD = {}
        tD = self.__docHelper.getAllSubCategoryNestedContexts()
        for k, v in tD.items():
            for kk, vv in v.items():
                if kk in subcatNestedD:
                    logger.warning("Duplicate nested subcategory specifications in %r %r", k, kk)
                # only take cases with an context path ...
                if "FIRST_CONTEXT_PATH" in vv:
                    subcatNestedD[kk[0]] = (kk[1], vv)
        #  cat = (subcat, {nested context dict})
        #
        retD = {}
        for catName in searchableCategoryD:
            if catName not in subcatNestedD:
                continue
            subCatName, nestedContextD = subcatNestedD[catName]
            #
            contextPath = nestedContextD["FIRST_CONTEXT_PATH"] if "FIRST_CONTEXT_PATH" in nestedContextD else None
            if not contextPath:
                logger.warning("Missing context path for nested category %s", catName)
                continue
            #
            if contextPath.count(".") > 1:
                k = contextPath.rfind(".")
                contextPath = contextPath[:k] + "_" + contextPath[k + 1 :]
            logger.debug("%s subcategory %s context path %r", catName, subCatName, contextPath)
            contextName = nestedContextD["CONTEXT_NAME"]
            cpCatName = contextPath.split(".")[0]
            cpAtName = contextPath.split(".")[1]
            nestedPathSearchContext = self.__docHelper.getSearchContexts(cpCatName, cpAtName)
            logger.debug("Nested (%r) context path for %r %r", contextName, cpCatName, cpAtName)
            if not nestedPathSearchContext:
                logger.warning("Missing nested (%r) search context for %r %r", contextName, cpCatName, cpAtName)
            #
            nfD = afD[cpCatName][cpAtName] if cpCatName in afD and cpAtName in afD[cpCatName] else {}
            logger.debug("FeatureD %r", nfD)
            # --
            enumMapD = {}
            enumDL = nfD["ENUMS_ANNOTATED"]
            if not enumDL:
                logger.warning("Missing nested enums %s.%s", cpCatName, cpAtName)
            else:
                logger.debug("All context enums count %d", len(enumDL))
                for enumD in enumDL:
                    if "name" not in enumD:
                        logger.warning("Missing nested enum (name) for %s.%s", cpCatName, cpAtName)
                #
                enumMapD = {enumD["value"]: enumD["name"] if "name" in enumD else enumD["detail"] for enumD in enumDL}
            # --
            nestedDescriptionText = self.__docHelper.getAttributeDescription(cpCatName, cpAtName, contextType="brief")
            if not nestedDescriptionText:
                logger.warning("Missing brief nested description %s.%s", cpCatName, cpAtName)
            else:
                logger.debug("Nested context description: %r", nestedDescriptionText)
                # --
            cvDL = nestedContextD["CONTEXT_ATTRIBUTE_VALUES"] if "CONTEXT_ATTRIBUTE_VALUES" in nestedContextD else []
            #
            if not cvDL:
                logger.warning("Missing context attribute values for %s", catName)
                # if no context values defined then use: all enums x searchable attributes in this category
                #
                # Template:  enum detail + search attribute brief description text
                for enumD in enumDL:
                    for atName in searchableCategoryD[catName]:
                        nnfD = afD[catName][atName]
                        subCatL = [d["id"] for d in nnfD["SUB_CATEGORIES"]] if "SUB_CATEGORIES" in nnfD else None
                        logger.debug("%s.%s %s subCatL %r", catName, atName, subCatName, subCatL)
                        if subCatL and subCatName in subCatL:
                            briefDescr = self.__docHelper.getAttributeDescription(catName, atName, contextType="brief")
                            tS = enumD["detail"] + " " + briefDescr
                            retD.setdefault((catName, atName), []).append(tS)
            else:
                # Only use context values from the full enum list with specified search paths.
                #
                # Template:  context value (enum detail) + search path attribute (brief description text)
                #  cVDL.append({"CONTEXT_VALUE": tD["CONTEXT_VALUE"], "SEARCH_PATHS": tD["SEARCH_PATHS"]})
                #
                for cvD in cvDL:
                    enumV = cvD["CONTEXT_VALUE"]
                    enumDetail = enumMapD[enumV] if enumV in enumMapD else None
                    if not enumDetail:
                        logger.warning("%s %s missing detail for enum value %s", catName, cpAtName, enumV)
                    for sp in cvD["SEARCH_PATHS"]:
                        if sp.count(".") > 1:
                            k = sp.rfind(".")
                            sp = sp[:k] + "_" + sp[k + 1 :]
                        cnS = sp.split(".")[0]
                        anS = sp.split(".")[1]
                        briefDescr = self.__docHelper.getAttributeDescription(cnS, anS, contextType="brief")
                        tS = enumDetail + " " + briefDescr
                        retD.setdefault((cnS, anS), []).append(tS)
                    for aD in cvD["ATTRIBUTES"]:
                        sp = aD["PATH"]
                        if sp.count(".") > 1:
                            k = sp.rfind(".")
                            sp = sp[:k] + "_" + sp[k + 1 :]
                        cnS = sp.split(".")[0]
                        anS = sp.split(".")[1]
                        briefDescr = self.__docHelper.getAttributeDescription(cnS, anS, contextType="brief")
                        tS = enumDetail + " " + briefDescr
                        retD.setdefault((cnS, anS), []).append(tS)
                        exL = aD["EXAMPLES"]
                        logger.debug("%s,%s sp %r exL %r", cnS, anS, sp, exL)
        #
        for k, vL in retD.items():
            for v in vL:
                logger.debug("%s : %r", k, v)
        #
        return retD

    def __getContentFeatures(self):
        """Get category and attribute features"""
        try:
            cH = ContentDefinitionHelper(cfgOb=self.__cfgOb)
            dictApi = self.__dP.getApiByLocators(dictLocators=[self.__pathPdbxDictionaryFile, self.__pathRcsbDictionaryFile])
            # logger.info("units = %r", dictApi.getUnits("pdbx_nmr_spectrometer", "manufacturer"))
            sdi = ContentDefinition(dictApi, databaseName="pdbx_core", contentDefHelper=cH)
            catNameL = sdi.getCategories()
            cfD = {}
            afD = {}
            for catName in catNameL:
                cfD[catName] = sdi.getCategoryFeatures(catName)
                afD[catName] = sdi.getAttributeFeatures(catName)
            #
            return cfD, afD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None, None


def schemaSearchGroupSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaSearchContextsTests("testSearchGroups"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = schemaSearchGroupSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
