##
# File:    DictMethodResourceProvider.py
# Author:  J. Westbrook
# Date:    3-Jun-2019
# Version: 0.001 Initial version
#
#
# Updates:
#  17-Jul-2019 jdw add resource for common utilities and dictionary api
#   7-Aug-2019 jdw use dictionary locator map
#  13-Aug-2019 jdw return class instances in all cases. Add cache management support.
#   9-Sep-2019 jdw add AtcProvider() and SiftsSummaryProvider()
#  25-Nov-2019 jdw add CitationReferenceProvider(), ChemCompProvider() and  JournalTitleAbbreviationProvider()'s
##
"""
Resource provider for DictMethodHelper tools.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.db.define.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
from rcsb.db.helpers.DictMethodCommonUtils import DictMethodCommonUtils
from rcsb.utils.chemref.AtcProvider import AtcProvider
from rcsb.utils.chemref.ChemCompModelProvider import ChemCompModelProvider
from rcsb.utils.chemref.ChemCompProvider import ChemCompProvider
from rcsb.utils.chemref.DrugBankProvider import DrugBankProvider
from rcsb.utils.citation.CitationReferenceProvider import CitationReferenceProvider
from rcsb.utils.citation.JournalTitleAbbreviationProvider import JournalTitleAbbreviationProvider
from rcsb.utils.ec.EnzymeDatabaseProvider import EnzymeDatabaseProvider
from rcsb.utils.io.SingletonClass import SingletonClass
from rcsb.utils.seq.SiftsSummaryProvider import SiftsSummaryProvider
from rcsb.utils.struct.CathClassificationProvider import CathClassificationProvider
from rcsb.utils.struct.ScopClassificationProvider import ScopClassificationProvider
from rcsb.utils.taxonomy.TaxonomyProvider import TaxonomyProvider
from rcsb.utils.validation.ValidationReportProvider import ValidationReportProvider

logger = logging.getLogger(__name__)


class DictMethodResourceProvider(SingletonClass):
    """ Resource provider for DictMethodHelper tools.

    """

    def __init__(self, cfgOb, **kwargs):
        """ Resource provider for dictionary method runner.

        Arguments:
            cfgOb {object} -- instance ConfigUtils class

        Keyword agruments:
            configName {string} -- configuration section name (default: default section name)
            cachePath {str} -- path used for temporary file management (default: '.')

        """
        self.__cfgOb = cfgOb

        self.__configName = kwargs.get("configName", self.__cfgOb.getDefaultSectionName())
        self.__cachePath = kwargs.get("cachePath", ".")
        #
        self.__drugBankMappingDict = {}
        self.__csdModelMappingDict = {}
        self.__taxU = None
        self.__ecU = None
        self.__scopU = None
        self.__cathU = None
        self.__dbU = None
        self.__ccU = None
        self.__ccmU = None
        self.__commonU = None
        self.__dApiW = None
        self.__atcP = None
        self.__siftsAbbreviated = kwargs.get("siftsAbbreviated", "PROD")
        self.__ssP = None
        self.__vrptP = None
        self.__crP = None
        self.__jtaP = None
        #
        #
        # self.__wsPattern = re.compile(r"\s+", flags=re.UNICODE | re.MULTILINE)
        # self.__re_non_digit = re.compile(r"[^\d]+")
        #
        self.__resourcesD = {
            "SiftsSummaryProvider instance": self.__fetchSiftsSummaryProvider,
            "Dictionary API instance (pdbx_core)": self.__fetchDictionaryApi,
            "TaxonomyProvider instance": self.__fetchTaxonomyProvider,
            "ScopProvider instance": self.__fetchScopProvider,
            "CathProvider instance": self.__fetchCathProvider,
            "EnzymeProvider instance": self.__fetchEnzymeProvider,
            "DrugBankProvider instance": self.__fetchDrugBankProvider,
            "ChemCompModelProvider instance": self.__fetchChemCompModelProvider,
            "ChemCompProvider instance": self.__fetchChemCompProvider,
            "AtcProvider instance": self.__fetchAtcProvider,
            "DictMethodCommonUtils instance": self.__fetchCommonUtils,
            "ValidationProvider instance": self.__fetchValidationProvider,
            "CitationReferenceProvider instance": self.__fetchCitationReferenceProvider,
            "JournalTitleAbbreviationProvider instance": self.__fetchJournalTitleAbbreviationProvider,
        }
        logger.debug("Dictionary resource provider init completed")
        #

    def echo(self, msg):
        logger.info(msg)

    def getResource(self, resourceName, default=None, useCache=True, **kwargs):
        """ Return the named input resource or the default value.

        Arguments:
            resourceName {str} -- resource name
            useCache (bool, optional): use current cace. Defaults to True.

        Keyword Arguments:
            default {obj} -- default return value for missing resources (default: {None})

        Returns:
            [obj] -- resource object
        """
        logger.debug("Requesting resource %r", resourceName)
        if resourceName in self.__resourcesD:
            return self.__resourcesD[resourceName](self.__cfgOb, self.__configName, self.__cachePath, useCache=useCache, **kwargs)
        else:
            logger.error("Request for unsupported resource %r returning %r", resourceName, default)
        #
        return default

    def cacheResources(self, useCache=False, **kwargs):
        """Update and optionally clear all resource caches.

        Args:
            useCache (bool, optional): use current cace. Defaults to False.

        Returns:
            bool: True for success or False otherwise
        """
        ret = True
        tName = "CHECKING" if useCache else "REBUILDING"
        logger.info("Begin %s cache for %d resources", tName, len(self.__resourcesD))
        #
        for resourceName in self.__resourcesD:
            logger.debug("Caching resources for %r", resourceName)
            tU = self.__resourcesD[resourceName](self.__cfgOb, self.__configName, self.__cachePath, useCache=useCache, **kwargs)
            ok = tU.testCache()
            if not ok:
                logger.error("%s %s fails", tName, resourceName)
            ret = ret and ok
            if not ret:
                logger.info("%s resource %r step status %r cumulative status %r", tName, resourceName, ok, ret)
        #
        logger.info("Completed %s %d resources with status %r", tName, len(self.__resourcesD), ret)
        return ret

    def __fetchCitationReferenceProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__crP:
            cachePath = os.path.join(cachePath, cfgOb.get("CITATION_REFERENCE_CACHE_DIR", sectionName=configName))
            self.__crP = CitationReferenceProvider(cachePath=cachePath, useCache=useCache, **kwargs)
        return self.__crP

    def __fetchJournalTitleAbbreviationProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__jtaP:
            cachePath = os.path.join(cachePath, cfgOb.get("CITATION_REFERENCE_CACHE_DIR", sectionName=configName))
            self.__jtaP = JournalTitleAbbreviationProvider(cachePath=cachePath, useCache=useCache, **kwargs)
        return self.__jtaP

    def __fetchTaxonomyProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__taxU:
            taxonomyDataPath = os.path.join(cachePath, cfgOb.get("NCBI_TAXONOMY_CACHE_DIR", sectionName=configName))
            self.__taxU = TaxonomyProvider(taxDirPath=taxonomyDataPath, useCache=useCache, **kwargs)
        return self.__taxU

    def __fetchScopProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__scopU:
            structDomainDataPath = os.path.join(cachePath, cfgOb.get("STRUCT_DOMAIN_CLASSIFICATION_CACHE_DIR", sectionName=configName))
            self.__scopU = ScopClassificationProvider(scopDirPath=structDomainDataPath, useCache=useCache, **kwargs)
        return self.__scopU

    def __fetchCathProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__cathU:
            structDomainDataPath = os.path.join(cachePath, cfgOb.get("STRUCT_DOMAIN_CLASSIFICATION_CACHE_DIR", sectionName=configName))
            self.__cathU = CathClassificationProvider(cathDirPath=structDomainDataPath, useCache=useCache, **kwargs)
        return self.__cathU

    def __fetchEnzymeProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__ecU:
            enzymeDataPath = os.path.join(cachePath, cfgOb.get("ENZYME_CLASSIFICATION_CACHE_DIR", sectionName=configName))
            self.__ecU = EnzymeDatabaseProvider(enzymeDirPath=enzymeDataPath, useCache=useCache, **kwargs)
        return self.__ecU

    #
    def __fetchDrugBankProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__dbU:
            dbDataPath = os.path.join(cachePath, cfgOb.get("DRUGBANK_CACHE_DIR", sectionName=configName))
            un = cfgOb.get("_DRUGBANK_AUTH_USERNAME", sectionName=configName)
            pw = cfgOb.get("_DRUGBANK_AUTH_PASSWORD", sectionName=configName)
            self.__dbU = DrugBankProvider(dirPath=dbDataPath, useCache=useCache, username=un, password=pw, **kwargs)
        return self.__dbU

    def __fetchChemCompModelProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__ccmU:
            dirPath = os.path.join(cachePath, cfgOb.get("CHEM_COMP_CACHE_DIR", sectionName=configName))
            self.__ccmU = ChemCompModelProvider(dirPath=dirPath, useCache=useCache, **kwargs)
        return self.__ccmU

    def __fetchChemCompProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__ccU:
            dirPath = os.path.join(cachePath, cfgOb.get("CHEM_COMP_CACHE_DIR", sectionName=configName))
            self.__ccU = ChemCompProvider(dirPath=dirPath, useCache=useCache, **kwargs)
        return self.__ccU

    def __fetchAtcProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__atcP:
            dirPath = os.path.join(cachePath, cfgOb.get("ATC_CACHE_DIR", sectionName=configName))
            self.__atcP = AtcProvider(dirPath=dirPath, useCache=useCache, **kwargs)
        return self.__atcP

    def __fetchSiftsSummaryProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__ssP:
            srcDirPath = os.path.join(cachePath, cfgOb.getPath("SIFTS_SUMMARY_DATA_PATH", sectionName=configName))
            cacheDirPath = os.path.join(cachePath, cfgOb.get("SIFTS_SUMMARY_CACHE_DIR", sectionName=configName))
            logger.debug("ssP %r %r", srcDirPath, cacheDirPath)
            self.__ssP = SiftsSummaryProvider(srcDirPath=srcDirPath, cacheDirPath=cacheDirPath, useCache=useCache, abbreviated=self.__siftsAbbreviated, **kwargs)
            logger.debug("ssP entry count %d", self.__ssP.getEntryCount())
        return self.__ssP

    def __fetchValidationProvider(self, cfgOb, configName, cachePath, useCache=True, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        if not self.__vrptP:
            dirPath = os.path.join(cachePath, cfgOb.get("DICTIONARY_CACHE_DIR", sectionName=configName))
            self.__vrptP = ValidationReportProvider(dirPath=dirPath, useCache=useCache)
        #
        return self.__vrptP

    def __fetchCommonUtils(self, cfgOb, configName, cachePath, useCache=None, **kwargs):
        logger.debug("configName %s cachePath %r kwargs %r", configName, cachePath, kwargs)
        _ = cfgOb
        _ = useCache
        if not self.__commonU:
            self.__commonU = DictMethodCommonUtils(**kwargs)
        return self.__commonU

    def __fetchDictionaryApi(self, cfgOb, configName, cachePath, useCache=None, **kwargs):
        logger.debug("configName %s cachePath %s kwargs %r", configName, cachePath, kwargs)
        schemaName = kwargs.get("schemaName", "pdbx_core")
        self.__dApiW = DictionaryApiProviderWrapper(cfgOb, cachePath, useCache=useCache)
        dictApi = self.__dApiW.getApiByName(schemaName)
        # numRev = dictApi.getDictionaryRevisionCount()
        return dictApi
