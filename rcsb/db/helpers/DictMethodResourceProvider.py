##
# File:    DictMethodResourceProvider.py
# Author:  J. Westbrook
# Date:    3-Jun-2019
# Version: 0.001 Initial version
#
#
# Updates:
#
##
"""
Resource provider for DictMethodHelper tools.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.utils.SingletonClass import SingletonClass
from rcsb.utils.ec.EnzymeDatabaseUtils import EnzymeDatabaseUtils
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.struct.CathClassificationUtils import CathClassificationUtils
from rcsb.utils.struct.ScopClassificationUtils import ScopClassificationUtils
from rcsb.utils.taxonomy.TaxonomyUtils import TaxonomyUtils

logger = logging.getLogger(__name__)


class DictMethodResourceProvider(SingletonClass):
    """ Resource provider for DictMethodHelper tools.

    """

    # Dictionary of current standard monomers -
    monDict3 = {
        "ALA": "A",
        "ARG": "R",
        "ASN": "N",
        "ASP": "D",
        "ASX": "B",
        "CYS": "C",
        "GLN": "Q",
        "GLU": "E",
        "GLX": "Z",
        "GLY": "G",
        "HIS": "H",
        "ILE": "I",
        "LEU": "L",
        "LYS": "K",
        "MET": "M",
        "PHE": "F",
        "PRO": "P",
        "SER": "S",
        "THR": "T",
        "TRP": "W",
        "TYR": "Y",
        "VAL": "V",
        "PYL": "O",
        "SEC": "U",
        "DA": "A",
        "DC": "C",
        "DG": "G",
        "DT": "T",
        "DU": "U",
        "DI": "I",
        "A": "A",
        "C": "C",
        "G": "G",
        "I": "I",
        "N": "N",
        "T": "T",
        "U": "U",
        # "UNK": "X",
        # "MSE":"M",
        # ".": "."
    }

    def __init__(self, cfgOb, **kwargs):
        """ Resource provider for dictionary method runner.

        Arguments:
            cfgOb {object} -- instance ConfigUtils class

        Keyword agruments:
            configName {string} -- configuration section name (default: default section name)
            workPath {str} -- path used for temporary file management (default: '.')

        """
        self.__cfgOb = cfgOb
        self.__cfgOb.getDefaultSectionName()
        self.__configName = kwargs.get("configName", self.__cfgOb.getDefaultSectionName())
        self.__workPath = kwargs.get("workPath", ".")
        #
        # self.__pathPdbxDictionaryFile = self.__cfgOb.getPath("PDBX_DICT_LOCATOR", sectionName=configName)
        # self.__pathRcsbDictionaryFile = self.__cfgOb.getPath("RCSB_DICT_LOCATOR", sectionName=configName)
        #
        # self.__siftsMappingFilePath = self.__cfgOb.getPath("SIFTS_SUMMARY_PATH", sectionName=configName)
        # self.__siftsMappingDict = {}
        #
        self.__drugBankMappingDict = {}
        self.__csdModelMappingDict = {}
        self.__taxU = None
        self.__ecU = None
        self.__scopU = None
        self.__cathU = None
        #
        # self.__wsPattern = re.compile(r"\s+", flags=re.UNICODE | re.MULTILINE)
        # self.__re_non_digit = re.compile(r"[^\d]+")
        #
        logger.info("Dictionary resource provider init completed")
        #

    def echo(self, msg):
        logger.info(msg)

    def getResource(self, resourceName, default=None, **kwargs):
        """ Return the named input resource or the default value.

        Arguments:
            resourceName {str} -- resource name

        Keyword Arguments:
            default {obj} -- default return value for missing resources (default: {None})

        Returns:
            [obj] -- resource object
        """
        resourcesD = {
            "TaxonomyUtils instance": self.__fetchTaxonomyUtils,
            "ScopUtils instance": self.__fetchScopUtils,
            "CathUtils instance": self.__fetchCathUtils,
            "EnzymeUtils instance": self.__fetchEnzymeUtils,
            "DrugBank accession mapping": self.__fetchDrugBankMapping,
            "CCDC accession mapping": self.__fetchCsdModelMapping,
            "monomer one-letter code mapping": self.__fetchMonomerCodes,
        }
        if resourceName in resourcesD:
            return resourcesD[resourceName](self.__cfgOb, self.__configName, self.__workPath, **kwargs)
        else:
            logger.error("Request for unsupported resource %r returning %r", resourceName, default)
        #
        return default

    def __fetchMonomerCodes(self, cfgOb, configName, workPath, **kwargs):
        logger.debug("Default %r configName %s workPath %s kwargs %r", cfgOb.getDefaultSectionName(), configName, workPath, kwargs)
        return DictMethodResourceProvider.monDict3

    def __fetchTaxonomyUtils(self, cfgOb, configName, workPath, **kwargs):
        logger.debug("configName %s workPath %s kwargs %r", configName, workPath, kwargs)
        if not self.__taxU:
            taxonomyDataPath = cfgOb.getPath("NCBI_TAXONOMY_PATH", sectionName=configName)
            self.__taxU = TaxonomyUtils(taxDirPath=taxonomyDataPath)
        return self.__taxU

    def __fetchScopUtils(self, cfgOb, configName, workPath, **kwargs):
        logger.debug("configName %s workPath %s kwargs %r", configName, workPath, kwargs)
        if not self.__scopU:
            structDomainDataPath = cfgOb.getPath("STRUCT_DOMAIN_CLASSIFICATION_DATA_PATH", sectionName=configName)
            self.__scopU = ScopClassificationUtils(scopDirPath=structDomainDataPath, useCache=True)
        return self.__scopU

    def __fetchCathUtils(self, cfgOb, configName, workPath, **kwargs):
        logger.debug("configName %s workPath %s kwargs %r", configName, workPath, kwargs)
        if not self.__cathU:
            structDomainDataPath = cfgOb.getPath("STRUCT_DOMAIN_CLASSIFICATION_DATA_PATH", sectionName=configName)
            self.__cathU = CathClassificationUtils(cathDirPath=structDomainDataPath, useCache=True)
        return self.__cathU

    def __fetchEnzymeUtils(self, cfgOb, configName, workPath, **kwargs):
        logger.debug("configName %s workPath %s kwargs %r", configName, workPath, kwargs)
        if not self.__ecU:
            enzymeDataPath = cfgOb.getPath("ENZYME_CLASSIFICATION_DATA_PATH", sectionName=configName)
            self.__ecU = EnzymeDatabaseUtils(enzymeDirPath=enzymeDataPath, useCache=True, clearCache=False)
        return self.__ecU

    #
    def __fetchDrugBankMapping(self, cfgOb, configName, workPath, **kwargs):
        logger.debug("configName %s workPath %s kwargs %r", configName, workPath, kwargs)
        if not self.__drugBankMappingDict:
            try:
                drugBankMappingFile = cfgOb.getPath("DRUGBANK_MAPPING_LOCATOR", sectionName=configName)
                mU = MarshalUtil(workPath=workPath)
                self.__drugBankMappingDict = mU.doImport(drugBankMappingFile, fmt="json")
                logger.debug("Fetching DrugBank mapping length %d", len(self.__drugBankMappingDict))
            except Exception as e:
                logger.exception("For %s failing with %s", drugBankMappingFile, str(e))
        return self.__drugBankMappingDict

    def __fetchCsdModelMapping(self, cfgOb, configName, workPath, **kwargs):
        """
        """
        logger.debug("configName %s workPath %s kwargs %r", configName, workPath, kwargs)
        if not self.__csdModelMappingDict:
            try:
                csdModelMappingFile = cfgOb.getPath("CCDC_MAPPING_LOCATOR", sectionName=configName)
                mU = MarshalUtil(workPath=workPath)
                self.__csdModelMappingDict = mU.doImport(csdModelMappingFile, fmt="json")
                logger.debug("Fetching CSD model length %d", len(self.__csdModelMappingDict))
            except Exception as e:
                logger.exception("For %s failing with %s", csdModelMappingFile, str(e))

        return self.__csdModelMappingDict
