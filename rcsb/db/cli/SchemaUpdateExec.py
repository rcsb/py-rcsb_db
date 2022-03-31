##
# File: SchemaUpdateExec.py
# Date: 11-Nov-2018  jdw
#
#  Execution wrapper  --  for schema production utilities --
#
#  Updates:
#   13-Dec-2018 jdw add Drugbank and I/HM schema options
#    7-Jan-2019 jdw overhaul
#   23-Nov-2021 dwp Add pdbx_comp_model_core
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
import logging
import os

from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def main():
    parser = argparse.ArgumentParser()
    #
    defaultConfigName = "site_info_configuration"
    #
    parser.add_argument("--update_chem_comp_ref", default=False, action="store_true", help="Update schema for Chemical Component reference definitions")
    parser.add_argument("--update_chem_comp_core_ref", default=False, action="store_true", help="Update core schema for Chemical Component reference definitions")
    parser.add_argument("--update_bird_chem_comp_ref", default=False, action="store_true", help="Update schema for Bird Chemical Component reference definitions")
    parser.add_argument("--update_bird_chem_comp_core_ref", default=False, action="store_true", help="Update core schema for Bird Chemical Component reference definitions")

    parser.add_argument("--update_bird_ref", default=False, action="store_true", help="Update schema for Bird reference definitions")
    parser.add_argument("--update_bird_family_ref", default=False, action="store_true", help="Update schema for Bird Family reference definitions")

    parser.add_argument("--update_pdbx", default=False, action="store_true", help="Update schema for PDBx entry data")
    parser.add_argument("--update_pdbx_core", default=False, action="store_true", help="Update schema for PDBx core entry/entity data")
    parser.add_argument("--update_pdbx_comp_model_core", default=False, action="store_true", help="Update schema for PDBx computational model core entry/entity data")
    #
    parser.add_argument("--update_repository_holdings", default=False, action="store_true", help="Update schema for repository holdings")
    parser.add_argument("--update_entity_sequence_clusters", default=False, action="store_true", help="Update schema for entity sequence clusters")
    parser.add_argument("--update_data_exchange", default=False, action="store_true", help="Update schema for data exchange status")
    parser.add_argument("--update_ihm_dev", default=False, action="store_true", help="Update schema for I/HM dev entry data")
    parser.add_argument("--update_drugbank_core", default=False, action="store_true", help="Update DrugBank schema")
    #
    parser.add_argument("--update_config_all", default=False, action="store_true", help="Update using configuration settings (e.g. DATABASE_NAMES_ALL)")
    parser.add_argument("--update_config_deployed", default=False, action="store_true", help="Update using configuration settings (e.g. DATABASE_NAMES_DEPLOYED)")
    parser.add_argument("--update_config_test", default=False, action="store_true", help="Update using configuration settings (e.g. DATABASE_NAMES_TEST)")
    #
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default=defaultConfigName, help="Configuration section name")
    #
    parser.add_argument("--cache_path", default=None, help="Schema cache directory path")
    parser.add_argument("--encoding_types", default=None, help="Schema encoding (rcsb|json|bson) (comma separated)")
    parser.add_argument("--validation_levels", default=None, help="Schema validation level (full|min) (comma separated)")
    parser.add_argument("--compare_only", default=False, action="store_true", help="Perform comparison with cached schema")
    #
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action="store_true", help="Use MOCK repository configuration for dependencies and testing")
    # parser.add_argument("--working_path", default=None, help="Working/alternative path for temporary and schema files")
    args = parser.parse_args()
    #
    debugFlag = args.debug
    if debugFlag:
        logger.setLevel(logging.DEBUG)
    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    #                                       Configuration Details
    configPath = args.config_path
    configName = args.config_name
    cachePath = args.cache_path
    compareOnly = args.compare_only
    #
    encodingTypes = args.encoding_types.split(",") if args.encoding_types else []
    validationLevels = args.validation_levels.split(",") if args.validation_levels else []
    dataTypingList = ["ANY", "SQL"]

    if not configPath:
        configPath = os.getenv("DBLOAD_CONFIG_PATH", None)
    try:
        if os.access(configPath, os.R_OK):
            os.environ["DBLOAD_CONFIG_PATH"] = configPath
            logger.info("Using configuation path %s (%s)", configPath, configName)
        else:
            logger.error("Missing or access issue with config file %r", configPath)
            exit(1)
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data") if args.mock else None
        cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=defaultConfigName, mockTopPath=mockTopPath)
        if configName != defaultConfigName:
            cfgOb.replaceSectionName(defaultConfigName, configName)
    except Exception as e:
        logger.error("Missing or access issue with config file %r with %s", configPath, str(e))
        exit(1)
    #
    databaseNameList = []
    if args.update_chem_comp_ref:
        databaseNameList.append("chem_comp")

    if args.update_bird_chem_comp_ref:
        databaseNameList.append("bird_chem_comp")

    if args.update_chem_comp_core_ref:
        databaseNameList.append("chem_comp_core")

    if args.update_bird_chem_comp_core_ref:
        databaseNameList.append("bird_chem_comp_core")

    if args.update_bird_ref:
        databaseNameList.append("bird")

    if args.update_bird_family_ref:
        databaseNameList.append("bird_family")

    if args.update_pdbx:
        databaseNameList.append("pdbx")

    if args.update_pdbx_core:
        databaseNameList.append("pdbx_core")

    if args.update_pdbx_comp_model_core:
        databaseNameList.append("pdbx_comp_model_core")

    if args.update_repository_holdings:
        databaseNameList.append("repository_holdings")

    if args.update_entity_sequence_clusters:
        databaseNameList.append("sequence_clusters")

    if args.update_data_exchange:
        databaseNameList.append("data_exchange")

    if args.update_ihm_dev:
        databaseNameList.append("ihm_dev")

    if args.update_drugbank_core:
        databaseNameList.append("drugbank_core")

    if args.update_config_deployed:
        databaseNameList = cfgOb.getList("DATABASE_NAMES_DEPLOYED", sectionName="database_catalog_configuration")
        dataTypingList = cfgOb.getList("DATATYPING_DEPLOYED", sectionName="database_catalog_configuration")
        validationLevels = cfgOb.getList("VALIDATION_LEVELS_DEPLOYED", sectionName="database_catalog_configuration")
        encodingTypes = cfgOb.getList("ENCODING_TYPES_DEPLOYED", sectionName="database_catalog_configuration")

    if args.update_config_all:
        databaseNameList = cfgOb.getList("DATABASE_NAMES_ALL", sectionName="database_catalog_configuration")
        dataTypingList = cfgOb.getList("DATATYPING_ALL", sectionName="database_catalog_configuration")
        validationLevels = cfgOb.getList("VALIDATION_LEVELS_ALL", sectionName="database_catalog_configuration")
        encodingTypes = cfgOb.getList("ENCODING_TYPES_ALL", sectionName="database_catalog_configuration")

    if args.update_config_test:
        databaseNameList = cfgOb.getList("DATABASE_NAMES_TEST", sectionName="database_catalog_configuration")
        dataTypingList = cfgOb.getList("DATATYPING_TEST", sectionName="database_catalog_configuration")
        validationLevels = cfgOb.getList("VALIDATION_LEVELS_TEST", sectionName="database_catalog_configuration")
        encodingTypes = cfgOb.getList("ENCODING_TYPES_TEST", sectionName="database_catalog_configuration")
    #
    scnD = cfgOb.get("document_collection_names", sectionName="document_helper_configuration")
    #
    databaseNameList = list(set(databaseNameList))
    logger.debug("Collections %s", list(scnD.items()))
    logger.debug("databaseNameList %s", databaseNameList)

    if compareOnly:
        schP = SchemaProvider(cfgOb, cachePath, useCache=True)
        difPathList = []
        for databaseName in databaseNameList:
            for dataTyping in dataTypingList:
                logger.debug("Building schema %s with types %s", databaseName, dataTyping)
                pth = schP.schemaDefCompare(databaseName, dataTyping)
                if pth:
                    difPathList.append(pth)
        if difPathList:
            logger.info("Schema definition difference path list %r", difPathList)
        difPathList = []
        for databaseName in databaseNameList:
            dD = schP.makeSchemaDef(databaseName, dataTyping="ANY", saveSchema=False)
            sD = SchemaDefAccess(dD)
            for cd in sD.getCollectionInfo():
                collectionName = cd["NAME"]
                for encodingType in encodingTypes:
                    if encodingType.lower() != "json":
                        continue
                    for level in validationLevels:
                        pth = schP.jsonSchemaCompare(databaseName, collectionName, encodingType, level)
                        if pth:
                            difPathList.append(pth)
        if difPathList:
            logger.info("JSON schema difference path list %r", difPathList)

    else:
        schP = SchemaProvider(cfgOb, cachePath, useCache=False)
        for databaseName in databaseNameList:
            for encodingType in encodingTypes:
                if encodingType == "rcsb":
                    for dataTyping in dataTypingList:
                        logger.info("Creating schema definition for content type %s data typing %s", databaseName, dataTyping)
                        schP.makeSchemaDef(databaseName, dataTyping=dataTyping, saveSchema=True)
                else:
                    if databaseName in scnD:
                        for dD in scnD[databaseName]:
                            collectionName = dD["NAME"]
                            for validationLevel in validationLevels:
                                logger.info("Creating %r schema for content type %s collection %s", encodingType, databaseName, collectionName)
                                schP.makeSchema(databaseName, collectionName, encodingType=encodingType, level=validationLevel, saveSchema=True)


if __name__ == "__main__":
    main()
