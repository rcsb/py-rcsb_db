##
# File: SchemaUpdateExec.py
# Date: 11-Nov-2018  jdw
#
#  Execution wrapper  --  for schema production utilities --
#
#  Updates:
#   13-Dec-2018 jdw add Drugbank and I/HM schema options
#    7-Jan-2019 jdw overhaul
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
import logging
import os

from rcsb.db.utils.SchemaDefUtil import SchemaDefUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


def main():
    parser = argparse.ArgumentParser()
    #
    defaultConfigName = 'site_info'
    #
    parser.add_argument("--update_chem_comp_ref", default=False, action='store_true', help="Update schema for Chemical Component reference definitions")
    parser.add_argument("--update_chem_comp_core_ref", default=False, action='store_true', help="Update core schema for Chemical Component reference definitions")
    parser.add_argument("--update_bird_chem_comp_ref", default=False, action='store_true', help="Update schema for Bird Chemical Component reference definitions")
    parser.add_argument("--update_bird_chem_comp_core_ref", default=False, action='store_true', help="Update core schema for Bird Chemical Component reference definitions")

    parser.add_argument("--update_bird_ref", default=False, action='store_true', help="Update schema for Bird reference definitions")
    parser.add_argument("--update_bird_family_ref", default=False, action='store_true', help="Update schema for Bird Family reference definitions")

    parser.add_argument("--update_pdbx", default=False, action='store_true', help="Update schema for PDBx entry data")
    parser.add_argument("--update_pdbx_core", default=False, action='store_true', help="Update schema for PDBx core entry/entity data")
    #
    parser.add_argument("--update_repository_holdings", default=False, action='store_true', help="Update schema for repository holdings")
    parser.add_argument("--update_entity_sequence_clusters", default=False, action='store_true', help="Update schema for entity sequence clusters")
    parser.add_argument("--update_data_exchange", default=False, action='store_true', help="Update schema for data exchange status")
    parser.add_argument("--update_ihm_dev", default=False, action='store_true', help="Update schema for I/HM dev entry data")
    parser.add_argument("--update_drugbank_core", default=False, action='store_true', help="Update DrugBank schema")
    #
    parser.add_argument("--update_config_all", default=False, action='store_true', help="Update using configuration settings (e.g. SCHEMA_NAMES_ALL)")
    parser.add_argument("--update_config_deployed", default=False, action='store_true', help="Update using configuration settings (e.g. SCHEMA_NAMES_DEPLOYED)")
    parser.add_argument("--update_config_test", default=False, action='store_true', help="Update using configuration settings (e.g. SCHEMA_NAMES_TEST)")
    #
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default=defaultConfigName, help="Configuration section name")
    #
    parser.add_argument("--schema_dirpath", default=None, help="Output schema directory path (overrides configuration settings)")
    parser.add_argument("--schema_types", default=None, help="Schema encoding (rcsb|json|bson) (comma separated)")
    parser.add_argument("--schema_levels", default=None, help="Schema validation level (full|min) (comma separated)")
    #
    parser.add_argument("--debug", default=False, action='store_true', help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action='store_true', help="Use MOCK repository configuration for dependencies and testing")
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
    schemaDirPath = args.schema_dirpath
    #
    schemaTypes = args.schema_types.split(',') if args.schema_types else []
    schemaLevels = args.schema_levels.split(',') if args.schema_levels else []
    dataTypingList = ['ANY', 'SQL']

    if not configPath:
        configPath = os.getenv('DBLOAD_CONFIG_PATH', None)
    try:
        if os.access(configPath, os.R_OK):
            os.environ['DBLOAD_CONFIG_PATH'] = configPath
            logger.info("Using configuation path %s (%s)" % (configPath, configName))
        else:
            logger.error("Missing or access issue with config file %r" % configPath)
            exit(1)
        mockTopPath = os.path.join(TOPDIR, 'rcsb', 'mock-data') if args.mock else None
        cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=defaultConfigName, mockTopPath=mockTopPath)
        if configName != defaultConfigName:
            cfgOb.replaceSectionName(defaultConfigName, configName)
    except Exception as e:
        logger.error("Missing or access issue with config file %r with %s" % (configPath, str(e)))
        exit(1)
    #
    schemaNameList = []
    if args.update_chem_comp_ref:
        schemaNameList.append('chem_comp')

    if args.update_bird_chem_comp_ref:
        schemaNameList.append('bird_chem_comp')

    if args.update_chem_comp_core_ref:
        schemaNameList.append('chem_comp_core')

    if args.update_bird_chem_comp_core_ref:
        schemaNameList.append('bird_chem_comp_core')

    if args.update_bird_ref:
        schemaNameList.append('bird')

    if args.update_bird_family_ref:
        schemaNameList.append('bird_family')

    if args.update_pdbx:
        schemaNameList.append('pdbx')

    if args.update_pdbx_core:
        schemaNameList.append('pdbx_core')

    if args.update_repository_holdings:
        schemaNameList.append('repository_holdings')

    if args.update_entity_sequence_clusters:
        schemaNameList.append('entity_sequence_clusters')

    if args.update_data_exchange:
        schemaNameList.append('data_exchange')

    if args.update_ihm_dev:
        schemaNameList.append('ihm_dev')

    if args.update_drugbank_core:
        schemaNameList.append('drugbank_core')

    if args.update_config_deployed:
        schemaNameList = cfgOb.getList('SCHEMA_NAMES_DEPLOYED', sectionName='schema_catalog_info')
        dataTypingList = cfgOb.getList('DATATYPING_DEPLOYED', sectionName='schema_catalog_info')
        schemaLevels = cfgOb.getList('SCHEMA_LEVELS_DEPLOYED', sectionName='schema_catalog_info')
        schemaTypes = cfgOb.getList('SCHEMA_TYPES_DEPLOYED', sectionName='schema_catalog_info')

    if args.update_config_all:
        schemaNameList = cfgOb.getList('SCHEMA_NAMES_ALL', sectionName='schema_catalog_info')
        dataTypingList = cfgOb.getList('DATATYPING_ALL', sectionName='schema_catalog_info')
        schemaLevels = cfgOb.getList('SCHEMA_LEVELS_ALL', sectionName='schema_catalog_info')
        schemaTypes = cfgOb.getList('SCHEMA_TYPES_ALL', sectionName='schema_catalog_info')

    if args.update_config_test:
        schemaNameList = cfgOb.getList('SCHEMA_NAMES_TEST', sectionName='schema_catalog_info')
        dataTypingList = cfgOb.getList('DATATYPING_TEST', sectionName='schema_catalog_info')
        schemaLevels = cfgOb.getList('SCHEMA_LEVELS_TEST', sectionName='schema_catalog_info')
        schemaTypes = cfgOb.getList('SCHEMA_TYPES_TEST', sectionName='schema_catalog_info')
    #
    scnD = cfgOb.get('schema_collection_names', sectionName='document_helper')
    #
    schemaNameList = list(set(schemaNameList))
    logger.debug("Collections %s" % (list(scnD.items())))
    logger.debug("schemaNameList %s" % schemaNameList)

    sdu = SchemaDefUtil(cfgOb)
    for schemaName in schemaNameList:
        for schemaType in schemaTypes:
            if schemaType == 'rcsb':
                for dataTyping in dataTypingList:
                    logger.info("Creating schema definition for content type %s data typing %s" % (schemaName, dataTyping))
                    sdu.makeSchemaDef(schemaName, dataTyping=dataTyping, saveSchema=True, altDirPath=schemaDirPath)
            else:
                if schemaName in scnD:
                    for d in scnD[schemaName]:
                        collectionName = d['NAME']
                        for schemaLevel in schemaLevels:
                            logger.info("Creating %r schema for content type %s collection %s" % (schemaType, schemaName, collectionName))
                            sdu.makeSchema(schemaName, collectionName, schemaType=schemaType, level=schemaLevel, saveSchema=True, altDirPath=schemaDirPath)


if __name__ == '__main__':
    main()
