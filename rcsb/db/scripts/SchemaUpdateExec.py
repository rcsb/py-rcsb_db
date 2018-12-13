##
# File: SchemaUpdateExec.py
# Date: 11-Nov-2018  jdw
#
#  Execution wrapper  --  for schema production utilities --
#
#  Updates:
#   13-Dec-2018 jdw add Drugbank and I/HM schema options
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
import logging
import os

from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.IoUtil import IoUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


class SchemaUpdateWorker(object):

    def __init__(self, configPath, schemaLevel='full', schemaDirPath=None, mockTopPath=None):
        self.__configPath = configPath
        self.__schemaDirPath = schemaDirPath
        self.__mockTopPath = mockTopPath
        self.__schemaLevel = schemaLevel
        #
        if schemaLevel == 'full':
            self.__enforceOpts = "mandatoryKeys|mandatoryAttributes|bounds|enums"
        else:
            self.__enforceOpts = "mandatoryKeys|enums"

    def buildRcsbSchema(self, schemaName):
        try:
            #
            smb = SchemaDefBuild(schemaName, self.__configPath, mockTopPath=self.__mockTopPath)
            for applicationName in ['ANY', 'SQL']:
                if self.__schemaDirPath:
                    pathSchemaDefJson = os.path.join(self.__schemaDirPath, 'schema_def-%s-%s.json' % (schemaName, applicationName))
                elif self.__mockTopPath:
                    pathSchemaDefJson = os.path.join(self.__mockTopPath, 'schema', 'schema_def-%s-%s.json' % (schemaName, applicationName))
                else:
                    logger.error("No RCSB schema path can be defined for %s" % schemaName)
                    return False
                cD = smb.build(applicationName=applicationName, schemaType='rcsb')
                #
                ioU = IoUtil()
                ioU.serialize(pathSchemaDefJson, cD, format='json', indent=3)
            return True
        except Exception as e:
            logger.exception("rcsb schema generation for %r failing with %s" % (schemaName, str(e)))
        return False

    def buildJsonSchema(self, schemaName, collectionName):
        try:
            #
            if self.__schemaDirPath:
                pathSchemaDefJson = os.path.join(self.__schemaDirPath, 'json-schema-%s-%s.json' % (self.__schemaLevel, collectionName))
            elif self.__mockTopPath:
                pathSchemaDefJson = os.path.join(self.__mockTopPath, 'json', 'json-schema-%s-%s.json' % (self.__schemaLevel, collectionName))
            else:
                logger.error("No JSON schema path defined for %s" % schemaName)
                return False
            #
            logger.info("Building schema %s %s in %s" % (schemaName, collectionName, pathSchemaDefJson))
            #
            smb = SchemaDefBuild(schemaName, self.__configPath, mockTopPath=self.__mockTopPath)
            cD = smb.build(collectionName, applicationName='JSON', schemaType='JSON', enforceOpts=self.__enforceOpts)
            #
            ioU = IoUtil()
            ioU.serialize(pathSchemaDefJson, cD, format='json', indent=3)
            return True
        except Exception as e:
            logger.exception("%r %r failing with %s" % (schemaName, collectionName, str(e)))
        return False

    def buildBsonSchema(self, schemaName, collectionName):
        try:

            if self.__schemaDirPath:
                pathSchemaDefBson = os.path.join(self.__schemaDirPath, 'bson-schema-%s-%s.json' % (self.__schemaLevel, collectionName))
            elif self.__mockTopPath:
                pathSchemaDefBson = os.path.join(self.__mockTopPath, 'json', 'bson-schema-%s-%s.json' % (self.__schemaLevel, collectionName))
            else:
                logger.error("No JSON schema path defined for %s" % schemaName)
                return False
            #
            smb = SchemaDefBuild(schemaName, self.__configPath, mockTopPath=self.__mockTopPath)
            cD = smb.build(collectionName, applicationName='BSON', schemaType='BSON', enforceOpts=self.__enforceOpts)
            #
            ioU = IoUtil()
            ioU.serialize(pathSchemaDefBson, cD, format='json', indent=3)

        except Exception as e:
            logger.exception("%r %r failing with %s" % (schemaName, collectionName, str(e)))
        return False


def main():
    parser = argparse.ArgumentParser()
    #
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
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default="site_info", help="Configuration section name")
    #
    parser.add_argument("--schema_dirpath", default=None, help="Output schema directory path")
    parser.add_argument("--schema_format", default=None, help="Schema encoding (rcsb|json|bson)")
    parser.add_argument("--schema_level", default=None, help="Schema validation level (full|min default=None)")
    #
    parser.add_argument("--debug", default=False, action='store_true', help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action='store_true', help="Use MOCK repository configuration for dependencies and testing")
    parser.add_argument("--working_path", default=None, help="Working path for temporary files")
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
    schemaFormat = args.schema_format
    schemaLevel = args.schema_level if args.schema_level in ['min', 'full'] else None

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
        cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
    except Exception as e:
        logger.error("Missing or access issue with config file %r with %s" % (configPath, str(e)))
        exit(1)
    #
    suw = SchemaUpdateWorker(configPath, schemaLevel, schemaDirPath=schemaDirPath, mockTopPath=mockTopPath)
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

    scnD = cfgOb.get('schema_collection_names', sectionName='document_helper')
    #
    logger.debug("Collections %s" % (list(scnD.items())))
    logger.debug("schemaNameList %s" % schemaNameList)

    for schemaName in schemaNameList:

        if schemaFormat == 'rcsb':
            logger.info("Creating RCSB schema for content type %s" % schemaName)
            suw.buildRcsbSchema(schemaName)
        if schemaName in scnD:
            for collectionName in scnD[schemaName]:
                logger.info("Creating Exchange JSON schema for content type %s %s" % (schemaName, collectionName))
                if schemaFormat == 'json':
                    logger.info("Creating Exchange JSON schema for content type %s %s" % (schemaName, collectionName))
                    suw.buildJsonSchema(schemaName, collectionName)
                elif schemaFormat == 'bson':
                    logger.info("Creating Exchange BSON schema for content type %s %s" % (schemaName, collectionName))
                    suw.buildBsonSchema(schemaName, collectionName)


if __name__ == '__main__':
    main()
