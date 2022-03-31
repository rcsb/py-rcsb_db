##
# File: RepoLoadExec.py
# Date: 15-Mar-2018  jdw
#
#  Execution wrapper  --  repository database loading utilities --
#  Updates:
#
#    21-Mar-2018 - jdw added content filters and separate collection for Bird chemical components
#    22-May-2018 - jdw add replacment load type, add options for input file paths
#    24-Mar-2018 - jdw add split collections for entries (preiliminary)
#    24-Mar-2018 - jdw add option to preserve paths from automatic repo scan
#    27-Mar-2018 - jdw update configuration handling and add support for mocking repository paths
#     4-Apr-2018 - jdw added option to prune documents to a size limit
#     3-Jul-2018 - jdw specialize for repository loading.
#    14-Jul-2018 - jdw add loading of separate
#    25-Jul-2018 - jdw die on input file path list processing error
#    20-Aug-2018 - jdw add load_pdbx_core load option
#     9-Sep-2018 - jdw expose --schema_level option
#     3-Dec-2018 - jdw add options to load specific core collections.
#    12-Dec-2018 - jdw add core_entity_monomer collection support
#    13-Dec-2018 - jdw add I/HM schema support
#    23-Nov-2021 - dwp Add pdbx_comp_model_core
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
import logging
import os
import sys

from rcsb.utils.dictionary.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.mongo.PdbxLoader import PdbxLoader
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def loadStatus(statusList, cfgOb, cachePath, readBackCheck=True):
    sectionName = "data_exchange_configuration"
    dl = DocumentLoader(cfgOb, cachePath, "MONGO_DB", numProc=2, chunkSize=2, documentLimit=None, verbose=False, readBackCheck=readBackCheck)
    #
    databaseName = cfgOb.get("DATABASE_NAME", sectionName=sectionName)
    # collectionVersion = cfgOb.get('COLLECTION_VERSION_STRING', sectionName=sectionName)
    collectionName = cfgOb.get("COLLECTION_UPDATE_STATUS", sectionName=sectionName)
    ok = dl.load(databaseName, collectionName, loadType="append", documentList=statusList, indexAttributeList=["update_id", "database_name", "object_name"], keyNames=None)
    return ok


def buildResourceCache(cfgOb, configName, cachePath, rebuildCache=False):
    """Generate and cache resource dependencies."""
    ret = False
    try:
        rp = DictMethodResourceProvider(cfgOb, configName=configName, cachePath=cachePath, restoreUseStash=True, restoreUseGit=True, providerTypeExclude=None)
        ret = rp.cacheResources(useCache=not rebuildCache, doBackup=False)
    except Exception as e:
        logger.exception("Failing with %s", str(e))
    return ret


def main():
    parser = argparse.ArgumentParser()
    #
    defaultConfigName = "site_info_configuration"
    #
    parser.add_argument("--full", default=False, action="store_true", help="Fresh full load in a new tables/collections")
    parser.add_argument("--replace", default=False, action="store_true", help="Load with replacement in an existing table/collection (default)")
    #
    parser.add_argument("--load_chem_comp_ref", default=False, action="store_true", help="Load Chemical Component reference definitions (public subset)")
    parser.add_argument("--load_chem_comp_core_ref", default=False, action="store_true", help="Load Chemical Component Core reference definitions (public subset)")
    parser.add_argument("--load_bird_chem_comp_ref", default=False, action="store_true", help="Load Bird Chemical Component reference definitions (public subset)")
    parser.add_argument("--load_bird_chem_comp_core_ref", default=False, action="store_true", help="Load Bird Chemical Component Core reference definitions (public subset)")
    parser.add_argument("--load_bird_ref", default=False, action="store_true", help="Load Bird reference definitions (public subset)")
    parser.add_argument("--load_bird_family_ref", default=False, action="store_true", help="Load Bird Family reference definitions (public subset)")
    parser.add_argument("--load_entry_data", default=False, action="store_true", help="Load PDBx entry data (current released subset)")
    parser.add_argument("--load_pdbx_core", default=False, action="store_true", help="Load all PDBx core collections (current released subset)")
    parser.add_argument("--load_pdbx_core_merge", default=False, action="store_true", help="Load all PDBx core collections with merged content (current released subset)")
    #
    parser.add_argument("--load_pdbx_core_entry", default=False, action="store_true", help="Load PDBx core entry (current released subset)")
    parser.add_argument("--load_pdbx_core_entity", default=False, action="store_true", help="Load PDBx core entity (current released subset)")
    parser.add_argument("--load_pdbx_core_entity_monomer", default=False, action="store_true", help="Load PDBx core entity monomer (current released subset)")
    parser.add_argument("--load_pdbx_core_assembly", default=False, action="store_true", help="Load PDBx core assembly (current released subset)")
    #
    parser.add_argument("--load_pdbx_comp_model_core", default=False, action="store_true", help="Load all PDBx computational model core collections (current released subset)")
    # Add these specific flags in later:
    # parser.add_argument("--load_pdbx_comp_model_core_merge", default=False, action="store_true",
    #                     help="Load all PDBx computational model core collections with merged content (current released subset)")
    # parser.add_argument("--load_pdbx_comp_model_core_entry", default=False, action="store_true", help="Load PDBx computational model core entry (current released subset)")
    # parser.add_argument("--load_pdbx_comp_model_core_entity", default=False, action="store_true", help="Load PDBx computational model core entity (current released subset)")
    # parser.add_argument("--load_pdbx_comp_model_core_entity_monomer", default=False, action="store_true",
    #                     help="Load PDBx computational model core entity monomer (current released subset)")
    # parser.add_argument("--load_pdbx_comp_model_core_assembly", default=False, action="store_true", help="Load PDBx computational model core assembly (current released subset)")
    #
    parser.add_argument("--load_ihm_dev", default=False, action="store_true", help="Load I/HM DEV model data (current released subset)")
    #
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default=defaultConfigName, help="Configuration section name")

    parser.add_argument("--db_type", default="mongo", help="Database server type (default=mongo)")

    parser.add_argument(
        "--document_style",
        default="rowwise_by_name_with_cardinality",
        help="Document organization (rowwise_by_name_with_cardinality|rowwise_by_name|columnwise_by_name|rowwise_by_id|rowwise_no_name",
    )
    parser.add_argument("--read_back_check", default=False, action="store_true", help="Perform read back check on all documents")
    parser.add_argument("--schema_level", default=None, help="Schema validation level (full|min default=None)")
    #
    parser.add_argument("--load_file_list_path", default=None, help="Input file containing load file path list (override automatic repository scan)")
    parser.add_argument("--fail_file_list_path", default=None, help="Output file containing file paths that fail to load")
    parser.add_argument("--save_file_list_path", default=None, help="Save repo file paths from automatic file system scan in this path")

    parser.add_argument("--num_proc", default=2, help="Number of processes to execute (default=2)")
    parser.add_argument("--chunk_size", default=10, help="Number of files loaded per process")
    parser.add_argument("--file_limit", default=None, help="Load file limit for testing")
    parser.add_argument("--prune_document_size", default=None, help="Prune large documents to this size limit (MB)")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action="store_true", help="Use MOCK repository configuration for testing")
    parser.add_argument("--cache_path", default=None, help="Cache path for resource files")
    parser.add_argument("--rebuild_cache", default=False, action="store_true", help="Rebuild cached resource files")
    parser.add_argument("--rebuild_schema", default=False, action="store_true", help="Rebuild schema on-the-fly if not cached")
    parser.add_argument("--vrpt_repo_path", default=None, help="Path to validation report repository")
    args = parser.parse_args()
    #
    debugFlag = args.debug
    if debugFlag:
        logger.setLevel(logging.DEBUG)
    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    #                                       Configuration Details
    configPath = args.config_path
    configName = args.config_name
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

        #
        if args.vrpt_repo_path:
            vrptPath = args.vrpt_repo_path
            if not os.access(vrptPath, os.R_OK):
                logger.error("Unreadable validation report repository path %r", vrptPath)
            envName = cfgOb.get("VRPT_REPO_PATH_ENV", sectionName=configName)
            os.environ[envName] = vrptPath
            logger.info("Using alternate validation report path %s", os.getenv(envName))

    except Exception as e:
        logger.error("Missing or access issue with config file %r with %s", configPath, str(e))
        exit(1)

    #
    try:
        readBackCheck = args.read_back_check
        numProc = int(args.num_proc)
        chunkSize = int(args.chunk_size)
        fileLimit = int(args.file_limit) if args.file_limit else None
        failedFilePath = args.fail_file_list_path
        fPath = args.load_file_list_path
        schemaLevel = args.schema_level if args.schema_level in ["min", "full", "minimum"] else None
        loadType = "full" if args.full else "replace"
        loadType = "replace" if args.replace else "full"
        saveInputFileListPath = args.save_file_list_path
        pruneDocumentSize = float(args.prune_document_size) if args.prune_document_size else None
        cachePath = args.cache_path if args.cache_path else "."
        cachePath = os.path.abspath(cachePath)
        rebuildCache = args.rebuild_cache if args.rebuild_cache else False
        rebuildSchemaFlag = args.rebuild_schema if args.rebuild_schema else False
        if args.document_style not in ["rowwise_by_name", "rowwise_by_name_with_cardinality", "columnwise_by_name", "rowwise_by_id", "rowwise_no_name"]:
            logger.error("Unsupported document style %s", args.document_style)
        if args.db_type != "mongo":
            logger.error("Unsupported database server type %s", args.db_type)
    except Exception as e:
        logger.exception("Argument processing problem %s", str(e))
        parser.print_help(sys.stderr)
        exit(1)

    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    #  Rebuild or check resource cache
    okS = True
    ok = buildResourceCache(cfgOb, configName, cachePath, rebuildCache=rebuildCache)
    if not ok:
        logger.error("Cache rebuild or check failure (rebuild %r) %r", rebuildCache, cachePath)
        exit(1)

    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    # Read any input path lists -
    #
    inputPathList = None
    if fPath:
        mu = MarshalUtil(workPath=cachePath)
        inputPathList = mu.doImport(fPath, fmt="list")
        if not inputPathList:
            logger.error("Missing or empty input file path list %s", fPath)
            exit(1)
    #
    ##
    if args.db_type == "mongo":
        mw = PdbxLoader(
            cfgOb,
            cachePath,
            resourceName="MONGO_DB",
            numProc=numProc,
            chunkSize=chunkSize,
            fileLimit=fileLimit,
            verbose=debugFlag,
            readBackCheck=readBackCheck,
            rebuildSchemaFlag=rebuildSchemaFlag,
        )
        #
        if args.load_chem_comp_ref:
            ok = mw.load(
                "chem_comp",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_chem_comp_core_ref:
            ok = mw.load(
                "chem_comp_core",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_bird_chem_comp_ref:
            ok = mw.load(
                "bird_chem_comp",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_bird_chem_comp_core_ref:
            ok = mw.load(
                "bird_chem_comp_core",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_bird_ref:
            ok = mw.load(
                "bird",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_bird_family_ref:
            ok = mw.load(
                "bird_family",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["BIRD_FAMILY_PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_entry_data:
            ok = mw.load(
                "pdbx",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_pdbx_core:
            ok = mw.load(
                "pdbx_core",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_pdbx_core_merge:
            ok = mw.load(
                "pdbx_core",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
                mergeContentTypes=["vrpt"],
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_pdbx_core_entity:
            ok = mw.load(
                "pdbx_core",
                collectionLoadList=["pdbx_core_entity"],
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_pdbx_core_entity_monomer:
            ok = mw.load(
                "pdbx_core",
                collectionLoadList=["pdbx_core_entity_monomer"],
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_pdbx_core_entry:
            ok = mw.load(
                "pdbx_core",
                collectionLoadList=["pdbx_core_entry"],
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_pdbx_core_assembly:
            ok = mw.load(
                "pdbx_core",
                collectionLoadList=["pdbx_core_assembly"],
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_pdbx_comp_model_core:
            ok = mw.load(
                "pdbx_comp_model_core",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        if args.load_ihm_dev:
            ok = mw.load(
                "ihm_dev",
                loadType=loadType,
                inputPathList=inputPathList,
                styleType=args.document_style,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=failedFilePath,
                saveInputFileListPath=saveInputFileListPath,
                pruneDocumentSize=pruneDocumentSize,
                validationLevel=schemaLevel,
            )
            okS = loadStatus(mw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)
        #
        logger.info("Operation completed with status %r " % ok and okS)


if __name__ == "__main__":
    main()
