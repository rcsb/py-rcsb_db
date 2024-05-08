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
#    19-Mar-2024 - dwp Updating arguments and making compatible with luigi workflow
#     5-Apr-2024 - dwp Change arguments and execution structure to make more flexible;
#                      Add arguments and logic to support CLI usage from weekly-update workflow;
#                      Add support for logging output to a specific file
#    25-Apr-2024 - dwp Add support for remote config file loading; use underscores instead of hyphens for arg choices
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import os
import sys
import argparse
import logging

from rcsb.db.wf.RepoLoadWorkflow import RepoLoadWorkflow
from rcsb.utils.config.ConfigUtil import ConfigUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger()


def main():
    parser = argparse.ArgumentParser()
    #
    parser.add_argument(
        "--op",
        default=None,
        required=True,
        help="Loading operation to perform",
        choices=["pdbx_loader", "build_resource_cache", "pdbx_db_wiper", "pdbx_id_list_splitter", "pdbx_loader_check", "etl_entity_sequence_clusters", "etl_repository_holdings"]
    )
    #
    parser.add_argument(
        "--load_type",
        default="replace",
        help="Type of load ('replace' for incremental and multi-worker load, 'full' for complete and fresh single-worker load)",
        choices=["replace", "full"],
    )
    #
    parser.add_argument(
        "--database",
        default=None,
        help="Database to load (most common choices are: 'pdbx_core', 'pdbx_comp_model_core', or 'bird_chem_comp_core')",
        choices=["pdbx_core", "pdbx_comp_model_core", "bird_chem_comp_core", "chem_comp", "chem_comp_core", "bird_chem_comp", "bird", "bird_family", "ihm_dev"],
    )
    #
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default="site_info_remote_configuration", help="Configuration section name")
    parser.add_argument(
        "--document_style",
        default="rowwise_by_name_with_cardinality",
        help="Document organization (rowwise_by_name_with_cardinality|rowwise_by_name|columnwise_by_name|rowwise_by_id|rowwise_no_name",
    )
    parser.add_argument("--cache_path", default=None, help="Cache path for resource files")
    parser.add_argument("--num_proc", default=2, help="Number of processes to execute (default=2)")
    parser.add_argument("--chunk_size", default=10, help="Number of files loaded per process")
    parser.add_argument("--max_step_length", default=500, help="Maximum subList size (default=500)")
    parser.add_argument("--schema_level", default="min", help="Schema validation level (full|min)")
    parser.add_argument("--collection_list", default=None, help="Specific collections to load")
    #
    parser.add_argument("--load_id_list_path", default=None, help="Input file containing the list of IDs to load in the current iteration by a single worker")
    parser.add_argument("--holdings_file_path", default=None, help="File containing the complete list of all IDs (or holdings files) that will be loaded")
    parser.add_argument("--load_file_list_path", default=None, help="Input file containing load file path list (override automatic repository scan)")
    parser.add_argument("--fail_file_list_path", default=None, help="Output file containing file paths that fail to load")
    parser.add_argument("--save_file_list_path", default=None, help="Save repo file paths from automatic file system scan in this path")
    parser.add_argument("--load_file_list_dir", default=None, help="Directory path for storing load file lists")
    parser.add_argument("--num_sublists", default=None, help="Number of sublists to create/load for the associated database")
    parser.add_argument("--force_reload", default=False, action="store_true", help="Force re-load of provided ID list (i.e., don't just load delta; useful for manual/test runs).")
    parser.add_argument("--provider_type_exclude", default=None, help="Resource provider types to exclude")
    #
    parser.add_argument("--db_type", default="mongo", help="Database server type (default=mongo)")
    parser.add_argument("--file_limit", default=None, help="Load file limit for testing")
    parser.add_argument("--prune_document_size", default=None, help="Prune large documents to this size limit (MB)")
    parser.add_argument("--regex_purge", default=False, action="store_true", help="Perform additional regex-based purge of all pre-existing documents for loadType != 'full'")
    parser.add_argument("--data_selectors", help="Data selectors, space-separated.", default=["PUBLIC_RELEASE"], required=False, nargs="+", metavar="")
    parser.add_argument("--disable_read_back_check", default=False, action="store_true", help="Disable read back check on all documents")
    parser.add_argument("--disable_merge_validation_reports", default=False, action="store_true", help="Disable merging of validation report data with the primary content type")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action="store_true", help="Use MOCK repository configuration for testing")
    parser.add_argument("--rebuild_cache", default=False, action="store_true", help="Rebuild cached resource files")
    parser.add_argument("--rebuild_schema", default=False, action="store_true", help="Rebuild schema on-the-fly if not cached")
    parser.add_argument("--vrpt_repo_path", default=None, help="Path to validation report repository")
    parser.add_argument("--cluster_filename_template", default=None, help="Filename template for cluster files (e.g., clusters-by-entity-90.txt)")
    parser.add_argument("--log_file_path", default=None, help="Path to runtime log file output.")
    #
    args = parser.parse_args()
    #
    try:
        op, commonD, loadD = processArguments(args)
    except Exception as err:
        logger.exception("Argument processing problem %s", str(err))
        raise ValueError("Argument processing problem") from err
    #
    #
    # Log input arguments
    loadLogD = {k: v for d in [commonD, loadD] for k, v in d.items() if k != "inputIdCodeList"}
    logger.info("running load op %r on loadLogD %r:", op, loadLogD)
    #
    # Run the operation
    okR = False
    rlWf = RepoLoadWorkflow(**commonD)
    if op in ["pdbx_loader", "etl_entity_sequence_clusters", "etl_repository_holdings"]:
        okR = rlWf.load(op, **loadD)
    #
    elif op == "build_resource_cache":
        okR = rlWf.buildResourceCache(rebuildCache=True, providerTypeExclude=loadD["providerTypeExclude"])
    #
    elif op == "pdbx_id_list_splitter":
        okR = rlWf.splitIdList(op, **loadD)
    #
    elif op == "pdbx_db_wiper":
        okR = rlWf.removeAndRecreateDbCollections(op, **loadD)
    #
    elif op == "pdbx_loader_check":
        okR = rlWf.loadCompleteCheck(op, **loadD)
    #
    else:
        logger.error("Unsupported op %r", op)
    #
    logger.info("Operation %r completed with status %r", op, okR)
    #
    if not okR:
        logger.error("Operation %r failed with status %r", op, okR)
        raise ValueError("Operation %r failed" % op)


def processArguments(args):
    # Logging details
    logFilePath = args.log_file_path
    debugFlag = args.debug
    if debugFlag:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if logFilePath:
        logDir = os.path.dirname(logFilePath)
        if not os.path.isdir(logDir):
            os.makedirs(logDir)
        handler = logging.FileHandler(logFilePath, mode="a")
        if debugFlag:
            handler.setLevel(logging.DEBUG)
        else:
            handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    #
    # Configuration details
    configPath = args.config_path
    configName = args.config_name
    if not (configPath and configName):
        logger.error("Config path and/or name not provided: %r, %r", configPath, configName)
        raise ValueError("Config path and/or name not provided: %r, %r" % (configPath, configName))
    mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data") if args.mock else None
    logger.info("Using configuration file %r (section %r)", configPath, configName)
    cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
    cfgObTmp = cfgOb.exportConfig()
    logger.info("Length of config object (%r)", len(cfgObTmp))
    if len(cfgObTmp) == 0:
        logger.error("Missing or access issue for config file %r", configPath)
        raise ValueError("Missing or access issue for config file %r" % configPath)
    else:
        del cfgObTmp
    #
    if args.vrpt_repo_path:
        vrptPath = args.vrpt_repo_path
        if not os.access(vrptPath, os.R_OK):
            logger.error("Unreadable validation report repository path %r", vrptPath)
            raise ValueError("Unreadable validation report repository path %r" % vrptPath)
        envName = cfgOb.get("VRPT_REPO_PATH_ENV", sectionName=configName)
        os.environ[envName] = vrptPath
        logger.info("Using alternate validation report path %s", os.getenv(envName))
    #
    # Do any additional argument checking
    op = args.op
    databaseName = args.database
    if not op:
        raise ValueError("Must supply a value to '--op' argument")
    if op == "pdbx_loader" and not databaseName:
        raise ValueError("Must supply a value to '--database' argument for op type 'pdbx-loader")
    #
    if databaseName == "bird_family":  # Not sure if this is relevant anymore
        dataSelectors = ["BIRD_FAMILY_PUBLIC_RELEASE"]
    else:
        dataSelectors = args.data_selectors if args.data_selectors else ["PUBLIC_RELEASE"]
    #
    if args.document_style not in ["rowwise_by_name", "rowwise_by_name_with_cardinality", "columnwise_by_name", "rowwise_by_id", "rowwise_no_name"]:
        logger.error("Unsupported document style %s", args.document_style)
    #
    cachePath = args.cache_path if args.cache_path else "."
    cachePath = os.path.abspath(cachePath)

    # Now collect arguments into dictionaries
    commonD = {
        "configPath": configPath,
        "configName": configName,
        "cachePath": cachePath,
        "mockTopPath": mockTopPath,
        "debugFlag": debugFlag,
    }
    loadD = {
        "databaseName": databaseName,
        "collectionNameList": args.collection_list,
        "loadType": args.load_type,
        "numProc": int(args.num_proc),
        "chunkSize": int(args.chunk_size),
        "maxStepLength": int(args.max_step_length),
        "dbType": args.db_type,
        "fileLimit": int(args.file_limit) if args.file_limit else None,
        "readBackCheck": not args.disable_read_back_check,
        "rebuildSchemaFlag": args.rebuild_schema,
        "holdingsFilePath": args.holdings_file_path,
        "failedFilePath": args.fail_file_list_path,
        "loadIdListPath": args.load_id_list_path,
        "loadFileListPath": args.load_file_list_path,
        "saveInputFileListPath": args.save_file_list_path,
        "loadFileListDir": args.load_file_list_dir,
        "numSublistFiles": int(args.num_sublists) if args.num_sublists else None,
        "schemaLevel": args.schema_level if args.schema_level in ["min", "full", "minimum"] else None,
        "pruneDocumentSize": float(args.prune_document_size) if args.prune_document_size else None,
        "regexPurge": args.regex_purge,
        "documentStyle": args.document_style,
        "dataSelectors": dataSelectors,
        "mergeValidationReports": not args.disable_merge_validation_reports,
        "providerTypeExclude": args.provider_type_exclude,
        "clusterFileNameTemplate": args.cluster_filename_template,
        "rebuildCache": args.rebuild_cache,
        "forceReload": args.force_reload,
    }

    return op, commonD, loadD


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Run failed %s", str(e))
        sys.exit(1)
