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
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
import logging
import os
import sys

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.mongo.PdbxLoader import PdbxLoader
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


def loadStatus(statusList, cfgOb, readBackCheck=True):
    sectionName = 'data_exchange_status'
    dl = DocumentLoader(cfgOb, "MONGO_DB", numProc=2, chunkSize=2,
                        documentLimit=None, verbose=False, readBackCheck=readBackCheck)
    #
    databaseName = cfgOb.get('DATABASE_NAME', sectionName=sectionName) + '_' + cfgOb.get('DATABASE_VERSION_STRING', sectionName=sectionName)
    collectionVersion = cfgOb.get('COLLECTION_VERSION_STRING', sectionName=sectionName)
    collectionName = cfgOb.get('COLLECTION_UPDATE_STATUS', sectionName=sectionName) + '_' + collectionVersion
    ok = dl.load(databaseName, collectionName, loadType='append', documentList=statusList,
                 indexAttributeList=['update_id', 'database_name', 'object_name'], keyNames=None)
    return ok


def main():
    parser = argparse.ArgumentParser()
    #
    #
    parser.add_argument("--full", default=False, action='store_true', help="Fresh full load in a new tables/collections")
    parser.add_argument("--replace", default=False, action='store_true', help="Load with replacement in an existing table/collection (default)")
    #
    parser.add_argument("--load_chem_comp_ref", default=False, action='store_true', help="Load Chemical Component reference definitions (public subset)")
    parser.add_argument("--load_bird_chem_comp_ref", default=False, action='store_true', help="Load Bird Chemical Component reference definitions (public subset)")
    parser.add_argument("--load_bird_ref", default=False, action='store_true', help="Load Bird reference definitions (public subset)")
    parser.add_argument("--load_bird_family_ref", default=False, action='store_true', help="Load Bird Family reference definitions (public subset)")
    parser.add_argument("--load_entry_data", default=False, action='store_true', help="Load PDBx entry data (current released subset)")
    parser.add_argument("--load_pdbx_core", default=False, action='store_true', help="Load PDBx core entry/entity data (current released subset)")
    #
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default="DEFAULT", help="Configuration section name")

    parser.add_argument("--db_type", default="mongo", help="Database server type (default=mongo)")

    parser.add_argument("--document_style", default="rowwise_by_name_with_cardinality",
                        help="Document organization (rowwise_by_name_with_cardinality|rowwise_by_name|columnwise_by_name|rowwise_by_id|rowwise_no_name")
    parser.add_argument("--read_back_check", default=False, action='store_true', help="Perform read back check on all documents")
    #
    parser.add_argument("--load_file_list_path", default=None, help="Input file containing load file path list (override automatic repository scan)")
    parser.add_argument("--fail_file_list_path", default=None, help="Output file containing file paths that fail to load")
    parser.add_argument("--save_file_list_path", default=None, help="Save repo file paths from automatic file system scan in this path")

    parser.add_argument("--num_proc", default=2, help="Number of processes to execute (default=2)")
    parser.add_argument("--chunk_size", default=10, help="Number of files loaded per process")
    parser.add_argument("--file_limit", default=None, help="Load file limit for testing")
    parser.add_argument("--prune_document_size", default=None, help="Prune large documents to this size limit (MB)")
    parser.add_argument("--debug", default=False, action='store_true', help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action='store_true', help="Use MOCK repository configuration for testing")
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
        cfgOb = ConfigUtil(configPath=configPath, sectionName=configName, mockTopPath=mockTopPath)
    except Exception as e:
        logger.error("Missing or access issue with config file %r" % configPath)
        exit(1)

    #
    try:
        readBackCheck = args.read_back_check
        numProc = int(args.num_proc)
        chunkSize = int(args.chunk_size)
        fileLimit = int(args.file_limit) if args.file_limit else None
        failedFilePath = args.fail_file_list_path
        fPath = args.load_file_list_path
        loadType = 'full' if args.full else 'replace'
        loadType = 'replace' if args.replace else 'full'
        saveInputFileListPath = args.save_file_list_path
        pruneDocumentSize = float(args.prune_document_size) if args.prune_document_size else None
        workPath = args.working_path if args.working_path else '.'
        if args.document_style not in ['rowwise_by_name', 'rowwise_by_name_with_cardinality', 'columnwise_by_name', 'rowwise_by_id', 'rowwise_no_name']:
            logger.error("Unsupported document style %s" % args.document_style)
        if args.db_type != "mongo":
            logger.error("Unsupported database server type %s" % args.db_type)
    except Exception as e:
        logger.exception("Argument processing problem %s" % str(e))
        parser.print_help(sys.stderr)
        exit(1)
    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    #
    # Read any input path lists -
    #

    inputPathList = None
    if fPath:
        mu = MarshalUtil(workPath=workPath)
        inputPathList = mu.doImport(fPath, format='list')
        if len(inputPathList) < 1:
            logger.error("Missing or empty input file path list %s" % fPath)
            exit(1)
    #
    ##
    if args.db_type == "mongo":
        mw = PdbxLoader(
            cfgOb,
            resourceName="MONGO_DB",
            numProc=numProc,
            chunkSize=chunkSize,
            fileLimit=fileLimit,
            verbose=debugFlag,
            readBackCheck=readBackCheck,
            workPath=workPath)

        if args.load_chem_comp_ref:
            ok = mw.load('chem_comp', loadType=loadType, inputPathList=inputPathList, styleType=args.document_style,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=failedFilePath,
                         saveInputFileListPath=saveInputFileListPath, pruneDocumentSize=pruneDocumentSize)
            okS = loadStatus(mw.getLoadStatus(), cfgOb, readBackCheck=readBackCheck)

        if args.load_bird_chem_comp_ref:
            ok = mw.load('bird_chem_comp', loadType=loadType, inputPathList=inputPathList, styleType=args.document_style,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=failedFilePath,
                         saveInputFileListPath=saveInputFileListPath, pruneDocumentSize=pruneDocumentSize)
            okS = loadStatus(mw.getLoadStatus(), cfgOb, readBackCheck=readBackCheck)

        if args.load_bird_ref:
            ok = mw.load('bird', loadType=loadType, inputPathList=inputPathList, styleType=args.document_style,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=failedFilePath,
                         saveInputFileListPath=saveInputFileListPath, pruneDocumentSize=pruneDocumentSize)
            okS = loadStatus(mw.getLoadStatus(), cfgOb, readBackCheck=readBackCheck)

        if args.load_bird_family_ref:
            ok = mw.load('bird_family', loadType=loadType, inputPathList=inputPathList, styleType=args.document_style,
                         dataSelectors=["BIRD_FAMILY_PUBLIC_RELEASE"], failedFilePath=failedFilePath,
                         saveInputFileListPath=saveInputFileListPath, pruneDocumentSize=pruneDocumentSize)
            okS = loadStatus(mw.getLoadStatus(), cfgOb, readBackCheck=readBackCheck)

        if args.load_entry_data:
            ok = mw.load('pdbx', loadType=loadType, inputPathList=inputPathList, styleType=args.document_style,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=failedFilePath,
                         saveInputFileListPath=saveInputFileListPath, pruneDocumentSize=pruneDocumentSize)
            okS = loadStatus(mw.getLoadStatus(), cfgOb, readBackCheck=readBackCheck)

        if args.load_pdbx_core:
            ok = mw.load('pdbx_core', loadType=loadType, inputPathList=inputPathList, styleType=args.document_style,
                         dataSelectors=["PUBLIC_RELEASE"], failedFilePath=failedFilePath,
                         saveInputFileListPath=saveInputFileListPath, pruneDocumentSize=pruneDocumentSize)
            okS = loadStatus(mw.getLoadStatus(), cfgOb, readBackCheck=readBackCheck)
        #
        logger.info("Operation completed with status %r " % ok and okS)


if __name__ == '__main__':
    main()
