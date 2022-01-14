##
# File: RepoScanExec.py
#
#  Execution wrapper  --  repository scanning utilities --
#
#  Updates:
#
# 28-Jun-2018 jdw update ScanRepoUtil prototype with workPath
#  3-Jul-2018 jdw update to latest ScanRepoUtil() prototype
# 20-Aug-2018 jdw engage incremental repository scan mode.
#  1-Aug-2021 jdw add scan_obsolete_entry_data option
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
import logging
import os
import sys
from collections import OrderedDict

from rcsb.utils.dictionary.DictionaryApiProviderWrapper import DictionaryApiProviderWrapper
from rcsb.utils.repository.ScanRepoUtil import ScanRepoUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def scanRepo(
    cfgOb,
    contentType,
    scanDataFilePath,
    numProc,
    chunkSize,
    fileLimit,
    scanType="full",
    inputPathList=None,
    pathListFilePath=None,
    dataCoverageFilePath=None,
    dataCoverageItemFilePath=None,
    dataTypeFilePath=None,
    failedFilePath=None,
    cachePath=None,
):
    """Utility method to scan the data repository of the input content type and store type and coverage details."""
    try:
        #
        # configName = cfgOb.getDefaultSectionName()
        dP = DictionaryApiProviderWrapper(cachePath, cfgOb=cfgOb, useCache=True)
        dictApi = dP.getApiByName(contentType)
        ###
        categoryList = sorted(dictApi.getCategoryList())
        dictSchema = {catName: sorted(dictApi.getAttributeNameList(catName)) for catName in categoryList}
        attributeDataTypeD = OrderedDict()
        for catName in categoryList:
            aD = {}
            for atName in dictSchema[catName]:
                aD[atName] = dictApi.getTypeCode(catName, atName)
            attributeDataTypeD[catName] = aD
        ###
        #
        sr = ScanRepoUtil(cfgOb, attributeDataTypeD=attributeDataTypeD, numProc=numProc, chunkSize=chunkSize, fileLimit=fileLimit, workPath=cachePath)
        ok = sr.scanContentType(
            contentType, scanType=scanType, inputPathList=inputPathList, scanDataFilePath=scanDataFilePath, failedFilePath=failedFilePath, saveInputFileListPath=pathListFilePath
        )
        if dataTypeFilePath:
            ok = sr.evalScan(scanDataFilePath, dataTypeFilePath, evalType="data_type")
        if dataCoverageFilePath:
            ok = sr.evalScan(scanDataFilePath, dataCoverageFilePath, evalType="data_coverage")
        if dataCoverageItemFilePath:
            ok = sr.evalScanItem(scanDataFilePath, dataCoverageItemFilePath)

        return ok
    except Exception as e:
        logger.exception("Failing with %s", str(e))


def main():
    parser = argparse.ArgumentParser()
    defaultConfigName = "site_info_configuration"
    #
    parser.add_argument("--scanType", default="full", help="Repository scan type (full|incr)")
    #
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scan_chem_comp_ref", default=False, action="store_true", help="Scan Chemical Component reference definitions (public subset)")
    group.add_argument("--scan_chem_comp_core_ref", default=False, action="store_true", help="Scan Chemical Component Core reference definitions (public subset)")
    group.add_argument("--scan_bird_chem_comp_ref", default=False, action="store_true", help="Scan Bird Chemical Component reference definitions (public subset)")
    group.add_argument("--scan_bird_chem_comp_core_ref", default=False, action="store_true", help="Scan Bird Chemical Component Core reference definitions (public subset)")
    group.add_argument("--scan_bird_ref", default=False, action="store_true", help="Scan Bird reference definitions (public subset)")
    group.add_argument("--scan_bird_family_ref", default=False, action="store_true", help="Scan Bird Family reference definitions (public subset)")
    group.add_argument("--scan_entry_data", default=False, action="store_true", help="Scan PDB entry data (current released subset)")
    group.add_argument("--scan_obsolete_entry_data", default=False, action="store_true", help="Scan obsolete PDB entry data")
    group.add_argument("--scan_comp_model_data", default=False, action="store_true", help="Scan computational model files (mock-data subset)")
    group.add_argument("--scan_ihm_dev", default=False, action="store_true", help="Scan PDBDEV I/HM entry data (current released subset)")
    #
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default=defaultConfigName, help="Configuration section name")

    parser.add_argument("--input_file_list_path", default=None, help="Input file containing file paths to scan")
    parser.add_argument("--output_file_list_path", default=None, help="Output file containing file paths scanned")
    parser.add_argument("--fail_file_list_path", default=None, help="Output file containing file paths that fail scan")
    parser.add_argument("--scan_data_file_path", default=None, help="Output working file storing scan data (Pickle)")
    parser.add_argument("--coverage_file_path", default=None, help="Coverage map (JSON) output path")
    parser.add_argument("--coverage_item_file_path", default=None, help="Coverage by item (tdd) output path")
    parser.add_argument("--type_map_file_path", default=None, help="Type map (JSON) output path")

    parser.add_argument("--num_proc", default=2, help="Number of processes to execute (default=2)")
    parser.add_argument("--chunk_size", default=10, help="Number of files loaded per process")
    parser.add_argument("--file_limit", default=None, help="Load file limit for testing")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action="store_true", help="Use MOCK repository configuration for testing")
    parser.add_argument("--cache_path", default=None, help="Cache path and working direcory for temporary files")
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
            logger.info("Using configuration path %s (%s)", configPath, configName)
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
    try:
        numProc = int(args.num_proc)
        chunkSize = int(args.chunk_size)
        fileLimit = int(args.file_limit) if args.file_limit else None
        #
        failedFilePath = args.fail_file_list_path

        scanType = args.scanType
        #
        inputFileListPath = args.input_file_list_path
        outputFileListPath = args.output_file_list_path
        scanDataFilePath = args.scan_data_file_path
        dataCoverageFilePath = args.coverage_file_path
        dataCoverageItemFilePath = args.coverage_item_file_path
        dataTypeFilePath = args.type_map_file_path
        cachePath = args.cache_path if args.cache_path else "."
    except Exception as e:
        logger.exception("Argument processing problem %s", str(e))
        parser.print_help(sys.stderr)
        exit(1)
    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    #
    # Read any input path lists -
    #
    inputPathList = None
    if inputFileListPath:
        mu = MarshalUtil(workPath=cachePath)
        inputPathList = mu.doImport(inputFileListPath, fmt="list")
    #
    ##

    if args.scan_chem_comp_ref:
        contentType = "chem_comp_core"

    elif args.scan_chem_comp_core_ref:
        contentType = "chem_comp_core"

    elif args.scan_bird_chem_comp_ref:
        contentType = "bird_chem_comp_core"

    elif args.scan_bird_chem_comp_core_ref:
        contentType = "bird_chem_comp_core"

    elif args.scan_bird_ref:
        contentType = "bird"

    elif args.scan_bird_family_ref:
        contentType = "bird_family"

    elif args.scan_entry_data:
        contentType = "pdbx"

    elif args.scan_obsolete_entry_data:
        contentType = "pdbx_obsolete"

    elif args.scan_comp_model_data:
        contentType = "pdbx_comp_model_core"

    elif args.scan_ihm_dev:
        contentType = "ihm_dev"

    ok = scanRepo(
        cfgOb,
        contentType,
        scanDataFilePath,
        numProc,
        chunkSize,
        fileLimit,
        scanType=scanType,
        inputPathList=inputPathList,
        pathListFilePath=outputFileListPath,
        dataCoverageFilePath=dataCoverageFilePath,
        dataCoverageItemFilePath=dataCoverageItemFilePath,
        dataTypeFilePath=dataTypeFilePath,
        failedFilePath=failedFilePath,
        cachePath=cachePath,
    )

    logger.info("Operation completed with status %r", ok)


if __name__ == "__main__":
    main()
