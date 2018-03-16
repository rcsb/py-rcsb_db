##
# File: DbLoadExec.py
# Date: 15-Mar-2018  jdw
#
#  Execution wrapper  --  database loading utilities --
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import os
import sys
import argparse

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

try:
    from rcsb_db import __version__
except Exception as e:
    sys.path.insert(0, TOPDIR)
    from rcsb_db import __version__

from rcsb_db.mongo.MongoDbLoaderWorker import MongoDbLoaderWorker


def main():
    parser = argparse.ArgumentParser()
    #
    parser.add_argument("--load_chem_comp_ref", default=False, action='store_true', help="Load Chemical Component Reference Data")
    parser.add_argument("--load_bird_ref", default=False, action='store_true', help="Load Bird Reference Data")
    parser.add_argument("--load_bird_family_ref", default=False, action='store_true', help="Load Bird Family Reference Data")
    parser.add_argument("--load_entry_data", default=False, action='store_true', help="Load entry data on the current entry load list")
    #
    #parser.add_argument("--path_entry_load_list", default=None, help="Path to PDBx/mmCIF entry load path list")
    #parser.add_argument("--make_entry_load_list", default=None, help="Create PDBx/mmCIF entry load path list")

    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default="DEFAULT", help="Configuration section name")

    parser.add_argument("--num_proc", default=2, help="Number of processes to execute")
    parser.add_argument("--chunk_size", default=10, help="Number of files loaded per process")
    parser.add_argument("--file_limit", default=None, help="File limit for testing")

    parser.add_argument("--db_type", default="mongo", help="Database server type")
    parser.add_argument("--document_style", default="rowwise_by_name_with_cardinality", help="Document organization (rowwise_by_name_with_cardinality|rowwise_by_name|columnwise_by_name|rowwise_by_id|rowwise_no_name")
    parser.add_argument("--read_back_check", default=False, action='store_true', help="Perform read back check on all documents")
    parser.add_argument("--debug", default=False, action='store_true', help="Turn on verbose logging")
    args = parser.parse_args()
    #
    if args.debug:
        logger.setLevel(logging.DEBUG)
    #
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
    except Exception as e:
        logger.error("Missing or access issue with config file %r" % configPath)
        exit(1)
    #
    try:
        readBackCheck = args.read_back_check
        numProc = int(args.num_proc)
        chunkSize = int(args.chunk_size)
        fileLimit = args.file_limit
        if args.file_limit:
            fileLimit = int(args.file_limit)

        if args.document_style not in ['rowwise_by_name', 'rowwise_by_name_with_cardinality', 'columnwise_by_name', 'rowwise_by_id', 'rowwise_no_name']:
            logger.error("Unsupported document style %s" % args.document_style)
        if args.db_type != "mongo":
            logger.error("Unsupported database server type %s" % args.db_type)
    except Exception as e:
        logger.exception("Argument processing problem %s" % str(e))
        parser.print_help(sys.stderr)
        exit(1)

    # -----------------------
    #
    if args.db_type == "mongo":
        mw = MongoDbLoaderWorker(configPath, configName, numProc=numProc, chunkSize=chunkSize, fileLimit=fileLimit, readBackCheck=readBackCheck)

        if args.load_chem_comp_ref:
            ok = mw.loadContentType('chem-comp', styleType=args.document_style)

        if args.load_bird_ref:
            ok = mw.loadContentType('bird', styleType=args.document_style)

        if args.load_bird_family_ref:
            ok = mw.loadContentType('bird-family', styleType=args.document_style)

        if args.load_entry_data:
            ok = mw.loadContentType('pdbx', styleType=args.document_style)

        logger.info("Operation completed with status %r " % ok)

if __name__ == '__main__':
    main()
