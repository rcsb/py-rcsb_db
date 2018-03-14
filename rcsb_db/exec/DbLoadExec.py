##
# File: DbLoadExec.py
# Date: 20-Feb-2018  jdw
#
#  Execution wrapper  --  database loading utilities --
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import os
import sys
import time
import shutil
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

from rcsb_db.loaders.SchemaDefDataPrep import SchemaDefDataPrep
from rcsb_db.schema.BirdSchemaDef import BirdSchemaDef
from rcsb_db.schema.ChemCompSchemaDef import ChemCompSchemaDef
from rcsb_db.schema.PdbxSchemaDef import PdbxSchemaDef
from rcsb_db.schema.DaInternalSchemaDef import DaInternalSchemaDef

from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo
from mmcif_utils.bird.PdbxFamilyIo import PdbxFamilyIo
from mmcif_utils.chemcomp.PdbxChemCompIo import PdbxChemCompIo

from rcsb_db.mongo.ConnectionBase import ConnectionBase
from rcsb_db.mongo.MongoDbUtil import MongoDbUtil

from rcsb_db.utils.ConfigUtils import ConfigUtils


def main():
    parser = argparse.ArgumentParser()
    #
    parser.add_argument("--load_chem_comp_ref", default=False, action='store_true', help="Load Chemical Component Reference Data")
    parser.add_argument("--load_bird_ref", default=False, action='store_true', help="Load Bird Reference Data")
    parser.add_argument("--load_bird_family_ref", default=False, action='store_true', help="Load Bird Family Reference Data")
    #
    parser.add_argument("--load_entry_data", default=False, action='store_true', help="Load entry data on the current entry load list")
    parser.add_argument("--path_entry_load_list", default=None, help="Path to PDBx/mmCIF entry load path list")
    parser.add_argument("--make_entry_load_list", default=None, help="Create PDBx/mmCIF entry load path list")

    parser.add_argument("--path_config_options", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default="DEFAULT", help="Configuration section name")

    args = parser.parse_args()
    #
    if args.config_path:
        logger.debug("Using config path %r" % args.config_path)
        if os.access(args.config_path, os.R_OK):
            os.environ['DBLOAD_CONFIG_PATH'] = args.config_path
        else:
            logger.error("Missing or access issue with config file %r" % args.config_path)
    else:
        logger.info("Using configuration path from the environment %s" % os.getenv('DBLOAD_CONFIG_PATH', None))

    #
    wc = ConfigUtils()

    if args.rebuild_primary_index_and_data:
        # --- Search for PMIDs
        #

    if args.laod_chem_comp_ref:
        wqx = WosLamrQueryExec()
        wqx.doUtQueryAndUpdate(skipExistingFlag=False)


if __name__ == '__main__':
    main()
