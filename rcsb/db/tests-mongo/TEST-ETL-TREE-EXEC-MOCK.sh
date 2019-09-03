#!/bin/bash
# File: TEST-ETL-TREE-EXEC-MOCK.sh
# Date: 3-Jul-2018 jdw
#
# resource cache path
export cp=../../../CACHE/
# Example tree node list load
#
etl_exec_cli --mock --full --etl_tree_node_lists   --cache_path ${cp}  --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >& ./test-output/LOGTREENODELIST
#
#