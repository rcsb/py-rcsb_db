#!/bin/bash
# File: TEST-ETL-TREE-EXEC-MOCK.sh
# Date: 3-Jul-2018 jdw
#
# Example tree node list load
#
etl_exec_cli --mock --full --etl_tree_node_lists   --working_path ./test-output  --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info= >& ./test-output/LOGTREENODELIST
#
#