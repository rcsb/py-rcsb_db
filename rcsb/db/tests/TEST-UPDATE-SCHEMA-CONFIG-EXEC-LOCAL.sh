#!/bin/bash
# File: TEST-SCHEMA-UPDATE-CONFIG-EXEC-LOCAL.sh
# Date: 8-Jan-2019 jdw
#
# Updates:
#
# Test schema update and comparison using configuration settings and storing data in the local test directory --
#
#
schema_update_cli --update_config_deployed --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration  > ./test-output/LOGUPDSCHEMACONFIG 2>&1
##
schema_update_cli --compare_only --update_config_deployed --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration  > ./test-output/LOGCOMPARECONFIG 2>&1
#