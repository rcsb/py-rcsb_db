#!/bin/bash
# File: TEST-SCHEMA-UPDATE-CONFIG-EXEC-LOCAL.sh
# Date: 8-Jan-2019 jdw
#
# Updates:
#
# Test schema update using configuration settings and storing data in the local test directory --
#
#
schema_update_cli  --mock --update_config_test --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info  > ./test-output/LOGUPDCONFIGSCHEMA 2>&1
#
