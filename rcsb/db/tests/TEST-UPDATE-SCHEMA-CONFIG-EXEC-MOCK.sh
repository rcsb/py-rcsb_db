#!/bin/bash
# File: TEST-SCHEMA-UPDATE-CONFIG-EXEC-MOCK.sh
# Date: 8-Jan-2019 jdw
#
# Updates:
#
# Test schema update using configuration settings in the mock-data repo --
#
#
schema_update_cli  --mock --update_config_all --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info  > ./test-output/LOGUPDCONFIGMOCKSCHEMA 2>&1
#
