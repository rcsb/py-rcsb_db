#!/bin/bash
# File: TEST-SCHEMA-UPDATE-EXEC-LOCAL.sh
# Date: 13-Nov-2018 jdw
#
# Updates:
#
# 13-Dec-2018 jdw Add I/HM DEV schema
#  7-Jan-2019 jdw update for simplified api
# 25-Aug-2019 jdw update to new config names and arguments
#  9-Feb-2020 jdw normalize argument names
#
# Test schema update in the local test directory --
#
#
schema_update_cli   --encoding_types rcsb,json,bson --validation_levels full,min  --update_bird_chem_comp_core_ref  --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli   --encoding_types rcsb,json,bson --validation_levels full,min  --update_pdbx_core                --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli   --encoding_types rcsb,json,bson --validation_levels full,min  --update_repository_holdings      --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli   --encoding_types rcsb,json,bson --validation_levels full,min  --update_data_exchange            --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli   --encoding_types rcsb,json,bson --validation_levels full,min  --update_entity_sequence_clusters --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli   --encoding_types rcsb,json,bson --validation_levels full,min  --update_ihm_dev                  --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli   --encoding_types rcsb,json,bson --validation_levels full,min  --update_drugbank_core            --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGUPDSCHEMA 2>&1
#
schema_update_cli  --compare_only --encoding_types json --validation_levels full  --update_bird_chem_comp_core_ref  --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGCOMPARESCHEMA 2>&1
schema_update_cli  --compare_only --encoding_types json --validation_levels full  --update_pdbx_core                --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGCOMPARESCHEMA 2>&1
schema_update_cli  --compare_only --encoding_types json --validation_levels full  --update_repository_holdings      --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGCOMPARESCHEMA 2>&1
schema_update_cli  --compare_only --encoding_types json --validation_levels full  --update_data_exchange            --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGCOMPARESCHEMA 2>&1
schema_update_cli  --compare_only --encoding_types json --validation_levels full  --update_entity_sequence_clusters --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGCOMPARESCHEMA 2>&1
schema_update_cli  --compare_only --encoding_types json --validation_levels full  --update_ihm_dev                  --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGCOMPARESCHEMA 2>&1
schema_update_cli  --compare_only --encoding_types json --validation_levels full  --update_drugbank_core            --cache_path ./test-output --config_path ../config/exdb-config-example.yml --config_name site_info_configuration >> ./test-output/LOGCOMPARESCHEMA 2>&1
#