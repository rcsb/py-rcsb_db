#!/bin/bash
# File: TEST-SCHEMA-UPDATE-EXEC-LOCAL.sh
# Date: 13-Nov-2018 jdw
#
# Updates:
#
# 13-Dec-2018 jdw Add I/HM DEV schema
#  7-Jan-2019 jdw update for simplified api
#
# Test schema update in the local test directory --
#
#
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_chem_comp_ref            --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info  > ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_chem_comp_core_ref       --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_bird_chem_comp_ref       --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_bird_chem_comp_core_ref  --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_bird_ref                 --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_bird_family_ref          --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_pdbx                     --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_pdbx_core                --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
#
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_repository_holdings      --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_data_exchange            --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_entity_sequence_clusters --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_ihm_dev                  --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
schema_update_cli  --mock --schema_types rcsb,json,bson --schema_levels full,min  --update_drugbank_core            --schema_dirpath ./test-output --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info >> ./test-output/LOGUPDSCHEMA 2>&1
#
