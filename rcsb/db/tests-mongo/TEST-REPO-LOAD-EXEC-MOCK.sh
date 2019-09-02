#!/bin/bash
# File: TEST-REPOLOAD-EXEC-MOCK.sh
# Date: 1-Sep-2019 jdw
#
# Example mock repository load (MongoDb)
#
export cp=../../../CACHE/
exdb_repo_load_cli  --mock --full  --load_chem_comp_core_ref  --rebuild_cache --cache_path ${cp}  --config_path ../config/exdb-config-example.yml  --config_name site_info_configuration --fail_file_list_path ./test-output/failed-cc-core-path-list.txt --read_back_check >& ./test-output/LOGCHEMCOMPCOREFULL
exdb_repo_load_cli  --mock --full  --rebuild_schema --load_bird_chem_comp_ref  --cache_path ${cp}  --config_path ../config/exdb-config-example.yml  --config_name site_info_configuration --fail_file_list_path ./test-output/failed-bird-cc-path-list.txt --read_back_check >& ./test-output/LOGBIRDCHEMCOMPFULL
exdb_repo_load_cli  --mock --full  --load_pdbx_core_merge  --cache_path ${cp} --config_path ../config/exdb-config-example.yml  --config_name site_info_configuration --save_file_list_path ./test-output/LATEST_PDBX_CORE_LOAD_LIST.txt --fail_file_list_path ./test-output/failed-pdbx-core-path-list.txt >& ./test-output/LOGPDBXCOREFULL
#
# exdb_repo_load_cli  --mock --full  --load_pdbx_core_merge     --vrpt_repo_path ../../mock-data/MOCK_VALIDATION_REPORTS  --cache_path ${cp}  --config_path ../config/exdb-config-example.yml  --config_name site_info_configuration --save_file_list_path ./test-output/LATEST_PDBX_CORE_LOAD_LIST.txt --fail_file_list_path ./test-output/failed-pdbx-core-path-list.txt >& ./test-output/LOGPDBXCOREFULL
exdb_repo_load_cli  --mock --full --rebuild_schema --load_ihm_dev  --cache_path ${cp}  --config_path ../config/exdb-config-example.yml  --config_name site_info_configuration --save_file_list_path ./test-output/LATEST_IHM_DEV_LOAD_LIST.txt --fail_file_list_path ./test-output/failed-ihm-dev-path-list.txt >& ./test-output/LOGIHMDEVFULL
#