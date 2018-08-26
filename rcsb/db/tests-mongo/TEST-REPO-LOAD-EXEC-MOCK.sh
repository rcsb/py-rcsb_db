#!/bin/bash
# File: TEST-REPOLOAD-EXEC-MOCK.sh
# Date: 3-Jul-2018 jdw
#
# Example mock repository load (MongoDb)
exdb_repo_load_cli  --mock --full  --load_chem_comp_ref       --working_path ./test-output  --config_path ../../mock-data/config/dbload-setup-example.cfg --config_name DEFAULT --fail_file_list_path ./test-output/failed-cc-path-list.txt --read_back_check >& ./test-output/LOGCHEMCOMPFULL
exdb_repo_load_cli  --mock --full  --load_bird_chem_comp_ref  --working_path ./test-output  --config_path ../../mock-data/config/dbload-setup-example.cfg --config_name DEFAULT --fail_file_list_path ./test-output/failed-bird-cc-path-list.txt --read_back_check >& ./test-output/LOGBIRDCHEMCOMPFULL
exdb_repo_load_cli  --mock --full  --load_bird_ref            --working_path ./test-output  --config_path ../../mock-data/config/dbload-setup-example.cfg --config_name DEFAULT --fail_file_list_path ./test-output/failed-bird-path-list.txt --read_back_check >& ./test-output/LOGBIRDFULL
exdb_repo_load_cli  --mock --full  --load_bird_family_ref     --working_path ./test-output  --config_path ../../mock-data/config/dbload-setup-example.cfg --config_name DEFAULT --fail_file_list_path ./test-output/failed-family-path-list.txt --read_back_check >& ./test-output/LOGBIRDFAMILY
exdb_repo_load_cli  --mock --full  --load_entry_data          --working_path ./test-output  --config_path ../../mock-data/config/dbload-setup-example.cfg --config_name DEFAULT --save_file_list_path ./test-output/LATEST_PDBX_LOAD_LIST.txt --fail_file_list_path ./test-output/failed-entry-path-list.txt >& ./test-output/LOGENTRYFULL
#
