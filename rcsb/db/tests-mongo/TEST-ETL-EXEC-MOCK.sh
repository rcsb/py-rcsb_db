#!/bin/bash
# File: TEST-ETL-EXEC-MOCK.sh
# Date: 3-Jul-2018 jdw
#
# Example mock sequence cluster load
#
etl_exec_cli --mock --full --etl_entity_sequence_clusters  --document_limit 1000 --working_path ./test-output  --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info --read_back_check >& ./test-output/LOGETLSEQCLUSTERS
#
# Example mock repository holdings load
etl_exec_cli --mock --full --etl_repository_holdings  --document_limit 1000  --working_path ./test-output  --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info --read_back_check >& ./test-output/LOGETLREPOHOLDINGS
#