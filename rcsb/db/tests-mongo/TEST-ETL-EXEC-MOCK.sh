#!/bin/bash
# File: TEST-ETL-EXEC-MOCK.sh
# Date: 3-Jul-2018 jdw
#
# resource cache path
export cp=../../../CACHE/
#
# Example mock sequence cluster load
#
etl_exec_cli --mock --full --etl_entity_sequence_clusters  --document_limit 1000 --cache_path ${cp}  --config_path ../config/exdb-config-example.yml  --config_name site_info_configuration --read_back_check >& ./test-output/LOGETLSEQCLUSTERS
#
# Example mock repository holdings load
etl_exec_cli --mock --full --etl_repository_holdings  --document_limit 1000  --cache_path ${cp} --config_path ../config/exdb-config-example.yml --config_name site_info_configuration --read_back_check >& ./test-output/LOGETLREPOHOLDINGS
#