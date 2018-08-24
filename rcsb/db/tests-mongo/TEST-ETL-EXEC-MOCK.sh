#!/bin/bash
# File: TEST-ETL-EXEC-MOCK.sh
# Date: 3-Jul-2018 jdw
#
# Example mock sequence cluster load
# python ../exec/ETLExec.py  --mock --full --etl_entity_sequence_clusters  --document_limit 1000 --data_set_id 2018_23  --sequence_cluster_data_path ../data/cluster_data/mmseqs-20180608  --working_path ./test-output  --config_path ../data/dbload-setup-example.cfg --config_name DEFAULT --read_back_check >& ./test-output/LOGETLSEQCLUSTERS
#
python ../exec/ETLExec.py  --mock --full --etl_entity_sequence_clusters  --document_limit 1000 --working_path ./test-output  --config_path ../data/dbload-setup-example.cfg --config_name DEFAULT --read_back_check >& ./test-output/LOGETLSEQCLUSTERS
#
# Example mock repository holdings load
# python ../exec/ETLExec.py  --mock --full --etl_repository_holdings  --document_limit 1000 --data_set_id 2018_23   --working_path ./test-output  --config_path ../data/dbload-setup-example.cfg --config_name DEFAULT --read_back_check >& ./test-output/LOGETLREPOHOLDINGS
python ../exec/ETLExec.py  --mock --full --etl_repository_holdings  --document_limit 1000  --working_path ./test-output  --config_path ../data/dbload-setup-example.cfg --config_name DEFAULT --read_back_check >& ./test-output/LOGETLREPOHOLDINGS
#