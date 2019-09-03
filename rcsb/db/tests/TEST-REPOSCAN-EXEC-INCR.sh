#!/bin/bash
# File: TEST-REPOSCAN-EXEC-INCR.sh
# Date: 20-Aug-2018
#
# Test of ScanRepo CLI to extend a full repository scan -
#
# First copy an existing scan data file to the test-output sub-directory for extension.
#gzcat ~/Desktop/scan-entry-data.pic.gz > ./test-output/scan-entry-data.pic
#
repo_scan_cli  --mock  --scanType incr --scan_entry_data  --working_path ./test-output --scan_data_file_path ./test-output/scan-entry-data.pic --coverage_file_path ./test-output/scan-entry-coverage.json --type_map_file_path ./test-output/scan-entry-type-map.json  --config_path ../config/exdb-config-example.yml --config_name site_info_configuration --fail_file_list_path ./test-output/scan-failed-entry-path-list.txt >& ./test-output/LOGENTRYFULL
#
