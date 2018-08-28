## RCSB Python Database Utility Classes

### Introduction

This module contains a collection of utility classes for processing and loading PDB repository and
derived data content using relational and document database servers.  One target data store for
these tools is a document database used to exchange content within the RCSB PDB data pipeline.

### Installation

Download the library source software from the project repository:

```bash

git clone --recurse-submodules https://github.com/rcsb/py-rcsb_db.git

```

Optionally, run test suite (Python versions 2.7, 3.6, and 3.7) using
[setuptools](https://setuptools.readthedocs.io/en/latest/) or
[tox](http://tox.readthedocs.io/en/latest/example/platform.html):

```bash
python setup.py test

or simply run

tox
```

Installation is via the program [pip](https://pypi.python.org/pypi/pip).  To run tests
from the source tree, the package must be installed in editable mode (i.e. -e):

```bash
pip install -e .
```

### Command Line Interfaces

A convenience CLI `exdb_repo_load_cli` is provided to support loading PDB repositories
containing entry and chemical reference data content types in the form of document collections
compatible with MongoDB.


```bash
exdb_repo_load_cli --help

usage:  exdb_repo_load_cli
                       [-h] [--full] [--replace] [--load_chem_comp_ref]
                       [--load_bird_chem_comp_ref] [--load_bird_ref]
                       [--load_bird_family_ref] [--load_entry_data]
                       [--load_pdbx_core] [--config_path CONFIG_PATH]
                       [--config_name CONFIG_NAME] [--db_type DB_TYPE]
                       [--document_style DOCUMENT_STYLE] [--read_back_check]
                       [--load_file_list_path LOAD_FILE_LIST_PATH]
                       [--fail_file_list_path FAIL_FILE_LIST_PATH]
                       [--save_file_list_path SAVE_FILE_LIST_PATH]
                       [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                       [--file_limit FILE_LIMIT]
                       [--prune_document_size PRUNE_DOCUMENT_SIZE] [--debug]
                       [--mock] [--working_path WORKING_PATH]

optional arguments:
  -h, --help            show this help message and exit
  --full                Fresh full load in a new tables/collections
  --replace             Load with replacement in an existing table/collection
                        (default)
  --load_chem_comp_ref  Load Chemical Component reference definitions (public
                        subset)
  --load_bird_chem_comp_ref
                        Load Bird Chemical Component reference definitions
                        (public subset)
  --load_bird_ref       Load Bird reference definitions (public subset)
  --load_bird_family_ref
                        Load Bird Family reference definitions (public subset)
  --load_entry_data     Load PDBx entry data (current released subset)
  --load_pdbx_core      Load PDBx core entry/entity data (current released
                        subset)
  --config_path CONFIG_PATH
                        Path to configuration options file
  --config_name CONFIG_NAME
                        Configuration section name
  --db_type DB_TYPE     Database server type (default=mongo)
  --document_style DOCUMENT_STYLE
                        Document organization (rowwise_by_name_with_cardinalit
                        y|rowwise_by_name|columnwise_by_name|rowwise_by_id|row
                        wise_no_name
  --read_back_check     Perform read back check on all documents
  --load_file_list_path LOAD_FILE_LIST_PATH
                        Input file containing load file path list (override
                        automatic repository scan)
  --fail_file_list_path FAIL_FILE_LIST_PATH
                        Output file containing file paths that fail to load
  --save_file_list_path SAVE_FILE_LIST_PATH
                        Save repo file paths from automatic file system scan
                        in this path
  --num_proc NUM_PROC   Number of processes to execute (default=2)
  --chunk_size CHUNK_SIZE
                        Number of files loaded per process
  --file_limit FILE_LIMIT
                        Load file limit for testing
  --prune_document_size PRUNE_DOCUMENT_SIZE
                        Prune large documents to this size limit (MB)
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for testing
  --working_path WORKING_PATH
                        Working path for temporary files


exdb_repo_load_cli --help

usage:  exdb_repo_load_cli [-h] [--full] [--replace] [--load_chem_comp_ref]
                           [--load_bird_chem_comp_ref] [--load_bird_ref]
                           [--load_bird_family_ref] [--load_entry_data]
                           [--config_path CONFIG_PATH] [--config_name CONFIG_NAME]
                           [--db_type DB_TYPE] [--document_style DOCUMENT_STYLE]
                           [--read_back_check]
                           [--load_file_list_path LOAD_FILE_LIST_PATH]
                           [--fail_file_list_path FAIL_FILE_LIST_PATH]
                           [--save_file_list_path SAVE_FILE_LIST_PATH]
                           [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                           [--file_limit FILE_LIMIT]
                           [--prune_document_size PRUNE_DOCUMENT_SIZE] [--debug]
                           [--mock] [--working_path WORKING_PATH]

optional arguments:
  -h, --help            show this help message and exit
  --full                Fresh full load in a new tables/collections
  --replace             Load with replacement in an existing table/collection
                        (default)
  --load_chem_comp_ref  Load Chemical Component reference definitions (public
                        subset)
  --load_bird_chem_comp_ref
                        Load Bird Chemical Component reference definitions
                        (public subset)
  --load_bird_ref       Load Bird reference definitions (public subset)
  --load_bird_family_ref
                        Load Bird Family reference definitions (public subset)
  --load_entry_data     Load PDB entry data (current released subset)
  --config_path CONFIG_PATH
                        Path to configuration options file
  --config_name CONFIG_NAME
                        Configuration section name
  --db_type DB_TYPE     Database server type (default=mongo)
  --document_style DOCUMENT_STYLE
                        Document organization (rowwise_by_name_with_cardinalit
                        y|rowwise_by_name|columnwise_by_name|rowwise_by_id|row
                        wise_no_name
  --read_back_check     Perform read back check on all documents
  --load_file_list_path LOAD_FILE_LIST_PATH
                        Input file containing load file path list (override
                        automatic repository scan)
  --fail_file_list_path FAIL_FILE_LIST_PATH
                        Output file containing file paths that fail to load
  --save_file_list_path SAVE_FILE_LIST_PATH
                        Save repo file paths from automatic file system scan
                        in this path
  --num_proc NUM_PROC   Number of processes to execute (default=2)
  --chunk_size CHUNK_SIZE
                        Number of files loaded per process
  --file_limit FILE_LIMIT
                        Load file limit for testing
  --prune_document_size PRUNE_DOCUMENT_SIZE
                        Prune large documents to this size limit (MB)
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for testing
  --working_path WORKING_PATH
                        Working path for temporary files

```

Part of the schema definition process supported by this module involves refining
the dictionary metadata with more specific data typing and coverage details.
A scanning tools is provided to collect and organize these details for the
other ETL tools in this package.  The following convenience CLI, `repo_scan_cli`,
is provided to scan supported PDB repository content and update data type and coverage details.


```bash
repo_scan_cli --help

usage: repo_scan_cli   [-h] [--dict_file_path DICT_FILE_PATH]
                       [--scan_chem_comp_ref | --scan_bird_chem_comp_ref | --scan_bird_ref | --scan_bird_family_ref | --scan_entry_data]
                       [--config_path CONFIG_PATH] [--config_name CONFIG_NAME]
                       [--input_file_list_path INPUT_FILE_LIST_PATH]
                       [--output_file_list_path OUTPUT_FILE_LIST_PATH]
                       [--fail_file_list_path FAIL_FILE_LIST_PATH]
                       [--scan_data_file_path SCAN_DATA_FILE_PATH]
                       [--coverage_file_path COVERAGE_FILE_PATH]
                       [--type_map_file_path TYPE_MAP_FILE_PATH]
                       [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                       [--file_limit FILE_LIMIT] [--debug] [--mock]
                       [--working_path WORKING_PATH]

optional arguments:
  -h, --help            show this help message and exit
  --dict_file_path DICT_FILE_PATH
                        PDBx/mmCIF dictionary file path
  --scan_chem_comp_ref  Scan Chemical Component reference definitions (public
                        subset)
  --scan_bird_chem_comp_ref
                        Scan Bird Chemical Component reference definitions
                        (public subset)
  --scan_bird_ref       Scan Bird reference definitions (public subset)
  --scan_bird_family_ref
                        Scan Bird Family reference definitions (public subset)
  --scan_entry_data     Scan PDB entry data (current released subset)
  --config_path CONFIG_PATH
                        Path to configuration options file
  --config_name CONFIG_NAME
                        Configuration section name
  --input_file_list_path INPUT_FILE_LIST_PATH
                        Input file containing file paths to scan
  --output_file_list_path OUTPUT_FILE_LIST_PATH
                        Output file containing file paths scanned
  --fail_file_list_path FAIL_FILE_LIST_PATH
                        Output file containing file paths that fail scan
  --scan_data_file_path SCAN_DATA_FILE_PATH
                        Output working file storing scan data (Pickle)
  --coverage_file_path COVERAGE_FILE_PATH
                        Coverage map (JSON) output path
  --type_map_file_path TYPE_MAP_FILE_PATH
                        Type map (JSON) output path
  --num_proc NUM_PROC   Number of processes to execute (default=2)
  --chunk_size CHUNK_SIZE
                        Number of files loaded per process
  --file_limit FILE_LIMIT
                        Load file limit for testing
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for testing
  --working_path WORKING_PATH
                        Working path for temporary files
```

The following CLI provides a preliminary access to ETL functions for processing
derived content types such as sequence comparative data.

```bash
etl_exec_cli  --help

usage: etl_exec_cli   [-h] [--full] [--etl_entity_sequence_clusters]
                      [--data_set_id DATA_SET_ID]
                      [--sequence_cluster_data_path SEQUENCE_CLUSTER_DATA_PATH]
                      [--config_path CONFIG_PATH] [--config_name CONFIG_NAME]
                      [--db_type DB_TYPE] [--read_back_check]
                      [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                      [--document_limit DOCUMENT_LIMIT]
                      [--prune_document_size PRUNE_DOCUMENT_SIZE] [--debug]
                      [--mock] [--working_path WORKING_PATH]

optional arguments:
  -h, --help            show this help message and exit
  --full                Fresh full load in a new tables/collections (Default)
  --etl_entity_sequence_clusters
                        ETL entity sequence clusters
  --data_set_id DATA_SET_ID
                        Data set identifier (e.g., 2018_14)
  --sequence_cluster_data_path SEQUENCE_CLUSTER_DATA_PATH
                        Sequence cluster data path
  --config_path CONFIG_PATH
                        Path to configuration options file
  --config_name CONFIG_NAME
                        Configuration section name
  --db_type DB_TYPE     Database server type (default=mongo)
  --read_back_check     Perform read back check on all documents
  --num_proc NUM_PROC   Number of processes to execute (default=2)
  --chunk_size CHUNK_SIZE
                        Number of files loaded per process
  --document_limit DOCUMENT_LIMIT
                        Load document limit for testing
  --prune_document_size PRUNE_DOCUMENT_SIZE
                        Prune large documents to this size limit (MB)
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for testing
  --working_path WORKING_PATH
                        Working path for temporary files
```

### Examples

If you are working in the source repository, then you can run the CLI commands in the following manner.
The following examples load data in the mock repositories in source distribution assuming you have a local
default installation of MongoDb (no user/pw assigned).

To run the command-line interface `exdb_repo_load_cli` outside of the source distribution, you will need to
create a configuration file with the appropriate path details and authentication credentials.

For instance, to perform a fresh/full load of all of the chemical component definitions in the mock repository:

```bash

cd rcsb/db/scripts
python RepoLoadExec.py --full  --load_chem_comp_ref  \
                      --config_path ../../mock-data/config/dbload-setup-example.cfg \
                      --config_name DEFAULT \
                      --fail_file_list_path failed-cc-path-list.txt \
                      --read_back_check
```


The following illustrates, a full load of the mock structure data repository followed by a reload with replacement of
this same data.

```bash

cd rcsb/db/scripts
python RepoLoadExec.py  --mock --full  --load_entry_data \
                     --config_path ../../mock-data/config/dbload-setup-example.cfg \
                     --config_name DEFAULT \
                     --save_file_list_path  LATEST_PDBX_LOAD_LIST.txt \
                     --fail_file_list_path failed-entry-path-list.txt

python RepoLoadExec.py --mock --replace  --load_entry_data \
                      --config_path ../../mock-data/config/dbload-setup-example.cfg \
                      --config_name DEFAULT \
                      --load_file_list_path  LATEST_PDBX_LOAD_LIST.txt \
                      --fail_file_list_path failed-entry-path-list.txt
```



### Configuration Example

RCSB/PDB repository path details are stored as configuration options.
An example configuration file included in this package is shown below.
This example is references dictionary resources and mock repository data
provided in the package in `rcsb/mock-data/*`.
The `DEFAULT` section provides database server connection details.  This is
followed by sections specifying the dictionaries and helper functions used
to define the schema for the each supported content type (e.g., pdbx, chem_comp,
bird, bird_family,.. ).


```bash
# File: dbload-setup-example.cfg
[DEFAULT]
#
BIRD_REPO_PATH=MOCK_BIRD_REPO
BIRD_FAMILY_REPO_PATH=MOCK_BIRD_FAMILY_REPO
BIRD_CHEM_COMP_REPO_PATH=MOCK_BIRD_CC_REPO
CHEM_COMP_REPO_PATH=MOCK_CHEM_COMP_REPO
PDBX_REPO_PATH=MOCK_PDBX_SANDBOX
#
RCSB_EXCHANGE_SANDBOX_PATH=MOCK_EXCHANGE_SANDBOX
RCSB_SEQUENCE_CLUSTER_DATA_PATH=cluster_data/mmseqs-20180608
#
MONGO_DB_HOST=localhost
MONGO_DB_PORT=27017
MONGO_DB_USER=
MONGO_DB_PASSWORD=
#
#
MYSQL_DB_HOST_NAME=localhost
MYSQL_DB_PORT_NUMBER=3306
#MYSQL_DB_USER_NAME=wwpdbmgr
#MYSQL_DB_PASSWORD=wwpdb0000
MYSQL_DB_USER_NAME=root
MYSQL_DB_PASSWORD=mysql0000
MYSQL_DB_DATABASE_NAME=mysql
#
CRATE_DB_HOST=localhost
CRATE_DB_PORT=4200
#
COCKROACH_DB_HOST=localhost
COCKROACH_DB_PORT=26257
COCKROACH_DB_NAME=system
COCKROACH_DB_USER_NAME=root
#
PDBX_DICT_LOCATOR=dictionaries/mmcif_pdbx_v5_next.dic
RCSB_DICT_LOCATOR=dictionaries/rcsb_mmcif_ext_v1.dic
PROVENANCE_INFO_LOCATOR=provenance/rcsb_extend_provenance_info.json
DICT_METHOD_HELPER_MODULE=rcsb.db.helpers.DictMethodRunnerHelper

## -------------------
[pdbx]
PDBX_DICT_LOCATOR=dictionaries/mmcif_pdbx_v5_next.dic
RCSB_DICT_LOCATOR=dictionaries/rcsb_mmcif_ext_v1.dic
PROVENANCE_INFO_LOCATOR=provenance/rcsb_extend_provenance_info.json
#
SCHEMA_NAME=pdbx
SCHEMA_DEF_LOCATOR_SQL=schema/schema_def-pdbx-SQL.json
SCHEMA_DEF_LOCATOR_ANY=schema/schema_def-pdbx-ANY.json
INSTANCE_DATA_TYPE_INFO_LOCATOR=data_type_info/scan-pdbx-type-map.json
APP_DATA_TYPE_INFO_LOCATOR=data_type_info/app_data_type_mapping.cif

DICT_HELPER_MODULE=rcsb.db.helpers.DictInfoHelper
SCHEMADEF_HELPER_MODULE=rcsb.db.helpers.SchemaDefHelper
DOCUMENT_HELPER_MODULE=rcsb.db.helpers.SchemaDocumentHelper
DICT_METHOD_HELPER_MODULE=rcsb.db.helpers.DictMethodRunnerHelper

## -------------------
[pdbx_core]
PDBX_DICT_LOCATOR=dictionaries/mmcif_pdbx_v5_next.dic
RCSB_DICT_LOCATOR=dictionaries/rcsb_mmcif_ext_v1.dic
PROVENANCE_INFO_LOCATOR=provenance/rcsb_extend_provenance_info.json
#
SCHEMA_NAME=pdbx_core
SCHEMA_DEF_LOCATOR_SQL=schema/schema_def-pdbx_core-SQL.json
SCHEMA_DEF_LOCATOR_ANY=schema/schema_def-pdbx_core-ANY.json
INSTANCE_DATA_TYPE_INFO_LOCATOR=data_type_info/scan-pdbx-type-map.json
APP_DATA_TYPE_INFO_LOCATOR=data_type_info/app_data_type_mapping.cif

DICT_HELPER_MODULE=rcsb.db.helpers.DictInfoHelper
SCHEMADEF_HELPER_MODULE=rcsb.db.helpers.SchemaDefHelper
DOCUMENT_HELPER_MODULE=rcsb.db.helpers.SchemaDocumentHelper
DICT_METHOD_HELPER_MODULE=rcsb.db.helpers.DictMethodRunnerHelper

## -------------------
[chem_comp]
PDBX_DICT_LOCATOR =dictionaries/mmcif_pdbx_v5_next.dic
RCSB_DICT_LOCATOR=dictionaries/rcsb_mmcif_ext_v1.dic
PROVENANCE_INFO_LOCATOR=provenance/rcsb_extend_provenance_info.json
#
SCHEMA_NAME=chem_comp
SCHEMA_DEF_LOCATOR_SQL=schema/schema_def-chem_comp-SQL.json
SCHEMA_DEF_LOCATOR_ANY=schema/schema_def-chem_comp-ANY.json
INSTANCE_DATA_TYPE_INFO_LOCATOR=data_type_info/scan-chem_comp-type-map.json
APP_DATA_TYPE_INFO_LOCATOR=data_type_info/app_data_type_mapping.cif

DICT_HELPER_MODULE=rcsb.db.helpers.DictInfoHelper
SCHEMADEF_HELPER_MODULE=rcsb.db.helpers.SchemaDefHelper
DOCUMENT_HELPER_MODULE=rcsb.db.helpers.SchemaDocumentHelper
DICT_METHOD_HELPER_MODULE=rcsb.db.helpers.DictMethodRunnerHelper

## -------------------
[bird_chem_comp]
PDBX_DICT_LOCATOR=dictionaries/mmcif_pdbx_v5_next.dic
RCSB_DICT_LOCATOR=dictionaries/rcsb_mmcif_ext_v1.dic
PROVENANCE_INFO_LOCATOR=provenance/rcsb_extend_provenance_info.json
#
SCHEMA_NAME=bird_chem_comp
SCHEMA_DEF_LOCATOR_SQL=schema/schema_def-bird_chem_comp-SQL.json
SCHEMA_DEF_LOCATOR_ANY=schema/schema_def-bird_chem_comp-ANY.json
INSTANCE_DATA_TYPE_INFO_LOCATOR=data_type_info/scan-bird_chem_comp-type-map.json
APP_DATA_TYPE_INFO_LOCATOR=data_type_info/app_data_type_mapping.cif

DICT_HELPER_MODULE=rcsb.db.helpers.DictInfoHelper
SCHEMADEF_HELPER_MODULE=rcsb.db.helpers.SchemaDefHelper
DOCUMENT_HELPER_MODULE=rcsb.db.helpers.SchemaDocumentHelper
DICT_METHOD_HELPER_MODULE=rcsb.db.helpers.DictMethodRunnerHelper

## -------------------
[bird]
PDBX_DICT_LOCATOR=dictionaries/mmcif_pdbx_v5_next.dic
RCSB_DICT_LOCATOR=dictionaries/rcsb_mmcif_ext_v1.dic
PROVENANCE_INFO_LOCATOR=provenance/rcsb_extend_provenance_info.json
#
SCHEMA_NAME=bird
SCHEMA_DEF_LOCATOR_SQL=schema/schema_def-bird-SQL.json
SCHEMA_DEF_LOCATOR_ANY=schema/schema_def-bird-ANY.json
INSTANCE_DATA_TYPE_INFO_LOCATOR=data_type_info/scan-bird-type-map.json
APP_DATA_TYPE_INFO_LOCATOR=data_type_info/app_data_type_mapping.cif

DICT_HELPER_MODULE=rcsb.db.helpers.DictInfoHelper
SCHEMADEF_HELPER_MODULE=rcsb.db.helpers.SchemaDefHelper
DOCUMENT_HELPER_MODULE=rcsb.db.helpers.SchemaDocumentHelper
DICT_METHOD_HELPER_MODULE=rcsb.db.helpers.DictMethodRunnerHelper

## -------------------
[bird_family]
PDBX_DICT_LOCATOR=dictionaries/mmcif_pdbx_v5_next.dic
RCSB_DICT_LOCATOR=dictionaries/rcsb_mmcif_ext_v1.dic
PROVENANCE_INFO_LOCATOR=provenance/rcsb_extend_provenance_info.json
#
SCHEMA_NAME=bird_family
SCHEMA_DEF_LOCATOR_SQL=schema/schema_def-bird_family-SQL.json
SCHEMA_DEF_LOCATOR_ANY=schema/schema_def-bird_family-ANY.json
INSTANCE_DATA_TYPE_INFO_LOCATOR=data_type_info/scan-bird_family-type-map.json
APP_DATA_TYPE_INFO_LOCATOR=data_type_info/app_data_type_mapping.cif

DICT_HELPER_MODULE=rcsb.db.helpers.DictInfoHelper
SCHEMADEF_HELPER_MODULE=rcsb.db.helpers.SchemaDefHelper
DOCUMENT_HELPER_MODULE=rcsb.db.helpers.SchemaDocumentHelper
DICT_METHOD_HELPER_MODULE=rcsb.db.helpers.DictMethodRunnerHelper

## -------------------
[entity_sequence_clusters]
DATABASE_NAME=sequence_clusters
DATABASE_VERSION_STRING=v5
COLLECTION_ENTITY_MEMBERS=entity_members
COLLECTION_ENTITY_MEMBERS_INDEX=data_set_id,entry_id,entity_id
COLLECTION_CLUSTER_MEMBERS=cluster_members
COLLECTION_CLUSTER_MEMBERS_INDEX=data_set_id,identity,cluster_id
COLLECTION_VERSION_STRING=v0_1
ENTITY_SCHEMA_NAME=rcsb_entity_sequence_cluster_entity_list
CLUSTER_SCHEMA_NAME=rcsb_entity_sequence_cluster_identifer_list
SEQUENCE_IDENTITY_LEVELS=100,95,90,70,50,30
COLLECTION_CLUSTER_PROVENANCE=cluster_provenance
PROVENANCE_KEY_NAME=rcsb_entity_sequence_cluster_prov
PROVENANCE_INFO_LOCATOR=provenance/rcsb_extend_provenance_info.json
## -------------------
[repository_holdings]
DATABASE_NAME=repository_holdings
DATABASE_VERSION_STRING=v5
COLLECTION_HOLDINGS_UPDATE=rcsb_repository_holdings_update
COLLECTION_HOLDINGS_CURRENT=rcsb_repository_holdings_current
COLLECTION_HOLDINGS_UNRELEASED=rcsb_repository_holdings_unreleased
COLLECTION_HOLDINGS_REMOVED=rcsb_repository_holdings_removed
COLLECTION_HOLDINGS_REMOVED_AUTHORS=rcsb_repository_holdings_removed_audit_authors
COLLECTION_HOLDINGS_SUPERSEDED=rcsb_repository_holdings_superseded
COLLECTION_VERSION_STRING=v0_1

## -------------------
[data_exchange_status]
DATABASE_NAME=data_exchange
DATABASE_VERSION_STRING=v5
COLLECTION_UPDATE_STATUS=rcsb_data_exchange_status
COLLECTION_VERSION_STRING=v0_1
## --------------------
```
