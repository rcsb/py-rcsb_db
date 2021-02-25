# RCSB DB

## A collection of Python Database Utility Classes

[![Build Status](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_apis/build/status/rcsb.py-rcsb_db?branchName=master)](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_build/latest?definitionId=12&branchName=master)

## Introduction

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

#### Installing in Ubuntu Linux (tested in 18.04)

You will need a few packages, before `pip install .` can work:

```bash

sudo apt install default-libmysqlclient-dev flex bison

```

### Installing on macOS

To use and develop this package on macOS requires a number of packages that are not
distributed as part of the base macOS operating system.
The following steps provide one approach to creating the development environment for this
package.  First, install the Apple [XCode](https://developer.apple.com/xcode/) package and associate command-line tools.
This will provide essential compilers and supporting tools.  The [HomeBrew](https://brew.sh/) package
manager provides further access to a variety of common open source services and tools.
Follow the instructions provided by at the [HomeBrew](https://brew.sh/) site to
install this system.   Once HomeBrew is installed, you can further install the
[MariaDB](https://mariadb.com/kb/en/library/installing-mariadb-on-macos-using-homebrew/) and
[MongoDB](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/) packages which
are required to support the ExDB  tools.  HomeBrew also provides a variety of options for
managing a [Python virtual environments](https://gist.github.com/Geoyi/f55ed54d24cc9ff1c14bd95fac21c042).

### Command Line Interfaces

A convenience CLI `schema_update_cli` is provided for generating operational schema from
PDBx/mmCIF dictionary metadata.  Schema are encoded for the ExDB  API (rcsb), and
for the document schema encoded in JSON and BSON formats.  The latter schema can be used to
validate the loadable document objects produced for the collections served by MongoDB.

```bash
 => schema_update_cli  --help
usage: schema_update_cli [-h] [--update_chem_comp_ref]
                         [--update_chem_comp_core_ref]
                         [--update_bird_chem_comp_ref]
                         [--update_bird_chem_comp_core_ref]
                         [--update_bird_ref] [--update_bird_family_ref]
                         [--update_pdbx] [--update_pdbx_core]
                         [--update_repository_holdings]
                         [--update_entity_sequence_clusters]
                         [--update_data_exchange] [--update_ihm_dev]
                         [--update_drugbank_core] [--update_config_all]
                         [--update_config_deployed] [--update_config_test]
                         [--config_path CONFIG_PATH]
                         [--config_name CONFIG_NAME]
                         [--cache_path SCHEMA_CACHE_PATH]
                         [--schema_types SCHEMA_TYPES]
                         [--schema_levels SCHEMA_LEVELS] [--debug] [--mock]

optional arguments:
  -h, --help            show this help message and exit
  --update_chem_comp_ref
                        Update schema for Chemical Component reference
                        definitions
  --update_chem_comp_core_ref
                        Update core schema for Chemical Component reference
                        definitions
  --update_bird_chem_comp_ref
                        Update schema for Bird Chemical Component reference
                        definitions
  --update_bird_chem_comp_core_ref
                        Update core schema for Bird Chemical Component
                        reference definitions
  --update_bird_ref     Update schema for Bird reference definitions
  --update_bird_family_ref
                        Update schema for Bird Family reference definitions
  --update_pdbx         Update schema for PDBx entry data
  --update_pdbx_core    Update schema for PDBx core entry/entity data
  --update_repository_holdings
                        Update schema for repository holdings
  --update_entity_sequence_clusters
                        Update schema for entity sequence clusters
  --update_data_exchange
                        Update schema for data exchange status
  --update_ihm_dev      Update schema for I/HM dev entry data
  --update_drugbank_core
                        Update DrugBank schema
  --update_config_all   Update using configuration settings (e.g.
                        DATABASE_NAMES_ALL)
  --update_config_deployed
                        Update using configuration settings (e.g.
                        DATABASE_NAMES_DEPLOYED)
  --update_config_test  Update using configuration settings (e.g.
                        DATABASE_NAMES_TEST)
  --config_path CONFIG_PATH
                        Path to configuration options file
  --config_name CONFIG_NAME
                        Configuration section name
  --cache_path CACHE_PATH
                        Schema cache directory path
  --schema_types SCHEMA_TYPES
                        Schema encoding (rcsb|json|bson) (comma separated)
  --schema_levels SCHEMA_LEVELS
                        Schema validation level (full|min) (comma separated)
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for dependencies and
                        testing
________________________________________________________________________________

```

For example, the following command will generate the JSON and BSON schema for the collections in the
pdbx_core schema.

```bash
schema_update_cli  --mock --schema_types json,bson \
                   --schema_level full  \
                   --update_pdbx_core   \
                   --cache_path . \
                   --config_path ./rcsb/db/config/exdb-config-example.yml  \
                   --config_name site_info_configuration
```

A convenience CLI `exdb_repo_load_cli` is provided to support loading PDB repositories
containing entry and chemical reference data content types in the form of document collections
compatible with MongoDB.

```bash
exdb_repo_load_cli --help

usage: exdb_repo_load_cli [-h] [--full] [--replace] [--load_chem_comp_ref]
                          [--load_chem_comp_core_ref]
                          [--load_bird_chem_comp_ref]
                          [--load_bird_chem_comp_core_ref] [--load_bird_ref]
                          [--load_bird_family_ref] [--load_entry_data]
                          [--load_pdbx_core] [--load_pdbx_core_merge]
                          [--load_pdbx_core_entry] [--load_pdbx_core_entity]
                          [--load_pdbx_core_entity_monomer]
                          [--load_pdbx_core_assembly] [--load_ihm_dev]
                          [--config_path CONFIG_PATH]
                          [--config_name CONFIG_NAME] [--db_type DB_TYPE]
                          [--document_style DOCUMENT_STYLE]
                          [--read_back_check] [--schema_level SCHEMA_LEVEL]
                          [--load_file_list_path LOAD_FILE_LIST_PATH]
                          [--fail_file_list_path FAIL_FILE_LIST_PATH]
                          [--save_file_list_path SAVE_FILE_LIST_PATH]
                          [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                          [--file_limit FILE_LIMIT]
                          [--prune_document_size PRUNE_DOCUMENT_SIZE]
                          [--debug] [--mock] [--cache_path CACHE_PATH]
                          [--rebuild_cache] [--rebuild_schema]
                          [--vrpt_repo_path VRPT_REPO_PATH]

optional arguments:
  -h, --help            show this help message and exit
  --full                Fresh full load in a new tables/collections
  --replace             Load with replacement in an existing table/collection
                        (default)
  --load_chem_comp_ref  Load Chemical Component reference definitions (public
                        subset)
  --load_chem_comp_core_ref
                        Load Chemical Component Core reference definitions
                        (public subset)
  --load_bird_chem_comp_ref
                        Load Bird Chemical Component reference definitions
                        (public subset)
  --load_bird_chem_comp_core_ref
                        Load Bird Chemical Component Core reference
                        definitions (public subset)
  --load_bird_ref       Load Bird reference definitions (public subset)
  --load_bird_family_ref
                        Load Bird Family reference definitions (public subset)
  --load_entry_data     Load PDBx entry data (current released subset)
  --load_pdbx_core      Load all PDBx core collections (current released
                        subset)
  --load_pdbx_core_merge
                        Load all PDBx core collections with merged content
                        (current released subset)
  --load_pdbx_core_entry
                        Load PDBx core entry (current released subset)
  --load_pdbx_core_entity
                        Load PDBx core entity (current released subset)
  --load_pdbx_core_entity_monomer
                        Load PDBx core entity monomer (current released
                        subset)
  --load_pdbx_core_assembly
                        Load PDBx core assembly (current released subset)
  --load_ihm_dev        Load I/HM DEV model data (current released subset)
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
  --schema_level SCHEMA_LEVEL
                        Schema validation level (full|min default=None)
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
  --cache_path CACHE_PATH
                        Cache path for resource files
  --rebuild_cache       Rebuild cached resource files
  --rebuild_schema      Rebuild schema on-the-fly if not cached
  --vrpt_repo_path VRPT_REPO_PATH
                        Path to validation report repository
________________________________________________________________________________
```

Part of the schema definition process supported by this module involves refining
the dictionary metadata with more specific data typing and coverage details.
A scanning tools is provided to collect and organize these details for the
other ETL tools in this package.  The following convenience CLI, `repo_scan_cli`,
is provided to scan supported PDB repository content and update data type and coverage details.

```bash
repo_scan_cli --help

usage: repo_scan_cli [-h] [--scanType SCANTYPE]
                     [--scan_chem_comp_ref | --scan_chem_comp_core_ref | --scan_bird_chem_comp_ref | --scan_bird_chem_comp_core_ref | --scan_bird_ref | --scan_bird_family_ref | --scan_entry_data | --scan_ihm_dev]
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
  --scanType SCANTYPE   Repository scan type (full|incr)
  --scan_chem_comp_ref  Scan Chemical Component reference definitions (public
                        subset)
  --scan_chem_comp_core_ref
                        Scan Chemical Component Core reference definitions
                        (public subset)
  --scan_bird_chem_comp_ref
                        Scan Bird Chemical Component reference definitions
                        (public subset)
  --scan_bird_chem_comp_core_ref
                        Scan Bird Chemical Component Core reference
                        definitions (public subset)
  --scan_bird_ref       Scan Bird reference definitions (public subset)
  --scan_bird_family_ref
                        Scan Bird Family reference definitions (public subset)
  --scan_entry_data     Scan PDB entry data (current released subset)
  --scan_ihm_dev        Scan PDBDEV I/HM entry data (current released subset)
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
________________________________________________________________________________

```

The following CLI provides a preliminary access to ETL functions for processing
derived content types such as sequence comparative data.

```bash
etl_exec_cli --help
usage: etl_exec_cli [-h] [--full] [--etl_entity_sequence_clusters]
                    [--etl_repository_holdings] [--data_set_id DATA_SET_ID]
                    [--sequence_cluster_data_path SEQUENCE_CLUSTER_DATA_PATH]
                    [--sandbox_data_path SANDBOX_DATA_PATH]
                    [--config_path CONFIG_PATH] [--config_name CONFIG_NAME]
                    [--db_type DB_TYPE] [--read_back_check]
                    [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                    [--document_limit DOCUMENT_LIMIT]
                    [--prune_document_size PRUNE_DOCUMENT_SIZE] [--debug]
                    [--mock] [--cache_path CACHE_PATH] [--rebuild_cache]

optional arguments:
  -h, --help            show this help message and exit
  --full                Fresh full load in a new tables/collections (Default)
  --etl_entity_sequence_clusters
                        ETL entity sequence clusters
  --etl_repository_holdings
                        ETL repository holdings
  --data_set_id DATA_SET_ID
                        Data set identifier (default= 2018_14 for current
                        week)
  --sequence_cluster_data_path SEQUENCE_CLUSTER_DATA_PATH
                        Sequence cluster data path (default set by
                        configuration
  --sandbox_data_path SANDBOX_DATA_PATH
                        Date exchange sandboxPath data path (default set by
                        configuration
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
  --cache_path CACHE_PATH
                        Path containing cache directories
  --rebuild_cache       Rebuild cached resource files
________________________________________________________________________________

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
                      --config_path ../config/exdb-config-example.yml \
                      --config_name site_info_configuration \
                      --fail_file_list_path failed-cc-path-list.txt \
                      --read_back_check
```

The following illustrates, a full load of the mock structure data repository followed by a reload with replacement of
this same data.

```bash

cd rcsb/db/scripts
python RepoLoadExec.py  --mock --full  --load_entry_data \
                     --config_path ../config/exdb-config-example.yml \
                     --config_name site_info_configuration \
                     --save_file_list_path  LATEST_PDBX_LOAD_LIST.txt \
                     --fail_file_list_path failed-entry-path-list.txt

python RepoLoadExec.py --mock --replace  --load_entry_data \
                      --config_path ../config/exdb-config-example.yml \
                      --config_name site_info_configuration \
                      --load_file_list_path  LATEST_PDBX_LOAD_LIST.txt \
                      --fail_file_list_path failed-entry-path-list.txt
```

### Configuration Example

RCSB/PDB repository path details are stored as configuration options.
An example configuration file included in this package is shown below.
This example is references dictionary resources and mock repository data
provided in the package in `rcsb/mock-data/*`. Example configuration details are
stored in rcsb/db/config/exdb-config-example.yml.The `site_info_configuration` section
in this file provides database server connection details and common path details.
This is followed by sections specifying the dictionaries, helper functions, and
configuration used to define the schema for the each supported content type
(e.g., pdbx_core, chem_comp_core, bird_chem_comp_core,.. ).

```yaml
site_info_configuration:
  # Site specific path and server configuration options - (REFERENCING DEVELOPMENT RESOURCES)
  #
  CONFIG_SUPPORT_TOKEN: CONFIG_SUPPORT_TOKEN_ENV
  #
  # Database server connection details
  #
  MONGO_DB_HOST: localhost
  MONGO_DB_PORT: "27017"
  _MONGO_DB_USER_NAME: ""
  _MONGO_DB_PASSWORD: ""
  MYSQL_DB_HOST_NAME: localhost
  MYSQL_DB_PORT_NUMBER: "3306"
  _MYSQL_DB_USER_NAME: wrIzBGtCsQmkjc7tbEPQ3oEaOnpvivXaKcQsvXD6kn4KHMvA7LCL4O9GlAI=
  _MYSQL_DB_PASSWORD: qXPp32Z6DhNVMwo9fQIK5+KB13c1Jd43E3Bn6LmJcSyXc0NAt4H/hwo/xglYpmELV5Vqaw==
  _MYSQL_DB_PASSWORD_ALT: s6mNxq3FIwZLrLiIeHpDZQcuVxfQqrR3gA+dEMOGgHwsjrJV5da08H74RmnNRus74Q==
  MYSQL_DB_DATABASE_NAME: mysql
  CRATE_DB_HOST: localhost
  CRATE_DB_PORT: "4200"
  COCKROACH_DB_HOST: localhost
  COCKROACH_DB_PORT: "26257"
  COCKROACH_DB_NAME: system
  _COCKROACH_DB_USER_NAME: HR2ez8iLbEpvN+hXKIQS3qa6/QpiFRpf/WvrfHiwfjcL09E+iWTQJhsxTsw=
  #
  # Primary repository data and related computed repository data paths
  #
  BIRD_REPO_PATH: MOCK_BIRD_REPO
  BIRD_FAMILY_REPO_PATH: MOCK_BIRD_FAMILY_REPO
  BIRD_CHEM_COMP_REPO_PATH: MOCK_BIRD_CC_REPO
  CHEM_COMP_REPO_PATH: MOCK_CHEM_COMP_REPO
  PDBX_REPO_PATH: MOCK_PDBX_SANDBOX
  RCSB_EXCHANGE_SANDBOX_PATH: MOCK_EXCHANGE_SANDBOX
  IHM_DEV_REPO_PATH: MOCK_IHM_REPO
  VRPT_REPO_PATH: MOCK_VALIDATION_REPORTS
  VRPT_REPO_PATH_ENV: VRPT_REPO_PATH_ALT
  #
  RCSB_EDMAP_LIST_PATH: MOCK_EXCHANGE_SANDBOX/status/edmaps.json
  #
  RCSB_SEQUENCE_CLUSTER_DATA_PATH: cluster_data/mmseqs_clusters_current
  SIFTS_SUMMARY_DATA_PATH: sifts-summary
  # -------------------------------------------------------------------------------------------
  #   -- Below are common across current deployments -
  #
  # Supporting and integrated resource data cache directory names
  #
  #DRUGBANK_CACHE_DIR: DrugBank
  _DRUGBANK_AUTH_USERNAME: 0qrpNd4OhGuVsJqEpcsAVEovZ0hl6QkgxmbTy3bPssd06Z9tuM6bJqgsWwmFCd0JnjIMIEWyKPMmF1pI5g==
  _DRUGBANK_AUTH_PASSWORD: lA/K132i8DOtLdMHfm3gpNqprZ6ABKjsCRxfcXIMnxpKQzBv/B6dmC7x1vRO86JhqdT0b84=
  DRUGBANK_MOCK_URL_TARGET: DrugBank/full_database.zip
  #
  #ATC_CACHE_DIR: atc
  #CHEM_COMP_CACHE_DIR: chem_comp
  NCBI_TAXONOMY_CACHE_DIR: NCBI
  ENZYME_CLASSIFICATION_CACHE_DIR: ec
  STRUCT_DOMAIN_CLASSIFICATION_CACHE_DIR: domains_struct
  SIFTS_SUMMARY_CACHE_DIR: sifts_summary
  DICTIONARY_CACHE_DIR: dictionaries
  DATA_TYPE_INFO_CACHE_DIR: data_type_and_coverage
  REPO_UTIL_CACHE_DIR: repo_util
  EXDB_CACHE_DIR: exdb
  CITATION_REFERENCE_CACHE_DIR: cit_ref
  #
  #
  PROVENANCE_INFO_LOCATOR: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/provenance/rcsb_extend_provenance_info.json
  PROVENANCE_INFO_CACHE_DIR: provenance
  #
  SCHEMA_DEFINITION_LOCATOR_PATH: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/schema_definitions
  SCHEMA_DEFINITION_CACHE_DIR: schema_definitions
  JSON_SCHEMA_DEFINITION_LOCATOR_PATH: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/json_schema_definitions
  JSON_SCHEMA_DEFINITION_CACHE_DIR: json_schema_definitions
  #
  # Helper class binding and mappings
  #
  DICT_METHOD_HELPER_MODULE_PATH_MAP:
    rcsb.utils.dictionary.DictMethodEntryHelper: rcsb.utils.dictionary.DictMethodEntryHelper
    rcsb.utils.dictionary.DictMethodChemRefHelper: rcsb.utils.dictionary.DictMethodChemRefHelper
    rcsb.utils.dictionary.DictMethodEntityHelper: rcsb.utils.dictionary.DictMethodEntityHelper
    rcsb.utils.dictionary.DictMethodAssemblyHelper: rcsb.utils.dictionary.DictMethodAssemblyHelper
    rcsb.utils.dictionary.DictMethodEntityInstanceHelper: rcsb.utils.dictionary.DictMethodEntityInstanceHelper
  # ------ ------ ------ ------ ------ ------ ------ -------
  # ADDED rcsb.db V0.966 Source dictionary locators -
  #
  PDBX_DICT_LOCATOR: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/dictionaries/mmcif_pdbx_v5_next.dic
  RCSB_DICT_LOCATOR: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/dictionaries/rcsb_mmcif_ext_v1.dic
  IHMDEV_DICT_LOCATOR: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/dictionaries/ihm-extension.dic
  FLR_DICT_LOCATOR: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/dictionaries/flr-extension.dic
  VRPT_DICT_LOCATOR: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/dictionaries/vrpt_mmcif_ext.dic
  VRPT_DICT_MAPPING_LOCATOR: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/dictionaries/vrpt_dictmap.json
  # ------ ------ ------ ------ ------ ------ ------ ------ ------
  # Added in rcsb.db V0.966 - Data type details and type mapping
  APP_DATA_TYPE_INFO_LOCATOR: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/data_type_and_coverage/app_data_type_mapping.cif
  INSTANCE_DATA_TYPE_INFO_LOCATOR_PATH: https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/data_type_and_coverage
  #
  CONTENT_DEF_HELPER_MODULE: rcsb.db.helpers.ContentDefinitionHelper
  DOCUMENT_DEF_HELPER_MODULE: rcsb.db.helpers.DocumentDefinitionHelper
  CONFIG_APPEND_LOCATOR_PATHS:
    - https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/config/exdb-config-schema.yml
# ------ ------ ------ ------ ------ ------ ------ ------ ------
#  Added V1.001 for stash storage server
#  -- This is a placeholder configuration to support remote testing --
#  local|server
  STASH_MODE: local
  STASH_LOCAL_BASE_PATH: stash-storage
  #
  STASH_SERVER_URL: https://raw.githubusercontent.com
  STASH_SERVER_FALLBACK_URL: https://raw.githubusercontent.com
  _STASH_SERVER_BASE_PATH: bIo7kGc2w6Oel0QNr7Pc/4bFfDQayhPxddnGHynP6yudxc44QYuAFoTFdOqY2ZzsM2DEk56r26MfG66bEQ42lp38guy837xwzN1Vgu+r9zvAm11HXEA=
  REFERENCE_SEQUENCE_ALIGNMETS: PDB
##
```
