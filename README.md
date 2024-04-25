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

### Configuration File

RCSB/PDB repository path details are stored as configuration options.
An example configuration file included in this package is viewable under `rcsb/db/config`: [exdb-config-example.yml](https://github.com/rcsb/py-rcsb_db/blob/master/rcsb/db/config/exdb-config-example.yml). This example references dictionary resources and mock repository data
provided in the package in `rcsb/mock-data/*`. The `site_info_configuration` section
in this file provides database server connection details and common path details.
This is followed by sections specifying the dictionaries, helper functions, and
configuration used to define the schema for the each supported content type
(e.g., pdbx_core, chem_comp_core, bird_chem_comp_core,.. ).

### Command Line Interfaces

#### Schema File Generation
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

##### Example Usage

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

#### ExDB Loading

A convenience CLI `exdb_repo_load_cli` is provided to support loading PDB repositories
containing entry and chemical reference data content types in the form of document collections
compatible with MongoDB.

```bash
exdb_repo_load_cli --help

usage: exdb_repo_load_cli [-h] [--op OP_TYPE] [--load_type LOAD_TYPE]
                          [--database DATABASE_NAME]
                          [--config_path CONFIG_PATH]
                          [--config_name CONFIG_NAME] [--db_type DB_TYPE]
                          [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                          [--document_style DOCUMENT_STYLE]
                          [--disable_read_back_check] [--schema_level SCHEMA_LEVEL]
                          [--load_id_list_path LOAD_ID_LIST_PATH]
                          [--load_file_list_path LOAD_FILE_LIST_PATH]
                          [--fail_file_list_path FAIL_FILE_LIST_PATH]
                          [--save_file_list_path SAVE_FILE_LIST_PATH]
                          [--file_limit FILE_LIMIT]
                          [--prune_document_size PRUNE_DOCUMENT_SIZE]
                          [--debug] [--mock] [--cache_path CACHE_PATH]
                          [--rebuild_cache] [--rebuild_schema]
                          [--vrpt_repo_path VRPT_REPO_PATH]

optional arguments:
  -h, --help            show this help message and exit
  --op {pdbx_loader,build_resource_cache,pdbx_db_wiper,pdbx_id_list_splitter,pdbx_loader_check,etl_entity_sequence_clusters,etl_repository_holdings}
                        Loading operation to perform
  --load_type {replace,full}
                        Type of load ('replace' for incremental and
                        multi-worker load, 'full' for complete and
                        fresh single-worker load)
  --database {pdbx_core,pdbx_comp_model_core,bird_chem_comp_core,chem_comp,chem_comp_core,bird_chem_comp,bird,bird_family,ihm_dev}
                        Database to load (most common choices are:
                        'pdbx_core', 'pdbx_comp_model_core', or
                        'bird_chem_comp_core')
  --config_path CONFIG_PATH
                        Path to configuration options file
  --config_name CONFIG_NAME
                        Configuration section name
  --document_style DOCUMENT_STYLE
                        Document organization (rowwise_by_name_with_c
                        ardinality|rowwise_by_name|columnwise_by_name
                        |rowwise_by_id|rowwise_no_name)
  --cache_path CACHE_PATH
                        Cache path for resource files
  --num_proc NUM_PROC   Number of processes to execute (default=2)
  --chunk_size CHUNK_SIZE
                        Number of files loaded per process
  --max_step_length MAX_STEP_LENGTH
                        Maximum subList size (default=500)
  --schema_level SCHEMA_LEVEL
                        Schema validation level (full|min)
  --collection_list COLLECTION_LIST
                        Specific collections to load
  --load_id_list_path LOAD_ID_LIST_PATH
                        Input file containing the list of IDs to load
                        in the current iteration by a single worker
  --holdings_file_path HOLDINGS_FILE_PATH
                        File containing the complete list of all IDs
                        (or holdings files) that will be loaded
  --load_file_list_path LOAD_FILE_LIST_PATH
                        Input file containing load file path list
                        (override automatic repository scan)
  --fail_file_list_path FAIL_FILE_LIST_PATH
                        Output file containing file paths that fail
                        to load
  --save_file_list_path SAVE_FILE_LIST_PATH
                        Save repo file paths from automatic file
                        system scan in this path
  --load_file_list_dir LOAD_FILE_LIST_DIR
                        Directory path for storing load file lists
  --num_sublists NUM_SUBLISTS
                        Number of sublists to create/load for the
                        associated database
  --force_reload        Force re-load of provided ID list (i.e.,
                        don't just load delta; useful for manual/test
                        runs).
  --provider_type_exclude PROVIDER_TYPE_EXCLUDE
                        Resource provider types to exclude
  --db_type DB_TYPE     Database server type (default=mongo)
  --file_limit FILE_LIMIT
                        Load file limit for testing
  --prune_document_size PRUNE_DOCUMENT_SIZE
                        Prune large documents to this size limit (MB)
  --regex_purge         Perform additional regex-based purge of all
                        pre-existing documents for loadType != 'full'
  --data_selectors  [ ...]
                        Data selectors, space-separated.
  --disable_read_back_check
                        Disable read back check on all documents
  --disable_merge_validation_reports
                        Disable merging of validation report data
                        with the primary content type
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for testing
  --rebuild_cache       Rebuild cached resource files
  --rebuild_schema      Rebuild schema on-the-fly if not cached
  --vrpt_repo_path VRPT_REPO_PATH
                        Path to validation report repository
________________________________________________________________________________
```

##### Example Usage
The following commands demonstrate how each type of operation (`--op`) is used for loading of PDB repository data to ExDB. For all commands, the following environmental variables must first be set:

```bash
export CONFIG_SUPPORT_TOKEN_ENV=personal_token_used_for_decrypting_config_variables
export OE_LICENSE=/path/to/oe_license.txt
export NLTK_DATA=/path/to/nltk_data
```

`--op build_resource_cache` - Build the external resource cache that will be used for and integrated with the loading of PDB structure data.
```bash
exdb_repo_load_cli --op "build_resource_cache" \
--config_path "/opt/etl-scratch/config/exdb-loader-config.yml" \
--config_name "site_info_remote_configuration" \
--num_proc 6  \
--cache_path "/opt/etl-scratch/data/CACHE" \

```

`--op pdbx_db_wiper` - Wipe the pre-existing database (and all of its collections).
```bash
exdb_repo_load_cli --op "pdbx_db_wiper" \
--database "pdbx_core" \
--config_path "/opt/etl-scratch/config/exdb-loader-config.yml" \
--config_name "site_info_remote_configuration" \
--cache_path "/opt/etl-scratch/data/CACHE" \

```

`--op pdbx_id_list_splitter` - Split the full list of input IDs into smaller, equally-sized sublists.
```bash
exdb_repo_load_cli --op "pdbx_id_list_splitter" \
--database "pdbx_core" \
--config_path "/opt/etl-scratch/config/exdb-loader-config.yml" \
--config_name "site_info_remote_configuration" \
--cache_path "/opt/etl-scratch/data/CACHE" \
--load_file_list_dir "/opt/etl-scratch/work-dir/load_file_lists" \
--holdings_file_path "https://files.wwpdb.org/pub/pdb/holdings/released_structures_last_modified_dates.json.gz" \
--num_sublists 10 \

```

`--op pdbx_loader` - Load a list of entry IDs to ExDB.
```bash
exdb_repo_load_cli --op "pdbx_loader" \
--database "pdbx_core" \
--load_type replace  \
--config_path /opt/etl-scratch/config/exdb-loader-config.yml \
--config_name site_info_remote_configuration \
--num_proc 8  \
--chunk_size 5  \
--max_step_length 500 \
--load_id_list_path "/opt/etl-scratch/work-dir/load_file_lists/pdbx_core_ids-1.txt" \
--cache_path "/opt/etl-scratch/data/CACHE" \

```

`--op pdbx_loader_check` - Check the resulting ExDB database to confirm that all expected documents were loaded.
```bash
exdb_repo_load_cli --op "pdbx_loader_check" \
--database "pdbx_core" \
--config_path "/opt/etl-scratch/config/exdb-loader-config.yml" \
--config_name "site_info_remote_configuration" \
--cache_path "/opt/etl-scratch/data/CACHE" \
--load_file_list_dir "/opt/etl-scratch/work-dir/load_file_lists" \
--holdings_file_path "https://files.wwpdb.org/pub/pdb/holdings/released_structures_last_modified_dates.json.gz" \
--num_sublists 10 \

```

#### Repository Scanning

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

#### ETL Processing
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

### Additional Examples

(*Note: The examples below are outdated and may not function as described. They are only kept here for historical reference.*)

If you are working in the source repository, then you can run the CLI commands in the following manner.
The following examples load data in the mock repositories in source distribution assuming you have a local
default installation of MongoDb (no user/pw assigned).

To run the command-line interface `exdb_repo_load_cli` outside of the source distribution, you will need to
create a configuration file with the appropriate path details and authentication credentials.

For instance, to perform a fresh/full load of all of the chemical component definitions in the mock repository:

```bash

cd rcsb/db/cli
python RepoLoadExec.py --full  --load_chem_comp_ref  \
                      --config_path ../config/exdb-config-example.yml \
                      --config_name site_info_configuration \
                      --fail_file_list_path failed-cc-path-list.txt \
                      --read_back_check
```

The following illustrates, a full load of the mock structure data repository followed by a reload with replacement of
this same data.

```bash

cd rcsb/db/cli
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
