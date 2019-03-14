## RCSB Pinelands
#### A collection of Python Database Utility Classes

=======


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

#### Installing in Ubuntu Linux (tested in 18.04)
You will need a few packages, before `pip install .` can work:
```
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
are required to support the Pinelands tools.  HomeBrew also provides a variety of options for
managing a [Python virtual environments](https://gist.github.com/Geoyi/f55ed54d24cc9ff1c14bd95fac21c042).

### Command Line Interfaces

A convenience CLI `schema_update_cli` is provided for generating operational schema from
PDBx/mmCIF dictionary metadata.  Schema are encoded for the PineLands API (rcsb), and
for the document schema encoded in JSON and BSON formats.  The latter schema can be used to
validate the loadable document objects produced for the collections served by MongoDB.

```bash
schema_update_cli  --help
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
                         [--schema_dirpath SCHEMA_DIRPATH]
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
                        SCHEMA_NAMES_ALL)
  --update_config_deployed
                        Update using configuration settings (e.g.
                        SCHEMA_NAMES_DEPLOYED)
  --update_config_test  Update using configuration settings (e.g.
                        SCHEMA_NAMES_TEST)
  --config_path CONFIG_PATH
                        Path to configuration options file
  --config_name CONFIG_NAME
                        Configuration section name
  --schema_dirpath SCHEMA_DIRPATH
                        Output schema directory path (overrides configuration
                        settings)
  --schema_types SCHEMA_TYPES
                        Schema encoding (rcsb|json|bson) (comma separated)
  --schema_levels SCHEMA_LEVELS
                        Schema validation level (full|min) (comma separated)
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for dependencies and
                        testing
__________________________________________________

```

For example, the following command will generate the JSON and BSON schema for the collections in the
pdbx_core schema.

```bash
schema_update_cli  --mock --schema_types json,bson \
                   --schema_level full  \
                   --update_pdbx_core   \
                   --schema_dirpath . \
                   --config_path ./rcsb/mock-data/config/dbload-setup-example.yml \
                   --config_name site_info
```
#
A convenience CLI `exdb_repo_load_cli` is provided to support loading PDB repositories
containing entry and chemical reference data content types in the form of document collections
compatible with MongoDB.


```bash
exdb_repo_load_cli --help

or
usage: RepoLoadExec.py [-h] [--full] [--replace] [--load_chem_comp_ref]
                       [--load_chem_comp_core_ref] [--load_bird_chem_comp_ref]
                       [--load_bird_chem_comp_core_ref] [--load_bird_ref]
                       [--load_bird_family_ref] [--load_entry_data]
                       [--load_pdbx_core] [--load_pdbx_core_merge]
                       [--load_pdbx_core_entry] [--load_pdbx_core_entity]
                       [--load_pdbx_core_entity_monomer]
                       [--load_pdbx_core_assembly] [--load_ihm_dev]
                       [--config_path CONFIG_PATH] [--config_name CONFIG_NAME]
                       [--db_type DB_TYPE] [--document_style DOCUMENT_STYLE]
                       [--read_back_check] [--schema_level SCHEMA_LEVEL]
                       [--load_file_list_path LOAD_FILE_LIST_PATH]
                       [--fail_file_list_path FAIL_FILE_LIST_PATH]
                       [--save_file_list_path SAVE_FILE_LIST_PATH]
                       [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                       [--file_limit FILE_LIMIT]
                       [--prune_document_size PRUNE_DOCUMENT_SIZE] [--debug]
                       [--mock] [--working_path WORKING_PATH]
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
  --working_path WORKING_PATH
                        Working path for temporary files
  --vrpt_repo_path VRPT_REPO_PATH
                        Path to validation report repository


```

Part of the schema definition process supported by this module involves refining
the dictionary metadata with more specific data typing and coverage details.
A scanning tools is provided to collect and organize these details for the
other ETL tools in this package.  The following convenience CLI, `repo_scan_cli`,
is provided to scan supported PDB repository content and update data type and coverage details.


```bash
repo_scan_cli --help

usage: repo_scan_cli [-h] [--dict_file_path DICT_FILE_PATH]
                     [--scanType SCANTYPE]
                     [--scan_chem_comp_ref | --scan_chem_comp_core_ref | --scan_bird_chem_comp_ref |
                      --scan_bird_chem_comp_core_ref | --scan_bird_ref | --scan_bird_family_ref | --scan_entry_data | --scan_ihm_dev]
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

```

The following CLI provides a preliminary access to ETL functions for processing
derived content types such as sequence comparative data.

```bash
etl_exec_cli --help
usage: etl_exec_cli [-h] [--full] [--etl_entity_sequence_clusters]
                    [--etl_repository_holdings] [--etl_chemref]
                    [--data_set_id DATA_SET_ID]
                    [--sequence_cluster_data_path SEQUENCE_CLUSTER_DATA_PATH]
                    [--sandbox_data_path SANDBOX_DATA_PATH]
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
  --etl_repository_holdings
                        ETL repository holdings
  --etl_chemref         ETL integrated chemical reference data
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
                      --config_path ../../mock-data/config/dbload-setup-example.yml \
                      --config_name site_info \
                      --fail_file_list_path failed-cc-path-list.txt \
                      --read_back_check
```


The following illustrates, a full load of the mock structure data repository followed by a reload with replacement of
this same data.

```bash

cd rcsb/db/scripts
python RepoLoadExec.py  --mock --full  --load_entry_data \
                     --config_path ../../mock-data/config/dbload-setup-example.yml \
                     --config_name site_info \
                     --save_file_list_path  LATEST_PDBX_LOAD_LIST.txt \
                     --fail_file_list_path failed-entry-path-list.txt

python RepoLoadExec.py --mock --replace  --load_entry_data \
                      --config_path ../../mock-data/config/dbload-setup-example.yml \
                      --config_name site_info \
                      --load_file_list_path  LATEST_PDBX_LOAD_LIST.txt \
                      --fail_file_list_path failed-entry-path-list.txt
```



### Configuration Example

RCSB/PDB repository path details are stored as configuration options.
An example configuration file included in this package is shown below.
This example is references dictionary resources and mock repository data
provided in the package in `rcsb/mock-data/*`.
The `site_server_info` section provides database server connection details.
Common path details are stored in configuration section `site_info`. This is followed by
sections specifying the dictionaries and helper functions used
to define the schema for the each supported content type (e.g., pdbx_core, chem_comp_core,
bird_chem_comp_core,.. ).


```bash
# File: dbload-setup-example.yml
#
---
DEFAULT: {}
#
# Site specific path and server configuration options -
#
site_info:
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
    RCSB_SEQUENCE_CLUSTER_DATA_PATH: cluster_data/mmseqs-20180608
    PDBX_DICT_LOCATOR: dictionaries/mmcif_pdbx_v5_next.dic
    RCSB_DICT_LOCATOR: dictionaries/rcsb_mmcif_ext_v1.dic
    IHMDEV_DICT_LOCATOR: dictionaries/ihm-extension.dic
    FLR_DICT_LOCATOR: dictionaries/flr-extension.dic
    #
    VRPT_DICT_LOCATOR: dictionaries/vrpt_mmcif_ext.dic
    VRPT_DICT_MAPPING_LOCATOR: dictionaries/vrpt_dictmap.json
    #
    PROVENANCE_INFO_LOCATOR: provenance/rcsb_extend_provenance_info.json
    APP_DATA_TYPE_INFO_LOCATOR: data_type_info/app_data_type_mapping.cif
    DICT_HELPER_MODULE: rcsb.db.helpers.DictInfoHelper
    SCHEMADEF_HELPER_MODULE: rcsb.db.helpers.SchemaDefHelper
    DOCUMENT_HELPER_MODULE: rcsb.db.helpers.SchemaDocumentHelper
    DICT_METHOD_HELPER_MODULE: rcsb.db.helpers.DictMethodRunnerHelper
    DRUGBANK_MAPPING_LOCATOR: DrugBank/drugbank_pdb_mapping.json
    DRUGBANK_DATA_LOCATOR: DrugBank/full_database.xml.gz
    CCDC_MAPPING_LOCATOR: chem_comp_models/ccdc_pdb_mapping.json
    #
    NCBI_TAXONOMY_PATH: NCBI
    ENZYME_CLASSIFICATION_DATA_PATH: ec
    #
    SCHEMA_DEF_LOCATOR_PATH: schema
    JSON_SCHEMA_LOCATOR_PATH: json-schema
    INSTANCE_DATA_TYPE_INFO_LOCATOR_PATH: data_type_info
    #
    MONGO_DB_HOST: localhost
    MONGO_DB_PORT: '27017'
    MONGO_DB_USER: ''
    MONGO_DB_PASSWORD: ''
    MYSQL_DB_HOST_NAME: localhost
    MYSQL_DB_PORT_NUMBER: '3306'
    MYSQL_DB_USER_NAME: root
    MYSQL_DB_PASSWORD: ChangeMeSoon
    MYSQL_DB_DATABASE_NAME: mysql
    CRATE_DB_HOST: localhost
    CRATE_DB_PORT: '4200'
    COCKROACH_DB_HOST: localhost
    COCKROACH_DB_PORT: '26257'
    COCKROACH_DB_NAME: system
    COCKROACH_DB_USER_NAME: root
#
# Inventory of current databases and collections -
#
schema_catalog_info:
  # All defined schema -
  SCHEMA_NAMES_ALL: pdbx,pdbx_core,chem_comp,chem_comp_core,bird,bird_family,bird_chem_comp,bird_chem_comp_core,repository_holdings,entity_sequence_clusters,data_exchange,drugbank_core,ihm_dev
  DATATYPING_ALL: ANY,SQL
  SCHEMA_TYPES_ALL: rcsb,json,bson
  SCHEMA_LEVELS_ALL: min,full
  #
  #  Schema in active use -
  #
  SCHEMA_NAMES_DEPLOYED: pdbx,pdbx_core,chem_comp,chem_comp_core,bird,bird_family,bird_chem_comp,bird_chem_comp_core,repository_holdings,entity_sequence_clusters,data_exchange,drugbank_core,ihm_dev
  DATATYPING_DEPLOYED: ANY,SQL
  SCHEMA_TYPES_DEPLOYED: rcsb,json,bson
  SCHEMA_LEVELS_DEPLOYED: min,full
  #
  # Schema subset used for CI testing -
  #
  SCHEMA_NAMES_TEST: pdbx_core
  DATATYPING_TEST: ANY,SQL
  SCHEMA_TYPES_TEST: rcsb,json,bson
  SCHEMA_LEVELS_TEST: min,full
  #
  #SCHEMA_NAMES_TEST: pdbx_core,chem_comp_core,bird_chem_comp_core,repository_holdings,entity_sequence_clusters,data_exchange,drugbank_core
  #DATATYPING_TEST: ANY,SQL
  #SCHEMA_FORMATS_TEST: rcsb,json,bson
  #SCHEMA_LEVELS_TEST: min,full#
#
# Some schema details for integrated collections -
#
entity_sequence_clusters:
    DATABASE_NAME: sequence_clusters
    DATABASE_VERSION_STRING: v5
    COLLECTION_ENTITY_MEMBERS: entity_members
    COLLECTION_ENTITY_MEMBERS_INDEX: data_set_id,entry_id,entity_id
    COLLECTION_CLUSTER_MEMBERS: cluster_members
    COLLECTION_CLUSTER_MEMBERS_INDEX: data_set_id,identity,cluster_id
    COLLECTION_VERSION_STRING: v1_0_2
    ENTITY_SCHEMA_NAME: rcsb_entity_sequence_cluster_entity_list
    CLUSTER_SCHEMA_NAME: rcsb_entity_sequence_cluster_identifer_list
    SEQUENCE_IDENTITY_LEVELS: 100,95,90,70,50,30
    COLLECTION_CLUSTER_PROVENANCE: cluster_provenance
    PROVENANCE_KEY_NAME: rcsb_entity_sequence_cluster_prov
    #SCHEMA_NAME: entity_sequence_clusters
    #SCHEMA_DEF_FILENAME_SQL: schema_def-entity_sequence_clusters-SQL.json
    #SCHEMA_DEF_FILENAME_ANY: schema_def-entity_sequence_clusters-ANY.json
    INSTANCE_DATA_TYPE_INFO_FILENAME: scan-entity_sequence_clusters-type-map.json
repository_holdings:
    DATABASE_NAME: repository_holdings
    DATABASE_VERSION_STRING: v5
    COLLECTION_HOLDINGS_UPDATE: repository_holdings_update
    COLLECTION_HOLDINGS_CURRENT: repository_holdings_current
    COLLECTION_HOLDINGS_UNRELEASED: repository_holdings_unreleased
    COLLECTION_HOLDINGS_PRERELEASE: repository_holdings_prerelease
    COLLECTION_HOLDINGS_REMOVED: repository_holdings_removed
    COLLECTION_HOLDINGS_REMOVED_AUTHORS: repository_holdings_removed_audit_authors
    COLLECTION_HOLDINGS_SUPERSEDED: repository_holdings_superseded
    COLLECTION_HOLDINGS_TRANSFERRED: repository_holdings_transferred
    COLLECTION_HOLDINGS_INSILICO_MODELS: repository_holdings_insilico_models
    COLLECTION_VERSION_STRING: v1_0_2
    #SCHEMA_NAME: entity_sequence_clusters
    #SCHEMA_DEF_FILENAME_SQL: schema_def-repository_holdings-SQL.json
    #SCHEMA_DEF_FILENAME_ANY: schema_def-repository_holdings-ANY.json
    #INSTANCE_DATA_TYPE_INFO_FILENAME: scan-repository_holdings-type-map.json
data_exchange:
    DATABASE_NAME: data_exchange
    DATABASE_VERSION_STRING: v5
    COLLECTION_UPDATE_STATUS: rcsb_data_exchange_status
    COLLECTION_VERSION_STRING: v1_0_2
    # SCHEMA_NAME: data_exchange
    #SCHEMA_DEF_FILENAME_SQL: schema_def-data_exchange-SQL.json
    #SCHEMA_DEF_FILENAME_ANY: schema_def-data_exchange-ANY.json
    #INSTANCE_DATA_TYPE_INFO_FILENAME: scan-data_exchange-type-map.json
#
# Common configuration options for helper classes used for dictionary content generation
# schema construction --
#
dictionary_helper:
    item_transformers:
        STRIP_WS:
            - CATEGORY_NAME: entity_poly
              ATTRIBUTE_NAME: pdbx_c_terminal_seq_one_letter_code
            - CATEGORY_NAME: entity_poly
              ATTRIBUTE_NAME: pdbx_n_terminal_seq_one_letter_code
            - CATEGORY_NAME: entity_poly
              ATTRIBUTE_NAME: pdbx_seq_one_letter_code
            - CATEGORY_NAME: entity_poly
              ATTRIBUTE_NAME: pdbx_seq_one_letter_code_can
            - CATEGORY_NAME: entity_poly
              ATTRIBUTE_NAME: pdbx_seq_one_letter_code_sample
            - CATEGORY_NAME: struct_ref
              ATTRIBUTE_NAME: pdbx_seq_one_letter_code
        TRANSLATE_XMLCHARREFS:
            - CATEGORY_NAME: citation
            - CATEGORY_NAME: citation_author
            - CATEGORY_NAME: struct
            - CATEGORY_NAME: audit_author
            - CATEGORY_NAME: chem_comp
    cardinality_parent_items:
        ihm_dev:
            CATEGORY_NAME: entry
            ATTRIBUTE_NAME: id
        bird:
            CATEGORY_NAME: pdbx_reference_molecule
            ATTRIBUTE_NAME: prd_id
        bird_family:
            CATEGORY_NAME: pdbx_reference_molecule_family
            ATTRIBUTE_NAME: family_prd_id
        chem_comp:
            CATEGORY_NAME: chem_comp
            ATTRIBUTE_NAME: id
        chem_comp_core:
            CATEGORY_NAME: chem_comp
            ATTRIBUTE_NAME: id
        bird_chem_comp:
            CATEGORY_NAME: chem_comp
            ATTRIBUTE_NAME: id
        bird_chem_comp_core:
            CATEGORY_NAME: chem_comp
            ATTRIBUTE_NAME: id
        pdbx:
            CATEGORY_NAME: entry
            ATTRIBUTE_NAME: id
        pdbx_core:
            CATEGORY_NAME: entry
            ATTRIBUTE_NAME: id
    cardinality_category_extras:
        - rcsb_load_status
        - pdbx_reference_molecule
        - pdbx_reference_molecule_family
        - drugbank_info
    selection_filters:
        ?       - PUBLIC_RELEASE
                - pdbx
        :       - CATEGORY_NAME: pdbx_database_status
                  ATTRIBUTE_NAME: status_code
                  VALUES:
                      - REL
        ?       - PUBLIC_RELEASE
                - pdbx_core
        :       - CATEGORY_NAME: pdbx_database_status
                  ATTRIBUTE_NAME: status_code
                  VALUES:
                      - REL
        ?       - PUBLIC_RELEASE
                - chem_comp
        :       - CATEGORY_NAME: chem_comp
                  ATTRIBUTE_NAME: pdbx_release_status
                  VALUES:
                      - REL
                      - REF_ONLY
                      #- OBS
        ?       - PUBLIC_RELEASE
                - chem_comp_core
        :       - CATEGORY_NAME: chem_comp
                  ATTRIBUTE_NAME: pdbx_release_status
                  VALUES:
                      - REL
                      - REF_ONLY
                      #- OBS
        ?       - PUBLIC_RELEASE
                - bird_chem_comp
        :       - CATEGORY_NAME: chem_comp
                  ATTRIBUTE_NAME: pdbx_release_status
                  VALUES:
                      - REL
                      - REF_ONLY
                      #- OBS
        ?       - PUBLIC_RELEASE
                - bird_chem_comp_core
        :       - CATEGORY_NAME: pdbx_reference_molecule
                  ATTRIBUTE_NAME: release_status
                  VALUES:
                      - REL
                      #- OBS
        ?       - PUBLIC_RELEASE
                - bird
        :       - CATEGORY_NAME: pdbx_reference_molecule
                  ATTRIBUTE_NAME: release_status
                  VALUES:
                      - REL
                      #- OBS
        ?       - PUBLIC_RELEASE
                - bird_family
        :       - CATEGORY_NAME: pdbx_reference_molecule_family
                  ATTRIBUTE_NAME: release_status
                  VALUES:
                      - REL
                      #- OBS
    type_code_classes:
        iterable:
            - TYPE_CODE: ucode-alphanum-csv
              DELIMITER: ","
            - TYPE_CODE: id_list
              DELIMITER: ","
            - TYPE_CODE: alphanum-scsv
              DELIMITER: ";"
            - TYPE_CODE: alphanum-csv
              DELIMITER: ","
            - TYPE_CODE: alphanum-vbsv
              DELIMITER: "|"
            - TYPE_CODE: int-csv
              DELIMITER: ","
            - TYPE_CODE: int-scsv
              DELIMITER: ";"
    query_string_selectors:
        iterable:
            - comma separate
    iterable_delimiters:
        - CATEGORY_NAME: chem_comp
          ATTRIBUTE_NAME: pdbx_synonyms
          DELIMITER: ";"
        - CATEGORY_NAME: citation
          ATTRIBUTE_NAME: rcsb_authors
          DELIMITER: "|"
        - CATEGORY_NAME: entity
          ATTRIBUTE_NAME: pdbx_ec
          DELIMITER: ","
    content_classes:
        ?       - GENERATED_CONTENT
                - pdbx
        :       - CATEGORY_NAME: rcsb_load_status
                  ATTRIBUTE_NAME_LIST:
                      - datablock_name
                      - load_date
                      - locator
                - CATEGORY_NAME: pdbx_struct_assembly_gen
                  ATTRIBUTE_NAME_LIST:
                      - ordinal
        ?       - GENERATED_CONTENT
                - pdbx_core
        :       - CATEGORY_NAME: rcsb_load_status
                  ATTRIBUTE_NAME_LIST:
                      - datablock_name
                      - load_date
                      - locator
                - CATEGORY_NAME: citation
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_authors
                - CATEGORY_NAME: pdbx_molecule
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_entity_id
                      - rcsb_comp_id
                - CATEGORY_NAME: pdbx_entity_nonpoly
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_prd_id
                - CATEGORY_NAME: struct_ref_seq
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_entity_id
                - CATEGORY_NAME: struct_ref_seq_dif
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_entity_id
                - CATEGORY_NAME: rcsb_entity_poly_info
                  ATTRIBUTE_NAME_LIST:
                      - ordinal_id
                      - entry_id
                      - entity_id
                      - comp_id
                      - chem_comp_count
                      - entity_sequence_length
                      - is_modified
                - CATEGORY_NAME: pdbx_struct_assembly_gen
                  ATTRIBUTE_NAME_LIST:
                      - ordinal
                - CATEGORY_NAME: pdbx_struct_assembly
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_details
                      - rcsb_candidate_assembly
                - CATEGORY_NAME: rcsb_entry_container_identifiers
                  ATTRIBUTE_NAME_LIST:
                      - entry_id
                      - entity_ids
                      - polymer_entity_ids
                      - non-polymer_entity_ids
                      - assembly_ids
                      - software_programs_combined
                - CATEGORY_NAME: rcsb_entity_container_identifiers
                  ATTRIBUTE_NAME_LIST:
                      - entry_id
                      - entity_id
                      - asym_ids
                      - auth_asym_ids
                      - chem_comp_ligands
                      - chem_comp_monomers
                - CATEGORY_NAME: rcsb_assembly_container_identifiers
                  ATTRIBUTE_NAME_LIST:
                      - entry_id
                      - assembly_id
                - CATEGORY_NAME: rcsb_entity_source_organism
                - CATEGORY_NAME: rcsb_entity_host_organism
                - CATEGORY_NAME: entity
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_multiple_source_flag
                      - rcsb_source_part_count
                      - rcsb_macromolecular_names_combined
                      - rcsb_ec_lineage_name
                      - rcsb_ec_lineage_id
                      - rcsb_ec_lineage_depth
                - CATEGORY_NAME: rcsb_entry_info
                  ATTRIBUTE_NAME_LIST:
                      - entry_id
                      - polymer_composition
                      - experimental_method
                      - experimental_method_count
                      - polymer_entity_count
                      - entity_count
                      - nonpolymer_entity_count
                      - branched_entity_count
                      - solvent_entity_count
                - CATEGORY_NAME: entity_poly
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_entity_polymer_type
                      - rcsb_prd_id
                - CATEGORY_NAME: rcsb_accession_info
                  ATTRIBUTE_NAME_LIST:
                      - entry_id
                      - status_code
                      - deposit_date
                      - initial_release_date
                      - major_revision
                      - minor_revision
                      - revision_date
                - CATEGORY_NAME: rcsb_entity_instance_container_identifiers
                - CATEGORY_NAME: rcsb_entity_monomer_container_identifiers
                - CATEGORY_NAME: rcsb_schema_container_identifiers
                - CATEGORY_NAME: pdbx_vrpt_summary
                - CATEGORY_NAME: pdbx_vrpt_summary
                - CATEGORY_NAME: pdbx_vrpt_model_list
                - CATEGORY_NAME: pdbx_vrpt_instance_results
                - CATEGORY_NAME: pdbx_vrpt_cyrange_domain
                - CATEGORY_NAME: pdbx_vrpt_entity_instance_results
                - CATEGORY_NAME: pdbx_vrpt_chemical_shift_list
                - CATEGORY_NAME: pdbx_vrpt_unmapped_chemical_shift
                - CATEGORY_NAME: pdbx_vrpt_unparsed_chemical_shift
                - CATEGORY_NAME: pdbx_vrpt_missing_nmrstar_tags
                - CATEGORY_NAME: pdbx_vrpt_random_coil_index
                - CATEGORY_NAME: pdbx_vrpt_chemical_shift_outlier
                - CATEGORY_NAME: pdbx_vrpt_referencing_offset
                - CATEGORY_NAME: pdbx_vrpt_assign_compl_well_defined
                - CATEGORY_NAME: pdbx_vrpt_assign_compl_full_length
                - CATEGORY_NAME: pdbx_vrpt_software
                - CATEGORY_NAME: pdbx_vrpt_bond_outliers
                - CATEGORY_NAME: pdbx_vrpt_angle_outliers
                - CATEGORY_NAME: pdbx_vrpt_stereo_outliers
                - CATEGORY_NAME: pdbx_vrpt_plane_outliers
                - CATEGORY_NAME: pdbx_vrpt_clashes
                - CATEGORY_NAME: pdbx_vrpt_symmetry_clashes
                - CATEGORY_NAME: pdbx_vrpt_mogul_bond_outliers
                - CATEGORY_NAME: pdbx_vrpt_mogul_angle_outliers
                - CATEGORY_NAME: pdbx_vrpt_mogul_torsion_outliers
                - CATEGORY_NAME: pdbx_vrpt_mogul_ring_outliers
                - CATEGORY_NAME: pdbx_validate_close_contact
                - CATEGORY_NAME: pdbx_validate_symm_contact
                - CATEGORY_NAME: pdbx_validate_rmsd_bond
                - CATEGORY_NAME: pdbx_validate_rmsd_angle
                - CATEGORY_NAME: pdbx_validate_torsion
                - CATEGORY_NAME: pdbx_validate_peptide_omega
                - CATEGORY_NAME: pdbx_validate_chiral
                - CATEGORY_NAME: pdbx_validate_planes
                - CATEGORY_NAME: pdbx_validate_planes_atom
                - CATEGORY_NAME: pdbx_validate_main_chain_plane
                - CATEGORY_NAME: pdbx_validate_polymer_linkage
                - CATEGORY_NAME: pdbx_distant_solvent_atoms
                #
        ?       - GENERATED_CONTENT
                - data_exchange
        :       - CATEGORY_NAME: rcsb_data_exchange_status
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - database_name
                      - object_name
                      - update_status_flag
                      - update_begin_timestamp
                      - update_end_timestamp
                - CATEGORY_NAME: rcsb_schema_container_identifiers
        ?       - EVOLVING_CONTENT
                - pdbx_core
        :       - CATEGORY_NAME: diffrn
                  ATTRIBUTE_NAME_LIST:
                      - pdbx_serial_crystal_experiment
                - CATEGORY_NAME: diffrn_detector
                  ATTRIBUTE_NAME_LIST:
                      - pdbx_frequency
                - CATEGORY_NAME: pdbx_serial_crystallography_measurement
                  ATTRIBUTE_NAME_LIST:
                      - diffrn_id
                      - pulse_energy
                      - pulse_duration
                      - xfel_pulse_repetition_rate
                      - pulse_photon_energy
                      - photons_per_pulse
                      - source_size
                      - source_distance
                      - focal_spot_size
                      - collimation
                      - collection_time_total
                - CATEGORY_NAME: pdbx_serial_crystallography_sample_delivery
                  ATTRIBUTE_NAME_LIST:
                      - diffrn_id
                      - description
                      - method
                - CATEGORY_NAME: pdbx_serial_crystallography_sample_delivery_injection
                  ATTRIBUTE_NAME_LIST:
                      - diffrn_id
                      - description
                      - injector_diameter
                      - injector_temperature
                      - injector_pressure
                      - flow_rate
                      - carrier_solvent
                      - crystal_concentration
                      - preparation
                      - power_by
                      - injector_nozzle
                      - jet_diameter
                      - filter_size
                - CATEGORY_NAME: pdbx_serial_crystallography_sample_delivery_fixed_target
                  ATTRIBUTE_NAME_LIST:
                      - diffrn_id
                      - description
                      - sample_holding
                      - support_base
                      - sample_unit_size
                      - crystals_per_unit
                      - sample_solvent
                      - sample_dehydration_prevention
                      - motion_control
                      - velocity_horizontal
                      - velocity_vertical
                      - details
                - CATEGORY_NAME: pdbx_serial_crystallography_data_reduction
                  ATTRIBUTE_NAME_LIST:
                      - diffrn_id
                      - frames_total
                      - xfel_pulse_events
                      - frame_hits
                      - crystal_hits
                      - droplet_hits
                      - frames_failed_index
                      - frames_indexed
                      - lattices_indexed
                      - xfel_run_numbers
        ?       - GENERATED_CONTENT
                - chem_comp
        :       - CATEGORY_NAME: rcsb_load_status
                  ATTRIBUTE_NAME_LIST:
                      - datablock_name
                      - load_date
                      - locator
                - CATEGORY_NAME: pdbx_chem_comp_audit
                  ATTRIBUTE_NAME_LIST:
                      - ordinal
        ?       - GENERATED_CONTENT
                - chem_comp_core
        :       - CATEGORY_NAME: rcsb_load_status
                  ATTRIBUTE_NAME_LIST:
                      - datablock_name
                      - load_date
                      - locator
                - CATEGORY_NAME: rcsb_chem_comp_synonyms
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - ordinal
                      - name
                      - provenance_code
                - CATEGORY_NAME: rcsb_chem_comp_info
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - atom_count
                      - atom_count_chiral
                      - bond_count
                      - bond_count_aromatic
                      - atom_count_heavy
                - CATEGORY_NAME: rcsb_chem_comp_descriptor
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - SMILES
                      - SMILES_stereo
                      - InChI
                      - InChIKey
                - CATEGORY_NAME: rcsb_chem_comp_related
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - ordinal
                      - resource_name
                      - resource_accession_code
                      - related_mapping_method
                - CATEGORY_NAME: rcsb_chem_comp_target
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - ordinal
                      - name
                      - interaction_type
                      - target_actions
                      - organism_common_name
                      - reference_database_name
                      - reference_database_accession_code
                      - provenance_code
                - CATEGORY_NAME: pdbx_chem_comp_audit
                  ATTRIBUTE_NAME_LIST:
                      - ordinal
                - CATEGORY_NAME: rcsb_chem_comp_container_identifiers
                - CATEGORY_NAME: rcsb_schema_container_identifiers
        ?       - GENERATED_CONTENT
                - bird_chem_comp
        :       - CATEGORY_NAME: rcsb_load_status
                  ATTRIBUTE_NAME_LIST:
                      - datablock_name
                      - load_date
                      - locator
                - CATEGORY_NAME: pdbx_chem_comp_audit
                  ATTRIBUTE_NAME_LIST:
                      - ordinal
        ?       - GENERATED_CONTENT
                - bird_chem_comp_core
        :       - CATEGORY_NAME: rcsb_load_status
                  ATTRIBUTE_NAME_LIST:
                      - datablock_name
                      - load_date
                      - locator
                - CATEGORY_NAME: citation
                  ATTRIBUTE_NAME_LIST:
                      - rcsb_authors
                - CATEGORY_NAME: rcsb_chem_comp_synonyms
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - ordinal
                      - name
                      - provenance_code
                - CATEGORY_NAME: rcsb_chem_comp_info
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - atom_count
                      - atom_count_chiral
                      - bond_count
                      - bond_count_aromatic
                      - atom_count_heavy
                - CATEGORY_NAME: rcsb_chem_comp_descriptor
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - SMILES
                      - SMILES_stereo
                      - InChI
                      - InChIKey
                - CATEGORY_NAME: rcsb_chem_comp_related
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - ordinal
                      - resource_name
                      - resource_accession_code
                      - related_mapping_method
                - CATEGORY_NAME: rcsb_chem_comp_target
                  ATTRIBUTE_NAME_LIST:
                      - comp_id
                      - ordinal
                      - name
                      - interaction_type
                      - target_actions
                      - organism_common_name
                      - reference_database_name
                      - reference_database_accession_code
                      - provenance_code
                - CATEGORY_NAME: pdbx_chem_comp_audit
                  ATTRIBUTE_NAME_LIST:
                      - ordinal
                - CATEGORY_NAME: rcsb_chem_comp_container_identifiers
                - CATEGORY_NAME: rcsb_schema_container_identifiers
        ?       - GENERATED_CONTENT
                - bird
        :       - CATEGORY_NAME: rcsb_load_status
                  ATTRIBUTE_NAME_LIST:
                      - datablock_name
                      - load_date
                      - locator
        ?       - GENERATED_CONTENT
                - bird_family
        :       - CATEGORY_NAME: rcsb_load_status
                  ATTRIBUTE_NAME_LIST:
                      - datablock_name
                      - load_date
                      - locator
        ?       - REPO_HOLDINGS_CONTENT
                - repository_holdings
        :       - CATEGORY_NAME: rcsb_repository_holdings_current
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - repository_content_types
                - CATEGORY_NAME: rcsb_repository_holdings_update
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - repository_content_types
                - CATEGORY_NAME: rcsb_repository_holdings_removed
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - repository_content_types
                      - deposit_date
                      - release_date
                      - remove_date
                      - title
                      - details
                      - audit_authors
                      - id_codes_replaced_by
                - CATEGORY_NAME: rcsb_repository_holdings_unreleased
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - status_code
                      - deposit_date
                      - deposit_date_coordinates
                      - deposit_date_structure_factors
                      - deposit_date_nmr_restraints
                      - hold_date_coordinates
                      - hold_date_structure_factors
                      - hold_date_nmr_restraints
                      - release_date
                      - title
                      - audit_authors
                      - author_prerelease_sequence_status
                - CATEGORY_NAME: rcsb_repository_holdings_prerelease
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - seq_one_letter_code
                - CATEGORY_NAME: rcsb_repository_holdings_removed_audit_author
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - ordinal_id
                      - audit_author
                - CATEGORY_NAME: rcsb_repository_holdings_superseded
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - id_codes_superseded
                - CATEGORY_NAME: rcsb_repository_holdings_transferred
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - status_code
                      - deposit_date
                      - release_date
                      - title
                      - audit_authors
                      - remote_accession_code
                      - remote_repository_name
                      - repository_content_types
                - CATEGORY_NAME: rcsb_repository_holdings_insilico_models
                  ATTRIBUTE_NAME_LIST:
                      - update_id
                      - entry_id
                      - status_code
                      - deposit_date
                      - release_date
                      - title
                      - audit_authors
                      - remove_date
                      - id_codes_replaced_by
                - CATEGORY_NAME: rcsb_schema_container_identifiers
        ?       - SEQUENCE_CLUSTER_CONTENT
                - entity_sequence_clusters
        :       - CATEGORY_NAME: rcsb_entity_sequence_cluster_list
                  ATTRIBUTE_NAME_LIST:
                      - data_set_id
                      - entry_id
                      - entity_id
                      - cluster_id
                      - identity
                - CATEGORY_NAME: rcsb_instance_sequence_cluster_list
                  ATTRIBUTE_NAME_LIST:
                      - data_set_id
                      - entry_id
                      - instance_id
                      - cluster_id
                      - identity
                - CATEGORY_NAME: rcsb_schema_container_identifiers
        ?       - CONSOLIDATED_BIRD_CONTENT
                - bird_chem_comp_core
        :       - CATEGORY_NAME: chem_comp
                  #ATTRIBUTE_NAME_LIST:
                  #    - id
                  #    - name
                  #    - formula
                  #    - formula_weight
                  #    - pdbx_release_status
                  #    - type
                - CATEGORY_NAME: pdbx_chem_comp_descriptor
                  ATTRIBUTE_NAME_LIST:
                       - comp_id
                       - type
                       - program
                       - program_version
                       - descriptor
                - CATEGORY_NAME: pdbx_chem_comp_identifier
                  ATTRIBUTE_NAME_LIST:
                       - comp_id
                       - type
                       - program
                       - program_version
                       - identifier
                - CATEGORY_NAME: pdbx_reference_molecule
                  ATTRIBUTE_NAME_LIST:
                       - prd_id
                       - name
                       - represent_as
                       - type
                       - type_evidence_code
                       - class
                       - class_evidence_code
                       - formula
                       - formula_weight
                       - chem_comp_id
                       - release_status
                       - replaces
                       - replaced_by
                       - compound_details
                       - description
                       - representative_PDB_id_code
                - CATEGORY_NAME: pdbx_reference_entity_list
                  ATTRIBUTE_NAME_LIST:
                       - prd_id
                       - ref_entity_id
                       - component_id
                       - type
                       - details
                - CATEGORY_NAME: pdbx_reference_entity_poly_link
                  ATTRIBUTE_NAME_LIST:
                       - prd_id
                       - ref_entity_id
                       - link_id
                       - atom_id_1
                       - comp_id_1
                       - entity_seq_num_1
                       - atom_id_2
                       - comp_id_2
                       - entity_seq_num_2
                       - value_order
                       - component_id
                - CATEGORY_NAME: pdbx_reference_entity_poly
                  ATTRIBUTE_NAME_LIST:
                       - prd_id
                       - ref_entity_id
                       - db_code
                       - db_name
                       - type
                - CATEGORY_NAME: pdbx_reference_entity_sequence
                  ATTRIBUTE_NAME_LIST:
                       - prd_id
                       - ref_entity_id
                       - type
                       - NRP_flag
                       - one_letter_codes
                - CATEGORY_NAME: pdbx_reference_entity_poly_seq
                  ATTRIBUTE_NAME_LIST:
                       - prd_id
                       - ref_entity_id
                       - num
                       - mon_id
                       - parent_mon_id
                       - hetero
                       - observed
                - CATEGORY_NAME: pdbx_reference_entity_src_nat
                  ATTRIBUTE_NAME_LIST:
                       - prd_id
                       - ref_entity_id
                       - ordinal
                       - taxid
                       - organism_scientific
                       - source
                       - source_id
                       - atcc
                       - db_code
                       - db_name
                - CATEGORY_NAME: pdbx_prd_audit
                  ATTRIBUTE_NAME_LIST:
                       - prd_id
                       - date
                       - processing_site
                       - action_type
                       - annotator
                       - details
                - CATEGORY_NAME: pdbx_reference_molecule_family
                  ATTRIBUTE_NAME_LIST:
                       - family_prd_id
                       - name
                       - release_status
                       - replaces
                       - replaced_by
                - CATEGORY_NAME: pdbx_reference_molecule_list
                  ATTRIBUTE_NAME_LIST:
                       - family_prd_id
                       - prd_id
                - CATEGORY_NAME: pdbx_reference_molecule_related_structures
                  ATTRIBUTE_NAME_LIST:
                       - family_prd_id
                       - ordinal
                       - citation_id
                       - db_name
                       - db_accession
                       - db_code
                       - name
                       - formula
                - CATEGORY_NAME: pdbx_reference_molecule_synonyms
                  ATTRIBUTE_NAME_LIST:
                       - family_prd_id
                       - prd_id
                       - ordinal
                       - source
                       - name
                - CATEGORY_NAME: pdbx_reference_molecule_annotation
                  ATTRIBUTE_NAME_LIST:
                       - family_prd_id
                       - prd_id
                       - ordinal
                       - source
                       - type
                       - text
                - CATEGORY_NAME: pdbx_reference_molecule_features
                  ATTRIBUTE_NAME_LIST:
                       - family_prd_id
                       - prd_id
                       - ordinal
                       - source_ordinal
                       - source
                       - type
                       - value
                - CATEGORY_NAME: pdbx_reference_molecule_details
                  ATTRIBUTE_NAME_LIST:
                       - family_prd_id
                       - ordinal
                       - source
                       - source_id
                       - text
                - CATEGORY_NAME: citation
                  ATTRIBUTE_NAME_LIST:
                       - id
                       - book_id_ISBN
                       - book_publisher
                       - book_publisher_city
                       - book_title
                       - coordinate_linkage
                       - country
                       - journal_abbrev
                       - journal_id_ASTM
                       - journal_id_CSD
                       - journal_id_ISSN
                       - journal_issue
                       - journal_volume
                       - language
                       - page_first
                       - page_last
                       - pdbx_database_id_DOI
                       - pdbx_database_id_PubMed
                       - rcsb_authors
                       - title
                       - unpublished_flag
                       - year
                - CATEGORY_NAME: citation_author
                  ATTRIBUTE_NAME_LIST:
                       - citation_id
                       - ordinal
                       - name
                - CATEGORY_NAME: pdbx_family_prd_audit
                  ATTRIBUTE_NAME_LIST:
                       - family_prd_id
                       - date
                       - processing_site
                       - action_type
                       - annotator
                       - details
                - CATEGORY_NAME: rcsb_schema_container_identifiers
        ?       - INTEGRATED_CONTENT
                - drugbank_core
        :       - CATEGORY_NAME: drugbank_info
                  ATTRIBUTE_NAME_LIST:
                      - drugbank_id
                      - name
                      - description
                      - synonyms
                      - brand_names
                      - affected_organism
                      - indication
                      - pharmacology
                      - mechanism_of_action
                      - cas_number
                      - drug_categories
                      - drug_groups
                - CATEGORY_NAME: drugbank_target
                  ATTRIBUTE_NAME_LIST:
                      - ordinal
                      - name
                      - interaction_type
                      - target_actions
                      - organism_common_name
                      - reference_database_name
                      - reference_database_accession_code
                      - seq_one_letter_code
                - CATEGORY_NAME: rcsb_schema_container_identifiers
    slice_parent_items:
        ?       - ENTITY
                - pdbx_core
        :       - CATEGORY_NAME: entity
                  ATTRIBUTE_NAME: id
        ?       - ASSEMBLY
                - pdbx_core
        :       - CATEGORY_NAME: pdbx_struct_assembly
                  ATTRIBUTE_NAME: id
        ?       - ENTITY_MONOMER
                - pdbx_core
        :       - CATEGORY_NAME: rcsb_entity_poly_info
                  ATTRIBUTE_NAME: ordinal_id
        ?       - ENTITY_INSTANCE
                - pdbx_core
        :       - CATEGORY_NAME: struct_asym
                  ATTRIBUTE_NAME: id
    slice_parent_filters:
        ?       - ENTITY
                - pdbx_core
        :       - CATEGORY_NAME: entity
                  ATTRIBUTE_NAME: type
                  VALUES:
                      - polymer
                      - non-polymer
                      - macrolide
                      - branched
                      - water
    slice_cardinality_category_extras:
        ?       - ENTITY
                - pdbx_core
        :       - rcsb_load_status
                - rcsb_entity_container_identifiers
        ?       - ASSEMBLY
                - pdbx_core
        :       - rcsb_load_status
                - rcsb_assembly_container_identifiers
        ?       - ENTITY_MONOMER
                - pdbx_core
        :       - rcsb_entity_poly_info
                - rcsb_entity_monomer_container_identifiers
        ?       - ENTITY_INSTANCE
                - pdbx_core
        :       - rcsb_load_status
                - rcsb_entity_instance_container_identifiers
    slice_category_extras:
        ?       - ENTITY
                - pdbx_core
        :       - rcsb_load_status
        ?       - ENTITY_INSTANCE
                - pdbx_core
        :       - rcsb_load_status
        ?       - ASSEMBLY
                - pdbx_core
        :       - rcsb_load_status
                - pdbx_struct_oper_list
#
##
#
schemadef_helper:
    #
    schema_info:
        ihm_dev:
            DATABASE_NAME: ihm_dev
            VERSION: v1_0_2
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-ihm_dev-type-map.json
        pdbx:
            DATABASE_NAME: pdbx
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-pdbx-type-map.json
        pdbx_core:
            DATABASE_NAME: pdbx_core
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-pdbx-type-map.json
        bird:
            DATABASE_NAME: bird
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-bird-type-map.json
        bird_family:
            DATABASE_NAME: bird
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-bird_family-type-map.json
        chem_comp:
            DATABASE_NAME: chem_comp
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-chem_comp-type-map.json
        chem_comp_core:
            DATABASE_NAME: chem_comp_core
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-chem_comp-type-map.json
        bird_chem_comp:
            DATABASE_NAME: chem_comp
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-bird_chem_comp-type-map.json
        bird_chem_comp_core:
            DATABASE_NAME: chem_comp_core
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-bird_chem_comp-type-map.json
        pdb_distro:
            DATABASE_NAME: stat
            VERSION: v1_0_2
            INSTANCE_DATA_TYPE_INFO_FILENAME:
        repository_holdings:
            DATABASE_NAME: repository_holdings
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME: scan-repository_holdings-type-map.json
        entity_sequence_clusters:
            DATABASE_NAME: sequence_clusters
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME:
        data_exchange:
            DATABASE_NAME: data_exchange
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME:
        drugbank_core:
            DATABASE_NAME: chem_comp_core
            VERSION: v5_0_3
            INSTANCE_DATA_TYPE_INFO_FILENAME:
    #
    schema_content_filters:
        ihm_dev:
            INCLUDE: []
            EXCLUDE:
                - atom_site
                - atom_site_anisotrop
                - pdbx_poly_seq_scheme
                - ihm_starting_model_coord
                - ihm_sphere_obj_site
        pdbx:
            INCLUDE: []
            EXCLUDE:
                - ATOM_SITE
                - ATOM_SITE_ANISOTROP
        pdbx_core:
            INCLUDE: []
            EXCLUDE:
                - ATOM_SITE
                - ATOM_SITE_ANISOTROP
        repository_holdings:
            INCLUDE:
                - rcsb_repository_holdings_update
                - rcsb_repository_holdings_current
                - rcsb_repository_holdings_unreleased
                - rcsb_repository_holdings_prerelease
                - rcsb_repository_holdings_removed
                - rcsb_repository_holdings_removed_audit_author
                - rcsb_repository_holdings_superseded
                - rcsb_repository_holdings_transferred
                - rcsb_repository_holdings_insilico_models
            EXCLUDE: []
        entity_sequence_clusters:
            INCLUDE:
                - rcsb_instance_sequence_cluster_list
                - rcsb_entity_sequence_cluster_list
                - software
                - citation
                - citation_author
            EXCLUDE: []
        data_exchange:
            INCLUDE:
                - rcsb_data_exchange_status
            EXCLUDE: []
        drugbank_core:
            INCLUDE:
                - drugbank_container_identifiers
                - drugbank_info
                - drugbank_target
            EXCLUDE: []
    block_attributes:
        ihm_dev:
            ATTRIBUTE_NAME: structure_id
            CIF_TYPE_CODE: code
            MAX_WIDTH: 16
            METHOD: datablockid()
        pdbx:
            ATTRIBUTE_NAME: structure_id
            CIF_TYPE_CODE: code
            MAX_WIDTH: 12
            METHOD: datablockid()
        bird:
            ATTRIBUTE_NAME: db_id
            CIF_TYPE_CODE: code
            MAX_WIDTH: 10
            METHOD: datablockid()
        bird_family:
            ATTRIBUTE_NAME: db_id
            CIF_TYPE_CODE: code
            MAX_WIDTH: 10
            METHOD: datablockid()
        chem_comp:
            ATTRIBUTE_NAME: component_id
            CIF_TYPE_CODE: code
            MAX_WIDTH: 10
            METHOD: datablockid()
        bird_chem_comp:
            ATTRIBUTE_NAME: component_id
            CIF_TYPE_CODE: code
            MAX_WIDTH: 10
            METHOD: datablockid()
        pdb_distro:
            ATTRIBUTE_NAME: structure_id
            CIF_TYPE_CODE: code
            MAX_WIDTH: 12
            METHOD: datablockid()
    #
    exclude_attributes:
        pdbx_core:
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: length_a_esd
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: length_b_esd
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: length_c_esd
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: angle_alpha_esd
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: angle_beta_esd
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: angle_gamma_esd
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: reciprocal_angle_alpha
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: reciprocal_angle_beta
            - CATEGORY_NAME: cell
              ATTRIBUTE_NAME: reciprocal_angle_gamma
        chem_comp_core:
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_model_coordinates_details
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_model_coordinates_missing_flag
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_ideal_coordinates_details
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_ideal_coordinates_missing_flag
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_model_coordinates_db_code
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_synonyms
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_type
            - CATEGORY_NAME: pdbx_chem_comp_audit
              ATTRIBUTE_NAME: processing_site
            - CATEGORY_NAME: pdbx_chem_comp_audit
              ATTRIBUTE_NAME: annotator
        bird_chem_comp_core:
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_model_coordinates_details
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_model_coordinates_missing_flag
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_ideal_coordinates_details
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_ideal_coordinates_missing_flag
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_model_coordinates_db_code
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_synonyms
            - CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: pdbx_type
            - CATEGORY_NAME: pdbx_chem_comp_audit
              ATTRIBUTE_NAME: processing_site
            - CATEGORY_NAME: pdbx_chem_comp_audit
              ATTRIBUTE_NAME: annotator
#
#
document_helper:
    #
    schema_collection_names:
        ihm_dev:
            - NAME: ihm_dev
              VERSION: v1_0_2
        pdbx:
            - NAME: pdbx
              VERSION: v5_0_3
            - NAME: pdbx_ext
              VERSION: v5_0_3
        pdbx_core:
            - NAME: pdbx_core_entity
              VERSION: v5_0_3
            - NAME: pdbx_core_entry
              VERSION: v5_0_3
            - NAME: pdbx_core_assembly
              VERSION: v5_0_3
            - NAME: pdbx_core_entity_monomer
              VERSION: v5_0_3
            - NAME: pdbx_core_entity_instance
              VERSION: v5_0_3
        bird:
            - NAME: bird
              VERSION: v5_0_3
        bird_family:
            - NAME: family
              VERSION: v5_0_3
        chem_comp:
            - NAME: chem_comp
              VERSION: v5_0_3
        chem_comp_core:
            - NAME: chem_comp_core
              VERSION: v5_0_3
        bird_chem_comp:
            - NAME: bird_chem_comp
              VERSION: v5_0_3
        bird_chem_comp_core:
            - NAME: bird_chem_comp_core
              VERSION: v5_0_3
        pdb_distro: []
        repository_holdings:
            - NAME: repository_holdings_update
              VERSION: v1_0_2
            - NAME: repository_holdings_current
              VERSION: v1_0_2
            - NAME: repository_holdings_unreleased
              VERSION: v1_0_2
            - NAME: repository_holdings_prerelease
              VERSION: v1_0_2
            - NAME: repository_holdings_removed
              VERSION: v1_0_2
            - NAME: repository_holdings_removed_audit_authors
              VERSION: v1_0_2
            - NAME: repository_holdings_superseded
              VERSION: v1_0_2
            - NAME: repository_holdings_transferred
              VERSION: v1_0_2
            - NAME: repository_holdings_insilico_models
              VERSION: v1_0_2
        entity_sequence_clusters:
            - NAME: cluster_members
              VERSION: v1_0_2
            - NAME: cluster_provenance
              VERSION: v1_0_2
            - NAME: entity_members
              VERSION: v1_0_2
        data_exchange:
            - NAME: rcsb_data_exchange_status
              VERSION: v1_0_2
        drugbank_core:
            - NAME: drugbank_core
              VERSION: v1_0_2
    #
    schema_content_filters:
        ihm_dev:
            INCLUDE: []
            EXCLUDE:
                - ATOM_SITE
        chem_comp_core:
            INCLUDE: []
            EXCLUDE:
                - CHEM_COMP_ATOM
                - CHEM_COMP_BOND
        bird_chem_comp_core:
            INCLUDE: []
            EXCLUDE:
                - CHEM_COMP_ATOM
                - CHEM_COMP_BOND
                - CITATION_AUTHOR
        pdbx:
            INCLUDE: []
            EXCLUDE:
                - NDB_STRUCT_CONF_NA
                - NDB_STRUCT_FEATURE_NA
                - NDB_STRUCT_NA_BASE_PAIR
                - NDB_STRUCT_NA_BASE_PAIR_STEP
                - PDBX_VALIDATE_CHIRAL
                - PDBX_VALIDATE_CLOSE_CONTACT
                - PDBX_VALIDATE_MAIN_CHAIN_PLANE
                - PDBX_VALIDATE_PEPTIDE_OMEGA
                - PDBX_VALIDATE_PLANES
                - PDBX_VALIDATE_PLANES_ATOM
                - PDBX_VALIDATE_POLYMER_LINKAGE
                - PDBX_VALIDATE_RMSD_ANGLE
                - PDBX_VALIDATE_RMSD_BOND
                - PDBX_VALIDATE_SYMM_CONTACT
                - PDBX_VALIDATE_TORSION
                - STRUCT_SHEET
                - STRUCT_SHEET_HBOND
                - STRUCT_SHEET_ORDER
                - STRUCT_SHEET_RANGE
                - STRUCT_CONF
                - STRUCT_CONF_TYPE
                - STRUCT_CONN
                - STRUCT_CONN_TYPE
                - ATOM_SITE
                - ATOM_SITE_ANISOTROP
                - PDBX_UNOBS_OR_ZERO_OCC_ATOMS
                - PDBX_UNOBS_OR_ZERO_OCC_RESIDUES
            SLICE:
        pdbx_ext:
            INCLUDE:
                - ENTRY
                - NDB_STRUCT_CONF_NA
                - NDB_STRUCT_FEATURE_NA
                - NDB_STRUCT_NA_BASE_PAIR
                - NDB_STRUCT_NA_BASE_PAIR_STEP
                - PDBX_VALIDATE_CHIRAL
                - PDBX_VALIDATE_CLOSE_CONTACT
                - PDBX_VALIDATE_MAIN_CHAIN_PLANE
                - PDBX_VALIDATE_PEPTIDE_OMEGA
                - PDBX_VALIDATE_PLANES
                - PDBX_VALIDATE_PLANES_ATOM
                - PDBX_VALIDATE_POLYMER_LINKAGE
                - PDBX_VALIDATE_RMSD_ANGLE
                - PDBX_VALIDATE_RMSD_BOND
                - PDBX_VALIDATE_SYMM_CONTACT
                - PDBX_VALIDATE_TORSION
                - STRUCT_SHEET
                - STRUCT_SHEET_HBOND
                - STRUCT_SHEET_ORDER
                - STRUCT_SHEET_RANGE
                - STRUCT_CONF
                - STRUCT_CONF_TYPE
                - STRUCT_CONN
                - STRUCT_CONN_TYPE
                - RCSB_LOAD_STATUS
            EXCLUDE: []
            SLICE:
        pdbx_core_entity:
            INCLUDE: []
            EXCLUDE:
                - rcsb_entity_instance_container_identifiers
                - rcsb_entity_monomer_container_identifiers
                - rcsb_entity_poly_info
                - pdbx_poly_seq_scheme
                - pdbx_nonpoly_scheme
                - pdbx_vrpt_summary
                - pdbx_vrpt_summary
                - pdbx_vrpt_model_list
                - pdbx_vrpt_instance_results
                - pdbx_vrpt_cyrange_domain
                - pdbx_vrpt_entity_instance_results
                - pdbx_vrpt_chemical_shift_list
                - pdbx_vrpt_unmapped_chemical_shift
                - pdbx_vrpt_unparsed_chemical_shift
                - pdbx_vrpt_missing_nmrstar_tags
                - pdbx_vrpt_random_coil_index
                - pdbx_vrpt_chemical_shift_outlier
                - pdbx_vrpt_referencing_offset
                - pdbx_vrpt_assign_compl_well_defined
                - pdbx_vrpt_assign_compl_full_length
                - pdbx_vrpt_software
                - pdbx_vrpt_bond_outliers
                - pdbx_vrpt_angle_outliers
                - pdbx_vrpt_stereo_outliers
                - pdbx_vrpt_plane_outliers
                - pdbx_vrpt_clashes
                - pdbx_vrpt_symmetry_clashes
                - pdbx_vrpt_mogul_bond_outliers
                - pdbx_vrpt_mogul_angle_outliers
                - pdbx_vrpt_mogul_torsion_outliers
                - pdbx_vrpt_mogul_ring_outliers
            SLICE: ENTITY
        pdbx_core_entity_monomer:
            INCLUDE: []
            EXCLUDE: []
            SLICE: ENTITY_MONOMER
        pdbx_core_assembly:
            INCLUDE: []
            EXCLUDE: []
            SLICE: ASSEMBLY
        pdbx_core_entry:
            INCLUDE:
                - AUDIT_AUTHOR
                - CELL
                - CITATION
                - CITATION_AUTHOR
                - DIFFRN
                - DIFFRN_DETECTOR
                - DIFFRN_RADIATION
                - DIFFRN_SOURCE
                - EM_2D_CRYSTAL_ENTITY
                - EM_3D_CRYSTAL_ENTITY
                - EM_3D_FITTING
                - EM_3D_RECONSTRUCTION
                - EM_EMBEDDING
                - EM_ENTITY_ASSEMBLY
                - EM_EXPERIMENT
                - EM_HELICAL_ENTITY
                - EM_IMAGE_RECORDING
                - EM_IMAGING
                - EM_SINGLE_PARTICLE_ENTITY
                - EM_SOFTWARE
                - EM_SPECIMEN
                - EM_STAINING
                - EM_VITRIFICATION
                - em_ctf_correction
                - em_particle_selection
                - ENTRY
                - EXPTL
                - EXPTL_CRYSTAL
                - EXPTL_CRYSTAL_GROW
                - PDBX_AUDIT_REVISION_DETAILS
                - PDBX_AUDIT_REVISION_HISTORY
                - PDBX_AUDIT_SUPPORT
                - PDBX_AUDIT_REVISION_GROUP
                - PDBX_AUDIT_REVISION_CATEGORY
                - PDBX_AUDIT_REVISION_ITEM
                - PDBX_DATABASE_PDB_OBS_SPR
                - PDBX_DATABASE_RELATED
                - PDBX_DATABASE_STATUS
                - PDBX_DEPOSIT_GROUP
                - PDBX_MOLECULE
                - PDBX_MOLECULE_FEATURES
                - PDBX_NMR_DETAILS
                - PDBX_NMR_ENSEMBLE
                - PDBX_NMR_EXPTL
                - PDBX_NMR_EXPTL_SAMPLE_CONDITIONS
                - PDBX_NMR_REFINE
                - PDBX_NMR_REPRESENTATIVE
                - PDBX_NMR_SAMPLE_DETAILS
                - PDBX_NMR_SOFTWARE
                - PDBX_NMR_SPECTROMETER
                - PDBX_SG_PROJECT
                - RCSB_ATOM_COUNT
                - RCSB_BINDING
                - RCSB_EXTERNAL_REFERENCES
                - RCSB_HAS_CHEMICAL_SHIFT_FILE
                - RCSB_HAS_ED_MAP_FILE
                - RCSB_HAS_FOFC_FILE
                - RCSB_HAS_NMR_V1_FILE
                - RCSB_HAS_NMR_V2_FILE
                - RCSB_HAS_STRUCTURE_FACTORS_FILE
                - RCSB_HAS_TWOFOFC_FILE
                - RCSB_HAS_VALIDATION_REPORT
                - RCSB_LATEST_REVISION
                - RCSB_MODELS_COUNT
                - RCSB_MOLECULAR_WEIGHT
                - RCSB_PUBMED
                - RCSB_RELEASE_DATE
                - REFINE
                - REFINE_ANALYZE
                - REFINE_HIST
                - REFINE_LS_RESTR
                - REFLNS
                - REFLNS_SHELL
                - SOFTWARE
                - STRUCT
                - STRUCT_KEYWORDS
                - SYMMETRY
                - RCSB_ACCESSION_INFO
                - RCSB_ENTRY_INFO
                - RCSB_LOAD_STATUS
                - RCSB_ENTRY_CONTAINER_IDENTIFIERS
                - PDBX_SERIAL_CRYSTALLOGRAPHY_MEASUREMENT
                - PDBX_SERIAL_CRYSTALLOGRAPHY_SAMPLE_DELIVERY
                - PDBX_SERIAL_CRYSTALLOGRAPHY_SAMPLE_DELIVERY_INJECTION
                - PDBX_SERIAL_CRYSTALLOGRAPHY_SAMPLE_DELIVERY_FIXED_TARGET
                - PDBX_SERIAL_CRYSTALLOGRAPHY_DATA_REDUCTION
                - PDBX_VRPT_SUMMARY
                - PDBX_SOLN_SCATTER
                - PDBX_SOLN_SCATTER_MODEL
            EXCLUDE: []
            SLICE:
        pdbx_core_entity_instance:
            INCLUDE: []
            EXCLUDE:
                - pdbx_poly_seq_scheme
                - pdbx_nonpoly_scheme
                - pdbx_struct_chem_comp_diagnostics
                - pdbx_refine_tls_group
                - pdbx_entity_instance_feature
                - pdbx_struct_mod_residue
                - refine_ls_restr_ncs
                - struct_ncs_dom_lim
            SLICE: ENTITY_INSTANCE
        repository_holdings_update:
            INCLUDE:
                - rcsb_repository_holdings_update
            EXCLUDE: []
            SLICE:
        repository_holdings_current:
            INCLUDE:
                - rcsb_repository_holdings_current
            EXCLUDE: []
            SLICE:
        repository_holdings_unreleased:
            INCLUDE:
                - rcsb_repository_holdings_unreleased
            EXCLUDE: []
            SLICE:
        repository_holdings_prerelease:
            INCLUDE:
                - rcsb_repository_holdings_prerelease
            EXCLUDE: []
            SLICE:
        repository_holdings_removed:
            INCLUDE:
                - rcsb_repository_holdings_removed
            EXCLUDE: []
            SLICE:
        repository_holdings_removed_audit_authors:
            INCLUDE:
                - rcsb_repository_holdings_removed_audit_author
            EXCLUDE: []
            SLICE:
        repository_holdings_superseded:
            INCLUDE:
                - rcsb_repository_holdings_superseded
            EXCLUDE: []
            SLICE:
        repository_holdings_transferred:
            INCLUDE:
                - rcsb_repository_holdings_transferred
            EXCLUDE: []
            SLICE:
        repository_holdings_insilico_models:
            INCLUDE:
                - rcsb_repository_holdings_insilico_models
            EXCLUDE: []
            SLICE:
        cluster_members:
            INCLUDE:
                - rcsb_entity_sequence_cluster_list
            EXCLUDE: []
            SLICE:
        cluster_provenance:
            INCLUDE:
                - software
                - citation
                - citation_author
            EXCLUDE: []
            SLICE:
        entity_members:
            INCLUDE:
                - rcsb_entity_sequence_cluster_list
            EXCLUDE: []
            SLICE:
        rcsb_data_exchange_status:
            INCLUDE:
                - rcsb_data_exchange_status
            EXCLUDE: []
            SLICE:
        drugbank_core:
            INCLUDE:
                - drugbank_info
                - drugbank_target
            EXCLUDE: []
            SLICE:
    collection_private_keys:
        ihm_dev:
            - NAME: entry.id
              CATEGORY_NAME: entry
              ATTRIBUTE_NAME: id
              PRIVATE_DOCUMENT_NAME: _entry_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        pdbx_core_entity:
            - NAME: entry.id
              CATEGORY_NAME: rcsb_entity_container_identifiers
              ATTRIBUTE_NAME: entry_id
              PRIVATE_DOCUMENT_NAME: _entry_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: entity.id
              CATEGORY_NAME: entity
              ATTRIBUTE_NAME: id
              PRIVATE_DOCUMENT_NAME: _entity_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: chem_comp.id
              CATEGORY_NAME: pdbx_entity_nonpoly
              ATTRIBUTE_NAME: comp_id
              PRIVATE_DOCUMENT_NAME: _comp_id
              MANDATORY: False
              UPDATE_ON_LOAD: False
            - NAME: _pdbx_entity_nonpoly.rcsb_prd_id
              CATEGORY_NAME: pdbx_entity_nonpoly
              ATTRIBUTE_NAME: rcsb_prd_id
              PRIVATE_DOCUMENT_NAME: _prd_id
              MANDATORY: False
              UPDATE_ON_LOAD: False
            - NAME: _entity_poly.rcsb_prd_id
              CATEGORY_NAME: entity_poly
              ATTRIBUTE_NAME: rcsb_prd_id
              PRIVATE_DOCUMENT_NAME: _prd_id
              MANDATORY: False
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        pdbx_core_entity_instance:
            - NAME: entry.id
              CATEGORY_NAME: rcsb_entity_instance_container_identifiers
              ATTRIBUTE_NAME: entry_id
              PRIVATE_DOCUMENT_NAME: _entry_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: entity.id
              CATEGORY_NAME: rcsb_entity_instance_container_identifiers
              ATTRIBUTE_NAME: entity_id
              PRIVATE_DOCUMENT_NAME: _entity_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: struct_asym.id
              CATEGORY_NAME: rcsb_entity_instance_container_identifiers
              ATTRIBUTE_NAME: asym_id
              PRIVATE_DOCUMENT_NAME: _asym_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_entity_instance_container_identifiers.auth_asym_id
              CATEGORY_NAME: rcsb_entity_instance_container_identifiers
              ATTRIBUTE_NAME: auth_asym_id
              PRIVATE_DOCUMENT_NAME: _auth_asym_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_entity_instance_container_identifiers.comp_id
              CATEGORY_NAME: rcsb_entity_instance_container_identifiers
              ATTRIBUTE_NAME: comp_id
              PRIVATE_DOCUMENT_NAME: _comp_id
              MANDATORY: False
              UPDATE_ON_LOAD: False
            - NAME: rcsb_entity_instance_container_identifiers.auth_seq_id
              CATEGORY_NAME: rcsb_entity_instance_container_identifiers
              ATTRIBUTE_NAME: auth_seq_id
              PRIVATE_DOCUMENT_NAME: _auth_seq_id
              MANDATORY: False
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        pdbx_core_entity_monomer:
            - NAME: entry.id
              CATEGORY_NAME: rcsb_entity_poly_info
              ATTRIBUTE_NAME: entry_id
              PRIVATE_DOCUMENT_NAME: _entry_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: entity.id
              CATEGORY_NAME: rcsb_entity_poly_info
              ATTRIBUTE_NAME: entity_id
              PRIVATE_DOCUMENT_NAME: _entity_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: chem_comp.id
              CATEGORY_NAME: rcsb_entity_poly_info
              ATTRIBUTE_NAME: comp_id
              PRIVATE_DOCUMENT_NAME: _comp_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        pdbx_core_assembly:
            - NAME: entry.id
              CATEGORY_NAME: rcsb_assembly_container_identifiers
              ATTRIBUTE_NAME: entry_id
              PRIVATE_DOCUMENT_NAME: _entry_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: pdbx_struct_assembly.id
              CATEGORY_NAME: pdbx_struct_assembly
              ATTRIBUTE_NAME: id
              PRIVATE_DOCUMENT_NAME: _assembly_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        pdbx_core_entry:
            - NAME: entry.id
              CATEGORY_NAME: entry
              ATTRIBUTE_NAME: id
              PRIVATE_DOCUMENT_NAME: _entry_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        chem_comp_core:
            - NAME: chem_comp.id
              CATEGORY_NAME: chem_comp
              ATTRIBUTE_NAME: id
              PRIVATE_DOCUMENT_NAME: _comp_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        bird_chem_comp_core:
            - NAME: pdbx_reference_molecule.chem_comp_id
              CATEGORY_NAME: pdbx_reference_molecule
              ATTRIBUTE_NAME: chem_comp_id
              PRIVATE_DOCUMENT_NAME: _comp_id
              MANDATORY: False
              UPDATE_ON_LOAD: False
            - NAME: pdbx_reference_molecule.prd_id
              CATEGORY_NAME: pdbx_reference_molecule
              ATTRIBUTE_NAME: prd_id
              PRIVATE_DOCUMENT_NAME: _prd_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        drugbank_core:
            - NAME: drugbank_info.drugbank_id
              CATEGORY_NAME: drugbank_info
              ATTRIBUTE_NAME: drugbank_id
              PRIVATE_DOCUMENT_NAME: _drugbank_id
              MANDATORY: True
              UPDATE_ON_LOAD: False
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_update:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_current:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_unreleased:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_prerelease:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_removed:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_removed_audit_authors:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_superseded:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_transferred:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        repository_holdings_insilico_models:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        cluster_members:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        cluster_provenance:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        entity_members:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
        rcsb_data_exchange_status:
            - NAME: rcsb_schema_container_identifiers.collection_schema_version
              CATEGORY_NAME: rcsb_schema_container_identifiers
              ATTRIBUTE_NAME: collection_schema_version
              PRIVATE_DOCUMENT_NAME: _schema_version
              MANDATORY: False
              UPDATE_ON_LOAD: True
    #
    collection_indices:
        ihm_dev:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - entry.id
        pdbx:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - entry.id
        pdbx_ext:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - entry.id
        pdbx_core_entity:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - rcsb_entity_container_identifiers.entry_id
              - rcsb_entity_container_identifiers.entity_id
            - INDEX_NAME: search_1
              ATTRIBUTE_NAMES:
              - rcsb_entity_container_identifiers.entry_id
              - rcsb_entity_container_identifiers.entry_id
            - INDEX_NAME: search_2
              ATTRIBUTE_NAMES:
              - rcsb_entity_container_identifiers.entry_id
              - rcsb_entity_container_identifiers.entity_id
              - rcsb_entity_container_identifiers.asym_ids
            - INDEX_NAME: search_3
              ATTRIBUTE_NAMES:
              - rcsb_entity_container_identifiers.entry_id
              - rcsb_entity_container_identifiers.entity_id
              - rcsb_entity_container_identifiers.auth_asym_ids
            - INDEX_NAME: search_4
              ATTRIBUTE_NAMES:
              - _entry_id
              - _entity_id
            - INDEX_NAME: search_5
              ATTRIBUTE_NAMES:
              - _entry_id
              - _comp_id
            - INDEX_NAME: replace
              ATTRIBUTE_NAMES:
              - _entry_id
        pdbx_core_entity_monomer:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - rcsb_entity_poly_info.entry_id
              - rcsb_entity_poly_info.entity_id
              - rcsb_entity_poly_info.comp_id
            - INDEX_NAME: search_1
              ATTRIBUTE_NAMES:
              - rcsb_entity_poly_info.entry_id
              - rcsb_entity_poly_info.entity_id
            - INDEX_NAME: search_2
              ATTRIBUTE_NAMES:
              - rcsb_entity_poly_info.entry_id
              - rcsb_entity_poly_info.entity_id
              - rcsb_entity_poly_info.comp_id
            - INDEX_NAME: search_3a
              ATTRIBUTE_NAMES:
              - rcsb_entity_container_identifiers.is_modified
            - INDEX_NAME: search_3b
              ATTRIBUTE_NAMES:
              - rcsb_entity_container_identifiers.is_heterogeneous
            - INDEX_NAME: search_4
              ATTRIBUTE_NAMES:
              - _entry_id
              - _entity_id
            - INDEX_NAME: search_5
              ATTRIBUTE_NAMES:
              - _entry_id
              - _comp_id
            - INDEX_NAME: replace
              ATTRIBUTE_NAMES:
              - _entry_id
        pdbx_core_assembly:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - rcsb_assembly_container_identifiers.entry_id
              - pdbx_struct_assembly.id
            - INDEX_NAME: search_1
              ATTRIBUTE_NAMES:
              - rcsb_assembly_container_identifiers.entry_id
              - rcsb_assembly_container_identifiers.assembly_id
            - INDEX_NAME: search_2
              ATTRIBUTE_NAMES:
              - _entry_id
              - _assembly_id
            - INDEX_NAME: replace
              ATTRIBUTE_NAMES:
              - _entry_id
        pdbx_core_entry:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - entry.id
            - INDEX_NAME: search_1
              ATTRIBUTE_NAMES:
              - rcsb_entry_container_identifiers.entry_id
              - rcsb_entry_container_identifiers.entity_ids
            - INDEX_NAME: search_2
              ATTRIBUTE_NAMES:
              - rcsb_entry_container_identifiers.entry_id
              - rcsb_entry_container_identifiers.polymer_entity_ids
            - INDEX_NAME: search_3
              ATTRIBUTE_NAMES:
              - rcsb_entry_container_identifiers.entry_id
              - rcsb_entry_container_identifiers.non_polymer_entity_ids
            - INDEX_NAME: search_4
              ATTRIBUTE_NAMES:
              - rcsb_entry_container_identifiers.entry_id
              - rcsb_entry_container_identifiers.assembly_ids
            - INDEX_NAME: search_5
              ATTRIBUTE_NAMES:
              - _entry_id
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - entry.id
        pdbx_core_entity_instance:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - rcsb_entity_instance_container_identifiers.entry_id
              - rcsb_entity_instance_container_identifiers.asym_id
            - INDEX_NAME: search_1
              ATTRIBUTE_NAMES:
              - rcsb_entity_instance_container_identifiers.entry_id
              - rcsb_entity_instance_container_identifiers.auth_asym_id
            - INDEX_NAME: search_2
              ATTRIBUTE_NAMES:
              - rcsb_entity_instance_container_identifiers.entry_id
              - rcsb_entity_instance_container_identifiers.entity_id
            - INDEX_NAME: search_3
              ATTRIBUTE_NAMES:
              - rcsb_entity_instance_container_identifiers.entry_id
              - rcsb_entity_instance_container_identifiers.comp_id
            - INDEX_NAME: search_4
              ATTRIBUTE_NAMES:
              - _entry_id
              - _asym_id
            - INDEX_NAME: search_5
              ATTRIBUTE_NAMES:
              - _entry_id
              - _auth_asym_id
            - INDEX_NAME: search_6
              ATTRIBUTE_NAMES:
              - _entry_id
              - _comp_id
            - INDEX_NAME: replace
              ATTRIBUTE_NAMES:
              - _entry_id
        bird:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - pdbx_reference_molecule.prd_id
        family:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - pdbx_reference_molecule_family.family_prd_id
        chem_comp:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - chem_comp.component_id
        chem_comp_core:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - chem_comp.id
        bird_chem_comp:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - chem_comp.component_id
        bird_chem_comp_core:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - chem_comp.id
        repository_holdings_update:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        repository_holdings_current:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        repository_holdings_unreleased:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        repository_holdings_prerelease:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        repository_holdings_removed:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        repository_holdings_removed_audit_authors:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        repository_holdings_superseded:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        repository_holdings_transferred:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        repository_holdings_insilico_models:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        cluster_members:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        cluster_provenance:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - software.name
        entity_members:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
        rcsb_data_exchange_status:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - update_id
              - database_name
              - object_name
        drugbank_core:
            - INDEX_NAME: primary
              ATTRIBUTE_NAMES:
              - drugbank_info.drugbank_id
    collection_subcategory_aggregates:
        cluster_members:
            - NAME: sequence_membership
              HAS_UNIT_CARDINALITY: False
        entity_members:
            - NAME: cluster_membership
              HAS_UNIT_CARDINALITY: False
        pdbx_core_entity:
            - NAME: taxonomy_lineage
              HAS_UNIT_CARDINALITY: False
            - NAME: rcsb_ec_lineage
              HAS_UNIT_CARDINALITY: False
    collection_retain_singleton:
        pdbx_core_entity_monomer: True



```