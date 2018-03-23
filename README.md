## RCSB Python Database Utility Classes

### Introduction

This module contains a collection of utility classes used by the RDSB Protein Data Bank
for loading and accessing various PDB content using relational and document database
servers.  These tools provide one approach to loading data into the RCSB Exchange database
currently hosted in MongoDB.

### Installation

Download the library source software from the project repository:

```bash

git clone  https://github.com/rcsb/py-rcsb_db.git

```

Optionally, run test suite (Python versions 2.7 or 3.6) using setuptools or tox:

```bash
python setup.py test

or

tox
```

Installation is via the program [pip](https://pypi.python.org/pypi/pip).

```bash
pip install .
```

A convenience CLI `exdb_load_cli` is provided currently to support loading various PDB content types
as document collections into a MongoDB server.

```bash
exdb_load_cli --help

usage: exdb_load_cli [-h]  [--load_full] [--load_with_replacement]
                           [--load_chem_comp_ref] [--load_bird_chem_comp_ref]
                           [--load_bird_ref] [--load_bird_family_ref]
                           [--load_entry_data] [--config_path CONFIG_PATH]
                           [--config_name CONFIG_NAME] [--db_type DB_TYPE]
                           [--document_style DOCUMENT_STYLE] [--read_back_check]
                           [--load_file_list_path LOAD_FILE_LIST_PATH]
                           [--fail_file_list_path FAIL_FILE_LIST_PATH]
                           [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                           [--file_limit FILE_LIMIT] [--debug]

optional arguments:
  -h, --help            show this help message and exit
  --load_full           Fresh full load in a new tables/collections
  --load_with_replacement
                        Load with replacement in an existing table/collection
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
  --num_proc NUM_PROC   Number of processes to execute (default=2)
  --chunk_size CHUNK_SIZE
                        Number of files loaded per process
  --file_limit FILE_LIMIT
                        Load file limit for testing
  --debug               Turn on verbose logging
```

RCSB/PDB repository path details are stored as configuration options.  An example
configuration file is shown below.

```bash
# File: db-load-setup-example.cfg
# Function: Template configuration file for rcsb_db utilities
#
[DEFAULT]
#
BIRD_REPO_PATH=/net/data/components/prd-v3
BIRD_FAMILY_REPO_PATH=/net/data/components/family-v3
BIRD_CHEM_COMP_REPO_PATH=/net/data/components/prdcc-v3
CHEM_COMP_REPO_PATH=/net/data/components/ligand-dict-v3
RCSB_PDBX_SANBOX_PATH=/net/beta_data/mmcif-pdbx-load-v5.0
#
MONGO_DB_HOST=localhost
MONGO_DB_PORT=27017
MONGO_DB_USER=
MONGO_DB_PASSWORD=
#
```

