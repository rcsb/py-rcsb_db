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

usage: exdb_load_cli [-h] [--full] [--replace] [--load_chem_comp_ref]
                     [--load_bird_chem_comp_ref] [--load_bird_ref]
                     [--load_bird_family_ref] [--load_entry_data]
                     [--config_path CONFIG_PATH] [--config_name CONFIG_NAME]
                     [--db_type DB_TYPE] [--document_style DOCUMENT_STYLE]
                     [--read_back_check]
                     [--load_file_list_path LOAD_FILE_LIST_PATH]
                     [--fail_file_list_path FAIL_FILE_LIST_PATH]
                     [--save_file_list_path SAVE_FILE_LIST_PATH]
                     [--num_proc NUM_PROC] [--chunk_size CHUNK_SIZE]
                     [--file_limit FILE_LIMIT] [--debug] [--mock]

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
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for testing
```


RCSB/PDB repository path details are stored as configuration options.  An example
configuration file is shown below which assumes a local MongoDB instance and
using the mock repositories installed with this software module.

```bash
# File: db-load-setup-example.cfg
# Function: Template configuration file for rcsb_db utilities using MOCK repository paths
#
[DEFAULT]
#
BIRD_REPO_PATH=MOCK_BIRD_REPO
BIRD_FAMILY_REPO_PATH=MOCK_BIRD_FAMILY_REPO
BIRD_CHEM_COMP_REPO_PATH=MOCK_BIRD_CC_REPO
CHEM_COMP_REPO_PATH=MOCK_CHEM_COMP_REPO
RCSB_PDBX_SANBOX_PATH=MOCK_PDBX_SANDBOX
#
MONGO_DB_HOST=localhost
MONGO_DB_PORT=27017
#
MONGO_DB_USER=
MONGO_DB_PASSWORD=
MONGO_DB_ADMIN_DB_NAME=
#
MONGO_DB_WRITE_CONCERN=majority
MONGO_DB_READ_CONCERN=majority
MONGO_DB_READ_PREFERENCE=nearest
MONGO_DB_WRITE_TO_JOURNAL=True

```

If you are working in the source repository, then you can run the CLI commands in the following manner.
These examples load data in the mock repositories in source distribution assuming you have a local
default installation of MongoDb (no user/pw assigned).

To run the command-line interface `exdb_load_cli` outside of the source distribution, you will need to
create a configuration file with the appropriate path and credential details.

For instance, to perform a fresh/full load of all of the chemical component definitions in the mock repository:
```
cd rcsb_db/exec
python DbLoadExec.py  --mock --full  --load_chem_comp_ref  \
                      --config_path ../data/dbload-setup-example.cfg \
                      --config_name DEFAULT
                      --fail_file_list_path failed-cc-path-list.txt \
                      --read_back_check
```


The following illustrates, a full of the mock structure data followed by are reload with replacement of
this same data.
```
python DbLoadExec.py --mock --full  --load_entry_data \
                     --config_path ../data/dbload-setup-example.cfg
                     --config_name DEFAULT
                     --save_file_list_path  LATEST_PDBX_LOAD_LIST.txt
                     --fail_file_list_path failed-entry-path-list.txt

python DbLoadExec.py  --mock --replace  --load_entry_data \
                      --config_path ../data/dbload-setup-example.cfg
                      --config_name DEFAULT
                      --load_file_list_path  LATEST_PDBX_LOAD_LIST.txt
                      --fail_file_list_path failed-entry-path-list.txt
```


