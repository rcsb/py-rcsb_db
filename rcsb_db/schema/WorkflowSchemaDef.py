##
# File:    WorkflowSchemaDef.py
# Author:  J. Westbrook
# Date:    8-Apr-2014
# Version: 0.001 Initial version
#
# Updates:
#    13-April-2015 jdw   added application tables deposition and user_data
#    17-April-2015 jdw   added tables for accession codes
#    29-April-2015 jdw   remove "DATE_BEGIN_PROCESSING"  "DATE_END_PROCESSING" attributes from deposition table
#    10-Nov-2015   jdw   add EM specific attributes to the deposition table
#
##
"""
Database schema defintions for tables used to manage the workflow engine processes.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import logging
logger = logging.getLogger(__name__)
from rcsb_db.schema.SchemaDefBase import SchemaDefBase


class WorkflowSchemaDef(SchemaDefBase):
    """ A data class containing schema definitions for workflow status and tracking tables.
    """
    _databaseName = "status"
    _versionedDatabaseName = "status_v5_0_1"
    _schemaDefDict = {
        "COMMUNICATION": {
            'SCHEMA_ID': "COMMUNICATION",
            'SCHEMA_NAME': "communication",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL_ID": "ordinal",
                "SENDER": "sender",
                "RECEIVER": "receiver",
                "DEP_SET_ID": "dep_set_id",
                "WF_CLASS_ID": "wf_class_id",
                "WF_INST_ID": "wf_inst_id",
                "WF_CLASS_FILE": "wf_class_file",
                "COMMAND": "command",
                "STATUS": "status",
                "ACTUAL_TIMESTAMP": "actual_timestamp",
                "PARENT_DEP_SET_ID": "parent_dep_set_id",
                "PARENT_WF_CLASS_ID": "parent_wf_class_id",
                "PARENT_WF_INST_ID": "parent_wf_inst_id",
                "DATA_VERSION": "data_version",
                "HOST": "host",
                "ACTIVITY": "activity"
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL_ID": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "SENDER": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "RECEIVER": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
                "DEP_SET_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 30, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "WF_CLASS_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 16, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
                "WF_INST_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 6},
                "WF_CLASS_FILE": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 7},
                "COMMAND": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 8},
                "STATUS": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 9},
                "ACTUAL_TIMESTAMP": {'APP_TYPE': "DECIMAL", "WIDTH": 20, "PRECISION": 8, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 10},
                "PARENT_DEP_SET_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 30, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 11},
                "PARENT_WF_CLASS_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 16, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 12},
                "PARENT_WF_INST_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 13},
                "DATA_VERSION": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 14},
                "HOST": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 15},
                "ACTIVITY": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 16}
            },
            "INDICES": {"i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["PARENT_WF_INST_ID"]},
                        "i2": {"TYPE": "SEARCH", "ATTRIBUTES": ["DEP_SET_ID"]}
                        }
        },
        "ENGINE_MONITORING": {
            'SCHEMA_ID': "ENGINE_MONITORING",
            'SCHEMA_NAME': "engine_monitoring",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL_ID": "ordinal",
                "HOSTNAME": "hostname",
                "TOTAL_PHYSICAL_MEM": "total_physical_mem",
                "TOTAL_VIRTUAL_MEM": "total_virtual_mem",
                "PHYSICAL_MEM_USAGE": "physical_mem_usage",
                "VIRTUAL_MEM_USAGE": "virtual_mem_usage",
                "CPU_USAGE": "cpu_usage",
                "CPU_NUMBER": "cpu_number",
                "IDS_SET": "ids_set",
                "STATUS_TIMESTAMP": "status_timestamp",
                "CACHED": "cached",
                "BUFFERS": "buffers",
                "SWAP_TOTAL": "swap_total",
                "SWAP_USED": "swap_used",
                "SWAP_FREE": "swap_free"
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL_ID": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "HOSTNAME": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "TOTAL_PHYSICAL_MEM": {'APP_TYPE': "DECIMAL", "WIDTH": 10, "PRECISION": 1, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
                "TOTAL_VIRTUAL_MEM": {'APP_TYPE': "DECIMAL", "WIDTH": 12, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "PHYSICAL_MEM_USAGE": {'APP_TYPE': "DECIMAL", "WIDTH": 10, "PRECISION": 1, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 5},
                "VIRTUAL_MEM_USAGE": {'APP_TYPE': "DECIMAL", "WIDTH": 10, "PRECISION": 1, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 6},
                "CPU_USAGE": {'APP_TYPE': "DECIMAL", "WIDTH": 10, "PRECISION": 3, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 7},
                "CPU_NUMBER": {'APP_TYPE': "INT", "WIDTH": 30, "PRECISION": 1, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 8},
                "IDS_SET": {'APP_TYPE': "VARCHAR", "WIDTH": 255, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 9},
                "STATUS_TIMESTAMP": {'APP_TYPE': "DECIMAL", "WIDTH": 20, "PRECISION": 8, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 10},
                "CACHED": {'APP_TYPE': "DECIMAL", "WIDTH": 12, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 11},
                "BUFFERS": {'APP_TYPE': "DECIMAL", "WIDTH": 12, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 12},
                "SWAP_TOTAL": {'APP_TYPE': "DECIMAL", "WIDTH": 12, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 13},
                "SWAP_USED": {'APP_TYPE': "DECIMAL", "WIDTH": 12, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 14},
                "SWAP_FREE": {'APP_TYPE': "DECIMAL", "WIDTH": 12, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 15}
            },
            "INDICES": {"i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["HOSTNAME"]},
                        }
        },

        "WF_TASK": {
            'SCHEMA_ID': "WF_TASK",
            'SCHEMA_NAME': "wf_task",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL_ID": "ordinal",
                "WF_TASK_ID": "wf_task_id",
                "WF_INST_ID": "wf_inst_id",
                "WF_CLASS_ID": "wf_class_id",
                "DEP_SET_ID": "dep_set_id",
                "TASK_NAME": "task_name",
                "TASK_STATUS": "task_status",
                "STATUS_TIMESTAMP": "status_timestamp",
                "TASK_TYPE": "task_type",
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL_ID": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "WF_TASK_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "WF_INST_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
                "WF_CLASS_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 16, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "DEP_SET_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 30, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
                "TASK_NAME": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 6},
                "TASK_STATUS": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 7},
                "STATUS_TIMESTAMP": {'APP_TYPE': "DECIMAL", "WIDTH": 20, "PRECISION": 8, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 8},
                "TASK_TYPE": {'APP_TYPE': "VARCHAR", "WIDTH": 25, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 9}
            },
            "INDICES": {
                "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_TASK_ID"]},
                "i2": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_INST_ID"]},
                "i3": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_CLASS_ID"]},
                "i4": {"TYPE": "SEARCH", "ATTRIBUTES": ["DEP_SET_ID"]},
            }
        },
        "WF_INSTANCE": {
            'SCHEMA_ID': "WF_INSTANCE",
            'SCHEMA_NAME': "wf_instance",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL_ID": "ordinal",
                "WF_INST_ID": "wf_inst_id",
                "WF_CLASS_ID": "wf_class_id",
                "DEP_SET_ID": "dep_set_id",
                "OWNER": "owner",
                "INST_STATUS": "inst_status",
                "STATUS_TIMESTAMP": "status_timestamp"
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL_ID": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "WF_INST_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "WF_CLASS_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 16, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
                "DEP_SET_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 30, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "OWNER": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
                "INST_STATUS": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 6},
                "STATUS_TIMESTAMP": {'APP_TYPE': "DECIMAL", "WIDTH": 20, "PRECISION": 8, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 7}
            },
            "INDICES": {
                "i2": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_INST_ID"]},
                "i3": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_CLASS_ID"]},
                "i5": {"TYPE": "SEARCH", "ATTRIBUTES": ["DEP_SET_ID"]}
            }
        },
        "WF_INSTANCE_LAST": {
            'SCHEMA_ID': "WF_INSTANCE_LAST",
            'SCHEMA_NAME': "wf_instance_last",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL_ID": "ordinal",
                "WF_INST_ID": "wf_inst_id",
                "WF_CLASS_ID": "wf_class_id",
                "DEP_SET_ID": "dep_set_id",
                "OWNER": "owner",
                "INST_STATUS": "inst_status",
                "STATUS_TIMESTAMP": "status_timestamp"
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL_ID": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "WF_INST_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "WF_CLASS_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 16, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
                "DEP_SET_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 30, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "OWNER": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
                "INST_STATUS": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 6},
                "STATUS_TIMESTAMP": {'APP_TYPE': "DECIMAL", "WIDTH": 20, "PRECISION": 8, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 7}
            },
            "INDICES": {
                "i2": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_INST_ID"]},
                "i3": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_CLASS_ID"]},
                "i5": {"TYPE": "SEARCH", "ATTRIBUTES": ["DEP_SET_ID"]}
            }
        },
        "WF_CLASS_DICT": {
            'SCHEMA_ID': "WF_CLASS_DICT",
            'SCHEMA_NAME': "wf_class_dict",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL_ID": "ordinal",
                "WF_CLASS_ID": "wf_class_id",
                "WF_CLASS_NAME": "wf_class_name",
                "TITLE": "title",
                "AUTHOR": "author",
                "VERSION": "version",
                "CLASS_FILE": "class_file"
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL_ID": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "WF_CLASS_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 16, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 2},
                "WF_CLASS_NAME": {'APP_TYPE': "VARCHAR", "WIDTH": 100, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
                "TITLE": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "AUTHOR": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
                "VERSION": {'APP_TYPE': "VARCHAR", "WIDTH": 8, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 6},
                "CLASS_FILE": {'APP_TYPE': "VARCHAR", "WIDTH": 100, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 7}
            },
            "INDICES": {"i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_CLASS_ID"]},
                        }
        },
        "WF_REFERENCE": {
            'SCHEMA_ID': "WF_REFERENCE",
            'SCHEMA_NAME': "wf_reference",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL_ID": "ordinal",
                "DEP_SET_ID": "dep_set_id",
                "WF_INST_ID": "wf_inst_id",
                "WF_TASK_ID": "wf_task_id",
                "WF_CLASS_ID": "wf_class_id",
                "HASH_ID": "hash_id",
                "VALUE": "value"
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL_ID": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "DEP_SET_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 30, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "WF_INST_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 2},
                "WF_TASK_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 2},
                "WF_CLASS_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 16, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 2},
                "HASH_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 20, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "VALUE": {'APP_TYPE': "VARCHAR", "WIDTH": 20, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2}
            },
            "INDICES": {
                "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_TASK_ID"]},
                "i2": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_INST_ID"]},
                "i3": {"TYPE": "SEARCH", "ATTRIBUTES": ["WF_CLASS_ID"]},
                "i4": {"TYPE": "SEARCH", "ATTRIBUTES": ["HASH_ID"]},
                "i5": {"TYPE": "SEARCH", "ATTRIBUTES": ["DEP_SET_ID"]},
            }

        },
        "DEPOSITION": {
            'SCHEMA_ID': "DEPOSITION",
            'SCHEMA_NAME': "deposition",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "DEP_SET_ID": "dep_set_id",
                "PDB_ID": "pdb_id",
                "INITIAL_DEPOSITION_DATE": "initial_deposition_date",
                "ANNOTATOR_INITIALS": "annotator_initials",
                "DEPOSIT_SITE": "deposit_site",
                "PROCESS_SITE": "process_site",
                "STATUS_CODE": "status_code",
                "AUTHOR_RELEASE_STATUS_CODE": "author_release_status_code",
                "TITLE": "title",
                "TITLE_EMDB": "title_emdb",
                "AUTHOR_LIST": "author_list",
                "AUTHOR_LIST_EMDB": "author_list_emdb",
                "EXP_METHOD": "exp_method",
                "STATUS_CODE_EXP": "status_code_exp",
                "SG_CENTER": "SG_center",
                "DEPPW": "depPW",
                "NOTIFY": "notify",
                #                "DATE_BEGIN_PROCESSING": "date_begin_processing",
                #                "DATE_END_PROCESSING": "date_end_processing",
                "EMAIL": "email",
                "LOCKING": "locking",
                "COUNTRY": "country",
                "NMOLECULE": "nmolecule",
                "EMDB_ID": "emdb_id",
                "BMRB_ID": "bmrb_id",
                "STATUS_CODE_EMDB": "status_code_emdb",
                "DEP_AUTHOR_RELEASE_STATUS_CODE_EMDB": "dep_author_release_status_code_emdb",
                "STATUS_CODE_BMRB": "status_code_bmrb",
                "STATUS_CODE_OTHER": "status_code_other"
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "DEP_SET_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 30, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 2},
                "PDB_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 4, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
                "INITIAL_DEPOSITION_DATE": {'APP_TYPE': "DATE", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "ANNOTATOR_INITIALS": {'APP_TYPE': "VARCHAR", "WIDTH": 12, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
                "DEPOSIT_SITE": {'APP_TYPE': "VARCHAR", "WIDTH": 8, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 6},
                "PROCESS_SITE": {'APP_TYPE': "VARCHAR", "WIDTH": 8, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 7},
                "STATUS_CODE": {'APP_TYPE': "VARCHAR", "WIDTH": 5, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 8},
                "AUTHOR_RELEASE_STATUS_CODE": {'APP_TYPE': "VARCHAR", "WIDTH": 5, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 9},
                "TITLE_EMDB": {'APP_TYPE': "VARCHAR", "WIDTH": 400, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 10},
                "TITLE": {'APP_TYPE': "VARCHAR", "WIDTH": 400, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 11},
                "AUTHOR_LIST": {'APP_TYPE': "VARCHAR", "WIDTH": 500, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 12},
                "AUTHOR_LIST_EMDB": {'APP_TYPE': "VARCHAR", "WIDTH": 500, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 13},
                "EXP_METHOD": {'APP_TYPE': "VARCHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 14},
                "STATUS_CODE_EXP": {'APP_TYPE': "VARCHAR", "WIDTH": 4, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 15},
                "SG_CENTER": {'APP_TYPE': "VARCHAR", "WIDTH": 40, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 16},
                "DEPPW": {'APP_TYPE': "VARCHAR", "WIDTH": 16, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 17},
                "NOTIFY": {'APP_TYPE': "VARCHAR", "WIDTH": 8, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 18},
                #                "DATE_BEGIN_PROCESSING": {'APP_TYPE': "DATE", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 17},
                #                "DATE_END_PROCESSING": {'APP_TYPE': "DATE", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 18},
                "EMAIL": {'APP_TYPE': "VARCHAR", "WIDTH": 64, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 19},
                "LOCKING": {'APP_TYPE': "VARCHAR", "WIDTH": 8, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 20},
                "COUNTRY": {'APP_TYPE': "VARCHAR", "WIDTH": 32, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 21},
                "NMOLECULE": {'APP_TYPE': "INT", "WIDTH": 11, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 22},
                "EMDB_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 9, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 23},
                "BMRB_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 6, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 24},
                "STATUS_CODE_EMDB": {'APP_TYPE': "VARCHAR", "WIDTH": 5, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 25},
                "DEP_AUTHOR_RELEASE_STATUS_CODE_EMDB": {'APP_TYPE': "VARCHAR", "WIDTH": 5, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 26},
                "STATUS_CODE_BMRB": {'APP_TYPE': "VARCHAR", "WIDTH": 5, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 27},
                "STATUS_CODE_OTHER": {'APP_TYPE': "VARCHAR", "WIDTH": 5, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 28},

            },
            "INDICES": {
                "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["DEP_SET_ID"]},
            }
        },
        "USER_DATA": {
            'SCHEMA_ID': "USER_DATA",
            'SCHEMA_NAME': "user_data",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "DEP_SET_ID": "dep_set_id",
                "EMAIL": "email",
                "LAST_NAME": "last_name",
                "ROLE": "role",
                "COUNTRY": "country",
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "DEP_SET_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 30, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 2},
                "EMAIL": {'APP_TYPE': "VARCHAR", "WIDTH": 64, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
                "LAST_NAME": {'APP_TYPE': "VARCHAR", "WIDTH": 64, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
                "ROLE": {'APP_TYPE': "VARCHAR", "WIDTH": 8, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
                "COUNTRY": {'APP_TYPE': "VARCHAR", "WIDTH": 32, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 6},
            },
            "INDICES": {
                "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["DEP_SET_ID"]},
            }
        },

        "PDB_ACCESSION": {
            'SCHEMA_ID': "PDB_ACCESSION",
            'SCHEMA_NAME': "pdbID",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "PDB_ID": "pdb_id",
                "USED": "used",
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "PDB_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 6, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "USED": {'APP_TYPE': "VARCHAR", "WIDTH": 2, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
            },
            "INDICES": {
                "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["PDB_ID"]},
            }
        },

        "BMRB_ACCESSION": {
            'SCHEMA_ID': "BMRB_ACCESSION",
            'SCHEMA_NAME': "bmrbID",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "BMRB_ID": "bmrb_id",
                "USED": "used",
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "BMRB_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 6, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "USED": {'APP_TYPE': "VARCHAR", "WIDTH": 2, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
            },
            "INDICES": {
                "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["BMRB_ID"]},
            }
        },
        "EMDB_ACCESSION": {
            'SCHEMA_ID': "EMDB_ACCESSION",
            'SCHEMA_NAME': "emdbID",
            'SCHEMA_TYPE': "transactional",
            "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "EMDB_ID": "emdb_id",
                "USED": "used",
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL": {'APP_TYPE': "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "EMDB_ID": {'APP_TYPE': "VARCHAR", "WIDTH": 6, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "USED": {'APP_TYPE': "VARCHAR", "WIDTH": 2, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
            },
            "INDICES": {
                "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["EMDB_ID"]},
            }
        }
    }

    def __init__(self, convertNames=False, verbose=True):
        super(WorkflowSchemaDef, self).__init__(databaseName=WorkflowSchemaDef._databaseName,
                                                schemaDefDict=WorkflowSchemaDef._schemaDefDict, convertNames=convertNames, versionedDatabaseName=WorkflowSchemaDef._versionedDatabaseName, verbose=verbose)
        self.__verbose = verbose

if __name__ == "__main__":
    wfsd = WorkflowSchemaDef()
    tableIdList = wfsd.getTableIdList()

    for tableId in tableIdList:
        aIdL = wfsd.getAttributeIdList(tableId)
        tObj = wfsd.getTable(tableId)
        attributeIdList = tObj.getAttributeIdList()
        attributeNameList = tObj.getAttributeNameList()
        logger.info("Ordered attribute Id   list %s\n" % (str(attributeIdList)))
        logger.info("Ordered attribute name list %s\n" % (str(attributeNameList)))
