##
# File:    StatusHistorySchemaDef.py
# Author:  J. Westbrook
# Date:    4-Jan-2015
# Version: 0.001 Initial version
#
# Updates:
#  6-Jan-2014  jdw  Updated to working schema content --
##
"""
Database schema defintions for status history table within the da_internal collection.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import sys
import logging
logger = logging.getLogger(__name__)

from rcsb_db.schema.SchemaDefBase import SchemaDefBase


class StatusHistorySchemaDef(SchemaDefBase):

    """ A data class containing schema definitions for data processing status history.
    """
    _databaseName = "da_internal"
    _versionedDatabaseName = "da_internal_v5_0_1"
    _schemaDefDict = {
        "PDBX_DATABASE_STATUS_HISTORY": {
            "TABLE_ID": "PDBX_DATABASE_STATUS_HISTORY",
            "TABLE_NAME": "pdbx_database_status_history",
            "TABLE_TYPE": "transactional",
            "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "ENTRY_ID": "entry_id",
                "PDB_ID": "pdb_id",
                "DATE_BEGIN": "date_begin",
                "DATE_END": "date_end",
                "STATUS_CODE_BEGIN": "status_code_begin",
                "STATUS_CODE_END": "status_code_end",
                "ANNOTATOR": "annotator",
                "DETAILS": "details",
                "DELTA_DAYS": "delta_days",
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL": {"SQL_TYPE": "INT UNSIGNED", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "ENTRY_ID": {"SQL_TYPE": "CHAR", "WIDTH": 15, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 2},
                "PDB_ID": {"SQL_TYPE": "CHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
                "DATE_BEGIN": {"SQL_TYPE": "DATETIME", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 4},
                "DATE_END": {"SQL_TYPE": "DATETIME", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 5},
                "STATUS_CODE_BEGIN": {"SQL_TYPE": "VARCHAR", "WIDTH": 24, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 6},
                "STATUS_CODE_END": {"SQL_TYPE": "VARCHAR", "WIDTH": 24, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 7},
                "ANNOTATOR": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 8},
                "DETAILS": {"SQL_TYPE": "VARCHAR", "WIDTH": 80, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 9},
                "DELTA_DAYS": {"SQL_TYPE": "FLOAT", "WIDTH": 10, "PRECISION": 4, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 10},
            },
            'ATTRIBUTE_MAP': {
                'ORDINAL': ('pdbx_database_status_history', 'ordinal', None, None),
                "ENTRY_ID": ('pdbx_database_status_history', "entry_id", None, None),
                "PDB_ID": ('pdbx_database_status_history', "pdb_id", None, None),
                "DATE_BEGIN": ('pdbx_database_status_history', "date_begin", None, None),
                "DATE_END": ('pdbx_database_status_history', "date_end", None, None),
                "STATUS_CODE_BEGIN": ('pdbx_database_status_history', "status_code_begin", None, None),
                "STATUS_CODE_END": ('pdbx_database_status_history', "status_code_end", None, None),
                "ANNOTATOR": ('pdbx_database_status_history', "annotator", None, None),
                "DETAILS": ('pdbx_database_status_history', "details", None, None),
                "DELTA_DAYS": ('pdbx_database_status_history', "delta_days", None, None),
            },
            "INDICES": {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": ["ORDINAL", "ENTRY_ID"]},
                        "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["ENTRY_ID"]},
                        "i2": {"TYPE": "SEARCH", "ATTRIBUTES": ["ANNOTATOR"]},
                        "i3": {"TYPE": "SEARCH", "ATTRIBUTES": ["ENTRY_ID", "STATUS_CODE_BEGIN", "STATUS_CODE_END"]},
                        },
            'MAP_MERGE_INDICES': {'pdbx_database_status_history': {'ATTRIBUTES': ('ordinal',
                                                                                  'entry_id',
                                                                                  'pdb_id',
                                                                                  'status_code_begin', 'status_code_end'),
                                                                   'TYPE': 'EQUI-JOIN'}},
            'TABLE_DELETE_ATTRIBUTE': 'ENTRY_ID',
        },
        "PDBX_ARCHIVE_FILE_INVENTORY": {
            "TABLE_ID": "PDBX_ARCHIVE_FILE_INVENTORY",
            "TABLE_NAME": "pdbx_archive_file_inventory",
            "TABLE_TYPE": "transactional",
            "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "ENTRY_ID": "entry_id",
                "CONTENT_TYPE": "content_type",
                "PARTITION_NUMBER": "partition_number",
                "VERSION_NUMBER": "version_number",
                "FORMAT_TYPE": "format_type",
                "TIMESTAMP": "timestamp",
                "FILE_SIZE": "file_size"
            },
            "ATTRIBUTE_INFO": {
                "ORDINAL": {"SQL_TYPE": "INT UNSIGNED", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "ENTRY_ID": {"SQL_TYPE": "CHAR", "WIDTH": 15, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 2},
                "CONTENT_TYPE": {"SQL_TYPE": "CHAR", "WIDTH": 50, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
                "PARTITION_NUMBER": {"SQL_TYPE": "INT UNSIGNED", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 4},
                "VERSION_NUMBER": {"SQL_TYPE": "INT UNSIGNED", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 5},
                "FORMAT_TYPE": {"SQL_TYPE": "CHAR", "WIDTH": 15, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 6},
                "TIMESTAMP": {"SQL_TYPE": "DATETIME", "WIDTH": 30, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 7},
                "FILE_SIZE": {"SQL_TYPE": "FLOAT", "WIDTH": 12, "PRECISION": 4, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 8},
            },
            'ATTRIBUTE_MAP': {
                'ORDINAL': ('pdbx_archive_file_inventory', 'ordinal', None, None),
                "ENTRY_ID": ('pdbx_archive_file_inventory', "entry_id", None, None),
                "CONTENT_TYPE": ('pdbx_archive_file_inventory', "content_type", None, None),
                "PARTITION_NUMBER": ('pdbx_archive_file_inventory', "partition_number", None, None),
                "VERSION_NUMBER": ('pdbx_archive_file_inventory', "version_number", None, None),
                "FORMAT_TYPE": ('pdbx_archive_file_inventory', "format_type", None, None),
                "TIMESTAMP": ('pdbx_archive_file_inventory', "timestamp", None, None),
                "FILE_SIZE": ('pdbx_archive_file_inventory', "file_size", None, None),
            },
            "INDICES": {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": ["ORDINAL", "ENTRY_ID"]},
                        "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["ENTRY_ID"]},
                        "i2": {"TYPE": "SEARCH", "ATTRIBUTES": ["CONTENT_TYPE"]},
                        "i3": {"TYPE": "SEARCH", "ATTRIBUTES": ["VERSION_NUMBER"]},
                        "i3": {"TYPE": "SEARCH", "ATTRIBUTES": ["ENTRY_ID", "CONTENT_TYPE", "VERSION_NUMBER"]},
                        },
            'MAP_MERGE_INDICES': {'pdbx_database_status_history': {'ATTRIBUTES': ('ordinal', 'entry_id'),
                                                                   'TYPE': 'EQUI-JOIN'}},
            'TABLE_DELETE_ATTRIBUTE': 'ENTRY_ID',
        }
    }

    def __init__(self, convertNames=False, verbose=True):
        super(
            StatusHistorySchemaDef,
            self).__init__(
            databaseName=StatusHistorySchemaDef._databaseName,
            schemaDefDict=StatusHistorySchemaDef._schemaDefDict, convertNames=convertNames, versionedDatabaseName=StatusHistorySchemaDef._versionedDatabaseName,
            verbose=verbose)
        self.__verbose = verbose


if __name__ == "__main__":
    msd = StatusHistorySchemaDef()
    tableIdList = msd.getTableIdList()

    for tableId in tableIdList:
        aIdL = msd.getAttributeIdList(tableId)
        tObj = msd.getTable(tableId)
        attributeIdList = tObj.getAttributeIdList()
        attributeNameList = tObj.getAttributeNameList()
        logger.info("Ordered attribute Id   list %s\n" % (str(attributeIdList)))
        logger.info("Ordered attribute name list %s\n" % (str(attributeNameList)))
