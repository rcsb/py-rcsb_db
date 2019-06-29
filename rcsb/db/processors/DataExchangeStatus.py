##
# File: DataExchangeStatus.py
# Author:  J. Westbrook
# Date:  10-Jul-2018
#
# Update:
# 14-Jul-2018 jdw update docs and return datetime objects for timestamp strings.
# 14-Jul-2018 jdw return timestamps from set methods
##

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging

from rcsb.db.utils.TimeUtil import TimeUtil

logger = logging.getLogger(__name__)


class DataExchangeStatus(object):
    """
    Create status records for data exchange operations.

    For example,

    loop_
     _rcsb_data_exchange_status.update_id
     _rcsb_data_exchange_status.database
     _rcsb_data_exchange_status.object
     _rcsb_data_exchange_status.update_status_flag
     _rcsb_data_exchange_status.update_begin_timestamp
     _rcsb_data_exchange_status.update_end_timestamp
    2018_23 chem_comp_v5 chem_comp Y '2018-07-11 11:51:37.958508+00:00' '2018-07-11 11:55:03.966508+00:00'
    # ... abbreviated ...

    """

    def __init__(self, **kwargs):
        self.__startTimestamp = None
        self.__endTimestamp = None
        self.__updateId = "unset"
        self.__statusFlag = "N"
        self.__databaseName = "unset"
        self.__objectName = "unset"
        self.__tU = TimeUtil()
        self.__kwargs = kwargs

    def setObject(self, databaseName, objectName):
        """Set the object for current status record.

        Args:
            databaseName (str): database container name
            objectName (str): object name (collection/table) within database

        Returns:
            bool: True for success or False otherwise
        """
        try:
            self.__databaseName = databaseName
            self.__objectName = objectName
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def setStartTime(self, tS=None, useUtc=True):
        """Set the start time for the current exchange operation.

        Args:
            tS (str, optional): timestamp for the start of the update operation (default=current time)
            useUtc (bool, optional): Report times in UTC

        Returns:
            str: isoformat timestamp or None otherwise
        """
        try:
            self.__startTimestamp = tS if tS else self.__tU.getTimestamp(useUtc=useUtc)
            return self.__startTimestamp
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None

    def setEndTime(self, tS=None, useUtc=True):
        """Set the end time for the current exchange operation.

        Args:
            tS (str, optional): timestamp for the end of the update operation (default=current time)
            useUtc (bool, optional): Report times in UTC

        Returns:
            str: isoformat timestamp or None otherwise
        """
        try:
            self.__endTimestamp = tS if tS else self.__tU.getTimestamp(useUtc=useUtc)
            return self.__endTimestamp
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None

    def setStatus(self, updateId=None, successFlag="Y"):
        """Set the update identifier (yyyy_<week_in_year>) and success flag for the current exchange operation.

        Args:
            updateId (str, optional): Update identifier (default=yyyy_<week_in_year>)
            successFlag (str, optional): 'Y'/'N'

        Returns:
            bool: True for success or False otherwise
        """
        try:
            self.__statusFlag = successFlag
            self.__updateId = updateId if updateId else self.__tU.getCurrentWeekSignature()
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def getStatus(self, useTimeStrings=False):
        """Get the current data exchange status document.

        Returns:
            dict: Updated list of status records including the appended current record

        """
        try:
            if useTimeStrings:
                sD = {
                    "update_id": self.__updateId,
                    "database_name": self.__databaseName,
                    "object_name": self.__objectName,
                    "update_status_flag": self.__statusFlag,
                    "update_begin_timestamp": self.__startTimestamp,
                    "update_end_timestamp": self.__endTimestamp,
                }
            else:
                sD = {
                    "update_id": self.__updateId,
                    "database_name": self.__databaseName,
                    "object_name": self.__objectName,
                    "update_status_flag": self.__statusFlag,
                    "update_begin_timestamp": self.__tU.getDateTimeObj(self.__startTimestamp),
                    "update_end_timestamp": self.__tU.getDateTimeObj(self.__endTimestamp),
                }
            return sD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return {}
