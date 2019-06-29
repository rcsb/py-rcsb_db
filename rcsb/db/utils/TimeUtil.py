##
# File:    TimeUtil.py
# Author:  J. Westbrook
# Date:    23-Jun-2018
# Version: 0.001
#
# Updates:
##
"""
Convenience utilities to manipulate timestamps.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import datetime
import logging

import dateutil.parser
import pytz
from dateutil.tz import tzlocal  # pylint: disable=ungrouped-imports

logger = logging.getLogger(__name__)


class TimeUtil(object):
    def __init__(self, **kwargs):
        pass

    def getTimestamp(self, useUtc=True):
        """Return an a pseudo ISO 8601 format timestamp string (2018-07-11 12:33:22.874957+00:00) including timezone details.

        Args:
            useUtc (bool, optional): Use UTC time reference

        Returns:
            str: ISO 8601 format timestamp string
        """
        dt = datetime.datetime.utcnow().replace(tzinfo=pytz.utc) if useUtc else datetime.datetime.now().replace(tzinfo=tzlocal())
        return dt.isoformat(" ")

    def getWeekSignature(self, yyyy, mm, dd):
        """Return week in year signature (e.g. 2018_21) for the input date (year, month and day).

        Args:
            yyyy (int): year
            mm (int): month in year (1-12)
            dd (int): day in month (1-##)

        Returns:
            str: week in year signature (<yyyy>_<week_number>)
        """
        return datetime.date(yyyy, mm, dd).strftime("%Y_%V")

    def getCurrentWeekSignature(self):
        """Return the curren tweek in year signature (e.g. 2018_21).

        Returns:
            str: week in year signature (<yyyy>_<week_number>)
        """
        dt = datetime.date.today()
        return dt.strftime("%Y_%V")

    def getDateTimeObj(self, tS):
        """Return a datetime object corresponding to the input timestamp string.

        Args:
            tS (str): timestamp string (e.g. 2018-07-11 12:33:22.874957+00:00)

        Returns:
            object: datetime object
        """
        try:
            return dateutil.parser.parse(tS)
        except Exception as e:
            logger.exception("Failing with %r", str(e))
        return None
