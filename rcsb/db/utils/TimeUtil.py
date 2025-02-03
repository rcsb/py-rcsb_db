"""
Convenience utilities to manipulate timestamps.

**DEPRECATED** Use builtin `datetime` and `zoneinfo` instead.
"""

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo


class TimeUtil:
    """
    Utilities for working with date-times.
    """

    def getTimestamp(self, dt: Optional[datetime] = None, useUtc: bool = True) -> str:
        """
        Returns an RFC 3339 timestamp with a space separator and microsecond precision.

        Arguments:
            dt: If `None`, uses `datetime.now()`.
            useUtc: Use UTC instead of the local zone.

        Returns:
            An RFC 3339 timestamp with exactly 6 fractional second digits.
            A UTC offset of 0 will be written as `+00:00` (not `Z`), even if `useUtc`.
            Example: `2025-02-03 11:37:14.108402-07:00`.
        """
        if dt is None:
            dt = datetime.now()
        dt = dt.astimezone(ZoneInfo("Etc/UTC") if useUtc else None)
        return dt.isoformat(" ", "microseconds")

    def getWeekSignature(self, yyyy: int, mm: int, dd: int) -> str:
        """
        Returns a "week signature", `%Y_%V` (e.g. 2018_21), from year, month, and day.
        """
        return datetime(yyyy, mm, dd).strftime("%Y_%V")

    def getCurrentWeekSignature(self) -> str:
        """
        Returns the "week signature" per [getWeekSignature][] for `datetime.now()`.
        """
        return datetime.now().strftime("%Y_%V")

    def getDateTimeObj(self, tS: str) -> datetime:
        """
        Parses an RFC 3339 timestamp by calling `datetime.fromisoformat(tS)`.
        Note that `getDateTimeObj(ts).tzname()` returns a string in the format `UTC[+-]hh:mm[:ss]`,
        which are not IANA timezones.

        Raises:
            ValueError: If `fromisoformat` raises it or if `tS` lacks a UTC offset.
        """
        dt = datetime.fromisoformat(tS) # happily uses any date/time separator
        if not dt.tzinfo:
            msg = f"The timestamp must have a UTC offset."
            raise ValueError(msg)
        return dt
