##
# File:    UpdateTimeUtil.py
# Author:  J. Westbrook
# Date:    24-Mar-2018
# Version: 0.001
#
# Updates:
##
"""
Time utilties related updated to PDB schedules.

Phase I: Every Saturday from 3:00 UTC, for every new entry, the following will be provided from the wwPDB website: sequence(s) (amino acid or nucleotide) for each distinct polymer and, where appropriate, the InChI string(s) for each distinct ligand and the crystallization pH value(s).

Phase II: Every Wednesday from 00:00 UTC, all new and modified data entries will be updated at each of the wwPDB FTP sites.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import os
import datetime
import dateutil

import logging
logger = logging.getLogger(__name__)


class UpdateTimeUtil(object):
    """
    """

    def __init__(self, verbose):
        self.__verbose=verbose
        from datetime import date
        yearBegin = date(date.today().year, 1, 1)
        yearEnd = date(date.today().year, 12, 31)


    def firstUpdate(self, yyyy):
        yearBegin = date(date.today().year, 1, 1)
        yearEnd = date(date.today().year, 12, 31)
        #
        NOW = datetime.datetime.now()
        TODAY = datetime.date.today()
        TODAY+dateutil.relativedelta(weekday=TH(+1))