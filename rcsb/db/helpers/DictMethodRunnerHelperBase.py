##
# File:    DictMethodRunnerHelperBase.py
# Author:  J. Westbrook
# Date:    18-Aug-2018
# Version: 0.001 Initial version
#
# Updates:
##
"""
Foundation class to invoke dictioary methods implemented as helper methods

Placeholder for future developer -

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

logger = logging.getLogger(__name__)


class DictMethodRunnerHelperBase(object):
    """ Foundation class to invoke dictioary methods implemented as helper methods.

    """

    def __init__(self, **kwargs):
        self._raiseExceptions = kwargs.get('raiseExceptions', False)

    def doSomething(self, kind):
        """ Placeholder for future development
        """
        if self._raiseExceptions:
            raise NotImplementedError()
        return []
