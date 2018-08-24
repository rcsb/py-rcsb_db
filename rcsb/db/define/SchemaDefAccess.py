##
# File:    SchemaDefAccess.py
# Author:  J. Westbrook
# Date:    15-Jum-2019
# Version: 0.001 Initial version
#
# Updates:
#  15-Jun-2018 jdw adapted from prior SchemaDefBase() with the static content definitions.
#
##
"""
Schema definition accessor implementation.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging

from rcsb.db.define.SchemaDefAccessBase import SchemaDefAccessBase

logger = logging.getLogger(__name__)


class SchemaDefAccess(SchemaDefAccessBase):
    """ Thin implementation of schema definition access class ...

    """

    def __init__(self, schemaDef, **kwargs):
        super(SchemaDefAccess, self).__init__(schemaDef, **kwargs)
