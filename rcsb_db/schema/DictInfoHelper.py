##
# File:    DictInfoHelper.py
# Author:  J. Westbrook
# Date:    18-May-2018
# Version: 0.001 Initial version
#
# Updates:
#  23-May-2018  jdw revise dependencies for helper function.  Change method api's
##
"""
Supplements dictionary information as required for schema production.

Single source of additional dictionary semantic content.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
logger = logging.getLogger(__name__)

from rcsb_db.schema.DictInfoHelperBase import DictInfoHelperBase


class DictInfoHelper(DictInfoHelperBase):
    """ Supplements dictionary information as required for schema production.

    """

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (below)
            dictPath (str, optional): path to the current dictioonary text
            dictSubset (str, optional): name of dictionary content subset

        """
        super(DictInfoHelper, self).__init__(**kwargs)
        self._dictSubset = kwargs.get('dictSubset', None)
        self._dictPath = kwargs.get("dictPath", None)

    def getDictPath(self):
        return self._dictPath

    def getDictSubSet(self):
        return self._dictSubset

    def getCardinalityKeyItem(self, dictSubset):
        """ Identify the parent item for the dictionary subset that can be used to
            identify child categories with unity cardinality.   That is, logically containing
            a single data row in any instance.

        """
        if dictSubset in ['bird']:
            return ('pdbx_reference_molecule', 'prd_id')
        elif dictSubset in ['bird_family']:
            return ('pdbx_reference_molecule_family', 'family_prd_id')
        elif dictSubset in ['chem_comp']:
            return ('chem_comp', 'id')
        elif dictSubset in ['pdbx']:
            return ('entry', 'id')
        #
        return ('', '')

    def getTypeCodes(self, kind):
        """
        """
        if kind in ['iterable']:
            return ['ucode-alphanum-csv', 'id_list']

        else:
            return []

    def getQueryStrings(self, kind):
        if kind in ['iterable']:
            return ['comma separate']

        else:
            return []
