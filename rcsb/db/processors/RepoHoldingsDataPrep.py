##
# File: RepoHoldingsDataPrep.py
# Author:  J. Westbrook
# Date:  12-Jul-2018
#
# Update:
# 16-Jul-2018 jdw adjust naming to current sandbox conventions.
#
##

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os

from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class RepoHoldingsDataPrep(object):
    """
    Consolidate legacy data describing repository content updates and repository entry status.

    """

    def __init__(self, **kwargs):
        self.__workPath = kwargs.get('workPath', None)
        self.__sandboxPath = kwargs.get('sandboxPath', None)
        #
        self.__mU = MarshalUtil(workPath=self.__workPath)
        #

    def getHoldingsUpdate(self, updateId, dirPath=None, **kwargs):
        """ Parse legacy lists defining the contents of the repository update

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing rcsb_repository_holdings_update
        """
        retL = []
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            updateTypeList = ['added', 'modified', 'obsolete']
            contentTypeList = ['entries', 'mr', 'cs', 'sf']
            contentNameD = {'entries': 'coordinates', 'mr': 'NMR restraints', 'cs': 'NMR chemical shifts', 'sf': 'structure factors'}
            #
            for updateType in updateTypeList:
                for contentType in contentTypeList:
                    fp = os.path.join(dirPath, 'update-lists', updateType + '-' + contentType)
                    entryIdL = self.__mU.doImport(fp, 'list')
                    #
                    for entryId in entryIdL:
                        uType = 'removed' if updateType == 'obsolete' else updateType
                        retL.append({'update_id': updateId, 'entryId': entryId, 'update_type': uType, 'repository_content_type': contentNameD[contentType]})
            return retL
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return retL

    def getHoldingsCurrent(self, updateId, dirPath=None, **kwargs):
        """ Parse legacy lists defining the current contents of the repository update

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing data for rcsb_repository_holdings_current
        """
        rD = {}
        retL = []
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            updateTypeList = ['all']
            contentTypeList = ['pdb', 'mr', 'cs', 'sf']
            contentNameD = {'pdb': 'coordinates', 'mr': 'NMR restraints', 'cs': 'NMR chemical shifts', 'sf': 'structure factors'}
            #
            for updateType in updateTypeList:
                for contentType in contentTypeList:
                    fp = os.path.join(dirPath, 'update-lists', updateType + '-' + contentType + '-list')
                    entryIdL = self.__mU.doImport(fp, 'list')
                    #
                    for entryId in entryIdL:
                        if entryId not in rD:
                            rD[entryId] = []
                        rD[entryId].append(contentNameD[contentType])
            for entryId in rD:
                retL.append({'update_id': updateId, 'entry_id': entryId, 'repository_content_types': rD[entryId]})
            return retL
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return retL

    def getHoldingsUnreleased(self, updateId, dirPath=None, **kwargs):
        """ Parse the legacy exchange status file containing details for unreleased entries:

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing data for rcsb_repository_holdings_unreleased
        """
        retL = []
        fields = []
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            fp = os.path.join(dirPath, 'status', 'status.txt')
            lines = self.__mU.doImport(fp, 'list')
            for line in lines:
                fields = line.split('\t')
                if len(fields) < 15:
                    continue
                d = {'update_id': updateId,
                     'entry_id': fields[1],
                     'status_code': fields[2],
                     'deposit_date': fields[3],
                     'deposit_date_coordinates': fields[4],
                     'deposit_date_structure_factors': fields[5],
                     'hold_date_struct_fact': fields[6],
                     'deposit_date_nmr_restraints': fields[7],
                     'hold_date_nmr_restraints': fields[8],
                     'release_date': fields[9],
                     'audit_author_list': fields[10],
                     'title': fields[11],
                     'author_prerelease_sequence': fields[12],
                     'hold_date_coordinates': fields[13],
                     'sg_project_name': fields[14],
                     'sg_project_abbreviation': fields[15]}
                retL.append({k: v for k, v in d.items() if v})
        except Exception as e:
            logger.error("Fields: %r" % fields)
            logger.exception("Failing with %s" % str(e))
        return retL

    def getHoldingsRemoved(self, updateId, dirPath=None, **kwargs):
        """ Parse the legacy exchange file containing details of removed entries:

            {
                "entryId": "125D",
                "obsoletedDate": "1998-04-15",
                "title": "SOLUTION STRUCTURE OF THE DNA-BINDING DOMAIN OF CD=2=-GAL4 FROM S. CEREVISIAE",
                "details": "",
                "depositionAuthors": [
                    "Baleja, J.D.",
                    "Wagner, G."
                ],
                "depositionDate": "1993-05-05",
                "releaseDate": "1994-01-31",
                "obsoletedBy": [
                    "1AW6"
                ]},

        Returns;
            (list) : list of dictionaries for rcsb_repository_holdings_removed
            (list) : list of dictionaries for rcsb_repository_holdings_removed_audit_authors
            (list) : list of dictionaries for rcsb_repository_holdings_superseded

        """
        # rcsb_repository_holdings_removed
        rL1 = []
        # rcsb_repository_holdings_removed_audit_authors
        rL2 = []
        # rcsb_repository_holdings_superseded
        rL3 = []
        #
        sD = {}
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            fp = os.path.join(dirPath, 'status', 'obsolete_entry.json_2')
            dD = self.__mU.doImport(fp, 'json')
            for d in dD:
                rbL = d['obsoletedBy'] if 'obsoletedBy' in d else []
                d1 = {'update_id': updateId, 'entry_id': d['entryId'], 'deposit_date': d['depositionDate'],
                      'remove_date': d['obsoletedDate'], 'release_date': d['releaseDate'], 'title': d['title'], 'details': d['details'],
                      'audit_authors': d['depositionAuthors'], 'id_codes_replaced_by': rbL}
                rL1.append({k: v for k, v in d1.items() if v})
                #
                for ii, author in enumerate(d['depositionAuthors']):
                    d2 = {'update_id': updateId, 'entry_id': d['entryId'], 'ordinal_id': ii + 1, 'audit_author': author}
                    rL2.append(d2)
                if 'obsoletedBy' in d:
                    for pdbId in d['obsoletedBy']:
                        if pdbId not in sD:
                            sD[pdbId] = []
                        sD[pdbId].append(d['entryId'])
            for pdbId in sD:
                d = {'update_id': updateId, 'entry_id': pdbId, 'id_codes_superseded': sD[pdbId]}
                rL3.append(d)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return rL1, rL2, rL3
