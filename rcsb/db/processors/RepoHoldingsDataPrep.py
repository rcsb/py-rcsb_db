##
# File: RepoHoldingsDataPrep.py
# Author:  J. Westbrook
# Date:  12-Jul-2018
#
# Update:
# 16-Jul-2018 jdw adjust naming to current sandbox conventions.
# 30-Oct-2018 jdw naming to conform to dictioanry and schema conventions.
#  8-Oct-2018 jdw adjustments for greater schema compliance and uniformity
#                 provide alternative date assignment for JSON and mongo validation
#
##

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os

import dateutil.parser

from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class RepoHoldingsDataPrep(object):
    """
    Consolidate legacy data describing repository content updates and repository entry status.

    """

    def __init__(self, **kwargs):
        self.__workPath = kwargs.get('workPath', None)
        self.__sandboxPath = kwargs.get('sandboxPath', None)
        self.__filterType = kwargs.get('filterType', '')
        self.__assignDates = 'assign-dates' in self.__filterType
        #
        self.__mU = MarshalUtil(workPath=self.__workPath)
        #

    # TODO include all of the accessioning details for SAS and homology model structures.
    def getHoldingsTransferred(self, updateId, dirPath, **kwargs):
        """ Parse legacy lists defining the repository contents transferred to alternative repositories

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing data for rcsb_repository_holdings_transferred
        """
        retL = []
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            # fp = os.path.join(dirPath, 'status', 'transfer-ma.list')
            # entryIdL = self.__mU.doImport(fp, 'list')
            #
            return retL
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return retL

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
                    uD = {}
                    for entryId in entryIdL:
                        if entryId not in uD:
                            uD[entryId] = []
                        uD[entryId].append(contentNameD[contentType])
                for entryId in uD:
                    uType = 'removed' if updateType == 'obsolete' else updateType
                    retL.append({'update_id': updateId, 'entry_id': entryId, 'update_type': uType, 'repository_content_types': uD[entryId]})
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
                     'status_code': fields[2]
                     # 'sg_project_name': fields[14],
                     # 'sg_project_abbreviation_': fields[15]}
                     }
                if fields[11] and len(fields[11].strip()):
                    d['title'] = fields[11]
                if fields[10] and len(fields[10].strip()):
                    # d['audit_authors'] = fields[10].split(';'),
                    d['audit_authors'] = fields[10]
                if fields[12] and len(fields[12].strip()):
                    d['author_prerelease_sequence'] = str(fields[12]).strip()
                dTupL = [('deposit_date', 3),
                         ('deposit_date_coordinates', 4),
                         ('deposit_date_structure_factors', 5),
                         ('hold_date_struct_fact', 6),
                         ('deposit_date_nmr_restraints', 7),
                         ('hold_date_nmr_restraints', 8),
                         ('release_date', 9),
                         ('hold_date_coordinates', 13)]
                for dTup in dTupL:
                    fN = dTup[1]
                    if fields[fN] and len(fields[fN]) >= 4:
                        d[dTup[0]] = dateutil.parser.parse(fields[fN]) if self.__assignDates else fields[fN]
                #
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
                d1 = {'update_id': updateId,
                      'entry_id': d['entryId'],
                      'title': d['title'],
                      'details': d['details'],
                      'audit_authors': d['depositionAuthors']}
                if rbL:
                    d1['id_codes_replaced_by'] = rbL

                dTupL = [('deposit_date', 'depositionDate'),
                         ('remove_date', 'obsoletedDate'),
                         ('release_date', 'releaseDate')]
                for dTup in dTupL:
                    fN = dTup[1]
                    if d[fN] and len(d[fN]) > 4:
                        d1[dTup[0]] = dateutil.parser.parse(d[fN]) if self.__assignDates else d[fN]

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
                if len(sD[pdbId]) > 1:
                    rL3.append({'update_id': updateId, 'entry_id': pdbId, 'id_codes_superseded': sD[pdbId]})

            logger.debug("rl3 %r" % rL3)
            logger.debug("Computed data lengths  %d %d %d" % (len(rL1), len(rL2), len(rL3)))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return rL1, rL2, rL3
