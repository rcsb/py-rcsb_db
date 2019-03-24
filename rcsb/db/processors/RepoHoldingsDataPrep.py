##
# File: RepoHoldingsDataPrep.py
# Author:  J. Westbrook
# Date:  12-Jul-2018
#
# Note:  This module uses well sandbox and repository defined file names
#        within the configuration defined RCSB exchange sandbox path.
#
# Update:
# 16-Jul-2018 jdw adjust naming to current sandbox conventions.
# 30-Oct-2018 jdw naming to conform to dictioanry and schema conventions.
#  8-Oct-2018 jdw adjustments for greater schema compliance and uniformity
#                 provide alternative date assignment for JSON and mongo validation
# 10-Oct-2018 jdw Add getHoldingsTransferred() for homology models transfered to Model Archive
# 28-Oct-2018 jdw update with semi-colon separated author lists in status and theoretical model files
# 25-Nov-2018 jdw add sequence/pdb_seq_prerelease.fasta and  _rcsb_repository_holdings_prerelease.seq_one_letter_code
# 27-Nov-2018 jdw update rcsb_repository_holdings_current for all entry content types
# 29-Nov-2018 jdw Add support for NMR restraint versions.
# 30-Nov-3018 jdw explicitly filter obsolete entries from current holdings
# 13-Dec-2018 jdw Adjust logic for reporting assembly format availibility
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

    def getHoldingsTransferred(self, updateId, dirPath=None, **kwargs):
        """ Parse legacy lists defining the repository contents transferred to alternative repositories

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing data for rcsb_repository_holdings_transferred
            list: List of dictionaries containing data for rcsb_repository_holdings_insilico_models

        Example input data:

        ma-czyyf : 262D - TITLE A THREE-DIMENSIONAL MODEL OF THE REV BINDING ELEMENT OF HIV- TITLE 2 1 DERIVED FROM ANALYSES OF IN VITRO SELECTED VARIANTS
        ma-cfqla : 163D - TITLE A THREE-DIMENSIONAL MODEL OF THE REV BINDING ELEMENT OF HIV- TITLE 2 1 DERIVED FROM ANALYSES OF IN VITRO SELECTED VARIANTS

        and -

        1DX2    REL 1999-12-16  2000-12-15  Tumour Targetting Human ...  Beiboer, S.H.W., Reurs, A., Roovers, R.C., Arends, J., Whitelegg, N.R.J., Rees, A.R., Hoogenboom, H.R.

        and -

        1APD    OBSLTE  1992-10-15      2APD
        1BU0    OBSLTE  1998-10-07      2BU0
        1CLJ    OBSLTE  1998-03-04      2CLJ
        1DU8    OBSLTE  2001-01-31      1GIE
        1I2J    OBSLTE  2001-01-06      1JA5

        """
        trsfL = []
        insL = []
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            fp = os.path.join(dirPath, 'status', 'theoretical_model_obsolete.tsv')
            lineL = self.__mU.doImport(fp, 'list')
            #
            obsDateD = {}
            obsIdD = {}
            for line in lineL:
                fields = line.split('\t')
                if len(fields) < 3:
                    continue
                entryId = str(fields[0]).strip().upper()
                obsDateD[entryId] = dateutil.parser.parse(fields[2]) if self.__assignDates else fields[2]
                if len(fields) > 3 and len(fields[3]) > 3:
                    obsIdD[entryId] = fields[3]
            logger.debug("Read %d obsolete insilico id codes" % len(obsDateD))
            # ---------  ---------  ---------  ---------  ---------  ---------  ---------
            fp = os.path.join(dirPath, 'status', 'model-archive-PDB-insilico-mapping.list')
            lineL = self.__mU.doImport(fp, 'list')
            #
            trD = {}
            for line in lineL:
                fields = line.split(':')
                if len(fields) < 2:
                    continue
                entryId = str(fields[1]).strip().upper()[:4]
                maId = str(fields[0]).strip()
                trD[entryId] = maId
            logger.debug("Read %d model archive id codes" % len(trD))
            #
            # ---------  ---------  ---------  ---------  ---------  ---------  ---------
            fp = os.path.join(dirPath, 'status', 'theoretical_model_v2.tsv')
            lineL = self.__mU.doImport(fp, 'list')
            #
            logger.debug("Read %d insilico id codes" % len(lineL))
            for line in lineL:
                fields = str(line).split('\t')
                if len(fields) < 6:
                    continue
                depDate = dateutil.parser.parse(fields[2]) if self.__assignDates else fields[2]
                relDate = None
                if len(fields[3]) >= 10 and not fields[3].startswith('0000'):
                    relDate = dateutil.parser.parse(fields[3]) if self.__assignDates else fields[3]

                statusCode = 'TRSF' if fields[1] == 'REL' else fields[1]

                entryId = str(fields[0]).upper()
                title = fields[4]
                #
                auditAuthors = [t.strip() for t in fields[5].split(';')]
                repId = None
                if entryId in trD:
                    repName = 'Model Archive'
                    repId = trD[entryId]

                #
                d = {'update_id': updateId,
                     'entry_id': entryId,
                     'status_code': statusCode,
                     'deposit_date': depDate,
                     'repository_content_types': ['coordinates'],
                     'title': title,
                     'audit_authors': auditAuthors}
                #
                if relDate:
                    d['release_date'] = relDate
                #
                if repId:
                    d['remote_accession_code'] = repId
                    d['remote_repository_name'] = repName
                if statusCode == 'TRSF':
                    trsfL.append(d)
                #
                #
                d = {'update_id': updateId,
                     'entry_id': entryId,
                     'status_code': statusCode,
                     'deposit_date': depDate,
                     'title': title,
                     'audit_authors': auditAuthors}
                #
                if relDate:
                    d['release_date'] = relDate
                #
                if entryId in obsDateD:
                    d['remove_date'] = relDate
                #
                if entryId in obsIdD:
                    d['id_codes_replaced_by'] = [obsIdD[entryId]]
                #
                insL.append(d)
            #
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        return trsfL, insL

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
                        entryId = entryId.strip().upper()
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
            tD = {}
            for updateType in updateTypeList:
                for contentType in contentTypeList:
                    fp = os.path.join(dirPath, 'update-lists', updateType + '-' + contentType + '-list')
                    entryIdL = self.__mU.doImport(fp, 'list')
                    #
                    for entryId in entryIdL:
                        entryId = entryId.strip().upper()
                        if entryId not in tD:
                            tD[entryId] = {}
                        tD[entryId][contentNameD[contentType]] = True
            #
            fp = os.path.join(dirPath, 'status', 'biounit_file_list.tsv')
            lines = self.__mU.doImport(fp, 'list')
            assemD = {}
            for line in lines:
                fields = line.split('\t')
                entryId = fields[0].strip().upper()
                assemId = fields[1].strip()
                if entryId not in assemD:
                    assemD[entryId] = []
                assemD[entryId].append(assemId)
            #
            #
            fp = os.path.join(dirPath, 'status', 'pdb_bundle_index_list.tsv')
            bundleIdList = self.__mU.doImport(fp, 'list')
            bundleD = {}
            for entryId in bundleIdList:
                bundleD[entryId.strip().upper()] = True
            #
            fp = os.path.join(dirPath, 'status', 'validation_report_list.tsv')
            vList = self.__mU.doImport(fp, 'list')
            valD = {}
            for entryId in vList:
                valD[entryId.strip().upper()] = True
            #
            #
            fp = os.path.join(dirPath, 'status', 'entries_without_polymers.tsv')
            pList = self.__mU.doImport(fp, 'list')
            pD = {}
            for entryId in pList:
                pD[entryId.strip().upper()] = False
            #
            #
            fp = os.path.join(dirPath, 'status', 'nmr_restraints_v2_list.tsv')
            nmrV2List = self.__mU.doImport(fp, 'list')
            nmrV2D = {}
            for entryId in nmrV2List:
                nmrV2D[entryId.strip().upper()] = False
            #
            fp = os.path.join(dirPath, 'status', 'edmaps.json')
            qD = self.__mU.doImport(fp, 'json')
            edD = {}
            for entryId in qD:
                edD[entryId.upper()] = qD[entryId]
            #
            fp = os.path.join(dirPath, 'status', 'obsolete_entry.json_2')
            oL = self.__mU.doImport(fp, 'json')
            obsD = {}
            for d in oL:
                obsD[d['entryId'].upper()] = True
            logger.info("Removed entry length %d" % len(obsD))
            #
            #
            # Revise content types bundles and assemblies
            #
            for entryId, dD in tD.items():
                if entryId in obsD:
                    continue
                rD[entryId] = []
                if 'coordinates' in dD and entryId in bundleD:
                    rD[entryId].append('entry PDB bundle')
                    rD[entryId].append('entry mmCIF')
                    rD[entryId].append('entry PDBML')
                else:
                    rD[entryId].append('entry PDB')
                    rD[entryId].append('entry mmCIF')
                    rD[entryId].append('entry PDBML')
                if entryId in assemD:
                    if entryId in bundleD:
                        rD[entryId].append('assembly mmCIF')
                    else:
                        rD[entryId].append('assembly PDB')
                #
                for cType in dD:
                    if cType not in ['coordinates', 'NMR restraints']:
                        rD[entryId].append(cType)
                    if cType == 'NMR restraints':
                        rD[entryId].append('NMR restraints V1')

                if entryId in nmrV2D:
                    rD[entryId].append('NMR restraints V2')
                #
                if entryId in valD:
                    rD[entryId].append('validation report')
                if entryId in edD:
                    rD[entryId].append('2fo-fc Map')
                    rD[entryId].append('fo-fc Map')
                    rD[entryId].append('Map Coefficients')
                if entryId not in pD:
                    rD[entryId].append('FASTA sequence')
            #
            for entryId in rD:
                if entryId in assemD:
                    retL.append({'update_id': updateId, 'entry_id': entryId, 'assembly_ids': assemD[entryId], 'repository_content_types': rD[entryId]})
                else:
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
            #
            fp = os.path.join(dirPath, 'status', 'status_v2.txt')
            lines = self.__mU.doImport(fp, 'list')
            for line in lines:
                fields = line.split('\t')
                if len(fields) < 15:
                    continue
                entryId = fields[1]
                d = {'update_id': updateId,
                     'entry_id': entryId,
                     'status_code': fields[2]
                     # 'sg_project_name': fields[14],
                     # 'sg_project_abbreviation_': fields[15]}
                     }
                if fields[11] and len(fields[11].strip()):
                    d['title'] = fields[11]
                if fields[10] and len(fields[10].strip()):
                    d['audit_authors'] = [t.strip() for t in fields[10].split(';')]
                    # d['audit_authors'] = fields[10]
                if fields[12] and len(fields[12].strip()):
                    d['author_prerelease_sequence_status'] = str(fields[12]).strip().replace('REALEASE', 'RELEASE')
                dTupL = [('deposit_date', 3),
                         ('deposit_date_coordinates', 4),
                         ('deposit_date_structure_factors', 5),
                         ('hold_date_structure_factors', 6),
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

    def getHoldingsPrerelease(self, updateId, dirPath=None, **kwargs):
        """ Parse the legacy exchange status file containing prerelease sequence data.

        Args:
            updateId (str): update identifier (e.g. 2018_32)
            dirPath (str): directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing data for rcsb_repository_holdings_prerelease

        """
        retL = []
        fields = []
        dirPath = dirPath if dirPath else self.__sandboxPath
        try:
            # Get prerelease sequence data
            fp = os.path.join(dirPath, 'sequence', 'pdb_seq_prerelease.fasta')
            sD = self.__mU.doImport(fp, 'fasta', commentStyle="prerelease")
            seqD = {}
            for sid in sD:
                fields = sid.split('_')
                entryId = str(fields[0]).upper()
                if entryId not in seqD:
                    seqD[entryId] = []
                seqD[entryId].append(sD[sid]['sequence'])
            logger.debug("Loaded prerelease sequences for %d entries" % len(seqD))
            #
            for entryId, seqL in seqD.items():
                d = {'update_id': updateId, 'entry_id': entryId, 'seq_one_letter_code': seqL}
                logger.debug("Adding prerelease sequences for %s" % entryId)
                retL.append({k: v for k, v in d.items() if v})
        except Exception as e:
            logger.error("Fields: %r" % fields)
            logger.exception("Failing with %s" % str(e))

        return retL
