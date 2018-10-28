##
# File:    DictMethodRunnerHelper.py
# Author:  J. Westbrook
# Date:    18-Aug-2018
# Version: 0.001 Initial version
#
# Updates:
#  4-Sep-2018 jdw add methods to construct entry and entity identier categories.
# 10-Sep-2018 jdw add method for citation author aggregation
# 22-Sep-2018 jdw add method assignAssemblyCandidates()
# 27-Oct-2018 jdw add method consolidateAccessionDetails()
#
##
"""
This helper class implements external method references in the RCSB dictionary extension.

All data accessors and structures here refer to dictionary category and attribute names.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import datetime
import logging

from mmcif.api.DataCategory import DataCategory

from rcsb.db.helpers.DictMethodRunnerHelperBase import DictMethodRunnerHelperBase

logger = logging.getLogger(__name__)


class DictMethodRunnerHelper(DictMethodRunnerHelperBase):
    """ Helper class implements external method references in the RCSB dictionary extension.

    """

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (dict)  Placeholder for future key-value arguments

        """
        super(DictMethodRunnerHelper, self).__init__(**kwargs)
        self._thing = kwargs.get("thing", None)
        logger.debug("Dictionary method helper init")
        #

    def echo(self, msg):
        logger.info(msg)

    def setDatablockId(self, dataContainer, catName, atName, **kwargs):
        try:
            val = dataContainer.getName()
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=[atName]))
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            rc = cObj.getRowCount()
            numRows = rc if rc else 1
            for ii in range(numRows):
                cObj.setValue(val, atName, ii)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def setLoadDateTime(self, dataContainer, catName, atName, **kwargs):
        try:
            val = dataContainer.getProp('load_date')
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=[atName]))
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            rc = cObj.getRowCount()
            numRows = rc if rc else 1
            for ii in range(numRows):
                cObj.setValue(val, atName, ii)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def setLocator(self, dataContainer, catName, atName, **kwargs):
        try:
            val = dataContainer.getProp('locator')
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=[atName]))
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            rc = cObj.getRowCount()
            numRows = rc if rc else 1
            for ii in range(numRows):
                cObj.setValue(val, atName, ii)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def setRowIndex(self, dataContainer, catName, atName, **kwargs):
        try:
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=[atName]))
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            #
            rc = cObj.getRowCount()
            numRows = rc if rc else 1
            for ii, iRow in enumerate(range(numRows), 1):
                # Note - we set the integer value as a string  -
                cObj.setValue(str(ii), atName, iRow)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def aggregateCitationAuthors(self, dataContainer, catName, atName, **kwargs):
        try:
            if not dataContainer.exists(catName) or not dataContainer.exists('citation_author'):
                return False
            #
            cObj = dataContainer.getObj(catName)
            if not cObj.hasAttribute(atName):
                cObj.appendAttribute(atName)
            citIdL = cObj.getAttributeValueList('id')
            #
            tObj = dataContainer.getObj('citation_author')
            #
            citIdL = list(set(citIdL))
            tD = {}
            for ii, citId in enumerate(citIdL):
                tD[citId] = tObj.selectValuesWhere('name', citId, 'citation_id')
            for ii in range(cObj.getRowCount()):
                citId = cObj.getValue('id', ii)
                cObj.setValue(';'.join(tD[citId]), atName, ii)
            return True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def buildContainerEntryIds(self, dataContainer, catName, **kwargs):
        """
        Build:

        loop_
        _rcsb_entry_container_identifiers.entry_id
        _rcsb_entry_container_identifiers.entity_ids
        _rcsb_entry_container_identifiers.polymer_entity_ids_polymer
        _rcsb_entry_container_identifiers.non-polymer_entity_ids
        _rcsb_entry_container_identifiers.assembly_ids
        ...
        """
        try:
            if not dataContainer.exists('entry'):
                return False
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=['entry_id', 'entity_ids', 'polymer_entity_ids',
                                                                              'non-polymer_entity_ids', 'assembly_ids']))
            #
            cObj = dataContainer.getObj(catName)

            tObj = dataContainer.getObj('entry')
            entryId = tObj.getValue('id', 0)
            cObj.setValue(entryId, 'entry_id', 0)
            #
            tObj = dataContainer.getObj('entity')
            entityIdL = tObj.getAttributeValueList('id')
            cObj.setValue(','.join(entityIdL), 'entity_ids', 0)
            #
            #
            pIdL = tObj.selectValuesWhere('id', 'polymer', 'type')
            tV = ','.join(pIdL) if pIdL else '?'
            cObj.setValue(tV, 'polymer_entity_ids', 0)

            npIdL = tObj.selectValuesWhere('id', 'non-polymer', 'type')
            tV = ','.join(npIdL) if npIdL else '?'
            cObj.setValue(tV, 'non-polymer_entity_ids', 0)
            #
            tObj = dataContainer.getObj('pdbx_struct_assembly')
            assemblyIdL = tObj.getAttributeValueList('id') if tObj else []
            tV = ','.join(assemblyIdL) if assemblyIdL else '?'
            cObj.setValue(tV, 'assembly_ids', 0)

            return True
        except Exception as e:
            logger.exception("For %s failing with %s" % (catName, str(e)))
        return False

    def buildContainerEntityIds(self, dataContainer, catName, **kwargs):
        """
        Build:

        loop_
        _rcsb_entity_container_identifiers.entry_id
        _rcsb_entity_container_identifiers.entity_id
        #
        _rcsb_entity_container_identifiers.asym_ids
        _rcsb_entity_container_identifiers.auth_asym_ids

        ...
        """
        try:
            if not (dataContainer.exists('entry') and dataContainer.exists('entity')):
                return False
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=['entry_id', 'entity_id', 'asym_ids', 'auth_asym_ids']))
            #
            cObj = dataContainer.getObj(catName)

            psObj = dataContainer.getObj('pdbx_poly_seq_scheme')
            npsObj = dataContainer.getObj('pdbx_nonpoly_scheme')
            #
            tObj = dataContainer.getObj('entry')
            entryId = tObj.getValue('id', 0)
            cObj.setValue(entryId, 'entry_id', 0)
            #
            tObj = dataContainer.getObj('entity')
            entityIdL = tObj.getAttributeValueList('id')
            for ii, entityId in enumerate(entityIdL):
                cObj.setValue(entryId, 'entry_id', ii)
                cObj.setValue(entityId, 'entity_id', ii)
                eType = tObj.getValue('type', ii)
                if eType == 'polymer':
                    asymIdL = psObj.selectValuesWhere('asym_id', entityId, 'entity_id')
                    authAsymIdL = psObj.selectValuesWhere('pdb_strand_id', entityId, 'entity_id')
                else:
                    asymIdL = npsObj.selectValuesWhere('asym_id', entityId, 'entity_id')
                    authAsymIdL = npsObj.selectValuesWhere('pdb_strand_id', entityId, 'entity_id')
                cObj.setValue(','.join(list(set(asymIdL))).strip(), 'asym_ids', ii)
                cObj.setValue(','.join(list(set(authAsymIdL))).strip(), 'auth_asym_ids', ii)
            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s" % (catName, str(e)))
        return False

    def buildContainerAssemblyIds(self, dataContainer, catName, **kwargs):
        """
        Build:

        loop_
        _rcsb_assembly_container_identifiers.entry_id
        _rcsb_assembly_container_identifiers.assembly_id
        ...
        """
        try:
            if not (dataContainer.exists('entry') and dataContainer.exists('pdbx_struct_assembly')):
                return False
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=['entry_id', 'assembly_id']))
            #
            cObj = dataContainer.getObj(catName)

            tObj = dataContainer.getObj('entry')
            entryId = tObj.getValue('id', 0)
            cObj.setValue(entryId, 'entry_id', 0)
            #
            tObj = dataContainer.getObj('pdbx_struct_assembly')
            assemblyIdL = tObj.getAttributeValueList('id')
            for ii, assemblyId in enumerate(assemblyIdL):
                cObj.setValue(entryId, 'entry_id', ii)
                cObj.setValue(assemblyId, 'assembly_id', ii)
            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s" % (catName, str(e)))
        return False

    def addDepositedAssembly(self, dataContainer, catName, **kwargs):
        """ Add the deposited coordinates as a separate assembly labeled as 'deposited'.

        """
        try:
            if not dataContainer.exists('struct_asym'):
                return False
            if not dataContainer.exists('pdbx_struct_assembly'):
                dataContainer.append(DataCategory('pdbx_struct_assembly', attributeNameList=['id', 'details', 'method_details',
                                                                                             'oligomeric_details', 'oligomeric_count',
                                                                                             'rcsb_details', 'rcsb_candidate_assembly']))
            if not dataContainer.exists('pdbx_struct_assembly_gen'):
                dataContainer.append(DataCategory('pdbx_struct_assembly_gen', attributeNameList=['assembly_id', 'oper_expression', 'asym_id_list', 'ordinal']))

            if not dataContainer.exists('pdbx_struct_oper_list'):
                row = ['1', 'identity operation', '1_555', 'x, y, z', '1.0000000000', '0.0000000000', '0.0000000000',
                       '0.0000000000', '0.0000000000', '1.0000000000', '0.0000000000', '0.0000000000',
                       '0.0000000000', '0.0000000000', '1.0000000000', '0.0000000000']
                atList = ['id', 'type', 'name', 'symmetry_operation', 'matrix[1][1]', 'matrix[1][2]', 'matrix[1][3]',
                          'vector[1]', 'matrix[2][1]', 'matrix[2][2]', 'matrix[2][3]', 'vector[2]',
                          'matrix[3][1]', 'matrix[3][2]', 'matrix[3][3]', 'vector[3]']
                dataContainer.append(DataCategory('pdbx_struct_oper_list', attributeNameList=atList, rowList=[row]))

            #
            logger.debug("Add deposited assembly for %s" % dataContainer.getName())
            cObj = dataContainer.getObj('struct_asym')
            asymIdL = cObj.getAttributeValueList('id')
            logger.debug("AsymIdL %r" % asymIdL)
            #
            # Ordinal is added by subsequent attribure-level method.
            tObj = dataContainer.getObj('pdbx_struct_assembly_gen')
            rowIdx = tObj.getRowCount()
            tObj.setValue('deposited', 'assembly_id', rowIdx)
            tObj.setValue('1', 'oper_expression', rowIdx)
            tObj.setValue(','.join(asymIdL), 'asym_id_list', rowIdx)
            #
            tObj = dataContainer.getObj('pdbx_struct_assembly')
            rowIdx = tObj.getRowCount()
            tObj.setValue('deposited', 'id', rowIdx)
            tObj.setValue('deposited_coordinates', 'details', rowIdx)
            logger.debug("Full row is %r" % tObj.getRow(rowIdx))
            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s" % (catName, str(e)))
        return False

    def filterAssemblyDetails(self, dataContainer, catName, atName, **kwargs):
        """ Filter _pdbx_struct_assembly.details -> _pdbx_struct_assembly.rcsb_details
            with a more limited vocabulary -

                'author_and_software_defined_assembly'
                'author_defined_assembly'
                'software_defined_assembly'

        """
        mD = {'author_and_software_defined_assembly': 'author_and_software_defined_assembly',
              'author_defined_assembly': 'author_defined_assembly',
              'complete icosahedral assembly': 'author_and_software_defined_assembly',
              'complete point assembly': 'author_and_software_defined_assembly',
              'crystal asymmetric unit': 'software_defined_assembly',
              'crystal asymmetric unit, crystal frame': 'software_defined_assembly',
              'details': 'software_defined_assembly',
              'helical asymmetric unit': 'software_defined_assembly',
              'helical asymmetric unit, std helical frame': 'software_defined_assembly',
              'icosahedral 23 hexamer': 'software_defined_assembly',
              'icosahedral asymmetric unit': 'software_defined_assembly',
              'icosahedral asymmetric unit, std point frame': 'software_defined_assembly',
              'icosahedral pentamer': 'software_defined_assembly',
              'pentasymmetron capsid unit': 'software_defined_assembly',
              'point asymmetric unit': 'software_defined_assembly',
              'point asymmetric unit, std point frame': 'software_defined_assembly',
              'representative helical assembly': 'author_and_software_defined_assembly',
              'software_defined_assembly': 'software_defined_assembly',
              'trisymmetron capsid unit': 'software_defined_assembly',
              'deposited_coordinates': 'software_defined_assembly'}
        #
        try:
            if not dataContainer.exists('pdbx_struct_assembly'):
                return False

            logger.debug("Filter assembly details for %s" % dataContainer.getName())
            tObj = dataContainer.getObj('pdbx_struct_assembly')
            if not tObj.hasAttribute(atName):
                tObj.appendAttribute(atName)
            #
            for iRow in range(tObj.getRowCount()):
                details = tObj.getValue('details', iRow)
                if details in mD:
                    tObj.setValue(mD[details], 'rcsb_details', iRow)
                else:
                    tObj.setValue('software_defined_assembly', 'rcsb_details', iRow)
                logger.debug("Full row is %r" % tObj.getRow(iRow))
            return True
        except Exception as e:
            logger.exception("For %s %s failing with %s" % (catName, atName, str(e)))
        return False

    def assignAssemblyCandidates(self, dataContainer, catName, atName, **kwargs):
        """ Flag candidate biological assemblies as 'author_defined_assembly' ad author_and_software_defined_assembly'

        """
        mD = {'author_and_software_defined_assembly': 'author_and_software_defined_assembly',
              'author_defined_assembly': 'author_defined_assembly',
              'complete icosahedral assembly': 'author_and_software_defined_assembly',
              'complete point assembly': 'author_and_software_defined_assembly',
              'crystal asymmetric unit': 'software_defined_assembly',
              'crystal asymmetric unit, crystal frame': 'software_defined_assembly',
              'details': 'software_defined_assembly',
              'helical asymmetric unit': 'software_defined_assembly',
              'helical asymmetric unit, std helical frame': 'software_defined_assembly',
              'icosahedral 23 hexamer': 'software_defined_assembly',
              'icosahedral asymmetric unit': 'software_defined_assembly',
              'icosahedral asymmetric unit, std point frame': 'software_defined_assembly',
              'icosahedral pentamer': 'software_defined_assembly',
              'pentasymmetron capsid unit': 'software_defined_assembly',
              'point asymmetric unit': 'software_defined_assembly',
              'point asymmetric unit, std point frame': 'software_defined_assembly',
              'representative helical assembly': 'author_and_software_defined_assembly',
              'software_defined_assembly': 'software_defined_assembly',
              'trisymmetron capsid unit': 'software_defined_assembly',
              'deposited_coordinates': 'software_defined_assembly'}
        #
        try:
            if not dataContainer.exists('pdbx_struct_assembly'):
                return False

            tObj = dataContainer.getObj('pdbx_struct_assembly')
            if not tObj.hasAttribute(atName):
                tObj.appendAttribute(atName)
            #
            for iRow in range(tObj.getRowCount()):
                details = tObj.getValue('details', iRow)
                if details in mD and mD[details] in ['author_and_software_defined_assembly', 'author_defined_assembly']:
                    tObj.setValue('Y', 'rcsb_candidate_assembly', iRow)
                else:
                    tObj.setValue('N', 'rcsb_candidate_assembly', iRow)
                logger.debug("Full row is %r" % tObj.getRow(iRow))
            return True
        except Exception as e:
            logger.exception("For %s %s failing with %s" % (catName, atName, str(e)))
        return False

    def __getAttribList(self, sObj, atTupL):
        atL = []
        atSL = []
        if sObj:
            for (atS, at) in atTupL:
                if sObj.hasAttribute(atS):
                    atL.append(at)
                    atSL.append(atS)
        return atSL, atL

    def filterSourceOrganismDetails(self, dataContainer, catName, **kwargs):
        """  Select relevant source and host organism details from primary data categories.

        Build:
            loop_
            _rcsb_entity_source_organism.entity_id
            _rcsb_entity_source_organism.pdbx_src_id
            _rcsb_entity_source_organism.source_type
            _rcsb_entity_source_organism.scientific_name
            _rcsb_entity_source_organism.common_name
            _rcsb_entity_source_organism.ncbi_taxonomy_id
            _rcsb_entity_source_organism.provenance_code
            _rcsb_entity_source_organism.beg_seq_num
            _rcsb_entity_source_organism.end_seq_num
            1 1 natural 'Homo sapiens' human 9606  'pdb-primary-data' 1 202
            # ... abbreviated


            loop_
            _rcsb_entity_host_organism.entity_id
            _rcsb_entity_host_organism.pdbx_src_id
            _rcsb_entity_host_organism.scientific_name
            _rcsb_entity_host_organism.common_name
            _rcsb_entity_host_organism.ncbi_taxonomy_id
            _rcsb_entity_host_organism.provenance_code
            _rcsb_entity_host_organism.beg_seq_num
            _rcsb_entity_host_organism.end_seq_num
            1 1 'Escherichia coli' 'E. coli' 562  'pdb-primary-data' 1 102
            # ... abbreviated

            And two related items -

            _entity.rcsb_multiple_source_flag
            _entity.rcsb_source_part_count

        """
        hostCatName = 'rcsb_entity_host_organism'
        try:
            logger.debug("Starting with  %r %r" % (dataContainer.getName(), catName))
            if catName == hostCatName:
                logger.debug("Skipping method for %r %r" % (dataContainer.getName(), catName))
                return True
            #
            # if there is no source information then exit
            if not (dataContainer.exists('entity_src_gen') or dataContainer.exists('entity_src_nat') or dataContainer.exists('pdbx_entity_src_syn')):
                return False
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=['entity_id',
                                                                              'pdbx_src_id',
                                                                              'source_type',
                                                                              'scientific_name',
                                                                              'common_name',
                                                                              'ncbi_taxonomy_id',
                                                                              'beg_seq_num',
                                                                              'end_seq_num',
                                                                              'provenance_code']))
            #
            if not dataContainer.exists(hostCatName):
                dataContainer.append(DataCategory(hostCatName, attributeNameList=['entity_id',
                                                                                  'pdbx_src_id',
                                                                                  'scientific_name',
                                                                                  'common_name',
                                                                                  'ncbi_taxonomy_id',
                                                                                  'beg_seq_num',
                                                                                  'end_seq_num',
                                                                                  'provenance_code']))
            cObj = dataContainer.getObj(catName)
            hObj = dataContainer.getObj(hostCatName)
            #
            s1Obj = dataContainer.getObj('entity_src_gen')
            atHTupL = [('entity_id', 'entity_id'),
                       ('pdbx_host_org_scientific_name', 'scientific_name'),
                       ('pdbx_host_org_common_name', 'common_name'),
                       ('pdbx_host_org_ncbi_taxonomy_id', 'ncbi_taxonomy_id'),
                       ('pdbx_src_id', 'pdbx_src_id'),
                       ('pdbx_beg_seq_num', 'beg_seq_num'),
                       ('pdbx_end_seq_num', 'end_seq_num')]
            atHSL, atHL = self.__getAttribList(s1Obj, atHTupL)
            #
            at1TupL = [('entity_id', 'entity_id'),
                       ('pdbx_gene_src_scientific_name', 'scientific_name'),
                       ('gene_src_common_name', 'common_name'),
                       ('pdbx_gene_src_ncbi_taxonomy_id', 'ncbi_taxonomy_id'),
                       ('pdbx_src_id', 'pdbx_src_id'),
                       ('pdbx_beg_seq_num', 'beg_seq_num'),
                       ('pdbx_end_seq_num', 'end_seq_num')]
            at1SL, at1L = self.__getAttribList(s1Obj, at1TupL)
            #
            s2Obj = dataContainer.getObj('entity_src_nat')
            at2TupL = [('entity_id', 'entity_id'),
                       ('pdbx_organism_scientific', 'scientific_name'),
                       ('nat_common_name', 'common_name'),
                       ('pdbx_ncbi_taxonomy_id', 'ncbi_taxonomy_id'),
                       ('pdbx_src_id', 'pdbx_src_id'),
                       ('pdbx_beg_seq_num', 'beg_seq_num'),
                       ('pdbx_end_seq_num', 'end_seq_num')
                       ]
            at2SL, at2L = self.__getAttribList(s2Obj, at2TupL)
            #
            s3Obj = dataContainer.getObj('pdbx_entity_src_syn')
            at3TupL = [('entity_id', 'entity_id'),
                       ('organism_scientific', 'scientific_name'),
                       ('organism_common_name', 'common_name'),
                       ('ncbi_taxonomy_id', 'ncbi_taxonomy_id'),
                       ('pdbx_src_id', 'pdbx_src_id'),
                       ('beg_seq_num', 'beg_seq_num'),
                       ('end_seq_num', 'end_seq_num')]
            at3SL, at3L = self.__getAttribList(s3Obj, at3TupL)
            #
            eObj = dataContainer.getObj('entity')
            entityIdL = eObj.getAttributeValueList('id')
            pCode = 'pdb-primary-data'
            #
            partCountD = {}
            srcL = []
            hostL = []
            for entityId in entityIdL:
                partCountD[entityId] = 1
                eL = []
                tf = False
                if s1Obj:
                    sType = 'genetically engineered'
                    vL = s1Obj.selectValueListWhere(at1SL, entityId, 'entity_id')
                    if vL:
                        for v in vL:
                            eL.append((entityId, sType, at1L, v))
                        logger.debug("%r entity %r - %r" % (sType, entityId, vL))
                        partCountD[entityId] = len(eL)
                        srcL.extend(eL)
                        tf = True
                    #
                    vL = s1Obj.selectValueListWhere(atHSL, entityId, 'entity_id')
                    if vL:
                        for v in vL:
                            hostL.append((entityId, sType, atHL, v))
                        logger.debug("%r entity %r - %r" % (sType, entityId, vL))
                    if tf:
                        continue

                if s2Obj:
                    sType = 'natural'
                    vL = s2Obj.selectValueListWhere(at2SL, entityId, 'entity_id')
                    if vL:
                        for v in vL:
                            eL.append((entityId, sType, at2L, v))
                        logger.debug("%r entity %r - %r" % (sType, entityId, vL))
                        partCountD[entityId] = len(eL)
                        srcL.extend(eL)
                        continue

                if s3Obj:
                    sType = 'synthetic'
                    vL = s3Obj.selectValueListWhere(at3SL, entityId, 'entity_id')
                    if vL:
                        for v in vL:
                            eL.append((entityId, sType, at3L, v))
                        logger.debug("%r entity %r - %r" % (sType, entityId, vL))
                        partCountD[entityId] = len(eL)
                        srcL.extend(eL)
                        continue

            iRow = 0
            for (entityId, sType, atL, v) in srcL:
                cObj.setValue(sType, 'source_type', iRow)
                cObj.setValue(pCode, 'provenance_code', iRow)
                for ii, at in enumerate(atL):
                    cObj.setValue(v[ii], at, iRow)
                logger.debug("%r entity %r - UPDATED %r %r" % (sType, entityId, atL, v))
                iRow += 1
            #
            iRow = 0
            for (entityId, sType, atL, v) in hostL:
                hObj.setValue(pCode, 'provenance_code', iRow)
                for ii, at in enumerate(atL):
                    hObj.setValue(v[ii], at, iRow)
                logger.debug("%r entity %r - UPDATED %r %r" % (sType, entityId, atL, v))
                iRow += 1
            #
            # Update entity attributes
            #    _entity.rcsb_multiple_source_flag
            #    _entity.rcsb_source_part_count
            for atName in ['rcsb_source_part_count', 'rcsb_multiple_source_flag']:
                if not eObj.hasAttribute(atName):
                    eObj.appendAttribute(atName)
            #
            for ii in range(eObj.getRowCount()):
                cFlag = 'Y' if partCountD[entityId] > 1 else 'N'
                entityId = eObj.getValue('id', ii)
                eObj.setValue(partCountD[entityId], 'rcsb_source_part_count', ii)
                eObj.setValue(cFlag, 'rcsb_multiple_source_flag', ii)

            return True
        except Exception as e:
            logger.exception("For %s failing with %s" % (catName, str(e)))
        return False

    def consolidateAccessionDetails(self, dataContainer, catName, **kwargs):
        """  Consolidate accession details into a single object.

             _rcsb_accession_info.entry_id                1ABC
             _rcsb_accession_info.status_code             REL
             _rcsb_accession_info.deposit_date            2018-01-11
             _rcsb_accession_info.initial_release_date    2018-03-23
             _rcsb_accession_info.major_revision          1
             _rcsb_accession_info.minor_revision          2
             _rcsb_accession_info.revision_date           2018-10-25


            #

            _pdbx_database_status.entry_id                        3OQP
            _pdbx_database_status.deposit_site                    RCSB
            _pdbx_database_status.process_site                    RCSB
            _pdbx_database_status.recvd_initial_deposition_date   2010-09-03
            _pdbx_database_status.status_code                     REL
            _pdbx_database_status.status_code_sf                  REL
            _pdbx_database_status.status_code_mr                  ?
            _pdbx_database_status.status_code_cs                  ?
            _pdbx_database_status.pdb_format_compatible           Y
            _pdbx_database_status.methods_development_category    ?
            _pdbx_database_status.SG_entry                        Y
            #
            loop_
            _pdbx_audit_revision_history.ordinal
            _pdbx_audit_revision_history.data_content_type
            _pdbx_audit_revision_history.major_revision
            _pdbx_audit_revision_history.minor_revision
            _pdbx_audit_revision_history.revision_date
            1 'Structure model' 1 0 2010-10-13
            2 'Structure model' 1 1 2011-07-13
            3 'Structure model' 1 2 2011-07-20
            4 'Structure model' 1 3 2014-11-12
            5 'Structure model' 1 4 2017-10-25
            #



        """
        ##
        try:
            logger.debug("Starting with  %r %r" % (dataContainer.getName(), catName))
            #
            # if there is incomplete accessioninformation then exit
            if not (dataContainer.exists('pdbx_database_status') or dataContainer.exists('pdbx_audit_revision_history')):
                return False
            # Create the new target category
            if not dataContainer.exists(catName):
                dataContainer.append(DataCategory(catName, attributeNameList=['entry_id',
                                                                              'status_code',
                                                                              'deposit_date',
                                                                              'initial_release_date',
                                                                              'major_revision',
                                                                              'minor_revision',
                                                                              'revision_date']))
            #
            cObj = dataContainer.getObj(catName)
            #
            tObj = dataContainer.getObj('pdbx_database_status')
            entryId = tObj.getValue('entry_id', 0)
            statusCode = tObj.getValue('status_code', 0)
            depositDate = tObj.getValue('recvd_initial_deposition_date', 0)
            #
            cObj.setValue(entryId, 'entry_id', 0)
            cObj.setValue(statusCode, 'status_code', 0)
            cObj.setValue(depositDate, 'deposit_date', 0)
            #
            tObj = dataContainer.getObj('pdbx_audit_revision_history')
            nRows = tObj.getRowCount()
            # Assuming the default sorting order from the release module -
            releaseDate = tObj.getValue('revision_date', 0)
            minorRevision = tObj.getValue('minor_revision', nRows - 1)
            majorRevision = tObj.getValue('major_revision', nRows - 1)
            revisionDate = tObj.getValue('revision_date', nRows - 1)
            cObj.setValue(releaseDate, 'initial_release_date', 0)
            cObj.setValue(minorRevision, 'minor_revision', 0)
            cObj.setValue(majorRevision, 'major_revision', 0)
            cObj.setValue(revisionDate, 'revision_date', 0)
            #
            return True
        except Exception as e:
            logger.exception("For %s failing with %s" % (catName, str(e)))
        return False

    def deferredItemMethod(self, dataContainer, catName, atName, **kwargs):
        """ Placeholder method to
        """
        logger.debug("Called deferred method for %r %r %r" % (dataContainer.getName(), catName, atName))
        return True

    def __getTimeStamp(self):
        utcnow = datetime.datetime.utcnow()
        ts = utcnow.strftime("%Y-%m-%d:%H:%M:%S")
        return ts
