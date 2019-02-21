##
# File: EntityInstanceExtractor.py
# Date: 19-Feb-2019  jdw
#
# Selected utilities to extract data from entity instance collections.
#
# Updates:
#
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import copy
import logging
from itertools import chain, islice
from operator import itemgetter

import numpy as np
import requests
from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil

logger = logging.getLogger(__name__)


class EntityInstanceExtractor(object):
    """ Selected utilities to extract data from entity instance collections.

            >>> from operator import itemgetter
            >>>
            >>> seq2 = [1, 2, 4, 5, 6, 8, 9, 10]
            >>> list = []
            >>> for k, g in groupby(enumerate(seq2), lambda (i,x):i-x):
            ...     list.append(map(itemgetter(1), g))
            ...
            >>> print list
            [[1, 2], [4, 5, 6], [8, 9, 10]]
            Or as a list comprehension:

            >>> [map(itemgetter(1), g) for k, g in groupby(enumerate(seq2), lambda (i,x):i-x)]
            [[1, 2], [4, 5, 6], [8, 9, 10]]


            ##
            ##

            import numpy as np

            def main():
                # Generate some random data
                x = np.cumsum(np.random.random(1000) - 0.5)
                condition = np.abs(x) < 1

                # Print the start and stop indicies of each region where the absolute
                # values of x are below 1, and the min and max of each of these regions
                for start, stop in contiguous_regions(condition):
                    segment = x[start:stop]
                    print start, stop
                    print segment.min(), segment.max()

            import numpy as np

            Samples = np.array([[1, 2, 3],
                               [1, 2]])
            c = np.hstack(Samples)  # Will gives [1,2,3,1,2]
            mean, std = np.mean(c), np.std(c)
            newSamples = np.asarray([(np.array(xi)-mean)/std for xi in Samples])
            print newSamples

    """

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb
        self.__resourceName = "MONGO_DB"
        #

    def getEntryInfo(self, **kwargs):
        """  Return a dictionary of PDB entries satifying the input conditions (e.g. method, resolution limit)
        """

        resLimit = kwargs.get('resLimit', 3.5)
        expMethod = kwargs.get('expMethod', 'X-ray')
        #
        dbName = kwargs.get('dbName', 'pdbx_v5')
        collectionName = kwargs.get('collectionName', 'pdbx_core_entry_v5_0_2')
        #
        entryD = {}
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(dbName, collectionName):
                    logger.info("%s %s document count is %d" % (dbName, collectionName, mg.count(dbName, collectionName)))
                    qD = {'rcsb_entry_info.experimental_method': expMethod, 'refine.0.ls_d_res_high': {"$lte": resLimit}}
                    selectL = ['_entry_id', 'rcsb_entry_info']
                    dL = mg.fetch(dbName, collectionName, selectL, queryD=qD)
                    logger.info("Selection %r fetch result count %d" % (selectL, len(dL)))
                    #
                    for d in dL:
                        if '_entry_id' not in d:
                            continue
                        entryD[d['_entry_id']] = {}
                        if 'rcsb_entry_info' in d and 'polymer_composition' in d['rcsb_entry_info']:
                            entryD[d['_entry_id']] = {'polymer_composition': d['rcsb_entry_info']['polymer_composition'],
                                                      'experimental_method': d['rcsb_entry_info']['experimental_method']}
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return entryD
        #

    def getEntityIds(self, entryIdList):
        """
        """
        dbName = 'pdbx_v5'
        collectionName = 'pdbx_core_entity_v5_0_2'
        docD = {}
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(dbName, collectionName):
                    logger.info("%s %s document count is %d" % (dbName, collectionName, mg.count(dbName, collectionName)))
                    for entryId in entryIdList:
                        qD = {'_entry_id': entryId}
                        selectL = ['rcsb_entity_container_identifiers']
                        tL = mg.fetch(dbName, collectionName, selectL, queryD=qD)
                        #
                        logger.info("Selection %r fetch result count %d" % (selectL, len(tL)))
                        docD[entryId] = [vv['rcsb_entity_container_identifiers'] for vv in tL]
            logger.info("docD is %r" % docD)
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return docD

    def getPolymerEntities(self, entryD, **kwargs):
        """  Add 'selected_polymer_entities' satisfying the input contiditions and add this to the input entry dictionary.
        """
        dbName = kwargs.get('dbName', 'pdbx_v5')
        collectionName = kwargs.get('collectionName', 'pdbx_core_entity_v5_0_2')
        resultKey = kwargs.get('resultKey', 'selected_polymer_entities')
        #
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(dbName, collectionName):
                    logger.info("%s %s document count is %d" % (dbName, collectionName, mg.count(dbName, collectionName)))
                    selectL = ['rcsb_entity_container_identifiers',
                               'entity_poly.type',
                               'entity_poly.pdbx_seq_one_letter_code_can',
                               'rcsb_entity_source_organism.ncbi_taxonomy_id',
                               'rcsb_entity_source_organism.ncbi_scientific_name',
                               'struct_ref.pdbx_seq_one_letter_code',
                               'struct_ref.pdbx_db_accession',
                               'struct_ref.db_name',
                               'struct_ref.entity_id'
                               ]
                    for entryId in entryD:
                        #
                        qD = {'_entry_id': entryId,
                              'entity_poly.rcsb_entity_polymer_type': 'Protein',
                              'entity.rcsb_multiple_source_flag': 'N'}
                        #
                        dL = mg.fetch(dbName, collectionName, selectL, queryD=qD)
                        logger.info("%s selection %r fetch result count %d" % (entryId, selectL, len(dL)))
                        eD = {}
                        for ii, d in enumerate(dL, 1):
                            rD = {}
                            logger.info("%s (%4d) d is %r" % (entryId, ii, d))
                            if 'rcsb_entity_container_identifiers' in d and 'asym_ids' in d['rcsb_entity_container_identifiers']:
                                rD['asym_ids'] = d['rcsb_entity_container_identifiers']['asym_ids']
                                rD['entity_id'] = d['rcsb_entity_container_identifiers']['entity_id']
                            if 'entity_poly' in d and 'type' in d['entity_poly']:
                                rD['type'] = d['entity_poly']['type']
                                rD['seq_one_letter_code_can'] = d['entity_poly']['pdbx_seq_one_letter_code_can']

                            if 'rcsb_entity_source_organism' in d:
                                rD['ncbi_taxonomy_id'] = d['rcsb_entity_source_organism'][0]['ncbi_taxonomy_id'] if 'ncbi_taxonomy_id' in d['rcsb_entity_source_organism'][0] else None
                                rD['ncbi_scientific_name'] = d['rcsb_entity_source_organism'][0][
                                    'ncbi_scientific_name'] if 'ncbi_scientific_name' in d['rcsb_entity_source_organism'][0] else None

                            if 'struct_ref' in d and len(d['struct_ref']) == 1:
                                rD['seq_one_letter_code_ref'] = d['struct_ref'][0]['pdbx_seq_one_letter_code'] if 'pdbx_seq_one_letter_code' in d['struct_ref'][0] else None
                                rD['db_accession'] = d['struct_ref'][0]['pdbx_db_accession'] if 'pdbx_db_accession' in d['struct_ref'][0] else None
                                rD['db_name'] = d['struct_ref'][0]['db_name'] if 'db_name' in d['struct_ref'][0] else None
                            else:
                                rD['seq_one_letter_code_ref'] = rD['db_accession'] = rD['db_name'] = None
                            if 'entity_id' in rD:
                                eD[rD['entity_id']] = copy.copy(rD)
                        entryD[entryId][resultKey] = copy.copy(eD)
            #
            for entryId in entryD:
                logger.info(">>  %s docD  %r" % (entryId, entryD[entryId]))

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return entryD

    def getEntityInstances(self, entryD, **kwargs):
        """ Get the selected validation data for the instances in the input entry dictionary.

        Args:
            resourceName (str):  resource name (e.g. DrugBank, CCDC)
            **kwargs: unused

        Returns:
            entryD: { }
        """
        dbName = kwargs.get('dbName', 'pdbx_v5')
        collectionName = kwargs.get('collectionName', 'pdbx_core_entity_instance_v5_0_2')
        #
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(dbName, collectionName):
                    logger.info("%s %s total document count is %d" % (dbName, collectionName, mg.count(dbName, collectionName)))
                    #
                    for entryId, d in entryD.items():
                        for entityId, peD in d['selected_polymer_entities'].items():
                            vD = {}
                            for asymId in peD['asym_ids']:
                                qD = {'_entry_id': entryId, '_asym_id': asymId}
                                # qD = {'rcsb_entity_instance_container_identifiers.entity_type': 'polymer'}
                                selectL = ['pdbx_vrpt_instance_results', 'pdbx_unobs_or_zero_occ_residues']
                                tL = mg.fetch(dbName, collectionName, selectL, queryD=qD)
                                logger.info("Result count %d" % len(tL))
                                logger.info('>>> %s %s (%s) dict key length %d ' % (collectionName, entryId, asymId, len(tL[0])))
                                d = {}

                                #
                                if False:
                                    d['pdbx_vrpt_instance_results'] = tL[0]['pdbx_vrpt_instance_results'] if 'pdbx_vrpt_instance_results' in tL[0] else []
                                    d['pdbx_unobs_or_zero_occ_residues'] = tL[0]['pdbx_unobs_or_zero_occ_residues'] if 'pdbx_unobs_or_zero_occ_residues' in tL[0] else []
                                else:
                                    irdL = tL[0]['pdbx_vrpt_instance_results'] if 'pdbx_vrpt_instance_results' in tL[0] else []
                                    oL = [{'OWAB': ird['OWAB'], 'label_seq_id': ird['label_seq_id'], 'label_comp_id': ird['label_comp_id']} for ird in irdL]
                                    d['pdbx_vrpt_instance_results'] = oL
                                    #
                                    urdL = tL[0]['pdbx_unobs_or_zero_occ_residues'] if 'pdbx_unobs_or_zero_occ_residues' in tL[0] else []
                                    oL = [{'label_seq_id': urd['label_seq_id'], 'label_comp_id': urd['label_comp_id']} for urd in urdL]
                                    d['pdbx_unobs_or_zero_occ_residues'] = oL
                                vD[asymId] = copy.copy(d)
                            entryD[entryId]['selected_polymer_entities'][entityId]['validation'] = copy.copy(vD)

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return entryD

    def analEntity(self, entityD, **kwargs):
        """

        {'polymer_composition': 'protein/NA', 'experimental_method': 'X-ray',
        'selected_polymer_entities': {'1': {'asym_ids': ['D', 'C', 'E', 'A', 'B', 'F'],
                   'entity_id': '1', 'type': 'polypeptide(L)',
                   'seq_one_letter_code_can': 'MAKGQSLQDPFLNALRRERVPVSIYLVNGIKLQGQIESFDQFVILLKNTVSQMVYKHAISTVVPS',
                   'ncbi_taxonomy_id': 511693,
                    'ncbi_scientific_name': 'Escherichia coli BL21',
                    'seq_one_letter_code_ref': 'MAKGQSLQDPFLNALRRERVPVSIYLVNGIKLQGQIESFDQFVILLKNTVSQMVYKHAISTVVPS',
                    'db_accession': 'C5W5L7',
                    'db_name': 'UNP',
                    'validation': {'D': {'pdbx_vrpt_instance_results': [{'OWAB': 29.45, 'label_seq_id': 5, 'label_comp_id': 'GLN'},
                                        {'OWAB': 26.12, 'label_seq_id': 6, 'label_comp_id': 'SER'},
                                        {'OWAB': 22.72, 'label_seq_id': 7, 'label_comp_id': 'LEU'},
                                        {'OWAB': 14.56, 'label_seq_id': 8, 'label_comp_id': 'GLN'},
                                        {'OWAB': 19.18, 'label_seq_id': 9, 'label_comp_id': 'ASP'},
                                        {'OWAB': 16.56, 'label_seq_id': 10, 'label_comp_id': 'PRO'},
                                        {'OWAB': 14.78, 'label_seq_id': 11, 'label_comp_id': 'PHE'},
                                        {'OWAB': 11.2, 'label_seq_id': 12, 'label_comp_id': 'LEU'}, }}...

                    'pdbx_unobs_or_zero_occ_residues': [{'label_seq_id': 1, 'label_comp_id': 'MET'},
                           {'label_seq_id': 2, 'label_comp_id': 'ALA'},
                            {'label_seq_id': 3, 'label_comp_id': 'LYS'},
                             {'label_seq_id': 4, 'label_comp_id': 'GLY'}]}
        ...
        """
        seqCache = {}
        entityId = entityD['entity_id']
        asymIdL = entityD['asym_ids']
        #
        refSeq = entityD['seq_one_letter_code_ref'] if 'seq_one_letter_code_ref' in entityD else None
        entitySeq = entityD['seq_one_letter_code_can'] if 'seq_one_letter_code_can' in entityD else None
        #
        # Get UniProt
        #
        dbName = entityD['db_name'] if 'db_name' in entityD else None
        dbAccession = entityD['db_accession'] if 'db_accession' in entityD else None
        dbRefSeq = seqCache[dbAccession] if dbAccession in seqCache else None
        if dbName in ['UNP'] and not dbRefSeq:
            dbRefSeq = self.__fetchUniprot(dbAccession)
            seqCache[dbAccession] = dbRefSeq

        if dbRefSeq:
            logger.info("%s (%s) db %4d:  %r" % (dbAccession, dbName, len(dbRefSeq), dbRefSeq))
        if refSeq:
            logger.info("%s (%s) pdb %4d:  %r" % (dbAccession, dbName, len(refSeq), refSeq))
        if entitySeq:
            logger.info("%s (%s) entity %4d:  %r" % (dbAccession, dbName, len(entitySeq), entitySeq))

        return False

    def __getSegments(self, values, ):
        x = np.asarray(values)
        # Generate some random data
        #x = np.cumsum(np.random.random(1000) - 0.5)
        #
        condition = np.abs(x) < 1

        # Print the start and stop indicies of each region where the absolute
        # values of x are below 1, and the min and max of each of these regions
        for start, stop in contiguous_regions(condition):
            segment = x[start:stop]
            print(start, stop)
            print(segment.min(), segment.max())

    def __contiguous_regions(self, condition):
        """Finds contiguous True regions of the boolean array "condition.

        Returns a 2D array where the first column is the start index of the region and the
        second column is the end index.

        """

        # Find the indicies of changes in "condition"
        d = np.diff(condition)
        idx, = d.nonzero()

        # We need to start things after the change in "condition". Therefore,
        # we'll shift the index by 1 to the right.
        idx += 1

        if condition[0]:
            # If the start of condition is True prepend a 0
            idx = np.r_[0, idx]

        if condition[-1]:
            # If the end of condition is True, append the length of the array
            idx = np.r_[idx, condition.size]  # Edit

        # Reshape the result into two columns
        idx.shape = (-1, 2)
        return idx

    def __window(self, seq, n=2):
        """Returns a sliding window (of width n) over data from the iterable
           s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...
        """
        it = iter(seq)
        result = tuple(islice(it, n))
        if len(result) == n:
            yield result
        for elem in it:
            result = result[1:] + (elem,)
            yield result

    def missing_elements(self, L):
        missing = chain.from_iterable(range(x + 1, y) for x, y in self.__window(L) if (y - x) > 1)
        return list(missing)

    def __fetchUniprot(self, uniProtId):
        BASE = "http://www.uniprot.org"
        KB_ENDPOINT = "/uniprot/"
        fS = ''
        try:
            fullUrl = BASE + KB_ENDPOINT + uniProtId + '.fasta'
            result = requests.get(fullUrl)
            if result.ok:
                fL = result.text.split('\n')
                fS = ''.join(fL[1:])
            else:
                logger.error("Request returns status %r" % result.status_code)
        except Exception as e:
            logger.exception("Failing request for %s with %s" % (uniProtId, str(e)))
        return fS
