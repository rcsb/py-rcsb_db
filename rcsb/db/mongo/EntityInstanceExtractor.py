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
from itertools import chain, groupby, islice
from operator import itemgetter
from statistics import mean, stdev

import numpy as np

from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

import requests

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
        self.__seqCache = {}
        self.__mU = MarshalUtil()
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
        savePath = kwargs.get('savePath', 'entry-data.pic')
        entryLimit = kwargs.get('entryLimit', None)
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
                    iCount = 0
                    for entryId in entryD:
                        #
                        if resultKey in entryD[entryId]:
                            continue
                        #
                        qD = {'_entry_id': entryId,
                              'entity_poly.rcsb_entity_polymer_type': 'Protein',
                              'entity.rcsb_multiple_source_flag': 'N'}
                        #
                        dL = mg.fetch(dbName, collectionName, selectL, queryD=qD)
                        logger.debug("%s query %r fetch result count %d" % (entryId, qD, len(dL)))
                        eD = {}
                        for ii, d in enumerate(dL, 1):
                            rD = {}
                            logger.debug("%s (%4d) d is %r" % (entryId, ii, d))
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
                                #
                                refDbName = rD['db_name']
                                dbAccession = rD['db_accession']
                                dbRefSeq = self.__seqCache[dbAccession] if dbAccession in self.__seqCache else None

                                if refDbName in ['UNP'] and not dbRefSeq:
                                    dbRefSeq = self.__fetchUniprot(dbAccession)
                                    self.__seqCache[dbAccession] = dbRefSeq
                                    logger.debug("Fetch uniprot %r" % dbRefSeq)
                                rD['ref_db_seq'] = dbRefSeq
                            else:
                                rD['seq_one_letter_code_ref'] = rD['db_accession'] = rD['db_name'] = None
                            #
                            if 'entity_id' in rD:
                                eD[rD['entity_id']] = copy.copy(rD)

                        entryD[entryId][resultKey] = copy.copy(eD)

                        iCount += 1
                        if iCount % 500 == 0:
                            logger.info("Completed polymer entities fetch %d/%d entries" % (iCount, len(entryD)))
                        if iCount % 2000 == 0:
                            ok = self.__mU.doExport(savePath, entryD, format='pickle')
                            logger.info("Saved polymer entity results (%d) status %r in %s" % (iCount, ok, savePath))
                        if entryLimit and iCount >= entryLimit:
                            logger.info("Quitting after %d" % iCount)
                            break
            #
            # for entryId in entryD:
            #    logger.debug(">>  %s docD  %r" % (entryId, entryD[entryId]))
            ok = self.__mU.doExport(savePath, entryD, format='pickle')
            logger.info("Saved polymer entity results (%d) status %r in %s" % (iCount, ok, savePath))
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return entryD

    def getEntityInstances(self, entryD, **kwargs):
        """ Get the selected validation data for the instances in the input entry dictionary.

        entryD[entryId]['selected_polymer_entities'][entityId]['validation'] = {}

        Add keys: 'pdbx_vrpt_instance_results'  and  'pdbx_unobs_or_zero_occ_residues' to the validation dictionary above.

        Args:
            resourceName (str):  resource name (e.g. DrugBank, CCDC)
            **kwargs: unused

        Returns:
            entryD: { }
        """
        dbName = kwargs.get('dbName', 'pdbx_v5')
        collectionName = kwargs.get('collectionName', 'pdbx_core_entity_instance_v5_0_2')
        savePath = kwargs.get('savePath', 'entry-data.pic')
        entryLimit = kwargs.get('entryLimit', None)
        #
        try:
            iCount = 0
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(dbName, collectionName):
                    logger.info("%s %s total document count is %d" % (dbName, collectionName, mg.count(dbName, collectionName)))
                    #
                    for entryId, d in entryD.items():
                        for entityId, peD in d['selected_polymer_entities'].items():
                            # if 'anal_instances' in peD:
                            #    continue
                            vD = {}
                            for asymId in peD['asym_ids']:
                                qD = {'_entry_id': entryId, '_asym_id': asymId}
                                # qD = {'rcsb_entity_instance_container_identifiers.entity_type': 'polymer'}
                                # selectL = ['pdbx_vrpt_instance_results', 'pdbx_unobs_or_zero_occ_residues']
                                selectL = ['pdbx_vrpt_instance_results']
                                tL = mg.fetch(dbName, collectionName, selectL, queryD=qD)
                                d = {}
                                logger.debug('>>> %s %s (%s) dict key length %d ' % (collectionName, entryId, asymId, len(tL[0])))

                                #
                                if False:
                                    d['pdbx_vrpt_instance_results'] = tL[0]['pdbx_vrpt_instance_results'] if 'pdbx_vrpt_instance_results' in tL[0] else []
                                    d['pdbx_unobs_or_zero_occ_residues'] = tL[0]['pdbx_unobs_or_zero_occ_residues'] if 'pdbx_unobs_or_zero_occ_residues' in tL[0] else []
                                #
                                if False:
                                    urdL = tL[0]['pdbx_unobs_or_zero_occ_residues'] if 'pdbx_unobs_or_zero_occ_residues' in tL[0] else []
                                    oL = [{'label_seq_id': urd['label_seq_id'], 'label_comp_id': urd['label_comp_id']} for urd in urdL]
                                    d['pdbx_unobs_or_zero_occ_residues'] = oL
                                #
                                try:
                                    irdL = tL[0]['pdbx_vrpt_instance_results'] if 'pdbx_vrpt_instance_results' in tL[0] else []
                                    oL = [{'label_seq_id': ird['label_seq_id'], 'label_comp_id': ird['label_comp_id']} for ird in irdL]
                                    d['pdbx_vrpt_instance_results_seq'] = oL
                                except Exception as e:
                                    logger.error("entryId %s entityId %s asymId %s bad validation data" % (entryId, entityId, asymId))

#
                                try:
                                    irdL = tL[0]['pdbx_vrpt_instance_results'] if 'pdbx_vrpt_instance_results' in tL[0] else []
                                    oL = [{'OWAB': ird['OWAB'], 'label_seq_id': ird['label_seq_id'], 'label_comp_id': ird['label_comp_id']} for ird in irdL]
                                    d['pdbx_vrpt_instance_results_occ'] = oL
                                except Exception as e:
                                    #logger.error("entryId %s entityId %s asymId %s bad validation data" % (entryId, entityId, asymId))
                                    pass

                                vD[asymId] = copy.copy(d)
                                #
                            analD = self.analEntity(entryId, peD, vD)
                            entryD[entryId]['selected_polymer_entities'][entityId]['anal_instances'] = copy.copy(analD)
                        iCount += 1
                        if iCount % 500 == 0:
                            logger.info("Completed %d/%d entries" % (iCount, len(entryD)))
                        if iCount % 2000 == 0:
                            ok = self.__mU.doExport(savePath, entryD, format='pickle')
                            logger.info("Saved polymer entity instance results (%d) status %r in %s" % (iCount, ok, savePath))
                        if entryLimit and iCount >= entryLimit:
                            break

        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return entryD

    def analEntity(self, entryId, entityD, vD, **kwargs):
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
                                                                            {'OWAB': 11.2, 'label_seq_id': 12, 'label_comp_id': 'LEU'}, }}...]

                                        'pdbx_unobs_or_zero_occ_residues': [{'label_seq_id': 1, 'label_comp_id': 'MET'},
                                               {'label_seq_id': 2, 'label_comp_id': 'ALA'},
                                                {'label_seq_id': 3, 'label_comp_id': 'LYS'},
                                                 {'label_seq_id': 4, 'label_comp_id': 'GLY'}]}

        """
        analD = {}
        try:
            entityId = entityD['entity_id']
            asymIdL = entityD['asym_ids']

            refSeq = entityD['seq_one_letter_code_ref'] if 'seq_one_letter_code_ref' in entityD else None
            entitySeq = entityD['seq_one_letter_code_can'] if 'seq_one_letter_code_can' in entityD else None
            # -------
            # Get UniProt
            #
            dbName = entityD['db_name'] if 'db_name' in entityD else None
            dbAccession = entityD['db_accession'] if 'db_accession' in entityD else None
            dbRefSeq = entityD['ref_db_seq'] if 'ref_db_seq' in entityD else None
            # --
            if dbRefSeq:
                logger.debug("%s (%s) ref db %4d:  %r" % (dbAccession, dbName, len(dbRefSeq), dbRefSeq))
            if refSeq:
                logger.debug("%s (%s) seq ref pdb %4d:  %r" % (dbAccession, dbName, len(refSeq), refSeq))
            if entitySeq:
                logger.debug("%s (%s) entity sample %4d:  %r" % (dbAccession, dbName, len(entitySeq), entitySeq))
            #
            lenRefDbSeq = len(dbRefSeq) if dbRefSeq else None
            lenEntitySeq = len(entitySeq)
            #sampleSeqCov = 1.0 - float(lenRefDbSeq - lenEntitySeq) / float(lenRefDbSeq) if lenRefDbSeq else None
            #

            # -
            for asymId in asymIdL:
                if asymId not in vD:
                    logger.error("Missing validatoin data for %s %s %s" % (entryId, entityId, asymId))
                    continue
                #
                irDL = vD[asymId]['pdbx_vrpt_instance_results_seq'] if 'pdbx_vrpt_instance_results_seq' in vD[asymId] else []
                lsL = list(set([d['label_seq_id'] for d in irDL]))
                lenInstanceSeq = len(lsL)

                instRefDbSeqCov = 1.0 - float(lenRefDbSeq - lenInstanceSeq) / float(lenRefDbSeq) if lenRefDbSeq else None
                instSampleSeqCov = 1.0 - float(lenEntitySeq - lenInstanceSeq) / float(lenEntitySeq)
                #
                occDL = vD[asymId]['pdbx_vrpt_instance_results_occ'] if 'pdbx_vrpt_instance_results_occ' in vD[asymId] else []
                # average the
                owabRegD = {}
                if len(occDL) > 0:
                    owabD = {}
                    for d in occDL:
                        owabD.setdefault(d['label_seq_id'], []).append(d['OWAB'])
                    #
                    # logger.info("owabD %r" % owabD)
                    meanOwabD = {k: mean(v) for k, v in owabD.items()}
                    meanOwab = mean(meanOwabD.values())
                    stdevOwab = stdev(meanOwabD.values())
                    #
                    logger.debug(">> Length of B values list %d mean %.3f stdev %.3f" % (len(meanOwabD), meanOwab, stdevOwab))
                    #
                    meanOwabA = np.array(list(meanOwabD.values()))
                    #
                    condition = meanOwabA > (meanOwab + meanOwab)
                    regL = self.__contiguous_regions(condition)
                    for ii, (start, stop) in enumerate(regL, 1):
                        segment = meanOwabA[start:stop]
                        logger.debug("B value range =  start %d stop %d min %.3f max %.3f" % (start, stop, segment.min(), segment.max()))
                        owabRegD[ii] = {'length': stop - start + 1, 'occ_min': segment.min(), 'occ_max': segment.max()}

                #
                #
                # if False:
                #    uDL = vD[asymId]['pdbx_unobs_or_zero_occ_residues'] if 'pdbx_unobs_or_zero_occ_residues' in vD[asymId] else []
                #    unobsL = [d['label_seq_id'] for d in uDL]
                #
                # segL = []
                # for k, g in groupby(enumerate(lsL), lambda x: x[0] - x[1]):
                #    logger.info(" Segment entryId %s entityId %s asymId %s:  %r" % (entryId, entityId, asymId, list(map(itemgetter(1), g))))
                #
                # for k, g in groupby(enumerate(lsL), lambda(i, x): i - x):
                #    logger.info(" entryId %s entityId %s asymId %s:  %r" % (entryId, entityId, asymId, list(map(itemgetter(1), g)))

                segL = [list(map(itemgetter(1), g)) for k, g in groupby(enumerate(lsL), lambda x: x[0] - x[1])]
                logger.debug("Modeled sequence length %d segments %d" % (len(lsL), len(segL)))
                #
                gapD = {}
                for ii in range(1, len(segL)):
                    bG = segL[ii - 1][-1]
                    eG = segL[ii][0]
                    gapD[ii] = eG - bG - 1
                    logger.debug("Gap %d length %d" % (ii, gapD[ii]))
                #
                #
                if instRefDbSeqCov:
                    logger.debug("Summary %s %s %s refcov %.2f  sampleCov %.2f - gaps (%d) %r owabs seqments (%d) %r" %
                                 (entryId, entityId, asymId, instRefDbSeqCov, instSampleSeqCov, len(gapD), list(gapD.values()), len(owabRegD), list(owabRegD.values())))
                else:
                    logger.debug("Summary %s %s %s sampleCov %.2f - gaps (%d) %r owabs seqments (%d) %r" %
                                 (entryId, entityId, asymId, instSampleSeqCov, len(gapD), list(gapD), len(owabRegD), list(owabRegD.values())))
                #
                analD[asymId] = {'coverage_inst_refdb': instRefDbSeqCov, 'coverage_inst_entity': instSampleSeqCov,
                                 'gapD': copy.copy(gapD), 'owabRegiond': copy.copy(owabRegD)}
                logger.debug("entry %s entity %s analD %r" % (entryId, entityId, analD))
        except Exception as e:
            logger.exception("%s failing with %s" % (entryId, str(e)))
        #
        return analD

    def __getSegments(self, values, ):
        x = np.asarray(values)
        # Generate some random data
        # x = np.cumsum(np.random.random(1000) - 0.5)
        #
        condition = np.abs(x) < 1

        # Print the start and stop indicies of each region where the absolute
        # values of x are below 1, and the min and max of each of these regions
        for start, stop in self.__contiguous_regions(condition):
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
