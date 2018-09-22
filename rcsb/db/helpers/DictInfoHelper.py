##
# File:    DictInfoHelper.py
# Author:  J. Westbrook
# Date:    18-May-2018
# Version: 0.001 Initial version
#
# Updates:
#  23-May-2018  jdw revise dependencies for helper function.  Change method api's
#  27-May-2018  jdw add attribrute level filters
#   1-Jun-2018  jdw add bridging class DataTransformInfo for attribute filters
#  13-Jun-2018  jdw add content classes to cover former base table feature
#  15-Jun-2018  jdw add support for alternative iterable delimiters
#  24-Jul-2018  jdw fix logic in processing _itemTransformers data
#   6-Aug-2018  jdw add slice parent item definitions in terms of parent items
#   8-Aug-2018  jdw add slice parent conditional filter definitions that could be applied to the parent data category,
#  10-Aug-2018  jdw add slice category and cardinality extras
#  18-Aug-2018  jdw add schema pdbx_core analogous to pdbx removing the block attribute.
#   7-Sep-2018  jdw add generated content classes for core schemas
#  10-Sep-2018  jdw add iterable details for semicolon separated text data
#  11-Sep-2018  jdw adjust slice cardinality constraints for entity and assembly identifier categories.
##
"""
This helper class supplements dictionary information as required for schema production.
I other words, the additional content conferred here would best be incorporated as part
of standard data definitions at some future point.

This is the single source of additional dictionary specific semantic content and is
design to avoid scattering dictionary semantic content haphazardly throughout the
code base.

All data accessors and structures here refer to dictionary category and attribute names.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.helpers.DictInfoHelperBase import DictInfoHelperBase
from rcsb.db.processors.DataTransformFactory import DataTransformInfo

logger = logging.getLogger(__name__)


class DictInfoHelper(DictInfoHelperBase):
    """ Supplements dictionary information as required for schema production.

    """
    # Data items requiring a particular data transformation -
    _itemTransformers = {
        'STRIP_WS': [
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_c_terminal_seq_one_letter_code'},
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_n_terminal_seq_one_letter_code'},
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_seq_one_letter_code'},
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_seq_one_letter_code_can'},
            {'CATEGORY_NAME': 'entity_poly', 'ATTRIBUTE_NAME': 'pdbx_seq_one_letter_code_sample'},
            {'CATEGORY_NAME': 'struct_ref', 'ATTRIBUTE_NAME': 'pdbx_seq_one_letter_code'}
        ]
    }

    _cardinalityParentItens = {
        'bird': {'CATEGORY_NAME': 'pdbx_reference_molecule', 'ATTRIBUTE_NAME': 'prd_id'},
        'bird_family': {'CATEGORY_NAME': 'pdbx_reference_molecule_family', 'ATTRIBUTE_NAME': 'family_prd_id'},
        'chem_comp': {'CATEGORY_NAME': 'chem_comp', 'ATTRIBUTE_NAME': 'id'},
        'bird_chem_comp': {'CATEGORY_NAME': 'chem_comp', 'ATTRIBUTE_NAME': 'id'},
        'pdbx': {'CATEGORY_NAME': 'entry', 'ATTRIBUTE_NAME': 'id'},
        'pdbx_core': {'CATEGORY_NAME': 'entry', 'ATTRIBUTE_NAME': 'id'}
    }
    _cardinalityCategoryExtras = ['rcsb_load_status']
    #
    _selectionFilters = {('PUBLIC_RELEASE', 'pdbx'): [{'CATEGORY_NAME': 'pdbx_database_status', 'ATTRIBUTE_NAME': 'status_code', 'VALUES': ['REL']}],
                         ('PUBLIC_RELEASE', 'pdbx_core'): [{'CATEGORY_NAME': 'pdbx_database_status', 'ATTRIBUTE_NAME': 'status_code', 'VALUES': ['REL']}],
                         ('PUBLIC_RELEASE', 'chem_comp'): [{'CATEGORY_NAME': 'chem_comp', 'ATTRIBUTE_NAME': 'pdbx_release_status', 'VALUES': ['REL', 'OBS', 'REF_ONLY']}],
                         ('PUBLIC_RELEASE', 'bird_chem_comp'): [{'CATEGORY_NAME': 'chem_comp', 'ATTRIBUTE_NAME': 'pdbx_release_status', 'VALUES': ['REL', 'OBS', 'REF_ONLY']}],
                         ('PUBLIC_RELEASE', 'bird'): [{'CATEGORY_NAME': 'pdbx_reference_molecule', 'ATTRIBUTE_NAME': 'release_status', 'VALUES': ['REL', 'OBS']}],
                         ('PUBLIC_RELEASE', 'bird_family'): [{'CATEGORY_NAME': 'pdbx_reference_molecule_family', 'ATTRIBUTE_NAME': 'release_status', 'VALUES': ['REL', 'OBS']}]
                         }

    _typeCodeClasses = {'iterable': ['ucode-alphanum-csv', 'id_list', 'alphanum-scsv']}
    _queryStringSelectors = {'iterable': ['comma separate']}
    # Put the non default iterable delimiter cases here -
    _iterableDelimiters = [{'CATEGORY_NAME': 'chem_comp', 'ATTRIBUTE_NAME': 'pdbx_synonyms', 'DELIMITER': ';'},
                           {'CATEGORY_NAME': 'citation', 'ATTRIBUTE_NAME': 'rcsb_authors', 'DELIMITER': ';'}]
    #
    # Categories/Attributes that will be included in a schema definitions even if they are not populated in any tabulated instance data -
    #
    _contentClasses = {('GENERATED_CONTENT', 'pdbx'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']},
                                                       {'CATEGORY_NAME': 'pdbx_struct_assembly_gen', 'ATTRIBUTE_NAME_LIST': ['ordinal']}],
                       ('GENERATED_CONTENT', 'pdbx_core'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']},
                                                            {'CATEGORY_NAME': 'citation', 'ATTRIBUTE_NAME_LIST': ['rcsb_authors']},
                                                            {'CATEGORY_NAME': 'pdbx_struct_assembly_gen', 'ATTRIBUTE_NAME_LIST': ['ordinal']},
                                                            {'CATEGORY_NAME': 'pdbx_struct_assembly', 'ATTRIBUTE_NAME_LIST': ['rcsb_details', 'rcsb_candidate_assembly']},
                                                            {'CATEGORY_NAME': 'rcsb_entry_container_identifiers', 'ATTRIBUTE_NAME_LIST': [
                                                                'entry_id', 'entity_ids', 'polymer_entity_ids', 'non-polymer_entity_ids', 'assembly_ids']},
                                                            {'CATEGORY_NAME': 'rcsb_entity_container_identifiers', 'ATTRIBUTE_NAME_LIST': ['entry_id', 'entity_id']},
                                                            {'CATEGORY_NAME': 'rcsb_assembly_container_identifiers', 'ATTRIBUTE_NAME_LIST': ['entry_id', 'assembly_id']}],
                       ('EVOLVING_CONTENT', 'pdbx_core'): [{'CATEGORY_NAME': 'diffrn', 'ATTRIBUTE_NAME_LIST': ['pdbx_serial_crystal_experiment']},
                                                           {'CATEGORY_NAME': 'diffrn_detector', 'ATTRIBUTE_NAME_LIST': ['pdbx_frequency']},
                                                           {'CATEGORY_NAME': 'pdbx_serial_crystallography_measurement',
                                                            'ATTRIBUTE_NAME_LIST': ['diffrn_id',
                                                                                    'pulse_energy',
                                                                                    'pulse_duration',
                                                                                    'xfel_pulse_repetition_rate',
                                                                                    'pulse_photon_energy',
                                                                                    'photons_per_pulse',
                                                                                    'source_size',
                                                                                    'source_distance',
                                                                                    'focal_spot_size',
                                                                                    'collimation',
                                                                                    'collection_time_total']},

                                                           {'CATEGORY_NAME': 'pdbx_serial_crystallography_sample_delivery',
                                                            'ATTRIBUTE_NAME_LIST': ['diffrn_id', 'description', 'method']},

                                                           {'CATEGORY_NAME': 'pdbx_serial_crystallography_sample_delivery_injection',
                                                            'ATTRIBUTE_NAME_LIST': ['diffrn_id',
                                                                                    'description',
                                                                                    'injector_diameter',
                                                                                    'injector_temperature',
                                                                                    'injector_pressure',
                                                                                    'flow_rate',
                                                                                    'carrier_solvent',
                                                                                    'crystal_concentration',
                                                                                    'preparation',
                                                                                    'power_by',
                                                                                    'injector_nozzle',
                                                                                    'jet_diameter',
                                                                                    'filter_size']},

                                                           {'CATEGORY_NAME': 'pdbx_serial_crystallography_sample_delivery_fixed_target',
                                                            'ATTRIBUTE_NAME_LIST': ['diffrn_id',
                                                                                    'description',
                                                                                    'sample_holding',
                                                                                    'support_base',
                                                                                    'sample_unit_size',
                                                                                    'crystals_per_unit',
                                                                                    'sample_solvent',
                                                                                    'sample_dehydration_prevention',
                                                                                    'motion_control',
                                                                                    'velocity_horizontal',
                                                                                    'velocity_vertical',
                                                                                    'details']},

                                                           {'CATEGORY_NAME': 'pdbx_serial_crystallography_data_reduction',
                                                            'ATTRIBUTE_NAME_LIST': ['diffrn_id',
                                                                                    'frames_total',
                                                                                    'xfel_pulse_events',
                                                                                    'frame_hits',
                                                                                    'crystal_hits',
                                                                                    'droplet_hits',
                                                                                    'frames_failed_index',
                                                                                    'frames_indexed',
                                                                                    'lattices_indexed',
                                                                                    'xfel_run_numbers']},
                                                           ],
                       ('GENERATED_CONTENT', 'chem_comp'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']}],
                       ('GENERATED_CONTENT', 'bird_chem_comp'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']}],
                       ('GENERATED_CONTENT', 'bird'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']}],
                       ('GENERATED_CONTENT', 'bird_family'): [{'CATEGORY_NAME': 'rcsb_load_status', 'ATTRIBUTE_NAME_LIST': ['datablock_name', 'load_date', 'locator']}],
                       }
#
#
# _diffrn.pdbx_serial_crystal_experiment
# _diffrn_detector.pdbx_frequency
#
#
    _sliceParentItems = {('ENTITY', 'pdbx_core'): [{'CATEGORY_NAME': 'entity', 'ATTRIBUTE_NAME': 'id'}],
                         ('ASSEMBLY', 'pdbx_core'): [{'CATEGORY_NAME': 'pdbx_struct_assembly', 'ATTRIBUTE_NAME': 'id'}]
                         }
    _sliceParentFilters = {('ENTITY', 'pdbx_core'): [{'CATEGORY_NAME': 'entity', 'ATTRIBUTE_NAME': 'type', 'VALUES': ['polymer', 'non-polymer', 'macrolide', 'branched']}]
                           }

    _sliceCardinalityCategoryExtras = {('ENTITY', 'pdbx_core'): ['rcsb_load_status', 'rcsb_entity_container_identifiers'],
                                       ('ASSEMBLY', 'pdbx_core'): ['rcsb_load_status', 'rcsb_assembly_container_identifiers']
                                       }
    _sliceCategoryExtras = {('ENTITY', 'pdbx_core'): ['rcsb_load_status'], ('ASSEMBLY', 'pdbx_core'): ['rcsb_load_status', 'pdbx_struct_oper_list']}

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (below)
            dictPath (str, optional): path to the current dictioonary text
            dictSubset (str, optional): name of dictionary content subset - alias for schema name


            Add - Include exclude filters on dictionary content -


        """
        super(DictInfoHelper, self).__init__(**kwargs)
        self._dictSubset = kwargs.get('dictSubset', None)
        self._dictPath = kwargs.get("dictPath", None)
        #
        self.__dti = DataTransformInfo()
        self.__itD = self.__getItemTransformD()
        #
        self.__categoryClasses = self.__getCategoryContentClasses()
        self.__attributeClasses = self.__getAttributeContentClasses()

    def getDelimiter(self, categoryName, attributeName, default=','):
        for tD in DictInfoHelper._iterableDelimiters:
            if tD['CATEGORY_NAME'] == categoryName and tD['ATTRIBUTE_NAME'] == attributeName:
                return tD['DELIMITER']
        return default

    def getCategoryContentClasses(self, dictSubset):
        try:
            rD = {}
            for catName, cDL in self.__categoryClasses.items():
                for cD in cDL:
                    if cD['DICT_SUBSET'] == dictSubset:
                        if catName not in rD:
                            rD[catName] = []
                        rD[catName].append(cD['CONTENT_CLASS'])

        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return rD

    def getAttributeContentClasses(self, dictSubset):
        try:
            rD = {}
            for (catName, atName), cDL in self.__attributeClasses.items():
                for cD in cDL:
                    if cD['DICT_SUBSET'] == dictSubset:
                        if (catName, atName) not in rD:
                            rD[(catName, atName)] = []
                        rD[(catName, atName)].append(cD['CONTENT_CLASS'])

        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return rD

    def __getCategoryContentClasses(self):
        classD = {}
        try:
            for cTup, cDL in DictInfoHelper._contentClasses.items():
                for cD in cDL:
                    if cD['CATEGORY_NAME'] not in classD:
                        classD[cD['CATEGORY_NAME']] = []
                    classD[cD['CATEGORY_NAME']].append({'CONTENT_CLASS': cTup[0], 'DICT_SUBSET': cTup[1]})
        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return classD

    def __getAttributeContentClasses(self):
        classD = {}
        try:
            for cTup, cDL in DictInfoHelper._contentClasses.items():
                for cD in cDL:
                    catName = cD['CATEGORY_NAME']
                    for atName in cD['ATTRIBUTE_NAME_LIST']:
                        if (catName, atName) not in classD:
                            classD[(catName, atName)] = []
                        classD[(catName, atName)].append({'CONTENT_CLASS': cTup[0], 'DICT_SUBSET': cTup[1]})
        except Exception as e:
            logger.debug("Failing with %s" % str(e))
        return classD

    def __getItemTransformD(self):
        itD = {}
        for f, dL in DictInfoHelper._itemTransformers.items():
            logger.debug("Verify transform method %r" % f)
            if self.__dti.isImplemented(f):
                for d in dL:
                    if (d['CATEGORY_NAME'], d['ATTRIBUTE_NAME']) not in itD:
                        itD[(d['CATEGORY_NAME'], d['ATTRIBUTE_NAME'])] = []
                    itD[(d['CATEGORY_NAME'], d['ATTRIBUTE_NAME'])].append(f)

        return itD

    def getDictPath(self):
        return self._dictPath

    def getDictSubSet(self):
        return self._dictSubset

    def getTransformItems(self, transformName):
        """ Return the list of items subject to the input attribute filter.

            _itemTransformers{<filterName> : [{'CATEGORY_NAME':..., 'ATTRIBUTE_NAME': ... },{}]
        """
        try:
            if self.__dti.isImplemented(transformName):
                return DictInfoHelper._itemTransformers[transformName]
        except Exception:
            return []

    def getItemTransforms(self, categoryName, attributeName):
        """ Return the list of transforms to be applied to the input item (categoryName, attributeName).
        """
        try:
            return self.__itD[(categoryName, attributeName)]
        except Exception:
            return []

    def getItemTransformD(self):
        """ Return the dictionary of transforms to be applied to the input item (categoryName, attributeName).
        """
        try:
            return self.__itD
        except Exception:
            return {}

    def getCardinalityCategoryExtras(self):
        return DictInfoHelper._cardinalityCategoryExtras

    def getCardinalityKeyItem(self, dictSubset):
        """ Identify the parent item for the dictionary subset that can be used to
            identify child categories with unity cardinality.   That is, logically containing
            a single data row in any instance.

        """
        try:
            return DictInfoHelper._cardinalityParentItens[dictSubset]
        except Exception:
            pass
        return {'CATEGORY_NAME': None, 'ATTRIBUTE_NAME': None}

    def getTypeCodes(self, kind):
        """
        """
        try:
            return DictInfoHelper._typeCodeClasses[kind]
        except Exception as e:
            logger.exception("Failing for kind %r with %s" % (kind, str(e)))
            pass
        return []

    def getQueryStrings(self, kind):
        try:
            return DictInfoHelper._queryStringSelectors[kind]
        except Exception:
            pass

        return []

    def getSelectionFilter(self, dictSubset, kind):
        """  Interim api for selection filters defined in terms of dictionary category and attributes name and their values.

        """
        try:
            return DictInfoHelper._selectionFilters[(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getSelectionFiltersBySubset(self, dictSubset):
        """  Interim api for selection filters for a particular dictionary subset.

        """
        try:
            return {kind: v for (kind, dS), v in DictInfoHelper._selectionFilters.items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getContentClass(self, dictSubset, kind):
        """  Interim api for special category classes.

        """
        try:
            return DictInfoHelper._specialContent[(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getContentClassBySubset(self, dictSubset):
        """  Interim api for special category classes.

        """
        try:
            return {kind: v for (kind, dS), v in DictInfoHelper._specialContent.items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getSliceParentItems(self, dictSubset, kind):
        """  Interim api for slice parent itens defined in terms of dictionary category and attributes name and their values.

        """
        try:
            return DictInfoHelper._sliceParentItems[(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getSliceParentsBySubset(self, dictSubset):
        """  Interim api for slice parent items for a particular dictionary subset.

        """
        try:
            return {kind: v for (kind, dS), v in DictInfoHelper._sliceParentItems.items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getSliceParentFilters(self, dictSubset, kind):
        """  Interim api for slice parent condition filters defined in terms of dictionary category and attributes name and their values.

        """
        try:
            return DictInfoHelper._sliceParentFilters[(kind, dictSubset)]
        except Exception:
            pass
        return []

    def getSliceParentFiltersBySubset(self, dictSubset):
        """  Interim api for slice parent condition filters for a particular dictionary subset.

        """
        try:
            return {kind: v for (kind, dS), v in DictInfoHelper._sliceParentFilters.items() if dS == dictSubset}
        except Exception:
            pass
        return {}

    def getSliceCardinalityCategoryExtras(self, dictSubset, kind):
        try:
            return DictInfoHelper._sliceCardinalityCategoryExtras[(kind, dictSubset)]
        except Exception:
            return []

    def getSliceCategoryExtras(self, dictSubset, kind):
        try:
            return DictInfoHelper._sliceCategoryExtras[(kind, dictSubset)]
        except Exception:
            return []
