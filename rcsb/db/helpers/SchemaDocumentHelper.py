##
# File:    SchemaDocumentHelper.py
# Author:  J. Westbrook
# Date:    7-Jun-2018
# Version: 0.001 Initial version
#
# Updates:
#  22-Jun-2018 jdw change collection attribute id specification to dot notation
#  14-Aug-2018 jdw generalize document key attribute to attribute list
#  20-Aug-2018 jdw slice details added to __schemaContentFilters
#   8-Oct-2018 jdw added getSubCategoryAggregates() method
#
##
"""
Inject additional document information into a schema definition.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.helpers.SchemaDocumentHelperBase import SchemaDocumentHelperBase

logger = logging.getLogger(__name__)


class SchemaDocumentHelper(SchemaDocumentHelperBase):
    """ Inject additional document information into a schema definition.

        Single source of additional document schema semantic content.
    """
    __schemaCollectionNames = {'pdbx': ['pdbx_v5_0_2', 'pdbx_ext_v5_0_2'],
                               'pdbx_core': ['pdbx_core_entity_v5_0_2', 'pdbx_core_entry_v5_0_2', 'pdbx_core_assembly_v5_0_2'],
                               'bird': ['bird_v5_0_2'],
                               'bird_family': ['family_v5_0_2'],
                               'chem_comp': ['chem_comp_v5_0_2'],
                               'bird_chem_comp': ['bird_chem_comp_v5_0_2'],
                               'pdb_distro': [],
                               'repository_holdings': ['repository_holdings_update_v0_1', 'repository_holdings_current_v0_1', 'repository_holdings_unreleased_v0_1',
                                                       'repository_holdings_removed_v0_1', 'repository_holdings_removed_audit_authors_v0_1',
                                                       'repository_holdings_superseded_v0_1', 'repository_holdings_transferred_v0_1',
                                                       'rcsb_repository_holdings_insilico_models_v0_1'],
                               'entity_sequence_clusters': ['cluster_members_v0_1', 'cluster_provenance_v0_1', 'entity_members_v0_1']
                               }
    #
    # RCSB_LOAD_STATUS must be included in all INCLUDE filters -
    #
    __schemaContentFilters = {'pdbx_v5_0_2': {'INCLUDE': [],
                                              'EXCLUDE': ['NDB_STRUCT_CONF_NA',
                                                          'NDB_STRUCT_FEATURE_NA',
                                                          'NDB_STRUCT_NA_BASE_PAIR',
                                                          'NDB_STRUCT_NA_BASE_PAIR_STEP',
                                                          'PDBX_VALIDATE_CHIRAL',
                                                          'PDBX_VALIDATE_CLOSE_CONTACT',
                                                          'PDBX_VALIDATE_MAIN_CHAIN_PLANE',
                                                          'PDBX_VALIDATE_PEPTIDE_OMEGA',
                                                          'PDBX_VALIDATE_PLANES',
                                                          'PDBX_VALIDATE_PLANES_ATOM',
                                                          'PDBX_VALIDATE_POLYMER_LINKAGE',
                                                          'PDBX_VALIDATE_RMSD_ANGLE',
                                                          'PDBX_VALIDATE_RMSD_BOND',
                                                          'PDBX_VALIDATE_SYMM_CONTACT',
                                                          'PDBX_VALIDATE_TORSION',
                                                          'STRUCT_SHEET',
                                                          'STRUCT_SHEET_HBOND',
                                                          'STRUCT_SHEET_ORDER',
                                                          'STRUCT_SHEET_RANGE',
                                                          'STRUCT_CONF',
                                                          'STRUCT_CONF_TYPE',
                                                          'STRUCT_CONN',
                                                          'STRUCT_CONN_TYPE',
                                                          'ATOM_SITE',
                                                          'ATOM_SITE_ANISOTROP',
                                                          'PDBX_UNOBS_OR_ZERO_OCC_ATOMS',
                                                          'PDBX_UNOBS_OR_ZERO_OCC_RESIDUES'],
                                              'SLICE': None
                                              },

                              'pdbx_ext_v5_0_2': {'INCLUDE': ['ENTRY', 'NDB_STRUCT_CONF_NA',
                                                              'NDB_STRUCT_FEATURE_NA',
                                                              'NDB_STRUCT_NA_BASE_PAIR',
                                                              'NDB_STRUCT_NA_BASE_PAIR_STEP',
                                                              'PDBX_VALIDATE_CHIRAL',
                                                              'PDBX_VALIDATE_CLOSE_CONTACT',
                                                              'PDBX_VALIDATE_MAIN_CHAIN_PLANE',
                                                              'PDBX_VALIDATE_PEPTIDE_OMEGA',
                                                              'PDBX_VALIDATE_PLANES',
                                                              'PDBX_VALIDATE_PLANES_ATOM',
                                                              'PDBX_VALIDATE_POLYMER_LINKAGE',
                                                              'PDBX_VALIDATE_RMSD_ANGLE',
                                                              'PDBX_VALIDATE_RMSD_BOND',
                                                              'PDBX_VALIDATE_SYMM_CONTACT',
                                                              'PDBX_VALIDATE_TORSION',
                                                              'STRUCT_SHEET',
                                                              'STRUCT_SHEET_HBOND',
                                                              'STRUCT_SHEET_ORDER',
                                                              'STRUCT_SHEET_RANGE',
                                                              'STRUCT_CONF',
                                                              'STRUCT_CONF_TYPE',
                                                              'STRUCT_CONN',
                                                              'STRUCT_CONN_TYPE',
                                                              'RCSB_LOAD_STATUS'],
                                                  'EXCLUDE': [],
                                                  'SLICE': None
                                                  },
                              'pdbx_core_entity_v5_0_2': {'INCLUDE': [], 'EXCLUDE': [], 'SLICE': 'ENTITY'},
                              'pdbx_core_assembly_v5_0_2': {'INCLUDE': [], 'EXCLUDE': [], 'SLICE': 'ASSEMBLY'},
                              'pdbx_core_entry_v5_0_2': {'INCLUDE': ['AUDIT_AUTHOR', 'CELL',
                                                                     'CITATION', 'CITATION_AUTHOR', 'DIFFRN', 'DIFFRN_DETECTOR', 'DIFFRN_RADIATION', 'DIFFRN_SOURCE', 'EM_2D_CRYSTAL_ENTITY',
                                                                     'EM_3D_CRYSTAL_ENTITY', 'EM_3D_FITTING', 'EM_3D_RECONSTRUCTION', 'EM_EMBEDDING', 'EM_ENTITY_ASSEMBLY', 'EM_EXPERIMENT',
                                                                     'EM_HELICAL_ENTITY', 'EM_IMAGE_RECORDING', 'EM_IMAGING', 'EM_SINGLE_PARTICLE_ENTITY', 'EM_SOFTWARE', 'EM_SPECIMEN',
                                                                     'EM_STAINING', 'EM_VITRIFICATION', 'ENTRY', 'EXPTL', 'EXPTL_CRYSTAL_GROW', 'PDBX_AUDIT_REVISION_DETAILS',
                                                                     'PDBX_AUDIT_REVISION_HISTORY', 'PDBX_AUDIT_SUPPORT', 'PDBX_DATABASE_PDB_OBS_SPR', 'PDBX_DATABASE_STATUS', 'PDBX_DEPOSIT_GROUP',
                                                                     'PDBX_MOLECULE', 'PDBX_MOLECULE_FEATURES', 'PDBX_NMR_DETAILS', 'PDBX_NMR_ENSEMBLE', 'PDBX_NMR_EXPTL', 'PDBX_NMR_EXPTL_SAMPLE_CONDITIONS',
                                                                     'PDBX_NMR_REFINE', 'PDBX_NMR_SAMPLE_DETAILS', 'PDBX_NMR_SOFTWARE', 'PDBX_NMR_SPECTROMETER', 'PDBX_SG_PROJECT',
                                                                     'RCSB_ATOM_COUNT', 'RCSB_BINDING', 'RCSB_EXTERNAL_REFERENCES', 'RCSB_HAS_CHEMICAL_SHIFT_FILE', 'RCSB_HAS_ED_MAP_FILE',
                                                                     'RCSB_HAS_FOFC_FILE', 'RCSB_HAS_NMR_V1_FILE', 'RCSB_HAS_NMR_V2_FILE', 'RCSB_HAS_STRUCTURE_FACTORS_FILE', 'RCSB_HAS_TWOFOFC_FILE',
                                                                     'RCSB_HAS_VALIDATION_REPORT', 'RCSB_LATEST_REVISION', 'RCSB_MODELS_COUNT', 'RCSB_MOLECULAR_WEIGHT', 'RCSB_PUBMED', 'RCSB_RELEASE_DATE',
                                                                     'REFINE', 'REFINE_ANALYZE', 'REFINE_HIST', 'REFINE_LS_RESTR', 'REFLNS', 'REFLNS_SHELL', 'SOFTWARE', 'STRUCT', 'STRUCT_KEYWORDS', 'SYMMETRY',
                                                                     'RCSB_LOAD_STATUS', 'RCSB_ENTRY_CONTAINER_IDENTIFIERS', 'PDBX_SERIAL_CRYSTALLOGRAPHY_MEASUREMENT',
                                                                     'PDBX_SERIAL_CRYSTALLOGRAPHY_SAMPLE_DELIVERY',
                                                                     'PDBX_SERIAL_CRYSTALLOGRAPHY_SAMPLE_DELIVERY_INJECTION',
                                                                     'PDBX_SERIAL_CRYSTALLOGRAPHY_SAMPLE_DELIVERY_FIXED_TARGET',
                                                                     'PDBX_SERIAL_CRYSTALLOGRAPHY_DATA_REDUCTION'],
                                                         'EXCLUDE': [], 'SLICE': None},
                              'repository_holdings_update_v0_1': {'INCLUDE': ['rcsb_repository_holdings_update'], 'EXCLUDE': [], 'SLICE': None},
                              'repository_holdings_current_v0_1': {'INCLUDE': ['rcsb_repository_holdings_current'], 'EXCLUDE': [], 'SLICE': None},
                              'repository_holdings_unreleased_v0_1': {'INCLUDE': ['rcsb_repository_holdings_unreleased'], 'EXCLUDE': [], 'SLICE': None},
                              'repository_holdings_removed_v0_1': {'INCLUDE': ['rcsb_repository_holdings_removed'], 'EXCLUDE': [], 'SLICE': None},
                              'repository_holdings_removed_audit_authors_v0_1': {'INCLUDE': ['rcsb_repository_holdings_removed_audit_author'], 'EXCLUDE': [], 'SLICE': None},
                              'repository_holdings_superseded_v0_1': {'INCLUDE': ['rcsb_repository_holdings_superseded'], 'EXCLUDE': [], 'SLICE': None},
                              'repository_holdings_transferred_v0_1': {'INCLUDE': ['rcsb_repository_holdings_transferred'], 'EXCLUDE': [], 'SLICE': None},
                              'repository_holdings_insilico_models_v0_1': {'INCLUDE': ['rcsb_repository_holdings_insilico_models'], 'EXCLUDE': [], 'SLICE': None},
                              'cluster_members_v0_1': {'INCLUDE': ['rcsb_entity_sequence_cluster_list'], 'EXCLUDE': [], 'SLICE': None},
                              'cluster_provenance_v0_1': {'INCLUDE': ['software', 'citation', 'citation_author'], 'EXCLUDE': [], 'SLICE': None},
                              'entity_members_v0_1': {'INCLUDE': ['rcsb_entity_sequence_cluster_list'], 'EXCLUDE': [], 'SLICE': None},
                              'rcsb_data_exchange_status_v0_1': {'INCLUDE': ['rcsb_data_exchange_status'], 'EXCLUDE': [], 'SLICE': None},
                              }

    __collectionAttributeNames = {'pdbx_v5_0_2': ['entry.id'],
                                  'pdbx_ext_v5_0_2': ['entry.id'],
                                  'pdbx_core_entity_v5_0_2': ['entry.id', 'entity.id'],
                                  'pdbx_core_assembly_v5_0_2': ['entry.id', 'pdbx_struct_assembly.id'],
                                  'pdbx_core_entry_v5_0_2': ['entry.id'],
                                  'bird_v5_0_2': ['pdbx_reference_molecule.prd_id'],
                                  'family_v5_0_2': ['pdbx_reference_molecule_family.family_prd_id'],
                                  'chem_comp_v5_0_2': ['chem_comp.component_id'],
                                  'bird_chem_comp_v5_0_2': ['chem_comp.component_id'],
                                  'repository_holdings_update_v0_1': ['update_id'],
                                  'repository_holdings_current_v0_1': ['update_id'],
                                  'repository_holdings_unreleased_v0_1': ['update_id'],
                                  'repository_holdings_removed_v0_1': ['update_id'],
                                  'repository_holdings_removed_audit_authors': ['update_id'],
                                  'repository_holdings_superseded_v0_1': ['update_id'],
                                  'repository_holdings_transferred_v0_1': ['update_id'],
                                  'repository_holdings_insilico_models_v0_1': ['update_id'],
                                  'cluster_members_v0_1': ['update_id'],
                                  'cluster_provenance_v0_1': ['software.name'],
                                  'entity_members_v0_1': ['update_id'],
                                  'rcsb_data_exchange_status_v0_1': ['update_id', 'database_name', 'object_name'],
                                  }
    #
    __collectionSubCategoryAggregates = {'cluster_members_v0_1': ['sequence_membership'],
                                         'entity_members_v0_1': ['cluster_membership']
                                         }

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: (below)

        """
        super(SchemaDocumentHelper, self).__init__(**kwargs)

    def getCollections(self, schemaName):
        cL = []
        try:
            cL = SchemaDocumentHelper.__schemaCollectionNames[schemaName]
        except Exception as e:
            logger.debug("Schema definitiona name %s failing with %s" % (schemaName, str(e)))
        return cL

    def getCollectionMap(self):
        try:
            return SchemaDocumentHelper.__schemaCollectionNames
        except Exception as e:
            logger.debug("Failing with %s" % (str(e)))
        return {}

    def getExcluded(self, collectionName):
        '''  For input collection, return the list of excluded schema identifiers.

        '''
        includeL = []
        try:
            includeL = [tS.upper() for tS in SchemaDocumentHelper.__schemaContentFilters[collectionName]['EXCLUDE']]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return includeL

    def getIncluded(self, collectionName):
        '''  For input collection, return the list of included schema identifiers.

        '''
        excludeL = []
        try:
            excludeL = [tS.upper() for tS in SchemaDocumentHelper.__schemaContentFilters[collectionName]['INCLUDE']]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return excludeL

    def getSliceFilter(self, collectionName):
        '''  For input collection, return an optional slice filter or None.

        '''
        sf = None
        try:
            sf = SchemaDocumentHelper.__schemaContentFilters[collectionName]['SLICE']
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return sf

    def getDocumentKeyAttributeNames(self, collectionName):
        r = []
        try:
            return SchemaDocumentHelper.__collectionAttributeNames[collectionName]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return r

    def getSubCategoryAggregates(self, collectionName):
        r = []
        try:
            return SchemaDocumentHelper.__collectionSubCategoryAggregates[collectionName]
        except Exception as e:
            logger.debug("Collection %s failing with %s" % (collectionName, str(e)))
        return r
