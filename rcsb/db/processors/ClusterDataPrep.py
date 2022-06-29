##
# File: ClusterDataPrep.py
# Author:  J. Westbrook
# Date: 23-Jun-2018
#
# Update:
#    24-Jun-2018 jdw update organization of extracted data sets
#     6-Jul-2018 jdw harmonize naming with extension dictionary
#     1-Jun-2022 dwp Expect argument clusterFileNameTemplate to be passed in from luigi configuration
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


class ClusterDataPrep(object):
    """
    Extract data for either entity or instance clusters.  For the entity case,

    For example -
     _rcsb_entity_sequence_cluster_list.data_set_id

    These items are assigned to sub_category = 'sequence_membership':

     _rcsb_entity_sequence_cluster_list.entry_id
     _rcsb_entity_sequence_cluster_list.entity_id

    These following items are assigned to 'sub_category'  = 'cluster_membership'

     _rcsb_entity_sequence_cluster_list.identity
     _rcsb_entity_sequence_cluster_list.cluster_id

    This above table definition is delivered as described and in a more compressed form in which
    the sub_category items are rolled-up into membership dictioanries:

    Document organized by polymer entity:

     [{'data_set_id': str, 'entry_id': str, 'entity_id': str, 'cluster_membership': [{'identity': int,  cluster_id': int }] }, ...]

     and document organized by identity and sequence cluster id:

     [{'data_set_id': str, 'identity': int, cluster_id': int, 'sequence_membership': [{'entry_id': str,  'entity_id': str }] }, ...]

    """

    def __init__(self, **kwargs):
        self.__workPath = kwargs.get("workPath", None)
        self.__instanceSchemaName = kwargs.get("instanceSchemaName", "rcsb_instance_sequence_cluster_list")
        self.__entitySchemaName = kwargs.get("entitySchemaName", "rcsb_entity_sequence_cluster_list")
        self.__clusterSchemaName = kwargs.get("clusterSchemaName", "rcsb_entity_sequence_cluster_identifer_list")
        self.__entityAttributeName = kwargs.get("entityAttributeName", "entity_id")
        self.__instanceAttributeName = kwargs.get("instanceAttributeName", "instance_id")
        # Just a note for the assumned naming conventions clusters-by-%(clusterType)s-%(level)s.txt of the cluster data files
        clusterFileNameTemplate = kwargs.get("clusterFileNameTemplate", None)
        self.__clusterFileNameTemplate = clusterFileNameTemplate if clusterFileNameTemplate else "clusters-by-%(clusterType)s-%(level)s.txt"

    def extract(self, dataSetId, clusterSetLocator, levels, clusterType="entity"):
        """Extract cluster membership details from an RSCB sequence cluster data set.   Data are
        returned for either entity or instance cluster types for all sequence identiry levels in the
        data set.  Naming follows conventions in the RCSB extension dictionary.


        Args:
            dataSetId (str): data set identifier (e.g., 2018_24 (week in year))
            clusterSetLocator (str): locator for the cluster data set
            levels (list):  list of sequence identity levels (integer percent)
            clusterType (str, optional): type of sequences in the data set (entity or chain|instance)

        Returns:
            TYPE: cifD, docBySequeneD, docByClusterD - dictioanries with CIF, sequence, and cluster organizations.

        """
        cifD = {}
        docBySequenceD = {}
        docByClusterD = {}
        try:
            ok = True
            if clusterType not in ["entity", "chain", "instance"]:
                ok = False
            # Levels must be string values internally -
            levelList = [str(level) for level in levels]
            #
            schemaNameMembers = self.__clusterSchemaName
            if clusterType.lower() == "entity":
                clusterTypeKey = self.__entityAttributeName
                schemaNameMembership = self.__entitySchemaName
            elif clusterType.lower() in ["chain", "instance"]:
                clusterTypeKey = self.__instanceAttributeName
                schemaNameMembership = self.__instanceSchemaName
            else:
                ok = False
        except Exception:
            ok = False

        if not ok:
            return cifD, docBySequenceD, docByClusterD
        #
        mU = MarshalUtil(workPath=self.__workPath)
        # mD[<level>] = d[<sequence_id>] = cluster_id
        mD = {}
        cD = {}
        for level in levelList:
            levelLoc = os.path.join(clusterSetLocator, self.__clusterFileNameTemplate % ({"clusterType": clusterType, "level": level}))
            cL = mU.doImport(levelLoc, fmt="list")
            clusterD = {ii: line.split() for ii, line in enumerate(cL, 1)}
            logger.debug("Cluster level %s length %d", level, len(clusterD))
            cD[level] = clusterD
            mD[level] = self.__makeIdDict(clusterD)

        # In case the clusters are not homogeneous
        sL = []
        for level, sD in mD.items():
            sL.extend(list(sD.keys()))
        seqIdL = sorted(list(set(sL)))
        #
        rD = {}
        for seqId in seqIdL:
            memberL = []
            for level in levelList:
                if seqId in mD[level]:
                    memberL.append(int(mD[level][seqId]))
                else:
                    logger.info("Missing value for level %s sequence id  %s\n", level, seqId)
                    memberL.append(None)
            # membership tuple
            rD[seqId] = tuple(memberL)
        logger.info("Length of cluster solution %d", len(rD))
        #
        #  - Document friendly organizations -
        #
        cL = []
        for seqId, memberTup in rD.items():
            seqIdL = seqId.rsplit("_", 1)
            dD = {"data_set_id": dataSetId, "entry_id": seqIdL[0], clusterTypeKey: seqIdL[1]}
            mL = []
            for ii, cId in enumerate(memberTup):
                if cId:
                    mL.append({"identity": int(levelList[ii]), "cluster_id": cId})
            dD["cluster_membership"] = mL
            cL.append(dD)
        docBySequenceD[schemaNameMembership] = cL
        #
        cL = []
        for level, clusterD in cD.items():
            for cId, mL in clusterD.items():
                dD = {"data_set_id": dataSetId, "identity": int(level), "cluster_id": cId}
                tL = []
                for seqId in mL:
                    seqIdL = seqId.rsplit("_", 1)
                    tL.append({"entry_id": seqIdL[0], clusterTypeKey: seqIdL[1]})
                dD["sequence_membership"] = tL
                cL.append(dD)
        docByClusterD[schemaNameMembers] = cL
        # - CIF friendly organization --
        #
        cL = []
        for ii, level in enumerate(levelList):
            for ky, cTup in rD.items():
                seqIdL = ky.rsplit("_", 1)
                dD = {"data_set_id": dataSetId, "entry_id": seqIdL[0], clusterTypeKey: seqIdL[1], "identity": int(level), "cluster_id": int(cTup[ii])}
                cL.append(dD)
        cifD[schemaNameMembership] = cL
        #
        return cifD, docBySequenceD, docByClusterD

    def __makeIdDict(self, clusterD):
        """Internal method returning the dictionary d[sequence_id] = cluster_id"""
        idD = {}
        for k, vL in clusterD.items():
            for tid in vL:
                idD[tid] = k
        return idD
