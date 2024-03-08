##
# File: MmseqsSequenceClustersExec.py
#
##
__docformat__ = "restructuredtext en"
__license__ = "Apache 2.0"

import argparse
import logging
import os
from rcsb.utils.seqalign import MMseqsUtils

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_fasta_file", default=None, action="store_true", required=True, help="The input FASTA file with all sequences to cluster")
    parser.add_argument("--id_level", default=None, action="store_true", required=True, help="The identity level for which we want clustering")
    parser.add_argument("--working_path", default=False, action="store_true", required=True, help="The directory to store files, including the final output file")
    parser.add_argument("--outfile_template", default=False, action="store_true", required=True, help="The name of the output files with 2 %s place holders: %(clusterType)s and %(level)s")
    parser.add_argument("--clustering_strategy", default=False, action="store_true", required=True, help="The mmseqs2 clustering strategy: easy-cluster or easy-linclust")
    parser.add_argument("--workers", default=1, action="store_true", required=False, help="The number of worker threads for mmseqs2 to use")
    parser.add_argument("--coverage", default=None, action="store_true", required=True, help="The coverage threshold for clustering")
    parser.add_argument("--mmseqs_bin", default="/usr/bin/mmseqs2", action="store_true", required=False, help="The path to the mmseqs2 executable")
    # strategy="easy-cluster"
    # strategy="easy-linclust"
    args = parser.parse_args()

    input_fasta_file = args.input_fasta_file
    idLevel = args.id_level
    mmseqsClusteringWorkingPath = args.working_path
    mmseqsClusterOutputFileTemplate = args.outfile_template
    strategy = args.strategy
    workers = args.workers
    coverage = args.coverage
    mmseqs_bin = args.mmseqs_bin

    if not os.path.isdir(mmseqsClusteringWorkingPath):
        os.makedirs(mmseqsClusteringWorkingPath)
    # This is the final output file.
    # Note that it can be anywhere, no need to be in the same dir as mmseqs output working dir
    cluster_file_name = mmseqsClusterOutputFileTemplate % ({"clusterType": "entity", "level": idLevel})
    cluster_file = os.path.join(mmseqsClusteringWorkingPath, cluster_file_name)

    mmseqs_gen = MMseqsUtils(mmseqsBinPath=mmseqs_bin)
    mmseqs_gen.cluster(input_fasta_file, idLevel, coverage, strategy, mmseqsClusteringWorkingPath, cluster_file, workers)


if __name__ == "__main__":
    main()
