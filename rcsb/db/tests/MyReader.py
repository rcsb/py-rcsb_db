##
# File MyReader.pu
#
#
##
import logging

from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class MyReader(object):
    def __init__(self):
        pass

    def readTestFile(self, filePath):
        """ Read input SIFTS summary file and return a list of dictionaries.
        """
        cL = []
        try:
            mU = MarshalUtil()
            cL = mU.doImport(filePath, fmt="tdd")
            logger.debug("Container list %d", len(cL))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return cL


if __name__ == "__main__":
    mR = MyReader()
    dL = mR.readTestFile(filePath="struct_site.tdd")
    logger.info("First result %r", dL[0])
    iCount = 0
    jCount = 0
    uCount = 0
    sCount = 0
    aCount = 0
    for ii, d in enumerate(dL):
        try:
            if (d["pdbx_auth_comp_id"] is not None and d["pdbx_auth_comp_id"]) and (d["pdbx_auth_asym_id"] is not None and d["pdbx_auth_asym_id"]):
                sCount += 1
                continue
            if d["pdbx_evidence_code"] == "Software":
                if d["details"] is not None and (("BINDING SITE FOR RESIDUE " in d["details"]) or ("binding site for residue " in d["details"])):
                    iCount += 1
                else:
                    logger.info("%s: |%r|", d["Structure_ID"], d["details"])
                    jCount += 1
            elif d["pdbx_evidence_code"] == "Author":
                aCount += 1
            else:
                uCount += 1
        except Exception as e:
            logger.exception("Failing at (%d) %r", ii, d)
    #
    logger.info("iCount %d jCount %d uCount %d sCount %d aCount %d", iCount, jCount, uCount, sCount, aCount)
    #
