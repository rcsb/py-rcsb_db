##
# File MyReader.pu
#
#
##

import logging
import os.path

from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class MyReader(object):
    def __init__(self):
        pass

    def readTestFile(self, filePath):
        """ Read input and retun a list of dictionaries.
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
    HERE = os.path.abspath(os.path.dirname(__file__))
    fp = os.path.join(HERE, "struct_site.tdd")
    dL = mR.readTestFile(filePath=fp)
    logger.info("First result %r", dL[0])
    iCount = 0
    jCount = 0
    uCount = 0
    sCount = 0
    aCount = 0
    asCount = 0
    sqCount = 0
    ssCount = 0
    insCount = 0
    for ii, d in enumerate(dL):
        evCode = d["pdbx_evidence_code"]
        compId = None
        authAsymId = None
        authSeqId = None
        details = None
        #
        tS = d["details"]
        if not evCode or evCode.lower() == "unknown":
            continue
        if not tS:
            # logger.info("no details %r", d)
            continue
        try:
            #
            ok = False
            prefixL = [
                "BINDING SITE FOR RESIDUE ",
                "binding site for residue ",
                "Binding site for Ligand ",
                "binding site for Ligand ",
                "Binding site for Mono-Saccharide ",
                "BINDING SITE FOR MONO-SACCHARIDE ",
                "binding site for Mono-Saccharide ",
                "binding site for Poly-Saccharide ",
                "binding site for nucleotide ",
            ]
            for prefix in prefixL:
                tup = tS.partition(prefix)
                if tup[1] == prefix:
                    ff = tup[2].split(" ")
                    # binding site for Ligand residues POL d 4 through N7P d 1 bound to THR b 1
                    if ff[0] == "residues" and len(ff) > 8 and ff[4].lower() == "through":
                        compIdA = ff[1]
                        authAsymIdA = ff[2]
                        authSeqIdA = ff[3]
                        compIdA = ff[5]
                        authAsymIdA = ff[6]
                        authSeqIdA = ff[7]
                        ok = True
                        continue
                    if len(ff) == 2:
                        compId = ff[0]
                        authAsymId = ff[1][0]
                        authSeqId = ff[1][1:]
                    elif len(ff) == 3:
                        compId = ff[0]
                        authAsymId = ff[1]
                        authSeqId = ff[2]
                    ok = True
                    continue
            if ok:
                continue
            # Binding site for residues GCD A 900 and NGA A 901
            # Binding site for residues FUC A1118 and BGC A1119'
            ok = False
            prefixL = [
                "Binding site for residues ",
                "binding site for residues ",
                "BINDING SITE FOR DI-SACCHARIDE ",
                "Binding site for Di-Saccharide ",
                "binding site for Di-Saccharide ",
                "binding site for Di-peptide ",
                "Binding site for Di-peptide ",
                "binding site for Di-nucleotide ",
            ]
            for prefix in prefixL:
                tup = tS.partition(prefix)
                if tup[1] == prefix:
                    ff = tup[2].split(" ")
                    if len(ff) == 5:
                        compIdA = ff[0]
                        authAsymIdA = ff[1][0]
                        authSeqIdA = ff[0][1:]
                        compIdB = ff[3]
                        authAsymIdB = ff[4][0]
                        authSeqIdB = ff[4][1:]
                    elif len(ff) == 7:
                        compIdA = ff[0]
                        authAsymIdA = ff[1]
                        authSeqIdA = ff[2]
                        compIdB = ff[4]
                        authAsymIdB = ff[5]
                        authSeqIdB = ff[6]
                    ok = True
                    continue
            if ok:
                continue
            #
            # BINDING SITE FOR LINKED RESIDUES A 1519 A 1520 A 1521 A 1522 A 1523 A 1524 A 1525
            # BINDING SITE FOR LINKED RESIDUES A 801 to 802
            ok = False
            prefixL = ["BINDING SITE FOR LINKED RESIDUES "]
            for prefix in prefixL:
                tup = tS.partition(prefix)
                if tup[1] == prefix:
                    ff = tup[2].split(" ")
                    if len(ff) == 4 and ff[2].lower() == "to":
                        authAsymIdA = ff[0]
                        authSeqIdA = ff[1]
                        authSeqIdB = ff[3]
                    elif len(ff) == 4 and ff[2].lower() != "to":
                        authAsymIdA = ff[0]
                        authSeqIdA = ff[1]
                        authSeqIdB = ff[3]
                    elif len(ff) > 4:
                        authAsymIdA = ff[0]
                        authSeqIdA = ff[1]
                        authSeqIdB = ff[-1]
                    ok = True
                    continue
            if ok:
                continue
            #
            #
            ok = False
            prefixL = ["BINDING SITE FOR CHAIN ", "binding site for chain "]
            for prefix in prefixL:
                tup = tS.partition(prefix)
                if tup[1] == prefix:
                    ff = tup[2].split(" ")
                    authAsymId = ff[0]
                    ok = True
                    continue
            if ok:
                continue
            #
            prefixL = ["Binding site for Poly-Saccharide ", "binding site for Poly-Saccharide ", "BINDING SITE FOR DIPEPTIDE"]
            ok = False
            for prefix in prefixL:
                tup = tS.partition(prefix)
                if tup[1] == prefix:
                    details = prefix
                    ok = True
                    continue
            if ok:
                continue

            if evCode.lower() == "software":
                logger.info("evCode %r unparsed details %r", evCode, tS)

        except Exception as e:
            logger.exception("Failing with %s %r", str(e), d)
    #
    for ii, d in enumerate(dL):

        try:
            aOk = False
            if d["pdbx_evidence_code"] == "Software":
                if d["details"] is not None and (("BINDING SITE FOR RESIDUE " in d["details"]) or ("binding site for residue " in d["details"])):
                    iCount += 1
                else:
                    logger.info("%s: |%r|", d["Structure_ID"], d["details"])
                    jCount += 1
            elif d["pdbx_evidence_code"] == "Author":
                aCount += 1
                aOk = True
            else:
                logger.info("Unexpected evcode %r", d["pdbx_evidence_code"])
                uCount += 1
            #
            if (
                (d["pdbx_auth_comp_id"] is not None and d["pdbx_auth_comp_id"])
                and (d["pdbx_auth_asym_id"] is not None and d["pdbx_auth_asym_id"])
                and (d["pdbx_auth_seq_id"] is not None and d["pdbx_auth_seq_id"])
            ):
                sCount += 1
                if aOk:
                    asCount += 1
            if d["pdbx_auth_asym_id"] is not None and d["pdbx_auth_asym_id"]:
                sqCount += 1
            if (d["pdbx_auth_asym_id"] is not None and d["pdbx_auth_asym_id"]) and (d["pdbx_auth_seq_id"] is not None and d["pdbx_auth_seq_id"]):
                ssCount += 1
        except Exception as e:
            logger.exception("Failing at (%d) %r with %s!", ii, d, str(e))
    #
    logger.info("iCount %d jCount %d uCount %d sCount %d aCount %d sqCount %d ssCount %d", iCount, jCount, uCount, sCount, aCount, sqCount, ssCount)
    #
