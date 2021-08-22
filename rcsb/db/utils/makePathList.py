##
# File   makePathList.py
# Date:  18-Feb-2018
#
#  Get the current list of release entry files in our data release file system.
#
##
import logging
import os

try:
    import os.scandir as scandir
except Exception:
    import scandir

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def makePdbxPathList(fp, cachePath=".", skipFile=None):
    """Return the list of pdbx file paths in the current repository."""

    try:
        skipD = {}
        if skipFile and os.access(skipFile, "r"):
            with open(skipFile, "r", encoding="utf-8") as ifh:
                for line in ifh:
                    idcode = str(line[:-1]).strip().lower() + ".cif"
                    skipD[idcode] = idcode
            logger.info("Skip list length %d", len(skipD))
        #
        with open(fp, "w", encoding="utf-8") as ofh:
            for root, _, files in scandir.walk(cachePath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) == 8 and name not in skipD:
                        ofh.write("%s\n" % os.path.join(root, name))
            #
            # logger.info("\nFound %d files in %s\n" % (len(pathList), cachePath))
        return True
    except Exception as e:
        logger.exception("Failing with %s", str(e))

    return False


if __name__ == "__main__":
    ok = makePdbxPathList("PDBXPATHLIST.txt", cachePath="/net/beta_data/mmcif-pdbx-load-v5.0", skipFile="./DONE.LIST")
