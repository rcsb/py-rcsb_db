def doRangesOverlap(r1, r2):
    if r1.start == r1.stop or r2.start == r2.stop:
        return False
    return (r1.start < r2.stop and r1.stop > r2.start) or (r1.stop > r2.start and r2.stop > r1.start)


def splitRangeList(rngL):
    """Separate the input list range objects into sublists of non-overlapping range segments

    Args:
        rngL (list): list or range objects

    Returns:
        (dict): dictionary of sublists (w/ keys 1,2,3) of non-overlapping range segments
    """
    grpD = {}
    numG = 0
    try:
        rngL.sort(key=lambda r: r.stop - r.start + 1, reverse=True)
        for rng in rngL:
            inGroup = False
            igrp = 0
            for grp, trngL in grpD.items():
                inGroup = any([doRangesOverlap(rng, trng) for trng in trngL])
                if inGroup:
                    igrp = grp
                    break
            numG = numG if inGroup else numG + 1
            igrp = igrp if inGroup else numG
            grpD.setdefault(igrp, []).append(rng)
    except Exception as e:
        # logger.exception("Failing with %s", str(e))
        print(str(e))

    return grpD


def testRangeSplit():
    tupL = [(1, 2), (1, 3), (1, 10), (11, 20), (19, 25), (30, 100), (1, 100), (200, 300), (350, 1400)]
    # tupL.sort(key=lambda t: t[1] - t[0] + 1, reverse=True)
    rngL = []
    for tup in tupL:
        rngL.append(range(tup[0], tup[1]))
    for ii in range(1, len(rngL)):
        print(ii)
        print(doRangesOverlap(rngL[ii - 1], rngL[ii]))

    grpD = splitRangeList(rngL)
    print(grpD)


if __name__ == "__main__":
    testRangeSplit()
