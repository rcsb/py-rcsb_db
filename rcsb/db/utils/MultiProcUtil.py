##
# File:    MultiProcUtil.py
# Author:  jdw
# Date:    3-Nov-2014
# Version: 0.001
#
# Updates:
# 9-Nov-2014  extend API to include variable number of result lists computed by each worker.
# 11-Nov-2014 extend API with additional options and working directory configuration
# 21-Apr-2015 jdw change the termination and reaping protocol.
#  9-Oct-2017 jdw add chunkSize option such that the input dataList to provide more granular distribution
#                 data among the works.  Defaults to numProc if unspecified.
# 27-Mar-2018 jdw add check for empty input
##
"""
Multiprocessing execution wrapper supporting tasks with list of inputs and a variable number of output lists.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import multiprocessing

logger = logging.getLogger(__name__)


class MultiProcWorker(multiprocessing.Process):

    """  Multi-processing working method wrapper --

         Worker method must support the following prototype -

         sucessList,resultList,diagList=workerFunc(runList=nextList,procName, optionsD, workingDir)
    """

    def __init__(self, taskQueue, successQueue, resultQueueList, diagQueue, workerFunc, verbose=False, optionsD=None, workingDir='.'):
        multiprocessing.Process.__init__(self)
        self.__taskQueue = taskQueue
        self.__successQueue = successQueue
        self.__resultQueueList = resultQueueList
        self.__diagQueue = diagQueue
        #
        self.__verbose = verbose
        self.__debug = True
        self.__workerFunc = workerFunc
        #
        self.__optionsD = optionsD if optionsD is not None else {}
        self.__workingDir = workingDir
        #

    def run(self):
        processName = self.name
        while True:
            nextList = self.__taskQueue.get()
            if nextList is None:
                # end of queue condition
                logger.debug("%s completed task list" % processName)
                break
            #
            rTup = self.__workerFunc(dataList=nextList, procName=processName, optionsD=self.__optionsD, workingDir=self.__workingDir)
            logger.debug("%s task list length %d rTup length %d" % (processName, len(nextList), len(rTup)))
            self.__successQueue.put(rTup[0])
            for ii, rq in enumerate(self.__resultQueueList):
                rq.put(rTup[ii + 1])
            self.__diagQueue.put(rTup[-1])
        return


class MultiProcUtil(object):
    def __init__(self, verbose=True):
        self.__verbose = verbose
        self.__workFunc = None
        self.__optionsD = {}
        self.__workingDir = '.'
        self.__loggingMP = True
        self.__sentinel = None

    def setOptions(self, optionsD):
        """ A dictionary of options that is passed as an argument to the worker function
        """
        self.__optionsD = optionsD

    def setWorkingDir(self, workingDir):
        """ A working directory option that is passed as an argument to the worker function.
        """
        self.__workingDir = workingDir

    def set(self, workerObj=None, workerMethod=None):
        """  WorkerObject is the instance of object with method named workerMethod()

             Worker method must support the following prototype -

             sucessList,resultList,diagList=workerFunc(runList=nextList, procName, optionsD, workingDir)
        """
        try:
            self.__workerFunc = getattr(workerObj, workerMethod)
            return True
        except AttributeError:
            logger.error("Object/attribute error")
            return False

    ##
    def runMulti(self, dataList=None, numProc=0, numResults=1, chunkSize=0):
        """ Start 'numProc' worker methods consuming the input dataList -

            Divide the dataList into sublists/chunks of size 'chunkSize'
            if chunkSize <= 0 use chunkSize = numProc

            Returns,   successFlag true|false
                       successList (data from the inut list that succeed)
                       resultLists[numResults] --  numResults result lists
                       diagList --  unique list of diagnostics --

        """
        #
        if numProc < 1:
            numProc = multiprocessing.cpu_count() * 2

        lenData = len(dataList)
        numProc = min(numProc, lenData)
        chunkSize = min(lenData, chunkSize)

        if chunkSize <= 0:
            numLists = numProc
        else:
            numLists = int(lenData / int(chunkSize))
        #
        subLists = [dataList[i::numLists] for i in range(numLists)]
        #
        if subLists and len(subLists) > 0:
            logger.debug("Running with numProc %d subtask count %d subtask length ~ %d" % (numProc, len(subLists), len(subLists[0])))
        #
        taskQueue = multiprocessing.Queue()
        successQueue = multiprocessing.Queue()
        diagQueue = multiprocessing.Queue()

        rqList = []
        retLists = []
        for ii in range(numResults):
            rqList.append(multiprocessing.Queue())
            retLists.append([])

        #
        #  Create list of worker processes
        #
        workers = [
            MultiProcWorker(
                taskQueue,
                successQueue,
                rqList,
                diagQueue,
                self.__workerFunc,
                verbose=self.__verbose,
                optionsD=self.__optionsD,
                workingDir=self.__workingDir) for i in range(numProc)]
        for w in workers:
            w.start()

        for subList in subLists:
            if len(subList) > 0:
                taskQueue.put(subList)

        for i in range(numProc):
            taskQueue.put(None)

        # np = numProc
        np = len(subLists)
        successList = []
        diagList = []
        tL = []
        while np:
            r = successQueue.get()
            if r is not None and len(r) > 0:
                successList.extend(r)

            d = diagQueue.get()
            if d is not None and len(d) > 0:
                for tt in d:
                    if len(str(tt).strip()) > 0:
                        tL.append(tt)

            for ii, rq in enumerate(rqList):
                r = rq.get()
                if r is not None and len(r) > 0:
                    retLists[ii].extend(r)

            np -= 1
        #
        diagList = list(set(tL))
        logger.debug("Input task length %d success length %d" % (len(dataList), len(successList)))
        try:
            for w in workers:
                w.terminate()
                w.join(1)
        except Exception as e:
            logger.error("termination/reaping failing\n")
            logger.exception("Failing with %s" % str(e))

        #
        if len(dataList) == len(successList):
            logger.debug("Complete run  - input task length %d success length %d" % (len(dataList), len(successList)))
            return True, [], retLists, diagList
        else:
            # logger.debug("data list %r " % dataList[:4])
            # logger.debug("successlist %r " % successList[:4])
            failList = list(set(dataList) - set(successList))
            logger.debug("Incomplete run  - input task length %d success length %d fail list %d" % (len(dataList), len(successList), len(failList)))

            return False, failList, retLists, diagList
