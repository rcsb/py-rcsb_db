##
# File:    MultiProcLogging.py
# Author:  jdw
# Date:    6-Apr-2018
# Version: 0.001
#
# Updates:
#
##
"""
Multiprocessing logging queue handler and listener.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import multiprocessing
import threading
import time

# try:
#    import Queue as queue
# except ImportError:
#    import queue

#
# import queue for exception definitions
try:
    import queue
except ImportError:
    import Queue as queue


class MultiProcLogging(object):

    def __init__(self, logger=None, format=None, level=None):
        """ Current logging instance or None - alternative format and level to be used within the bounded context.

            Redirect log requests to an multi-proc queue and a listener that
            redirects the request to handers bound to the input logger instance.

        """
        self.__handlerInitialList = []
        self.__handlerWrappedList = []
        #
        self.logger = logger if logger else logging.getLogger()
        #
        self.__loggingQueue = multiprocessing.Queue(-1)
        self.__ql = None
        self.__altFmt = logging.Formatter(format) if format else None
        self.__altLevel = level if level else None

    def __setup(self):
        #
        #  Replace current handlers with queue
        for i, hi in enumerate(list(self.logger.handlers)):
            # name = 'wrapped-{0}'.format(i)
            fmt = hi.formatter
            level = hi.level
            self.__handlerInitialList.append((hi, fmt, level))
            #
            if self.__altFmt:
                hi.setFormatter(self.__altFmt)
            if self.__altLevel:
                hi.setLevel(self.__altLevel)
            self.logger.removeHandler(hi)
        #
        # One wrapped/queue handler for the input logger (w/ all handlers)
        #
        hw = MultiProcLogQueueHandler(self.__loggingQueue)
        self.__handlerWrappedList.append(hw)
        self.logger.addHandler(hw)
        # ------------------------------------------
        #
        self.__ql = MultiProcLogQueueListener(self.__loggingQueue, [hi for hi, f, l in self.__handlerInitialList])
        self.__ql.start()
        #

    def __recover(self):
        # replace handlers and config
        time.sleep(0.1)
        #
        for wh in self.__handlerWrappedList:
            self.logger.removeHandler(wh)
        for (ih, f, l) in self.__handlerInitialList:
            ih.setFormatter(f)
            ih.setLevel(l)
            self.logger.addHandler(ih)

        # stop listening
        self.__ql.stop()
        # close the quueue
        self.__loggingQueue.close()
        self.__loggingQueue.join_thread()
        #
        return True

    def __enter__(self):
        self.__setup()
        return self.logger

    def __exit__(self, *args):
        return self.__recover()


# The following handler and listener classes are provided for Py2 compatibility
# and come from the Python 2 distribution.  They are mimimally adapted here.
#
#
class MultiProcLogQueueHandler(logging.Handler):
    """
    This logging handler sends events to a queue. Typically, it would be used together
    with a multiprocessing Queue to centralise logging to file in one process
    (in a multi-process application), so as to avoid file write contention
    between processes.
    """

    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        logging.Handler.__init__(self)
        self.queue = queue
        #
        # self.setLevel(level)
        # self.setFormatter(format)

    def enqueue(self, record):
        """
        Enqueue a record.

        The base implementation uses put_nowait. You may want to override
        this method if you want to use blocking, timeouts or custom queue
        implementations.
        """
        self.queue.put_nowait(record)

    def prepare(self, record):
        """
        Prepares a record for queuing. The object returned by this method is
        enqueued.

        The base implementation formats the record to merge the message
        and arguments, and removes unpickleable items from the record
        in-place.

        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also puts the message into
        # record.message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info attribute, as it's no longer needed and, if not None,
        # will typically not be pickleable.
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        return record

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue, preparing it for pickling first.
        """
        try:
            self.enqueue(self.prepare(record))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)


class MultiProcLogQueueListener(object):
    """
    This class implements an internal threaded listener which watches for
    LogRecords being added to a queue, removes them and passes them to a
    list of handlers for processing.
    """
    _sentinel = None

    def __init__(self, queue, handlerL):
        """
        Initialise an instance with the specified queue and handlers.
        """
        self.queue = queue
        self.handlers = handlerL
        self._stop = threading.Event()
        self._thread = None

    def dequeue(self, block):
        """
        Dequeue a record and return it, optionally blocking.

        The base implementation uses get. You may want to override this method
        if you want to use timeouts or work with custom queue implementations.
        """
        return self.queue.get(block)

    def start(self):
        """
        Start the listener.

        This starts up a background thread to monitor the queue for
        LogRecords to process.
        """
        self._thread = t = threading.Thread(target=self._monitor)
        t.setDaemon(True)
        t.start()

    def prepare(self, record):
        """
        Prepare a record for handling.

        This method just returns the passed-in record. You may want to
        override this method if you need to do any custom marshalling or
        manipulation of the record before passing it to the handlers.
        """
        return record

    def handle(self, record):
        """
        Handle a record.

        This just loops through the handlers offering them the record
        to handle.
        """
        record = self.prepare(record)
        for handler in self.handlers:
            handler.handle(record)

    def _monitor(self):
        """
        Monitor the queue for records, and ask the handler
        to deal with them.

        This method runs on a separate, internal thread.
        The thread will terminate if it sees a sentinel object in the queue.
        """
        while not self._stop.isSet():
            try:
                record = self.dequeue(True)
                if record is self._sentinel:
                    break
                self.handle(record)
            except queue.Empty:
                pass
        # There might still be records in the queue.
        while True:
            try:
                record = self.dequeue(False)
                if record is self._sentinel:
                    break
                self.handle(record)
            except queue.Empty:
                break

    def stop(self):
        """
        Stop the listener.

        This asks the thread to terminate, and then waits for it to do so.
        Note that if you don't call this before your application exits, there
        may be some records still left on the queue, which won't be processed.
        """
        self._stop.set()
        self.queue.put_nowait(self._sentinel)
        self._thread.join(5.0)
        self._thread = None
