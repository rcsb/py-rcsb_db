##
# File: FileUtil.py
#
# Skeleton implementation for File I/O
#
# Updates:
#  5-Jun-2018 jdw add support for local copy operations using shutil.copy
#
##

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

try:
    from urllib.parse import urlsplit
except Exception:
    from urlparse import urlsplit
#
import logging
import os.path
import shutil

logger = logging.getLogger(__name__)


class FileUtil(object):
    """ Skeleton implementation for File I/O operations

    """
    def __init__(self, workPath=None, **kwargs):
        self.__workPath = workPath

    #
    def get(self, remote, local, **kwargs):
        """  Fetch remote file to a local path.

        """
        try:
            ret = False
            localFlag = self.isLocal(remote)
            #
            if localFlag:
                rPath = self.getFilePath(remote)
                lPath = self.getFilePath(local)
                ret = shutil.copyfile(rPath, lPath)
            else:
                ret = False
            #
        except Exception as e:
            logger.exception("For remote %r local %r failing with %s" % (remote, local, str(e)))

        #
        return ret

    def put(self, local, remote, **kwargs):
        """ Copy local file to remote location.
        """
        try:
            ret = False
            localFlag = self.isLocal(remote)
            #
            if localFlag:
                rPath = self.getFilePath(remote)
                lPath = self.getFilePath(local)
                ret = shutil.copyfile(lPath, rPath)
            else:
                ret = False
        except Exception as e:
            logger.exception("For remote %r local %r failing with %s" % (remote, local, str(e)))
            ret = False
        return ret

    def getFilePath(self, locator):
        try:
            locSp = urlsplit(locator)
            return locSp.path
        except Exception as e:
            logger.exception("For locator %r failing with %s" % (locator, str(e)))
        return None

    def getFileName(self, locator):
        try:
            locSp = urlsplit(locator)
            (pth, fn) = os.path.split(locSp.path)
            return fn
        except Exception as e:
            logger.exception("For locator %r failing with %s" % (locator, str(e)))
        return None

    def isLocal(self, locator):
        try:
            locSp = urlsplit(locator)
            return locSp.scheme in ['', 'file']
        except Exception as e:
            logger.exception("For locator %r failing with %s" % (locator, str(e)))
        return None

    def getScheme(self, locator):
        try:
            locSp = urlsplit(locator)
            return locSp.scheme
        except Exception as e:
            logger.exception("For locator %r Failing with %s" % (locator, str(e)))
        return None
