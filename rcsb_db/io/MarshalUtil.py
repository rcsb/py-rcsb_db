##
# File: MarshalUtil.py
# Date: 4-Jun-2018
#
# Updates:
#      19-Jun-2018 jdw propagate the class workpath to serialize/deserialize methods explicitly
#
# For py 27 pip install backports.tempfile
##

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import tempfile

from rcsb_db.io.FileUtil import FileUtil
from rcsb_db.io.IoUtil import IoUtil

logger = logging.getLogger(__name__)


class MarshalUtil(object):
    """
        ftp://username:password@hostname/

        # toCif
        #
        with  create_tmp_dir:
            create working file
            copy working file to remote destination


        # fromCif
        #
        if local:
            read local file
        else:
            with create_tmp_dir:
                fetch working file from remote location
                read working file

    """

    def __init__(self, **kwargs):
        self.__workPath = kwargs.get('workPath', '.')
        self.__workDirSuffix = kwargs.get('workDirSuffix', 'marshall_')
        self.__workDirPrefix = kwargs.get('workDirSuffix', '_tempdir')
        #
        self.__fileU = FileUtil(workPath=self.__workPath)
        self.__ioU = IoUtil()

    def doExport(self, locator, obj, format="mmcif", marshalHelper=None, **kwargs):
        """
        """
        try:
            ret = False
            cacheLocalFlag = kwargs.get('cacheLocal', False)
            localFlag = self.__fileU.isLocal(locator)
            if marshalHelper:
                myObj = marshalHelper(obj, **kwargs)
            else:
                myObj = obj
            #
            if localFlag and not cacheLocalFlag:
                localFilePath = self.__fileU.getFilePath(locator)
                ret = self.__ioU.serialize(localFilePath, myObj, format=format, workPath=self.__workPath, **kwargs)
            else:
                with tempfile.TemporaryDirectory(suffix=self.__workDirSuffix, prefix=self.__workDirPrefix, dir=self.__workPath) as tmpDirName:
                    # write a local copy then copy to destination -
                    #
                    localFilePath = os.path.join(self.__workPath, tmpDirName, self.__fileU.getFileName(locator))
                    ok1 = self.__ioU.serialize(localFilePath, myObj, format=format, workPath=self.__workPath, **kwargs)
                    ok2 = True
                    if ok1:
                        ok2 = self.__fileU.put(localFilePath, locator, **kwargs)
                ret = ok1 and ok2
        except Exception as e:
            logger.exception("Exporting locator %r failing with %s" % (locator, str(e)))

        return ret

    def doImport(self, locator, format="mmcif", marshalHelper=None, **kwargs):
        """
        """
        try:
            ret = None
            cacheLocalFlag = kwargs.get('cacheLocal', False)
            localFlag = self.__fileU.isLocal(locator)
            #
            if localFlag and not cacheLocalFlag:
                filePath = self.__fileU.getFilePath(locator)
                myObj = self.__ioU.deserialize(filePath, format=format, workPath=self.__workPath, **kwargs)
            else:
                #
                with tempfile.TemporaryDirectory(suffix=self.__workDirSuffix, prefix=self.__workDirPrefix, dir=self.__workPath) as tmpDirName:
                    # fetch first then read a local copy -
                    #
                    localFilePath = os.path.join(self.__workPath, tmpDirName, self.__fileU.getFileName(locator))
                    self.__fileU.get(locator, localFilePath, **kwargs)
                    myObj = self.__ioU.deserialize(localFilePath, format=format, workPath=self.__workPath, **kwargs)

            if marshalHelper:
                ret = marshalHelper(myObj, **kwargs)
            else:
                ret = myObj
        except Exception as e:
            logger.exception("Importing locator %r failing with %s" % (locator, str(e)))
            ret = None
        return ret
