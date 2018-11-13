##
# File:    DictMethodRunner.py
# Author:  J. Westbrook
# Date:    18-Aug-2018
# Version: 0.001 Initial version
#
# Updates:
# 12-Nov-2018 jdw Run block methods after category and attribute methods.
#
##
"""
Manage the invocation of dictionary methods implemented in helper classes.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
from operator import itemgetter

from mmcif.api.DictionaryApi import DictionaryApi

from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class DictMethodRunner(object):
    """ Manage the invocation of dictionary methods implemented in helper classes.

    """

    def __init__(self, dictLocators, methodHelper=None, **kwargs):
        """
        Args:
            dictLocators (list string): dictionary locator list
            methodHelper (class instance, optional): a subclass of of methodoHelperBase.

        """
        self.__dApi = self.__setup(dictLocators, **kwargs)
        self.__methodD = self.__getMethodInfo()
        logger.debug("Method index %r" % self.__methodD.items())
        self.__methodHelper = methodHelper

    def __setup(self, dictLocators, **kwargs):
        """ Return an instance of a dictionary API instance for the input dictionary list
        """
        mU = MarshalUtil()
        containerList = []
        for dictLocator in dictLocators:
            containerList.extend(mU.doImport(dictLocator, format="mmcif-dict"))
        #
        dApi = DictionaryApi(containerList=containerList, consolidate=True, verbose=True)
        return dApi

    def __getMethodInfo(self):
        methodD = {}
        methodIndex = self.__dApi.getMethodIndex()
        for item, mrL in methodIndex.items():
            for mr in mrL:
                mId = mr.getId()
                catName = mr.getCategoryName()
                atName = mr.getAttributeName()
                mType = mr.getType()
                if (catName, atName) not in methodD:
                    methodD[(catName, atName)] = []
                methDef = self.__dApi.getMethod(mId)
                mLang = methDef.getLanguage()
                mCode = methDef.getCode()
                mImplement = methDef.getInline()
                mPriority = methDef.getPriority()
                d = {'METHOD_LANGUAGE': mLang, 'METHOD_IMPLEMENT': mImplement, 'METHOD_TYPE': mType, 'METHOD_CODE': mCode, 'METHOD_PRIORITY': mPriority}
                methodD[(catName, atName)].append(d)
            #
        ##
        logger.debug("Method dictionary %r" % methodD)
        return methodD

    def __invokeAttributeMethod(self, dataContainer, catName, atName, methodName):
        """
        """
        ok = False
        try:
            theMeth = getattr(self.__methodHelper, methodName, None)
            ok = theMeth(dataContainer, catName, atName)
        except Exception as e:
            logger.exception("Failed invoking attribute %s %s method %r with %s" % (catName, atName, methodName, str(e)))
        return ok

    def __invokeCategoryMethod(self, dataContainer, catName, methodName):
        """
        """
        ok = False
        try:
            theMeth = getattr(self.__methodHelper, methodName, None)
            ok = theMeth(dataContainer, catName)
        except Exception as e:
            logger.exception("Failed invoking category %s method %r with %s" % (catName, methodName, str(e)))
        return ok

    def __invokeDatablockMethod(self, dataContainer, blockName, methodName):
        """
        """
        ok = False
        try:
            theMeth = getattr(self.__methodHelper, methodName, None)
            ok = theMeth(dataContainer, blockName)
        except Exception as e:
            logger.exception("Failed invoking block %s method %r with %s" % (blockName, methodName, str(e)))
        return ok

    def apply(self, dataContainer):
        """ Apply datablock, category and attribute dictionary methods on the input data container.
        """
        mTupL = self.__getCategoryMethods(dataContainer)
        logger.debug("Category methods %r" % mTupL)
        for catName, _, methodName, _ in mTupL:
            self.__invokeCategoryMethod(dataContainer, catName, methodName)

        mTupL = self.__getAttributeMethods(dataContainer)
        logger.debug("Attribute methods %r" % mTupL)
        for catName, atName, methodName, _ in mTupL:
            self.__invokeAttributeMethod(dataContainer, catName, atName, methodName)

        mTupL = self.__getDatablockMethods(dataContainer)
        logger.debug("Datablock methods %r" % mTupL)
        for blockName, _, methodName, _ in mTupL:
            self.__invokeDatablockMethod(dataContainer, blockName, methodName)

        return True

    def __getDatablockMethods(self, container, methodCodes=["calculate_with_helper"]):
        mL = []
        try:
            for (dictName, _), mDL in self.__methodD.items():
                for mD in mDL:
                    if mD['METHOD_CODE'].lower() in methodCodes and mD['METHOD_TYPE'].lower() == 'datablock':
                        tL = mD['METHOD_IMPLEMENT'].split('.')
                        methodName = ''.join(tL[1:])
                        mL.append((dictName, None, methodName, mD['METHOD_PRIORITY']))
            mL = sorted(mL, key=itemgetter(3))
            return mL
        except Exception as e:
            logger.exception("Failing dictName %s with %s" % (dictName, str(e)))
        return mL

    def __getCategoryMethods(self, container, methodCodes=["calculate_with_helper"]):
        mL = []
        try:
            for (catName, _), mDL in self.__methodD.items():
                for mD in mDL:
                    if mD['METHOD_CODE'].lower() in methodCodes and mD['METHOD_TYPE'].lower() == 'category':
                        tL = mD['METHOD_IMPLEMENT'].split('.')
                        methodName = ''.join(tL[1:])
                        mL.append((catName, None, methodName, mD['METHOD_PRIORITY']))
            mL = sorted(mL, key=itemgetter(3))
            return mL
        except Exception as e:
            logger.exception("Failing catName %r with %s" % (catName, str(e)))
        return mL

    def __getAttributeMethods(self, container, methodCodes=["calculate_with_helper"]):
        mL = []
        try:
            for (catName, atName), mDL in self.__methodD.items():
                for mD in mDL:
                    if mD['METHOD_CODE'].lower() in methodCodes and mD['METHOD_TYPE'].lower() == 'attribute':
                        tL = mD['METHOD_IMPLEMENT'].split('.')
                        methodName = ''.join(tL[1:])
                        mL.append((catName, atName, methodName, mD['METHOD_PRIORITY']))
            mL = sorted(mL, key=itemgetter(3))
            return mL
        except Exception as e:
            logger.exception("Failing catName %s atName %s with %s" % (catName, atName, str(e)))
        return mL
