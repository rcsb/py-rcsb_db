##
# File:    DataTransformFactory.py
# Author:  J. Westbrook
# Date:    10-Apr-2018
#
#
# Updates:
#
#  4-Sep-2018 jdw add enumeration normalization (in progress)
# 13-Feb-2019 jdw limit translateXMLCharRefs() to categories/attributes
#                 configured with a transform filter.
#                 Refactor time critical sections of processRecords()
#                 to minimize costly functon calls for simple casts.
# 24-Mar-2019 jdw adjust null value filtering
##
"""
Factory for functional elements of the transformations between input data and
and loadable data using specifications from the schema map definition.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import collections
import logging
import re
from functools import reduce

import dateutil.parser

import pytz

from rcsb.db.utils.TextUtil import unescapeXmlCharRef

TrfValue = collections.namedtuple('TrfValue', 'value, atId, origLength, isNull')

logger = logging.getLogger(__name__)


class DataTransformInfo(object):
    """ Map transformation attribute filter names to data transformation filter implementations.

    """

    def __init__(self):
        # mapD {<external attribute filter name>: <implementation names>, ...}
        self.__mapD = {'STRIP_WS': 'STRIP_WS', 'TRANSLATE_XMLCHARREFS': 'TRANSLATE_XMLCHARREFS'}

    def isImplemented(self, attributeFilter):
        ok = False
        try:
            if attributeFilter in self.__mapD:
                ok = True
        except Exception as e:
            logger.exception("Failing with %s" % str(e))

        logger.debug("Filter %r status %r" % (attributeFilter, ok))
        return ok

    def getTransformFilterName(self, attributeFilter):
        try:
            return self.__mapD[attributeFilter]
        except Exception:
            pass
        return None


class DataTransformFactory(object):
    """Factory for functional elements of the transformations between input data and
        and loadable data using specifications from the schema map definition.

        input string value -> (Null Handling -> cast type ->)

        input string iterable value ->


    """

    def __init__(self, schemaDefAccessObj, filterType):
        self.__sD = schemaDefAccessObj
        self.__wsPattern = re.compile(r"\s+", flags=re.UNICODE | re.MULTILINE)
        logger.debug("filterType %r" % filterType)
        logger.debug("Schema database %r" % self.__sD.getDatabaseName())
        self.__FLAGS = {}
        self.__FLAGS['dropEmpty'] = 'drop-empty-attributes' in filterType
        self.__FLAGS['skipMaxWidth'] = 'skip-max-width' in filterType
        self.__FLAGS['assignDates'] = 'assign-dates' in filterType
        self.__FLAGS['convertIterables'] = 'convert-iterables' in filterType
        self.__FLAGS['normalizeEnums'] = 'normalize-enums' in filterType
        self.__FLAGS['translateXMLCharRefs'] = 'translateXMLCharRefs' in filterType
        self.__FLAGS['normalizeDates'] = True
        logger.debug("FLAGS settings are %r" % self.__FLAGS)
        #
        self.__wsPattern = re.compile(r"\s+", flags=re.UNICODE | re.MULTILINE)
        self.__dti = DataTransformInfo()
        self.__dT = self.__build()
        self.__nullValueD = {'string': '', 'integer': r'\N', 'float': r'\N', 'date': r'\N', 'datetime': r'\N'}

    def __build(self):
        """ Internal method that stores transformations for each table so that these may
            be later repeatedly applied to a data stream.

           fD[tableId]'atFuncD'] -> {atId1: [f1,f2,.. ], atId2: [f1,f2,...], ...}
        """
        fD = {}
        for tableId in self.__sD.getSchemaIdList():
            tD = {}
            tObj = self.__sD.getSchemaObject(tableId)
            dt = DataTransform(tObj)
            aD = {}
            typeD = {}
            pureCastD = {}
            for atId in tObj.getAttributeIdList():
                if tObj.isOtherAttributeType(atId):
                    # skip attributes with no mapping correspondence
                    continue
                #
                aD[atId] = []
                #
                if self.__FLAGS['convertIterables'] and tObj.isIterable(atId):
                    if tObj.isAttributeStringType(atId):
                        aD[atId].append(dt.castIterableString)
                        #
                        if self.__FLAGS['translateXMLCharRefs']:
                            aD[atId].append(dt.translateXMLCharRefsIt)
                    elif tObj.isAttributeIntegerType(atId):
                        aD[atId].append(dt.castIterableInteger)
                    elif tObj.isAttributeFloatType(atId):
                        aD[atId].append(dt.castIterableFloat)
                #
                elif tObj.isAttributeStringType(atId):
                    typeD[atId] = 'string'
                    aD[atId].append(dt.castString)
                    if not self.__FLAGS['skipMaxWidth']:
                        aD[atId].append(dt.truncateString)
                    #
                    for ft in tObj.getAttributeFilterTypes(atId):
                        if self.__dti.getTransformFilterName(ft) == "STRIP_WS":
                            aD[atId].append(dt.stripWhiteSpace)
                        if self.__FLAGS['translateXMLCharRefs'] and self.__dti.getTransformFilterName(ft) == "TRANSLATE_XMLCHARREFS":
                            aD[atId].append(dt.translateXMLCharRefs)
                    #
                    # if self.__FLAGS['translateXMLCharRefs']:
                    #    aD[atId].append(dt.translateXMLCharRefs)
                elif tObj.isAttributeIntegerType(atId):
                    aD[atId].append(dt.castInteger)
                    typeD[atId] = 'integer'
                elif tObj.isAttributeFloatType(atId):
                    aD[atId].append(dt.castFloat)
                    typeD[atId] = 'float'
                elif self.__FLAGS['assignDates'] and tObj.isAttributeDateType(atId):
                    typeD[atId] = 'date'
                    aD[atId].append(dt.castDateToObj)
                elif self.__FLAGS['normalizeDates'] and tObj.isAttributeDateType(atId):
                    if tObj.getAttributeType(atId).lower() == 'datetime':
                        typeD[atId] = 'datetime'
                        aD[atId].append(dt.castDateTimeToIsoDate)
                    elif tObj.getAttributeType(atId).lower() == 'date':
                        aD[atId].append(dt.castDateToIsoDate)
                        typeD[atId] = 'date'
                else:
                    aD[atId].append(dt.castString)
                    #typeD[tObj.getAttributeName(atId)] = 'string'
                    typeD[atId] = 'string'
                #
                if self.__FLAGS['normalizeEnums'] and tObj.isEnumerated(atId):
                    logger.debug("Normalizing enums for %s %s" % (tableId, atId))
                    aD[atId].append(dt.normalizeEnum)
                #
                if len(aD[atId]) == 1 and atId in typeD and typeD[atId] in ['string', 'float', 'integer']:
                    pureCastD[tObj.getAttributeName(atId)] = typeD[atId]
            # Transformation functions keyed by attribute 'name'
            tD['atIdD'] = tObj.getMapAttributeIdDict()
            tD['atNameD'] = tObj.getMapAttributeNameDict()
            tD['atNullValues'] = tObj.getAppNullValueDict()
            tD['atFuncD'] = {tD['atIdD'][k]: v for k, v in aD.items()}
            tD['pureCast'] = pureCastD
            #
            fD[tableId] = tD
        ##
        return fD

    def get(self, tableId):
        try:
            return self.__dT[tableId]
        except Exception as e:
            logger.error("Missing table %r with error %s" % (tableId, str(e)))
        return {}

    def processRecord(self, tableId, row, attributeNameList, containerName=None):
        """
            Input row data (list) ordered according to the input attribute names list.

            Processing respects various null handling policies.


            return   d[atId]=rowdata for the input row list

        """
        # get the transform object for the current table
        #
        # Avoiding method call ...
        # dT = self.get(tableId)
        dT = self.__dT[tableId] if tableId in self.__dT else {}
        #
        atName = None
        try:
            d = {} if self.__FLAGS['dropEmpty'] else {k: v for k, v in dT['atNullValues'].items()}
            for ii, atName in enumerate(attributeNameList):
                if atName not in dT['atNameD']:
                    continue
                #
                # nullFlag = False if row[ii] else True
                nullFlag = False if row[ii] is not None else True
                #
                # Incorporate pure casts into this loop for performance -
                if atName in dT['pureCast']:
                    if nullFlag and self.__FLAGS['dropEmpty']:
                        continue
                    if ((row[ii] == '?') or (row[ii] == '.') or (row[ii]) == ''):
                        if self.__FLAGS['dropEmpty']:
                            continue
                        else:
                            d[dT['atNameD'][atName]] = self.__nullValueD[dT['pureCast'][atName]]
                            continue
                    else:
                        if dT['pureCast'][atName] == 'string':
                            d[dT['atNameD'][atName]] = row[ii]
                        elif dT['pureCast'][atName] == 'integer':
                            d[dT['atNameD'][atName]] = int(row[ii])
                        elif dT['pureCast'][atName] == 'float':
                            d[dT['atNameD'][atName]] = float(row[ii])
                        continue
                else:
                    # Apply list of functions on an initial value (i.e. TrfValue for the ii(th) element of the row.
                    vT = reduce(lambda x, y: y(x), dT['atFuncD'][atName], TrfValue(row[ii], dT['atNameD'][atName], 0, nullFlag))
                    if self.__FLAGS['dropEmpty'] and vT.isNull:
                        continue
                    d[dT['atNameD'][atName]] = vT.value
        except Exception as e:
            logger.error("Failing for %r table %s atName %s with %s" % (containerName, tableId, atName, str(e)))
            logger.exception("Failing for %r table %s atName %s with %s" % (containerName, tableId, atName, str(e)))

        return d


class DataTransform(object):
    """ Factory for functional elements of the transformations between input data and
        and loadable data using specifications from the schema map definition.

        input string value -> (Null Handling -> cast type ->)

        input string iterable value ->
    """

    def __init__(self, tObj):
        #
        self.__wsPattern = re.compile(r"\s+", flags=re.UNICODE | re.MULTILINE)

        # SchemaDef Table Object -
        #
        #
        # Set null / missing value place holders
        self.__nullValueString = ''
        self.__nullValueOther = r'\N'
        self.__tObj = tObj
        #

    def normalizeEnum(self, trfTup):
        """
            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        # origLength = len(trfTup.value)
        # if ((origLength == 0) or (trfTup.value == '?') or (trfTup.value == '.')):
        #    return TrfValue(self.__nullValueString, trfTup.atId, origLength, True)
        if trfTup.value and isinstance(trfTup.value, (list,)):
            tL = [self.__tObj.normalizeEnum(trfTup.atId, t) for t in trfTup.value]
            nVal = tL
        else:
            nVal = self.__tObj.normalizeEnum(trfTup.atId, trfTup.value)
        # logger.info("Normalizing %r %r" % (trfTup.atId, trfTup.value))
        return TrfValue(nVal, trfTup.atId, trfTup.origLength, False)

    def XcastString(self, trfTup):
        """
            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        origLength = len(trfTup.value)
        if ((origLength == 0) or (trfTup.value == '?') or (trfTup.value == '.')):
            return TrfValue(self.__nullValueString, trfTup.atId, origLength, True)
        return TrfValue(trfTup.value, trfTup.atId, origLength, False)

    def castString(self, trfTup):
        """
            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        if ((trfTup.value == '?') or (trfTup.value == '.') or (len(trfTup.value) == 0)):
            return TrfValue(self.__nullValueString, trfTup.atId, trfTup.origLength, True)
        return TrfValue(trfTup.value, trfTup.atId, trfTup.origLength, False)

    def castIterableString(self, trfTup):
        """
            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        if ((trfTup.value == '?') or (trfTup.value == '.') or (len(trfTup.value) == 0)):
            return TrfValue(self.__nullValueString, trfTup.atId, trfTup.origLength, True)
        vL = [v.strip() for v in trfTup.value.split(self.__tObj.getIterableSeparator(trfTup.atId))]
        return TrfValue(vL, trfTup.atId, trfTup.origLength, False)

    def castInteger(self, trfTup):
        """
            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        if ((trfTup.value == '?') or (trfTup.value == '.') or (len(trfTup.value) == 0)):
            return TrfValue(self.__nullValueOther, trfTup.atId, trfTup.origLength, True)
        return TrfValue(int(trfTup.value), trfTup.atId, trfTup.origLength, False)

    def castIterableInteger(self, trfTup):
        """
            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        if ((trfTup.value == '?') or (trfTup.value == '.') or (len(trfTup.value) == 0)):
            return TrfValue(self.__nullValueOther, trfTup.atId, trfTup.origLength, True)
        vL = [int(v.strip()) for v in trfTup.value.split(self.__tObj.getIterableSeparator(trfTup.atId))]
        return TrfValue(vL, trfTup.atId, trfTup.origLength, False)

    def castFloat(self, trfTup):
        """
            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        if ((trfTup.value == '?') or (trfTup.value == '.') or (len(trfTup.value) == 0)):
            return TrfValue(self.__nullValueOther, trfTup.atId, trfTup.origLength, True)
        return TrfValue(float(trfTup.value), trfTup.atId, trfTup.origLength, False)

    def castIterableFloat(self, trfTup):
        """
            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        if ((trfTup.value == '?') or (trfTup.value == '.') or (len(trfTup.value) == 0)):
            return TrfValue(self.__nullValueOther, trfTup.atId, trfTup.origLength, True)
        vL = [float(v.strip()) for v in trfTup.value.split(self.__tObj.getIterableSeparator(trfTup.atId))]
        return TrfValue(vL, trfTup.atId, trfTup.origLength, False)

    def castDateToObj(self, trfTup):
        """ Cast the input date (optional time) string (yyyy-mm-dd:hh::mm:ss) to a Python DateTime object -

            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        origLength = len(trfTup.value)
        if ((origLength == 0) or (trfTup.value == '?') or (trfTup.value == '.')):
            return TrfValue(self.__nullValueOther, trfTup.atId, origLength, True)
        tv = trfTup.value.replace(":", " ", 1)
        return TrfValue(dateutil.parser.parse(tv), trfTup.atId, origLength, False)

    def castDateTimeToIsoDate(self, trfTup):
        """ Cast the input date (optional time) string (yyyy-mm-dd:hh::mm:ss) to a Python DateTime object -

            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        origLength = len(trfTup.value)
        if ((origLength == 0) or (trfTup.value == '?') or (trfTup.value == '.')):
            return TrfValue(self.__nullValueOther, trfTup.atId, origLength, True)
        tv = trfTup.value.replace(":", " ", 1)
        tS = dateutil.parser.parse(tv).replace(tzinfo=pytz.UTC).isoformat()

        return TrfValue(tS, trfTup.atId, origLength, False)

    def castDateToIsoDate(self, trfTup):
        """ Cast the input date (optional time) string (yyyy-mm-dd:hh::mm:ss) to a Python DateTime object -

            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        origLength = len(trfTup.value)
        if ((origLength == 0) or (trfTup.value == '?') or (trfTup.value == '.')):
            return TrfValue(self.__nullValueOther, trfTup.atId, origLength, True)
        tv = trfTup.value.replace(":", " ", 1)
        tS = dateutil.parser.parse(tv).isoformat()

        return TrfValue(tS[:10], trfTup.atId, origLength, False)

    def castDateToString(self, trfTup):
        """ Cast the input date (optional time) string (yyyy-mm-dd:hh::mm:ss) as a string unchanged -

            Return:  TrfValue tuple
        """
        if trfTup.isNull:
            return trfTup
        origLength = len(trfTup.value)
        if ((origLength == 0) or (trfTup.value == '?') or (trfTup.value == '.')):
            return TrfValue(self.__nullValueOther, trfTup.atId, origLength, True)
        return TrfValue(trfTup.value, trfTup.atId, origLength, False)

    def stripWhiteSpace(self, trfTup):
        """ Remove all white space from the input value.

            Return:  ReturnValue tuple
        """
        if trfTup.isNull:
            return trfTup
        value = self.__wsPattern.sub("", trfTup.value)
        return TrfValue(value, trfTup.atId, trfTup.origLength, False)

    def truncateString(self, trfTup):
        """ Truncate string value to maximum length setting.

            Return:  ReturnValue tuple
        """
        if trfTup.isNull:
            return trfTup
        return TrfValue(trfTup.value[:self.__tObj.getAttributeWidth(trfTup.atId)], trfTup.atId, trfTup.origLength, False)

    def translateXMLCharRefs(self, trfTup):
        """ Convert XML Character references to unicode.

            Return:  ReturnValue tuple
        """
        if trfTup.isNull:
            return trfTup
        return TrfValue(unescapeXmlCharRef(trfTup.value), trfTup.atId, trfTup.origLength, False)

    def translateXMLCharRefsIt(self, trfTup):
        """ Convert XML Character references to unicode.

            Return:  ReturnValue tuple
        """
        if trfTup.isNull:
            return trfTup
        #
        vL = [unescapeXmlCharRef(v) for v in trfTup.value]
        return TrfValue(vL, trfTup.atId, trfTup.origLength, False)
