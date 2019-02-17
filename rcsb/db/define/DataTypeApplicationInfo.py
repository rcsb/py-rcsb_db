##
# File:    DataTypeApplicationInfo.py
# Author:  J. Westbrook
# Date:    19-Apr-2018
# Version: 0.001 Initial version
#
# Updates:
#  22-May-2018 jdw standardize data names and fix alignment in default data sections
#  23-May-2018 jdw change assumptions for update method and add tests.
#   7-Jun-2018 jdw rename and rescope.
#  15-Aug-2018 jdw add mapping for JSON types based on generic 'ANY' typing.
#  29-Sep-2018 jdw make JSON date and datetime type explicit in JSON
#  12-Oct-2018 jdw unsuppress datetime mapping
#   7-Jan-2019 jdw applicationName->dataTyping
#
##
"""
Manage mapping of default application data type to dictionary data types.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import copy
import logging

from mmcif.api.DataCategory import DataCategory
from mmcif.api.PdbxContainers import DataContainer

from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class DataTypeApplicationInfo(object):

    cifTypes = [
        "code", "ucode", "line", "uline", "text", "int", "float", "name",
        "idname", "any", "yyyy-mm-dd", "uchar3", "uchar1", "symop", "atcode",
        "yyyy-mm-dd:hh:mm", "fax", "phone", "email", "code30", "float-range",
        "operation_expression", "yyyy-mm-dd:hh:mm-flex", "ec-type", "ucode-alphanum-csv",
        "int-range", "point_symmetry", "id_list", "4x3_matrix", "non_negative_int", "positive_int", "emd_id",
        "pdb_id", "point_group", "point_group_helical", "boolean", "author", "orcid_id", "symmetry_operation", ""
    ]
    # These are generic types -
    appTypes = [
        "char", "char", "char", "char", "char", "int", "float", "char", "char",
        "text", "date", "char", "char", "char", "char", "datetime", "char",
        "char", "char", "char", "char", "char", "datetime", "char", "char",
        "char", "char", "char", "char", "int", "int", "char", "char", "char", "char", "char", "char", "char", "char", ""
    ]

    defaultWidths = [
        "10", "10", "80", "80", "200", "10", "10", "80", "80", "255", "15", "4",
        "2", "10", "6", "20", "25", "25", "80", "30", "30", "30", "20", "10",
        "25", "20", "80", "100", "10", "10", "10", "15", "20", "20", "20", "5", "80", "20", "80", ""
    ]

    defaultPrecisions = [
        "0", "0", "0", "0", "0", "0", "6", "0", "0", "0", "0", "0", "0", "0",
        "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
        "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", ""
    ]

    #
    def __init__(self, locator=None, dataTyping='ANY', workPath=None, **kwargs):
        self.__workPath = workPath
        self.__locator = locator
        self.__dataTyping = dataTyping
        self.__maxCharWidth = 32768
        self.__dtmD = self.__setup(self.__locator, self.__dataTyping)

    def __setup(self, locator, dataTyping):
        appName = 'ANY' if dataTyping in ['JSON', 'BSON'] else dataTyping
        if locator:
            logger.debug("Using data mapping from %s for %s" % (locator, appName))
            dtmD = self.readDefaultDataTypeMap(locator, dataTyping=appName)
        else:
            logger.debug(">>>> Falling back to default type mapping. ")
            dtmD = self.getDefaultDataTypeMap(dataTyping=appName)
        # for JSON - transform the generic 'ANY' data types -
        if dataTyping == 'JSON':
            for cifType, tD in dtmD.items():
                if tD['application_name'] == 'ANY':
                    if tD['app_type_code'] in ['char', 'text']:
                        tD['app_type_code'] = 'string'
                    elif tD['app_type_code'] in ['date', 'datetime']:
                        tD['app_type_code'] = tD['app_type_code']
                    elif tD['app_type_code'] in ['float']:
                        tD['app_type_code'] = 'number'
                    elif tD['app_type_code'] in ['int']:
                        tD['app_type_code'] = 'integer'
                tD['application_name'] = 'JSON'
        elif dataTyping == 'BSON':
            for cifType, tD in dtmD.items():
                if tD['application_name'] == 'ANY':
                    if tD['app_type_code'] in ['char', 'text']:
                        tD['app_type_code'] = 'string'
                    elif tD['app_type_code'] in ['float', 'double']:
                        tD['app_type_code'] = 'double'
                    elif tD['app_type_code'] in ['date', 'datetime']:
                        tD['app_type_code'] = 'date'
                    elif tD['app_type_code'] in ['int', 'integer']:
                        tD['app_type_code'] = 'int'
                tD['application_name'] = 'BSON'
        #
        return dtmD

    def getDefaultDataTypeMap(self, dataTyping='ANY'):
        try:
            mapD = {}
            for (cifType, simpleType, defWidth, defPrecision) in zip(DataTypeApplicationInfo.cifTypes,
                                                                     DataTypeApplicationInfo.appTypes,
                                                                     DataTypeApplicationInfo.defaultWidths,
                                                                     DataTypeApplicationInfo.defaultPrecisions):
                if self.__isNull(cifType):
                    continue
                mapD[cifType] = {
                    'app_type_code': simpleType,
                    'app_precision_default': defPrecision,
                    'app_width_default': defWidth,
                    'type_code': cifType,
                    'application_name': dataTyping}
            return mapD
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return {}

    def updateCharType(self, isKey, appType, dataWidth, defaultWidth, bufferPercent=30.0, minWidth=10):
        """ Skeleton implementation needs to add bracket typing around [80, 256, 1024, 2048, ... ]
        """
        retDataWidth = defaultWidth
        retDataType = appType
        iWidth = dataWidth + int(bufferPercent * 0.01 * dataWidth)
        retDataWidth = iWidth if iWidth > minWidth else minWidth
        if self.__dataTyping.upper() in ['SQL', 'MYSQL', 'COCKROACH', 'CRATE']:

            if appType.upper() in ['CHAR', 'VARCHAR'] and retDataWidth > self.__maxCharWidth:
                retDataType = 'TEXT'
            if appType.upper() in ['TEXT'] and isKey:
                retDataType = 'CHAR'
        else:
            pass
        return (retDataType, retDataWidth)

    def hasType(self, cifType):
        """
        """
        try:
            return cifType in self.__dtmD
        except Exception:
            return False

    def getAppTypeName(self, cifType, default=None):
        """
        """
        try:
            return self.__dtmD[cifType]['app_type_code']
        except Exception:
            return default

    def getAppTypeDefaultWidth(self, cifType, default=None):
        """
        """
        try:
            return self.__dtmD[cifType]['app_width_default']
        except Exception:
            return default

    def getAppTypeDefaultPrecision(self, cifType, default=None):
        """
        """
        try:
            return self.__dtmD[cifType]['app_precision_default']
        except Exception:
            return default

    def writeDefaultDataTypeMap(self, outPath, dataTyping='ANY'):
        """ Write data file containing application default dictionary to application data type mapping

                  data_rcsb_data_type_map
                    loop_
                    _pdbx_data_type_application_map.application_name
                    _pdbx_data_type_application_map.type_code
                    _pdbx_data_type_application_map.app_type_code
                    _pdbx_data_type_application_map.app_precision_default
                    _pdbx_data_type_application_map.app_width_default
                    # .... type mapping data ...
        """
        try:
            #
            containerList = []
            curContainer = DataContainer("rcsb_data_type_map")
            aCat = DataCategory("pdbx_data_type_application_map")
            aCat.appendAttribute("application_name")
            aCat.appendAttribute("type_code")
            aCat.appendAttribute("app_type_code")
            aCat.appendAttribute("app_width_default")
            aCat.appendAttribute("app_precision_default")
            for (cifType, simpleType, defWidth, defPrecision) in zip(DataTypeApplicationInfo.cifTypes,
                                                                     DataTypeApplicationInfo.appTypes,
                                                                     DataTypeApplicationInfo.defaultWidths,
                                                                     DataTypeApplicationInfo.defaultPrecisions):
                if self.__isNull(cifType):
                    continue
                aCat.append([dataTyping, cifType, simpleType, defWidth, defPrecision])
            curContainer.append(aCat)
            containerList.append(curContainer)
            #
            mU = MarshalUtil(workPath=self.__workPath)
            ok = mU.doExport(outPath, containerList, format="mmcif", enforceAscii=True, useCharRefs=True, raiseExceptions=True)

            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def updateDefaultDataTypeMap(self, filePath, mapD, dataTyping='ANY'):
        """ Update data file containing application default data type mapping with any
            updates from the input type mapping dictionary

           mapD['cif_type_code'] -> ['application_name', 'app_type_code', 'app_precision_default', 'app_width_default', 'type_code']

                  data_rcsb_data_type_map
                    loop_
                    _pdbx_data_type_application_map.application_name
                    _pdbx_data_type_application_map.type_code
                    _pdbx_data_type_application_map.app_type_code
                    _pdbx_data_type_application_map.app_precision_default
                    _pdbx_data_type_application_map.app_width_default
                    # .... type mapping data ...
        """
        try:
            #
            mD = copy.deepcopy(mapD)
            mU = MarshalUtil(workPath=self.__workPath)
            containerList = mU.doImport(filePath, format="mmcif", enforceAscii=True, useCharRefs=True, raiseExceptions=True)
            for container in containerList:
                if container.getName() == 'rcsb_data_type_map':
                    catObj = container.getObj('pdbx_data_type_application_map')
                    rIL = []
                    for ii in range(catObj.getRowCount()):
                        d = catObj.getRowAttributeDict(ii)
                        if d['application_name'] == dataTyping:
                            rIL.append(ii)
                            mD[d['type_code']] = {k: d[k] for k in ['application_name', 'app_type_code', 'app_precision_default', 'app_width_default', 'type_code']}
                            continue
                    ok = catObj.removeRows(rIL)
                    atNameL = catObj.getAttributeList()
                    for ky in mapD:
                        r = [mapD[ky][atN] for atN in atNameL]
                        catObj.append(r)
            #
            # Write updated data file
            mU = MarshalUtil(workPath=self.__workPath)
            ok = mU.doExport(filePath, containerList, format="mmcif", enforceAscii=True, useCharRefs=True, raiseExceptions=True)

            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def readDefaultDataTypeMap(self, locator, dataTyping='ANY'):
        """ Read data file containing application default data type mapping

                  data_rcsb_data_type_map
                    loop_
                    _pdbx_data_type_application_map.application_name
                    _pdbx_data_type_application_map.type_code
                    _pdbx_data_type_application_map.app_type_code
                    _pdbx_data_type_application_map.app_precision_default
                    _pdbx_data_type_application_map.app_width_default
                    # .... type mapping data ...

            Return (dict):  map[cifType] -> appType, width, precision
                        mapD['cif_type_code'] -> ['application_name', 'app_type_code', 'app_precision_default', 'app_width_default', 'type_code']
        """
        try:
            #
            mapD = {}
            mU = MarshalUtil(workPath=self.__workPath)
            containerList = mU.doImport(locator, format="mmcif", enforceAscii=True, useCharRefs=True, raiseExceptions=True)

            for container in containerList:
                if container.getName() == 'rcsb_data_type_map':
                    catObj = container.getObj('pdbx_data_type_application_map')
                    for ii in range(catObj.getRowCount()):
                        d = catObj.getRowAttributeDict(ii)
                        if d['application_name'] == dataTyping:
                            mapD[d['type_code']] = {k: d[k] for k in ['app_type_code', 'application_name', 'type_code']}
                            mapD[d['type_code']].update({k: int(d[k]) for k in ['app_precision_default', 'app_width_default']})
            return mapD
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return {}

    def __isNull(self, value):
        if not value:
            return True
        if (len(value) == 0) or (value == '?') or (value == '.'):
            return True
        return False
