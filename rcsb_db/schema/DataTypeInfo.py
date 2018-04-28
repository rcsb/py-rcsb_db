##
# File:    DataTypeInfo.py
# Author:  J. Westbrook
# Date:    19-Apr-2018
# Version: 0.001 Initial version
#
# Updates:
#
##
"""
Manage default application data type to dictionary data type mapping.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import copy
import logging
logger = logging.getLogger(__name__)

from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from mmcif.api.DataCategory import DataCategory
from mmcif.api.PdbxContainers import DataContainer


class DataTypeInfo(object):

    cifTypes = [
        "code", "ucode", "line", "uline", "text", "int", "float", "name",
        "idname", "any", "yyyy-mm-dd", "uchar3", "uchar1", "symop", "atcode",
        "yyyy-mm-dd:hh:mm", "fax", "phone", "email", "code30", "float-range",
        "operation_expression", "yyyy-mm-dd:hh:mm-flex", "ec-type", "ucode-alphanum-csv",
        "int-range", "point_symmetry", "id_list", "4x3_matrix", "non_negative_int", "positive_int", "emd_id",
        "pdb_id", "point_group", "point_group_helical", "boolean", "author", "orcid_id", "symmetry_operation", ""
    ]

    appTypes = [
        "char", "char", "char", "char", "char", "int", "float", "char", "char",
        "text", "datetime", "char", "char", "char", "char", "datetime", "char",
        "char", "char", "char", "char", "char", "datetime", "char", "char",
        "char", "char", "char", "char", "int", "int", "char", "char", "char", "char", "char", "char", "char", "char", ""
    ]

    defaultWidths = [
        "10", "10", "80", "80", "200", "10", "10", "80", "80", "255", "15", "4",
        "2", "10", "6", "20", "25", "25", "80", "30", "30", "30", "20", "10",
        "25", "20", "80", "100", "10", "10", "10", "15", "20", "20", "5", "80", "20", "80", ""
    ]

    defaultPrecisions = [
        "0", "0", "0", "0", "0", "0", "6", "0", "0", "0", "0", "0", "0", "0",
        "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
        "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", ""
    ]

    #
    def __init__(self, filePath=None, applicationName='ANY', workPath=None):
        self.__workPath = workPath
        self.__filePath = filePath
        self.__applicationName = applicationName
        self.__setup(self.__filePath, self.__applicationName)

    def __setup(self, filePath, applicationName):
        if filePath:
            self.__dtmD = self.readDefaultDataTypeMap(filePath, applicationName='ANY')
        else:
            self.__dtmD = self.getDefaultDataTypeMap(applicationName='ANY')

    def getDefaultDataTypeMap(self, applicationName='ANY'):
        try:
            mapD = {}
            for (cifType, simpleType, defWidth, defPrecision) in zip(DataTypeInfo.cifTypes,
                                                                     DataTypeInfo.appTypes,
                                                                     DataTypeInfo.defaultWidths,
                                                                     DataTypeInfo.defaultPrecisions):
                mapD[cifType] = {
                    'app_type_code': simpleType,
                    'app_precision': defPrecision,
                    'app_width': defWidth,
                    'type_code': cifType,
                    'application_name': applicationName}
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return {}

    def hasType(self, cifType):
        """
        """
        try:
            return cifType in self.__dtmD
        except Exception as e:
            return False

    def getAppTypeName(self, cifType, default=None):
        """
        """
        try:
            return self.__dtmD[cifType]['app_type_code']
        except Exception as e:
            return default

    def getAppTypeWidth(self, cifType, default=None):
        """
        """
        try:
            return self.__dtmD[cifType]['app_width']
        except Exception as e:
            return default

    def getAppTypePrecision(self, cifType, default=None):
        """
        """
        try:
            return self.__dtmD[cifType]['app_precision']
        except Exception as e:
            return default

    def writeDefaultDataTypeMap(self, outPath, applicationName='ANY'):
        """ Write data file containing application default dictionary to application data type mapping

                  data_rcsb_data_type_map
                    loop_
                    _pdbx_data_type_application_map.application_name
                    _pdbx_data_type_application_map.type_code
                    _pdbx_data_type_application_map.app_type_code
                    _pdbx_data_type_application_map.app_precision
                    _pdbx_data_type_application_map.app_width
                    # .... type mapping data ...
        """
        try:
            #
            containerList = []
            curContainer = DataContainer("rcsb_data_type_map")
            aCat = DataCategory("pdbx_application_data_type_map")
            aCat.appendAttribute("application_name")
            aCat.appendAttribute("type_code")
            aCat.appendAttribute("app_type_code")
            aCat.appendAttribute("app_width")
            aCat.appendAttribute("app_precision")
            for (cifType, simpleType, defWidth, defPrecision) in zip(DataTypeInfo.cifTypes,
                                                                     DataTypeInfo.appTypes,
                                                                     DataTypeInfo.defaultWidths,
                                                                     DataTypeInfo.defaultPrecisions):
                aCat.append([applicationName, cifType, simpleType, defWidth, defPrecision])
            curContainer.append(aCat)
            containerList.append(curContainer)
            #
            myIo = IoAdapter(raiseExceptions=True, useCharRefs=True)
            ok = myIo.writeFile(outPath, containerList=containerList, enforceAscii=True)
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def updateDefaultDataTypeMap(self, filePath, mapD, applicationName='ANY'):
        """ Update data file containing application default data type mapping with any
            updates from the input type mapping dictionary

           mapD['cif_type_code'] -> ['application_name', 'app_type_code', 'app_precision', 'app_width', 'type_code']

                  data_rcsb_data_type_map
                    loop_
                    _pdbx_data_type_application_map.application_name
                    _pdbx_data_type_application_map.type_code
                    _pdbx_data_type_application_map.app_type_code
                    _pdbx_data_type_application_map.app_precision
                    _pdbx_data_type_application_map.app_width
                    # .... type mapping data ...
        """
        try:
            #
            mD = copy.deepcopy(mapD)
            myIo = IoAdapter(raiseExceptions=True, useCharRefs=True)
            containerList = myIo.readFile(filePath, enforceAscii=True, outDirPath=self.__workPath)
            for container in containerList:
                if container.getName() == 'rcsb_data_type_map':
                    catObj = container.getObj('pdbx_data_type_application_map')
                    rIL = []
                    for ii in range(catObj.getRowCount()):
                        d = catObj.getRowAttributeDict(ii)
                        if d['application_name'] == applicationName:
                            rIL.append(ii)
                        if d['application_name'] == applicationName and d['type_code'] in mD:
                            mD[d['type_code']] = {k: d[k] for k in ['application_name', 'type_code']}
                            continue
                        elif d['application_name'] == applicationName:
                            # types unique to the file for this application so add these
                            mD[d['type_code']] = {k: d[k] for k in ['application_name', 'app_type_code', 'app_precision', 'app_width', 'type_code']}
                    ok = catObj.removeRows(rIL)
                    atNameL = catObj.getAttributes()
                    for ky in mapD:
                        r = [mapD[ky][atN] for atN in atNameL]
                        catObj.append(r)
            #
            # Write updated data file
            myIo = IoAdapter(raiseExceptions=True, useCharRefs=True)
            ok = myIo.writeFile(filePath, containerList=containerList, enforceAscii=True)
            return ok
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return False

    def readDefaultDataTypeMap(self, inpPath, applicationName='ANY'):
        """ Read data file containing application default data type mapping

                  data_rcsb_data_type_map
                    loop_
                    _pdbx_data_type_application_map.application_name
                    _pdbx_data_type_application_map.type_code
                    _pdbx_data_type_application_map.app_type_code
                    _pdbx_data_type_application_map.app_precision
                    _pdbx_data_type_application_map.app_width
                    # .... type mapping data ...

            Return (dict):  map[cifType] -> appType, width, precision
                        mapD['cif_type_code'] -> ['application_name', 'app_type_code', 'app_precision', 'app_width', 'type_code']
        """
        try:
            #
            mapD = {}
            myIo = IoAdapter(raiseExceptions=True, useCharRefs=True)
            containerList = myIo.readFile(inpPath, enforceAscii=True, outDirPath=self.__workPath)
            for container in containerList:
                if container.getName() == 'rcsb_data_type_map':
                    catObj = container.getObj('pdbx_data_type_application_map')
                    for ii in range(catObj.getRowCount()):
                        d = catObj.getRowAttributeDict(ii)
                        if d['application_name'] == applicationName:
                            mapD[d['type_code']] = {k: d[k] for k in ['app_type_code', 'app_precision', 'app_width', 'application_name', 'type_code']}

            return mapD
        except Exception as e:
            logger.exception("Failing with %s" % str(e))
        return {}
