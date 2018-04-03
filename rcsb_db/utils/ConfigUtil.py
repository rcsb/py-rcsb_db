##
# File:    ConfigUtil.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#   31-Mar-2018  jdw standardize argument names
##
"""
 Manage simple configuration options.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import os
try:
    from configparser import ConfigParser as cp
except ImportError:
    from ConfigParser import SafeConfigParser as cp


import logging
logger = logging.getLogger(__name__)


class ConfigUtil(object):

    def __init__(self, configPath=None, sectionName='DEFAULT', fallbackEnvPath='CONFIG_UTIL_PATH'):
        myConfigPath = configPath if configPath is not None else os.getenv(fallbackEnvPath, 'setup.cfg')
        self.__cD = {}
        self.__cD = self.__rdConfigFile(myConfigPath, sectionName)
        if len(self.__cD) < 1:
            logger.warn("Missing or incomplete configuration information in file %s" % myConfigPath)

    def get(self, name, default=None):
        val = default
        try:
            val = str(self.__cD[name])
        except Exception as e:
            logger.debug("Missing config option %r assigned default value %r" % (name, default))
        #
        return val

    def getList(self, name, default=None):
        valL = default if default is not None else []
        try:
            valL = str(self.__cD[name]).split(',')
        except Exception as e:
            logger.debug("Missing config option list %r assigned default value %r" % (name, default))
        #
        return valL

    def __getConfig(self, configPath, sectionName):
        self.__cD = self.__rdConfigFile(configPath, sectionName)

    def __rdConfigFile(self, configPath, sectionName):
        try:
            config = cp()
            config.sections()
            config.read(configPath)
            #
            if sectionName in config:
                logger.debug("Configuration options %s" % ([k for k in config[sectionName]]))
                return config[sectionName]
            else:
                return {}
        except Exception as e:
            logger.error("Failed processing configuration file %s section %s with %s" % (configPath, sectionName, str(e)))
        return {}
