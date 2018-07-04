##
# File:    ConfigUtil.py
# Author:  J. Westbrook
# Date:    14-Mar-2018
# Version: 0.001
#
# Updates:
#   31-Mar-2018  jdw standardize argument names
#   16-Jun-2018. jdw add more convenient support for multiple config sections
#   18-Jun-2018  jdw push the mocking down to a new getPath() method.
##
"""
 Manage simple configuration options.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os

try:
    from configparser import ConfigParser as cp
except ImportError:
    from ConfigParser import SafeConfigParser as cp


logger = logging.getLogger(__name__)


class ConfigUtil(object):

    def __init__(self, configPath=None, sectionName='DEFAULT', fallbackEnvPath='CONFIG_UTIL_PATH', mockTopPath=None, **kwargs):
        myConfigPath = configPath if configPath is not None else os.getenv(fallbackEnvPath, 'setup.cfg')
        self.__defaultSectionName = sectionName
        self.__mockTopPath = mockTopPath
        self.__cD = self.__rdConfigFile(myConfigPath)
        if len(self.__cD) < 1:
            logger.warning("Missing or incomplete configuration information in file %s" % myConfigPath)

    def dump(self):
        for section in self.__cD:
            logger.info("Configuration section: %s" % section)
            for opt in self.__cD[section]:
                logger.info(" ++++  option %s  : %r " % (opt, self.__cD[section][opt]))

    def get(self, name, default=None, sectionName='DEFAULT'):
        val = default
        try:
            mySection = sectionName if sectionName != 'DEFAULT' else self.__defaultSectionName
            val = str(self.__cD[mySection][name])
        except Exception as e:
            if False:
                logger.debug("Missing config option %r assigned default value %r (%s)" % (name, default, str(e)))
        #
        return val

    def getPath(self, name, default=None, sectionName='DEFAULT'):
        """ Convenience method supporting mocking
        """
        val = default
        try:
            mySection = sectionName if sectionName != 'DEFAULT' else self.__defaultSectionName
            if self.__mockTopPath:
                val = os.path.join(self.__mockTopPath, str(self.__cD[mySection][name]))
            else:
                val = str(self.__cD[mySection][name])
        except Exception as e:
            logger.debug("Missing config option %r assigned default value %r (%s)" % (name, default, str(e)))
        #
        return val

    def getList(self, name, default=None, sectionName='DEFAULT'):
        valL = default if default is not None else []
        try:
            mySection = sectionName if sectionName != 'DEFAULT' else self.__defaultSectionName
            valL = str(self.__cD[mySection][name]).split(',')
        except Exception as e:
            logger.debug("Missing config option list %r assigned default value %r (%s)" % (name, default, str(e)))
        #
        return valL

    def __rdConfigFile(self, configPath):
        try:
            config = cp()
            config.sections()
            config.read(configPath)
            return config
        except Exception as e:
            logger.error("Failed processing configuration file %s with %s" % (configPath, str(e)))
        return {}
