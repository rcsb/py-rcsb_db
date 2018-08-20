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
#   20-Aug-2018  jdw add getHelper() to return an instance of a module/class
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
import sys

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
        """Return configuration value of input configuration option.

        Args:
            name (str): configuration option name
            default (str, optional): default value returned if no configuration option is provided
            sectionName (str, optional): configuration section name

        Returns:
            str: configuration option value

        """
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
        """ Return path associated with the input configuration option. This method supports mocking where
        the MOCK_TOP_PATH will be prepended to the configuration path.

        Args:
            name (str): configuration option name
            default (str, optional): default value returned if no configuration option is provided
            sectionName (str, optional): configuration section name

        Returns:
            str: configuration path

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

    def getHelper(self, name, default=None, sectionName='DEFAULT', **kwargs):
        """Return an instance of module/class corresponding to the configuration module path.


        Args:
            name (str): configuration option name
            default (str, optional): default return value
            sectionName (str, optional): configuration section name
            **kwargs: key-value arguments passed to the module/class instance

        Returns:
            object: instance of module/class


        """
        val = default
        try:
            mySection = sectionName if sectionName != 'DEFAULT' else self.__defaultSectionName
            val = str(self.__cD[mySection][name])
        except Exception as e:
            if False:
                logger.debug("Missing config option %r assigned default value %r (%s)" % (name, default, str(e)))
        #
        return self.__getHelper(val, **kwargs)

    def __getHelper(self, modulePath, **kwargs):
        aMod = __import__(modulePath, globals(), locals(), [''])
        sys.modules[modulePath] = aMod
        #
        # Strip off any leading path to the module before we instaniate the object.
        mpL = modulePath.split('.')
        moduleName = mpL[-1]
        #
        aObj = getattr(aMod, moduleName)(**kwargs)
        return aObj

    def __rdConfigFile(self, configPath):
        try:
            config = cp()
            config.sections()
            config.read(configPath)
            return config
        except Exception as e:
            logger.error("Failed processing configuration file %s with %s" % (configPath, str(e)))
        return {}
