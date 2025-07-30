##
# File:    SchemaProvider.py
# Author:  J. Westbrook
# Date:    18-Aug-2019
# Version: 0.001
#
# Updates:
#    26-Aug-2019 jdw  add database name to json schema name, add schema rebuild option.
#     6-Sep-2019 jdw  add rcsb extensions to the the json schema full options
#
##
"""
A collection of schema build and caching methods.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import pprint

from rcsb.db.define.SchemaDefAccess import SchemaDefAccess
from rcsb.db.define.SchemaDefBuild import SchemaDefBuild
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.SingletonClass import SingletonClass

logger = logging.getLogger(__name__)


class SchemaProvider(SingletonClass):
    """A collection of schema build and caching methods.

    Static cache worflow:

        <authorative source>  <--   <cache dir>  <-  client API

    Compute workflow:

    <dependent resource files, config file, dictionaries> -> [schema builder] --> <schema def> --> <Json schema>

    """

    def __init__(self, cfgOb, cachePath, useCache=True, rebuildFlag=False, **kwargs):
        """A collection of schema build and caching methods.

        Args:
            cfgOb (object): ConfigInfo() instance
            cachePath (str): path to directory containing schema
            useCache (bool, optional): use cached schema. Defaults to True.
            rebuildFlag (bool, optional): on-the-fly rebuild and cache schema
        """

        self.__cfgOb = cfgOb
        self.__configName = self.__cfgOb.getDefaultSectionName()
        self.__cachePath = os.path.abspath(cachePath)
        self.__useCache = useCache
        self.__rebuildFlag = rebuildFlag
        self.__useCache = rebuildFlag if rebuildFlag else useCache
        #
        self.__workPath = os.path.join(self.__cachePath, "work")

        self.__fileU = FileUtil(workPath=os.path.join(self.__cachePath, "work"))
        self.__schemaCachePath = os.path.join(self.__cachePath, self.__cfgOb.get("SCHEMA_DEFINITION_CACHE_DIR", sectionName=self.__configName))
        self.__jsonSchemaCachePath = os.path.join(self.__cachePath, self.__cfgOb.get("JSON_SCHEMA_DEFINITION_CACHE_DIR", sectionName=self.__configName))
        self.__fileU.mkdir(self.__schemaCachePath)
        self.__fileU.mkdir(self.__jsonSchemaCachePath)
        self.__kwargs = kwargs
        #
        # If below causes problems, then can copy the getDatabaseMongoName method from DocumentDefinitionHelper into this file
        self.__documentDefHelper = self.__cfgOb.getHelper("DOCUMENT_DEF_HELPER_MODULE", sectionName=self.__configName, cfgOb=self.__cfgOb)

    def getSchemaOptions(self, schemaLevel, extraOpts=None):
        opts = extraOpts + "|" if extraOpts else ""
        if schemaLevel == "full":
            return opts + "mandatoryKeys|mandatoryAttributes|bounds|enums|rcsb"
        elif schemaLevel in ["min", "minimum"]:
            return opts + "mandatoryKeys|enums|rcsb"
        else:
            return opts

    def getSchemaInfo(self, schemaGroupName, dataTyping="ANY"):
        """Convenience method to return essential schema details for the input repository content type.

        Args:
            schemaGroupName (str): schema name  (e.g. pdbx, bird, chem_comp, ...)
            dataTyping (str, optional): Application name for the target schema (e.g. ANY, SQL, ...)

        Returns:
            tuple: SchemaDefAccess(object), target database name, target collection name list, primary index attribute list


        """
        sd = None
        dbName = None
        collectionNameList = []
        docIndexD = {}
        try:
            mU = MarshalUtil(workPath=self.__workPath)
            schemaLocator = self.__getSchemaDefLocator(schemaGroupName, dataTyping=dataTyping)
            if self.__rebuildFlag:
                filePath = os.path.join(self.__schemaCachePath, self.__fileU.getFileName(schemaLocator))
                self.makeSchemaDef(schemaGroupName, dataTyping=dataTyping, saveSchema=True)
            else:
                filePath = self.__reload(schemaLocator, self.__schemaCachePath, useCache=self.__useCache)

            if not filePath:
                logger.error("Unable to recover schema %s (%s)", schemaGroupName, dataTyping)
            logger.debug("ContentType %r dataTyping %r schemaLocator %r", schemaGroupName, dataTyping, schemaLocator)
            schemaDef = mU.doImport(filePath, fmt="json")
            if schemaDef:
                logger.debug("Using cached schema definition for %s application %s", schemaGroupName, dataTyping)
                sd = SchemaDefAccess(schemaDef)
                if sd:
                    dbName = sd.getDatabaseName()
                    collectionInfoList = sd.getCollectionInfo()
                    logger.debug("Schema %s database name %s collections %r", schemaGroupName, dbName, collectionInfoList)
                    for cd in collectionInfoList:
                        collectionName = cd["NAME"]
                        collectionNameList.append(collectionName)
                        docIndexD[collectionName] = sd.getDocumentIndices(collectionName)

        except Exception as e:
            logger.exception("Retreiving schema %s for %s failing with %s", schemaGroupName, dataTyping, str(e))

        return sd, dbName, collectionNameList, docIndexD

    def schemaDefCompare(self, schemaGroupName, dataTyping="ANY"):
        """Compare computed schema defintion with current source/cached version.

        Args:
            schemaGroupName (str): schema definition name for comparison
            dataTyping (str, optional): data type conventions for the schema comparison. Defaults to "ANY".

        Returns:
            (str): file path for schema difference or None
        """
        mU = MarshalUtil(workPath=self.__workPath)
        schemaDiffPath = os.path.join(self.__cachePath, "schema_diff")
        mU.mkdir(schemaDiffPath)
        schemaPath = self.__getSchemaDefLocator(schemaGroupName, dataTyping=dataTyping)
        fn = self.__fileU.getFileName(schemaPath)
        sD = self.makeSchemaDef(schemaGroupName, dataTyping=dataTyping)
        v2 = sD["DATABASE_VERSION"]
        # ----
        # tPath = os.path.join(self.__schemaCachePath, self.__fileU.getFileName(schemaPath) + "-test")
        # logger.info("Exporting schema def to %s", tPath)
        # mU.doExport(tPath, sD, fmt="json", indent=3)
        # sD = mU.doImport(tPath, fmt="json")
        # ----
        cPath = os.path.join(self.__schemaCachePath, self.__fileU.getFileName(schemaPath))
        sDCache = mU.doImport(cPath, fmt="json")
        v1 = sDCache["DATABASE_VERSION"]
        #
        numDiff, difD = self.schemaCompare(sDCache, sD)
        #
        # jD = diff(sDCache, sD, syntax="explicit", marshal=True)
        diffPath = None
        if numDiff:
            bn, _ = os.path.splitext(fn)
            diffPath = os.path.join(schemaDiffPath, bn + "-" + v1 + "-" + v2 + "-diff.json")
            # logger.info("diff for %s %s = \n%s", schemaGroupName, dataTyping, pprint.pformat(difD, indent=3, width=100))
            mU.doExport(diffPath, difD, fmt="json", indent=3)
        #
        return diffPath

    def jsonSchemaCompare(self, schemaGroupName, collectionName, encodingType, level, extraOpts=None):
        """Compare computed JSON schema defintion with current source/cached version.

        Args:
            schemaGroupName (str): schema name
            collectionName (str): collection name
            encodingType (str): schema data type conventions (JSON|BSON)
            level (str): metadata level (min|full)
            extraOpts (str): extra schema construction options

        Returns:
            (str): path to the difference file or None
        """
        mU = MarshalUtil(workPath=self.__workPath)
        schemaDiffPath = os.path.join(self.__cachePath, "schema_diff")
        mU.mkdir(schemaDiffPath)
        schemaLocator = self.__getJsonSchemaLocator(schemaGroupName, collectionName, encodingType, level)
        fn = self.__fileU.getFileName(schemaLocator)
        schemaPath = os.path.join(self.__jsonSchemaCachePath, fn)
        #
        sD = self.makeSchema(schemaGroupName, collectionName, encodingType=encodingType, level=level, saveSchema=False, extraOpts=extraOpts)
        v2 = self.__getSchemaVersion(sD)
        # ----
        # tPath = os.path.join(self.__jsonSchemaCachePath, self.__fileU.getFileName(schemaPath) + "-test")
        # logger.info("Exporting json schema to %s", tPath)
        # mU.doExport(tPath, sD, fmt="json", indent=3)
        # ----
        #
        sDCache = mU.doImport(schemaPath, fmt="json")
        v1 = self.__getSchemaVersion(sDCache)
        if not v1:
            logger.error("no version for %s - %s %s", schemaLocator, schemaGroupName, collectionName)
        #
        numDiff, difD = self.schemaCompare(sDCache, sD)
        # jD = diff(sDCache, sD, marshal=True, syntax="explicit")
        diffPath = None
        if numDiff:
            logger.debug("diff for %s %s %s %s = \n%s", schemaGroupName, collectionName, encodingType, level, pprint.pformat(difD, indent=3, width=100))
            bn, _ = os.path.splitext(fn)
            diffPath = os.path.join(schemaDiffPath, bn + "-" + v1 + "-" + v2 + "-diff.json")
            mU.doExport(diffPath, difD, fmt="json", indent=3)

        return diffPath

    def __getSchemaVersion(self, jsonSchema):
        try:
            comment = jsonSchema["$comment"] if "$comment" in jsonSchema else ""
            ff = comment.split(":")
            version = ff[1].strip()
            return version
        except Exception as e:
            logger.exception("Failing for with %s", str(e))
        return ""

    def __getSchemaDefLocator(self, schemaGroupName, dataTyping="ANY"):
        """Internal method returning schema definition path for the input content type and application.
        Defines schema definition naming convention -

        Args:
         schemaGroupName (str): schema name (e.g. pdbx, bird, chem_comp, ...)
         dataTyping (str, optional): Application name for the target schema (e.g. ANY, SQL, ...)

         Returns:

          str: schema definition file locator

        """
        schemaLocator = None
        try:
            locPath = self.__cfgOb.get("SCHEMA_DEFINITION_LOCATOR_PATH", sectionName=self.__configName)
            fn = "schema_def-%s-%s.json" % (schemaGroupName, dataTyping.upper())
            schemaLocator = os.path.join(locPath, fn)
        except Exception as e:
            logger.exception("Retreiving schema definition path %s for %s failing with %s", schemaGroupName, dataTyping, str(e))
        return schemaLocator

    def __getJsonSchemaLocator(self, schemaGroupName, collectionName, encodingType="BSON", level="full"):
        """Internal method returning JSON schema path for the input collection data type convention and level.
        Defines the JSON/BSON schema naming convention -

        Args:
         schemaGroupName (str): schema group name
         collectionName (str): collection name in document store
         encodingType (str, optional): data type convention (BSON|JSON)
         level (str, optional): Completeness of the schema (e.g. min or full)

         Returns:

         str: schema file locator

        """
        schemaLocator = None
        try:
            sdType = None
            sLevel = None
            schemaLocator = None
            if encodingType.upper() in ["JSON", "BSON"]:
                sdType = encodingType.lower()
            if level.lower() in ["min", "minimun"]:
                sLevel = "min"
            elif level.lower() in ["full"]:
                sLevel = level.lower()
            #
            if sdType and sLevel:
                locPath = self.__cfgOb.get("JSON_SCHEMA_DEFINITION_LOCATOR_PATH", sectionName=self.__configName)
                # BETTER TO DO THIS HERE AND GENERATE A FILE WITH '-DW-' NAMESPACE, RATHER THAN MAPPING DB NAMES DURING LOAD STEP
                databaseName = self.getDatabaseMongoName(schemaGroupName)
                fn = "%s-%s-db-%s-col-%s.json" % (sdType, sLevel, databaseName, collectionName)
                schemaLocator = os.path.join(locPath, fn)
                logger.info("JSON schemaLocator: %r", schemaLocator)
            else:
                logger.error("Unsupported schema options:  %s level %r type %r", collectionName, level, encodingType)
                schemaLocator = None
        except Exception as e:
            logger.debug("Retreiving JSON schema definition for %s type %s failing with %s", collectionName, encodingType, str(e))
        #
        return schemaLocator

    def __reload(self, locator, dirPath, useCache=True):
        #
        fn = self.__fileU.getFileName(locator)
        filePath = os.path.join(dirPath, fn)
        logger.debug("Target cache filePath %s", filePath)
        self.__fileU.mkdir(dirPath)
        if not useCache:
            try:
                os.remove(filePath)
            except Exception:
                pass
        #
        if useCache and self.__fileU.exists(filePath):
            ok = True
        else:
            logger.info("Fetch data from source %s to %s", locator, filePath)
            ok = self.__fileU.get(locator, filePath)

        return filePath if ok else None

    def getJsonSchema(self, schemaGroupName, collectionName, encodingType="BSON", level="full", extraOpts=None):
        """Return JSON schema (w/ BSON types) object for the input collection and level.and

        Args:
            schemaGroupName (str): schema group name
            collectionName (str): collection name in document store
            encodingType (str, optional): data type convention (BSON|JSON)
            level (str, optional): Completeness of the schema (e.g. min or full)

        Returns:
            dict: Schema object

        """
        sObj = None
        schemaLocator = self.__getJsonSchemaLocator(schemaGroupName, collectionName, encodingType=encodingType, level=level)
        #
        if self.__rebuildFlag:
            filePath = os.path.join(self.__schemaCachePath, self.__fileU.getFileName(schemaLocator))
            self.makeSchema(schemaGroupName, collectionName, encodingType=encodingType, level=level, extraOpts=extraOpts)
        else:
            logger.info("Reload schema filePath: %r", filePath)
            filePath = self.__reload(schemaLocator, self.__jsonSchemaCachePath, useCache=self.__useCache)
        mU = MarshalUtil(workPath=self.__workPath)
        if filePath and mU.exists(filePath):
            mU = MarshalUtil(workPath=self.__workPath)
            sObj = mU.doImport(filePath, fmt="json")
        else:
            logger.debug("Failed to read schema for %s %r", collectionName, level)
        return sObj

    def makeSchema(self, schemaGroupName, collectionName, encodingType="BSON", level="full", saveSchema=False, extraOpts=None):
        """Create the JSON or BSON schema file for a given database and collection (i.e., the files under, 'json_schema_definitions')

        Args:
            schemaGroupName (str): schema group name (e.g., 'pdbx_comp_model_core')
            collectionName (str): collection name in document store (e.g., 'pdbx_comp_model_core_entry')
            encodingType (str, optional): data type convention (BSON|JSON)
            level (str, optional): Completeness of the schema (e.g. min or full)
            saveSchema (bool, optional): whether to save the schema to jsonSchemaCachePath or not (default False)
            extraOpts (str, optional): extra schema construction options

        Returns:
            dict: JSON or BSON schema

        """
        try:
            smb = SchemaDefBuild(schemaGroupName, self.__cfgOb, configName=self.__configName, cachePath=self.__cachePath)
            #
            cD = None
            stU = encodingType.upper()
            cD = smb.build(collectionName, dataTyping=stU, encodingType=stU, enforceOpts=self.getSchemaOptions(level, extraOpts=extraOpts))
            if cD and saveSchema:
                schemaLocator = self.__getJsonSchemaLocator(schemaGroupName, collectionName, encodingType=encodingType, level=level)
                localPath = os.path.join(self.__jsonSchemaCachePath, self.__fileU.getFileName(schemaLocator))
                logger.info("Output makeSchema localPath: %r", localPath)
                mU = MarshalUtil(workPath=self.__workPath)
                mU.doExport(localPath, cD, fmt="json", indent=3, enforceAscii=False)
        except Exception as e:
            logger.exception("Building schema %s collection %s failing with %s", schemaGroupName, collectionName, str(e))
        return cD

    def makeSchemaDef(self, schemaGroupName, dataTyping="ANY", saveSchema=False):
        """Create the schema definition file for a given database (i.e., the files under 'schema_definitions')

        Args:
            schemaGroupName (str): Schema group name (e.g., 'pdbx_comp_model_core')
            dataTyping (str, optional): Application name for the target schema (e.g. ANY, SQL, ...)
            saveSchema (bool, optional): whether to save the schema to schemaCachePath or not (default False)

        Returns:
            dict: schema definition dictionary

        """
        schemaDef = None
        try:
            smb = SchemaDefBuild(schemaGroupName, self.__cfgOb, configName=self.__configName, cachePath=self.__cachePath)
            schemaDef = smb.build(dataTyping=dataTyping, encodingType="rcsb")
            if schemaDef and saveSchema:
                schemaLocator = self.__getSchemaDefLocator(schemaGroupName, dataTyping=dataTyping)
                localPath = os.path.join(self.__schemaCachePath, self.__fileU.getFileName(schemaLocator))
                mU = MarshalUtil(workPath=self.__workPath)
                mU.doExport(localPath, schemaDef, fmt="json", indent=3, enforceAscii=False)
        except Exception as e:
            logger.exception("Building schema %s failing with %s", schemaGroupName, str(e))
        return schemaDef

    def schemaCompare(self, orgD, newD):
        """Compute the difference of nested dictionaries."""
        fOrgD = self.__flatten(orgD)
        fNewD = self.__flatten(newD)
        if len(fOrgD) != len(fNewD):
            logger.debug("Schema lengths differ: org %d new %d", len(fOrgD), len(fNewD))
        #
        addedD = {k: fNewD[k] for k in set(fNewD) - set(fOrgD)}
        removedD = {k: fOrgD[k] for k in set(fOrgD) - set(fNewD)}
        changedOrgD = {k: fOrgD[k] for k in set(fOrgD) & set(fNewD) if fOrgD[k] != fNewD[k]}
        changedNewD = {k: fNewD[k] for k in set(fOrgD) & set(fNewD) if fOrgD[k] != fNewD[k]}
        chD = {}
        for ky in changedOrgD:
            kyS = ".".join(ky)
            vOrg = changedOrgD[ky]
            vNew = changedNewD[ky]
            if isinstance(vOrg, (list, tuple)) and isinstance(vNew, (list, tuple)):
                # logger.info(" >> %r vOrg %r vNew %r", ky, vOrg, vNew)
                dV = list(set(vNew) - set(vOrg))
                if dV:
                    chD[kyS] = {"diff": dV}
            else:
                chD[kyS] = {"from": vOrg, "to": vNew}
        #
        nT = len(addedD) + len(removedD) + len(chD)
        diffD = {"added": [".".join(kk) for kk in addedD.keys()], "removed": [".".join(kk) for kk in removedD.keys()], "changed": chD}
        return nT, diffD

    def __flatten(self, inpDict, prefix=None):
        prefix = prefix[:] if prefix else []
        outDict = {}
        for key, value in inpDict.items():
            if isinstance(value, dict) and value:
                deeper = self.__flatten(value, prefix + [key])
                outDict.update({tuple(key2): val2 for key2, val2 in deeper.items()})
            elif isinstance(value, (list, tuple)) and value:
                for index, sublist in enumerate(value, start=1):
                    if isinstance(sublist, dict) and sublist:
                        deeper = self.__flatten(sublist, prefix + [key] + [str(index)])
                        outDict.update({tuple(key2): val2 for key2, val2 in deeper.items()})
                    else:
                        outDict[tuple(prefix + [key] + [str(index)])] = value
            else:
                outDict[tuple(prefix + [key])] = value
        return outDict

    def __flattenX(self, inpDict, prefix=None):
        prefix = prefix[:] if prefix else []
        # separator = "."
        outDict = {}
        for key, value in inpDict.items():
            if isinstance(value, dict) and value:
                deeper = self.__flatten(value, prefix + [key])
                outDict.update({tuple(key2): val2 for key2, val2 in deeper.items()})
            elif isinstance(value, list) and value:
                for index, sublist in enumerate(value, start=1):
                    if isinstance(sublist, dict) and sublist:
                        deeper = self.__flatten(sublist, prefix + [key] + [str(index)])
                        outDict.update({tuple(key2): val2 for key2, val2 in deeper.items()})
                    else:
                        outDict[tuple(prefix + [key] + [str(index)])] = value
            else:
                outDict[tuple(prefix + [key])] = value
        return outDict

    def __flattenOrg(self, inpDict, separator=".", prefix=""):
        outDict = {}
        for key, value in inpDict.items():
            if isinstance(value, dict) and value:
                deeper = self.__flattenOrg(value, separator, prefix + key + separator)
                outDict.update({key2: val2 for key2, val2 in deeper.items()})
            elif isinstance(value, list) and value:
                for index, sublist in enumerate(value, start=1):
                    if isinstance(sublist, dict) and sublist:
                        deeper = self.__flattenOrg(sublist, separator, prefix + key + separator + str(index) + separator)
                        outDict.update({key2: val2 for key2, val2 in deeper.items()})
                    else:
                        outDict[prefix + key + separator + str(index)] = value
            else:
                outDict[prefix + key] = value
        return outDict

    def __dictGen(self, indict, pre=None):
        pre = pre[:] if pre else []
        if isinstance(indict, dict):
            for key, value in indict.items():
                if isinstance(value, dict):
                    for dD in self.__dictGen(value, pre + [key]):
                        yield dD
                elif isinstance(value, list) or isinstance(value, tuple):
                    for v in value:
                        for dD in self.__dictGen(v, pre + [key]):
                            yield dD
                else:
                    yield pre + [key, value]
        else:
            yield indict

    def getDatabaseMongoName(self, schemaGroupName=None):
        """Get the actual MongoDB name to load data to, given the schema group name.
        """
        return self.__documentDefHelper.getDatabaseMongoName(schemaGroupName)
