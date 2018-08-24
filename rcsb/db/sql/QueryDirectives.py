##
# File:    QueryDirectives.py
# Author:  J. Westbrook
# Date:    19-Jun-2015
# Version: 0.001 Initial version
#
# Updates:
#  21-Jun-2015 jdw extend order directives
#  22-Jun-2015 jdw add VALUE_LIST_CONDITION for selecting alternatives values -
#  04-Jul-2015 jdw  add accessor for current attribute selection -
#  09-Aug-2015 jdw add __queryDirSub(self, inpQueryDirList, domD={})
#  09-Aug-2015 jdw  add support for multi-valued references - DOM_REF_#
##
"""
A collection of classes to generate SQL commands to perform queries and schema construction.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import sys

from rcsb.db.sql.SqlGen import SqlGenCondition, SqlGenQuery

logger = logging.getLogger(__name__)


class QueryDirectives(object):

    """ Process query directives and generate SQL instructions.

       mini- SQL Query API token stream.

       Ordered selection list:

       SELECT_ITEM:<ordinal>:ITEM:<tableId.columnId>
       SELECT_ITEM:<ordinal>:ITEM:DOM_REF:<dom_form_element_id_name>

       tableId.columnId as defined in the supporting schema definition.

       Example:

       SELECT_ITEM:1:ITEM:DOM_REF:xtype
       SELECT_ITEM:2:ITEM:DOM_REF:ytype

       Query conditions:

       Conditions for single values (ordered):

       VALUE_CONDITION:<ordinal>:LOP:<logical conjunction (pre)>:ITEM:<tableId.columnId>:COP:<comparsion operator>:VALUE:<value>
       VALUE_CONDITION:<ordinal>:LOP:<logical conjunction (pre)>:ITEM:<tableId.columnId>:COP:<comparsion operator>:VALUE:DOM_REF:<dom_form_id_name>

       < comparison operator > in ['EQ', 'NE', 'GE', 'GT', 'LT', 'LE', 'LIKE', 'NOT LIKE']

       Examples:

       VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.crystal_twin:COP:GT:VALUE:DOM_REF:twin
       VALUE_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE:DOM_REF:molecular_type
       VALUE_CONDITION:3:LOP:AND:ITEM:pdbx_webselect.space_group_name_H_M:COP:EQ:VALUE:DOM_REF:spaceg
       VALUE_CONDITION:4:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software

       VALUE_CONDITION:5:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:GE:VALUE:DOM_REF:date1
       VALUE_CONDITION:6:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:LE:VALUE:DOM_REF:date2

      Conditions for multiple values (ordered):

       VALUE_LIST_CONDITION:<ordinal>:LOP:<logical conjunction (pre)>:ITEM:<tableId.columnId>:COP:<comparsion operator>:VALUE_LOP:<logical conjunction>:VALUE_LIST:<value>
       VALUE_LIST_CONDITION:<ordinal>:LOP:<logical conjunction (pre)>:ITEM:<tableId.columnId>:COP:<comparsion operator>:VALUE_LOP:<logical conjunction>:VALUE_LIST:DOM_REF:<dom_form_id_name>

       < comparison operator > in ['EQ', 'NE', 'GE', 'GT', 'LT', 'LE', 'LIKE', 'NOT LIKE']

       Examples:

       VALUE_LIST_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE_LOP:OR:VALUE_LIST:DOM_REF:molecular_type

       Value condition(s) with indirect reference

       VALUE_KEYED_CONDITION:<ordinal>:LOP:<logical conjunction>:CONDITION_LIST_ID:<supporting_condition_list_id>:VALUE:<value>
       VALUE_KEYED_CONDITION:<ordinal>:LOP:<logical conjunction>:CONDITION_LIST_ID:<supporting_condition_list_id>:VALUE:DOM_REF:<dom_form_id_name>

       Example:

       VALUE_KEYED_CONDITION:15:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:solution

       Value condition list:

          Key values from VALUE_KEY_CONDIION declared as a set of VALUE_CONDITIONS.  This provides the
          means to associate a more complex query condition with a single input key value.

       Example:

       CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MR%
       CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MOLECULAR REPLACEMENT%

       CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%SAD%
       CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MAD%

       CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MR%
       CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MOLECULAR REPLACEMENT%
       CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%SAD%
       CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MAD%

       Join condition (ordered):

       JOIN_CONDITION:<ordinal>:LOP:<logical conjunction (pre)>:L_ITEM:<tableId.columnId>:COP:<comparsion operator>:R_ITEM:<value>
       JOIN_CONDITION:<ordinal>:LOP:<logical conjunction (pre)>:L_ITEM:DOM_REF:<dom_form_id_name>:COP:<comparsion operator>:R_ITEM:DOM_REF:<dom_form_id_name>

        Example:

        JOIN_CONDITION:1:LOP:AND:L_ITEM:pdbx_database_related.structure_id:COP:EQ:R_ITEM:entry.id


       Sort order list:

       ORDER_ITEM:1:ITEM:<tableId.columnId>:SORT_ORDER:<ASC,DESC>
       ORDER_ITEM:1:ITEM:DOM_REF:<dom_form_element_id_name>:SORT_ORDER:<ASC,DESC>

       Example:

       ORDER_ITEM:1:ITEM:DOM_REF:xtype
       ORDER_ITEM:2:ITEM:DOM_REF:ytype


    """

    def __init__(self, schemaDefObj, verbose=False):
        """  Input:

             schemaDefObj =  is instance of class derived from SchemaDefBase().

        """
        self.__sd = schemaDefObj
        self.__verbose = verbose
        self.__debug = True
        #
        self.__selectTupList = []
        self.__orgSelectCount = 0

    def build(self, queryDirL=[], domD={}, appendValueConditonsToSelect=False, queryDirSeparator=':', domRefSeparator='|'):
        ''' Build SQL instructure from the input list of query directives and dictionary or dom references.

        '''
        if self.__verbose:
            logger.debug("\n+%s.%s() dom dictionary length domD %d\n" %
                         (self.__class__.__name__, sys._getframe().f_code.co_name, len(domD)))
        tL = []
        qL = []
        self.__selectTupList = []
        #
        #
        for qD in queryDirL:
            tL.extend(qD.split(queryDirSeparator))

        # if self.__verbose:
        #     ("\n+%s.%s() tL %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, tL))
        #
        qL = self.__queryDirSub(inpQueryDirList=tL, domD=domD, domRefSeparator=domRefSeparator)

        if self.__debug:
            logger.debug("+%s.%s() length qL %d\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, len(qL)))
            for q in qL:
                logger.debug("+%s.%s() qL %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, q))
        # Now parse the token list --
        #
        selectD, orderD, conditionD, self.__orgSelectCount = self.__parseTokenList(qL, appendValueConditonsToSelect)
        sqlS = self.__sqlGen(selectD, orderD, conditionD)
        return sqlS

    def getAttributeSelectList(self):
        """  Return the current list of [(tableId,attributeId),...] in query order -
        """
        return self.__selectTupList, self.__orgSelectCount

    def __getTokenD(self, tL, index, nPairs):
        '''  Return a dictionary of token and value pairs in the input list starting at tL[index].
        '''
        tD = {}
        try:
            i1 = index
            i2 = index + nPairs * 2
            for i in range(i1, i2, 2):
                tD[tL[i]] = tL[i + 1]
        except Exception as e:
            if self.__verbose:
                logger.error("\n+%s.%s() fails with index %d nPairs %d tL %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, index, nPairs, tL))
                logger.exception("Failing with %s" % str(e))

        return tD

        #
        #
    def __parseTokenList(self, qdL, appendValueConditonsToSelect=False):
        """
            Parse input list of tokens and return dictionaries of instructions (selections, conditions, sorting order)
            for input to the SQL query generator.
        """
        try:
            selectD = {}
            conditionD = {}
            keyCondD = {}
            condListD = {}
            orderD = {}
            #
            tD = {}
            #
            i = 0
            while i < len(qdL):
                # Get selections  -
                #
                if qdL[i] in ['SELECT_ITEM']:
                    ordinal = int(str(qdL[i + 1]))
                    tD = self.__getTokenD(qdL, i + 2, 1)
                    if (('ITEM' in tD) and (tD['ITEM'] is not None)):
                        tdotc = str(tD['ITEM']).split('.')
                        # (tableId, attributeId)  apply the upper case convention used in schema map
                        selectD[ordinal] = (tdotc[0].upper(), tdotc[1].upper())
                    else:
                        if self.__verbose:
                            logger.debug("\n+%s.%s() selection incomplete at i = %d\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, i))
                            for k, v in tD.items():
                                logger.debug(" --- tD --  %r %r\n" % (k, v))
                        # raise ValueError("Selection definition incomplete")
                    i += 4
                    continue
                elif qdL[i] in ['VALUE_CONDITION']:
                    ordinal = int(str(qdL[i + 1]))
                    tD = self.__getTokenD(qdL, i + 2, 4)
                    if (('VALUE' in tD) and (tD['VALUE'] is not None)):
                        if 'LOP' in tD and 'ITEM' in tD and 'COP' in tD:
                            tdotc = str(tD['ITEM']).split('.')
                            tableId = tdotc[0].upper()
                            attributeId = tdotc[1].upper()
                            tObj = self.__sD.getSchemaObject(tableId)
                            aType = tObj.getAttributeType(attributeId)
                            cop = str(tD['COP']).upper()
                            conditionD[ordinal] = {'cType': 'value', 'lOp': tD['LOP'], 'cObj': ((tableId, attributeId), cop, (tD['VALUE'], aType))}
                        else:
                            raise ValueError("Value condition incomplete")
                    else:
                        pass
                    i += 10
                    continue
                elif qdL[i] in ['VALUE_LIST_CONDITION']:
                    ordinal = int(str(qdL[i + 1]))
                    tD = self.__getTokenD(qdL, i + 2, 5)
                    if (('VALUE_LIST' in tD) and (tD['VALUE_LIST'] is not None)):
                        if 'LOP' in tD and 'ITEM' in tD and 'COP' in tD and 'VALUE_LOP' in tD:
                            tdotc = str(tD['ITEM']).split('.')
                            tableId = tdotc[0].upper()
                            attributeId = tdotc[1].upper()
                            tObj = self.__sD.getSchemaObject(tableId)
                            aType = tObj.getAttributeType(attributeId)
                            cop = str(tD['COP']).upper()
                            vLop = str(tD['VALUE_LOP']).upper()
                            if isinstance(tD['VALUE_LIST'], list):
                                vL = tD['VALUE_LIST']
                            else:
                                vL = [tD['VALUE_LIST']]
                            conditionD[ordinal] = {'cType': 'value_list', 'lOp': tD['LOP'], 'cObj': ((tableId, attributeId), cop, vLop, (vL, aType))}
                        else:
                            raise ValueError("Value list condition incomplete")
                    else:
                        pass
                    i += 12
                    continue
                elif qdL[i] in ['JOIN_CONDITION']:
                    ordinal = int(str(qdL[i + 1]))
                    tD = self.__getTokenD(qdL, i + 2, 4)
                    if 'LOP' in tD and 'L_ITEM' in tD and 'COP' in tD and 'R_ITEM' in tD:
                        ltdotc = str(tD['L_ITEM']).split('.')
                        ltableId = ltdotc[0].upper()
                        lattributeId = ltdotc[1].upper()
                        rtdotc = str(tD['R_ITEM']).split('.')
                        rtableId = rtdotc[0].upper()
                        rattributeId = rtdotc[1].upper()
                        cop = str(tD['COP']).upper()
                        conditionD[ordinal] = {'cType': 'join', 'lOp': tD['LOP'], 'cObj': ((ltableId, lattributeId), cop, (rtableId, rattributeId))}
                    else:
                        raise ValueError("Join condition incomplete")
                    i += 10
                    continue
                elif qdL[i] in ['CONDITION_LIST']:
                    # example: CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.solution:COP:LIKE:VALUE:%MR%
                    ordinal = int(str(qdL[i + 1]))
                    tD = self.__getTokenD(qdL, i + 2, 5)
                    if (('VALUE' in tD) and (tD['VALUE'] is not None)):
                        if 'LOP' in tD and 'ITEM' in tD and 'COP' in tD and 'KEY' in tD:
                            tdotc = str(tD['ITEM']).split('.')
                            tableId = tdotc[0].upper()
                            attributeId = tdotc[1].upper()
                            tObj = self.__sD.getSchemaObject(tableId)
                            aType = tObj.getAttributeType(attributeId)
                            cop = str(tD['COP']).upper()
                            ky = str(tD['KEY'])
                            # ('PDB_ENTRY_TMP', 'PDB_ID'), 'LIKE', ('x-ray', 'char'), 'AND')
                            if ordinal not in condListD:
                                condListD[ordinal] = {}
                            if ky not in condListD[ordinal]:
                                condListD[ordinal][ky] = []
                            condListD[ordinal][ky].append((tD['LOP'], (tableId, attributeId), cop, (tD['VALUE'], aType)))
                        else:
                            raise ValueError("Value condition incomplete")
                    else:
                        pass

                    i += 12
                    continue
                elif qdL[i] in ['VALUE_KEYED_CONDITION']:
                    # example: "VALUE_KEYED_CONDITION:15:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:solution"
                    ordinal = int(str(qdL[i + 1]))
                    tD = self.__getTokenD(qdL, i + 2, 3)
                    if (('VALUE' in tD) and (tD['VALUE'] is not None)):
                        if 'LOP' in tD and 'CONDITION_LIST_ID' in tD:
                            keyCondD[ordinal] = (int(str(tD['CONDITION_LIST_ID'])), tD['VALUE'], tD['LOP'])
                        else:
                            raise ValueError("Value key condition incomplete")
                    else:
                        pass
                    i += 8
                    continue
                elif qdL[i] in ['ORDER_ITEM']:
                    ordinal = int(str(qdL[i + 1]))
                    tD = self.__getTokenD(qdL, i + 2, 2)
                    if (('ITEM' in tD) and ('SORT_ORDER' in tD) and (tD['ITEM'] is not None)):
                        tdotc = str(tD['ITEM']).split('.')
                        # (tableId, attributeId)  apply the upper case convention used in schema map
                        if tD['SORT_ORDER'] in ['ASC', 'ASCENDING', 'INCREASING']:
                            sf = 'ASC'
                        elif tD['SORT_ORDER'] in ['DESC', 'DESCENDING', 'DECREASING']:
                            sf = 'DESC'
                        else:
                            sf = 'ASC'

                        orderD[ordinal] = ((tdotc[0].upper(), tdotc[1].upper()), sf)
                    else:
                        if self.__verbose:
                            logger.debug("\n+%s.%s() orderby incomplete at i = %d\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, i))
                            for k, v in tD.items():
                                logger.debug(" --- tD --  %r %r\n" % (k, v))
                        # raise ValueError("Order definition incomplete")
                    i += 6
                    continue
                else:
                    pass
        except Exception as e:
            if self.__verbose:
                logger.error("\n+%s.%s() fails at i = %d\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, i))
                for k, v in tD.items():
                    logger.error(" --- tD --  %r %r\n" % (k, v))
                logger.exception("Failing with %s" % str(e))

        #
        # Create condition groups by expanding key-value condition definition using the supporting condition list info in condListD ...
        #
        for ordinal, keyCond in keyCondD.items():
            condListId, keyValue, lOp = keyCond
            conditionD[ordinal] = {'cType': 'group', 'lOp': lOp, 'cObj': []}
            if condListId in condListD:
                logger.debug("++++condListId %r keyValue %r lOp %r\n" % (condListId, keyValue, lOp))
                if keyValue in condListD[condListId]:
                    for cond in condListD[condListId][keyValue]:
                        logger.debug("+++++++condListId %r keyValue %r lOp %r cond %r\n" % (condListId, keyValue, lOp, cond))
                        # example : ('OR', ('PDBX_WEBSELECT', 'METHOD_TO_DETERMINE_STRUCT'), 'LIKE', ('MOLECULAR REPLACEMENT', 'char')
                        # using  condListD[ordinal][ky].append((tD['LOP'], (tableId, attributeId), cop, (tD['VALUE'], aType)))
                        conditionD[ordinal]['cObj'].append(cond)
        #
        if self.__verbose:
            for k, v in selectD.items():
                logger.debug("\n+%s.%s() select %r  %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, k, v))
            for k, v in orderD.items():
                logger.debug("\n+%s.%s() order  %r  %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, k, v))
            #
            for k, v in keyCondD.items():
                logger.debug("\n+%s.%s() keycondD  %r  %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, k, v))
            for k1, vD in condListD.items():
                for k2, v in vD.items():
                    logger.debug("\n+%s.%s() condListD %r  %r  %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, k1, k2, v))
            #
            for k1, vD in conditionD.items():
                for k2, v in vD.items():
                    logger.debug("\n+%s.%s() ordinal %3d type %r: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, k1, k2, v))

        #
        orgSelectCount = len(selectD)
        if appendValueConditonsToSelect:
            vSelectL = []
            for k in sorted(conditionD.keys()):
                tD = conditionD[k]
                if tD['cType'] in ['value', 'value_list']:
                    vSelectL.append(tD['cObj'][0])
                elif tD['cType'] in ['group']:
                    cL = tD['cObj']
                    for c in cL:
                        vSelectL.append(c[1])
            nxtOrd = max(selectD.keys()) + 1
            for vSelect in vSelectL:
                selectD[nxtOrd] = vSelect
                nxtOrd += 1

        #
        return selectD, orderD, conditionD, orgSelectCount

    def __sqlGen(self, selectD, orderD, conditionD):
        #
        sqlGen = SqlGenQuery(schemaDefObj=self.__sd, verbose=self.__verbose)

        sTableIdList = []
        #        for sTup in sList:
        for k in sorted(selectD.keys()):
            sTup = selectD[k]
            sqlGen.addSelectAttributeId(attributeTuple=(sTup[0], sTup[1]))
            sTableIdList.append(sTup[0])
            self.__selectTupList.append(sTup)

        sqlCondition = SqlGenCondition(schemaDefObj=self.__sd, verbose=self.__verbose)
        if len(conditionD) > 0:

            for k in sorted(conditionD.keys()):
                cD = conditionD[k]
                cObj = cD['cObj']
                lOp = cD['lOp']
                if cD['cType'] in ['value']:
                    sqlCondition.addValueCondition(lhsTuple=cObj[0], opCode=cObj[1], rhsTuple=cObj[2], preOp=lOp)
                elif cD['cType'] in ['join']:
                    sqlCondition.addJoinCondition(lhsTuple=cObj[0], opCode=cObj[1], rhsTuple=cObj[2], preOp=lOp)
                elif cD['cType'] in ['group']:
                    sqlCondition.addGroupValueConditionList(cD['cObj'], preOp=lOp)
                elif cD['cType'] in ['value_list']:
                    # build cDefList = [(lPreOp,lhsTuple, opCode, rhsTuple), ...] from value_list -
                    # cObj  = ((tableId, attributeId), cop, vLop, (tD['VALUE_LIST'], aType))}
                    #
                    vL = cObj[3][0]
                    #
                    vType = cObj[3][1]
                    vOp = cObj[2]
                    lhsTuple = cObj[0]
                    cOp = cObj[1]
                    cDefList = []
                    for v in vL:
                        cDefList.append((vOp, lhsTuple, cOp, (v, vType)))
                    sqlCondition.addGroupValueConditionList(cDefList, preOp=lOp)
                else:
                    pass

        sqlCondition.addTables(sTableIdList)
        sqlGen.setCondition(sqlCondition)

        for k in sorted(orderD.keys()):
            oTup, sf = orderD[k]
            sqlGen.addOrderByAttributeId(attributeTuple=oTup, sortFlag=sf)
        #
        sqlS = sqlGen.getSql()
        if (self.__verbose):
            logger.debug("\n+%s.%s() sql:\n%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sqlS))
        sqlGen.clear()
        #
        return sqlS

    def __queryDirSub(self, inpQueryDirList, domD={}, domRefSeparator='|'):
        """  Substitute DOM references into the input query directive list -

             Substitions:
                            DOM_REF    -> domD[DOM_REF value]
                            DOM_REF_#  -> str(domD[DOM_REF value]).split(domRefSeparator)[#] (# = 0,1,2,...)

                            Note -- DOM_REF_1 DOM_REF_2  allows a single dom element name to carry
                                    multiple correlated values as in a "select" (e.g. dom-ref -> myselect = "value1|value2")

        """
        qL = []
        try:
            i = 0
            while (i < len(inpQueryDirList)):
                t = inpQueryDirList[i]
                if t.upper().startswith('DOM_REF_'):
                    indx = int(str(t.upper()).split('_')[2])
                    if inpQueryDirList[i + 1] in domD and domD[inpQueryDirList[i + 1]] is not None and len(domD[inpQueryDirList[i + 1]]) > 0:
                        if (isinstance(domD[inpQueryDirList[i + 1]], list) and (len(domD[inpQueryDirList[i + 1]]) > 1)):
                            tV = [str(tt).split(domRefSeparator)[indx] for tt in domD[inpQueryDirList[i + 1]]]
                        elif (isinstance(domD[inpQueryDirList[i + 1]], list) and (len(domD[inpQueryDirList[i + 1]]) == 1)):
                            tV = str(domD[inpQueryDirList[i + 1]][0]).split(domRefSeparator)[indx]
                        else:
                            tV = str(domD[inpQueryDirList[i + 1]]).split(domRefSeparator)[indx]
                    else:
                        tV = ''
                    qL.append(tV if len(tV) > 0 else None)
                    i += 1
                elif t.upper() in ['DOM_REF']:
                    if ((inpQueryDirList[i + 1] in domD) and (domD[inpQueryDirList[i + 1]] is not None) and (len(domD[inpQueryDirList[i + 1]]) > 0)):
                        if (isinstance(domD[inpQueryDirList[i + 1]], list) and (len(domD[inpQueryDirList[i + 1]]) > 1)):
                            tV = domD[inpQueryDirList[i + 1]]
                        elif (isinstance(domD[inpQueryDirList[i + 1]], list) and (len(domD[inpQueryDirList[i + 1]]) == 1)):
                            tV = domD[inpQueryDirList[i + 1]][0]
                        else:
                            tV = domD[inpQueryDirList[i + 1]]
                    else:
                        tV = ''
                    qL.append(tV if len(tV) > 0 else None)
                    i += 1
                else:
                    qL.append(t)
                i += 1
        except Exception as e:
            if self.__verbose:
                logger.error("\n+%s.%s() fails at i = %d\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, i))
                for ii, qd in enumerate(inpQueryDirList):
                    logger.error(" --- qd %4d  %r\n" % (ii, qd))
                logger.exception("Failing with %s" % str(e))

        return qL
