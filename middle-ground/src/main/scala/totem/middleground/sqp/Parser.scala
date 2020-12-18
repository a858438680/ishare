/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

package totem.middleground.sqp

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.io.Source

object ParsingState extends Enumeration {
  type ParssingState = Value

  val PreProcess = Value("PreProcess")
  val Leaf = Value("LeafPhase")
  val NonLeaf = Value("NonLeafPhase")
  val End = Value("EndPhase")
}

object Parser {

  private var curQid = 0
  private val NULLSTR = ""
  private val RETURNSTR = "return"
  private val COMMACHAR = ','
  private val LEFTPARENTHESIS = '('
  private val RIGHTPARENTHESIS = ')'
  private val ANDSTR = "and"
  private val DOTSTR = "."
  private val VALSTR = "val"
  private val LONGEQSTR = "==="
  private val GSTR = ">"
  private val GEQSTR = ">="
  private val SHORTEQSTR = "="
  private val LEFTATTR = "$\""
  private val RIGHTATTR = "\""
  private val NEWSTR = "new"
  private val SPARKSTR = "spark"
  private val LITSTR = "lit(1L)"

  val INNER = 1
  val OUTER = 0

  def parseQuery(fileName: String, qid: Int): PlanOperator = {
    val lines = Source.fromFile(fileName).getLines().map(_.trim).toArray
    curQid = qid

    val subQuery = parseSubQuery(lines, 0)
    val rootOperator = Utils.genRootOperator(curQid)

    rootOperator.setChildren(Array[PlanOperator](subQuery))
    subQuery.setParents(Array[PlanOperator](rootOperator))

    rootOperator
  }

  def extractRawColomn(str: String): String = {
    getStringBetween(str, LEFTATTR, RIGHTATTR)
  }

  private def parseSubQuery(lines: Array[String], curLine: Int): PlanOperator = {
    var cur = nextLine(lines, curLine) // Find the first non-blank line
    var parsingState = ParsingState.PreProcess
    var symbol: String = NULLSTR
    var op: PlanOperator = null
    val aggFuncDef = new mutable.HashMap[String, String]()
    val symbolToOp = new mutable.HashMap[String, PlanOperator]()

    while (parsingState != ParsingState.End) {
      parsingState match {
        case ParsingState.PreProcess =>
          val pair = preProcess(lines, cur, aggFuncDef)
          parsingState = pair._1
          cur = pair._2

        case ParsingState.Leaf =>
          val quadruple = parseLeaf(lines, cur)
          parsingState = quadruple._1
          cur = quadruple._2
          symbol = quadruple._3
          op = quadruple._4
          if (quadruple._4 != null) symbolToOp.put(symbol, op)

        case ParsingState.NonLeaf =>
          val quadruple = parseNonLeaf(lines, cur, aggFuncDef, symbolToOp)
          parsingState = quadruple._1
          cur = quadruple._2
          symbol = quadruple._3
          op = quadruple._4
          if (quadruple._4 != null) symbolToOp.put(symbol, op)
        case _ =>
      }
    }

    assert(symbolToOp.size == 1)
    op
  }

  private def nextLine(lines: Array[String], cur: Int): Int = {
    var nextLine = cur + 1
    while (lines(nextLine).trim.isEmpty) {
      nextLine += 1
    }
    nextLine
  }

  private def preProcess(lines: Array[String],
                         cur: Int,
                         aggFuncDef: HashMap[String, String]): (ParsingState.Value, Int) = {
    // Transit into leaf state
    if (isLeafParsingState(lines(cur))) {
      return (ParsingState.Leaf, cur)
    }

    // Still in Preprocess, parse the current line
    val line = lines(cur)
    val funcName = getStringBetween(line, VALSTR, SHORTEQSTR)
    val funcType = getStringAfter(line, NEWSTR)
    aggFuncDef.put(funcName, funcType)

    // Next line
    val nextIdx = nextLine(lines, cur)
    (ParsingState.PreProcess, nextIdx)
  }

  private def parseLeaf(lines: Array[String],
                        cur: Int): (ParsingState.Value, Int, String, PlanOperator) = {
    var curIdx = cur
    var line = lines(curIdx)

    // Transit into non-leaf state
    if (!isLeafParsingState(line)) {
      return (ParsingState.NonLeaf, curIdx, NULLSTR, null)
    }

    // Now, let's process a scan operator along with its filter
    val (symbol, scanOperator) = parseScanOperator(line)

    curIdx = nextLine(lines, curIdx)
    line = lines(curIdx)
    var selectOperator = scanOperator
    if (line.startsWith(".filter")) {
      selectOperator = parseSelectOperator(line, true, scanOperator)

      // Next line
      curIdx = nextLine(lines, curIdx)
    }

    (ParsingState.Leaf, curIdx, symbol, selectOperator)
  }

  private def parseNonLeaf(lines: Array[String],
                           cur: Int,
                           aggFuncDef: mutable.HashMap[String, String],
                           symbolToOp: mutable.HashMap[String, PlanOperator]):
  (ParsingState.Value, Int, String, PlanOperator) = {
    var curIdx = cur
    var line = lines(curIdx)

    val symbol =
      if (line.startsWith(VALSTR)) getStringBetween(line, VALSTR, SHORTEQSTR)
      else NULLSTR

    var parsingState = ParsingState.NonLeaf

    if (line.indexOf("spark") != -1) { // This is a subquery
      val subQueryName = getStringBetween(line, SHORTEQSTR, "(spark")
      val subQueryIdx = findSubQuery(lines, subQueryName)
      val subOP = parseSubQuery(lines, subQueryIdx)
      curIdx = nextLine(lines, curIdx)

      return (parsingState, curIdx, symbol, subOP)
    }

    val rootSymbol =
      if (line.startsWith(VALSTR)) getStringAfter(line, SHORTEQSTR)
      else {
        parsingState = ParsingState.End
        getStringAfter(line, RETURNSTR)
      }

    var rootOP = symbolToOp.getOrElse(rootSymbol, {
      println(s"Error ${rootSymbol} not found")
      null
    })
    symbolToOp.remove(rootSymbol)

    curIdx = nextLine(lines, curIdx)
    line = lines(curIdx)
    while (line.startsWith(DOTSTR)) { // a new op starts with dot

      getStringBetween(line, DOTSTR, LEFTPARENTHESIS.toString) match {
        case "filter" =>
          rootOP = parseSelectOperator(line, false, rootOP)
        case "select" =>
          rootOP = parseProjectOperator(line, rootOP)
        case "groupBy" =>
          val pair = parseGroupByAggOperator(lines, curIdx, aggFuncDef, rootOP)
          curIdx = pair._1
          rootOP = pair._2
        case "agg" =>
          val groupByAttrs = mutable.HashSet.empty[String]
          val pair = parseAggOperator(lines, curIdx, groupByAttrs, aggFuncDef, rootOP)
          curIdx = pair._1
          rootOP = pair._2
        case "join" =>
          val pair = parseJoinOperator(lines, curIdx, symbolToOp, rootOP)
          curIdx = pair._1
          rootOP = pair._2
        case _ =>
          println(s"Error ${line} does not start with a valid operator")
      }

      curIdx = nextLine(lines, curIdx)
      line = lines(curIdx)
    }

    (parsingState, curIdx, symbol, rootOP)

  }

  private def findSubQuery(lines: Array[String], subQueryName: String): Int = {
    val subQueryDef = "def " + subQueryName
    for (idx <- lines.indices) {
      if (lines(idx).indexOf(subQueryDef) != -1) {
        return idx
      }
    }

    System.err.println(s"Error: Subquery ${subQueryName} not found")
    System.exit(1)
    -1
  }

  private def parseScanOperator(line: String): (String, PlanOperator) = {
    val symbol = getStringBetween(line, VALSTR, SHORTEQSTR)
    val tableName = getStringBetween(line, "spark, \"", "\"")

    val dfStr = line
    val qidSet = Array(curQid)
    val outputAttrs = mutable.HashSet.empty[String]
    val referencedAttrs = mutable.HashSet.empty[String]
    val aliasAttrs = mutable.HashMap.empty[String, String]

    val scanOperator = new ScanOperator(qidSet, outputAttrs, referencedAttrs,
      aliasAttrs, dfStr, tableName)

    (symbol, scanOperator)
  }

  private def parseSelectOperator(line: String,
                                  leafFilter: Boolean,
                                  childOP: PlanOperator): PlanOperator = {
    val dfStr = line
    val qidSet = Array(curQid)
    val outputAttrs = mutable.HashSet.empty[String]
    val referencedAttrs = mutable.HashSet.empty[String]
    val aliasAttrs = mutable.HashMap.empty[String, String]

    // strip the parentheses
    val start = line.indexOf(LEFTPARENTHESIS)
    val predStr = line.substring(start + 1, line.length - 1)

    val predStrArray = topLevelSplit(predStr, s" $ANDSTR ")
    val predArray =
      if (leafFilter) {
        val tmpPredArray = new Array[Predicate](predStrArray.length)
        predStrArray.zipWithIndex.foreach(pair => {
          val str = pair._1
          val idx = pair._2
          val pred = parsePredicate(str, referencedAttrs)
          tmpPredArray(idx) = pred
        })
        tmpPredArray
      } else {
        new Array[Predicate](0)
      }

    val selectOp =
      new SelectOperator(qidSet, outputAttrs, referencedAttrs,
        aliasAttrs, dfStr, predArray, qidSet)

    selectOp.setChildren(Array(childOP))
    childOP.setParents(Array(selectOp))

    selectOp
  }

  private def parsePredicate(pred: String,
                             referencedAttrs: mutable.HashSet[String]): Predicate = {
    val rawPred = stripParentheses(pred.trim)
    val predArray = topLevelSplit(rawPred, " ")

    val left =
      if (predArray(0).startsWith(LEFTATTR)) {
        val tmpAttr = getStringBetween(predArray(0), LEFTATTR, RIGHTATTR)
        referencedAttrs.add(tmpAttr)
        tmpAttr
      } else predArray(0)

    val right =
      if (predArray(2).startsWith(LEFTATTR)) {
        val tmpAttr = getStringBetween(predArray(2), LEFTATTR, RIGHTATTR)
        referencedAttrs.add(tmpAttr)
        tmpAttr
      } else {
        if (predArray.length > 3) {
          val predBuf = new StringBuffer()
          predArray.zipWithIndex.foreach(pair => {
            val str = pair._1
            val idx = pair._2
            if (idx >=2 ) {
              predBuf.append(predArray(idx))
              if (idx != (predArray.length - 1)) predBuf.append(" ")
            }
          })
          predBuf.toString
        } else predArray(2)
      }

    Predicate(left, predArray(1), right)
  }

  private def parseProjectOperator(line: String, childOP: PlanOperator): PlanOperator = {
    val dfStr = line
    val qidSet = Array(curQid)
    val outputAttrs = mutable.HashSet.empty[String]
    val referencedAttrs = mutable.HashSet.empty[String]
    val aliasAttrs = mutable.HashMap.empty[String, String]

    // strip the parentheses
    val start = line.indexOf(LEFTPARENTHESIS)
    val projStr = line.substring(start + 1, line.length - 1)

    val projStrArray = topLevelSplit(projStr, COMMACHAR.toString)
    projStrArray.foreach(parseRefAlias(_, outputAttrs, referencedAttrs, aliasAttrs))

    val projOp = new ProjectOperator(qidSet, outputAttrs, referencedAttrs, aliasAttrs, dfStr)

    projOp.setChildren(Array(childOP))
    childOP.setParents(Array(projOp))

    projOp
  }

  private def parseRefAlias(refStr: String,
                            outputAttrs: mutable.HashSet[String],
                            referencedAttrs: mutable.HashSet[String],
                            aliasAttrs: mutable.HashMap[String, String]): Unit = {
    // find all referenced attrs
    val attrs = getMultipleStringBetween(refStr, LEFTATTR, RIGHTATTR)
    if (refStr.indexOf(LITSTR) != -1) attrs.add(LITSTR)

    val alias = getStringBetween(refStr, ".as(\"", "\")")

    attrs.foreach(referencedAttrs.add)

    if (alias.compareTo(NULLSTR) == 0) { // no alias
      assert(attrs.size == 1)
      attrs.foreach(outputAttrs.add)
    } else { // use alias
      outputAttrs.add(alias)
      val combinedStrBuf = new StringBuffer()
      attrs.iterator.zipWithIndex.foreach(pair => {
        val attr = pair._1
        val idx = pair._2
        combinedStrBuf.append(attr)
        if (idx != (attrs.size - 1)) combinedStrBuf.append(COMMACHAR)
      })
      aliasAttrs.put(alias, combinedStrBuf.toString)
    }
  }

  private def parseGroupByAggOperator(lines: Array[String],
                                      curIdx: Int,
                                      aggFuncDef: mutable.HashMap[String, String],
                                      childOP: PlanOperator): (Int, PlanOperator) = {
    val line = lines(curIdx)
    val groupByAttrsArray = getMultipleStringBetween(line, LEFTATTR, RIGHTATTR)
    val groupByAttrs = mutable.HashSet.empty[String]
    groupByAttrsArray.foreach(groupByAttrs.add)

    parseAggOperator(lines, nextLine(lines, curIdx), groupByAttrs, aggFuncDef, childOP)
  }

  private def parseAggOperator(lines: Array[String],
                               cur: Int,
                               groupByAttrs: mutable.HashSet[String],
                               aggFuncDef: mutable.HashMap[String, String],
                               childOP: PlanOperator): (Int, PlanOperator) = {
    val startIdx =
      if (groupByAttrs.isEmpty) cur
      else cur - 1 // Including the groupBy line

    val qidSet = Array(curQid)
    val outputAttrs = mutable.HashSet.empty[String]
    val referencedAttrs = mutable.HashSet.empty[String]
    val aliasAttrs = mutable.HashMap.empty[String, String]
    val aggFunc = mutable.HashMap.empty[String, String]
    val aliasFunc = mutable.HashMap.empty[String, String]

    groupByAttrs.foreach(outputAttrs.add)
    groupByAttrs.foreach(referencedAttrs.add)

    var curIdx = nextLine(lines, cur) // skip the first line (start with .agg)
    var line = lines(curIdx)
    while (!newOperator(line) && includeAggFunc(line, aggFunc, aggFuncDef)) {
      parseRefAlias(line, outputAttrs, referencedAttrs, aliasAttrs)
      mapFromAliasToAggFunc(line, aggFuncDef, aliasFunc)

      curIdx = nextLine(lines, curIdx)
      line = lines(curIdx)
    }

    val strBuf = new StringBuffer()
    for (idx <- startIdx until curIdx) {
      strBuf.append(lines(idx))
      strBuf.append("\n")
    }
    val dfStr = strBuf.toString

    val aggOp = new AggOperator(qidSet, outputAttrs, referencedAttrs,
      aliasAttrs, dfStr, aliasFunc, groupByAttrs, aggFunc)

    aggOp.setChildren(Array(childOP))
    childOP.setParents(Array(aggOp))

    (curIdx - 1, aggOp)
  }

  private def includeAggFunc(line: String,
                             aggFunc: mutable.HashMap[String, String],
                             aggFuncDef: mutable.HashMap[String, String]): Boolean = {

    var includeAgg = false

    Array("max", "min").foreach(funcStr => {
      if (line.indexOf(funcStr) != -1) includeAgg = true
    })

    aggFuncDef.foreach(pair => {
      if (line.indexOf(pair._1) != -1) {
        aggFunc.put(pair._1, pair._2)
        includeAgg = true
      }
    })

    includeAgg
  }

  private def mapFromAliasToAggFunc(line: String,
                                    aggFuncDef: mutable.HashMap[String, String],
                                    aliasToFunc: mutable.HashMap[String, String]): Unit = {

    val alias = getStringBetween(line, ".as(\"", "\")")

    Array("max", "min").foreach(funcStr => {
      if (line.indexOf(funcStr) != -1) aliasToFunc.put(alias, funcStr)
    })

    aggFuncDef.foreach(pair => {
      if (line.indexOf(pair._1) != -1) {
        aliasToFunc.put(alias, pair._1)
      }
    })
  }

  private def parseJoinOperator(lines: Array[String],
                                cur: Int,
                                symbolToOp: mutable.HashMap[String, PlanOperator],
                                outerOP: PlanOperator): (Int, PlanOperator) = {
    val qidSet = Array(curQid)
    val outputAttrs = mutable.HashSet.empty[String]
    val referencedAttrs = mutable.HashSet.empty[String]
    val aliasAttrs = mutable.HashMap.empty[String, String]

    // Get full String
    val strBuf = new StringBuffer
    strBuf.append(lines(cur))
    strBuf.append("\n")

    var curIdx = nextLine(lines, cur)
    var line = lines(curIdx)
    while(!newOperator(line)) {
      strBuf.append(line)
      strBuf.append("\n")

      curIdx = nextLine(lines, curIdx)
      line = lines(curIdx)
    }

    val dfStr = strBuf.toString.trim

    // strip parentheses
    val startIdx = dfStr.indexOf(LEFTPARENTHESIS)
    val joinStr  = dfStr.substring(startIdx + 1, dfStr.length - 1)

    val joinStrArray = topLevelSplit(joinStr, COMMACHAR.toString)
    val innerSymbol = joinStrArray(0)
    val joinCondition = joinStrArray(1)
    val joinType =
      if (joinStrArray.length == 2) "inner"
      else getStringBetween(joinStrArray(2), "\"", "\"")

    val innerOP = symbolToOp.getOrElse(innerSymbol, {
      println(s"Error inner ${innerSymbol} not found")
      null
    })
    symbolToOp.remove(innerSymbol)

    // Add referenced attrs
    getMultipleStringBetween(joinCondition, LEFTATTR, RIGHTATTR).foreach(referencedAttrs.add)

    var andIdx = joinCondition.indexOf(ANDSTR)
    var postIdx = 0
    if (andIdx == -1) {
      andIdx = joinCondition.length
      postIdx = joinCondition.length
    } else {
      postIdx = andIdx + ANDSTR.length
    }

    val primaryJoin = joinCondition.substring(0, andIdx)
    var op: String = ""
    val joinKey =
      if (primaryJoin.indexOf(LONGEQSTR) != -1) {
        op = LONGEQSTR
        // primaryJoin.split(LONGEQSTR).map(getStringBetween(_, LEFTATTR, RIGHTATTR))
        primaryJoin.split(LONGEQSTR).map(_.trim)
      } else if (primaryJoin.indexOf(GEQSTR) != -1) {
        op = GEQSTR
        // primaryJoin.split(GSTR).map(getStringBetween(_, LEFTATTR, RIGHTATTR))
        primaryJoin.split(GEQSTR).map(_.trim)
      } else {
        op = GSTR
        // primaryJoin.split(GEQSTR).map(getStringBetween(_, LEFTATTR, RIGHTATTR))
        primaryJoin.split(GSTR).map(_.trim)
      }
    val postFilter = joinCondition.substring(postIdx, joinCondition.length)

    val joinOP = new JoinOperator(qidSet, outputAttrs, referencedAttrs,
      aliasAttrs, dfStr, joinKey, op, joinType, postFilter)

    // Set parent/child
    joinOP.setChildren(Array(outerOP, innerOP))
    innerOP.setParents(Array(joinOP))
    outerOP.setParents(Array(joinOP))

    return (curIdx - 1, joinOP)
  }

  private def newOperator(line: String): Boolean = {

    if (line.startsWith(VALSTR) ||
        line.startsWith(DOTSTR) ||
        line.startsWith(RETURNSTR) ||
        line.startsWith("}")) {
      true
    } else {
      false
    }

  }

  // Utilities

  private def isLeafParsingState(line: String): Boolean = {
    if (line.indexOf("DataUtils.loadStreamTable") != -1) true
    else false
  }

  private def isRangePred(op: String): Boolean = {
    op match {
      case LONGEQSTR =>
        false
      case _ =>
        true
    }
  }

  private def isAttr(attr: String): Boolean = {
    if (attr.startsWith("$\"")) true
    else false
  }

  private def getStringBetween(line: String, start: String, end: String): String = {
    val fromIndex = line.indexOf(start) + start.length
    if (fromIndex - start.length == -1) return NULLSTR

    val toIndex = line.indexOf(end, fromIndex)
    line.substring(fromIndex, toIndex).trim
  }

  private def getMultipleStringBetween(line: String, start: String,
                                   end: String): HashSet[String] = {
    val strSet = new mutable.HashSet[String]()
    var startIdx = 0
    var endIdx = 0
    var fromIdx = 0
    while (fromIdx != -1 && fromIdx < line.length) {
      startIdx = line.indexOf(start, fromIdx)
      endIdx = line.indexOf(end, startIdx + start.length)

      if (startIdx != -1 && endIdx != -1) {
        strSet.add(line.substring(startIdx + start.length, endIdx).trim)
      } else {
        endIdx = line.length
      }

      fromIdx = line.indexOf(start, endIdx)
    }

    strSet
  }

  private def getStringAfter(line: String, start: String): String = {
    val fromIndex = line.indexOf(start) + start.length
    val retStr = line.substring(fromIndex).trim
    retStr
  }

  private def stripParentheses(line: String): String = {
    if (line(0) != LEFTPARENTHESIS) line
    else line.substring(1, line.length - 1)
  }

  private def topLevelSplit(line: String, splitStr: String): Array[String] = {
    val strArrayBuf = new ArrayBuffer[String]()
    var level = 0
    var startIdx = 0
    var splitIdx = line.indexOf(splitStr, startIdx)
    for (curIdx <- 0 until line.length) {
      val c = line(curIdx)

      if (c == LEFTPARENTHESIS) level += 1
      else if (c == RIGHTPARENTHESIS) level -= 1

      if (curIdx == splitIdx) {
        if (level == 0) {
          strArrayBuf.append(line.substring(startIdx, curIdx))
          startIdx = curIdx + splitStr.length
        }
        splitIdx = line.indexOf(splitStr, curIdx + splitStr.length)
      }
    }
    strArrayBuf.append(line.substring(startIdx, line.length))

    strArrayBuf.toArray
  }

}
