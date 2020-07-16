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
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

import totem.middleground.sqp.tpchquery.TPCHQuery

import org.apache.spark.sql.sqpmeta.{PredInfo, SubQueryInfo}

case class QueryConfig (queryNames: Array[String],
                        numBatches: Array[Int],
                        constraints: Array[String],
                        subQueryInfo: Array[SubQueryInfo],
                        schedulingOrder: Array[Int],
                        queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                        shareTopics: Array[String])

object QueryGenerator {

  def printSubQueryProgram(queryGraph: QueryGraph): Unit = {
    val newQueryGraph = populateCompleteQueryInfo(queryGraph)
    val subQueries = newQueryGraph.subQueries
    subQueries.foreach(subQuery => {
      println(generateQueryStr(subQuery) + "\n")
    })

    val queryConfig = generateQueryConfig(queryGraph)
    printQueryConfig(queryConfig)
  }

  private def printQueryConfig(queryConfig: QueryConfig): Unit = {
    println("Query Config\n")
    queryConfig.subQueryInfo.zipWithIndex.filter(pair => {
      val info = pair._1
      info.predInfoMap.nonEmpty
    }).foreach(pair => {
      val info = pair._1
      val idx = pair._2
      val predMapStr = predMapToString(info.predInfoMap)
      println(s"${queryConfig.queryNames(idx)}\n$predMapStr")
    })
  }

  def predMapToString(predMap: mutable.HashMap[Int, mutable.HashSet[PredInfo]]): String = {
    val strBuf = new StringBuffer()
    predMap.foreach(pair => {
      val qid = pair._1
      val predSetStr = predSetToString(pair._2)
      strBuf.append(s"$qid -> $predSetStr\n")
    })
    strBuf.toString
  }

  def predSetToString(predSet: mutable.HashSet[PredInfo]): String = {
    val strBuf = new StringBuffer()
    strBuf.append("[")
    predSet.iterator.zipWithIndex.foreach(pair => {
      val value = pair._1
      val idx = pair._2
      strBuf.append(s"${value.toString}")
      if (idx != (predSet.size - 1)) strBuf.append(", ")
    })
    strBuf.append("]")
    strBuf.toString
  }

  def generateQueryAndConfiguration(queryGraph: QueryGraph):
  (Array[TPCHQuery], QueryConfig) = {

    val newQueryGraph = populateCompleteQueryInfo(queryGraph)
    val tpchQueries = newQueryGraph.subQueries.map(generateTPCHQuery)
    val queryConfig = generateQueryConfig(queryGraph)

    (tpchQueries, queryConfig)
  }

  private def populateCompleteQueryInfo(queryGraph: QueryGraph):
  QueryGraph = {

    val qidToQuery = queryGraph.qidToQuery
    val subQueries = queryGraph.subQueries

    // Generate max output
    qidToQuery.foreach(pair => generateMaxOutput(pair._2))

    // Generate required output
    qidToQuery.foreach(pair => generateRequiredOutput(pair._2, mutable.HashSet.empty[String]))
    subQueries.foreach(_.genRequiredOutputArray())

    queryGraph
  }

  private def findSubQueries(op: PlanOperator,
                             subQueries: ArrayBuffer[PlanOperator],
                             visited: mutable.HashSet[PlanOperator]): Unit = {
    if (op.parentOps.length > 1 && !visited.contains(op)) {
      subQueries.append(op)
      visited.add(op)
    }

    op.childOps.foreach(findSubQueries(_, subQueries, visited))
  }

  private def generateName(op: PlanOperator, uid: Int): Unit = {
    val qidSet = op.getQidSet

    val qidBuf = new StringBuffer()
    for (idx <- qidSet.indices) {
      qidBuf.append("_")
      qidBuf.append(qidSet(idx))
    }
    val qidStr = qidBuf.toString

    val qName = s"Q$uid$qidStr"

    op.setSubQueryName(qName)
    op.setUID(uid)
  }

  private def generateMaxOutput(tree: PlanOperator): mutable.HashSet[TypeAttr] = {

    var maxOutput = mutable.HashSet.empty[TypeAttr]

    // Already generated max output
    if (tree.possibleOutputAttrs.nonEmpty) {
      tree.possibleOutputAttrs.foreach(maxOutput.add)
    } else {
      tree.childOps.foreach(generateMaxOutput(_).foreach(maxOutput.add))

      // Add the max possible output attrs for the current node
      tree match {
        case scan: ScanOperator =>
          val allAttrs = Catalog.getAllAttrs(scan.getTableName)
          allAttrs.foreach(maxOutput.add)

        case proj: ProjectOperator =>

          val projOutput = proj.getOutputAttrs
          val projAlias = proj.getAliasAttrs
          val newOutput = mutable.HashSet.empty[TypeAttr]

          projOutput.foreach(attrName => {

            if (attrExist(attrName, maxOutput)) {
              newOutput.add(getTypeAttr(attrName, maxOutput))
            } else { // There exist a alias, find the type

              // This is hard coding for TPC-H
              if (attrName.contains("_year")) {
                newOutput.add(TypeAttr(attrName, "int"))
              } else if (attrName.compareTo("cntrycode") == 0) {
                newOutput.add(TypeAttr(attrName, "string"))
              } else if (attrName.compareTo("volume") == 0 ||
                attrName.compareTo("amount") == 0) {
                newOutput.add(TypeAttr(attrName, "double"))
              } else { // Here, alias only includes one attribute
                val realName = projAlias(attrName)
                val typeAttr = getTypeAttr(realName, maxOutput)
                val realType = typeAttr.typeStr
                newOutput.add(TypeAttr(attrName, realType))
              }
            }

          })

          maxOutput = newOutput

        case agg: AggOperator =>

          val aggOutput = agg.getOutputAttrs
          val aggAlias = agg.getAliasAttrs
          val newOutput = mutable.HashSet.empty[TypeAttr]
          val groupByAttrs = agg.getGroupByAttrs

          aggOutput.foreach(attrName => {

            if (groupByAttrs.contains(attrName)) {
              val typeAttr = getTypeAttr(attrName, maxOutput)
              if (typeAttr != null) newOutput.add(typeAttr)
              else System.err.println(s"GroupBy Attr $attrName not exists")
            } else if (attrExist(attrName, maxOutput)) {
              System.err.println(s"alias $attrName exists in Agg input")
            } else { // There exist a alias, find the type

              // This is hard coding for TPC-H
              val realName = aggAlias(attrName)
              if (attrName.compareTo("high_line_count") == 0 ||
                  attrName.compareTo("low_line_count") == 0) {
                newOutput.add(TypeAttr(attrName, "long"))
              } else if (attrName.compareTo("supplier_cnt") == 0) {
                newOutput.add(TypeAttr(attrName, "int"))
              } else if (realName.contains("lit(1L)")) {
                // Here, we consider the count case
                newOutput.add(TypeAttr(attrName, "long"))
              } else { // default case
                newOutput.add(TypeAttr(attrName, "double"))
              }
            }

          })

          maxOutput = newOutput

        case _: JoinOperator =>
        case _: SelectOperator =>
        case _ =>
      }

      val newMaxOutput = mutable.HashSet.empty[TypeAttr]
      maxOutput.foreach(newMaxOutput.add)
      tree.setPossibleOutputAttrs(newMaxOutput)
    }

    maxOutput

  }

  private def generateRequiredOutput(tree: PlanOperator,
                                     requiredOutput: mutable.HashSet[String]): Unit = {

    val requiredAttrs = genRequiredAttrs(requiredOutput, tree.possibleOutputAttrs)
    requiredAttrs.foreach(tree.requireOutputAttrs.add)

    val newReqOutput = mutable.HashSet.empty[String]

    tree match {
      case proj: ProjectOperator =>
        tree.getReferencedAttrs.foreach(newReqOutput.add)

      case agg: AggOperator =>
        tree.getReferencedAttrs.foreach(newReqOutput.add)

      case select: SelectOperator =>
        requiredOutput.foreach(newReqOutput.add)
        tree.getReferencedAttrs.foreach(newReqOutput.add)

      case join: JoinOperator =>
        requiredOutput.foreach(newReqOutput.add)
        tree.getReferencedAttrs.foreach(newReqOutput.add)

      case rootOperator: RootOperator =>
        requiredOutput.foreach(newReqOutput.add)
        rootOperator.childOps(0).getOutputAttrs.foreach(newReqOutput.add)

      case scan: ScanOperator =>
      case _ =>

    }

    tree.childOps.foreach(generateRequiredOutput(_, newReqOutput))
  }

  private def genRequiredAttrs(requiredOutput: mutable.HashSet[String],
                               maxOutput: mutable.HashSet[TypeAttr]): mutable.HashSet[TypeAttr] = {

    val requiredAttrs = mutable.HashSet.empty[TypeAttr]

    requiredOutput.foreach(attrName => {
      val typeAttr = getTypeAttr(attrName, maxOutput)
      if (typeAttr != null) requiredAttrs.add(typeAttr)
    })

    requiredAttrs
  }

  private def attrExist(attrName: String, attrSet: mutable.HashSet[TypeAttr]): Boolean = {
    attrSet.foreach(typeAttr => {
      if (typeAttr.attr.compareTo(attrName) == 0) return true
    })

    false
  }

  private def getTypeAttr(attrName: String, attrSet: mutable.HashSet[TypeAttr]): TypeAttr = {
    attrSet.foreach(typeAttr => {
      if (typeAttr.attr.compareTo(attrName) == 0) return typeAttr
    })

    null
  }

  private def generateQueryStr(subQuery: PlanOperator): String = {
    val intermediateTable = genIntermediateTable(subQuery)
    val dfProgram = genProgram(subQuery)

    s"""
        |
        |import totem.middleground.tpch._
        |import totem.middleground.sqp.tpchquery.TPCHQuery
        |
        |import org.apache.spark.sql.{DataFrame, SparkSession}
        |import org.apache.spark.sql.avro.{from_avro, SchemaConverters}
        |import org.apache.spark.sql.functions._
        |import org.apache.spark.sql.types.StructType
        |
        |private class ${subQuery.subQueryName} extends TPCHQuery {
        |    $intermediateTable
        |
        |    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
        |       $dfProgram
        |    }
        |}
        |
        |scala.reflect.classTag[${subQuery.subQueryName}].runtimeClass
        |
      """.stripMargin
  }

  private def generateTPCHQuery(subQuery: PlanOperator): TPCHQuery = {

    val queryStr = generateQueryStr(subQuery)

    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val classDef = tb.parse(queryStr)
    val clazz = tb.compile(classDef).apply().asInstanceOf[Class[TPCHQuery]]
    clazz.getConstructor().newInstance()
  }

  private def genIntermediateTable(subQuery: PlanOperator): String = {
    val strBuf = new StringBuffer().append("\n")
    val visited = mutable.HashSet.empty[PlanOperator]
    genTableDefHelper(subQuery, strBuf, visited, true)
    strBuf.toString
  }

  private def genTableDefHelper(subQuery: PlanOperator,
                                strBuf: StringBuffer,
                                visited: mutable.HashSet[PlanOperator],
                                isRoot: Boolean): Unit = {

    if (subQuery.parentOps.length > 1 && !isRoot) { // One intermediate table
      if (!visited.contains(subQuery)) {
        val tableName = subQuery.subQueryName
        strBuf.append(
          s"private val $tableName = new StructType()\n"
        )
        subQuery.requireOutputArray.foreach(typeAttr => {
          val attrName = typeAttr.attr
          val typeName = typeAttr.typeStr
          val str = s""".add("$attrName", "$typeName")\n"""
          strBuf.append(str)
        })
        strBuf.append("\n")
        visited.add(subQuery)
      }
    } else {
      subQuery.childOps.foreach(genTableDefHelper(_, strBuf, visited, false))
    }
  }

  private def genProgram(subQuery: PlanOperator): String = {
    val strBuf = new StringBuffer()
    val aggFuncDef = mutable.HashMap.empty[String, String]

    strBuf.append(
      """
        |import spark.implicits._
      """.stripMargin)

    val corProg = genProgramHelper(subQuery, aggFuncDef, true)
    strBuf.append("\n")
    aggFuncDef.foreach(pair => {
      strBuf.append(s"val ${pair._1} = new ${pair._2}\n")
    })
    strBuf.append("\nval result = ")
    strBuf.append(corProg)

    if (subQuery.isInstanceOf[RootOperator]) {
      strBuf.append(
        s"""
          | DataUtils.writeToSinkWithExtraOptions(
          |   result, query_name, uid, numBatch, constraint)
        """.stripMargin)
    } else { // write intermediate table
      strBuf.append(
        s"""
           | DataUtils.writeToKafkaWithExtraOptions(
           |    result, "${subQuery.subQueryName}", query_name, uid,
           |       numBatch, constraint, tpchSchema.checkpointLocation)
         """.stripMargin
      )
    }

    strBuf.toString
  }

  private def genProgramHelper(subQuery: PlanOperator,
                               aggFuncDef: mutable.HashMap[String, String],
                               isRoot: Boolean): String = {

    val newStrBuf = new StringBuffer()
    if (subQuery.parentOps.length > 1 && !isRoot) { // This is the case for loading shared table

      newStrBuf.append(
        s"""loadSharedTable(spark, "${subQuery.subQueryName}", ${subQuery.subQueryName})"""
      )

    } else {

      val childStrs = subQuery.childOps.map(child => {
        genProgramHelper(child, aggFuncDef, false)
      })

      subQuery match {
        case scan: ScanOperator =>
          val tableAlias = Catalog.getTableAlias(scan.getTableName)
          val str =
            "DataUtils.loadStreamTable(spark, " +
               s""""${scan.getTableName}", "$tableAlias", tpchSchema)"""
          newStrBuf.append(str)
        case proj: ProjectOperator =>
          newStrBuf.append(childStrs(0))
          newStrBuf.append("\n")
          newStrBuf.append(proj.getDFStr)
        case select: SelectOperator =>
          newStrBuf.append(childStrs(0))
          newStrBuf.append("\n")
          newStrBuf.append(select.getDFStr)
        case join: JoinOperator =>
          newStrBuf.append(s"(${childStrs(0)})\n")
          newStrBuf.append(
            s""".join(${childStrs(1)}, ${join.getJoinCondition}, "${join.getJoinType}")"""
          )
        case agg: AggOperator =>
          agg.getAGGFuncDef.foreach(pair => {
            aggFuncDef.put(pair._1, pair._2)
          })
          newStrBuf.append(childStrs(0))
          newStrBuf.append("\n")
          newStrBuf.append(agg.getDFStr.trim)
        case _ => // RootOperator
          newStrBuf.append(childStrs(0))
          newStrBuf.append("\n.select(\"*\")")
      }
    }

    if (isRoot && subQuery.parentOps.length > 1) {
      newStrBuf.append("\n.select(")
      subQuery.requireOutputArray.zipWithIndex.foreach(pair => {
        val outputAttr = pair._1
        val idx = pair._2
        newStrBuf.append(s"""$$"${outputAttr.attr}"""")
        if (idx != subQuery.requireOutputArray.length - 1) {
          newStrBuf.append(", ")
        }
      })
      newStrBuf.append(")")
    }

    newStrBuf.toString
  }

  private def generateQueryConfig(queryGraph: QueryGraph): QueryConfig = {
    val numBatches = queryGraph.numBatches
    val constraints = Array.fill[String](numBatches.length)("1.0")
    val queryDependency = queryGraph.queryDependency
    val subQueries = queryGraph.subQueries
    val schedulingOrder = queryGraph.schedulingOrder

    val queryNames = subQueries.map(_.subQueryName)
    val shareTopics = subQueries.filter(_.parentOps.length > 1).map(_.subQueryName)

    val subQueryInfo = subQueries.map(subQuery => {

      val predInfoMap = mutable.HashMap.empty[Int, mutable.HashSet[PredInfo]]
      val aggCluster = new ArrayBuffer[Long]()
      val qidSet = new ArrayBuffer[Int]()
      val isRoot = true
      subQuery.getQidSet.foreach(qidSet.append(_))

      collectQueryInfo(subQuery, predInfoMap, aggCluster, isRoot)
      SubQueryInfo(qidSet.toArray, predInfoMap, aggCluster.toArray)
    })

    QueryConfig(queryNames, numBatches, constraints, subQueryInfo,
      schedulingOrder, queryDependency, shareTopics)
  }

  private def collectQueryInfo(subQuery: PlanOperator,
                               predInfoMap: mutable.HashMap[Int, mutable.HashSet[PredInfo]],
                               aggCluster: ArrayBuffer[Long], isRoot: Boolean): Unit = {

    if (subQuery.parentOps.length == 1 || isRoot) {

      subQuery.childOps.foreach(child => {
        collectQueryInfo(child, predInfoMap, aggCluster, false)
      })

      subQuery match {
        case select: SelectOperator =>
          if (select.getQidSet.length > 1) {
            select.selectSet.foreach(qid => {
              if (!predInfoMap.contains(qid)) {
                val newPredInfoSet = mutable.HashSet.empty[PredInfo]
                predInfoMap.put(qid, newPredInfoSet)
              }
              val predInfoSet = predInfoMap(qid)
              select.getPredicates.foreach(pred => {
                convertPredToInternal(pred).foreach(predInfoSet.add)
              })
            })
          }
        case agg: AggOperator =>
          if (aggCluster.isEmpty) {
            val qidSet = agg.getQidSet
            var qidCluster = 0L
            qidSet.foreach(qid => {
              qidCluster |= (1L << qid)
            })
            aggCluster.append(qidCluster)
            // qidSet.map(convertQidToBitVec).foreach(aggCluster.append(_))
          }
        case _ =>
      }
    }

  }

  private def convertQidToBitVec(qid: Int): Long = {
    1L << qid
  }

  private def convertPredToInternal(predicate: Predicate): Array[PredInfo] = {
    val left = predicate.left
    val op = predicate.op
    val right = predicate.right

    if (op.compareTo("between") == 0) {
      val rightArray = stripParenthesis(right).split(",").map(_.trim).map(stripQuote)
      Array(
        PredInfo(left, ">=", rightArray(0)),
        PredInfo(left, "<=", rightArray(1)))
    } else if (op.compareTo("like") == 0) {
      val (newRight, mode) = stripModSymbol(stripQuote(stripParenthesis(right)))
      if (mode == 0) Array(PredInfo(left, "Contains", newRight))
      else if (mode == 1) Array(PredInfo(left, "StartsWith", newRight))
      else Array(PredInfo(left, "EndsWith", newRight))
    } else if (op.compareTo("===") == 0) {
      Array(PredInfo(left, "=", stripQuote(right.trim)))
    } else if (op.compareTo("=!=") == 0) {
      Array(PredInfo(left, "!=", stripQuote(right.trim)))
    } else if (op.compareTo("isin") != 0) {
      Array(PredInfo(left, op, stripQuote(right.trim)))
    } else {
      Array.empty[PredInfo]
    }
  }

  private def stripQuote(str: String): String = {
    val quote = '\"'
    if (str(0) != quote) str
    else str.substring(1, str.length - 1)
  }

  private def stripParenthesis(str: String): String = {
    val left = '('
    if (str(0) != left) str
    else str.substring(1, str.length - 1)
  }

  // 0 contains
  // 1 StartsWith
  // 2 EndsWith
  private def stripModSymbol(str: String): (String, Int) = {
    val mod = '%'
    if (str(0) == mod && str(str.length - 1) == mod) {
      (str.substring(1, str.length - 1), 0)
    } else if (str(str.length - 1) == mod) {
      (str.substring(0, str.length - 1), 1)
    } else {
      (str.substring(1, str.length), 2)
    }
  }

}
