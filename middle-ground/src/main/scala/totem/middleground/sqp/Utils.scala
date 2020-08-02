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

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Utils {

  private val INDENT = "\t"
  private val NULLSTR = ""

  def printClusterSet(cluster: mutable.HashSet[mutable.HashSet[Int]]): Unit = {
    println("\nQid Cluster After Optimization")
    cluster.zipWithIndex.foreach(pair => {
      val qidSet = pair._1
      val clusterIdx = pair._2 + 1
      val qidSetStr = qidSetToString(qidSet.toArray)
      println(s"Cluster $clusterIdx: $qidSetStr")
    })
  }

  def printPaceConfig(queryGraph: QueryGraph): Unit = {
    val subQueries = queryGraph.subQueries
    val qidToConstraints = queryGraph.qidToConstraints
    val qidToFinalWork = queryGraph.qidToFinalWork

    println("")
    queryGraph.fullQidSet.foreach(qid => {
      val constraint = qidToConstraints(qid)
      val finalWork = qidToFinalWork(qid)

      var uid = -1
      subQueries.foreach(subQuery => {
        if (subQuery.isInstanceOf[RootOperator] && subQuery.getQidSet(0) == qid) {
          uid = subQuery.subQueryUID
        }
      })

      println(s"Query $qid with constraint $constraint and finalwork $finalWork")
      println("================================================================")
      printPaceConfigHelper(uid, "", queryGraph.numBatches, queryGraph.queryDependency)
      println("")

    })
  }

  private def printPaceConfigHelper(uid: Int,
                                    indent: String,
                                    batchNums: Array[Int],
                                    queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]]):
  Unit = {
    println(s"${indent}Q_${uid}: ${batchNums(uid)}")
    queryDependency(uid).foreach(childUid => {
      printPaceConfigHelper(childUid, indent + "\t", batchNums, queryDependency)
    })
  }

  def printQueryGraph(queryGraph: QueryGraph): Unit = {

    val queue = new mutable.Queue[PlanOperator]
    queryGraph.qidToQuery.foreach(pair => queue.enqueue(pair._2))
    val visited = mutable.HashSet.empty[PlanOperator]

    while (queue.nonEmpty) {
      val newOPArray = printPlanTree(queue.dequeue(), visited)
      newOPArray.foreach(queue.enqueue(_))
      if (queue.nonEmpty) println("==========================")
    }
  }

  private def printPlanTree(op: PlanOperator,
                            visited: mutable.HashSet[PlanOperator]): Array[PlanOperator] = {
    printOPWithIndent(op, NULLSTR, visited)
  }

  private def printOPWithIndent(op: PlanOperator,
                                indent: String,
                                visited: mutable.HashSet[PlanOperator]): Array[PlanOperator] = {

    if (op.parentOps.length > 1) {
      println(s"${indent}Mat ${op.toString}")

      if (visited.contains(op)) {
        Array.empty[PlanOperator]
      } else {
        visited.add(op)
        val newOP = op.copy()
        newOP.setParents(Array.empty[PlanOperator])
        Array(newOP)
      }

    } else {
      println(s"${indent}${op.toString}")
      val arrayBuf = new ArrayBuffer[PlanOperator]()
      op.childOps.foreach(
        printOPWithIndent(_, indent + INDENT, visited).foreach(arrayBuf.append(_)))
      arrayBuf.toArray
    }
  }

  def resetCopy(op: PlanOperator): Unit = {
    op.copyOP = null
    op.childOps.foreach(resetCopy)
  }

  def getParsedQueryGraph(dir: String, configName: String): QueryGraph = {
    val configInfo = Utils.parseConfigFile(configName)

    val qidToQuery = mutable.HashMap.empty[Int, PlanOperator]
    val qidToConstraints = mutable.HashMap.empty[Int, Double]
    val fullQidSet = mutable.HashSet.empty[Int]
    configInfo.foreach(info => {
      val queryName = info._1
      val qid = info._2
      val constraint = info._3
      val dfName = s"$dir/$queryName.df"
      val query = Parser.parseQuery(dfName, qid)

      fullQidSet.add(qid)
      qidToQuery.put(qid, query)
      qidToConstraints.put(qid, constraint)
    })

    val qidToFinalWork = mutable.HashMap.empty[Int, Double]
    val qidToUids = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    val uidToQid = mutable.HashMap.empty[Int, Int]
    val queries = Array.empty[PlanOperator]
    val queryDependency = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    val schedulingOrder = Array.empty[Int]
    val numBatches = Array.empty[Int]
    QueryGraph(qidToQuery, qidToConstraints, fullQidSet,
      queries, qidToUids, uidToQid, queryDependency, qidToFinalWork, schedulingOrder, numBatches)
  }

  def parseConfigFile(configName: String): Array[(String, Int, Double)] = {
    val lines = Source.fromFile(configName).getLines().map(_.trim).toArray
    lines.map(line => {
      val configInfo = line.split(",").map(_.trim)
      (configInfo(0), configInfo(1).toInt, configInfo(2).toDouble)
    })
  }

  def qidSetToString(qidSet: Array[Int]): String = {
    val strBuf = new StringBuffer()
    strBuf.append("[")
    for (idx <- qidSet.indices) {
      strBuf.append(qidSet(idx))
      if (idx != (qidSet.length - 1)) strBuf.append(", ")
    }
    strBuf.append("]")
    strBuf.toString
  }

  def aliasToString(aliasAttrs: mutable.HashMap[String, String]): String = {
    val strBuf = new StringBuffer()
    strBuf.append("[")
    aliasAttrs.iterator.zipWithIndex.foreach(pair => {
      val idx = pair._2
      val kv = pair._1
      strBuf.append(s"${kv._1} -> ${kv._2}")
      if (idx != (aliasAttrs.size - 1)) strBuf.append("; ")
    })
    strBuf.append("]")
    strBuf.toString
  }

  def predicatesToString(predicates: Array[Predicate]): String = {
    val strBuf = new StringBuffer()
    strBuf.append("[")
    predicates.zipWithIndex.foreach(pair => {
      val pred = pair._1
      val idx = pair._2
      strBuf.append(s"$pred")
      if (idx != (predicates.length - 1)) strBuf.append(", ")
    })
    strBuf.append("]")
    strBuf.toString
  }

  def groupByToString(groupByAttrs: mutable.HashSet[String]): String = {
    val strBuf = new StringBuffer()
    strBuf.append("[")
    groupByAttrs.iterator.zipWithIndex.foreach(pair => {
      val value = pair._1
      val idx = pair._2
      strBuf.append(s"${value}")
      if (idx != (groupByAttrs.size - 1)) strBuf.append(", ")
    })
    strBuf.append("]")
    strBuf.toString
  }

  def outputToString(outputAttrs: mutable.HashSet[String]): String = {
    val strBuf = new StringBuffer()
    strBuf.append("[")
    outputAttrs.iterator.zipWithIndex.foreach(pair => {
      val value = pair._1
      val idx = pair._2
      strBuf.append(s"${value}")
      if (idx != (outputAttrs.size - 1)) strBuf.append(", ")
    })
    strBuf.append("]")
    strBuf.toString
  }

  def getCurTimeStamp(): String = {
    val form = new SimpleDateFormat("MM-dd-HH:mm:ss")
    val c = Calendar.getInstance()
    form.format(c.getTime())
  }

  def latencyWithOrder(uidQueue: mutable.Queue[Int],
                       latencyArray: Array[Double],
                       uidDependency: mutable.HashMap[Int, mutable.HashSet[Int]])
  : mutable.HashMap[Int, Double] = {
    var curLatency = 0.0
    val latencyMap = mutable.HashMap.empty[Int, Double]

    val finished = mutable.HashSet.empty[Int]
    while (uidQueue.nonEmpty) {
      val curUid = uidQueue.dequeue()
      val latency = getLatency(curUid, finished, latencyArray, uidDependency)
      curLatency += latency
      latencyMap.put(curUid, curLatency)
    }

    latencyMap
  }

  private def getLatency(uid: Int, finished: mutable.HashSet[Int],
                         latencyArray: Array[Double],
                         uidDependency: mutable.HashMap[Int, mutable.HashSet[Int]]): Double = {
    if (finished.contains(uid)) {
      0.0
    } else {
      finished.add(uid)

      latencyArray(uid) +
        uidDependency(uid).map(childUid => {
          getLatency(childUid, finished, latencyArray, uidDependency)
        }).foldRight(0.0)((A, B) => {
          A + B
        })

    }
  }

}

class ProgressSimulator(maxBatchNum: Int,
                        numBatchArray: Array[Int],
                        queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]]) {

  private val numSubQ = numBatchArray.length
  private val batchIDArray = new Array[Int](numSubQ)
  for (idx <- batchIDArray.indices) batchIDArray(idx) = 0

  private val candidateSet = new mutable.HashSet[Int]
  private var step = 1

  def nextStep(): (Int, Array[mutable.HashSet[Int]]) = {

    if (step > maxBatchNum) return (step, Array.empty)

    val progress = step.toDouble/maxBatchNum.toDouble
    for (uid <- batchIDArray.indices) {
      if (numBatchArray(uid) > 0) {
        val threshold = (batchIDArray(uid) + 1).toDouble / numBatchArray(uid).toDouble
        if (progress >= threshold) {
          candidateSet.add(uid)
          batchIDArray(uid) += 1
        }
      }
    }

    val setArray = buildDependencySet(candidateSet)
    val oldStep = step
    step += 1

    candidateSet.clear()

    (oldStep, setArray)
  }

  private def buildDependencySet(candidateSet: mutable.HashSet[Int]):
  Array[mutable.HashSet[Int]] = {
    if (queryDependency == null) return Array(candidateSet)

    val buf = new ArrayBuffer[mutable.HashSet[Int]]
    while (candidateSet.nonEmpty) {
      val tmpHashSet = new mutable.HashSet[Int]

      candidateSet.foreach(uid => {
        queryDependency.get(uid) match {
          case None => tmpHashSet.add(uid)
          case Some(depSet) =>
            val leafNode =
              !depSet.exists(depUID => {
                candidateSet.contains(depUID)
              })
            if (leafNode) tmpHashSet.add(uid)
        }
      })

      tmpHashSet.foreach(uid => {
        candidateSet.remove(uid)
      })

      buf.append(tmpHashSet)
    }

    buf.toArray
  }

}

case class QueryGraph(qidToQuery: mutable.HashMap[Int, PlanOperator],
                      qidToConstraints: mutable.HashMap[Int, Double],
                      fullQidSet: mutable.HashSet[Int],
                      subQueries: Array[PlanOperator],
                      qidToUids: mutable.HashMap[Int, mutable.HashSet[Int]],
                      uidtoQid: mutable.HashMap[Int, Int],
                      queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                      qidToFinalWork: mutable.HashMap[Int, Double],
                      schedulingOrder: Array[Int],
                      numBatches: Array[Int])
