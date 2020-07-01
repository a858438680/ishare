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

object Utils {

  private val INDENT = "\t"
  private val NULLSTR = ""

  def printPlanGraph(rootOPArray: Array[PlanOperator]): Unit = {

    val queue = new mutable.Queue[PlanOperator]
    rootOPArray.foreach(queue.enqueue(_))
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

}
