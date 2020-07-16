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

package org.apache.spark.sql.sqpmeta

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class SubQueryInfo (qidArray: Array[Int],
                         predInfoMap: mutable.HashMap[Int, mutable.HashSet[PredInfo]],
                         aggQidCluster: Array[Long]) {

  def extractQidSet(): Long = {
    var qidSet: Long = 0
    qidArray.foreach(qid => {
      qidSet |= (1L << qid)
    })
    qidSet
  }

  def getPredQidArray(predInfoSet: mutable.HashSet[PredInfo]): Array[Int] = {
    val qidBuf = ArrayBuffer[Int]()

    predInfoMap.foreach(pair => {
      val qid = pair._1
      val candSet = pair._2
      if (sameSet(candSet, predInfoSet)) {
        qidBuf.append(qid)
      }
    })

    qidBuf.toArray
  }

  private def sameSet(candidateSet: mutable.HashSet[PredInfo],
                      predInfoSet: mutable.HashSet[PredInfo]): Boolean = {
    var prefix = ""
    var isValidSet = true
    predInfoSet.foreach(pred => {
      val idx = pred.left.indexOf("_")
      if (idx == -1) isValidSet = false
      else prefix = pred.left.substring(0, idx + 1)
    })

    if (!isValidSet) return false // Not valid input set

    val newCandSet = mutable.HashSet.empty[PredInfo]
    candidateSet.foreach(pred => {
      if (pred.left.startsWith(prefix)) newCandSet.add(pred)
    })

    if (newCandSet.diff(predInfoSet).isEmpty && predInfoSet.diff(newCandSet).isEmpty) true
    else false
  }

}

case class PredInfo (left: String, op: String, right: String) {
  override def toString: String = {
    s"$left $op $right"
  }
}

object SubQueryInfo {

  private val andString = " && "
  private val orString = " || "
  private val dotStr = ")."

  private def countCommonString(commonStr: String, conditionStr: String): Int = {
    var count = 0
    var lastIndex = 0
    while (lastIndex != -1) {
      lastIndex = conditionStr.indexOf(commonStr, lastIndex)
      if (lastIndex != -1) {
        count += 1
        lastIndex += commonStr.length
      }
    }
    count
  }

  private def endIndexOfFirstPred(conditionStr: String): Int = {
    val andIdx = conditionStr.indexOf(andString, 0)
    val orIdx = conditionStr.indexOf(orString, 0)
    if (orIdx == -1 && andIdx == -1) {
      -1
    } else if (orIdx == -1) {
      andIdx
    } else if (andIdx == -1) {
      orIdx
    } else {
      math.min(orIdx, andIdx)
    }
  }

  private def extractPredStrs(conditionStr: String): Array[String] = {
    val numSubPred =
      countCommonString(andString, conditionStr) + 1 // Only consider conjunctive predicates

    val conditionStrArray = new Array[String](numSubPred)

    var tmpStr = conditionStr.trim
    var remainPred = numSubPred
    while (remainPred > 1) {

      tmpStr = tmpStr.substring(1, tmpStr.length - 1) // strip a parenthesis
      val andIdx = tmpStr.lastIndexOf(andString) // find the last andString

      // find the tail predicate and generate new string
      conditionStrArray(remainPred - 1) = tmpStr.substring(andIdx + andString.length).trim
      tmpStr = tmpStr.substring(0, andIdx).trim

      remainPred -= 1
    }

    conditionStrArray(remainPred - 1) = tmpStr
    conditionStrArray
  }

  def extractPredInfo(inputConditionStr: String): mutable.HashSet[PredInfo] = {
    val predSet = mutable.HashSet.empty[PredInfo]
    val conditionStrArray = extractPredStrs(inputConditionStr)
    conditionStrArray.map(parseOnePredInfo).filter(_ != null).foreach(predSet.add)

    predSet
  }

  private def parseOnePredInfo(conditionStr: String): PredInfo = {

    if (conditionStr.contains(" IN (")) return null // Do not evaluate isin predicate

    val not = conditionStr.startsWith("NOT")
    val mode =
      if (conditionStr.startsWith("Contains")) 0
      else if (conditionStr.startsWith("StartsWith")) 1
      else if (conditionStr.startsWith("EndsWith")) 2
      else -1

    val numDotStr = countCommonString(dotStr, conditionStr)

    if (numDotStr == 1) {
      val dotIdx = conditionStr.lastIndexOf(dotStr)
      if (dotIdx == -1) return null

      val predStr = conditionStr.substring(dotIdx + 2, conditionStr.length - 1)
      if (mode == -1) {
        val predStrArray = predStr.split("\\s+")
        if (predStr.contains("as string")) { // this is the case for date comparison
          if (predStrArray.length == 5) {
            val left = predStrArray(0)
            val right = predStrArray(4)
            val op =
              if (predStrArray(3).compareTo("=") == 0 && not) "!="
              else predStrArray(3)

            PredInfo(left, op, right)
          } else {
            null
          }
        } else if (predStrArray.length >= 3) {
          val left = predStrArray(0)
          val op =
            if (predStrArray(1).compareTo("=") == 0 && not) "!="
            else predStrArray(1)

          val rightBuf = new StringBuffer()
          for (idx <- 2 until predStrArray.length) {
            rightBuf.append(predStrArray(idx))
            if (idx < predStrArray.length - 1) rightBuf.append(" ")
          }
          val right = rightBuf.toString

          PredInfo(left, op, right)
        } else {
          null
        }
      } else { // this is the case for like

        val predStrArray = predStr.split(", ")
        val left = predStrArray(0)
        val right = predStrArray(1)
        val op =
          if (mode == 0) "Contains"
          else if (mode == 1) "StartsWith"
          else "EndsWith"

        PredInfo(left, op, right)
      }
    } else if (numDotStr == 2) { // refer two attrs

      val firstDotIdx = conditionStr.indexOf(dotStr)
      val lastDotIdx = conditionStr.lastIndexOf(dotStr)

      val leftEnd = conditionStr.indexOf(" ", firstDotIdx)
      val left = conditionStr.substring(firstDotIdx + 2, leftEnd)

      val opEnd = conditionStr.indexOf(" ", leftEnd + 1)
      val op = conditionStr.substring(leftEnd + 1, opEnd).trim

      val rightEnd = conditionStr.length - 1
      val right = conditionStr.substring(lastDotIdx + 2, rightEnd)

      PredInfo(left, op, right)
    } else {
      null
    }

  }
}
