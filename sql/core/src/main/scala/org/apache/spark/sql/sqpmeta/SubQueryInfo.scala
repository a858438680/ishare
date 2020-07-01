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

import scala.collection.mutable.ArrayBuffer

case class SubQueryInfo (qidArray: Array[Int],
                         predInfoArray: Array[PredInfo],
                         aggQidCluster: Array[Long]) {

  def extractQidSet(): Long = {
    var qidSet: Long = 0
    qidArray.foreach(qid => {
      qidSet |= (1 << qid)
    })
    qidSet
  }

  def getPredQidArray(predInfo: PredInfo): Array[Int] = {
    val qidBuf = ArrayBuffer[Int]()
    val predStr = predInfo.toString
    predInfoArray.foreach(tmpInfo => {
      if (tmpInfo.toString.compareToIgnoreCase(predStr) == 0) {
        qidBuf.append(tmpInfo.qid)
      }
    })

    qidBuf.toArray
  }
}

case class PredInfo (qid: Int, left: String, op: String, right: String) {
  override def toString: String = {
    s"$left $op $right"
  }
}

object SubQueryInfo {

  private val andString = " && "
  private val orString = " || "

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

  private def extractFirstPredString(conditionStr: String): String = {
    val numSubPred =
      countCommonString(andString, conditionStr) + countCommonString(orString, conditionStr) + 1

    if (numSubPred == 1) return conditionStr

    val startIndex = numSubPred - 1
    val endIndex = endIndexOfFirstPred(conditionStr)
    conditionStr.substring(startIndex, endIndex)
  }

  def extractPredInfo(inputConditionStr: String): PredInfo = {
    val conditionStr = extractFirstPredString(inputConditionStr)

    val not = conditionStr.startsWith("NOT")

    val dotIdx = conditionStr.lastIndexOf(").")
    if (dotIdx == -1) return null

    val predStr = conditionStr.substring(dotIdx + 2, conditionStr.length - 1)
    val predStrArray = predStr.split("\\s+")
    if (predStrArray.length == 3) {
      val left = predStrArray(0)
      val right = predStrArray(2)
      val op =
        if (predStrArray(1).compareTo("=") == 0 && not) "!="
        else predStrArray(1)

      PredInfo(-1, left, op, right)
    } else if (predStrArray.length == 5) { // this is the case for date comparison
      val left = predStrArray(0)
      val right = predStrArray(4)
      val op =
        if (predStrArray(3).compareTo("=") == 0 && not) "!="
        else predStrArray(3)

      PredInfo(-1, left, op, right)
    } else {
      null
    }
  }

}
