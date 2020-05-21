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

  def getPredQid(predInfo: PredInfo): Int = {
    val predStr = predInfo.toString
    predInfoArray.foreach(tmpInfo => {
      if (tmpInfo.toString.compareToIgnoreCase(predStr) == 0) {
        return tmpInfo.qid
      }
    })

    -1
  }
}

case class PredInfo (qid: Int, left: String, op: String, right: String) {
  override def toString: String = {
    s"$left $op $right"
  }
}

object SubQueryInfo {

  def extractPredInfo(conditionStr: String): PredInfo = {
    val not = conditionStr.startsWith("NOT")

    val dotIdx = conditionStr.lastIndexOf(").")
    if (dotIdx == -1) return null

    val predStr = conditionStr.substring(dotIdx + 2, conditionStr.length - 1)
    val predStrArray = predStr.split("\\s+")
    if (predStrArray.length == 3) {
      val left = predStrArray(0)
      val right = predStrArray(1)
      val op =
        if (predStrArray(1).compareTo("=") == 0 && not) "!="
        else predStrArray(1)

      PredInfo(-1, left, op, right)
    } else if (predStrArray.length == 5) {
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
