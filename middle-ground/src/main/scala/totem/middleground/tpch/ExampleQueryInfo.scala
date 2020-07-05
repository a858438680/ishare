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

package totem.middleground.tpch

import scala.collection.mutable

import org.apache.spark.sql.sqpmeta.{PredInfo, SubQueryInfo}

object ExampleQueryInfo {
  val subQueryInfoArray: Array[SubQueryInfo] = new Array[SubQueryInfo](3)

  // For SubQuery 2
  var qidArray = Array(0, 1)
  var predInfoMap = mutable.HashMap.empty[Int, mutable.HashSet[PredInfo]]
  predInfoMap.put(0, mutable.HashSet(
    PredInfo("l_shipdate", "<=", "1994-01-01"),
    PredInfo("p_size", "=", "1")))
  predInfoMap.put(1, mutable.HashSet(
    PredInfo("l_shipdate", "<=", "1994-01-01")
  ))
  var aggQidCluster = Array(1L, 2L)
  // var aggQidCluster = Array(3L)

  val subQuery2 = new SubQueryInfo(qidArray, predInfoMap, aggQidCluster)

  // For SubQuery 0
  qidArray = Array(0)
  predInfoMap = mutable.HashMap.empty[Int, mutable.HashSet[PredInfo]]
  aggQidCluster = Array(1L)

  val subQuery0 = new SubQueryInfo(qidArray, predInfoMap, aggQidCluster)

  // For SubQuery 1
  qidArray = Array(1)
  predInfoMap = mutable.HashMap.empty[Int, mutable.HashSet[PredInfo]]
  aggQidCluster = Array(2L)

  val subQuery1 = new SubQueryInfo(qidArray, predInfoMap, aggQidCluster)

  subQueryInfoArray(0) = subQuery0
  subQueryInfoArray(1) = subQuery1
  subQueryInfoArray(2) = subQuery2

  def getShareSubQueryInfo: Array[SubQueryInfo] = {
    subQueryInfoArray
  }

  def getSepSubQueryInfo: Array[SubQueryInfo] = {
    Array(getOneSepSubQueryInfo, getOneSepSubQueryInfo)
  }

  private def getOneSepSubQueryInfo: SubQueryInfo = {
    val qidArray = Array(0)
    val predInfoMap = mutable.HashMap.empty[Int, mutable.HashSet[PredInfo]]
    val aggQidCluster = Array(1L)

    new SubQueryInfo(qidArray, predInfoMap, aggQidCluster)
  }

}
