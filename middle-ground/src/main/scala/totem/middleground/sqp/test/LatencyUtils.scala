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

package totem.middleground.sqp.test

import scala.collection.mutable
import scala.io.Source

object LatencyUtils {

  def getConstraints(batchFileName: String,
                     goalFileName: String): mutable.HashMap[Int, Double] = {
    val batchTimeMap = parseBatchTimeFile(batchFileName)
    val goalMap = parseGoalFile(goalFileName)
    val constraintMap = mutable.HashMap.empty[Int, Double]
    goalMap.foreach(pair => {
      val qid = pair._1
      val goal = pair._2
      val batchTime = batchTimeMap(qid)
      constraintMap.put(qid, batchTime * goal)
    })
    constraintMap
  }

  private def parseBatchTimeFile(fileName: String): mutable.HashMap[Int, Double] = {
    val lines = Source.fromFile(fileName).getLines().map(_.trim).toArray
    val batchTimeMap = mutable.HashMap.empty[Int, Double]
    lines.foreach(line => {
      val items = line.split(",")
      batchTimeMap.put(items(0).trim.toInt, items(1).trim.toDouble)
    })
    batchTimeMap
  }

  private def parseGoalFile(fileName: String): mutable.HashMap[Int, Double] = {
    val lines = Source.fromFile(fileName).getLines().map(_.trim).toArray
    val goalMap = mutable.HashMap.empty[Int, Double]
    lines.foreach(line => {
      val items = line.split(",")
      goalMap.put(items(1).trim.toInt, items(2).trim.toDouble)
    })
    goalMap
  }

  def parseQueryStandaloneLatencyFile(fileName: String):
  (mutable.HashMap[Int, String], mutable.HashMap[Int, Double], String) = {
    val lines = Source.fromFile(fileName).getLines().map(_.trim).toArray
    val qidToLatencyMap = mutable.HashMap.empty[Int, Double]
    val qidToApproachMap = mutable.HashMap.empty[Int, String]
    var thisApproach: String = ""
    lines.foreach(line => {
      if (line.nonEmpty) {
        val items = line.split("\\t")
        val approach = items(1).trim
        val qid = items(2).toInt
        val latency = items(3).toDouble
        qidToApproachMap.put(qid, approach)
        qidToLatencyMap.put(qid, latency)
        thisApproach = approach
      }
    })

    (qidToApproachMap, qidToLatencyMap, thisApproach)
  }
}
