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
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import totem.middleground.sqp.Utils

object ScheduleUtils {

  def buildErrorBarString(triple: (Double, Double, Double)): String = {
    f"${triple._1}%.2f\t${triple._2}%.2f\t${triple._3}%.2f"
  }

  def findErrorBar(diff: Array[Double]): (Double, Double, Double) = {
    val minDiff = diff.min
    val maxDiff = diff.max
    val avgDiff = diff.sum/diff.length.toDouble

    (avgDiff, minDiff, maxDiff)
  }

  def computeDiff(latencyA: Array[Double], latencyB: Array[Double])
  : (Double, Double, Double) = {
    // Compute the min/man/avg difference
    val minA = latencyA.min
    val maxA = latencyA.max
    val avgA = latencyA.sum/latencyA.length.toDouble

    val minB = latencyB.min
    val maxB = latencyB.max
    val avgB = latencyB.sum/latencyB.length.toDouble

    (avgA - avgB, minA - minB, maxA - maxB)
  }

  def buildSchedulingOrder(qidList: List[Int],
                                   qidToUid: mutable.HashMap[Int, Int])
  : Array[Int] = {
    val schedulingOrder = new Array[Int](qidList.size)
    qidList.zipWithIndex.foreach(pair => {
      val qid = pair._1
      val idx = pair._2
      schedulingOrder(idx) = qidToUid(qid)
    })
    schedulingOrder
  }

  def fromStackToStandalone(stackLatency: Array[Double]): Array[Double] = {
    val standaloneLatency = new Array[Double](stackLatency.length)
    stackLatency.zipWithIndex.foreach(pair => {
      val latency = pair._1
      val idx = pair._2
      if (idx == 0) standaloneLatency(idx) = latency
      else standaloneLatency(idx) = latency - stackLatency(idx - 1)
    })
    standaloneLatency
  }

  def fromStandaloneToStack(standaloneLatency: Array[Double]): Array[Double] = {
    val stackLatency = new Array[Double](standaloneLatency.length)
    standaloneLatency.zipWithIndex.foreach(pair => {
      val latency = pair._1
      val idx = pair._2
      if (idx == 0) stackLatency(idx) = latency
      else stackLatency(idx) = latency + stackLatency(idx - 1)
    })
   stackLatency
  }

  def parseDependencyFile(fileName: String):
  mutable.HashMap[Int, mutable.HashSet[Int]] = {
    val lines = Source.fromFile(fileName).getLines().map(_.trim).toArray
    val dependency = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    lines.foreach(line => {
      if (line.nonEmpty) {
        val items = line.split("\\t")
        val uid = items(2).toInt
        val uidSet = mutable.HashSet.empty[Int]
        if (items.length >= 4) {
          items(3).split(",").foreach(depUid => {
            uidSet.add(depUid.trim.toInt)
          })
        }
        dependency.put(uid, uidSet)
      }
    })
    dependency
  }

  def parseSubQueryLatencyFile(fileName: String,
                                       rootQueries: mutable.HashSet[Int]):
  (Array[Double], mutable.HashMap[Int, Int]) = {
    val lines = Source.fromFile(fileName).getLines().map(_.trim).toArray
    val uidToLatencyMap = mutable.HashMap.empty[Int, Double]
    val qidToUid = mutable.HashMap.empty[Int, Int]
    lines.foreach(line => {
      if (line.nonEmpty) {
        val items = line.split("\\t")
        val uid = items(2).toInt
        uidToLatencyMap.put(uid, items(items.length - 1).toDouble)
        if (rootQueries.contains(uid)) {
          qidToUid.put(getQid(items(3)), uid)
        }
      }
    })
    val latencyArray = new Array[Double](uidToLatencyMap.size)
    uidToLatencyMap.foreach(pair => {
      val uid = pair._1
      val latency = pair._2
      latencyArray(uid) = latency
    })

    (latencyArray, qidToUid)
  }

  def computeQueryLatency(latencyArray: Array[Double],
                          dependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                          schedulingOrder: Array[Int])
  : Array[Double] = {
    val rootQueries = schedulingOrder
    val uidQueue = mutable.Queue.empty[Int]
    rootQueries.foreach(uidQueue.enqueue(_))

    val latencyMap = Utils.latencyWithOrder(uidQueue, latencyArray, dependency)
    val arrayBuffer = new ArrayBuffer[Double]
    schedulingOrder.foreach(uid => {
      arrayBuffer.append(latencyMap(uid))
    })
    arrayBuffer.toArray
  }

  def findRootQueries(dependency: mutable.HashMap[Int, mutable.HashSet[Int]])
  : mutable.HashSet[Int] = {
    val numSubQ = dependency.size
    val rootQueries = mutable.HashSet.empty[Int]
    for (uid <- 0 until numSubQ) rootQueries.add(uid)

    dependency.foreach(pair => {
      val depSet = pair._2
      depSet.foreach(rootQueries.remove)
    })

    rootQueries
  }

  def getQid(queryName: String): Int = {
    val idx = queryName.indexOf("_")
    queryName.substring(idx + 1).toInt
  }
}
