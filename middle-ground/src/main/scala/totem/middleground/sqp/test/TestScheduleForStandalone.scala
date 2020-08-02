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

import java.io.{FileWriter, PrintWriter}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

import totem.middleground.sqp.Utils

object TestScheduleForStandalone {

  def main(args: Array[String]): Unit = {

    import ScheduleUtils._

    if (args.length < 5) {
      System.err.println(
        "Usage: TestScheduleForStandalone [Batchtime] [Goal Configuration File] " +
          "[SubQueryLatency File] [Dependency File] [statDir]")
      System.exit(1)
    }

    val testCase = 100
    val statDir = args(4)

    val constraintMap = getConstraints(args(0), args(1))

    val dependency = parseDependencyFile(args(3))
    val rootQueries = findRootQueries(dependency)
    val (latencyArray, qidToUid) = parseSubQueryLatencyFile(args(2), rootQueries)

    val qidSet = mutable.HashSet.empty[Int]
    qidToUid.keySet.foreach(qidSet.add)
    val qidList = qidSet.toList

    val avgMiss = new Array[Double](testCase)
    val maxMiss = new Array[Double](testCase)

    for (caseId <- 0 until testCase) {
      val randomList = Random.shuffle(qidList)

      val schedulingOrder = buildSchedulingOrder(randomList, qidToUid)
      val stackLatency = computeQueryLatency(latencyArray, dependency, schedulingOrder)
      val standaloneLatency = fromStackToStandalone(stackLatency)

      val constraintArray = buildConstraintArray(constraintMap, randomList)

      val pair = getMissedLatency(constraintArray, standaloneLatency)
      avgMiss(caseId) = pair._1
      maxMiss(caseId) = pair._2
    }

    val avgStr = buildErrorBarString(findErrorBar(avgMiss))
    val maxStr = buildErrorBarString(findErrorBar(maxMiss))

    val timestamp = Utils.getCurTimeStamp()
    val outputStr = s"$timestamp\t$avgStr\t$maxStr"
    val standaloneFile = statDir + "/schedule_standalone.stat"
    val standaloneWriter = new PrintWriter(new FileWriter(standaloneFile, true))
    standaloneWriter.println(outputStr)
    standaloneWriter.close()

    println(outputStr)

    Thread.sleep(1000)
  }

  private def buildConstraintArray(constraintMap: mutable.HashMap[Int, Double],
                                   qidList: List[Int]): Array[Double] = {
    val constraintBuf = new ArrayBuffer[Double]()
    qidList.foreach(qid => {
      constraintBuf.append(constraintMap(qid))
    })
    constraintBuf.toArray
  }

  private def getMissedLatency(constraint: Array[Double], realLatency: Array[Double])
  : (Double, Double) = {
    val missedLatency = new Array[Double](constraint.length)
    constraint.zip(realLatency).zipWithIndex.foreach(pair => {
      val idx = pair._2
      val constraintLatency = pair._1._1
      val realLatency = pair._1._2
      missedLatency(idx) =
        if (realLatency - constraintLatency < 0) 0.0
        else realLatency - constraintLatency
    })
    val maxMiss = missedLatency.max
    val avgMiss = missedLatency.sum/missedLatency.length.toDouble
    (avgMiss, maxMiss)
  }

  private def getConstraints(batchFileName: String,
                             goalFileName: String): mutable.HashMap[Int, Double] = {
    val batchTimeMap = parseBatchTimeFile(batchFileName)
    val goalMap = parseGoalFile(goalFileName)
    val constraintMap = mutable.HashMap.empty[Int, Double]
    batchTimeMap.foreach(pair => {
      val qid = pair._1
      val batchTime = pair._2
      val goal = goalMap(qid)
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
}
