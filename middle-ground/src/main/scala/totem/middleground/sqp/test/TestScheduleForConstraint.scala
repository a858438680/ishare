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
import scala.util.Random

import totem.middleground.sqp.Utils

object TestScheduleForConstraint {

  def main(args: Array[String]): Unit = {

    import ScheduleUtils._
    import LatencyUtils._

    if (args.length < 6) {
      System.err.println(
        "Usage: TestScheduleForStandalone [Batchtime] [Goal Configuration File] " +
          "[SubQueryLatency File] [Dependency File] [Standalone or not] [statDir]")
      System.exit(1)
    }

    val testCase = 100
    val standalone =
      if (args(4).toLowerCase.compareTo("true") == 0) true
      else false
    val statDir = args(5)

    val constraintMap = getConstraints(args(0), args(1))

    val dependency = parseDependencyFile(args(3))
    val rootQueries = findRootQueries(dependency)
    val (latencyArray, qidToUid) = parseSubQueryLatencyFile(args(2), rootQueries)

    val qidSet = mutable.HashSet.empty[Int]
    qidToUid.keySet.foreach(qidSet.add)
    val qidList = qidSet.toList

    val avgMiss = new Array[Double](testCase)
    val minMiss = new Array[Double](testCase)
    val maxMiss = new Array[Double](testCase)

    val avgPerMiss = new Array[Double](testCase)
    val minPerMiss = new Array[Double](testCase)
    val maxPerMiss = new Array[Double](testCase)

    for (caseId <- 0 until testCase) {
      val randomList = Random.shuffle(qidList)

      val schedulingOrder = buildSchedulingOrder(randomList, qidToUid)
      val stackLatency = computeQueryLatency(latencyArray, dependency, schedulingOrder)
      val standaloneLatency = fromStackToStandalone(stackLatency)

      val constraintArray = buildConstraintArray(constraintMap, randomList)
      val stackConstraint = fromStandaloneToStack(constraintArray)

      val tuple =
        if (standalone) getMissedLatency(constraintArray, standaloneLatency)
        else getMissedLatency(stackConstraint, stackLatency)
      avgMiss(caseId) = tuple._1
      minMiss(caseId) = tuple._2
      maxMiss(caseId) = tuple._3

      avgPerMiss(caseId) = tuple._4
      minPerMiss(caseId) = tuple._5
      maxPerMiss(caseId) = tuple._6
    }

    val avgStr = buildErrorBarString(findErrorBar(avgMiss))
    val minStr = buildErrorBarString(findErrorBar(minMiss))
    val maxStr = buildErrorBarString(findErrorBar(maxMiss))

    val avgPerStr = buildErrorBarString(findErrorBar(avgPerMiss))
    val minPerStr = buildErrorBarString(findErrorBar(minPerMiss))
    val maxPerStr = buildErrorBarString(findErrorBar(maxPerMiss))

    val timestamp = Utils.getCurTimeStamp()
    val outputStr = s"$timestamp\t$avgStr\t$minStr\t$maxStr"
    val perOutputStr = s"$timestamp\t$avgPerStr\t$minPerStr\t$maxPerStr"
    val outputFile =
      if (standalone) statDir + "/schedule_constraint_standalone.stat"
      else statDir + "/schedule_constraint_stack.stat"
    val writer = new PrintWriter(new FileWriter(outputFile, true))
    writer.println(outputStr)
    writer.println(perOutputStr)
    writer.close()

    println(outputStr)
    println(perOutputStr)

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
  : (Double, Double, Double, Double, Double, Double) = {
    val missedLatency = new Array[Double](constraint.length)
    val missedPercentage = new Array[Double](constraint.length)

    constraint.zip(realLatency).zipWithIndex.foreach(pair => {
      val idx = pair._2
      val constraintLatency = pair._1._1
      val realLatency = pair._1._2
      missedLatency(idx) =
        if (realLatency - constraintLatency < 0) 0.0
        else realLatency - constraintLatency

      missedPercentage(idx) =
        if (realLatency < constraintLatency) 0.0
        else ((realLatency - constraintLatency)/constraintLatency) * 100.0
    })
    val avgMiss = missedLatency.sum/missedLatency.length.toDouble
    val maxMiss = missedLatency.max
    val minMiss = missedLatency.min

    val avgPerMiss = missedPercentage.sum/missedPercentage.length.toDouble
    val maxPerMiss = missedPercentage.max
    val minPerMiss = missedPercentage.min
    (avgMiss, minMiss, maxMiss, avgPerMiss, minPerMiss, maxPerMiss)
  }
}
