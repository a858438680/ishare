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
import scala.util.Random

import totem.middleground.sqp.Utils

object TestScheduleForStack {
  def main(args: Array[String]): Unit = {

    import ScheduleUtils._

    if (args.length < 5) {
      System.err.println(
        "Usage: TestScheduleForStack [SubQueryLatency FileA] [Dependency FileA] " +
        "[SubQueryLatency FileB] [Dependency FileB] [statDir]")
      System.exit(1)
    }

    val testCase = 100

    val dependencyA = parseDependencyFile(args(1))
    val rootQueriesA = findRootQueries(dependencyA)
    val (latencyArrayA, qidToUidA) = parseSubQueryLatencyFile(args(0), rootQueriesA)

    val dependencyB = parseDependencyFile(args(3))
    val rootQueriesB = findRootQueries(dependencyB)
    val (latencyArrayB, qidToUidB) = parseSubQueryLatencyFile(args(2), rootQueriesB)

    val statDir = args(4)

    val qidSet = mutable.HashSet.empty[Int]
    qidToUidA.keySet.foreach(qidSet.add)
    val qidList = qidSet.toList

    val minDiff = new Array[Double](testCase)
    val maxDiff = new Array[Double](testCase)
    val avgDiff = new Array[Double](testCase)

    for (caseId <- 0 until testCase) {
      val randomList = Random.shuffle(qidList)

      val schedulingOrderA = buildSchedulingOrder(randomList, qidToUidA)
      val stackLatencyA = computeQueryLatency(latencyArrayA, dependencyA, schedulingOrderA)

      val schedulingOrderB = buildSchedulingOrder(randomList, qidToUidB)
      val stackLatencyB = computeQueryLatency(latencyArrayB, dependencyB, schedulingOrderB)

      val triple = computeDiff(stackLatencyA, stackLatencyB)
      avgDiff(caseId) = triple._1
      minDiff(caseId) = triple._2
      maxDiff(caseId) = triple._3
    }

    val avgStr = buildErrorBarString(findErrorBar(avgDiff))
    val minStr = buildErrorBarString(findErrorBar(minDiff))
    val maxStr = buildErrorBarString(findErrorBar(maxDiff))

    val timestamp = Utils.getCurTimeStamp()
    val outputStr = s"$timestamp\t$avgStr\t$minStr\t$maxStr"
    val stackFile = statDir + "/schedule_stack.stat"
    val stackWriter = new PrintWriter(new FileWriter(stackFile, true))
    stackWriter.println(outputStr)
    stackWriter.close()

    println(outputStr)

    Thread.sleep(1000)
  }
}
