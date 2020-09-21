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
import scala.io.Source

object TestMissedLatency {

  def main(args: Array[String]): Unit = {
    import ScheduleUtils._
    import LatencyUtils._
    import totem.middleground.sqp.Utils

    if (args.length < 6) {
      System.err.println(
        "Usage: TestMissedLatency [Batchtime] [Goal Configuration File] " +
          "[Standalone Latency] [SubQuery File] [Dependency Map] [statDir]")
      System.exit(1)
    }

    val subQueryFile = args(3)
    val statDir = args(5)
    val constraintMap = getConstraints(args(0), args(1))
    val (approachMap, latencyMap, thisApproach) =
      parseQueryStandaloneLatencyFile(args(2))

    val (batchNumArray, qidToUids, qidToLatency) = parseSubQueryBatchNumFile(subQueryFile)

    // val dependency = parseDependencyFile(args(4))
    // val rootUidQueries = findRootQueries(dependency)
    // val (latencyArray, qidToRootUid) = parseSubQueryLatencyFile(subQueryFile, rootUidQueries)
    // val qidList = qidToRootUid.keysIterator.toList
    // val schedulingOrder = buildSchedulingOrder(qidList, qidToRootUid)
    // val stackLatency = computeQueryLatency(latencyArray, dependency, schedulingOrder)
    // val standaloneLatency = fromStackToStandalone(stackLatency)

    val missAbsFile = statDir + "/missAbs.stat"
    val missPerFile = statDir + "/missPer.stat"
    val timestamp = Utils.getCurTimeStamp()

    val absWriter = new PrintWriter(new FileWriter(missAbsFile, true))
    val perWriter = new PrintWriter(new FileWriter(missPerFile, true))

    qidToUids.keysIterator.foreach(qid => {
      val latency = qidToLatency(qid)
      val approach = thisApproach
      val constraint = constraintMap(qid)

      val overhead = computeStartupOverhead(qid, qidToUids, batchNumArray)

      val missAbs = math.max(0, latency - overhead - constraint)
      val missPer = math.max(0, (latency - overhead - constraint)/constraint)

      val absOutputStr = s"$timestamp\t$approach\t$qid\t$missAbs"
      val perOutputStr = s"$timestamp\t$approach\t$qid\t$missPer"

      absWriter.println(absOutputStr)
      perWriter.println(perOutputStr)
    })

    absWriter.close()
    perWriter.close()
  }

  val startUpCost = 2500.0
  def computeStartupOverhead(qid: Int,
                             qidToUid: mutable.HashMap[Int, mutable.HashSet[Int]],
                             batchNumArray: Array[Int]): Double = {
    var overhead = 0.0
    qidToUid(qid).foreach(uid => {
      if (batchNumArray(uid) == 1) overhead += startUpCost
    })
    overhead
  }

  def parseSubQueryBatchNumFile(fileName: String):
  (Array[Int],
    mutable.HashMap[Int, mutable.HashSet[Int]],
    mutable.HashMap[Int, Double]) = {

    val lines = Source.fromFile(fileName).getLines().map(_.trim).toArray
    val uidToBatchNumMap = mutable.HashMap.empty[Int, Int]
    val qidToUids = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    val qidToStandaloneLatency = mutable.HashMap.empty[Int, Double]
    lines.foreach(line => {
      if (line.nonEmpty) {
        val items = line.split("\\t")
        val uid = items(2).toInt
        val latency = items(6).toDouble
        uidToBatchNumMap.put(uid, items(4).toInt)
        getQids(items(3)).foreach(qid => {
          val uidSet = qidToUids.getOrElseUpdate(qid, mutable.HashSet.empty[Int])
          uidSet.add(uid)

          val curLatency = qidToStandaloneLatency.getOrElse(qid, 0.0)
          qidToStandaloneLatency.put(qid, curLatency + latency)
        })
      }
    })
    val batchNumArray = new Array[Int](uidToBatchNumMap.size)
    uidToBatchNumMap.foreach(pair => {
      val uid = pair._1
      val batchNum = pair._2
      batchNumArray(uid) = batchNum
    })

    (batchNumArray, qidToUids, qidToStandaloneLatency)
  }

  def getQids(queryName: String): mutable.HashSet[Int] = {
    val idx = queryName.indexOf("_")
    val qidString = queryName.substring(idx + 1)

    val qidSet = mutable.HashSet.empty[Int]
    qidString.split("_").foreach(qidStr => {
      qidSet.add(qidStr.toInt)
    })

    qidSet
  }

}
