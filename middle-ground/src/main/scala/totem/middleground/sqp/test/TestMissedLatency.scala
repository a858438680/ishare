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

object TestMissedLatency {

  def main(args: Array[String]): Unit = {

    import LatencyUtils._
    import totem.middleground.sqp.Utils

    if (args.length < 4) {
      System.err.println(
        "Usage: TestMissedLatency [Batchtime] [Goal Configuration File] " +
          "[Standalone Latency] [statDir]")
      System.exit(1)
    }

    val statDir = args(3)
    val constraintMap = getConstraints(args(0), args(1))
    val (approachMap, latencyMap) = parseQueryStandaloneLatencyFile(args(2))

    val missAbsFile = statDir + "/missAbs.stat"
    val missPerFile = statDir + "/missPer.stat"
    val timestamp = Utils.getCurTimeStamp()

    val absWriter = new PrintWriter(new FileWriter(missAbsFile, true))
    val perWriter = new PrintWriter(new FileWriter(missPerFile, true))

    latencyMap.foreach(pair => {
      val qid = pair._1
      val latency = pair._2
      val approach = approachMap(qid)
      val constraint = constraintMap(qid)

      val missAbs = math.max(0, latency - constraint)
      val missPer = math.max(0, (latency - constraint)/constraint)

      val absOutputStr = s"$timestamp\t$approach\t$qid\t$missAbs"
      val perOutputStr = s"$timestamp\t$approach\t$qid\t$missPer"

      absWriter.println(absOutputStr)
      perWriter.println(perOutputStr)
    })

    absWriter.close()
    perWriter.close()
  }

}
