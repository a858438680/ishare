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

import totem.middleground.sqp.Catalog
import totem.middleground.sqp.Optimizer
import totem.middleground.sqp.Utils

object TestOptimizerCost {
  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println("Usage: TestOptimizerCost [DF directory] " +
        "[Config file] [Pred file] [StatDir] [MaxBatchNum]")
      System.exit(1)
    }

    Catalog.setMaxBatchNum(args(4).toInt)
    Optimizer.initializeOptimizer(args(2))
    testOptimizerCost(args(0), args(1), args(3), args(4))
  }

  private def testOptimizerCost(dir: String, configName: String,
                                statDir: String, maxBatchNum: String): Unit = {

    // No sharing, Uniform
    var queryGraph = Utils.getParsedQueryGraph(dir, configName)
    val noShareTime = Optimizer.testOptimizerWithoutSharing(queryGraph)

    // No share, Nonuniform
    queryGraph = Utils.getParsedQueryGraph(dir, configName)
    val InQPTime = Optimizer.testOptimizerWithInQP(queryGraph)

    // Batch share, uniform
    queryGraph = Utils.getParsedQueryGraph(dir, configName)
    val batchShareTime = Optimizer.testOptimizerWithBatchMQO(queryGraph)

    // ishare
    queryGraph = Utils.getParsedQueryGraph(dir, configName)
    val iShareTime = Optimizer.testOptimizerWithSQP(queryGraph)

    val statFile = statDir + "/optimizercost.stat"
    val statWriter = new PrintWriter(new FileWriter(statFile, true))
    statWriter.println(s"$maxBatchNum\t$noShareTime\t$InQPTime\t$batchShareTime\t" +
      s"$iShareTime")
    statWriter.close()
  }
}
