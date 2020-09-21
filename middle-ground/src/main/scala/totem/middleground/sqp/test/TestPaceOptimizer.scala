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

import totem.middleground.sqp.Optimizer
import totem.middleground.sqp.Utils

object TestPaceOptimizer {
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: TestPaceOptimizer [DF directory]" +
        "[Config file] [Pred file] [Enable Unshare]")
      System.exit(1)
    }

    Optimizer.initializeOptimizer(args(2))
    testOptimizerForPaceConfiguration(args(0), args(1), args(3))
  }

  private def testOptimizerForPaceConfiguration(dir: String,
                                                configName: String,
                                                enableUnshare: String): Unit = {
    val unshare =
      if (enableUnshare.toLowerCase.compareTo("true") == 0) true
      else false

    val queryGraph = Utils.getParsedQueryGraph(dir, configName)
    val newQueryGraph = Optimizer.OptimizeUsingBatchMQO(queryGraph)
    // val newQueryGraph = Optimizer.OptimizedWithoutSharing(queryGraph)
    // val newQueryGraph = Optimizer.OptimizeUsingSQP(queryGraph, unshare)
    // val newQueryGraph = Optimizer.OptimizeWithInQP(queryGraph)

    Utils.printQueryGraph(newQueryGraph)
    Utils.printPaceConfig(newQueryGraph)
    Utils.printClusterSet(Optimizer.getMutliQueryCluster(newQueryGraph))
  }
}
