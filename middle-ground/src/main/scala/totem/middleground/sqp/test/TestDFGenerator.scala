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

import totem.middleground.sqp._

object TestDFGenerator {
  def main(args: Array[String]): Unit = {

    // if (args.length < 2) {
    //   System.err.println("Usage: TestOptimizer [Path to DF file] [Qid]")
    //   System.exit(1)
    // }

    if (args.length < 3) {
      System.err.println("Usage: TestOptimizer [DF directory] [Config file] [Pred file]")
      System.exit(1)
    }

    Optimizer.initializeOptimizer(args(2))
    testDFGenerator(args(0), args(1))
  }

  private def testDFGenerator(dir: String, configName: String): Unit = {
    val queryGraph = Utils.getParsedQueryGraph(dir, configName)
    // val newQueryGraph = Optimizer.OptimizeUsingSQP(queryGraph, true)
    val newQueryGraph = Optimizer.OptimizeUsingHolistic(queryGraph)

    Utils.printQueryGraph(newQueryGraph)
    QueryGenerator.printSubQueryProgram(newQueryGraph)
  }

}
