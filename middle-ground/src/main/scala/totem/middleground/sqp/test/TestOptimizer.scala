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

import scala.io.Source

import totem.middleground.sqp.Optimizer
import totem.middleground.sqp.Parser
import totem.middleground.sqp.Utils

object TestOptimizer {

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
    testOptimizerForMultiQuery(args(0), args(1))
  }

  private def testOptimizerForSameQuery(fileName: String, qid: Int): Unit = {
    val subQuery = Parser.parseQuery(fileName, qid)
    Utils.printPlanGraph(
      Array(Optimizer.OptimizeOneQuery(subQuery)))
  }

  private def testOptimizerForMultiQuery(dir: String, configName: String): Unit = {
    val configInfo = parseConfigFile(configName)

    val queries = configInfo.map(info => {
      val queryName = info._1
      val qid = info._2
      val dfName = s"$dir/$queryName.df"
      Parser.parseQuery(dfName, qid)
    }).map(Optimizer.OptimizeOneQuery)

    Utils.printPlanGraph(Optimizer.OptimizeMultiQuery(queries))
  }

  private def parseConfigFile(configName: String): Array[(String, Int)] = {
    val lines = Source.fromFile(configName).getLines().map(_.trim).toArray
    lines.map(line => {
      val configInfo = line.split(",").map(_.trim)
      (configInfo(0), configInfo(1).toInt)
    })
  }

}
