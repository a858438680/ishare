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

import totem.middleground.sqp.{Optimizer, PlanOperator, Utils}

object FindSPJ {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: TestOptimizer [DF directory] [Config file] [Pred file]")
      System.exit(1)
    }

    Optimizer.initializeOptimizer(args(2))
    val dir = args(0)
    val configName = args(1)

    val spjMap = mutable.HashMap.empty[Int, mutable.HashSet[PlanOperator]]
    val parentMap = mutable.HashMap.empty[Int, mutable.HashSet[PlanOperator]]
    val queryGraph = Utils.getParsedQueryGraph(dir, configName)
    queryGraph.qidToQuery.foreach(pair => {
      val qid = pair._1
      val query = pair._2

      Utils.findSPJSubquery(query, qid, spjMap, parentMap)
      val spjSet = spjMap.getOrElse(qid, mutable.HashSet.empty[PlanOperator])
      print(s"Query $qid: ${spjSet.size}\n")
    })

  }

}


