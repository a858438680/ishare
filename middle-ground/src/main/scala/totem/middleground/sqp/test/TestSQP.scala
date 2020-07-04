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

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import totem.middleground.sqp.{Optimizer, Parser, PlanOperator, QueryGenerator}

import org.apache.spark.sql.sqpmeta.SubQueryInfo
import org.apache.spark.sql.sqpnetwork.MetaServer

object TestSQP {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: TestOptimizer [DF directory] [Config file] [Pred file]")
      System.exit(1)
    }

    val dfDir = args(0)
    val configFile = args(1)
    val predFile = args(2)

    Optimizer.initializeOptimizer(predFile)
    val optimizedQueries = optimizeMultiQuery(dfDir, configFile)

    val pair = QueryGenerator.generateQueryAndConfiguration(optimizedQueries)
    val queries = pair._1
    val queryConfig = pair._2
  }

  private def optimizeMultiQuery(dir: String, configName: String): Array[PlanOperator] = {
    val configInfo = parseConfigFile(configName)

    val queries = configInfo.map(info => {
      val queryName = info._1
      val qid = info._2
      val dfName = s"$dir/$queryName.df"
      Parser.parseQuery(dfName, qid)
    }).map(Optimizer.OptimizeOneQuery)

    Optimizer.OptimizeMultiQuery(queries)
  }

  private def parseConfigFile(configName: String): Array[(String, Int)] = {
    val lines = Source.fromFile(configName).getLines().map(_.trim).toArray
    lines.map(line => {
      val configInfo = line.split(",").map(_.trim)
      (configInfo(0), configInfo(1).toInt)
    })
  }
}

class ServerThread (numSubQ: Int, port: Int,
                    qnames: Array[String],
                    numBatchArray: Array[Int],
                    dependency: HashMap[Int, HashSet[Int]],
                    queryInfo: Array[SubQueryInfo]) extends Thread {

  private val MAX_BATCH_NUM = 100
  private val server = new MetaServer(numSubQ, port)
  private val batchIDArray = new Array[Int](numBatchArray.length)
  private var allTotalTime = 0.0
  private val totalTime = new Array[Double](numBatchArray.length)
  private val finalTime = new Array[Double](numBatchArray.length)
  private val initialStartupTime = 3000.0

  server.loadSharedQueryInfo(queryInfo)

  for (idx <- batchIDArray.indices) batchIDArray(idx) = 0

  override def run(): Unit = {
    server.startServer()

    val candidateSet = new mutable.HashSet[Int]
    for (step <- 1 until MAX_BATCH_NUM + 1) {
      val progress = step.toDouble/MAX_BATCH_NUM.toDouble

      for (uid <- batchIDArray.indices) {
        val threshold = (batchIDArray(uid) + 1).toDouble/numBatchArray(uid).toDouble
        if (progress >= threshold) {
          candidateSet.add(uid)
          batchIDArray(uid) += 1
        }
      }

      val setArray = buildDependencySet(candidateSet)

      // if (step == MAX_BATCH_NUM) {
      //   setArray.foreach(execSet => {
      //     execSet.foreach(uid => {
      //       server.startOneExecution(uid)
      //     })

      //     var maxExecTime = 0.0
      //     execSet.foreach(uid => {
      //       val msg = server.getStatMessage(uid)
      //       totalTime(uid) += msg.execTime
      //       maxExecTime = math.max(msg.execTime, maxExecTime)
      //       if (step == MAX_BATCH_NUM) {
      //         finalTime(uid) = msg.execTime
      //       }
      //     })
      //     allTotalTime += maxExecTime

      //   })
      // } else {

      setArray.foreach(execSet => {
        execSet.foreach(uid => {
          server.startOneExecution(uid)
          val msg = server.getStatMessage(uid)
          totalTime(uid) += msg.execTime
          allTotalTime += msg.execTime
          if (step == MAX_BATCH_NUM) {
            finalTime(uid) = msg.execTime
          }
        })
      })

      // }


      candidateSet.clear()
    }

    server.stopServer()
  }

  private def buildDependencySet(candidateSet: mutable.HashSet[Int]):
  Array[mutable.HashSet[Int]] = {
    if (dependency == null) return Array(candidateSet)

    val buf = new ArrayBuffer[mutable.HashSet[Int]]
    while (candidateSet.nonEmpty) {
      val tmpHashSet = new mutable.HashSet[Int]

      candidateSet.foreach(uid => {
        dependency.get(uid) match {
          case None => tmpHashSet.add(uid)
          case Some(depSet) =>
            val leafNode =
              !depSet.exists(depUID => {
                candidateSet.contains(depUID)
              })
            if (leafNode) tmpHashSet.add(uid)
        }
      })

      tmpHashSet.foreach(uid => {
        candidateSet.remove(uid)
      })

      buf.append(tmpHashSet)
    }

    buf.toArray
  }

  def reportStats(): Unit = {
    for (uid <- batchIDArray.indices) {
      printf("query %d, batchNum %d\n", uid, numBatchArray(uid))
      printf("total time\tfinal time\n")
      printf("%.2f\t%.2f\n", totalTime(uid), finalTime(uid))
    }
    printf("%.2f\n", allTotalTime - initialStartupTime)
  }

}
