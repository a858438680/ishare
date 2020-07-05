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
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

import totem.middleground.sqp._
import totem.middleground.sqp.tpchquery.TPCHQuery
import totem.middleground.tpch._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sqpmeta.SubQueryInfo
import org.apache.spark.sql.sqpnetwork.MetaServer

object TestSQP {

  def main(args: Array[String]): Unit = {

    if (args.length < 13) {
      System.err.println("Usage: TestSQP " +
        "[Bootstrap-Servers]" +
        "[Number-shuffle-partition]" +
        "[Statistics dir] " +
        "[SF]" +
        "[HDFS root]" +
        "[Num of input partitions]" +
        "[Path to kafka topics command] " +
        "[Zookeeper address]" +
        "[Path to hdfs command]" +
        "[Execution mode: 0 - NoShare, 1 - BatchShare, 2 - SQPShare]" +
        "[DF directory]" +
        "[Config file]" +
        "[Pred file]")
      System.exit(1)
    }

    val bootstrap = args(0)
    val shuffleNum = args(1)
    val statDIR = args(2)
    val SF = args(3).toDouble
    val hdfsRoot = args(4)
    val inputPartitions = args(5).toInt
    val kafkaCommand = args(6)
    val zookeeper = args(7)
    val hdfsCommand = args(8)
    val port = 8887
    val executionMode =
      if (args(9).compareTo("0") == 0) ExecutionMode.NoShare
      else if (args(9).compareTo("1") == 0) ExecutionMode.BatchShare
      else ExecutionMode.SQPShare

    val dfDir = args(10)
    val configFile = args(11)
    val predFile = args(12)

    Optimizer.initializeOptimizer(predFile)
    val optimizedQueries = optimizeMultiQuery(dfDir, configFile, executionMode)

    val pair = QueryGenerator.generateQueryAndConfiguration(optimizedQueries)
    val subQueries = pair._1
    val queryConfig = pair._2

    val testSQP = new TestSQP(bootstrap, shuffleNum, statDIR, SF, hdfsRoot, inputPartitions,
      kafkaCommand, zookeeper, hdfsCommand, port, executionMode, subQueries, queryConfig)

    testSQP.startQueries()
  }

  private def optimizeMultiQuery(dir: String,
                                 configName: String,
                                 executionMode: ExecutionMode.Value): Array[PlanOperator] = {
    val configInfo = Utils.parseConfigFile(configName)

    val queries = configInfo.map(info => {
      val queryName = info._1
      val qid = info._2
      val dfName = s"$dir/$queryName.df"
      Parser.parseQuery(dfName, qid)
    })

    if (executionMode == ExecutionMode.BatchShare) {
      Optimizer.OptimizeUsingBatchMQO(queries)
    } else if (executionMode == ExecutionMode.SQPShare) {
      Optimizer.OptimizeUsingSQP(queries)
    } else { // No Sharing
      queries
    }

  }
}

class TestSQP(bootstrap: String, shuffleNum: String, statDIR: String, SF: Double,
              hdfsRoot: String, inputPartitions: Int, kafkaCommand: String,
              zookeeper: String, hdfsCommand: String, port: Int,
              executionMode: ExecutionMode.Value,
              subQueries: Array[TPCHQuery], queryConfig: QueryConfig) {

  if (executionMode == ExecutionMode.NoShare) {
    println("No Shared Execution")
  } else if (executionMode == ExecutionMode.BatchShare) {
    println("Shared Execution using Batch Optimizer")
  } else {
    println("Shared Execution of SQP")
  }

  DataUtils.bootstrap = bootstrap

  private val sparkConf = new SparkConf()
    .set(SQLConf.SHUFFLE_PARTITIONS.key, shuffleNum)
    .set(SQLConf.SLOTHDB_STAT_DIR.key, statDIR)
    .set(SQLConf.SQP_PORT.key, port.toString)
    .set(SQLConf.SQP_SOURCE_PARTITION.key, inputPartitions.toString)
    .set("spark.scheduler.mode", "FAIR")

  private val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .appName("Shared Query Execution")
    .getOrCreate()

  private val largeDataset = false
  private val numSubQ = subQueries.length
  private val numBatches = queryConfig.numBatches
  private val queryNames = queryConfig.queryNames
  private val constraints = queryConfig.constraints
  private val subQueryInfos = queryConfig.subQueryInfo
  private val queryDependency = queryConfig.queryDependency
  private val shareTopics = queryConfig.shareTopics

  private val tpchSchemas = new Array[TPCHSchema](numSubQ)
  for (idx <- 0 until numSubQ) {
    tpchSchemas(idx) = new TPCHSchema
    tpchSchemas(idx).setQueryMetaData(numBatches(idx), SF,
      hdfsRoot, inputPartitions, largeDataset)
  }

  subQueries.zipWithIndex.foreach(pair => {
    val subQuery = pair._1
    val idx = pair._2
    subQuery.initialize(queryNames(idx), idx.toString, numBatches(idx).toString,
      constraints(idx), SF, tpchSchemas(idx), sparkSession)
  })

  private val serverThread =
    new ServerThread(numSubQ, port, queryNames, numBatches, queryDependency, subQueryInfos)

  private def createSharedTopics(): Unit = {
    shareTopics.foreach(shareTopic => {
      val commandStr = s"$kafkaCommand -zookeeper $zookeeper --create " +
        s"--topic $shareTopic --partitions ${inputPartitions} --replication-factor 1"
      val ret = commandStr.!
      if (ret != 0) {
        println(s"Create topic $shareTopic failed")
      }
    })
  }

  private def deleteSharedTopics(): Unit = {
    shareTopics.foreach(shareTopic => {
      val commandStr = s"$kafkaCommand -zookeeper $zookeeper " +
        s"--delete --topic $shareTopic"
      val ret = commandStr.!
      if (ret != 0) {
        printf(s"Delete topic $shareTopic failed\n")
      }
    })
  }

  private def clearCheckpoint(): Unit = {
    val checkpointPath = tpchSchemas(0).checkpointLocation
    val commandStr = s"$hdfsCommand dfs -rm -r $checkpointPath/*"
    val ret = commandStr.!
    if (ret != 0) {
      printf("Delete checkpoint failed\n")
    }
  }

  def startQueries(): Unit = {
    if (executionMode != ExecutionMode.NoShare) {
      createSharedTopics()
    }

    subQueries.foreach(_.start())
    serverThread.start()

    subQueries.foreach(_.join())
    serverThread.join()
    serverThread.reportStats()

    if (executionMode != ExecutionMode.NoShare) {
      deleteSharedTopics()
      clearCheckpoint()
    }
  }

}

object ExecutionMode extends Enumeration {
  type ExecutionMode = Value

  val NoShare: ExecutionMode.Value = Value("NoShare")
  val BatchShare: ExecutionMode.Value = Value("BatchShare")
  val SQPShare: ExecutionMode.Value = Value("SQPShare")
}

class ServerThread (numSubQ: Int, port: Int,
                    qnames: Array[String],
                    numBatchArray: Array[Int],
                    dependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                    queryInfo: Array[SubQueryInfo]) extends Thread {

  private val MAX_BATCH_NUM = 100
  private val server = new MetaServer(numSubQ, port)
  private val batchIDArray = new Array[Int](numSubQ)
  private var allTotalTime = 0.0
  private val totalTime = new Array[Double](numSubQ)
  private val finalTime = new Array[Double](numSubQ)
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
    for (uid <- 0 until numSubQ) {
      printf("query %d, batchNum %d\n", uid, numBatchArray(uid))
      printf("total time\tfinal time\n")
      printf("%.2f\t%.2f\n", totalTime(uid), finalTime(uid))
    }
    printf("%.2f\n", allTotalTime - initialStartupTime)
  }

}
