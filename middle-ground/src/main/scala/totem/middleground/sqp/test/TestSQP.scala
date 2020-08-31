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
        "[Execution mode: 0 - NoShare, 1 - BatchShare, 2 - SQPShare, 3 - InQP]" +
        "[Enable Unshare: True or False]" +
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
      else if (args(9).compareTo("2") == 0) ExecutionMode.SQPShare
      else ExecutionMode.InQP

    val enableUnShare =
      if (args(10).toLowerCase.compareTo("true") == 0) true
      else false
    val dfDir = args(11)
    val configFile = args(12)
    val predFile = args(13)

    Optimizer.initializeOptimizer(predFile)
    val optimizedQueries = optimizeMultiQuery(dfDir, configFile, executionMode, enableUnShare)

    val start = System.nanoTime()
    val pair = QueryGenerator.generateQueryAndConfiguration(optimizedQueries)
    println(s"Query generation takes ${(System.nanoTime() - start)/1000000} ms\n")

    val subQueries = pair._1
    val queryConfig = pair._2

    val testSQP = new TestSQP(bootstrap, shuffleNum, statDIR, SF, hdfsRoot, inputPartitions,
      kafkaCommand, zookeeper, hdfsCommand, port, executionMode, enableUnShare,
      subQueries, queryConfig)

    testSQP.startQueries()
  }

  private def optimizeMultiQuery(dir: String,
                                 configName: String,
                                 executionMode: ExecutionMode.Value,
                                 enableUnShare: Boolean): QueryGraph = {
    var start = System.nanoTime()
    val queryGraph = Utils.getParsedQueryGraph(dir, configName)
    val parseTime = (System.nanoTime() - start)/1000000

    println(s"Parsing takes $parseTime ms")

    start = System.nanoTime()
    val optimizedGraph =
      if (executionMode == ExecutionMode.BatchShare) {
        Optimizer.OptimizeUsingBatchMQO(queryGraph)
      } else if (executionMode == ExecutionMode.SQPShare) {
        Optimizer.OptimizeUsingSQP(queryGraph, enableUnShare)
      } else if (executionMode == ExecutionMode.InQP) {
        Optimizer.OptimizeWithInQP(queryGraph)
      } else { // No Sharing
        Optimizer.OptimizeWithoutSharing(queryGraph)
      }
    val optTime = (System.nanoTime() - start)/1000000
    Utils.printPaceConfig(optimizedGraph)

    println(s"Optimization takes $optTime ms\n")

    optimizedGraph
  }
}

class TestSQP (bootstrap: String, shuffleNum: String, statDIR: String, SF: Double,
              hdfsRoot: String, inputPartitions: Int, kafkaCommand: String,
              zookeeper: String, hdfsCommand: String, port: Int,
              executionMode: ExecutionMode.Value,
              enableUnShare: Boolean,
              subQueries: Array[TPCHQuery], queryConfig: QueryConfig) {

  if (executionMode == ExecutionMode.NoShare) {
    println("No Shared Execution")
  } else if (executionMode == ExecutionMode.BatchShare) {
    println("Shared Execution using Batch Optimizer")
  } else if (executionMode == ExecutionMode.SQPShare && enableUnShare) {
    println("Shared Execution of iShare(True)")
  } else if (executionMode == ExecutionMode.SQPShare && !enableUnShare) {
    println("Shared Execution of iShare(False)")
  } else {
    println("No Shared Execution with InQP")
  }

  DataUtils.bootstrap = bootstrap

  private val sparkConf = new SparkConf()
    .set(SQLConf.SHUFFLE_PARTITIONS.key, shuffleNum)
    .set(SQLConf.SLOTHDB_STAT_DIR.key, statDIR)
    .set(SQLConf.SQP_PORT.key, port.toString)
    .set(SQLConf.SQP_SOURCE_PARTITION.key, inputPartitions.toString)
    .set("spark.scheduler.mode", "FAIR")

  // private val sparkSession = SparkSession.builder()
  //   .config(sparkConf)
  //   .appName("Shared Query Execution")
  //   .getOrCreate()

  private val largeDataset = false
  private val numSubQ = subQueries.length
  private val numBatches = queryConfig.numBatches
  private val queryNames = queryConfig.queryNames
  private val constraints = queryConfig.constraintMap
  private val subQueryInfos = queryConfig.subQueryInfo
  private val schedulingOrder = queryConfig.schedulingOrder
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
      "1.0", SF, tpchSchemas(idx))
  })

  private val isInQP = executionMode == ExecutionMode.InQP

  private val serverThread =
    new ServerThread(numSubQ, port, queryNames, numBatches, schedulingOrder,
      queryDependency, subQueryInfos, subQueries, sparkConf, isInQP, this)

  def getConstraints(): mutable.HashMap[Int, Double] = {
    constraints
  }

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
    if (executionMode == ExecutionMode.BatchShare ||
      executionMode == ExecutionMode.SQPShare ||
      executionMode == ExecutionMode.InQP) {
      createSharedTopics()
    }

    // subQueries.foreach(_.start())
    serverThread.start()

    // subQueries.foreach(_.join())
    serverThread.join()
    val execStr =
      if (executionMode == ExecutionMode.SQPShare && enableUnShare) {
        executionMode.toString + "(True)"
      } else if (executionMode == ExecutionMode.SQPShare && !enableUnShare) {
        executionMode.toString + "(False)"
      } else executionMode.toString
    serverThread.reportStats(statDIR, execStr)

    if (executionMode == ExecutionMode.BatchShare ||
      executionMode == ExecutionMode.SQPShare ||
      executionMode == ExecutionMode.InQP) {
      deleteSharedTopics()
      clearCheckpoint()
    }
  }

  def failCleanup(): Unit = {
     if (executionMode == ExecutionMode.BatchShare ||
         executionMode == ExecutionMode.SQPShare ||
         executionMode == ExecutionMode.InQP) {
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
  val InQP: ExecutionMode.Value = Value("InQP")
}

class ServerThread (numSubQ: Int, port: Int,
                    qnames: Array[String],
                    numBatchArray: Array[Int],
                    uidOrder: Array[Int],
                    dependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                    queryInfo: Array[SubQueryInfo],
                    subqueries: Array[TPCHQuery],
                    sparkConf: SparkConf,
                    isInQP: Boolean,
                    driver: TestSQP) extends Thread {

  private val server = new MetaServer(numSubQ, port)
  private var allTotalTime = 0.0
  private val totalTime = new Array[Double](numSubQ)
  private val finalTime = new Array[Double](numSubQ)
  private val initialStartupTime = 4000.0
  private val maxBatchNum = Catalog.getMaxBatchNum

  private val batchNumForScheduling = Array.fill[Int](numBatchArray.length)(1)
  private val progressSimulator =
    new ProgressSimulator(maxBatchNum, batchNumForScheduling, dependency)
  private val parentDep = Optimizer.fromQueryDepToParentDep(dependency)

  server.loadSharedQueryInfo(queryInfo)

  override def run(): Unit = {

    var setArray: Array[mutable.HashSet[Int]] = null
    var curStep = 0
    while (curStep != maxBatchNum) {
      val pair = progressSimulator.nextStep()
      curStep = pair._1
      setArray = pair._2
    }

    // This is the order for scheduling sub-queries
    // Each subquery uses its own SparkSession
    setArray.foreach(execSet => {
      execSet.foreach(uid => {

        val sparkSession = SparkSession.builder()
          .config(sparkConf)
          .appName("Shared Query Execution")
          .getOrCreate()
        subqueries(uid).setSparkSession(sparkSession)
        subqueries(uid).start()
        server.startOneQuery()

        val parentBatchNum =
          if (!isInQP || parentDep(uid).isEmpty) -1
          else {
            val parentUid = parentDep(uid).max
            numBatchArray(parentUid)
          }

        val numBatch = numBatchArray(uid)
        var parentStep = 0
        for (curStep <- 0 until numBatch) {

          val repair =
            if (isInQP) {
              val curProgress = (curStep + 1).toDouble/numBatch.toDouble
              val parentProgress = (parentStep + 1).toDouble/parentBatchNum.toDouble
              if (curProgress >= parentProgress) {
                parentStep += 1
                true
              } else false
            } else true

          server.startOneExecution(uid, repair)
          val msg = server.getStatMessage(uid)
          // clean up here
          if (msg.batchID == -1) {
            println("Caught an exception, start shutting down the program")
            server.terminateQuery(uid)
            server.stopOneQuery(uid)
            server.stopServerSocket()
            driver.failCleanup()
            System.exit(1)
          }
          val execTime =
            if (curStep == 0) {
              math.max(msg.execTime - initialStartupTime, 10)
            } else {
              math.max(msg.execTime, 10)
            }
          totalTime(uid) += execTime
          allTotalTime += execTime
          if (curStep == (numBatchArray(uid) - 1)) {
            finalTime(uid) = execTime
          }
        }

        server.stopOneQuery(uid)
        Thread.sleep(2000)
        sparkSession.stop()
        Thread.sleep(2000)

      })
    })

    server.stopServerSocket()
  }

  def reportStats(statDir: String, executionModeStr: String): Unit = {
    for (uid <- 0 until numSubQ) {
      println(s"${qnames(uid)}, batchNum ${numBatchArray(uid)}")
      printf("total time\tfinal time\n")
      printf("%.2f\t%.2f\n", totalTime(uid), finalTime(uid))
    }
    printf("Total time for all queries: %.2f\n\n", allTotalTime)
    printf("Latency (Shortest Job First)\n")
    val rootQueries = uidOrder
    val uidQueue = mutable.Queue.empty[Int]
    rootQueries.foreach(uidQueue.enqueue(_))
    val latencyMap = Utils.latencyWithOrder(uidQueue, finalTime, dependency)
    rootQueries.foreach(uid => {
      val latency = latencyMap(uid)
      println(s"Q${getRawQueryName(qnames(uid))}\t$latency")
    })

    val timestamp = Utils.getCurTimeStamp()

    // Write stats into files
    writeSubqueryInfo(statDir, executionModeStr, timestamp)
    writeLatencyInfo(statDir, executionModeStr, timestamp, rootQueries, latencyMap)
    writeAggInfo(statDir, executionModeStr, timestamp)
    writeConfig(statDir)
  }

  private def writeSubqueryInfo(statDir: String,
                                executionModeStr: String,
                                timestamp: String): Unit = {
    // Write standalone latency for each subquery
    val subQueryFile = statDir + "/subquery.stat"
    val subQueryWriter = new PrintWriter(new FileWriter(subQueryFile, true))
    for (uid <- 0 until numSubQ) {
      subQueryWriter.println(s"${timestamp}\t${executionModeStr}\t${uid}\t${qnames(uid)}\t" +
        s"${numBatchArray(uid)}\t${totalTime(uid)}\t${finalTime(uid)}")
    }
    subQueryWriter.print("\n")
    subQueryWriter.close()

    // Write subquery dependency
    val depFile = statDir + "/dependency.stat"
    val depWriter = new PrintWriter(new FileWriter(depFile, true))
    for (uid <- 0 until numSubQ) {
      val depUids = dependency(uid).toArray
      val depStr = uidArrayToString(depUids)
      depWriter.println(s"${timestamp}\t${executionModeStr}\t${uid}\t${depStr}")
    }
    depWriter.print("\n")
    depWriter.close()
  }

  private def uidArrayToString(depUids: Array[Int]): String = {
    val stringBuffer = new StringBuffer()
    depUids.zipWithIndex.foreach(pair => {
      val uid = pair._1
      val idx = pair._2
      if (idx < depUids.length - 1) stringBuffer.append(uid + ",")
      else stringBuffer.append(uid)
    })
    stringBuffer.toString
  }

  private def writeLatencyInfo(statDir: String,
                               executionModeStr: String,
                               timestamp: String,
                               rootQueries: Array[Int],
                               latencyMap: mutable.HashMap[Int, Double]): Unit = {
    // write stacked latency
    val stackFile = statDir + "/stack.stat"
    val stackWriter = new PrintWriter(new FileWriter(stackFile, true))
    rootQueries.foreach(uid => {
      val latency = latencyMap(uid)
      stackWriter.println(s"${timestamp}\t$executionModeStr\t" +
        s"${getRawQueryName(qnames(uid))}\t$latency")
    })
    stackWriter.print("\n")
    stackWriter.close()

    val standaloneFile = statDir + "/standalone.stat"
    val standaloneWriter = new PrintWriter(new FileWriter(standaloneFile, true))
    var prevUid = -1
    rootQueries.foreach(uid => {
      val latency = latencyMap(uid)
      val aloneLatency =
        if (prevUid == -1) latency
        else {
          val prevLatency = latencyMap(prevUid)
          latency - prevLatency
        }
      prevUid = uid
      standaloneWriter.println(s"${timestamp}\t$executionModeStr\t" +
        s"${getRawQueryName(qnames(uid))}\t$aloneLatency")
    })

    standaloneWriter.print("\n")
    standaloneWriter.close()
  }

  private def writeConfig(statDir: String): Unit = {
    val configFile = statDir + "/goal.conf"
    val goalWriter = new PrintWriter(new FileWriter(configFile, true))
    val constriantMap = driver.getConstraints()
    constriantMap.foreach(pair => {
      val qid = pair._1
      val constraint = pair._2
      goalWriter.println(s"Q${qid},$qid,$constraint")
    })
    goalWriter.println("\n")
    goalWriter.close()
  }

  private def writeAggInfo(statDir: String,
                           executionModeStr: String,
                           timestamp: String): Unit = {
    val aggFile = statDir + "/total.stat"
    val aggWriter = new PrintWriter(new FileWriter(aggFile, true))
    aggWriter.println(s"${timestamp}\t$executionModeStr\t$allTotalTime")
    aggWriter.close()
  }

  private def getRawQueryName(queryName: String): String = {
    val idx = queryName.indexOf("_")
    queryName.substring(idx + 1)
  }

  private def simulateLatency(): mutable.HashMap[Int, Double] = {
    val candidateSet = mutable.HashSet.empty[Int]
    for (uid <- 0 until numSubQ) candidateSet.add(uid)

    var runningSet = mutable.HashMap.empty[Int, Double]
    var curLatency = 0.0

    val rootQueries = findRootQueries()
    val latencyMap = mutable.HashMap.empty[Int, Double]

    while (candidateSet.nonEmpty) {
      val runnable = runnableSet(candidateSet)

      runnable.foreach(uid => {
        if (!runningSet.contains(uid)) runningSet.put(uid, finalTime(uid))
      })

      val triple = finishOneQuery(runningSet, curLatency)
      val finishedUid = triple._1
      curLatency = triple._2
      runningSet = triple._3

      if (rootQueries.contains(finishedUid)) {
        latencyMap.put(finishedUid, curLatency)
      }

      candidateSet.remove(finishedUid)
    }

    latencyMap
  }

  private def runnableSet(candidateSet: mutable.HashSet[Int]): mutable.HashSet[Int] = {
    if (dependency == null) return candidateSet

    val runnable = mutable.HashSet.empty[Int]

    candidateSet.foreach(uid => {
      dependency.get(uid) match {
        case None => runnable.add(uid)
        case Some(depSet) =>
          val leafNode =
            !depSet.exists(depUID => {
              candidateSet.contains(depUID)
            })
          if (leafNode) runnable.add(uid)
      }
    })

    runnable
  }

  private def finishOneQuery(runningSet: mutable.HashMap[Int, Double],
                             curLatency: Double):
  (Int, Double, mutable.HashMap[Int, Double]) = {

    val minLatencyPair =
      runningSet.foldLeft((-1, Double.MaxValue))((minLatency, pair) => {
        if (pair._2 < minLatency._2) pair
        else minLatency
      })

    val minUid = minLatencyPair._1
    val minLatency = minLatencyPair._2

    val newLatency = (minLatency * runningSet.size.toDouble) + curLatency
    val newRunningSet = mutable.HashMap.empty[Int, Double]
    runningSet.remove(minUid)
    runningSet.foreach(pair => {
      newRunningSet.put(pair._1, pair._2 - minLatency)
    })

    (minUid, newLatency, newRunningSet)
  }

  private def findRootQueries(): mutable.HashSet[Int] = {
    val rootQueries = mutable.HashSet.empty[Int]
    for (uid <- 0 until numSubQ) rootQueries.add(uid)

    dependency.foreach(pair => {
      val depSet = pair._2
      depSet.foreach(rootQueries.remove)
    })

    rootQueries
  }

}
