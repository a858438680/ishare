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
package totem.middleground.tpch

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.sys.process._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.{from_avro, SchemaConverters}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sqpmeta.SubQueryInfo
import org.apache.spark.sql.sqpnetwork.MetaServer
import org.apache.spark.sql.types.StructType


class CandidateQuery (query_name: String, uid: String, numBatch: String, constraint: String,
                      tpchSchema: TPCHSchema, sparkSession: SparkSession, shareTopic: String)
  extends Thread
{
  def execQuery(): Unit = {

    query_name.toLowerCase match {
      case "q1_alone" =>
        execQ1_Alone(sparkSession, tpchSchema)
      case "q2_alone" =>
        execQ2_Alone(sparkSession, tpchSchema)
      case "q12_share" =>
        execQ12_Share(sparkSession, tpchSchema)
      case "q1_sep" =>
        execQ1_Sep(sparkSession, tpchSchema)
      case "q2_sep" =>
        execQ2_Sep(sparkSession, tpchSchema)
      case "q3_alone" =>
        execQ3_Alone(sparkSession, tpchSchema)
      case "q4_alone" =>
        execQ4_Alone(sparkSession, tpchSchema)
      case "q34_share" =>
        execQ34_Share(sparkSession, tpchSchema)
      case "q3_sep" =>
        execQ3_Sep(sparkSession, tpchSchema)
      case "q4_sep" =>
        execQ4_Sep(sparkSession, tpchSchema)
      case "q5_alone" =>
        execQ5_Alone(sparkSession, tpchSchema)
      case "q6_alone" =>
        execQ6_Alone(sparkSession, tpchSchema)
      case "q56_share" =>
        execQ56_Share(sparkSession, tpchSchema)
      case "q5_sep" =>
        execQ5_Sep(sparkSession, tpchSchema)
      case "q6_sep" =>
        execQ6_Sep(sparkSession, tpchSchema)
      case "q7" =>
        execQ7(sparkSession, tpchSchema)
      case _ =>
        printf("Not yet supported %s\n", query_name)
    }

  }

  private def execQ1_Alone(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val sum_qty = new DoubleSum
    val count_order = new Count

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)

    val result = l.filter($"l_shipdate" <= "1993-09-01")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum_qty($"l_quantity").as("sum_qty"),
        count_order(lit(1L)).as("count_order")
      )

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ2_Alone(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val doubleSum = new DoubleSum

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
    val p = DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)

    val result = l.filter($"l_shipdate" <= "1993-09-01")
      .groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
      .join(p, $"l_partkey" === $"p_partkey")
      .agg((doubleSum($"avg_quantity") / 7.0).as("avg_yearly"))

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  val shareSchema = new StructType()
    .add("l_partkey", "long")
    .add("avg_quantity", "double")
    // .add("p_size", "int")
    // .add("p_brand", "string")
    // .add("l_partkey", "long")
    // .add("l_quantity", "double")
    // .add("l_returnflag", "string")
    // .add("l_linestatus", "string")

  private def loadSharedTable(spark: SparkSession): DataFrame = {
    val alias = shareTopic
    val shareAvroSchema = SchemaConverters.toAvroType(shareSchema).toString
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", DataUtils.bootstrap)
      .option("subscribe", shareTopic)
      .option("startingOffsets", "earliest")
      .load().select(from_avro(col("value"), shareAvroSchema).as(alias))
      .selectExpr(alias + ".*")
  }

  private def execQ12_Share(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)

    val result = l.filter($"l_shipdate" <= "1993-09-01")
      .select($"l_partkey", $"l_quantity", $"l_returnflag", $"l_linestatus")

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToKafkaWithExtraOptions(
      result, shareTopic, unique_query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)
  }

  private def execQ1_Sep(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val sum_qty = new DoubleSum
    val count_order = new Count

    val share_l = loadSharedTable(spark)

    val result = share_l.groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum_qty($"l_quantity").as("sum_qty"),
        count_order(lit(1L)).as("count_order")
      )

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ2_Sep(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val doubleSum = new DoubleSum

    val share_l = loadSharedTable(spark)
    val p = DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)

    val result = share_l.groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
      .join(p, $"l_partkey" === $"p_partkey")
      .agg((doubleSum($"avg_quantity") / 7.0).as("avg_yearly"))

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ3_Alone(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val sum = new DoubleSum

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
      .filter($"l_partkey" % 10 === 0)
    val result = l.groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
      .agg(
        sum($"avg_quantity").as("sum quantity"))

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ4_Alone(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val doubleSum = new DoubleSum

    val doubleAvg1 = new DoubleAvg
    val doubleAvg2 = new DoubleAvg

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
      .filter($"l_partkey" % 10 === 0)
      .groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
    val p = DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema)

    val tmpResult = p.join(agg_l, $"p_partkey" === $"l_partkey")
    val result = ps
      .join(tmpResult, $"ps_partkey" === $"l_partkey")
      .agg((doubleSum($"avg_quantity") / 7.0).as("avg_yearly"),
        doubleAvg1($"p_retailprice").as("avg_retailprice"),
        doubleAvg2($"ps_supplycost").as("avg_supplycost"))

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ34_Share(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
      .filter($"l_partkey" % 10 === 0)

    val result = l.groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
      .select($"l_partkey", $"avg_quantity")

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToKafkaWithExtraOptions(
      result, shareTopic, unique_query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)
  }

  private def execQ3_Sep(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val share_agg_l = loadSharedTable(spark)

    val result = share_agg_l.agg(
      doubleSum($"avg_quantity").as("sum quantity"))

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ4_Sep(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum
    val doubleAvg = new DoubleAvg
    val doubleAvg2 = new DoubleAvg

    val share_agg_l = loadSharedTable(spark)

    val p = DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema)

    val tmpResult = p
      .join(share_agg_l, $"p_partkey" === $"l_partkey")
    val result = ps
      .join(tmpResult, $"ps_partkey" === $"l_partkey")
      .agg((doubleSum($"avg_quantity") / 7.0).as("avg_yearly"),
        doubleAvg($"p_retailprice").as("avg_retailprice"),
        doubleAvg2($"ps_supplycost").as("avg_supplycost"))

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ5_Alone(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val sum = new DoubleSum

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
      .filter($"l_shipdate" <= "1994-01-01")
      .groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
    val p = DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
      .filter($"p_size" === 1 and $"p_brand" === "Brand#13")
    val result = p.join(agg_l, $"p_partkey" === $"l_partkey")
      .agg(
        sum($"avg_quantity").as("sum quantity"))

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ6_Alone(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val doubleSum = new DoubleSum

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
      .filter($"l_shipdate" <= "1994-01-01")
      .groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
    val p = DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema)

    // val tmpResult = p.join(agg_l, $"p_partkey" === $"l_partkey")
    // val result = ps
    //   .join(tmpResult, $"ps_partkey" === $"l_partkey")
    //   .agg((doubleSum($"avg_quantity") / 7.0).as("avg_yearly"))

    val result = agg_l.join(p, $"l_partkey" === $"p_partkey")
        .join(ps, $"l_partkey" === $"ps_partkey")
      .agg((doubleSum($"avg_quantity") / 7.0).as("avg_yearly"))

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ56_Share(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
      .filter($"l_shipdate" <= "1994-01-01")
      .groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))

    val p = DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
      .filter($"p_size" === 1 and $"p_brand" === "Brand#13")

    val result = agg_l.join(p, $"l_partkey" === $"p_partkey")
     .select($"l_partkey", $"avg_quantity")

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToKafkaWithExtraOptions(
      result, shareTopic, unique_query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)
  }

  private def execQ5_Sep(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val share_join = loadSharedTable(spark)

    val result = share_join.agg(
      doubleSum($"avg_quantity").as("sum quantity"))

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ6_Sep(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val share_join = loadSharedTable(spark)
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema)

    val result = ps
      .join(share_join, $"ps_partkey" === $"l_partkey")
      .agg((doubleSum($"avg_quantity") / 7.0).as("avg_yearly"))

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  private def execQ7(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
    import spark.implicits._

    val sum_qty = new DoubleSum
    val count_order = new Count

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)

    val result = l.filter($"l_shipdate" =!= "1993-09-01" or $"l_quantity" <= 10.0)
      .filter($"l_quantity" <= 10.0)
      .filter($"l_quantity" === 10.0)
      .filter($"l_quantity" =!= 10.0)
      .filter($"l_quantity" < 10.0)
      .filter($"l_returnflag" < "RI")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum_qty($"l_quantity").as("sum_qty"),
        count_order(lit(1L)).as("count_order")
      )

    // result.explain(true)

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

  override def run(): Unit = {
    execQuery()
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

    val candidateSet = new HashSet[Int]
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

  private def buildDependencySet(candidateSet: HashSet[Int]): Array[HashSet[Int]] = {
    if (dependency == null) return Array(candidateSet)

    val buf = new ArrayBuffer[HashSet[Int]]
    while (candidateSet.nonEmpty) {
      val tmpHashSet = new HashSet[Int]

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

private object AloneConfig {
  val queryNames = Array("q5_alone", "q6_alone")
  val numBatches = Array(2, 2)
  val constraints = Array("0.5", "0.05")
  val numSubQ = 2
  val tpchSchemas = new Array[TPCHSchema](numSubQ)
  val sharetopic = ""

  // val queryNames = Array("q7")
  // val numBatches = Array(20)
  // val constraints = Array("0.05")
  // val numSubQ = 1
  // val tpchSchemas = new Array[TPCHSchema](numSubQ)
  // val sharetopic = ""
}

private object ShareConfig {
  val queryNames = Array("q5_sep", "q6_sep", "q56_share")
  val numBatches = Array(2, 2, 2)
  val constraints = Array("0.5", "0.05", "0.01")
  val numSubQ = 3
  val tpchSchemas = new Array[TPCHSchema](numSubQ)
  val sharetopic = "sharetopic"
  val dependency =
    HashMap(
      0 -> HashSet(2),
      1 -> HashSet(2))

  // val queryNames = Array("q4_sep", "q34_share")
  // val numBatches = Array(2, 50)
  // val constraints = Array("0.5", "0.05")
  // val numSubQ = 2
  // val tpchSchemas = new Array[TPCHSchema](numSubQ)
  // val sharetopic = "sharetopic"
  // val dependency =
  //   HashMap(
  //     0 -> HashSet(1))
}

class SharedExample (bootstrap: String, shuffleNum: String, statDIR: String, SF: Double,
                     hdfsRoot: String, execution_mode: String, inputPartitions: Int,
                     kafkaCommand: String, zookeeper: String, hdfsCommand: String, port: Int)
{
  DataUtils.bootstrap = bootstrap

  private val isShare = false

  private val largeDataset = false
  private val queryNames =
    if (isShare) ShareConfig.queryNames
    else AloneConfig.queryNames
  private val numBatches =
    if (isShare) ShareConfig.numBatches
    else AloneConfig.numBatches
  private val constraints =
    if (isShare) ShareConfig.constraints
    else AloneConfig.constraints
  private val tpchSchemas =
    if (isShare) ShareConfig.tpchSchemas
    else AloneConfig.tpchSchemas
  private val numSubQ =
    if (isShare) ShareConfig.numSubQ
    else AloneConfig.numSubQ
  private val shareTopic =
    if (isShare) ShareConfig.sharetopic
    else AloneConfig.sharetopic
  private val dependency =
    if (isShare) ShareConfig.dependency
    else null

  if (isShare) printf("running shared execution\n")
  else printf("run not share execution\n")

  for (idx <- 0 until numSubQ) {
    tpchSchemas(idx) = new TPCHSchema
    tpchSchemas(idx).setQueryMetaData(numBatches(idx), SF,
      hdfsRoot, inputPartitions, largeDataset)
  }

  private val sparkConf = new SparkConf()
    .set(SQLConf.SHUFFLE_PARTITIONS.key, shuffleNum)
    .set(SQLConf.SLOTHDB_STAT_DIR.key, statDIR)
    .set(SQLConf.SLOTHDB_EXECUTION_MODE.key, execution_mode)
    .set(SQLConf.SQP_PORT.key, port.toString)
    .set(SQLConf.SQP_SOURCE_PARTITION.key, inputPartitions.toString)
    .set("spark.scheduler.mode", "FAIR")

  private val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .appName("Shared Query Execution")
    .getOrCreate()

  private val queries = new Array[CandidateQuery](numSubQ)
  for (idx <- 0 until numSubQ) {
    val candidateQuery =
      new CandidateQuery(queryNames(idx), idx.toString, numBatches(idx).toString,
        constraints(idx), tpchSchemas(idx), sparkSession, shareTopic)
    queries(idx) = candidateQuery
  }

  private val queryInfo =
    if (isShare) ExampleQueryInfo.getShareSubQueryInfo
    else ExampleQueryInfo.getSepSubQueryInfo

  private val serverThread =
    new ServerThread(numSubQ, port, queryNames, numBatches, dependency, queryInfo)

  private def createSharedTopic(): Unit = {
    val commandStr = s"$kafkaCommand -zookeeper $zookeeper --create " +
      s"--topic $shareTopic --partitions ${inputPartitions} --replication-factor 1"
    val ret = commandStr.!
    if (ret != 0) {
      printf("Create topic failed\n")
    }
  }

  private def deleteSharedTopic(): Unit = {
    val commandStr = s"$kafkaCommand -zookeeper $zookeeper " +
      s"--delete --topic $shareTopic"
    val ret = commandStr.!
    if (ret != 0) {
      printf("Delete topic failed\n")
    }
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
    if (isShare) createSharedTopic()

    queries.foreach(_.start())
    serverThread.start()

    queries.foreach(_.join())
    serverThread.join()
    serverThread.reportStats()

    if (isShare) {
      deleteSharedTopic()
      clearCheckpoint()
    }
  }
}

object SharedExample {

  def main(args: Array[String]): Unit = {

    if (args.length < 10) {
      System.err.println("Usage: QueryTPCH <bootstrap-servers>" +
        "<number-shuffle-partition> <statistics dir> <SF> <HDFS root>" +
        "<execution mode [0: inc-aware(subpath),1: inc-aware(subplan), 2: inc-oblivious, " +
        "3: generate inc statistics, 4: training]>  <num of input partitions> " +
        "<path to kafka topics command> <zookeeper address> <path to hdfs command>")
      System.exit(1)
    }

    val port = 8887
    val tpch = new SharedExample(args(0), args(1), args(2),
      args(3).toDouble, args(4), args(5), args(6).toInt, args(7), args(8), args(9), port)

    tpch.startQueries()
  }

}

// scalastyle:off println
