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

package totem.middleground.tpch

import java.io.{BufferedReader, FileReader}
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.{from_avro, to_avro}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger


object DataUtils {

  var bootstrap: String = null

  def loadStreamTable(spark: SparkSession,
                      tableName: String,
                      alias: String,
                      tpchSchema: TPCHSchema): DataFrame = {
    val (_, avroSchema, _, _, topics, offsetPerTrigger) = tpchSchema.GetMetaData(tableName).get

    printf(s"load $tableName with offset $offsetPerTrigger\n")

    return spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", offsetPerTrigger)
      .load().select(from_avro(col("value"), avroSchema).as(alias))
      .selectExpr(alias + ".*")
  }

  def loadStaticTable(spark: SparkSession, tableName: String, alias: String): DataFrame = {
    val (schema, _, _, staticPath, _, _) =
      TPCHSchema.defaultTPCHSchema.GetMetaData(tableName).get

    return spark
      .read
      .format("csv")
      .option("sep", "|")
      .schema(schema)
      .load(staticPath)
  }

  def loadIOLAPDoubleTable(spark: SparkSession, path: String): Double = {
    val reader = new BufferedReader(new FileReader((path)))
    val value = reader.readLine().toDouble
    reader.close()

    value
  }

  def loadIOLAPLongTable(spark: SparkSession, path: String): Array[Long] = {
    val reader = new BufferedReader(new FileReader((path)))
    val keyArray = new ArrayBuffer[Long]()

    var endOfLine = false
    var line: String = ""
    while(!endOfLine) {
      line = reader.readLine()
      if (line != null) keyArray.append(line.toLong)
      else endOfLine = true
    }

    reader.close()

    keyArray.toArray
  }

  def writeToSink(query_result: DataFrame, query_name: String): Unit = {
    val q = query_result
      .writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(100, TimeUnit.MILLISECONDS))
      // .option("checkpointLocation", TPCHSchema.checkpointLocation + "/" + query_name)
      .queryName(query_name)
      .start()

    q.awaitTermination()
  }

  def writeToSinkWithExtraOptions(query_result: DataFrame,
                                  query_name: String,
                                  uid: String,
                                  numBatch: String,
                                  constraint: String): Unit = {
    val digit_constraint = constraint.toDouble
    val constraint_key =
      if (digit_constraint <= 1.0) SQLConf.SLOTHDB_LATENCY_CONSTRAINT.key
      else SQLConf.SLOTHDB_RESOURCE_CONSTRAINT.key

    val q = query_result
      .writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(100, TimeUnit.MILLISECONDS))
      .option(SQLConf.SLOTHDB_BATCH_NUM.key, numBatch)
      .option(constraint_key, constraint)
      .option(SQLConf.SQP_UID.key, uid)
      .queryName(query_name)
      .start()

    q.awaitTermination()
  }

  def writeToKafkaWithExtraOptions(query_result: DataFrame,
                                   output_topic: String,
                                   query_name: String,
                                   uid: String,
                                   numBatch: String,
                                   constraint: String,
                                   checkpointLocation: String): Unit = {
    val digit_constraint = constraint.toDouble
    val constraint_key =
      if (digit_constraint <= 1.0) SQLConf.SLOTHDB_LATENCY_CONSTRAINT.key
      else SQLConf.SLOTHDB_RESOURCE_CONSTRAINT.key

    val q = query_result.select(lit("8bytekey") as "key", to_avro(struct("*")) as "value")
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime(100, TimeUnit.MILLISECONDS))
      .option(SQLConf.SLOTHDB_BATCH_NUM.key, numBatch)
      .option(constraint_key, constraint)
      .option(SQLConf.SQP_UID.key, uid)
      .option(SQLConf.SQP_MED_PLAN.key, "true")
      .option("topic", output_topic)
      .option("kafka.bootstrap.servers", bootstrap)
      .option("checkpointLocation", checkpointLocation + "/" + query_name.toLowerCase)
      .queryName(query_name)
      .start()

    q.awaitTermination()
  }

  def writeToFile(query_result: DataFrame, query_name: String, path: String): Unit = {
    val q = query_result.coalesce(1)
      .writeStream
      .outputMode("append")
      .format("csv")
      .option("path", path)
      .option("checkpointLocation",
        TPCHSchema.defaultTPCHSchema.checkpointLocation + "/" + query_name)
      .trigger(Trigger.ProcessingTime(100, TimeUnit.MILLISECONDS))
      .queryName(query_name)
      .start()

    q.awaitTermination()
  }

}
