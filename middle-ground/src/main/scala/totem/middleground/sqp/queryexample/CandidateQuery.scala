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

package totem.middleground.sqp.queryexample

import totem.middleground.tpch._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.{from_avro, SchemaConverters}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sqpmeta.SubQueryInfo
import org.apache.spark.sql.sqpnetwork.MetaServer
import org.apache.spark.sql.types.StructType

class CandidateQuery extends Thread {
  private var query_name: String = ""
  private var uid: String = ""
  private var numBatch: String = ""
  private var constraint: String = ""
  private var tpchSchema: TPCHSchema = _
  private var spark: SparkSession = _

  def initialize(query_name: String, uid: String, numBatch: String, constraint: String,
                 tpchSchema: TPCHSchema, sparkSession: SparkSession): Unit = {
    this.query_name = query_name
    this.uid = uid
    this.numBatch = numBatch
    this.constraint = constraint
    this.tpchSchema = tpchSchema
    this.spark = sparkSession
  }

  private def loadSharedTable(spark: SparkSession,
                              shareTopic: String,
                              shareSchema: StructType): DataFrame = {
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

  override def run(): Unit = {
    execQuery(spark, tpchSchema)
  }

  private def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {
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

    val unique_query_name = query_name + "_" + uid
    DataUtils.writeToSinkWithExtraOptions(
      result, unique_query_name, uid, numBatch, constraint)
  }

}
