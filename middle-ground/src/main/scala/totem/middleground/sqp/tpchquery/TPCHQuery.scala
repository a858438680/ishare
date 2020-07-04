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

package totem.middleground.sqp.tpchquery

import totem.middleground.tpch._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.{from_avro, SchemaConverters}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

abstract class TPCHQuery extends Thread {
  protected var query_name: String = ""
  protected var uid: String = ""
  protected var numBatch: String = ""
  protected var constraint: String = ""
  protected var SF: Double = _
  protected var tpchSchema: TPCHSchema = _
  protected var spark: SparkSession = _

  def initialize(query_name: String, uid: String, numBatch: String, constraint: String,
                 SF: Double, tpchSchema: TPCHSchema, sparkSession: SparkSession): Unit = {
    this.query_name = query_name
    this.uid = uid
    this.numBatch = numBatch
    this.constraint = constraint
    this.tpchSchema = tpchSchema
    this.spark = sparkSession
    this.SF = SF
  }

  protected def loadSharedTable(spark: SparkSession,
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

  def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit
}
