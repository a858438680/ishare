// scalastyle:off
package totem.middleground.sqp.test

import totem.middleground.tpch._
import totem.middleground.sqp.tpchquery.TPCHQuery

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.{from_avro, SchemaConverters}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

private class MyQuery extends TPCHQuery {

  private val Q1_2 = new StructType()
    .add("n_name", "string")
    .add("s_acctbal", "double")
    .add("s_suppkey", "long")
    .add("s_address", "string")
    .add("s_phone", "string")
    .add("s_comment", "string")
    .add("s_name", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._
    val result =((loadSharedTable(spark, "Q1_2", Q1_2))
      .join((DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema))
        .join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
          .filter(($"p_size" === 15) and ($"p_type" like ("%BRASS"))), $"ps_partkey" === $"p_partkey", "inner"), $"s_suppkey" === $"ps_suppkey", "inner"))
      .join((loadSharedTable(spark, "Q1_2", Q1_2))
        .join(DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema), $"s_suppkey" === $"ps_suppkey", "inner")
        .groupBy($"ps_partkey")
        .agg(
          min($"ps_supplycost").as("min_supplycost"))
        .select($"ps_partkey".as("min_partkey"), $"min_supplycost")
        , ($"p_partkey" === $"min_partkey") and  ($"ps_supplycost" === $"min_supplycost"), "inner")
      .select($"s_acctbal", $"s_name", $"n_name", $"p_partkey", $"p_mfgr", $"s_address", $"s_phone", $"s_comment")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}
