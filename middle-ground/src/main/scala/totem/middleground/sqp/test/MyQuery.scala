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

private class Q0_0 extends TPCHQuery {

  private val Q44_0_1 = new StructType()
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("s_name", "string")

  private val Q48_0_1 = new StructType()
    .add("l2_orderkey", "long")
    .add("l2_suppkey", "long")

  private val Q49_0_1 = new StructType()
    .add("l3_orderkey", "long")
    .add("l3_suppkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val count = new Count

    val result = ((loadSharedTable(spark, "Q44_0_1", Q44_0_1))
      .join(loadSharedTable(spark, "Q48_0_1", Q48_0_1), ($"l_orderkey" === $"l2_orderkey") and  ($"l_suppkey" =!= $"l2_suppkey"), "left_semi"))
      .join(loadSharedTable(spark, "Q49_0_1", Q49_0_1), ($"l_orderkey" === $"l3_orderkey") and  ($"l_suppkey" =!= $"l3_suppkey"), "left_anti")
      .groupBy($"s_name")
      .agg(
        count(lit(1L)).as("numwait"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q1_1 extends TPCHQuery {

  private val Q44_0_1 = new StructType()
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("s_name", "string")

  private val Q48_0_1 = new StructType()
    .add("l2_orderkey", "long")
    .add("l2_suppkey", "long")

  private val Q49_0_1 = new StructType()
    .add("l3_orderkey", "long")
    .add("l3_suppkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val count = new Count

    val result = ((loadSharedTable(spark, "Q44_0_1", Q44_0_1))
      .join(loadSharedTable(spark, "Q48_0_1", Q48_0_1), ($"l_orderkey" === $"l2_orderkey") and  ($"l_suppkey" =!= $"l2_suppkey"), "left_semi"))
      .join(loadSharedTable(spark, "Q49_0_1", Q49_0_1), ($"l_orderkey" === $"l3_orderkey") and  ($"l_suppkey" =!= $"l3_suppkey"), "left_anti")
      .groupBy($"s_name")
      .agg(
        count(lit(1L)).as("numwait"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q2_2 extends TPCHQuery {

  private val Q50_2_3 = new StructType()
    .add("avg_disc", "double")
    .add("sum_disc_price", "double")
    .add("sum_qty", "double")
    .add("sum_charge", "double")
    .add("l_linestatus", "string")
    .add("avg_price", "double")
    .add("sum_base_price", "double")
    .add("avg_qty", "double")
    .add("count_order", "long")
    .add("l_returnflag", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q50_2_3", Q50_2_3)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q3_3 extends TPCHQuery {

  private val Q50_2_3 = new StructType()
    .add("avg_disc", "double")
    .add("sum_disc_price", "double")
    .add("sum_qty", "double")
    .add("sum_charge", "double")
    .add("l_linestatus", "string")
    .add("avg_price", "double")
    .add("sum_base_price", "double")
    .add("avg_qty", "double")
    .add("count_order", "long")
    .add("l_returnflag", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q50_2_3", Q50_2_3)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q4_4 extends TPCHQuery {

  private val Q51_5_4 = new StructType()
    .add("n_name", "string")
    .add("s_acctbal", "double")
    .add("p_partkey", "long")
    .add("s_address", "string")
    .add("s_phone", "string")
    .add("p_mfgr", "string")
    .add("s_comment", "string")
    .add("s_name", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q51_5_4", Q51_5_4)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q5_5 extends TPCHQuery {

  private val Q51_5_4 = new StructType()
    .add("n_name", "string")
    .add("s_acctbal", "double")
    .add("p_partkey", "long")
    .add("s_address", "string")
    .add("s_phone", "string")
    .add("p_mfgr", "string")
    .add("s_comment", "string")
    .add("s_name", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q51_5_4", Q51_5_4)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q6_6 extends TPCHQuery {

  private val Q55_6_7 = new StructType()
    .add("o_orderdate", "date")
    .add("o_shippriority", "int")
    .add("revenue", "double")
    .add("l_orderkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q55_6_7", Q55_6_7)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q7_7 extends TPCHQuery {

  private val Q55_6_7 = new StructType()
    .add("o_orderdate", "date")
    .add("o_shippriority", "int")
    .add("revenue", "double")
    .add("l_orderkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q55_6_7", Q55_6_7)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q8_8 extends TPCHQuery {

  private val Q56_9_16_17_6_10_7_11_8 = new StructType()
    .add("o_orderdate", "date")
    .add("o_orderkey", "long")
    .add("o_shippriority", "int")
    .add("o_orderpriority", "string")
    .add("o_custkey", "long")

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val order_count = new Count

    val result = (loadSharedTable(spark, "Q56_9_16_17_6_10_7_11_8", Q56_9_16_17_6_10_7_11_8))
      .join(loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8), $"o_orderkey" === $"l_orderkey", "left_semi")
      .groupBy($"o_orderpriority")
      .agg(
        order_count(lit(1L)).as("order_count"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q9_9 extends TPCHQuery {

  private val Q56_9_16_17_6_10_7_11_8 = new StructType()
    .add("o_orderdate", "date")
    .add("o_orderkey", "long")
    .add("o_shippriority", "int")
    .add("o_orderpriority", "string")
    .add("o_custkey", "long")

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val order_count = new Count

    val result = (loadSharedTable(spark, "Q56_9_16_17_6_10_7_11_8", Q56_9_16_17_6_10_7_11_8))
      .join(loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8), $"o_orderkey" === $"l_orderkey", "left_semi")
      .groupBy($"o_orderpriority")
      .agg(
        order_count(lit(1L)).as("order_count"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q10_10 extends TPCHQuery {

  private val Q57_10_11 = new StructType()
    .add("n_name", "string")
    .add("revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q57_10_11", Q57_10_11)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q11_11 extends TPCHQuery {

  private val Q57_10_11 = new StructType()
    .add("n_name", "string")
    .add("revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q57_10_11", Q57_10_11)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q12_12 extends TPCHQuery {

  private val Q59_12_13 = new StructType()
    .add("revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q59_12_13", Q59_12_13)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q13_13 extends TPCHQuery {

  private val Q59_12_13 = new StructType()
    .add("revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q59_12_13", Q59_12_13)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q14_14 extends TPCHQuery {

  private val Q60_15_14 = new StructType()
    .add("cust_nation", "string")
    .add("revenue", "double")
    .add("supp_nation", "string")
    .add("l_year", "int")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q60_15_14", Q60_15_14)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q15_15 extends TPCHQuery {

  private val Q60_15_14 = new StructType()
    .add("cust_nation", "string")
    .add("revenue", "double")
    .add("supp_nation", "string")
    .add("l_year", "int")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q60_15_14", Q60_15_14)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q16_16 extends TPCHQuery {

  private val Q62_16_17 = new StructType()
    .add("mkt_share", "double")
    .add("o_year", "int")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q62_16_17", Q62_16_17)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q17_17 extends TPCHQuery {

  private val Q62_16_17 = new StructType()
    .add("mkt_share", "double")
    .add("o_year", "int")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q62_16_17", Q62_16_17)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q18_18 extends TPCHQuery {

  private val Q63_19_18 = new StructType()
    .add("sum_profit", "double")
    .add("nation", "string")
    .add("o_year", "int")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q63_19_18", Q63_19_18)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q19_19 extends TPCHQuery {

  private val Q63_19_18 = new StructType()
    .add("sum_profit", "double")
    .add("nation", "string")
    .add("o_year", "int")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q63_19_18", Q63_19_18)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q20_20 extends TPCHQuery {

  private val Q64_20_21 = new StructType()
    .add("c_custkey", "long")
    .add("c_address", "string")
    .add("n_name", "string")
    .add("c_name", "string")
    .add("c_comment", "string")
    .add("revenue", "double")
    .add("c_phone", "string")
    .add("c_acctbal", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q64_20_21", Q64_20_21)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q21_21 extends TPCHQuery {

  private val Q64_20_21 = new StructType()
    .add("c_custkey", "long")
    .add("c_address", "string")
    .add("n_name", "string")
    .add("c_name", "string")
    .add("c_comment", "string")
    .add("revenue", "double")
    .add("c_phone", "string")
    .add("c_acctbal", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q64_20_21", Q64_20_21)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q22_22 extends TPCHQuery {

  private val Q65_22_23 = new StructType()
    .add("ps_partkey", "long")
    .add("product_value", "double")

  private val Q67_22_23 = new StructType()
    .add("small_value", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (loadSharedTable(spark, "Q65_22_23", Q65_22_23))
      .join(loadSharedTable(spark, "Q67_22_23", Q67_22_23), $"product_value" > $"small_value", "cross")
      .select($"ps_partkey", $"product_value")
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q23_23 extends TPCHQuery {

  private val Q65_22_23 = new StructType()
    .add("ps_partkey", "long")
    .add("product_value", "double")

  private val Q67_22_23 = new StructType()
    .add("small_value", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (loadSharedTable(spark, "Q65_22_23", Q65_22_23))
      .join(loadSharedTable(spark, "Q67_22_23", Q67_22_23), $"product_value" > $"small_value", "cross")
      .select($"ps_partkey", $"product_value")
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q24_24 extends TPCHQuery {

  private val Q68_24_25 = new StructType()
    .add("l_shipmode", "string")
    .add("low_line_count", "long")
    .add("high_line_count", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q68_24_25", Q68_24_25)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q25_25 extends TPCHQuery {

  private val Q68_24_25 = new StructType()
    .add("l_shipmode", "string")
    .add("low_line_count", "long")
    .add("high_line_count", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q68_24_25", Q68_24_25)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q26_26 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val custdist = new Count
    val count_not_null = new Count_not_null

    val result = (DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema))
      .join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"c_custkey" === $"o_custkey", "left_outer")
      .groupBy($"c_custkey")
      .agg(
        count_not_null($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(
        custdist(lit(1L)).as("custdist"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q27_27 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val custdist = new Count
    val count_not_null = new Count_not_null

    val result = (DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema))
      .join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"c_custkey" === $"o_custkey", "left_outer")
      .groupBy($"c_custkey")
      .agg(
        count_not_null($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(
        custdist(lit(1L)).as("custdist"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q28_28 extends TPCHQuery {

  private val Q69_28_29 = new StructType()
    .add("promo_revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q69_28_29", Q69_28_29)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q29_29 extends TPCHQuery {

  private val Q69_28_29 = new StructType()
    .add("promo_revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q69_28_29", Q69_28_29)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q30_30 extends TPCHQuery {

  private val Q70_30_31 = new StructType()
    .add("total_revenue", "double")
    .add("s_suppkey", "long")
    .add("s_address", "string")
    .add("s_phone", "string")
    .add("s_name", "string")

  private val Q72_30_31 = new StructType()
    .add("max_revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (loadSharedTable(spark, "Q70_30_31", Q70_30_31))
      .join(loadSharedTable(spark, "Q72_30_31", Q72_30_31), $"total_revenue" >= $"max_revenue", "cross")
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total_revenue")
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q31_31 extends TPCHQuery {

  private val Q70_30_31 = new StructType()
    .add("total_revenue", "double")
    .add("s_suppkey", "long")
    .add("s_address", "string")
    .add("s_phone", "string")
    .add("s_name", "string")

  private val Q72_30_31 = new StructType()
    .add("max_revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (loadSharedTable(spark, "Q70_30_31", Q70_30_31))
      .join(loadSharedTable(spark, "Q72_30_31", Q72_30_31), $"total_revenue" >= $"max_revenue", "cross")
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total_revenue")
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q32_32 extends TPCHQuery {

  private val Q53_33_5_32_4 = new StructType()
    .add("ps_supplycost", "double")
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("ps_suppkey", "long")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")

  private val Q73_33_32 = new StructType()
    .add("s_suppkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val supplier_cnt = new Count

    val result = (loadSharedTable(spark, "Q53_33_5_32_4", Q53_33_5_32_4))
      .join(loadSharedTable(spark, "Q73_33_32", Q73_33_32), $"ps_suppkey" === $"s_suppkey", "left_anti")
      .select($"p_brand", $"p_type", $"p_size", $"ps_suppkey")
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(
        supplier_cnt($"ps_suppkey").as("supplier_cnt"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q33_33 extends TPCHQuery {

  private val Q53_33_5_32_4 = new StructType()
    .add("ps_supplycost", "double")
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("ps_suppkey", "long")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")

  private val Q73_33_32 = new StructType()
    .add("s_suppkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val supplier_cnt = new Count

    val result = (loadSharedTable(spark, "Q53_33_5_32_4", Q53_33_5_32_4))
      .join(loadSharedTable(spark, "Q73_33_32", Q73_33_32), $"ps_suppkey" === $"s_suppkey", "left_anti")
      .select($"p_brand", $"p_type", $"p_size", $"ps_suppkey")
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(
        supplier_cnt($"ps_suppkey").as("supplier_cnt"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q34_34 extends TPCHQuery {

  private val Q74_34_35 = new StructType()
    .add("avg_yearly", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q74_34_35", Q74_34_35)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q35_35 extends TPCHQuery {

  private val Q74_34_35 = new StructType()
    .add("avg_yearly", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q74_34_35", Q74_34_35)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q36_36 extends TPCHQuery {

  private val Q61_15_37_20_21_36_14 = new StructType()
    .add("c_custkey", "long")
    .add("o_orderdate", "date")
    .add("l_shipdate", "date")
    .add("c_address", "string")
    .add("o_orderkey", "long")
    .add("l_extendedprice", "double")
    .add("l_suppkey", "long")
    .add("o_totalprice", "double")
    .add("c_acctbal", "double")
    .add("l_discount", "double")
    .add("c_nationkey", "long")
    .add("c_name", "string")
    .add("l_quantity", "double")
    .add("c_comment", "string")
    .add("c_phone", "string")

  private val Q75_37_36 = new StructType()
    .add("agg_orderkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum2 = new DoubleSum

    val result = (loadSharedTable(spark, "Q61_15_37_20_21_36_14", Q61_15_37_20_21_36_14))
      .join(loadSharedTable(spark, "Q75_37_36", Q75_37_36), $"o_orderkey" === $"agg_orderkey", "left_semi")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(
        doubleSum2($"l_quantity").as("sum_quantity"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q37_37 extends TPCHQuery {

  private val Q61_15_37_20_21_36_14 = new StructType()
    .add("c_custkey", "long")
    .add("o_orderdate", "date")
    .add("l_shipdate", "date")
    .add("c_address", "string")
    .add("o_orderkey", "long")
    .add("l_extendedprice", "double")
    .add("l_suppkey", "long")
    .add("o_totalprice", "double")
    .add("c_acctbal", "double")
    .add("l_discount", "double")
    .add("c_nationkey", "long")
    .add("c_name", "string")
    .add("l_quantity", "double")
    .add("c_comment", "string")
    .add("c_phone", "string")

  private val Q75_37_36 = new StructType()
    .add("agg_orderkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum2 = new DoubleSum

    val result = (loadSharedTable(spark, "Q61_15_37_20_21_36_14", Q61_15_37_20_21_36_14))
      .join(loadSharedTable(spark, "Q75_37_36", Q75_37_36), $"o_orderkey" === $"agg_orderkey", "left_semi")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(
        doubleSum2($"l_quantity").as("sum_quantity"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q38_38 extends TPCHQuery {

  private val Q76_38_39 = new StructType()
    .add("revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q76_38_39", Q76_38_39)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q39_39 extends TPCHQuery {

  private val Q76_38_39 = new StructType()
    .add("revenue", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q76_38_39", Q76_38_39)
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q40_40 extends TPCHQuery {

  private val Q77_40_41 = new StructType()
    .add("ps_partkey", "long")
    .add("ps_suppkey", "long")

  private val Q54_33_19_16_34_5_17_32_35_18_4_40_41 = new StructType()
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")

  private val Q47_0_1_22_40_23_41 = new StructType()
    .add("n_nationkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = ((DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema))
      .join((loadSharedTable(spark, "Q77_40_41", Q77_40_41))
        .join(loadSharedTable(spark, "Q54_33_19_16_34_5_17_32_35_18_4_40_41", Q54_33_19_16_34_5_17_32_35_18_4_40_41), $"ps_partkey" === $"p_partkey", "left_semi")
        .select($"ps_suppkey"), $"s_suppkey" === $"ps_suppkey", "left_semi"))
      .join(loadSharedTable(spark, "Q47_0_1_22_40_23_41", Q47_0_1_22_40_23_41), $"s_nationkey" === $"n_nationkey", "inner")
      .select($"s_name", $"s_address")
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q41_41 extends TPCHQuery {

  private val Q77_40_41 = new StructType()
    .add("ps_partkey", "long")
    .add("ps_suppkey", "long")

  private val Q54_33_19_16_34_5_17_32_35_18_4_40_41 = new StructType()
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")

  private val Q47_0_1_22_40_23_41 = new StructType()
    .add("n_nationkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = ((DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema))
      .join((loadSharedTable(spark, "Q77_40_41", Q77_40_41))
        .join(loadSharedTable(spark, "Q54_33_19_16_34_5_17_32_35_18_4_40_41", Q54_33_19_16_34_5_17_32_35_18_4_40_41), $"ps_partkey" === $"p_partkey", "left_semi")
        .select($"ps_suppkey"), $"s_suppkey" === $"ps_suppkey", "left_semi"))
      .join(loadSharedTable(spark, "Q47_0_1_22_40_23_41", Q47_0_1_22_40_23_41), $"s_nationkey" === $"n_nationkey", "inner")
      .select($"s_name", $"s_address")
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q42_42 extends TPCHQuery {

  private val Q78_42_43 = new StructType()
    .add("c_custkey", "long")
    .add("c_phone", "string")
    .add("c_acctbal", "double")

  private val Q79_42_43 = new StructType()
    .add("avg_acctbal", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum = new DoubleSum
    val numcust = new Count

    val result = ((loadSharedTable(spark, "Q78_42_43", Q78_42_43))
      .join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"c_custkey" === $"o_custkey", "left_anti"))
      .join(loadSharedTable(spark, "Q79_42_43", Q79_42_43), $"c_acctbal" > $"avg_acctbal", "cross")
      .select(substring($"c_phone", 1, 2).as("cntrycode"), $"c_acctbal")
      .groupBy($"cntrycode")
      .agg(
        numcust(lit(1L)).as("numcust"),
        doubleSum($"c_acctbal").as("totalacctbal"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q43_43 extends TPCHQuery {

  private val Q78_42_43 = new StructType()
    .add("c_custkey", "long")
    .add("c_phone", "string")
    .add("c_acctbal", "double")

  private val Q79_42_43 = new StructType()
    .add("avg_acctbal", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum = new DoubleSum
    val numcust = new Count

    val result = ((loadSharedTable(spark, "Q78_42_43", Q78_42_43))
      .join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"c_custkey" === $"o_custkey", "left_anti"))
      .join(loadSharedTable(spark, "Q79_42_43", Q79_42_43), $"c_acctbal" > $"avg_acctbal", "cross")
      .select(substring($"c_phone", 1, 2).as("cntrycode"), $"c_acctbal")
      .groupBy($"cntrycode")
      .agg(
        numcust(lit(1L)).as("numcust"),
        doubleSum($"c_acctbal").as("totalacctbal"))
      .select("*")
    DataUtils.writeToSinkWithExtraOptions(
      result, query_name, uid, numBatch, constraint)

  }
}







private class Q44_0_1 extends TPCHQuery {

  private val Q45_0_15_1_37_20_24_21_36_25_14 = new StructType()
    .add("o_orderdate", "date")
    .add("l_shipdate", "date")
    .add("o_orderkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("o_totalprice", "double")
    .add("o_orderpriority", "string")
    .add("l_orderkey", "long")
    .add("o_custkey", "long")
    .add("l_discount", "double")

  private val Q47_0_1_22_40_23_41 = new StructType()
    .add("n_nationkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = ((loadSharedTable(spark, "Q45_0_15_1_37_20_24_21_36_25_14", Q45_0_15_1_37_20_24_21_36_25_14))
      .join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"l_suppkey" === $"s_suppkey", "inner"))
      .join(loadSharedTable(spark, "Q47_0_1_22_40_23_41", Q47_0_1_22_40_23_41), $"s_nationkey" === $"n_nationkey", "inner")
      .select($"l_suppkey", $"l_orderkey", $"s_name")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q44_0_1", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q45_0_15_1_37_20_24_21_36_25_14 extends TPCHQuery {

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8))
      .join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema)
        .filter($"o_orderdate" between ("1994-05-01", "1994-10-01"))
        .filter($"o_orderdate" between ("1994-05-01", "1994-10-01"))
        .filter($"o_orderstatus" === "F"), $"l_orderkey" === $"o_orderkey", "inner")
      .select($"o_orderdate", $"l_shipdate", $"o_orderkey", $"l_extendedprice", $"l_shipmode", $"l_quantity", $"l_suppkey", $"o_totalprice", $"o_orderpriority", $"l_orderkey", $"o_custkey", $"l_discount")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q45_0_15_1_37_20_24_21_36_25_14", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
      .filter($"l_shipdate" > "1998-03-15")
      .filter($"l_shipdate" > "1998-03-15")
      .filter(($"l_shipdate" between ("1994-01-01", "1995-01-01")) and ($"l_discount" between (0.05, 0.07)) and ($"l_quantity" < 24.0))
      .filter(($"l_shipdate" between ("1994-01-01", "1995-01-01")) and ($"l_discount" between (0.05, 0.07)) and ($"l_quantity" < 24.0))
      .filter($"l_shipdate" between ("1995-01-01", "1995-06-31"))
      .filter($"l_shipdate" between ("1995-01-01", "1995-06-31"))
      .filter(($"l_returnflag" === "R") and ($"l_shipdate" between ("1994-02-01", "1994-10-01")))
      .filter(($"l_returnflag" === "R") and ($"l_shipdate" between ("1994-02-01", "1994-10-01")))
      .filter(($"l_shipmode" === "MAIL") and ($"l_receiptdate" > $"l_commitdate") and ($"l_shipdate" < $"l_commitdate") and ($"l_receiptdate" === "1994-01-01"))
      .filter(($"l_shipmode" === "MAIL") and ($"l_receiptdate" > $"l_commitdate") and ($"l_shipdate" < $"l_commitdate") and ($"l_receiptdate" === "1994-01-01"))
      .filter($"l_shipdate" between ("1994-09-01", "1994-10-01"))
      .filter($"l_shipdate" between ("1994-09-01", "1994-10-01"))
      .filter($"l_shipdate" between ("1995-01-01", "1995-04-01"))
      .filter($"l_shipdate" between ("1995-01-01", "1995-04-01"))
      .filter($"l_shipdate" between ("1994-01-01", "1994-06-31"))
      .filter($"l_shipdate" between ("1994-01-01", "1994-06-31"))
      .filter(($"l_shipinstruct" === "DELIVER IN PERSON") and ($"l_shipmode" isin ("AIR", "AIR REG")))
      .filter(($"l_shipinstruct" === "DELIVER IN PERSON") and ($"l_shipmode" isin ("AIR", "AIR REG")))
      .filter(($"l_receiptdate" > $"l_commitdate") and ($"l_shipdate" between ("1994-07-01", "1994-10-01")))
      .select($"l_shipdate", $"l_partkey", $"l_extendedprice", $"l_shipmode", $"l_quantity", $"l_suppkey", $"l_orderkey", $"l_discount")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q47_0_1_22_40_23_41 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
      .filter($"n_name" === "GERMANY")
      .filter($"n_name" === "GERMANY")
      .filter($"n_name" === "CANADA")
      .filter($"n_name" === "CANADA")
      .filter($"n_name" === "SAUDI ARABIA")
      .select($"n_nationkey")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q47_0_1_22_40_23_41", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q48_0_1 extends TPCHQuery {

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8)
      .select($"l_orderkey".as("l2_orderkey"), $"l_suppkey".as("l2_suppkey"))
      .select($"l2_orderkey", $"l2_suppkey")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q48_0_1", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q49_0_1 extends TPCHQuery {

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8)
      .select($"l_orderkey".as("l3_orderkey"), $"l_suppkey".as("l3_suppkey"))
      .select($"l3_orderkey", $"l3_suppkey")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q49_0_1", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q50_2_3 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val sum_base_price = new DoubleSum
    val avg_disc = new DoubleAvg
    val sum_qty = new DoubleSum
    val sum_disc_price = new Sum_disc_price
    val avg_qty = new DoubleAvg
    val count_order = new Count
    val avg_price = new DoubleAvg
    val sum_charge = new Sum_disc_price_with_tax

    val result = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
      .filter($"l_shipdate" <= "1998-09-01")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum_qty($"l_quantity").as("sum_qty"),
        sum_base_price($"l_extendedprice" * $"l_discount").as("sum_base_price"),
        sum_disc_price($"l_extendedprice", $"l_discount").as("sum_disc_price"),
        sum_charge($"l_extendedprice", $"l_discount", $"l_tax").as("sum_charge"),
        avg_qty($"l_quantity").as("avg_qty"),
        avg_price($"l_extendedprice").as("avg_price"),
        avg_disc($"l_discount").as("avg_disc"),
        count_order(lit(1L)).as("count_order"))
      .select($"avg_disc", $"sum_disc_price", $"sum_qty", $"sum_charge", $"l_linestatus", $"avg_price", $"sum_base_price", $"avg_qty", $"count_order", $"l_returnflag")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q50_2_3", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q51_5_4 extends TPCHQuery {

  private val Q52_5_10_4_11 = new StructType()
    .add("n_name", "string")
    .add("s_acctbal", "double")
    .add("s_suppkey", "long")
    .add("s_nationkey", "long")
    .add("s_address", "string")
    .add("s_phone", "string")
    .add("s_comment", "string")
    .add("s_name", "string")

  private val Q53_33_5_32_4 = new StructType()
    .add("ps_supplycost", "double")
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("ps_suppkey", "long")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = ((loadSharedTable(spark, "Q52_5_10_4_11", Q52_5_10_4_11))
      .join(loadSharedTable(spark, "Q53_33_5_32_4", Q53_33_5_32_4), $"s_suppkey" === $"ps_suppkey", "inner"))
      .join((loadSharedTable(spark, "Q52_5_10_4_11", Q52_5_10_4_11))
        .join(DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema), $"s_suppkey" === $"ps_suppkey", "inner")
        .groupBy($"ps_partkey")
        .agg(
          min($"ps_supplycost").as("min_supplycost"))
        .select($"ps_partkey".as("min_partkey"), $"min_supplycost"), ($"p_partkey" === $"min_partkey") and  ($"ps_supplycost" === $"min_supplycost"), "inner")
      .select($"s_acctbal", $"s_name", $"n_name", $"p_partkey", $"p_mfgr", $"s_address", $"s_phone", $"s_comment")
      .select($"n_name", $"s_acctbal", $"p_partkey", $"s_address", $"s_phone", $"p_mfgr", $"s_comment", $"s_name")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q51_5_4", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q52_5_10_4_11 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = ((DataUtils.loadStreamTable(spark, "region", "r", tpchSchema)
      .filter($"r_name" === "ASIA")
      .filter($"r_name" === "ASIA")
      .filter($"r_name" === "EUROPE"))
      .join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema), $"r_regionkey" === $"n_regionkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"n_nationkey" === $"s_nationkey", "inner")
      .select($"n_name", $"s_acctbal", $"s_suppkey", $"s_nationkey", $"s_address", $"s_phone", $"s_comment", $"s_name")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q52_5_10_4_11", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q53_33_5_32_4 extends TPCHQuery {

  private val Q54_33_19_16_34_5_17_32_35_18_4_40_41 = new StructType()
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema))
      .join(loadSharedTable(spark, "Q54_33_19_16_34_5_17_32_35_18_4_40_41", Q54_33_19_16_34_5_17_32_35_18_4_40_41), $"ps_partkey" === $"p_partkey", "inner")
      .select($"ps_supplycost", $"p_partkey", $"p_size", $"ps_suppkey", $"p_mfgr", $"p_type", $"p_brand")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q53_33_5_32_4", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q54_33_19_16_34_5_17_32_35_18_4_40_41 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
      .filter($"p_type" === "ECONOMY ANODIZED STEEL")
      .filter($"p_type" === "ECONOMY ANODIZED STEEL")
      .filter($"p_name" like ("%green%"))
      .filter($"p_name" like ("%green%"))
      .filter(($"p_brand" =!= "Brand#45") and ($"p_size" isin (49, 15, 9)))
      .filter(($"p_brand" =!= "Brand#45") and ($"p_size" isin (49, 15, 9)))
      .filter($"p_brand" === "Brand#23" and $"p_container" === "MED BOX")
      .filter($"p_brand" === "Brand#23" and $"p_container" === "MED BOX")
      .filter($"p_name" like ("forest%"))
      .filter($"p_name" like ("forest%"))
      .filter(($"p_size" === 15) and ($"p_type" like ("%BRASS")))
      .select($"p_partkey", $"p_size", $"p_mfgr", $"p_type", $"p_brand")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q54_33_19_16_34_5_17_32_35_18_4_40_41", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q55_6_7 extends TPCHQuery {

  private val Q56_9_16_17_6_10_7_11_8 = new StructType()
    .add("o_orderdate", "date")
    .add("o_orderkey", "long")
    .add("o_shippriority", "int")
    .add("o_orderpriority", "string")
    .add("o_custkey", "long")

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val result = ((DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema)
      .filter($"c_mktsegment" === "BUILDING"))
      .join(loadSharedTable(spark, "Q56_9_16_17_6_10_7_11_8", Q56_9_16_17_6_10_7_11_8), $"c_custkey" === $"o_custkey", "inner"))
      .join(loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8), $"o_orderkey" === $"l_orderkey", "inner")
      .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(
        sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))
      .select($"l_orderkey", $"revenue", $"o_orderdate", $"o_shippriority")
      .select($"o_orderdate", $"o_shippriority", $"revenue", $"l_orderkey")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q55_6_7", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q56_9_16_17_6_10_7_11_8 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema)
      .filter($"o_orderdate" between ("1993-07-01", "1993-10-01"))
      .filter($"o_orderdate" between ("1993-07-01", "1993-10-01"))
      .filter($"o_orderdate" between ("1994-01-01", "1994-09-01"))
      .filter($"o_orderdate" between ("1994-01-01", "1994-09-01"))
      .filter($"o_orderdate" between ("1994-01-01", "1994-08-31"))
      .filter($"o_orderdate" between ("1994-01-01", "1994-08-31"))
      .filter($"o_orderdate" < "1992-03-15")
      .select($"o_orderdate", $"o_orderkey", $"o_shippriority", $"o_orderpriority", $"o_custkey")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q56_9_16_17_6_10_7_11_8", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q57_10_11 extends TPCHQuery {

  private val Q52_5_10_4_11 = new StructType()
    .add("n_name", "string")
    .add("s_acctbal", "double")
    .add("s_suppkey", "long")
    .add("s_nationkey", "long")
    .add("s_address", "string")
    .add("s_phone", "string")
    .add("s_comment", "string")
    .add("s_name", "string")

  private val Q58_16_17_10_11 = new StructType()
    .add("o_orderdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_suppkey", "long")
    .add("o_custkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val result = (loadSharedTable(spark, "Q52_5_10_4_11", Q52_5_10_4_11))
      .join((loadSharedTable(spark, "Q58_16_17_10_11", Q58_16_17_10_11))
        .join(DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema), $"o_custkey" === $"c_custkey", "inner"), $"s_nationkey" === $"c_nationkey" and  $"s_suppkey" === $"l_suppkey", "inner")
      .groupBy($"n_name")
      .agg(
        sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))
      .select($"n_name", $"revenue")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q57_10_11", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q58_16_17_10_11 extends TPCHQuery {

  private val Q56_9_16_17_6_10_7_11_8 = new StructType()
    .add("o_orderdate", "date")
    .add("o_orderkey", "long")
    .add("o_shippriority", "int")
    .add("o_orderpriority", "string")
    .add("o_custkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema))
      .join(loadSharedTable(spark, "Q56_9_16_17_6_10_7_11_8", Q56_9_16_17_6_10_7_11_8), $"l_orderkey" === $"o_orderkey", "inner")
      .select($"o_orderdate", $"l_partkey", $"l_extendedprice", $"l_suppkey", $"o_custkey", $"l_discount")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q58_16_17_10_11", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q59_12_13 extends TPCHQuery {

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum = new DoubleSum

    val result = loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8)
      .agg(
        doubleSum($"l_extendedprice" * $"l_discount").as("revenue"))
      .select($"revenue")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q59_12_13", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q60_15_14 extends TPCHQuery {

  private val Q61_15_37_20_21_36_14 = new StructType()
    .add("c_custkey", "long")
    .add("o_orderdate", "date")
    .add("l_shipdate", "date")
    .add("c_address", "string")
    .add("o_orderkey", "long")
    .add("l_extendedprice", "double")
    .add("l_suppkey", "long")
    .add("o_totalprice", "double")
    .add("c_acctbal", "double")
    .add("l_discount", "double")
    .add("c_nationkey", "long")
    .add("c_name", "string")
    .add("l_quantity", "double")
    .add("c_comment", "string")
    .add("c_phone", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val result = (((loadSharedTable(spark, "Q61_15_37_20_21_36_14", Q61_15_37_20_21_36_14))
      .join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"l_suppkey" === $"s_suppkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
        .select($"n_name".as("supp_nation"), $"n_nationkey".as("n1_nationkey")), $"s_nationkey" === $"n1_nationkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
        .select($"n_name".as("cust_nation"), $"n_nationkey".as("n2_nationkey")), $"c_nationkey" === $"n2_nationkey", "inner")
      .filter(($"supp_nation" === "FRANCE" and $"cust_nation" === "GERMANY") or ($"supp_nation" === "GERMANY" and $"cust_nation" === "FRANCE"))
      .select($"supp_nation", $"cust_nation", year($"l_shipdate").as("l_year"), $"l_extendedprice", $"l_discount")
      .groupBy($"supp_nation", $"cust_nation", $"l_year")
      .agg(
        sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))
      .select($"cust_nation", $"revenue", $"supp_nation", $"l_year")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q60_15_14", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q61_15_37_20_21_36_14 extends TPCHQuery {

  private val Q45_0_15_1_37_20_24_21_36_25_14 = new StructType()
    .add("o_orderdate", "date")
    .add("l_shipdate", "date")
    .add("o_orderkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("o_totalprice", "double")
    .add("o_orderpriority", "string")
    .add("l_orderkey", "long")
    .add("o_custkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (loadSharedTable(spark, "Q45_0_15_1_37_20_24_21_36_25_14", Q45_0_15_1_37_20_24_21_36_25_14))
      .join(DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema), $"o_custkey" === $"c_custkey", "inner")
      .select($"c_custkey", $"o_orderdate", $"l_shipdate", $"c_address", $"o_orderkey", $"l_extendedprice", $"l_suppkey", $"o_totalprice", $"c_acctbal", $"l_discount", $"c_nationkey", $"c_name", $"l_quantity", $"c_comment", $"c_phone")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q61_15_37_20_21_36_14", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q62_16_17 extends TPCHQuery {

  private val Q58_16_17_10_11 = new StructType()
    .add("o_orderdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_suppkey", "long")
    .add("o_custkey", "long")
    .add("l_discount", "double")

  private val Q54_33_19_16_34_5_17_32_35_18_4_40_41 = new StructType()
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val udaf_q8 = new UDAF_Q8

    val result = ((((((loadSharedTable(spark, "Q58_16_17_10_11", Q58_16_17_10_11))
      .join(loadSharedTable(spark, "Q54_33_19_16_34_5_17_32_35_18_4_40_41", Q54_33_19_16_34_5_17_32_35_18_4_40_41), $"l_partkey" === $"p_partkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"l_suppkey" === $"s_suppkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema), $"o_custkey" === $"c_custkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
        .select($"n_regionkey".as("n1_regionkey"), $"n_nationkey".as("n1_nationkey")), $"c_nationkey" === $"n1_nationkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "region", "r", tpchSchema)
        .filter($"r_name" === "AMERICA"), $"n1_regionkey" === $"r_regionkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
        .select($"n_name".as("n2_name"), $"n_nationkey".as("n2_nationkey")), $"s_nationkey" === $"n2_nationkey", "inner")
      .select(year($"o_orderdate").as("o_year"), ($"l_extendedprice" * ($"l_discount" - 1) * -1).as("volume"), $"n2_name")
      .groupBy($"o_year")
      .agg(
        udaf_q8($"n2_name", $"volume").as("mkt_share"))
      .select($"mkt_share", $"o_year")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q62_16_17", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q63_19_18 extends TPCHQuery {

  private val Q54_33_19_16_34_5_17_32_35_18_4_40_41 = new StructType()
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum = new DoubleSum

    val result = (((((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema))
      .join(loadSharedTable(spark, "Q54_33_19_16_34_5_17_32_35_18_4_40_41", Q54_33_19_16_34_5_17_32_35_18_4_40_41), $"l_partkey" === $"p_partkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"l_orderkey" === $"o_orderkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema), $"l_partkey" === $"ps_partkey" and  $"l_suppkey" === $"ps_suppkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"l_suppkey" === $"s_suppkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema), $"s_nationkey" === $"n_nationkey", "inner")
      .select($"n_name".as("nation"), year($"o_orderdate").as("o_year"), (($"l_extendedprice" * ($"l_discount" - 1) * -1) - $"ps_supplycost" * $"l_quantity").as("amount"))
      .groupBy($"nation", $"o_year")
      .agg(
        doubleSum($"amount").as("sum_profit"))
      .select($"sum_profit", $"nation", $"o_year")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q63_19_18", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q64_20_21 extends TPCHQuery {

  private val Q61_15_37_20_21_36_14 = new StructType()
    .add("c_custkey", "long")
    .add("o_orderdate", "date")
    .add("l_shipdate", "date")
    .add("c_address", "string")
    .add("o_orderkey", "long")
    .add("l_extendedprice", "double")
    .add("l_suppkey", "long")
    .add("o_totalprice", "double")
    .add("c_acctbal", "double")
    .add("l_discount", "double")
    .add("c_nationkey", "long")
    .add("c_name", "string")
    .add("l_quantity", "double")
    .add("c_comment", "string")
    .add("c_phone", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val revenue = new Sum_disc_price

    val result = (loadSharedTable(spark, "Q61_15_37_20_21_36_14", Q61_15_37_20_21_36_14))
      .join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema), $"c_nationkey" === $"n_nationkey", "inner")
      .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(
        revenue($"l_extendedprice", $"l_discount").as("revenue"))
      .select($"c_custkey", $"c_address", $"n_name", $"c_name", $"c_comment", $"revenue", $"c_phone", $"c_acctbal")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q64_20_21", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q65_22_23 extends TPCHQuery {

  private val Q66_22_23 = new StructType()
    .add("ps_supplycost", "double")
    .add("ps_partkey", "long")
    .add("ps_availqty", "int")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum = new DoubleSum

    val result = loadSharedTable(spark, "Q66_22_23", Q66_22_23)
      .groupBy($"ps_partkey")
      .agg(
        doubleSum($"ps_supplycost" * $"ps_availqty").as("product_value"))
      .select($"ps_partkey", $"product_value")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q65_22_23", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q66_22_23 extends TPCHQuery {

  private val Q47_0_1_22_40_23_41 = new StructType()
    .add("n_nationkey", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = ((DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema))
      .join(loadSharedTable(spark, "Q47_0_1_22_40_23_41", Q47_0_1_22_40_23_41), $"s_nationkey" === $"n_nationkey", "inner"))
      .join(DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema), $"s_suppkey" === $"ps_suppkey", "inner")
      .select($"ps_supplycost", $"ps_partkey", $"ps_availqty")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q66_22_23", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q67_22_23 extends TPCHQuery {

  private val Q66_22_23 = new StructType()
    .add("ps_supplycost", "double")
    .add("ps_partkey", "long")
    .add("ps_availqty", "int")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum = new DoubleSum

    val result = loadSharedTable(spark, "Q66_22_23", Q66_22_23)
      .agg(
        doubleSum($"ps_supplycost" * $"ps_availqty" * 0.0001/SF).as("small_value"))
      .select($"small_value")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q67_22_23", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q68_24_25 extends TPCHQuery {

  private val Q45_0_15_1_37_20_24_21_36_25_14 = new StructType()
    .add("o_orderdate", "date")
    .add("l_shipdate", "date")
    .add("o_orderkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("o_totalprice", "double")
    .add("o_orderpriority", "string")
    .add("l_orderkey", "long")
    .add("o_custkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val udaf_q12_low = new UDAF_Q12_LOW
    val udaf_q12_high = new UDAF_Q12_HIGH

    val result = loadSharedTable(spark, "Q45_0_15_1_37_20_24_21_36_25_14", Q45_0_15_1_37_20_24_21_36_25_14)
      .groupBy($"l_shipmode")
      .agg(
        udaf_q12_high($"o_orderpriority").as("high_line_count"),
        udaf_q12_low($"o_orderpriority").as("low_line_count"))
      .select($"l_shipmode", $"low_line_count", $"high_line_count")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q68_24_25", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q69_28_29 extends TPCHQuery {

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val udaf_q14 = new UDAF_Q14
    val sum_disc_price = new Sum_disc_price

    val result = (loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8))
      .join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema), $"l_partkey" === $"p_partkey", "inner")
      .agg(
        ((udaf_q14($"p_type", $"l_extendedprice", $"l_discount")/sum_disc_price($"l_extendedprice", $"l_discount")) * 100).as("promo_revenue"))
      .select($"promo_revenue")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q69_28_29", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q70_30_31 extends TPCHQuery {

  private val Q71_30_31 = new StructType()
    .add("total_revenue", "double")
    .add("supplier_no", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = (DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema))
      .join(loadSharedTable(spark, "Q71_30_31", Q71_30_31), $"s_suppkey" === $"supplier_no", "inner")
      .select($"total_revenue", $"s_suppkey", $"s_address", $"s_phone", $"s_name")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q70_30_31", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q71_30_31 extends TPCHQuery {

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val result = loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8)
      .groupBy($"l_suppkey")
      .agg(
        sum_disc_price($"l_extendedprice", $"l_discount").as("total_revenue"))
      .select($"l_suppkey".as("supplier_no"), $"total_revenue")
      .select($"total_revenue", $"supplier_no")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q71_30_31", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q72_30_31 extends TPCHQuery {

  private val Q71_30_31 = new StructType()
    .add("total_revenue", "double")
    .add("supplier_no", "long")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = loadSharedTable(spark, "Q71_30_31", Q71_30_31)
      .agg(
        max($"total_revenue").as("max_revenue"))
      .select($"max_revenue")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q72_30_31", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q73_33_32 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema)
      .filter($"s_comment" like ("%Customer%Complaints%"))
      .select($"s_suppkey")
      .select($"s_suppkey")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q73_33_32", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q74_34_35 extends TPCHQuery {

  private val Q54_33_19_16_34_5_17_32_35_18_4_40_41 = new StructType()
    .add("p_partkey", "long")
    .add("p_size", "int")
    .add("p_mfgr", "string")
    .add("p_type", "string")
    .add("p_brand", "string")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val doubleSum = new DoubleSum

    val result = ((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema))
      .join(DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
        .groupBy($"l_partkey")
        .agg(
          (doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
        .select($"l_partkey".as("agg_l_partkey"), $"avg_quantity"), $"l_partkey" === $"agg_l_partkey" and  $"l_quantity" < $"avg_quantity", "inner"))
      .join(loadSharedTable(spark, "Q54_33_19_16_34_5_17_32_35_18_4_40_41", Q54_33_19_16_34_5_17_32_35_18_4_40_41), $"l_partkey" === $"p_partkey", "inner")
      .agg(
        (doubleSum($"l_extendedprice") / 7.0).as("avg_yearly"))
      .select($"avg_yearly")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q74_34_35", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q75_37_36 extends TPCHQuery {

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum1 = new DoubleSum

    val result = loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8)
      .groupBy($"l_orderkey")
      .agg(
        doubleSum1($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("agg_orderkey"))
      .select($"agg_orderkey")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q75_37_36", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q76_38_39 extends TPCHQuery {

  private val Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8 = new StructType()
    .add("l_shipdate", "date")
    .add("l_partkey", "long")
    .add("l_extendedprice", "double")
    .add("l_shipmode", "string")
    .add("l_quantity", "double")
    .add("l_suppkey", "long")
    .add("l_orderkey", "long")
    .add("l_discount", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val result = (loadSharedTable(spark, "Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8", Q46_0_15_30_9_1_37_31_38_24_39_25_12_13_20_6_21_36_28_7_14_29_8))
      .join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema), $"l_partkey" === $"p_partkey" and  ((($"p_brand" === "Brand#12") and
        ($"p_container" isin("SM CASE", "SM BOX", "SM PACK", "SM PKG")) and
        ($"l_quantity" >= 1 and $"l_quantity" <= 11) and
        ($"p_size" between(1, 5))
        )
        or (($"p_brand" === "Brand#23") and
        ($"p_container" isin("MED BAG", "MED BOX", "MED PKG", "MED PACK")) and
        ($"l_quantity" >= 10 and $"l_quantity" <= 20) and
        ($"p_size" between(1, 10))
        )
        or (($"p_brand" === "Brand#34") and
        ($"p_container" isin("LG CASE", "LG BOX", "LG PACK", "LG PKG")) and
        ($"l_quantity" >= 20 and $"l_quantity" <= 30) and
        ($"p_size" between(1, 15)))), "inner")
      .agg(
        sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))
      .select($"revenue")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q76_38_39", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q77_40_41 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleSum = new DoubleSum

    val result = (DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema))
      .join(DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
        .filter($"l_shipdate" between ("1994-01-01", "1994-06-31"))
        .groupBy($"l_partkey", $"l_suppkey")
        .agg(
          (doubleSum($"l_quantity") * 0.5).as("agg_l_sum"))
        .select($"l_partkey".as("agg_l_partkey"), $"l_suppkey".as("agg_l_suppkey"), $"agg_l_sum"), $"ps_partkey" === $"agg_l_partkey" and  $"ps_suppkey" === $"agg_l_suppkey" and $"ps_availqty" > $"agg_l_sum", "inner")
      .select($"ps_partkey", $"ps_suppkey")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q77_40_41", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q78_42_43 extends TPCHQuery {



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._


    val result = DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema)
      .filter(($"c_acctbal" > 0.0) and (substring($"c_phone", 1, 2) isin ("13", "31")))
      .select($"c_custkey", $"c_phone", $"c_acctbal")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q78_42_43", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}







private class Q79_42_43 extends TPCHQuery {

  private val Q78_42_43 = new StructType()
    .add("c_custkey", "long")
    .add("c_phone", "string")
    .add("c_acctbal", "double")



  override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

    import spark.implicits._

    val doubleAvg = new DoubleAvg

    val result = loadSharedTable(spark, "Q78_42_43", Q78_42_43)
      .agg(
        doubleAvg($"c_acctbal").as("avg_acctbal"))
      .select($"avg_acctbal")
    DataUtils.writeToKafkaWithExtraOptions(
      result, "Q79_42_43", query_name, uid,
      numBatch, constraint, tpchSchema.checkpointLocation)

  }
}
