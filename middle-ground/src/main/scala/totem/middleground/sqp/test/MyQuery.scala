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


private class Q0_17 extends TPCHQuery {

private val Q22_17 = new StructType()
.add("l_partkey", "long")
.add("avg_quantity", "double")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum = new DoubleSum

val result = ((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema))
.join(loadSharedTable(spark, "Q22_17", Q22_17)
.select($"l_partkey".as("agg_l_partkey"), $"avg_quantity"), $"l_partkey" === $"agg_l_partkey" and  $"l_quantity" < $"avg_quantity", "inner"))
.join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
.filter($"p_brand" === "Brand#23" and $"p_container" === "MED BOX"), $"l_partkey" === $"p_partkey", "inner")
.agg(
(doubleSum($"l_extendedprice") / 7.0).as("avg_yearly"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q1_8 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val udaf_q8 = new UDAF_Q8

val result = (((((((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema)
.filter($"o_orderdate" between ("1994-01-01", "1994-08-31")), $"l_orderkey" === $"o_orderkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
.filter($"p_type" === "ECONOMY ANODIZED STEEL"), $"l_partkey" === $"p_partkey", "inner"))
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
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q2_11 extends TPCHQuery {

private val Q23_11 = new StructType()
.add("ps_partkey", "long")
.add("product_value", "double")

private val Q24_11 = new StructType()
.add("small_value", "double")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._


val result = (loadSharedTable(spark, "Q23_11", Q23_11))
.join(loadSharedTable(spark, "Q24_11", Q24_11), $"product_value" > $"small_value", "cross")
.select($"ps_partkey", $"product_value")
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q3_20 extends TPCHQuery {

private val Q25_20 = new StructType()
.add("l_partkey", "long")
.add("l_suppkey", "long")
.add("agg_l_sum", "double")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._


val result = ((DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema))
.join(((DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema))
.join(loadSharedTable(spark, "Q25_20", Q25_20)
.select($"l_partkey".as("agg_l_partkey"), $"l_suppkey".as("agg_l_suppkey"), $"agg_l_sum"), $"ps_partkey" === $"agg_l_partkey" and  $"ps_suppkey" === $"agg_l_suppkey" and $"ps_availqty" > $"agg_l_sum", "inner"))
.join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
.filter($"p_name" like ("forest%")), $"ps_partkey" === $"p_partkey", "left_semi")
.select($"ps_suppkey"), $"s_suppkey" === $"ps_suppkey", "left_semi"))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
.filter($"n_name" === "CANADA"), $"s_nationkey" === $"n_nationkey", "inner")
.select($"s_name", $"s_address")
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q4_2 extends TPCHQuery {

private val Q26_2 = new StructType()
.add("min_supplycost", "double")
.add("min_partkey", "double")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._


val result = ((((DataUtils.loadStreamTable(spark, "region", "r", tpchSchema)
.filter($"r_name" === "EUROPE"))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema), $"r_regionkey" === $"n_regionkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"n_nationkey" === $"s_nationkey", "inner"))
.join((DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema))
.join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
.filter(($"p_size" === 15) and ($"p_type" like ("%BRASS"))), $"ps_partkey" === $"p_partkey", "inner"), $"s_suppkey" === $"ps_suppkey", "inner"))
.join(loadSharedTable(spark, "Q26_2", Q26_2), ($"p_partkey" === $"min_partkey") and  ($"ps_supplycost" === $"min_supplycost"), "inner")
.select($"s_acctbal", $"s_name", $"n_name", $"p_partkey", $"p_mfgr", $"s_address", $"s_phone", $"s_comment")
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q5_5 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val sum_disc_price = new Sum_disc_price

val result = (((DataUtils.loadStreamTable(spark, "region", "r", tpchSchema)
.filter($"r_name" === "ASIA"))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema), $"r_regionkey" === $"n_regionkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"n_nationkey" === $"s_nationkey", "inner"))
.join(((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema)
.filter($"o_orderdate" between ("1994-01-01", "1994-09-01")), $"l_orderkey" === $"o_orderkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema), $"o_custkey" === $"c_custkey", "inner"), $"s_suppkey" === $"l_suppkey" and  $"s_nationkey" === $"c_nationkey", "inner")
.groupBy($"n_name")
.agg(
sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q6_14 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val udaf_q14 = new UDAF_Q14
val sum_disc_price = new Sum_disc_price

val result = (DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter($"l_shipdate" between ("1994-09-01", "1994-10-01")))
.join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema), $"l_partkey" === $"p_partkey", "inner")
.agg(
((udaf_q14($"p_type", $"l_extendedprice", $"l_discount")/sum_disc_price($"l_extendedprice", $"l_discount")) * 100).as("promo_revenue"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q7_4 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val order_count = new Count

val result = (DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema)
.filter($"o_orderdate" between ("1993-07-01", "1993-10-01")))
.join(DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter(($"l_receiptdate" > $"l_commitdate") and ($"l_shipdate" between ("1994-07-01", "1994-10-01"))), $"o_orderkey" === $"l_orderkey", "left_semi")
.groupBy($"o_orderpriority")
.agg(
order_count(lit(1L)).as("order_count"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q8_13 extends TPCHQuery {

private val Q27_13 = new StructType()
.add("c_count", "double")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val custdist = new Count

val result = loadSharedTable(spark, "Q27_13", Q27_13)
.groupBy($"c_count")
.agg(
custdist(lit(1L)).as("custdist"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q9_22 extends TPCHQuery {

private val Q28_22 = new StructType()
.add("avg_acctbal", "double")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum = new DoubleSum
val numcust = new Count

val result = ((DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema)
.filter(($"c_acctbal" > 0.0) and (substring($"c_phone", 1, 2) isin ("13", "31"))))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"c_custkey" === $"o_custkey", "left_anti"))
.join(loadSharedTable(spark, "Q28_22", Q28_22), $"c_acctbal" > $"avg_acctbal", "cross")
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







private class Q10_16 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val supplier_cnt = new Count

val result = ((DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema))
.join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
.filter(($"p_brand" =!= "Brand#45") and ($"p_size" isin (49, 15, 9))), $"ps_partkey" === $"p_partkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema)
.filter($"s_comment" like ("%Customer%Complaints%"))
.select($"s_suppkey"), $"ps_suppkey" === $"s_suppkey", "left_anti")
.select($"p_brand", $"p_type", $"p_size", $"ps_suppkey")
.groupBy($"p_brand", $"p_type", $"p_size")
.agg(
supplier_cnt($"ps_suppkey").as("supplier_cnt"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q11_7 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val sum_disc_price = new Sum_disc_price

val result = (((((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter($"l_shipdate" between ("1995-01-01", "1995-06-31")))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"l_orderkey" === $"o_orderkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema), $"o_custkey" === $"c_custkey", "inner"))
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
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q12_1 extends TPCHQuery {



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
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q13_10 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val revenue = new Sum_disc_price

val result = (((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter(($"l_returnflag" === "R") and ($"l_shipdate" between ("1994-02-01", "1994-10-01"))))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema)
.filter($"o_orderdate" between ("1994-05-01", "1994-10-01")), $"l_orderkey" === $"o_orderkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema), $"o_custkey" === $"c_custkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema), $"c_nationkey" === $"n_nationkey", "inner")
.groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
.agg(
revenue($"l_extendedprice", $"l_discount").as("revenue"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q14_19 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val sum_disc_price = new Sum_disc_price

val result = (DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter(($"l_shipinstruct" === "DELIVER IN PERSON") and ($"l_shipmode" isin ("AIR", "AIR REG"))))
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
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q15_18 extends TPCHQuery {

private val Q29_18 = new StructType()
.add("l_orderkey", "long")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum2 = new DoubleSum

val result = (((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter($"l_shipdate" between ("1994-01-01", "1994-06-31")))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"l_orderkey" === $"o_orderkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema), $"o_custkey" === $"c_custkey", "inner"))
.join(loadSharedTable(spark, "Q29_18", Q29_18)
.filter($"sum_quantity" > 300)
.select($"l_orderkey".as("agg_orderkey")), $"o_orderkey" === $"agg_orderkey", "left_semi")
.groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
.agg(
doubleSum2($"l_quantity").as("sum_quantity"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q16_9 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum = new DoubleSum

val result = (((((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema))
.join(DataUtils.loadStreamTable(spark, "part", "p", tpchSchema)
.filter($"p_name" like ("%green%")), $"l_partkey" === $"p_partkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"l_orderkey" === $"o_orderkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema), $"l_partkey" === $"ps_partkey" and  $"l_suppkey" === $"ps_suppkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"l_suppkey" === $"s_suppkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema), $"s_nationkey" === $"n_nationkey", "inner")
.select($"n_name".as("nation"), year($"o_orderdate").as("o_year"), (($"l_extendedprice" * ($"l_discount" - 1) * -1) - $"ps_supplycost" * $"l_quantity").as("amount"))
.groupBy($"nation", $"o_year")
.agg(
doubleSum($"amount").as("sum_profit"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q17_21 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val count = new Count

val result = (((((DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter(($"l_receiptdate" > $"l_commitdate") and ($"l_shipdate" between ("1994-07-01", "1994-10-01"))))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema)
.filter($"o_orderstatus" === "F"), $"l_orderkey" === $"o_orderkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"l_suppkey" === $"s_suppkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
.filter($"n_name" === "SAUDI ARABIA"), $"s_nationkey" === $"n_nationkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter(($"l_receiptdate" > $"l_commitdate") and ($"l_shipdate" between ("1994-07-01", "1994-10-01")))
.select($"l_orderkey".as("l2_orderkey"), $"l_suppkey".as("l2_suppkey")), ($"l_orderkey" === $"l2_orderkey") and  ($"l_suppkey" =!= $"l2_suppkey"), "left_semi"))
.join(DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter(($"l_receiptdate" > $"l_commitdate") and ($"l_shipdate" between ("1994-07-01", "1994-10-01")))
.select($"l_orderkey".as("l3_orderkey"), $"l_suppkey".as("l3_suppkey")), ($"l_orderkey" === $"l3_orderkey") and  ($"l_suppkey" =!= $"l3_suppkey"), "left_anti")
.groupBy($"s_name")
.agg(
count(lit(1L)).as("numwait"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q18_3 extends TPCHQuery {

private val Q30_3 = new StructType()
.add("o_orderdate", "date")
.add("o_shippriority", "int")
.add("revenue", "double")
.add("l_orderkey", "long")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._


val result = loadSharedTable(spark, "Q30_3", Q30_3)
.select($"l_orderkey", $"revenue", $"o_orderdate", $"o_shippriority")
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q19_12 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val udaf_q12_low = new UDAF_Q12_LOW
val udaf_q12_high = new UDAF_Q12_HIGH

val result = (DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter(($"l_shipmode" === "MAIL") and ($"l_receiptdate" > $"l_commitdate") and ($"l_shipdate" < $"l_commitdate") and ($"l_receiptdate" === "1994-01-01")))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"l_orderkey" === $"o_orderkey", "inner")
.groupBy($"l_shipmode")
.agg(
udaf_q12_high($"o_orderpriority").as("high_line_count"),
udaf_q12_low($"o_orderpriority").as("low_line_count"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q20_15 extends TPCHQuery {

private val Q31_15 = new StructType()
.add("total_revenue", "double")
.add("l_suppkey", "long")

private val Q32_15 = new StructType()
.add("max_revenue", "double")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._


val result = ((DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema))
.join(loadSharedTable(spark, "Q31_15", Q31_15)
.select($"l_suppkey".as("supplier_no"), $"total_revenue"), $"s_suppkey" === $"supplier_no", "inner"))
.join(loadSharedTable(spark, "Q32_15", Q32_15), $"total_revenue" >= $"max_revenue", "cross")
.select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total_revenue")
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q21_6 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum = new DoubleSum

val result = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter(($"l_shipdate" between ("1994-01-01", "1995-01-01")) and ($"l_discount" between (0.05, 0.07)) and ($"l_quantity" < 24.0))
.agg(
doubleSum($"l_extendedprice" * $"l_discount").as("revenue"))
.select("*")
 DataUtils.writeToSinkWithExtraOptions(
   result, query_name, uid, numBatch, constraint)

    }
}







private class Q22_17 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleAvg = new DoubleAvg

val result = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.groupBy($"l_partkey")
.agg(
(doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
.select($"l_partkey", $"avg_quantity")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q22_17", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q23_11 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum = new DoubleSum

val result = ((DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
.filter($"n_name" === "GERMANY"), $"s_nationkey" === $"n_nationkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema), $"s_suppkey" === $"ps_suppkey", "inner")
.groupBy($"ps_partkey")
.agg(
doubleSum($"ps_supplycost" * $"ps_availqty").as("product_value"))
.select($"ps_partkey", $"product_value")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q23_11", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q24_11 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum = new DoubleSum

val result = ((DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema)
.filter($"n_name" === "GERMANY"), $"s_nationkey" === $"n_nationkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema), $"s_suppkey" === $"ps_suppkey", "inner")
.agg(
doubleSum($"ps_supplycost" * $"ps_availqty" * 0.0001/SF).as("small_value"))
.select($"small_value")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q24_11", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q25_20 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum = new DoubleSum

val result = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter($"l_shipdate" between ("1994-01-01", "1994-06-31"))
.groupBy($"l_partkey", $"l_suppkey")
.agg(
(doubleSum($"l_quantity") * 0.5).as("agg_l_sum"))
.select($"l_partkey", $"l_suppkey", $"agg_l_sum")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q25_20", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q26_2 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._


val result = (((DataUtils.loadStreamTable(spark, "region", "r", tpchSchema)
.filter($"r_name" === "EUROPE"))
.join(DataUtils.loadStreamTable(spark, "nation", "n", tpchSchema), $"r_regionkey" === $"n_regionkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "supplier", "s", tpchSchema), $"n_nationkey" === $"s_nationkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "partsupp", "ps", tpchSchema), $"s_suppkey" === $"ps_suppkey", "inner")
.groupBy($"ps_partkey")
.agg(
min($"ps_supplycost").as("min_supplycost"))
.select($"ps_partkey".as("min_partkey"), $"min_supplycost")
.select($"min_supplycost", $"min_partkey")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q26_2", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q27_13 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val count_not_null = new Count_not_null

val result = (DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema), $"c_custkey" === $"o_custkey", "left_outer")
.groupBy($"c_custkey")
.agg(
count_not_null($"o_orderkey").as("c_count"))
.select($"c_count")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q27_13", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q28_22 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleAvg = new DoubleAvg

val result = DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema)
.filter(($"c_acctbal" > 0.0) and (substring($"c_phone", 1, 2) isin ("13", "31")))
.agg(
doubleAvg($"c_acctbal").as("avg_acctbal"))
.select($"avg_acctbal")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q28_22", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q29_18 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val doubleSum1 = new DoubleSum

val result = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.groupBy($"l_orderkey")
.agg(
doubleSum1($"l_quantity").as("sum_quantity"))
.select($"l_orderkey")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q29_18", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q30_3 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val sum_disc_price = new Sum_disc_price

val result = ((DataUtils.loadStreamTable(spark, "customer", "c", tpchSchema)
.filter($"c_mktsegment" === "BUILDING"))
.join(DataUtils.loadStreamTable(spark, "orders", "o", tpchSchema)
.filter($"o_orderdate" < "1992-03-15"), $"c_custkey" === $"o_custkey", "inner"))
.join(DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter($"l_shipdate" > "1998-03-15"), $"o_orderkey" === $"l_orderkey", "inner")
.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
.agg(
sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))
.select($"o_orderdate", $"o_shippriority", $"revenue", $"l_orderkey")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q30_3", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q31_15 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val sum_disc_price = new Sum_disc_price

val result = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter($"l_shipdate" between ("1995-01-01", "1995-04-01"))
.groupBy($"l_suppkey")
.agg(
sum_disc_price($"l_extendedprice", $"l_discount").as("total_revenue"))
.select($"total_revenue", $"l_suppkey")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q31_15", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q32_15 extends TPCHQuery {

private val Q33_15 = new StructType()
.add("total_revenue", "double")
.add("l_suppkey", "long")



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._


val result = loadSharedTable(spark, "Q33_15", Q33_15)
.select($"l_suppkey".as("supplier_no"), $"total_revenue")
.agg(
max($"total_revenue").as("max_revenue"))
.select($"max_revenue")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q32_15", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}







private class Q33_15 extends TPCHQuery {



    override def execQuery(spark: SparkSession, tpchSchema: TPCHSchema): Unit = {

import spark.implicits._

val sum_disc_price = new Sum_disc_price

val result = DataUtils.loadStreamTable(spark, "lineitem", "l", tpchSchema)
.filter($"l_shipdate" between ("1995-01-01", "1995-04-01"))
.groupBy($"l_suppkey")
.agg(
sum_disc_price($"l_extendedprice", $"l_discount").as("total_revenue"))
.select($"total_revenue", $"l_suppkey")
 DataUtils.writeToKafkaWithExtraOptions(
    result, "Q33_15", query_name, uid,
       numBatch, constraint, tpchSchema.checkpointLocation)

    }
}