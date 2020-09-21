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

package totem.middleground.sqp

import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.io.Source

object Catalog {

  private var catalog: Catalog = _

  private var MAX_BATCH_NUM = 100
  private val MIN_BATCH_SIZE = 50.0

  def initCatalog(predFile: String): Unit = {
    catalog = new Catalog(predFile)
  }

  def getJoinCardinality(leftKey: String, rightKey: String,
                         leftCard: Double, rightCard: Double): Double = {
    catalog.getJoinCardinality(leftKey, rightKey, leftCard, rightCard)
  }

  def getSelectivity(predStr: String): Double = {
    catalog.getSelectivity(predStr)
  }

  def getPostFilterSelectivity(postFilter: String): Double = {
    catalog.getPostFilterSelectivity(postFilter)
  }

  def getTableSize(tableName: String): Double = {
    catalog.getTableSize(tableName)
  }

  def getAllAttrs(tableName: String): mutable.HashSet[TypeAttr] = {
    catalog.getAllAttrs(tableName)
  }

  def getTypeForAttr(attrName: String): String = {
    catalog.getTypeForAttr(attrName)
  }

  def getTableAlias(tableName: String): String = {
    catalog.getTableAlias(tableName)
  }

  def getGroupNum(columns: mutable.HashSet[String]): Double = {
    catalog.getGroupNum(columns)
  }

  def setMaxBatchNum(maxBatchNum: Int): Unit = {
    this.MAX_BATCH_NUM = maxBatchNum
  }

  def getMaxBatchNum: Int = MAX_BATCH_NUM
  def getMinBatchSize: Double = MIN_BATCH_SIZE
}

class Catalog (predFile: String) {

  val predInfo = parsePredFile(predFile)
  val cardRatio = HashMap(

    // Lineitem
    "l_orderkey:o_orderkey" -> 1.0,
    "o_orderkey:l_orderkey" -> 4.0,

    "l_suppkey:ps_suppkey"  -> 1.0,
    "l_partkey:ps_partkey"  -> 1.0,
    "ps_suppkey:l_suppkey"  -> 7.5,
    "ps_partkey:l_partkey"  -> 7.5,

    "l_partkey:p_partkey"   -> 1.0,
    "l_suppkey:s_suppkey"   -> 1.0,
    "p_partkey:l_partkey"   -> 30.0,
    "s_suppkey:l_suppkey"   -> 600.0,

    // Orders
    "o_custkey:c_custkey"   -> 1.0,
    "c_custkey:o_custkey"   -> 10.0,

    // PartSupp
    "ps_partkey:p_partkey"  -> 1.0,
    "ps_suppkey:s_suppkey"  -> 1.0,
    "p_partkey:ps_partkey"  -> 4.0,
    "s_suppkey:ps_suppkey"  -> 80.0,

    // Nation
    "s_nationkey:c_nationkey" -> 6000.0,
    "c_nationkey:s_nationkey" -> 400.0,
    "n_nationkey:s_nationkey" -> 400.0,
    "s_nationkey:n_nationkey" -> 1.0,
    "n_nationkey:c_nationkey" -> 6000.0,
    "c_nationkey:n_nationkey" -> 1.0,

    // Region
    "n_regionkey:r_regionkey" -> 1.0,
    "r_regionkey:n_regionkey" -> 5.0,

    // Q21
    "l_orderkey:l2_orderkey" -> 1.0,
    "l2_orderkey:l_orderkey" -> 1.0,
    "l_orderkey:l3_orderkey" -> 1.0,
    "l3_orderkey:l_orderkey" -> 1.0,

    // Q15
    "s_suppkey:supplier_no" -> 1.0,
    "supplier_no:s_suppkey" -> 1.0,
    "total_revenue:max_revenue" -> 1.0,
    "max_revenue:total_revenue" -> 1.0,

    // Q2
    "p_partkey:min_partkey" -> 0.2,
    "min_partkey:p_partkey" -> 1.0,

    // Q17
    "l_partkey:agg_l_partkey" -> 1.0,
    "agg_l_partkey:l_partkey" -> 30.0,

    // Q18
    "o_orderkey:agg_orderkey" -> 4.0,
    "agg_orderkey:o_orderkey" -> 1.0,
    "l_orderkey:agg_orderkey" -> 1.0,
    "agg_orderkey:l_orderkey" -> 4.0,

    // Q11
    "product_value:small_value" -> 1.0,
    "small_value:product_value" -> 1000.0,

    // Q20
    "ps_partkey:agg_l_partkey" -> 1.0,
    "agg_l_partkey:ps_partkey" -> 4.0,

    // Q22
    "c_acctbal:avg_acctbal" -> 1.0,
    "avg_acctbal:c_acctbal" -> 1000.0,

    // Q7
    "s_nationkey:n1_nationkey" -> 1.0,
    "n1_nationkey:s_nationkey" -> 400.0,
    "c_nationkey:n2_nationkey" -> 1.0,
    "n2_nationkey:c_nationkey" -> 400.0,

    // Q8
    "c_nationkey:n1_nationkey" -> 1.0,
    "n1_nationkey:c_nationkey" -> 6000.0,
    "n1_regionkey:r_regionkey" -> 1.0,
    "r_regionkey:n1_regionkey" -> 5.0,
    "s_nationkey:n2_nationkey" -> 1.0,
    "n2_nationkey:s_nationkey" -> 400.0,

    // Q45
    "avg_quantity:ps_availqty" -> 10.0,
    "ps_availqty:avg_quantity" -> 10.0

  )

  val groupNumInfo = Array(
    (HashSet("l_returnflag", "l_linestatus"), 4),
    (HashSet("ps_partkey"), 100000),
    (HashSet("l_orderkey", "o_orderdate", "o_shippriority"), 1),
    (HashSet("o_orderpriority"), 1),
    (HashSet("n_name"), 5),
    (HashSet("supp_nation", "cust_nation", "l_year"), 2),
    (HashSet("o_year"), 1),
    (HashSet("nation", "o_year"), 175),
    (HashSet("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name",
     "c_address", "c_comment"), 44124),
    (HashSet("l_shipmode"), 1),
    (HashSet("c_custkey"), 150000),
    (HashSet("c_count"), 42),
    (HashSet("l_suppkey"), 10000),
    (HashSet("p_brand", "p_type", "p_size"), 7092),
    (HashSet("l_partkey"), 200000),
    (HashSet("l_orderkey"), 1500000),
    (HashSet("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"), 10),
    (HashSet("l_partkey", "l_suppkey"), 344404),
    (HashSet("s_name"), 1),
    (HashSet("cntrycode"), 2)
  )

  val postFilterSelectivity = HashMap(
    """$"l_quantity" < $"avg_quantity"""" -> 0.08,
    """.filter(($"supp_nation" === "FRANCE" and $"cust_nation" === "GERMANY") """ +
      """or ($"supp_nation" === "GERMANY" and $"cust_nation" === "FRANCE"))""" -> 0.1,
    """.filter($"sum_quantity" > 300)""" -> 0.01)

  val SF = 1

  val baseTableSize = HashMap(
    "lineitem"  -> 6000000,
    "orders"    -> 1500000,
    "customer"  -> 150000,
    "supplier"  -> 10000,
    "part"      -> 200000,
    "partsupp"  -> 800000,
    "nation"    -> 25,
    "region"    -> 5
  )

  private val lineitemSchema = mutable.HashSet(
    TypeAttr("l_orderkey", "long"),
    TypeAttr("l_partkey", "long"),
    TypeAttr("l_suppkey", "long"),
    TypeAttr("l_linenumber", "int"),
    TypeAttr("l_quantity", "double"),
    TypeAttr("l_extendedprice", "double"),
    TypeAttr("l_discount", "double"),
    TypeAttr("l_tax", "double"),
    TypeAttr("l_returnflag", "string"),
    TypeAttr("l_linestatus", "string"),
    TypeAttr("l_shipdate", "date"),
    TypeAttr("l_commitdate", "date"),
    TypeAttr("l_receiptdate", "date"),
    TypeAttr("l_shipinstruct", "string"),
    TypeAttr("l_shipmode", "string"),
    TypeAttr("l_comment", "string")
  )

  private val ordersSchema = mutable.HashSet(
    TypeAttr("o_orderkey", "long"),
    TypeAttr("o_custkey", "long"),
    TypeAttr("o_orderstatus", "string"),
    TypeAttr("o_totalprice", "double"),
    TypeAttr("o_orderdate", "date"),
    TypeAttr("o_orderpriority", "string"),
    TypeAttr("o_clerk", "string"),
    TypeAttr("o_shippriority", "int"),
    TypeAttr("o_comment", "string")
  )

  private val customerSchema = mutable.HashSet(
    TypeAttr("c_custkey", "long"),
    TypeAttr("c_name", "string"),
    TypeAttr("c_address", "string"),
    TypeAttr("c_nationkey", "long"),
    TypeAttr("c_phone", "string"),
    TypeAttr("c_acctbal", "double"),
    TypeAttr("c_mktsegment", "string"),
    TypeAttr("c_comment", "string")
  )

  private val partSchema = mutable.HashSet(
    TypeAttr("p_partkey", "long"),
    TypeAttr("p_name", "string"),
    TypeAttr("p_mfgr", "string"),
    TypeAttr("p_brand", "string"),
    TypeAttr("p_type", "string"),
    TypeAttr("p_size", "int"),
    TypeAttr("p_container", "string"),
    TypeAttr("p_retailprice", "double"),
    TypeAttr("p_comment", "string")
  )

  private val partsuppSchema = mutable.HashSet(
    TypeAttr("ps_partkey", "long"),
    TypeAttr("ps_suppkey", "long"),
    TypeAttr("ps_availqty", "int"),
    TypeAttr("ps_supplycost", "double"),
    TypeAttr("ps_comment", "string")
  )

  private val supplierSchema = mutable.HashSet(
    TypeAttr("s_suppkey", "long"),
    TypeAttr("s_name", "string"),
    TypeAttr("s_address", "string"),
    TypeAttr("s_nationkey", "long"),
    TypeAttr("s_phone", "string"),
    TypeAttr("s_acctbal", "double"),
    TypeAttr("s_comment", "string")
  )

  private val nationSchema = mutable.HashSet(
    TypeAttr("n_nationkey", "long"),
    TypeAttr("n_name", "string"),
    TypeAttr("n_regionkey", "long"),
    TypeAttr("n_comment", "string")
  )

  private val regionSchema = mutable.HashSet(
    TypeAttr("r_regionkey", "long"),
    TypeAttr("r_name", "string"),
    TypeAttr("r_comment", "string")
  )

  private def parsePredFile(fileName: String): mutable.HashMap[String, Double] = {

    val predMap = mutable.HashMap.empty[String, Double]

    val predLines = Source.fromFile(fileName).getLines().map(_.trim)
    predLines.foreach(line => {
      if (line != "") {
        val predArray = line.split(":").map(_.trim)
        predMap.put(predArray(0), predArray(1).toDouble)
      }
    })

    predMap
  }

  def getJoinCardinality(leftKey: String, rightKey: String,
                         leftCard: Double, rightCard: Double): Double = {
    if (leftCard < 0.01 || rightCard < 0.01) return 0.0
    val card1 = estimateJoinCardHelper(leftKey, rightKey)
    val card2 = estimateJoinCardHelper(rightKey, leftKey)
    val maxCard = math.min(card1, card2)

    val leftTableSize = getTableSizeFromKey(leftKey)
    val rightTableSize = getTableSizeFromKey(rightKey)
    maxCard * (leftCard/leftTableSize) * (rightCard/rightTableSize)
  }

  def getAllAttrs(tableName: String): mutable.HashSet[TypeAttr] = {
    tableName.toLowerCase match {
      case "lineitem" => lineitemSchema
      case "orders" => ordersSchema
      case "customer" => customerSchema
      case "part" => partSchema
      case "supplier" => supplierSchema
      case "partsupp" => partsuppSchema
      case "nation" => nationSchema
      case "region" => regionSchema
      case _ =>
        System.err.println(s"Unknown table $tableName")
        System.exit(1)
        null
    }
  }

  def getTableAlias(tableName: String): String = {
     tableName.toLowerCase match {
      case "lineitem" => "l"
      case "orders" => "o"
      case "customer" => "c"
      case "part" => "p"
      case "supplier" => "s"
      case "partsupp" => "ps"
      case "nation" => "n"
      case "region" => "r"
      case _ =>
        System.err.println(s"Unknown table $tableName")
        System.exit(1)
        ""
    }
  }

  def getTypeForAttr(attrName: String): String = {
    val tableName = getTableNameFromKey(attrName)
    val attrs = getAllAttrs(tableName)
    var typeStr = ""
    attrs.foreach(typeAttr => {
      if (typeAttr.attr.compareTo(attrName) == 0) typeStr = typeAttr.typeStr
    })
    typeStr
  }

  private def estimateJoinCardHelper(leftKey: String, rightKey: String): Double = {
    val ratio = cardRatio.getOrElse(s"$leftKey:$rightKey", {

      System.err.println(s"$leftKey:$rightKey Not Recognized")
      System.exit(1)
      1.0
    })

    val leftTableSize = getTableSizeFromKey(leftKey)
    val rightTableSize = getTableSizeFromKey(rightKey)

    val maxDistinctValue = rightTableSize/ratio
    val distinctValue = math.min(maxDistinctValue, rightTableSize)

    leftTableSize * (rightTableSize/distinctValue)
  }

  private def getTableNameFromKey(key: String): String = {
    val idx = key.indexOf("_") + 1
    key.substring(0, idx) match {
      case "l_" => "lineitem"
      case "o_" => "orders"
      case "c_" => "customer"
      case "s_" => "supplier"
      case "ps_" => "partsupp"
      case "p_" => "part"
      case "n_" => "nation"
      case "r_" => "region"
      case "l2_" => "lineitem" // Q21
      case "l3_" => "lineitem" // Q21
      case "supplier_" => "supplier" // Q15
      case "total_" => "supplier" // Q15
      case "max_" => "supplier"  // Q15
      case "min_" => "part" // Q2
      case "agg_" =>
        if (key.compareTo("agg_l_partkey") == 0) "part" // Q17/Q20
        else "orders" // Q18
      case "small_" => "region" // Q11
      case "product_" => "supplier" // Q11
      case "avg_" => "customer" // Q22
      case "n1_" => "nation" // Q7
      case "n2_" => "nation" // Q7
      case _ =>
        System.err.println(s"Key $key Not Recognized")
        System.exit(1)
        ""
    }
  }

  private def getTableSizeFromKey(key: String): Double = {
    getTableSize(getTableNameFromKey(key))
  }

  def getPostFilterSelectivity(postFilter: String): Double = {
    postFilterSelectivity.getOrElse(postFilter.trim, 1.0)
  }

  def getSelectivity(predStr: String): Double = {
    predInfo.getOrElse(predStr, {
      System.err.println(s"Predicate $predStr Not Recognized")
      System.exit(1)
      1.0
    })
  }

  def getTableSize(tableName: String): Double = {
    baseTableSize(tableName.toLowerCase)
  }

  def getGroupNum(columns: mutable.HashSet[String]): Double = {
    var groupNum = 1.0
    var findGroup = false
    groupNumInfo.foreach(pair => {
      val candSet = pair._1
      val candGroupNum = pair._2
      if (candSet.diff(columns).isEmpty && columns.diff(candSet).isEmpty) {
        groupNum = candGroupNum
        findGroup = true
      }
    })

    if (!findGroup) {
      val groupByStrBuf = new StringBuffer()
      columns.foreach(col => {groupByStrBuf.append(s"$col:")})
      System.err.println(s"GroupBy columns $groupByStrBuf not found")
    }

    groupNum
  }

}
