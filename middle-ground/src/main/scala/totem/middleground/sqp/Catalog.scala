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
import scala.collection.mutable
import scala.io.Source

object Catalog {

  private var catalog: Catalog = _

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

  def getTableSize(tableName: String): Double = {
    catalog.getTableSize(tableName)
  }

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
    "r_regionkey:n_regionkey" -> 5.0

  )

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
    val card1 = estimateJoinCardHelper(leftKey, rightKey, leftCard, rightCard)
    val card2 = estimateJoinCardHelper(rightKey, leftKey, rightCard, leftCard)
    math.min(card1, card2)
  }

  private def estimateJoinCardHelper(leftKey: String, rightKey: String,
                                     leftCard: Double, rightCard: Double): Double = {
    val ratio = cardRatio.getOrElse(s"$leftKey:$rightKey", {

      System.err.println(s"$leftKey:$rightKey Not Recognized")
      System.exit(1)
      1.0
    })

    val rightTableSize = getTableSizeFromKey(rightKey)

    val maxDistinctValue = rightTableSize/ratio
    val distinctValue = math.min(maxDistinctValue, rightCard)

    leftCard * (rightCard/distinctValue)
  }

  private def getTableSizeFromKey(key: String): Double = {

    val idx = key.indexOf("_") + 1
    key.substring(0, idx) match {
      case "l_" =>
        baseTableSize("lineitem").toDouble
      case "o_" =>
        baseTableSize("orders").toDouble
      case "c_" =>
        baseTableSize("customer").toDouble
      case "s_" =>
        baseTableSize("supplier").toDouble
      case "ps_" =>
        baseTableSize("partsupp").toDouble
      case "p_" =>
        baseTableSize("part").toDouble
      case "n_" =>
        baseTableSize("nation").toDouble
      case "r_" =>
        baseTableSize("region").toDouble
      case _ =>
        System.err.println(s"Key $key Not Recognized")
        System.exit(1)
        1.0
    }

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

}
