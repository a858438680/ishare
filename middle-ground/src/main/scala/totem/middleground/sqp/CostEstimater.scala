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

import scala.collection.mutable

object CostEstimater {

  val READCOST = 1
  val WRITECOST = 3
  val ROOTQID = -1
  val STDRATIO = 1.0

  private case class CostInfo (qidSet: mutable.HashSet[Int],
                               selectSet: mutable.HashSet[Int],
                               cardMap: mutable.HashMap[Int, Double],
                               var totalCost: Double)

  def shareWithMatBenefit(op: PlanOperator, otherOP: PlanOperator): Double = {

    val newOP = mergePlanTree(op, otherOP)
    val leftCost = batchCostWithMat(op)
    val rightCost = batchCostWithMat(otherOP)
    val nonShareCost = leftCost + rightCost
    val shareCost = batchCostWithMat(newOP)

    nonShareCost - shareCost
  }

  private def batchCostWithMat(op: PlanOperator): Double = {
    batchCostWithMatHelper(op).totalCost
  }

  private def batchCostWithMatHelper(op: PlanOperator): CostInfo = {
    op match {
      case joinOp: JoinOperator =>

        val leftCostInfo = batchCostWithMatHelper(joinOp.childOps(0))
        val rightCostInfo = batchCostWithMatHelper(joinOp.childOps(1))
        filterByQidSet(joinOp.getQidSet, leftCostInfo)
        filterByQidSet(joinOp.getQidSet, rightCostInfo)

        val qidSet = mutable.HashSet.empty[Int]
        joinOp.getQidSet.foreach(qidSet.add)

        val cardMap = mutable.HashMap.empty[Int, Double]
        leftCostInfo.cardMap.keySet.foreach(qid => {
          val leftKey = Parser.extractRawColomn(joinOp.getJoinKeys(0))
          val rightKey = Parser.extractRawColomn(joinOp.getJoinKeys(1))
          val leftCard = leftCostInfo.cardMap(qid)
          val rightCard = rightCostInfo.cardMap(qid)
          cardMap.put(qid, Catalog.getJoinCardinality(leftKey, rightKey, leftCard, rightCard))
        })

        val selectSet = mutable.HashSet.empty[Int]
        val totalCost =
          getCost(cardMap(ROOTQID), joinOp) + leftCostInfo.totalCost + rightCostInfo.totalCost

        CostInfo(qidSet, selectSet, cardMap, totalCost)

      case scanOP: ScanOperator =>

        val tableName = scanOP.getTableName
        val tableSize = Catalog.getTableSize(tableName)

        val qidSet = mutable.HashSet.empty[Int]
        scanOP.getQidSet.foreach(qidSet.add)

        val cardMap = mutable.HashMap.empty[Int, Double]
        qidSet.foreach(cardMap.put(_, tableSize))
        cardMap.put(ROOTQID, tableSize)

        val selectSet = mutable.HashSet.empty[Int]

        val totalCost = getCost(tableSize, scanOP) + tableSize * READCOST
        CostInfo(qidSet, selectSet, cardMap, totalCost)

      case selectOP: SelectOperator =>

        val costInfo = batchCostWithMatHelper(selectOP.childOps(0))
        filterByQidSet(selectOP.getQidSet, costInfo)

        // apply select operator
        val selectivity = getSelectivity(selectOP)
        val selectSet = selectOP.getSelectSet
        selectSet.foreach(qid => {
          costInfo.selectSet.add(qid)
          val oldCard = costInfo.cardMap(qid)
          costInfo.cardMap.put(qid, oldCard * selectivity)
        })

        // See whether we can filter
        filterBySelect(costInfo)

        costInfo.totalCost += getCost(costInfo.cardMap(ROOTQID), selectOP)
        costInfo

      case projectOP: ProjectOperator =>

        val costInfo = batchCostWithMatHelper(projectOP.childOps(0))
        filterByQidSet(projectOP.getQidSet, costInfo)

        costInfo.totalCost += getCost(costInfo.cardMap(ROOTQID), projectOP)
        costInfo

      case aggOP: AggOperator => // TODO: this is buggy, should fix later

        val costInfo = batchCostWithMatHelper(aggOP.childOps(0))
        filterByQidSet(aggOP.getQidSet, costInfo)

        costInfo

      case _ =>
        System.err.println(s"Operator $op not supported yet")
        System.exit(1)
        null
    }
  }

  private def filterByQidSet(qidArray: Array[Int], costInfo: CostInfo): Unit = {

    val qidSet = mutable.HashSet.empty[Int]
    qidArray.foreach(qidSet.add)

    val diffSet = costInfo.qidSet.diff(qidSet)
    diffSet.foreach(qid => {
      costInfo.qidSet.remove(qid)
      costInfo.selectSet.remove(qid)
      costInfo.cardMap.remove(qid)
    })

    // Update CardMap
    val newTotalCard =
      qidSet.map(qid => {
        costInfo.cardMap.getOrElse(qid, {
          val qidStr = Utils.qidSetToString(qidArray)
          val diffStr = Utils.qidSetToString(costInfo.qidSet.toArray)
          System.err.println(s"No ${qid} $qidStr $diffStr")
          0.0
        })
        // costInfo.cardMap(qid)
      }).foldRight(0.0)((A, B) => {
        A + B
      })

    val curTotalCard = costInfo.cardMap(ROOTQID)
    costInfo.cardMap.put(ROOTQID, math.min(curTotalCard, newTotalCard))
  }

  private def filterBySelect(costInfo: CostInfo): Unit = {
    val diffSet = costInfo.qidSet.diff(costInfo.selectSet)
    if (diffSet.isEmpty) {
      costInfo.selectSet.clear()
      val totalCard = costInfo.cardMap(ROOTQID)

      val selArray =
        costInfo.cardMap.filter(_._1 != ROOTQID).map(pair => {
          pair._2/totalCard
        })

      var selectivity = 1.0
      selArray.foreach(sel => {
        selectivity *= 1.0 - sel
      })
      selectivity = 1.0 - selectivity

      costInfo.cardMap.put(ROOTQID, totalCard * selectivity)
    }
  }

  private def getSelectivity(selectOperator: SelectOperator): Double = {
    selectOperator.getPredicates.map(pred => {
      Catalog.getSelectivity(pred.toString)
    }).foldRight(1.0)((A, B) => {
      A * B
    })
  }

  private def getCost(outCard: Double, op: PlanOperator): Double = {

    var totalCost = 0.0
    if (op.parentOps.length > 1) {
      totalCost += WRITECOST * outCard + READCOST
      totalCost += READCOST * outCard * op.parentOps.length
    } else {
      totalCost += READCOST * outCard * STDRATIO
    }

    totalCost
  }

  private def mergePlanTree(op: PlanOperator, otherOP: PlanOperator): PlanOperator = {
    val newOP = copyPlanTree(op)
    val newOtherOP = copyPlanTree(otherOP)

    val mergedOP = Optimizer.mergeOpForDiffQueryHelper(newOP, newOtherOP)
    mergedOP.setParents(op.parentOps ++ otherOP.parentOps)

    mergedOP
  }

  private def copyPlanTree(op: PlanOperator): PlanOperator = {
    copyTreeHelper(Optimizer.dummyOperator, op)
  }

  private def copyTreeHelper(parent: PlanOperator, op: PlanOperator): PlanOperator = {
    val newOP = op.copy()
    val childOps = op.childOps.map(copyTreeHelper(newOP, _))
    val parentOps = new Array[PlanOperator](op.parentOps.length)
    for (i <- parentOps.indices) {
      if (i == 0) parentOps(i) = parent
      else parentOps(i) = Optimizer.dummyOperator
    }
    newOP.setChildren(childOps)
    newOP.setParents(parentOps)
    newOP
  }
}
