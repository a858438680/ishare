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

  private val ROOTQID = -1

  private val READCOST = 1.0
  private val WRITECOST = 3.0
  private val STDRATIO = 1.0

  private val JOINREADCOST = READCOST * 1.0
  private val SCANREADCOST = READCOST * 1.0
  private val AGGREADCOST = READCOST * 1.0
  private val PROJREADCOST = READCOST * 0.1
  private val SELREADCOST = READCOST * 0.1

  private val BASESTARTUPCOST = 100.0
  private val scanStartupCost = BASESTARTUPCOST * 0.1
  private val projStartupCost = BASESTARTUPCOST * 0.1
  private val selectStartupCost = BASESTARTUPCOST * 0.1
  private val joinStartupCost = BASESTARTUPCOST * 10
  private val aggStartupCost = BASESTARTUPCOST * 3

  private case class CostInfo (qidSet: mutable.HashSet[Int],
                               selectSet: mutable.HashSet[Int],
                               cardMap: mutable.HashMap[Int, Double],
                               delCardMap: mutable.HashMap[Int, Double],
                               var totalCost: Double)

  def estimatePlanGraphCost(subQueries: Array[PlanOperator],
                            batchNums: Array[Int],
                            queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]]):
  (Double, Array[Double]) = {

    val numSubQ = subQueries.length
    val totalWork = new Array[Double](numSubQ)
    val finalWork = new Array[Double](numSubQ)
    var totalWorkSum = 0.0
    val maxBatchNum = Catalog.getMaxBatchNum
    val progressSimulator = new ProgressSimulator(maxBatchNum, batchNums, queryDependency)

    subQueries.foreach(resetCostInfo)

    var curStep = 0
    while (curStep < maxBatchNum) {

      val pair = progressSimulator.nextStep()
      curStep = pair._1
      val setArray = pair._2

      setArray.foreach(execSet => {
        execSet.foreach(uid => {
          if (curStep == maxBatchNum) {
            val a = 1
          }
          val oneExecWork =
            estimateOneExecutionCost(subQueries(uid), batchNums(uid), uid)
          totalWork(uid) += oneExecWork
          totalWorkSum += oneExecWork
          if (curStep == maxBatchNum) {
           finalWork(uid) = oneExecWork
          }
        })
      })

    }

    (totalWorkSum, finalWork)
  }

  def buildQidFinalWork(qidToUids: mutable.HashMap[Int, mutable.HashSet[Int]],
                        finalWork: Array[Double]): mutable.HashMap[Int, Double] = {
    val qidToFinalWork = mutable.HashMap.empty[Int, Double]

    qidToUids.foreach(pair => {
      val qid = pair._1
      val uidSet = pair._2

      val sumFinalWork =
        uidSet.foldRight(0.0)((uid, curSum) => {
          finalWork(uid) + curSum
        })

      qidToFinalWork.put(qid, sumFinalWork)
    })

    qidToFinalWork
  }

  private def resetCostInfo(op: PlanOperator): Unit = {
    op.resetCostInfo()
    op.childOps.foreach(resetCostInfo)
  }

  private def estimateOneExecutionCost(subQuery: PlanOperator,
                                       batchNum: Int,
                                       uid: Int): Double = {
    val isSubQueryRoot = true
    incrementalCostHelper(subQuery, batchNum, uid, isSubQueryRoot).totalCost
  }

  private def incrementalCostHelper(op: PlanOperator,
                                    batchNum: Int,
                                    uid: Int,
                                    isSubQueryRoot: Boolean): CostInfo = {

    if (op.parentOps.length > 1 && !isSubQueryRoot) { // A materialize operator
      val qidSet = fromArrayQidSet(op.getQidSet)
      val selectSet = mutable.HashSet.empty[Int]

      val cardMap = readMat(op.cardMap, op.offSetMap, uid, qidSet)
      val delCardMap = readMat(op.delCardMap, op.delOffSetMap, uid, qidSet)

      val totalCost = (cardMap(ROOTQID) + delCardMap(ROOTQID)) * READCOST
      CostInfo(qidSet, selectSet, cardMap, delCardMap, totalCost)

    } else {

      val isRoot = false

      op match {
        case scanOP: ScanOperator =>
          val tableSize = Catalog.getTableSize(scanOP.getTableName)
          val batchIdx = scanOP.batchIdx
          val batchSize =
            if (tableSize/batchNum >= Catalog.getMinBatchSize) {
              tableSize/batchNum
            } else {
              val tmpBatchSize =
                math.min(tableSize - (batchIdx.toDouble * Catalog.getMinBatchSize),
                  Catalog.getMinBatchSize)
              math.max(tmpBatchSize, 0.0)
            }
          scanOP.batchIdx += 1

          val qidSet = fromArrayQidSet(scanOP.getQidSet)
          val selectSet = mutable.HashSet.empty[Int]

          val cardMap = createEmptyCardMap(qidSet)
          setOneValueForCardMap(cardMap, qidSet, batchSize)
          val delCardMap = createEmptyCardMap(qidSet)

          val baseCost = batchSize * SCANREADCOST + scanStartupCost
          if (scanOP.parentOps.length > 1) {
              System.err.println(s"Materializing the whole table" +
                s"for ${Utils.qidSetToString(scanOP.getQidSet)}")
          }
          val totalCost = getMatCost(baseCost, batchSize, scanOP)
          val costInfo = CostInfo(qidSet, selectSet, cardMap, delCardMap, totalCost)
          materialize(costInfo, scanOP, qidSet)
          costInfo

        case projOP: ProjectOperator =>
          val costInfo = incrementalCostHelper(projOP.childOps(0), batchNum, uid, isRoot)
          filterByQidSet(projOP.getQidSet, costInfo)

          val baseSize = getTotalCard(costInfo)
          val baseCost = baseSize * PROJREADCOST + projStartupCost
          costInfo.totalCost += getMatCost(baseCost, baseSize, projOP)
          materialize(costInfo, projOP, fromArrayQidSet(projOP.getQidSet))
          costInfo

        case selOP: SelectOperator =>
          val costInfo = incrementalCostHelper(selOP.childOps(0), batchNum, uid, isRoot)
          filterByQidSet(selOP.getQidSet, costInfo)

          val baseSize = getTotalCard(costInfo)
          val baseCost = baseSize * SELREADCOST + selectStartupCost

          // apply select operator
          val selectivity = getSelectivity(selOP)
          val opSelectSet = selOP.getSelectSet
          opSelectSet.foreach(costInfo.selectSet.add)
          applySelectOperator(costInfo.cardMap, opSelectSet, selectivity)
          applySelectOperator(costInfo.delCardMap, opSelectSet, selectivity)

          // See whether we can filter
          filterBySelect(costInfo)

          costInfo.totalCost += getMatCost(baseCost, getTotalCard(costInfo), selOP)
          materialize(costInfo, selOP, fromArrayQidSet(selOP.getQidSet))
          costInfo

        case aggOP: AggOperator =>
          val costInfo = incrementalCostHelper(aggOP.childOps(0), batchNum, uid, isRoot)
          filterByQidSet(aggOP.getQidSet, costInfo)

          val realGroups =
            if (aggOP.getGroupByAttrs.isEmpty) 1.0
            else Catalog.getGroupNum(aggOP.getGroupByAttrs)
          val stateSize = aggOP.stateSize
          val inputInsert = costInfo.cardMap(ROOTQID)
          val inputDelete = costInfo.delCardMap(ROOTQID)
          aggOP.stateSize = stateSize + inputInsert - inputDelete

          val (outInsert, outDelete) =
            if (stateSize < realGroups) {
              val insert = scala.math.min(realGroups - stateSize, inputInsert)
              val insert_for_update = scala.math.max(0, inputInsert - (realGroups - stateSize))
              val insert_to_update = scala.math.min(stateSize, insert_for_update)
              val totalUpdate = inputDelete + insert_to_update
              (insert + totalUpdate, totalUpdate)
            } else {
              val totalUpdate = scala.math.min(inputInsert + inputDelete, realGroups)
              (totalUpdate, totalUpdate)
            }

          val qidSet = fromArrayQidSet(aggOP.getQidSet)
          setOneValueForCardMap(costInfo.cardMap, qidSet, outInsert)
          setOneValueForCardMap(costInfo.delCardMap, qidSet, outDelete)

          val baseSize = inputInsert + inputDelete
          val baseCost = baseSize * AGGREADCOST + aggStartupCost
          costInfo.totalCost += getMatCost(baseCost, outInsert + outDelete, aggOP)
          materialize(costInfo, aggOP, qidSet)
          costInfo

        case joinOP: JoinOperator =>
          val innerCostInfo =
            incrementalCostHelper(joinOP.childOps(Parser.INNER), batchNum, uid, isRoot)
          val outerCostInfo =
            incrementalCostHelper(joinOP.childOps(Parser.OUTER), batchNum, uid, isRoot)
          filterByQidSet(joinOP.getQidSet, innerCostInfo)
          filterByQidSet(joinOP.getQidSet, outerCostInfo)

          val qidSet = fromArrayQidSet(joinOP.getQidSet)
          val selectSet = mutable.HashSet.empty[Int]

          val outCardMap = createEmptyCardMap(qidSet)
          val delOutCardMap = createEmptyCardMap(qidSet)

          val outerKey = Parser.extractRawColomn(joinOP.getJoinKeys(Parser.OUTER))
          val innerKey = Parser.extractRawColomn(joinOP.getJoinKeys(Parser.INNER))
          val innerStateSizeMap = joinOP.innerStateSizeMap
          val outerStateSizeMap = joinOP.outerStateSizeMap
          val postFilterSelectivity = Catalog.getPostFilterSelectivity(joinOP.getPostFilter())
          applyOneJoin(innerCostInfo.cardMap, innerCostInfo.delCardMap, innerStateSizeMap,
            outerStateSizeMap, innerKey, outerKey, postFilterSelectivity, outCardMap,
            delOutCardMap, qidSet)
          applyOneJoin(outerCostInfo.cardMap, outerCostInfo.delCardMap, outerStateSizeMap,
            innerStateSizeMap, outerKey, innerKey, postFilterSelectivity, outCardMap,
            delOutCardMap, qidSet)

          val baseSize = getTotalCard(innerCostInfo) + getTotalCard(outerCostInfo)
          val baseCost = baseSize * JOINREADCOST + joinStartupCost
          val outputSize = outCardMap(ROOTQID) + delOutCardMap(ROOTQID)
          val totalCost = innerCostInfo.totalCost + outerCostInfo.totalCost +
            getMatCost(baseCost, outputSize, joinOP)
          val costInfo = CostInfo(qidSet, selectSet, outCardMap, delOutCardMap, totalCost)
          materialize(costInfo, joinOP, qidSet)
          costInfo

        case rootOP: RootOperator =>
          incrementalCostHelper(rootOP.childOps(0), batchNum, uid, isRoot)

        case _ =>
          System.err.println(s"Operator $op not supported yet")
          System.exit(1)
          null
      }
    }

  }

  private def applyOneJoin(inputCardMap: mutable.HashMap[Int, Double],
                           inputDelCardMap: mutable.HashMap[Int, Double],
                           thisStateSizeMap: mutable.HashMap[Int, Double],
                           otherStateSizeMap: mutable.HashMap[Int, Double],
                           thisJoinKey: String,
                           otherJoinKey: String,
                           postFilterSelectivity: Double,
                           outputCardMap: mutable.HashMap[Int, Double],
                           outputDelCardMap: mutable.HashMap[Int, Double],
                           qidSet: mutable.HashSet[Int]): Unit = {
    if (thisStateSizeMap.isEmpty) resetCardMap(thisStateSizeMap, qidSet)
    if (otherStateSizeMap.isEmpty) resetCardMap(otherStateSizeMap, qidSet)

    val fullQidSet = qidSet ++ mutable.HashSet(ROOTQID)
    fullQidSet.foreach(qid => {
      val thisCard = inputCardMap(qid)
      val thisDelCard = inputDelCardMap(qid)
      val thisStateSize = thisStateSizeMap(qid)
      val otherStateSize = otherStateSizeMap(qid)

      thisStateSizeMap.put(qid, thisStateSize + thisCard - thisDelCard)
      val oldOutCard = outputCardMap(qid)
      val oldOutDelCard = outputDelCardMap(qid)
      outputCardMap.put(qid, oldOutCard +
        Catalog.getJoinCardinality(thisJoinKey, otherJoinKey, thisCard, otherStateSize)
          * postFilterSelectivity)
      outputDelCardMap.put(qid, oldOutDelCard +
        Catalog.getJoinCardinality(thisJoinKey, otherJoinKey, thisDelCard, otherStateSize)
          * postFilterSelectivity)
    })
  }

  private def getMatCost(baseCost: Double, baseSize: Double, op: PlanOperator): Double = {
    if (op.parentOps.length > 1) {
      baseCost + baseSize * WRITECOST
    } else baseCost
  }

  private def readMat(cardMap: mutable.HashMap[Int, Double],
                      offsetMap: mutable.HashMap[Int, mutable.HashMap[Int, Double]],
                      uid: Int, qidSet: mutable.HashSet[Int]): mutable.HashMap[Int, Double] = {
    val thisOffsetMap =
      if (!offsetMap.contains(uid)) {
        val newOffSetMap = createEmptyCardMap(qidSet)
        offsetMap.put(uid, newOffSetMap)
        newOffSetMap
      } else offsetMap(uid)

    val newCardMap = createEmptyCardMap(qidSet)
    (qidSet ++ mutable.HashSet(ROOTQID)).foreach(qid => {
      val readOffset = thisOffsetMap(qid)
      val totalOffset = cardMap(qid)
      thisOffsetMap.put(qid, totalOffset)
      newCardMap.put(qid, totalOffset - readOffset)
    })

    newCardMap
  }

  private def materialize(costInfo: CostInfo,
                          op: PlanOperator,
                          qidSet: mutable.HashSet[Int]): Unit = {
    if (op.parentOps.length > 1) {
      materializeHelper(op.cardMap, costInfo.cardMap, qidSet)
      materializeHelper(op.delCardMap, costInfo.delCardMap, qidSet)
    }
  }

  private def materializeHelper(matCardMap: mutable.HashMap[Int, Double],
                                cardMap: mutable.HashMap[Int, Double],
                                qidSet: mutable.HashSet[Int]): Unit = {
    if (matCardMap.isEmpty) resetCardMap(matCardMap, qidSet)
    (qidSet ++ mutable.HashSet(ROOTQID)).foreach(qid => {
      val newCard = matCardMap(qid) + cardMap(qid)
      matCardMap.put(qid, newCard)
    })
  }

  private def getTotalCard(costInfo: CostInfo): Double = {
    costInfo.cardMap(ROOTQID) + costInfo.delCardMap(ROOTQID)
  }

  private def applySelectOperator(cardMap: mutable.HashMap[Int, Double],
                                  opSelectSet: Array[Int],
                                  selectivity: Double): Unit = {
    opSelectSet.foreach(qid => {
      val oldCard = cardMap(qid)
      cardMap.put(qid, oldCard * selectivity)
    })
  }

  private def setOneValueForCardMap(cardMap: mutable.HashMap[Int, Double],
                                    qidSet: mutable.HashSet[Int],
                                    oneValue: Double): Unit = {
    qidSet.foreach(qid => {
      cardMap.put(qid, oneValue)
    })
    cardMap.put(ROOTQID, oneValue)
  }

  private def createEmptyCardMap(qidSet: mutable.HashSet[Int]):
  mutable.HashMap[Int, Double] = {
    val cardMap = mutable.HashMap.empty[Int, Double]
    resetCardMap(cardMap, qidSet)
    cardMap
  }

  private def resetCardMap(cardMap: mutable.HashMap[Int, Double],
                           qidSet: mutable.HashSet[Int]): Unit = {
    qidSet.foreach(qid => {
      cardMap.put(qid, 0.0)
    })
    cardMap.put(ROOTQID, 0.0)
  }

  private def copyCardMapInfo(newMap: mutable.HashMap[Int, Double],
                              oldMap: mutable.HashMap[Int, Double],
                              qidSet: mutable.HashSet[Int]): Unit = {
    qidSet.foreach(qid => {
      newMap.put(qid, oldMap(qid))
    })
    newMap.put(ROOTQID, oldMap(ROOTQID))
  }

  private def fromArrayQidSet(qidSet: Array[Int]): mutable.HashSet[Int] = {
    val newSet = mutable.HashSet.empty[Int]
    qidSet.foreach(newSet.add)
    newSet
  }

  def shareWithMatBenefit(op: PlanOperator,
                          otherOP: PlanOperator,
                          isSameQuery: Boolean): Double = {

    val newOP = mergePlanTree(op, otherOP, isSameQuery)
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

        val qidSet = fromArrayQidSet(joinOp.getQidSet)
        val selectSet = mutable.HashSet.empty[Int]

        val cardMap = mutable.HashMap.empty[Int, Double]
        leftCostInfo.cardMap.keySet.foreach(qid => {
          val leftKey = Parser.extractRawColomn(joinOp.getJoinKeys(0))
          val rightKey = Parser.extractRawColomn(joinOp.getJoinKeys(1))
          val leftCard = leftCostInfo.cardMap(qid)
          val rightCard = rightCostInfo.cardMap(qid)
          cardMap.put(qid, Catalog.getJoinCardinality(leftKey, rightKey, leftCard, rightCard))
        })
        val delCardMap = createEmptyCardMap(qidSet)

        val totalCost =
          getCost(cardMap(ROOTQID), joinOp) + leftCostInfo.totalCost + rightCostInfo.totalCost

        CostInfo(qidSet, selectSet, cardMap, delCardMap, totalCost)

      case scanOP: ScanOperator =>

        val tableName = scanOP.getTableName
        val tableSize = Catalog.getTableSize(tableName)

        val qidSet = fromArrayQidSet(scanOP.getQidSet)
        val selectSet = mutable.HashSet.empty[Int]

        val cardMap = createEmptyCardMap(qidSet)
        setOneValueForCardMap(cardMap, qidSet, tableSize)
        val delCardMap = createEmptyCardMap(qidSet)

        val totalCost = tableSize * READCOST + getCost(tableSize, scanOP)
        CostInfo(qidSet, selectSet, cardMap, delCardMap, totalCost)

      case selectOP: SelectOperator =>

        val costInfo = batchCostWithMatHelper(selectOP.childOps(0))
        filterByQidSet(selectOP.getQidSet, costInfo)

        // apply select operator
        val selectivity = getSelectivity(selectOP)
        val opSelectSet = selectOP.getSelectSet
        opSelectSet.foreach(costInfo.selectSet.add)
        applySelectOperator(costInfo.cardMap, opSelectSet, selectivity)

        // See whether we can filter
        filterBySelect(costInfo)

        costInfo.totalCost += getCost(costInfo.cardMap(ROOTQID), selectOP)
        costInfo

      case projectOP: ProjectOperator =>

        val costInfo = batchCostWithMatHelper(projectOP.childOps(0))
        filterByQidSet(projectOP.getQidSet, costInfo)

        costInfo.totalCost += getCost(costInfo.cardMap(ROOTQID), projectOP)
        costInfo

      case aggOP: AggOperator =>

        val costInfo = batchCostWithMatHelper(aggOP.childOps(0))
        filterByQidSet(aggOP.getQidSet, costInfo)

        val outCard =
          if (aggOP.getGroupByAttrs.isEmpty) 1.0
          else Catalog.getGroupNum(aggOP.getGroupByAttrs)

        val qidSet = fromArrayQidSet(aggOP.getQidSet)
        setOneValueForCardMap(costInfo.cardMap, qidSet, outCard)

        costInfo.totalCost += getCost(outCard, aggOP)
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
      costInfo.delCardMap.remove(qid)
    })

    // Update CardMap
    var newTotalCard = 0.0
    var newTotalDelCard = 0.0

    qidSet.foreach(qid => {
      val card = costInfo.cardMap.getOrElse(qid, {
        val qidStr = Utils.qidSetToString(qidArray)
        val diffStr = Utils.qidSetToString(costInfo.qidSet.toArray)
        System.err.println(s"No ${qid} $qidStr $diffStr")
        0.0
      })

      val delCard = costInfo.delCardMap.getOrElse(qid, {
        val qidStr = Utils.qidSetToString(qidArray)
        val diffStr = Utils.qidSetToString(costInfo.qidSet.toArray)
        System.err.println(s"Delete No ${qid} $qidStr $diffStr")
        0.0
      })
      newTotalCard += card
      newTotalDelCard += delCard
    })

    val curTotalCard = costInfo.cardMap(ROOTQID)
    costInfo.cardMap.put(ROOTQID, math.min(curTotalCard, newTotalCard))

    val curTotalDelCard = costInfo.delCardMap(ROOTQID)
    costInfo.delCardMap.put(ROOTQID, math.min(curTotalDelCard, newTotalDelCard))
  }

  private def filterBySelect(costInfo: CostInfo): Unit = {
    val diffSet = costInfo.qidSet.diff(costInfo.selectSet)
    if (diffSet.isEmpty) {
      costInfo.selectSet.clear()
      updateCardMapForSelect(costInfo.cardMap)
      updateCardMapForSelect(costInfo.delCardMap)
    }
  }

  private def updateCardMapForSelect(cardMap: mutable.HashMap[Int, Double]): Unit = {
    val totalCard = cardMap(ROOTQID)
    if (totalCard < 1.0) return

    val selArray =
      cardMap.filter(_._1 != ROOTQID).map(pair => {
        pair._2 / totalCard
      })

    var selectivity = 1.0
    selArray.foreach(sel => {
      selectivity *= 1.0 - sel
    })
    selectivity = 1.0 - selectivity

    cardMap.put(ROOTQID, totalCard * selectivity)
  }

  private def getSelectivity(selectOperator: SelectOperator): Double = {
    val postFilterSelectivity = Catalog.getPostFilterSelectivity(selectOperator.getDFStr)
    selectOperator.getPredicates.map(pred => {
      Catalog.getSelectivity(pred.toString)
    }).foldRight(postFilterSelectivity)((A, B) => {
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

  private def mergePlanTree(op: PlanOperator,
                            otherOP: PlanOperator,
                            isSameQuery: Boolean): PlanOperator = {
    Utils.resetCopy(op)
    Utils.resetCopy(otherOP)

    val newOP = copyPlanTree(op)
    val newOtherOP = copyPlanTree(otherOP)

    val sigToState = mutable.HashMap.empty[String, (SubsumeState.Value, SelectOperator)]
    val mergedOP =
      if (isSameQuery) {
        val newMergedOP = Optimizer.mergeOpForSameQueryHelper(newOP, newOtherOP, sigToState)
        newMergedOP.setParents(newOP.parentOps ++ newOtherOP.parentOps)
        newMergedOP
      } else Optimizer.mergeOpForDiffQueryHelper(newOP, newOtherOP)
    mergedOP
  }

  private def copyPlanTree(op: PlanOperator): PlanOperator = {
    copyTreeHelper(Optimizer.dummyOperator, op)
  }

  private def copyTreeHelper(newParent: PlanOperator, op: PlanOperator): PlanOperator = {
    if (op.copyOP != null) {
      val newOP = op.copyOP
      for (i <- newOP.parentOps.indices) {
        if (newOP.parentOps(i) == Optimizer.dummyOperator) {
          newOP.parentOps(i) = newParent
        }
      }
      newOP
    } else {
      val newOP = op.copy()
      val childOps = op.childOps.map(copyTreeHelper(newOP, _))
      val parentOps = new Array[PlanOperator](op.parentOps.length)
      for (i <- parentOps.indices) {
        if (i == 0) parentOps(i) = newParent
        else parentOps(i) = Optimizer.dummyOperator
      }
      newOP.setChildren(childOps)
      newOP.setParents(parentOps)
      op.copyOP = newOP
      newOP
    }
  }
}
