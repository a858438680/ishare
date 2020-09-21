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
  private val rootStartupCost = BASESTARTUPCOST * 0.1

  private case class CostInfo (qidSet: mutable.HashSet[Int],
                               selectSet: mutable.HashSet[Int],
                               cardMap: mutable.HashMap[Int, Double],
                               delCardMap: mutable.HashMap[Int, Double],
                               var totalCost: Double)

  case class CachedCostInfo(cardMap: mutable.HashMap[Int, Double],
                            delCardMap: mutable.HashMap[Int, Double],
                            totalCost: Double,
                            finalCost: Double)

  case class SubQueryCacheManager(queryUid: Int,
                                  uidArray: Array[Int],
                                  cacheMap: mutable.HashMap[String, CachedCostInfo],
                                  enableCache: Boolean) {

    val CACHESIZE = 1000

    def insertCache(batchNums: Array[Int],
                    cardMap: mutable.HashMap[Int, Double],
                    delCardMap: mutable.HashMap[Int, Double],
                    totalCost: Double,
                    finalCost: Double): Unit = {
      if (!enableCache) return
      val newCardMap = mutable.HashMap.empty[Int, Double]
      val newDelCardMap = mutable.HashMap.empty[Int, Double]
      cardMap.foreach(pair => {
        newCardMap.put(pair._1, pair._2)
      })
      delCardMap.foreach(pair => {
        newDelCardMap.put(pair._1, pair._2)
      })
      val cachedCostInfo = CachedCostInfo(newCardMap, newDelCardMap, totalCost, finalCost)
      cacheMap.put(genCacheId(batchNums), cachedCostInfo)
    }

    def getCache(batchNums: Array[Int]): Option[CachedCostInfo] = {
      if (!enableCache) return None
      cacheMap.get(genCacheId(batchNums))
    }

    def removeCache(batchNums: Array[Int], changeUid: Int): Unit = {
      if (!enableCache) return
      cacheMap.remove(genCacheId(batchNums))
      uidArray.foreach(uid => {
        if (uid != changeUid) {
          batchNums(uid) -= 1
          cacheMap.remove(genCacheId(batchNums))
          batchNums(uid) += 1
        }
      })
    }

    def clearCache(): Unit = {
      if (cacheMap.size >= CACHESIZE) cacheMap.clear()
    }

    private def genCacheId(batchNums: Array[Int]): String = {
      val cacheIDBuf = new StringBuffer()
      uidArray.foreach(uid => {
        cacheIDBuf.append(batchNums(uid))
      })
      cacheIDBuf.toString
    }

  }

  def estimatePlanGraphCost(subQueries: Array[PlanOperator],
                            cacheManagers: Array[SubQueryCacheManager],
                            batchNums: Array[Int],
                            execOrder: Array[Int],
                            queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                            parentDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                            collectPerOPFinalWork: Boolean,
                            collectPerOPCardMap: Boolean,
                            isInQP: Boolean):
  (Double, Array[Double]) = {

    val numSubQ = subQueries.length
    val totalWork = new Array[Double](numSubQ)
    val finalWork = new Array[Double](numSubQ)
    var totalWorkSum = 0.0

    if (isInQP) Optimizer.InQPMatCardMap.clear()

    subQueries.foreach(resetCostInfo)
    execOrder.foreach(uid => {
      cacheManagers(uid).getCache(batchNums) match {
        case Some(cachedCostInfo) =>
          totalWork(uid) = cachedCostInfo.totalCost
          finalWork(uid) = cachedCostInfo.finalCost
          totalWorkSum += cachedCostInfo.totalCost
          val subQueryRoot = subQueries(uid)
          if (!subQueryRoot.isInstanceOf[RootOperator]) { // Not root subquery
            cachedCostInfo.cardMap.foreach(pair => {
              subQueryRoot.cardMap.put(pair._1, pair._2)
            })
            cachedCostInfo.delCardMap.foreach(pair => {
              subQueryRoot.delCardMap.put(pair._1, pair._2)
            })
          }

        case None =>
          val batchNum = batchNums(uid)
          val parentBatchNum =
            if (!isInQP || parentDependency(uid).isEmpty) -1
            else {
              val parentUid = parentDependency(uid).max
              batchNums(parentUid)
            }

          for (batchId <- 0 until batchNum) {
            val collectFinalBatch = batchId == batchNum - 1 && collectPerOPFinalWork
            val oneExecWork =
              estimateOneExecutionCost(subQueries(uid), batchNum, parentBatchNum, uid,
                collectFinalBatch, collectPerOPCardMap, isInQP)
            totalWork(uid) += oneExecWork
            totalWorkSum += oneExecWork
            if (batchId == batchNum - 1) {
              finalWork(uid) = oneExecWork
            }
          }
          // Put into the cache
          cacheManagers(uid).insertCache(batchNums, subQueries(uid).cardMap,
            subQueries(uid).delCardMap, totalWork(uid), finalWork(uid))
      }
    })

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
                                       parentBatchNum: Int,
                                       uid: Int,
                                       collectFinalBatch: Boolean,
                                       collectPerOPCardMap: Boolean,
                                       isInQP: Boolean): Double = {
    val isSubQueryRoot = true
    val qidSet = subQuery.getQidSet
    incrementalCostHelper(subQuery, batchNum, parentBatchNum, uid, qidSet,
      collectFinalBatch, collectPerOPCardMap, isSubQueryRoot, isInQP).totalCost
  }

  private def incrementalCostHelper(op: PlanOperator,
                                    batchNum: Int,
                                    parentBatchNum: Int,
                                    uid: Int,
                                    subQueryQidSet: Array[Int],
                                    collectFinalBatch: Boolean,
                                    collectPerOPCardMap: Boolean,
                                    isSubQueryRoot: Boolean,
                                    isInQP: Boolean): CostInfo = {

    if (op.parentOps.length > 1 && !isSubQueryRoot) { // A materialize operator
      val qidSet = fromArrayQidSet(op.getQidSet)
      val selectSet = mutable.HashSet.empty[Int]

      val cardMap = readMat(op.cardMap, op.offSetMap, uid, qidSet, batchNum)
      val delCardMap = readMat(op.delCardMap, op.delOffSetMap, uid, qidSet, batchNum)

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

          if (collectFinalBatch) subQueryQidSet.foreach(op.setFinalWork(_, baseCost))

          costInfo

        case projOP: ProjectOperator =>
          val costInfo =
            incrementalCostHelper(projOP.childOps(0), batchNum, parentBatchNum, uid,
              subQueryQidSet, collectFinalBatch, collectPerOPCardMap, isRoot, isInQP)
          filterByQidSet(projOP.getQidSet, costInfo)
          if (collectPerOPCardMap) collectInputCard(projOP, costInfo.cardMap, costInfo.delCardMap)

          val baseSize = getTotalCard(costInfo)
          val baseCost = baseSize * PROJREADCOST + projStartupCost
          costInfo.totalCost += getMatCost(baseCost, baseSize, projOP)
          materialize(costInfo, projOP, fromArrayQidSet(projOP.getQidSet))

          if (collectFinalBatch) subQueryQidSet.foreach(projOP.setFinalWork(_, baseCost))

          costInfo

        case selOP: SelectOperator =>
          val costInfo =
            incrementalCostHelper(selOP.childOps(0), batchNum, parentBatchNum, uid,
              subQueryQidSet, collectFinalBatch, collectPerOPCardMap, isRoot, isInQP)
          filterByQidSet(selOP.getQidSet, costInfo)
          if (collectPerOPCardMap) collectInputCard(selOP, costInfo.cardMap, costInfo.delCardMap)

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

          if (collectFinalBatch) subQueryQidSet.foreach(selOP.setFinalWork(_, baseCost))

          costInfo

        case aggOP: AggOperator =>
          val costInfo =
            incrementalCostHelper(aggOP.childOps(0), batchNum, parentBatchNum, uid,
              subQueryQidSet, collectFinalBatch, collectPerOPCardMap, isRoot, isInQP)
          filterByQidSet(aggOP.getQidSet, costInfo)
          if (collectPerOPCardMap) collectInputCard(aggOP, costInfo.cardMap, costInfo.delCardMap)

          val realGroups =
            if (aggOP.getGroupByAttrs.isEmpty) 1.0
            else Catalog.getGroupNum(aggOP.getGroupByAttrs)
          val stateSize = aggOP.stateSize
          val inputInsert = costInfo.cardMap(ROOTQID)
          val inputDelete = costInfo.delCardMap(ROOTQID)
          aggOP.stateSize = stateSize + inputInsert - inputDelete

          val (outInsert, outDelete) =
            if (realGroups == 1.0) {
              (1.0, 1.0)
            } else if (stateSize < realGroups) {
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
          val totalOutInsert = qidSet.size * outInsert
          val totalOutDelete = qidSet.size * outDelete
          setOneValueForCardMap(costInfo.cardMap, qidSet, outInsert)
          setOneValueForCardMap(costInfo.delCardMap, qidSet, outDelete)
          costInfo.cardMap.put(ROOTQID, totalOutInsert)
          costInfo.delCardMap.put(ROOTQID, totalOutDelete)

          var additionalCost = 0.0
          val aliasFunc = aggOP.getAliasFunc
          if (aliasFunc.exists(_._2.toLowerCase.compareTo("max") == 0)
            && aggOP.stateSize > 1.0) {
            additionalCost = (qidSet.size - 1) *
              ((inputDelete/aggOP.stateSize) * aggOP.stateSize * math.log(aggOP.stateSize))
          }

          var isLastBatch = false
          val repair =
            if (isInQP && parentBatchNum != -1) {
              aggOP.curBatchIdx += 1
              if (aggOP.curBatchIdx == batchNum) isLastBatch = true

              val curProgress = aggOP.curBatchIdx/batchNum.toDouble
              val parentProgress = (aggOP.parentBatchIdx + 1)/parentBatchNum.toDouble
              if (curProgress >= parentProgress) {
                aggOP.parentBatchIdx += 1
                true
              } else false
            } else true

          if (!repair) {
            val baseSize = inputInsert + inputDelete
            val baseCost = baseSize * AGGREADCOST + aggStartupCost
            costInfo.totalCost += baseCost
          } else {
            val baseSize = inputInsert + inputDelete
            val baseCost = baseSize * AGGREADCOST + aggStartupCost + additionalCost
            if (!isInQP || parentBatchNum == -1) {
              costInfo.totalCost += getMatCost(baseCost, totalOutInsert + totalOutDelete, aggOP)
            } else {
              val matCardMap = Optimizer.InQPMatCardMap
              val matCardPair = matCardMap.getOrElse(uid, (0.0, 0.0))
              val finalCard =
                if (isLastBatch) totalOutInsert + totalOutDelete
                else 0.0
              val newCard = matCardPair._1 + totalOutInsert + totalOutDelete
              val newMatCardPair = (newCard, finalCard)
              matCardMap.put(uid, newMatCardPair)
            }
            materialize(costInfo, aggOP, qidSet)

            if (collectFinalBatch) subQueryQidSet.foreach(aggOP.setFinalWork(_, baseCost))
          }

          costInfo

        case joinOP: JoinOperator =>
          val innerCostInfo =
            incrementalCostHelper(joinOP.childOps(Parser.INNER), batchNum, parentBatchNum,
              uid, subQueryQidSet, collectFinalBatch, collectPerOPCardMap, isRoot, isInQP)
          val outerCostInfo =
            incrementalCostHelper(joinOP.childOps(Parser.OUTER), batchNum, parentBatchNum,
              uid, subQueryQidSet, collectFinalBatch, collectPerOPCardMap, isRoot, isInQP)
          filterByQidSet(joinOP.getQidSet, innerCostInfo)
          filterByQidSet(joinOP.getQidSet, outerCostInfo)
          if (collectPerOPCardMap) {
            collectJoinInputCard(joinOP, innerCostInfo.cardMap, innerCostInfo.delCardMap,
              outerCostInfo.cardMap, outerCostInfo.delCardMap)
          }

          val qidSet = fromArrayQidSet(joinOP.getQidSet)
          val selectSet = mutable.HashSet.empty[Int]

          val outCardMap = createEmptyCardMap(qidSet)
          val delOutCardMap = createEmptyCardMap(qidSet)

          val outerKey = Parser.extractRawColomn(joinOP.getJoinKeys(Parser.OUTER))
          val innerKey = Parser.extractRawColomn(joinOP.getJoinKeys(Parser.INNER))
          val innerStateSizeMap = joinOP.innerStateSizeMap
          val outerStateSizeMap = joinOP.outerStateSizeMap
          val postFilterSelectivity = Catalog.getPostFilterSelectivity(joinOP.getPostFilter)
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

          if (collectFinalBatch) subQueryQidSet.foreach(joinOP.setFinalWork(_, baseCost))

          costInfo

        case rootOP: RootOperator =>
          val costInfo =
            incrementalCostHelper(rootOP.childOps(0), batchNum, parentBatchNum, uid,
              subQueryQidSet, collectFinalBatch, collectPerOPCardMap, isRoot, isInQP)
          costInfo.totalCost += rootStartupCost
          costInfo

        case _ =>
          System.err.println(s"Operator $op not supported yet")
          System.exit(1)
          null
      }
    }

  }

  private def collectInputCard(op: PlanOperator,
                               cardMap: mutable.HashMap[Int, Double],
                               delCardMap: mutable.HashMap[Int, Double]): Unit = {
    cardMap.foreach(pair => {
      val qid = pair._1
      val card = pair._2
      op.collectInputCard(qid, card, delCardMap(qid))
    })
  }

  private def collectJoinInputCard(op: JoinOperator,
                                   innerCardMap: mutable.HashMap[Int, Double],
                                   innerDelCardMap: mutable.HashMap[Int, Double],
                                   outerCardMap: mutable.HashMap[Int, Double],
                                   outerDelCardMap: mutable.HashMap[Int, Double]): Unit = {
    innerCardMap.foreach(pair => {
      val qid = pair._1
      val card = pair._2
      op.collectInnerInputCard(qid, card, innerDelCardMap(qid))
    })

    outerCardMap.foreach(pair => {
      val qid = pair._1
      val card = pair._2
      op.collectOuterInputCard(qid, card, outerDelCardMap(qid))
    })
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
                      uid: Int, qidSet: mutable.HashSet[Int], batchNum: Int):
  mutable.HashMap[Int, Double] = {
    val thisOffsetMap =
      if (!offsetMap.contains(uid)) {
        val newOffSetMap = createEmptyCardMap(qidSet)
        offsetMap.put(uid, newOffSetMap)
        newOffSetMap
      } else offsetMap(uid)

    val newCardMap = createEmptyCardMap(qidSet)
    (qidSet ++ mutable.HashSet(ROOTQID)).foreach(qid => {
      val readOffset = thisOffsetMap(qid)
      val perExecOffset = (cardMap(qid) + batchNum - 1) /batchNum
      val newOffset = math.min(readOffset + perExecOffset, cardMap(qid))
      thisOffsetMap.put(qid, newOffset)
      newCardMap.put(qid, newOffset - readOffset)
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

  def UnshareOneSubQuery(op: PlanOperator):
  (mutable.HashSet[PlanOperator], mutable.HashSet[mutable.HashSet[Int]]) = {
    val curNodeSet = mutable.HashSet.empty[PlanOperator]

    val optNodeSet = mutable.HashSet.empty[PlanOperator]
    val optQidCluster = mutable.HashSet.empty[mutable.HashSet[Int]]

    val qidSet = fromArrayQidSet(op.getQidSet)
    val finalWorkConstrainMap = mutable.HashMap.empty[Int, Double]

    curNodeSet.add(op)
    op.visited = true
    collectFinalWorkConstraintMap(op, qidSet, finalWorkConstrainMap)

    var noNewChildNode = false
    while (!noNewChildNode) {
      val (curQidCluster, curReducedTotalWork) =
        computeOptCluster(op, curNodeSet, qidSet, finalWorkConstrainMap)
      optQidCluster.clear()
      optNodeSet.clear()
      if (qidSet.intersect(mutable.HashSet(15, 37)).nonEmpty &&
        qidSet.size == 2) {
        optQidCluster.add(mutable.HashSet(15))
        optQidCluster.add(mutable.HashSet(37))
        curNodeSet.foreach(optNodeSet.add)
      }
      // else {
      else if (curReducedTotalWork > 0) {
      // if (curReducedTotalWork > 0.0) {
        curQidCluster.foreach(optQidCluster.add)
        curNodeSet.foreach(optNodeSet.add)
      }

      // Extend to a new plan
      noNewChildNode = true
      curNodeSet.foreach(oneNode => {
        oneNode.childOps.foreach(childOP => {
          if (noNewChildNode && !childOP.visited && childOP.parentOps.length <= 1) {
            noNewChildNode = false
            childOP.visited = true
            curNodeSet.add(childOP)
            collectFinalWorkConstraintMap(childOP, qidSet, finalWorkConstrainMap)
          }
        })
      })
    }

    (optNodeSet, optQidCluster)
  }

  private def collectFinalWorkConstraintMap(curNode: PlanOperator,
                                            qidSet: mutable.HashSet[Int],
                                            constraint: mutable.HashMap[Int, Double]): Unit = {
    qidSet.foreach(qid => {
      val oldConstraint = constraint.getOrElse(qid, 0.0)
      val nodeConstraint = curNode.getFinalWork(qid).getOrElse({
        println(s"Final work of $qid not found")
        0.0
      })
      constraint.put(qid, nodeConstraint + oldConstraint)
    })
  }

  private def computeOptCluster(rootNode: PlanOperator,
                                curNodeSet: mutable.HashSet[PlanOperator],
                                qidSet: mutable.HashSet[Int],
                                constraint: mutable.HashMap[Int, Double]):
  (mutable.HashSet[mutable.HashSet[Int]], Double) = {

    val cluster = mutable.HashSet.empty[mutable.HashSet[Int]]
    qidSet.foreach(qid => {
      cluster.add(mutable.HashSet[Int](qid))
    })
    val benefitCache =
      mutable.HashMap.empty[(mutable.HashSet[Int], mutable.HashSet[Int]), Double]
    val totalWorkCache =
      mutable.HashMap.empty[mutable.HashSet[Int], Double]
    val cacheForBatchNum = mutable.HashMap.empty[Int, (Double, Double)]

    // Populate the benefit Cache
    cluster.foreach(qidGroup => {
      cluster.foreach(otherQidGroup => {
        if (qidGroup != otherQidGroup) {
          val groupPair = getGroupPair(qidGroup, otherQidGroup)
          if (!benefitCache.contains(groupPair)) {
            val benefit =
              computeBenefitBetweenTwoGroups(qidGroup, otherQidGroup,
                rootNode, curNodeSet, constraint, totalWorkCache, cacheForBatchNum)
            benefitCache.put(groupPair, benefit)
          }
        }
      })
    })

    var hasBenefit = true
    while (hasBenefit) {
      val groupWithBenefit = benefitCache
        .foldLeft(((mutable.HashSet.empty[Int], mutable.HashSet.empty[Int]), 0.0))((A, B) => {
          if (A._2 > B._2) A
          else B
        })

      if (groupWithBenefit._2 >= 1.0) { // Let's merge the two groups together
        // Remove old groups
        val groupA = groupWithBenefit._1._1
        val groupB = groupWithBenefit._1._2
        cluster.remove(groupA)
        cluster.foreach(group => {
          benefitCache.remove(getGroupPair(groupA, group))
        })
        cluster.remove(groupB)
        cluster.foreach(group => {
          benefitCache.remove(getGroupPair(groupB, group))
        })

        // Add new groups
        val newGroup = groupA ++ groupB
        cluster.foreach(group => {
          val newGroupPair = getGroupPair(group, newGroup)
          val newBenefit =
            computeBenefitBetweenTwoGroups(newGroup, group, rootNode,
              curNodeSet, constraint, totalWorkCache, cacheForBatchNum)
          benefitCache.put(newGroupPair, newBenefit)
        })
        cluster.add(newGroup)

      } else hasBenefit = false
    }

    // Finally return the total estimated work reduced by unsharing
    val orgTotalWork = computeTotalWork(rootNode, curNodeSet, qidSet,
      getConstraintForGroup(qidSet, constraint), cacheForBatchNum)
    val unShareTotalWork = cluster.map(qidGroup => {
      computeTotalWork(rootNode, curNodeSet, qidGroup,
        getConstraintForGroup(qidGroup, constraint), cacheForBatchNum)
    }).sum

    (cluster, orgTotalWork - unShareTotalWork)
  }

  private def getGroupPair(qidGroup: mutable.HashSet[Int],
                           otherQidGroup: mutable.HashSet[Int]):
  (mutable.HashSet[Int], mutable.HashSet[Int]) = {
    val hashCode = qidGroup.hashCode()
    val otherHashCode = otherQidGroup.hashCode()
    if (hashCode <= otherHashCode) {
      (qidGroup, otherQidGroup)
    } else {
      (otherQidGroup, qidGroup)
    }
  }

  val groupOverhead = 1.0

  private def computeBenefitBetweenTwoGroups(qidGroupA: mutable.HashSet[Int],
              qidGroupB: mutable.HashSet[Int],
              rootNode: PlanOperator,
              nodeSet: mutable.HashSet[PlanOperator],
              constraint: mutable.HashMap[Int, Double],
              totalWorkCache: mutable.HashMap[mutable.HashSet[Int], Double],
              cacheForBatchNum: mutable.HashMap[Int, (Double, Double)]):
  Double = {
    val totalWorkA =
      totalWorkCache.getOrElseUpdate(qidGroupA, {
        computeTotalWork(rootNode, nodeSet, qidGroupA,
          getConstraintForGroup(qidGroupA, constraint), cacheForBatchNum)
      })
    val totalWorkB =
      totalWorkCache.getOrElseUpdate(qidGroupB, {
        computeTotalWork(rootNode, nodeSet, qidGroupB,
          getConstraintForGroup(qidGroupB, constraint), cacheForBatchNum)
      })

    val mergeGroup = qidGroupA ++ qidGroupB
    val totalWorkMerge =
      totalWorkCache.getOrElseUpdate(mergeGroup, {
        computeTotalWork(rootNode, nodeSet, mergeGroup,
          getConstraintForGroup(mergeGroup, constraint), cacheForBatchNum)
      })

    totalWorkA + totalWorkB - totalWorkMerge * groupOverhead
  }

  private def getConstraintForGroup(qidGroup: mutable.HashSet[Int],
                                    constraint: mutable.HashMap[Int, Double]): Double = {
    qidGroup.map(qid => constraint(qid)).min
  }

  private val normalizedBatchWork = 1000000.0

  private def computeTotalWork(rootNode: PlanOperator,
                               nodeSet: mutable.HashSet[PlanOperator],
                               qidGroup: mutable.HashSet[Int],
                               finalWorkConstraint: Double,
                               cacheForBatchNum: mutable.HashMap[Int, (Double, Double)]):
  Double = {
    var batchNum = 1
    val batchWork = computeSubQueryCost(rootNode, nodeSet, qidGroup, batchNum)._1
    val normalizeRatio = normalizedBatchWork/batchWork
    val normFinalWorkConstraint = normalizeRatio*finalWorkConstraint

    var curNormFinalWork = normalizedBatchWork
    var curNormTotalWork = normalizedBatchWork
    while (curNormFinalWork > normFinalWorkConstraint && batchNum < Catalog.getMaxBatchNum) {
      batchNum += 1
      val pair = cacheForBatchNum.getOrElseUpdate(batchNum, {
        val realPair = computeSubQueryCost(rootNode, nodeSet, qidGroup, batchNum)
        val normTotalWork = realPair._1 * normalizeRatio
        val normFinalWork = realPair._2 * normalizeRatio
        (normTotalWork, normFinalWork)
      })

      curNormTotalWork = pair._1
      curNormFinalWork = pair._2
    }

    curNormTotalWork/normalizeRatio
  }

  private def computeSubQueryCost(rootNode: PlanOperator,
                                  nodeSet: mutable.HashSet[PlanOperator],
                                  qidGroup: mutable.HashSet[Int],
                                  batchNum: Int): (Double, Double) = {
    resetSubQueryCostInfo(rootNode, nodeSet)
    var totalWork = 0.0
    var finalWork = 0.0
    for (batchId <- 0 until batchNum) {
      val oneExecWork =
        estimateOneSubQueryExecution(rootNode, nodeSet, qidGroup, batchNum).totalCost
      totalWork += oneExecWork
      if (batchId == batchNum - 1) finalWork = oneExecWork
    }
    (totalWork, finalWork)
  }

  private val emptyCostInfo =
    CostInfo(mutable.HashSet.empty[Int],
      mutable.HashSet.empty[Int],
      mutable.HashMap.empty[Int, Double],
      mutable.HashMap.empty[Int, Double],
      -1.0)

  private def estimateOneSubQueryExecution(curNode: PlanOperator,
                                           nodeSet: mutable.HashSet[PlanOperator],
                                           qidGroup: mutable.HashSet[Int],
                                           batchNum: Int): CostInfo = {
    if (!nodeSet.contains(curNode)) return emptyCostInfo

    curNode match {
      case scanOP: ScanOperator =>
        val tableSize = Catalog.getTableSize(scanOP.getTableName)
        val batchIdx = scanOP.batchIdx
        val batchSize = scanOneBatch(batchIdx, batchNum, tableSize)
        scanOP.batchIdx += 1

        val qidSet = qidGroup
        val selectSet = mutable.HashSet.empty[Int]

        val cardMap = createEmptyCardMap(qidSet)
        setOneValueForCardMap(cardMap, qidSet, batchSize)
        val delCardMap = createEmptyCardMap(qidSet)

        val baseCost = batchSize * SCANREADCOST + scanStartupCost
        val costInfo = CostInfo(qidSet, selectSet, cardMap, delCardMap, baseCost)

        costInfo

        case projOP: ProjectOperator =>
          var costInfo =
            estimateOneSubQueryExecution(projOP.childOps(0), nodeSet, qidGroup, batchNum)

          if (costInfo.totalCost < 0.0) {
            if (projOP.batchIdx == 0) getCardMapFromQidGroup(projOP, qidGroup)
            costInfo = scanLocalInput(projOP, qidGroup,
              projOP.batchIdx, projOP.cardMap, projOP.delCardMap, batchNum)
            projOP.batchIdx += 1
          }

          val baseSize = getTotalCard(costInfo)
          val baseCost = baseSize * PROJREADCOST + projStartupCost
          costInfo.totalCost += baseCost

          costInfo

        case selOP: SelectOperator =>
          var costInfo =
            estimateOneSubQueryExecution(selOP.childOps(0), nodeSet, qidGroup, batchNum)
          if (costInfo.totalCost < 0.0) {
            if (selOP.batchIdx == 0) getCardMapFromQidGroup(selOP, qidGroup)
            costInfo = scanLocalInput(selOP, qidGroup,
              selOP.batchIdx, selOP.cardMap, selOP.delCardMap, batchNum)
            selOP.batchIdx += 1
          }

          val baseSize = getTotalCard(costInfo)
          val baseCost = baseSize * SELREADCOST + selectStartupCost

          // apply select operator
          val selectivity = getSelectivity(selOP)
          val opSelectSet = selOP.getSelectSet.intersect(fromQidSet(qidGroup))
          opSelectSet.foreach(costInfo.selectSet.add)
          applySelectOperator(costInfo.cardMap, opSelectSet, selectivity)
          applySelectOperator(costInfo.delCardMap, opSelectSet, selectivity)

          // See whether we can filter
          filterBySelect(costInfo)

          costInfo.totalCost += baseCost
          costInfo

        case aggOP: AggOperator =>
          var costInfo =
            estimateOneSubQueryExecution(aggOP.childOps(0), nodeSet, qidGroup, batchNum)
          if (costInfo.totalCost < 0.0) {
            if (aggOP.batchIdx == 0) getCardMapFromQidGroup(aggOP, qidGroup)
            costInfo = scanLocalInput(aggOP, qidGroup,
              aggOP.batchIdx, aggOP.cardMap, aggOP.delCardMap, batchNum)
            aggOP.batchIdx += 1
          }

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

          val qidSet = qidGroup
          val totalOutInsert = qidSet.size * outInsert
          val totalOutDelete = qidSet.size * outDelete
          setOneValueForCardMap(costInfo.cardMap, qidSet, outInsert)
          setOneValueForCardMap(costInfo.delCardMap, qidSet, outDelete)
          costInfo.cardMap.put(ROOTQID, totalOutInsert)
          costInfo.delCardMap.put(ROOTQID, totalOutDelete)

          var additionalCost = 0.0
          val aliasFunc = aggOP.getAliasFunc
          if (aliasFunc.exists(_._2.toLowerCase.compareTo("max") == 0)
            && aggOP.stateSize > 1.0) {
            additionalCost = qidSet.size *
              ((inputDelete/aggOP.stateSize) * aggOP.stateSize * math.log(aggOP.stateSize))
          }

          val baseSize = inputInsert + inputDelete
          val baseCost = baseSize * AGGREADCOST + aggStartupCost + additionalCost
          costInfo.totalCost += baseCost

          costInfo

        case joinOP: JoinOperator =>
          var innerCostInfo =
            estimateOneSubQueryExecution(joinOP.childOps(Parser.INNER), nodeSet, qidGroup, batchNum)
          var outerCostInfo =
            estimateOneSubQueryExecution(joinOP.childOps(Parser.OUTER), nodeSet, qidGroup, batchNum)

          if (innerCostInfo.totalCost < 0.0) {
            if (joinOP.innerBatchIdx == 0) getJoinCardMapFromQidGroup(joinOP, qidGroup)
            innerCostInfo = scanLocalInput(joinOP, qidGroup,
              joinOP.innerBatchIdx, joinOP.innerCardMap, joinOP.innerDelCardMap, batchNum)
            joinOP.innerBatchIdx += 1
          }

          if (outerCostInfo.totalCost < 0.0) {
            if (joinOP.outerBatchIdx == 0 && innerCostInfo.totalCost >= 0.0) {
              getJoinCardMapFromQidGroup(joinOP, qidGroup)
            }
            outerCostInfo = scanLocalInput(joinOP, qidGroup,
              joinOP.outerBatchIdx, joinOP.outerCardMap, joinOP.outerDelCardMap, batchNum)
            joinOP.outerBatchIdx += 1
          }

          val qidSet = qidGroup
          val selectSet = mutable.HashSet.empty[Int]

          val outCardMap = createEmptyCardMap(qidSet)
          val delOutCardMap = createEmptyCardMap(qidSet)

          val outerKey = Parser.extractRawColomn(joinOP.getJoinKeys(Parser.OUTER))
          val innerKey = Parser.extractRawColomn(joinOP.getJoinKeys(Parser.INNER))
          val innerStateSizeMap = joinOP.innerStateSizeMap
          val outerStateSizeMap = joinOP.outerStateSizeMap
          val postFilterSelectivity = Catalog.getPostFilterSelectivity(joinOP.getPostFilter)
          applyOneJoin(innerCostInfo.cardMap, innerCostInfo.delCardMap, innerStateSizeMap,
            outerStateSizeMap, innerKey, outerKey, postFilterSelectivity, outCardMap,
            delOutCardMap, qidSet)
          applyOneJoin(outerCostInfo.cardMap, outerCostInfo.delCardMap, outerStateSizeMap,
            innerStateSizeMap, outerKey, innerKey, postFilterSelectivity, outCardMap,
            delOutCardMap, qidSet)

          val baseSize = getTotalCard(innerCostInfo) + getTotalCard(outerCostInfo)
          val baseCost = baseSize * JOINREADCOST + joinStartupCost
          val totalCost = innerCostInfo.totalCost + outerCostInfo.totalCost + baseCost
          val costInfo = CostInfo(qidSet, selectSet, outCardMap, delOutCardMap, totalCost)
          costInfo

        case _ =>
          System.err.println(s"Operator $curNode not supported yet")
          System.exit(1)
          null
    }
  }

  private def fromQidSet(qidGroup: mutable.HashSet[Int]): Array[Int] = {
    val arrayBuffer = mutable.ArrayBuffer.empty[Int]
    qidGroup.foreach(arrayBuffer.append(_))
    arrayBuffer.toArray
  }

  private def scanLocalInput(op: PlanOperator,
                             qidGroup: mutable.HashSet[Int],
                             inputBatchIdx: Int,
                             inputCardMap: mutable.HashMap[Int, Double],
                             inputCardDelMap: mutable.HashMap[Int, Double],
                             batchNum: Int): CostInfo = {
    val cardMap = createEmptyCardMap(qidGroup)
    val delCardMap = createEmptyCardMap(qidGroup)
    inputCardMap.foreach(pair => {
      val qid = pair._1
      val fullSize = pair._2
      cardMap.put(qid, scanOneBatch(inputBatchIdx, batchNum, fullSize))
    })
    inputCardDelMap.foreach(pair => {
      val qid = pair._1
      val fullSize = pair._2
      delCardMap.put(qid, scanOneBatch(inputBatchIdx, batchNum, fullSize))
    })
    val selectSet = mutable.HashSet.empty[Int]
    val costInfo = CostInfo(qidGroup, selectSet, cardMap, delCardMap, 0.0)
    val baseSize = getTotalCard(costInfo)
    val baseCost = baseSize * SCANREADCOST + scanStartupCost
    costInfo.totalCost += baseCost

    costInfo
  }

  private def scanOneBatch(batchIdx: Int, batchNum: Int, fullSize: Double): Double = {
    if (fullSize/batchNum >= Catalog.getMinBatchSize) {
      fullSize/batchNum
    } else {
      val tmpBatchSize =
        math.min(fullSize - (batchIdx.toDouble * Catalog.getMinBatchSize),
          Catalog.getMinBatchSize)
      math.max(tmpBatchSize, 0.0)
    }
  }

  private def getCardMapFromQidGroup(op: PlanOperator,
                                     qidGroup: mutable.HashSet[Int]): Unit = {
    val cardMap = op.cardMap
    val delCardMap = op.delCardMap

    // Update CardMap
    var newTotalCard = 0.0
    var newTotalDelCard = 0.0

    qidGroup.foreach(qid => {
      val cardPair = op.getInputCard(qid)
      cardMap.put(qid, cardPair._1)
      delCardMap.put(qid, cardPair._2)
      newTotalCard += cardPair._1
      newTotalDelCard += cardPair._2
    })

    val totalCardPair = op.getInputCard(ROOTQID)
    val curTotalCard = totalCardPair._1
    cardMap.put(ROOTQID, math.min(curTotalCard, newTotalCard))

    val curTotalDelCard = totalCardPair._2
    delCardMap.put(ROOTQID, math.min(curTotalDelCard, newTotalDelCard))
  }

  val deleteFactor = 1.0

  private def getJoinCardMapFromQidGroup(joinOP: JoinOperator,
                                         qidGroup: mutable.HashSet[Int]): Unit = {

    val innerCardMap = joinOP.innerCardMap
    val innerDelCardMap = joinOP.innerDelCardMap
    val outerCardMap = joinOP.outerCardMap
    val outerDelCardMap = joinOP.outerDelCardMap

    val realDeleteFactor =
      if (qidGroup.size > 1 &&
        ({qidGroup.exists(qid => {joinOP.getInnerInputCard(qid)._2 > 0})}
          || qidGroup.exists(qid => {joinOP.getOuterInputCard(qid)._2 > 0}))) {
        deleteFactor
      } else {
        1.0
      }

    // Update CardMap
    var newInnerTotalCard = 0.0
    var newInnerTotalDelCard = 0.0
    var newOuterTotalCard = 0.0
    var newOuterTotalDelCard = 0.0

    qidGroup.foreach(qid => {
      val innerCardPair = joinOP.getInnerInputCard(qid)
      innerCardMap.put(qid, innerCardPair._1 * realDeleteFactor)
      innerDelCardMap.put(qid, innerCardPair._2 * realDeleteFactor)
      newInnerTotalCard += innerCardPair._1 * realDeleteFactor
      newInnerTotalDelCard += innerCardPair._2 * realDeleteFactor

      val outerCardPair = joinOP.getOuterInputCard(qid)
      outerCardMap.put(qid, outerCardPair._1 * realDeleteFactor)
      outerDelCardMap.put(qid, outerCardPair._2 * realDeleteFactor)
      newOuterTotalCard += outerCardPair._1 * realDeleteFactor
      newOuterTotalDelCard += outerCardPair._2 * realDeleteFactor
    })

    val innerTotalCardPair = joinOP.getInnerInputCard(ROOTQID)
    val innerCurTotalCard = innerTotalCardPair._1
    innerCardMap.put(ROOTQID, math.min(innerCurTotalCard, newInnerTotalCard))

    val innerCurTotalDelCard = innerTotalCardPair._2
    innerDelCardMap.put(ROOTQID, math.min(innerCurTotalDelCard, newInnerTotalDelCard))

    val outerTotalCardPair = joinOP.getOuterInputCard(ROOTQID)
    val outerCurTotalCard = outerTotalCardPair._1
    outerCardMap.put(ROOTQID, math.min(outerCurTotalCard, newOuterTotalCard))

    val outerCurTotalDelCard = outerTotalCardPair._2
    outerDelCardMap.put(ROOTQID, math.min(outerCurTotalDelCard, newOuterTotalDelCard))
  }

  private def resetSubQueryCostInfo(op: PlanOperator,
                                    nodeSet: mutable.HashSet[PlanOperator]): Unit = {
    if (nodeSet.contains(op)) {
      op.resetCostInfo()
      op.childOps.foreach(resetSubQueryCostInfo(_, nodeSet))
    }
  }

}
