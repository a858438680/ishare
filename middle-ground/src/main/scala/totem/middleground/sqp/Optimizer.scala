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

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import totem.middleground.sqp.CostEstimater.{CachedCostInfo, SubQueryCacheManager}

object SubsumeState extends Enumeration {
  type SubsumeState = Value

  val SubSet = Value("Subset")
  val SuperSet = Value("SuperSet")
  val SameSet = Value("SameSet")
  val IntersectSet = Value("IntersectSet")
}

object Optimizer {

  val InQPMatCardMap: mutable.HashMap[Int, (Double, Double)] =
    mutable.HashMap.empty[Int, (Double, Double)]

  val dummyOperator: PlanOperator = genDummyOperator()

  val finalWorkRatio = 1.0

  def initializeOptimizer(predFile: String): Unit = {
    Catalog.initCatalog(predFile)
  }

  def estimateQueryGraphCost(queryGraph: QueryGraph): Double = {
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryGraphWithSubqueries = findSubQueries(queryGraph)
    val isInQP = false

    var totalTime: Long = 0L
    val start = System.nanoTime()

    val (_, minCost) =
        decideNonUniformPace(queryGraphWithSubqueries, batchFinalWork, new Array[Int](0), isInQP)

    totalTime += (System.nanoTime() - start)/1000000
    println(s"Estimating query graph cost: ${totalTime}ms")

    minCost
  }

  def OptimizeUsingSQP(queryGraph: QueryGraph, enableUnshare: Boolean):
  QueryGraph = {
    val nonUniform = true
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryGraphWithPerOPFinalWork = getPerOPFinalWork(queryGraph, batchFinalWork)
    val queryGraphWithMQO = batchMQO(queryGraphWithPerOPFinalWork)
    val queryGraphWithSubqueries = findSubQueries(queryGraphWithMQO)
    val isInQP = false
    val queryGraphWithPace =
      decideExecutionPace(queryGraphWithSubqueries, batchFinalWork, nonUniform, isInQP)
    if (enableUnshare) {
      UnshareMQO(queryGraphWithPace, batchFinalWork)
    }
    else queryGraphWithPace
  }

  def OptimizeUsingBatchMQO(queryGraph: QueryGraph): QueryGraph = {
    val nonUniform = false
    val isInQP = false
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryGraphWithSubqueries = findSubQueries(batchMQO(queryGraph))
    decideExecutionPace(queryGraphWithSubqueries, batchFinalWork, nonUniform, isInQP)
  }

  def OptimizeWithoutSharing(queryGraph: QueryGraph): QueryGraph = {
    val nonUniform = false
    val isInQP = false
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryGraphWithSubqueries = findSubQueries(queryGraph)
    decideExecutionPace(queryGraphWithSubqueries, batchFinalWork, nonUniform, isInQP)
  }

  def OptimizeWithInQP(queryGraph: QueryGraph): QueryGraph = {
    val nonUniform = true
    val isInQP = true
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryWithAnnotatedBlockingOP = breakSingleQueryIntoSubqueries(queryGraph)
    val queryGraphWithSubqueries = findSubQueries(queryWithAnnotatedBlockingOP)
    decideExecutionPace(queryGraphWithSubqueries, batchFinalWork, nonUniform, isInQP)
  }

  def testOptimizerWithSQP(queryGraph: QueryGraph): Double = {
    val enableUnshare = true
    val nonUniform = true
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryGraphWithPerOPFinalWork = getPerOPFinalWork(queryGraph, batchFinalWork)
    val queryGraphWithMQO = batchMQO(queryGraphWithPerOPFinalWork)
    val queryGraphWithSubqueries = findSubQueries(queryGraphWithMQO)

    val start = System.nanoTime()
    val isInQP = false
    val queryGraphWithPace =
      decideExecutionPace(queryGraphWithSubqueries, batchFinalWork, nonUniform, isInQP)
    if (enableUnshare) {
      UnshareMQO(queryGraphWithPace, batchFinalWork)
    }

    val optTime = (System.nanoTime() - start)/1000000
    optTime
  }

  def testOptimizerWithInQP(queryGraph: QueryGraph): Double = {
    val nonUniform = true
    val isInQP = true
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryWithAnnotatedBlockingOP = breakSingleQueryIntoSubqueries(queryGraph)
    val queryGraphWithSubqueries = findSubQueries(queryWithAnnotatedBlockingOP)

    val start = System.nanoTime()
    decideExecutionPace(queryGraphWithSubqueries, batchFinalWork, nonUniform, isInQP)
    val optTime = (System.nanoTime() - start)/1000000
    optTime
  }

  def testOptimizerWithBatchMQO(queryGraph: QueryGraph): Double = {
    val nonUniform = false
    val isInQP = false
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryGraphWithSubqueries = findSubQueries(batchMQO(queryGraph))

    val start = System.nanoTime()
    decideExecutionPace(queryGraphWithSubqueries, batchFinalWork, nonUniform, isInQP)
    val optTime = (System.nanoTime() - start)/1000000
    optTime
  }

  def testOptimizerWithoutSharing(queryGraph: QueryGraph): Double = {
    val nonUniform = false
    val isInQP = false
    val batchFinalWork = getAllBatchFinalWork(queryGraph)
    val queryGraphWithSubqueries = findSubQueries(queryGraph)

    val start = System.nanoTime()
    decideExecutionPace(queryGraphWithSubqueries, batchFinalWork, nonUniform, isInQP)
    val optTime = (System.nanoTime() - start)/1000000
    optTime
  }

  private def batchMQO(queryGraph: QueryGraph): QueryGraph = {
    queryGraph.fullQidSet.foreach(qid => {
      val newQuery = OptimizeOneQuery(queryGraph.qidToQuery(qid))
      queryGraph.qidToQuery.put(qid, newQuery)
    })

    ConventionalMQO(queryGraph)
  }

  def OptimizeOneQuery(op: PlanOperator): PlanOperator = {
    genSignature(op)

    val sigToOperator = mutable.HashMap.empty[String, Array[PlanOperator]]
    val isSameQuery = true

    mergeSubExpression(op, sigToOperator, isSameQuery)

    op
  }

  def getMutliQueryCluster(queryGraph: QueryGraph):
  mutable.HashSet[mutable.HashSet[Int]] = {

    val cluster = mutable.HashSet.empty[mutable.HashSet[Int]]

    queryGraph.qidToQuery.foreach(pair => {
      val query = pair._2
      val oneCluster = new mutable.HashSet[Int]()
      query.getQidSet.foreach(oneCluster.add)
      cluster.add(oneCluster)
    })

    queryGraph.qidToQuery.foreach(pair => {
      getMultiQueryClusterHelper(pair._2, cluster)
    })

    cluster
  }

  private def getMultiQueryClusterHelper(op: PlanOperator,
                                         cluster: mutable.HashSet[mutable.HashSet[Int]]): Unit = {
    val qidSet = op.getQidSet

    val setToMerge = mutable.HashSet.empty[mutable.HashSet[Int]]
    qidSet.map(qid => {
      cluster.map(clusterSet => {
        if (clusterSet.contains(qid)) setToMerge.add(clusterSet)
      })
    })

    setToMerge.foreach(cluster.remove)
    val newSet = mutable.HashSet.empty[Int]
    setToMerge.foreach(oneSet => {
      oneSet.foreach(newSet.add)
    })
    cluster.add(newSet)

    op.childOps.foreach(getMultiQueryClusterHelper(_, cluster))
  }

  private def breakSingleQueryIntoSubqueries(queryGraph: QueryGraph): QueryGraph = {

    queryGraph.qidToQuery.foreach(pair =>
      if (pair._1 == 17 ||  pair._1 == 15 || pair._1 == 45 || pair._1 == 46) {
        annotateBlockingOperator(pair._2, dummyOperator, false)
      })

    queryGraph
  }

  private def annotateBlockingOperator(op: PlanOperator,
                                       parent: PlanOperator,
                                       metAgg: Boolean): Unit = {
    var isMetAgg = metAgg
    op match {
      case _: AggOperator =>
        assert(op.parentOps.length == 1)
        if (isMetAgg && !parent.isInstanceOf[SelectOperator]) {
          val newParentOps = op.parentOps ++ Array(dummyOperator)
          op.parentOps = newParentOps
        } else {
          isMetAgg = true
        }
      case _: JoinOperator =>
        isMetAgg = true
      case _ =>
    }

    op.childOps.foreach(annotateBlockingOperator(_, op, isMetAgg))
  }

  private def findSubQueries(queryGraph: QueryGraph): QueryGraph = {

    val qidToUids = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    val uidtoQid = mutable.HashMap.empty[Int, Int]

    val subQueryBuf = ArrayBuffer.empty[PlanOperator]
    val visited = mutable.HashSet.empty[PlanOperator]
    val qidToQuery = queryGraph.qidToQuery
    qidToQuery.foreach(pair => {subQueryBuf.append(pair._2)})
    qidToQuery.foreach(pair => {findSubQueryHelper(pair._2, subQueryBuf, visited)})
    val subQueries = subQueryBuf.toArray

    subQueries.zipWithIndex.foreach(pair => {
      val query = pair._1
      val uid = pair._2

      if (query.isInstanceOf[RootOperator]) {
        val rootQuery = query.asInstanceOf[RootOperator]
        uidtoQid.put(uid, rootQuery.getQidSet(0))
      }

      query.getQidSet.foreach(qid => {
        val uidSet =
          if (!qidToUids.contains(qid)) {
            val newUidSet = mutable.HashSet.empty[Int]
            qidToUids.put(qid, newUidSet)
            newUidSet
          } else qidToUids(qid)
        uidSet.add(uid)
      })

      generateName(query, uid)
    })

    val dependency = collectDependency(subQueries)

    QueryGraph(queryGraph.qidToQuery, queryGraph.qidToConstraints,
      queryGraph.fullQidSet, subQueries, qidToUids, uidtoQid,
      dependency, mutable.HashMap.empty[Int, Double], Array.empty[Int], Array.empty[Int])
  }


  def getAllBatchFinalWork(queryGraph: QueryGraph): mutable.HashMap[Int, Double] = {
    val batchFinalWork = mutable.HashMap.empty[Int, Double]
    queryGraph.qidToQuery.foreach(pair => {
      val qid = pair._1
      val query = pair._2
      batchFinalWork.put(qid, getBatchFinalWork(query) * finalWorkRatio)
    })
    batchFinalWork
  }

  def getPerOPFinalWork(queryGraph: QueryGraph,
                        batchFinalWork: mutable.HashMap[Int, Double]): QueryGraph = {
    val graphWithSubQueries = findSubQueries(queryGraph)
    val (graphWithBatchNum, _) = decideUniformPace(graphWithSubQueries, batchFinalWork)

    graphWithBatchNum.numBatches.zipWithIndex.foreach(pair => {
      val batchNum = pair._1
      val uid = pair._2
      val query = graphWithBatchNum.subQueries(uid)

      val subQueries = Array(query)
      val batchNums = Array(batchNum)
      val queryDependency = mutable.HashMap(0 -> mutable.HashSet.empty[Int])
      val execOrder = Array(0)
      val cacheManagers = Array(SubQueryCacheManager(0, null, null, false))
      val collectPerOPFinalWork = true
      val collectPerOPCardMap = false
      val isInQP = false
      val parentDependency = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
      CostEstimater.estimatePlanGraphCost(subQueries, cacheManagers, batchNums,
        execOrder, queryDependency, parentDependency, collectPerOPFinalWork,
        collectPerOPCardMap, isInQP)
    })

    graphWithBatchNum
  }

  private def getBatchFinalWork(op: PlanOperator): Double = {
    val subQueries = Array(op)
    val batchNums = Array(1)
    val queryDependency = mutable.HashMap(0 -> mutable.HashSet.empty[Int])
    val execOrder = Array(0)
    val cacheManagers = Array(SubQueryCacheManager(0, null, null, false))
    val collectPerOPFinalWork = false
    val collectPerOPCardMap = false
    val isInQP = false
    val parentDep = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    CostEstimater.estimatePlanGraphCost(subQueries, cacheManagers,
      batchNums, execOrder, queryDependency, parentDep,
      collectPerOPFinalWork, collectPerOPCardMap, isInQP)._1
  }

  private def decideExecutionPace(queryGraph: QueryGraph,
                                  batchFinalWork: mutable.HashMap[Int, Double],
                                  nonUniform: Boolean,
                                  isInQP: Boolean): QueryGraph = {
    var totalTime: Long = 0L

    val start = System.nanoTime()

    val (retQueryGraph, _) =
      if (nonUniform) {
        decideNonUniformPace(queryGraph, batchFinalWork, new Array[Int](0), isInQP)
      } else {
        decideUniformPace(queryGraph, batchFinalWork)
      }

    totalTime += (System.nanoTime() - start)/1000000

    println(s"Decide Execution Pace Time: ${totalTime}ms")

    retQueryGraph
  }

  private def decideUniformPace(queryGraph: QueryGraph,
                                batchFinalWork: mutable.HashMap[Int, Double]):
  (QueryGraph, Double) = {
    val subQueries = queryGraph.subQueries
    val numSubQ = subQueries.length
    val qidToUids = queryGraph.qidToUids
    val queryDependency = queryGraph.queryDependency
    val finalWorkConstraints = mutable.HashMap.empty[Int, Double]
    batchFinalWork.foreach(pair => {
      val qid = pair._1
      val batchWork = pair._2
      finalWorkConstraints.put(qid, batchWork * queryGraph.qidToConstraints(qid))
    })

    val finalBatchNums = new Array[Int](numSubQ)
    val newBatchNums = new Array[Int](numSubQ)
    val cluster = genClusters(queryGraph.fullQidSet, qidToUids)
    val execOrder = fromQueryDepToExecOrder(queryDependency, numSubQ)
    val cacheManagers = new Array[SubQueryCacheManager](numSubQ)
    for (uid <- 0 until numSubQ) cacheManagers(uid) = SubQueryCacheManager(uid, null, null, false)

    val collectPerOPFinalWork = false
    val collectPerOPCardMap = false
    val isInQP = false
    val parentDep = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    var totalWork: Double = 0.0

    cluster.foreach(qidCluster => {
      for (idx <- 0 until numSubQ) newBatchNums(idx) = 0

      val maxBatchNum = Catalog.getMaxBatchNum
      var curBatchNum = maxBatchNum
      var meetConstraint = true
      var perClusterTotalwork = 0.0

      while (meetConstraint && curBatchNum >= 1) {
        qidCluster.foreach(qid => {
          qidToUids(qid).foreach(uid => newBatchNums(uid) = curBatchNum)
        })

        val (curTotalWork, finalWork) =
          CostEstimater.estimatePlanGraphCost(subQueries,
            cacheManagers, newBatchNums, execOrder, queryDependency, parentDep,
            collectPerOPFinalWork, collectPerOPCardMap, isInQP)
        val qidToFinalWork =
          CostEstimater.buildQidFinalWork(qidToUids, finalWork)
        perClusterTotalwork = curTotalWork

        meetConstraint =
          !qidToFinalWork.exists(pair => {
            val qid = pair._1
            val curFinalWork = pair._2
            curFinalWork > finalWorkConstraints(qid)
          })

        if (meetConstraint) curBatchNum -= 1
      }

      totalWork += perClusterTotalwork

      curBatchNum = math.min(maxBatchNum, curBatchNum + 1)
      qidCluster.foreach(qid => {
        qidToUids(qid).foreach(uid => finalBatchNums(uid) = curBatchNum)
      })

    })

    // Generate scheduling order
    val qidOrder = finalWorkConstraints.toSeq.sortWith((pairA, pairB) => {
      pairA._2 < pairB._2
    }).map(_._1).toArray

    val uidOrderBuf = new ArrayBuffer[Int]()
    qidOrder.foreach(qid => {
      subQueries.foreach(subQuery => {
        if (subQuery.isInstanceOf[RootOperator] && subQuery.getQidSet(0) == qid) {
          uidOrderBuf.append(subQuery.subQueryUID)
        }
      })
    })

    val newQueryGraph =
      QueryGraph(queryGraph.qidToQuery, queryGraph.qidToConstraints, queryGraph.fullQidSet,
        queryGraph.subQueries, queryGraph.qidToUids, queryGraph.uidtoQid,
        queryGraph.queryDependency, finalWorkConstraints, uidOrderBuf.toArray, finalBatchNums)

    (newQueryGraph, totalWork)
  }

  private def decideNonUniformPace(queryGraph: QueryGraph,
                                   batchFinalWork: mutable.HashMap[Int, Double],
                                   cacheBatchNum: Array[Int],
                                   isInQP: Boolean):
  (QueryGraph, Double) = {
    val subQueries = queryGraph.subQueries
    val numSubQ = subQueries.length
    val qidToUids = queryGraph.qidToUids
    val queryDependency = queryGraph.queryDependency
    val finalWorkConstraints = mutable.HashMap.empty[Int, Double]
    batchFinalWork.foreach(pair => {
      val qid = pair._1
      val batchWork = pair._2
      finalWorkConstraints.put(qid, batchWork * queryGraph.qidToConstraints(qid))
    })

    val finalBatchNums = new Array[Int](numSubQ)
    val cluster = genClusters(queryGraph.fullQidSet, qidToUids)

    val parentDep = fromQueryDepToParentDep(queryDependency)
    val execOrder = fromQueryDepToExecOrder(queryDependency, numSubQ)
    val cacheManagers = new Array[SubQueryCacheManager](numSubQ)
    for (uid <- 0 until numSubQ) {
      val uidArray = findChildUidArray(uid, queryDependency)
      val cacheMap = mutable.HashMap.empty[String, CachedCostInfo]
      val useCache =
        if (isInQP) false
        else true
      cacheManagers(uid) = SubQueryCacheManager(uid, uidArray, cacheMap, useCache)
    }

    val useCachedBatchnum = cacheBatchNum.length == numSubQ
    var totalWork: Double = 0

    cluster.foreach(qidCluster => {

      var lastDecBatchUid = -1

      val uidCluster = mutable.HashSet.empty[Int]
      qidCluster.foreach(qid => {
        qidToUids(qid).foreach(uidCluster.add)
      })
      val activeUidSet = mutable.HashSet.empty[Int]
      uidCluster.foreach(activeUidSet.add)

      val maxBatchNum = Catalog.getMaxBatchNum
      var curBatchNum = new Array[Int](numSubQ)
      for (uid <- 0 until numSubQ) {
        if (uidCluster.contains(uid)) {
          curBatchNum(uid) =
            if (useCachedBatchnum) cacheBatchNum(uid)
            else maxBatchNum
        } else curBatchNum(uid) = 0
      }

      val collectPerOPFinalWork = false
      val collectPerOPCardMap = false
      var finish = false
      var perClusterTotalWork: Double = 0.0

      while (!finish) {

        val (curTotalWork, finalWork) =
          CostEstimater.estimatePlanGraphCost(subQueries, cacheManagers, curBatchNum, execOrder,
            queryDependency, parentDep, collectPerOPFinalWork, collectPerOPCardMap, isInQP)
        val qidToFinalWork =
          CostEstimater.buildQidFinalWork(qidToUids, finalWork)
        perClusterTotalWork = curTotalWork

        val existViolateConstrat =
          qidToFinalWork.exists(pair => {
            val qid = pair._1
            val curFinalWork = pair._2
            val violateConstraint = curFinalWork > finalWorkConstraints(qid)
            if (violateConstraint && lastDecBatchUid == -1) { // First Try
              qidToUids(qid).foreach(activeUidSet.remove)
            }
            violateConstraint
          })

        if (existViolateConstrat && lastDecBatchUid != -1) {
          activeUidSet.remove(lastDecBatchUid)
          curBatchNum(lastDecBatchUid) = curBatchNum(lastDecBatchUid) + 1
          lastDecBatchUid = -1
        }

        if (activeUidSet.nonEmpty) {
          val candSet = findUidCandidates(curBatchNum, uidCluster, parentDep)
          val realCandSet = candSet.intersect(activeUidSet)
          if (realCandSet.nonEmpty) {
            val pair = decreaseBatchNum(curBatchNum, realCandSet, subQueries, cacheManagers,
              execOrder, queryDependency, parentDep, isInQP)
            curBatchNum = pair._1
            lastDecBatchUid = pair._2
          } else {
            finish = true
            lastDecBatchUid = -1
          }
        } else {
          finish = true
        }
      }

      totalWork += perClusterTotalWork

      uidCluster.foreach(uid => {
        finalBatchNums(uid) =
          if (uid == lastDecBatchUid) {
            math.min(maxBatchNum, curBatchNum(uid) + 1)
          } else {
            curBatchNum(uid)
          }
      })

    })

    if (isInQP) {
      val collectPerOPFinalWork = false
      val collectPerOPCardMap = false
      CostEstimater.estimatePlanGraphCost(subQueries, cacheManagers, finalBatchNums, execOrder,
        queryDependency, parentDep, collectPerOPFinalWork, collectPerOPCardMap, isInQP)
    }

    // Generate scheduling order
    val qidOrder = finalWorkConstraints.toSeq.sortWith((pairA, pairB) => {
      pairA._2 < pairB._2
    }).map(_._1).toArray

    val uidOrderBuf = new ArrayBuffer[Int]()
    qidOrder.foreach(qid => {
      subQueries.foreach(subQuery => {
        if (subQuery.isInstanceOf[RootOperator] && subQuery.getQidSet(0) == qid) {
          uidOrderBuf.append(subQuery.subQueryUID)
        }
      })
    })

    val newQueryGraph =
      QueryGraph(queryGraph.qidToQuery, queryGraph.qidToConstraints, queryGraph.fullQidSet,
        queryGraph.subQueries, queryGraph.qidToUids, queryGraph.uidtoQid,
        queryGraph.queryDependency, finalWorkConstraints, uidOrderBuf.toArray, finalBatchNums)

    (newQueryGraph, totalWork)
  }

  private def findChildUidArray(uid: Int,
                                queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]]):
  Array[Int] = {
    val arrayBuf = new ArrayBuffer[Int]()
    val queue = new mutable.Queue[Int]()
    queue.enqueue(uid)
    while (queue.nonEmpty) {
      val curId = queue.dequeue()
      arrayBuf.append(curId)
      queryDependency(curId).foreach(queue.enqueue(_))
    }
    arrayBuf.toArray
  }

  private def findUidCandidates(curBatchNum: Array[Int],
                                uidCluster: mutable.HashSet[Int],
                                parentDependency: mutable.HashMap[Int, mutable.HashSet[Int]]):
  mutable.HashSet[Int] = {

    val candSet = mutable.HashSet.empty[Int]

    uidCluster.foreach(uid => {
      if (curBatchNum(uid) > 1) {
        val deps = parentDependency(uid)
        if (!deps.exists(parentUid => {
          curBatchNum(parentUid) >= curBatchNum(uid)
        })) {
          candSet.add(uid)
        }
      }
    })

    candSet
  }

  private def fromQueryDepToExecOrder(queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                                      numSubQ: Int):
  Array[Int] = {
    val candSet = mutable.HashSet.empty[Int]
    for (uid <- 0 until numSubQ) candSet.add(uid)

    val buf = new ArrayBuffer[mutable.HashSet[Int]]
    while (candSet.nonEmpty) {
      val tmpHashSet = new mutable.HashSet[Int]

      candSet.foreach(uid => {
        queryDependency.get(uid) match {
          case None => tmpHashSet.add(uid)
          case Some(depSet) =>
            val leafNode =
              !depSet.exists(depUID => {
                candSet.contains(depUID)
              })
            if (leafNode) tmpHashSet.add(uid)
        }
      })

      tmpHashSet.foreach(uid => {
        candSet.remove(uid)
      })

      buf.append(tmpHashSet)
    }

    val uidBuf = new ArrayBuffer[Int]
    buf.foreach(uidSet => {
      uidSet.foreach(uidBuf.append(_))
    })

    uidBuf.toArray
  }

  def fromQueryDepToParentDep(
              queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]]):
  mutable.HashMap[Int, mutable.HashSet[Int]] = {
    val parentDep = mutable.HashMap.empty[Int, mutable.HashSet[Int]]

    queryDependency.foreach(pair => {
      val parent = pair._1
      val children = pair._2
      if (!parentDep.contains(parent)) {
        parentDep.put(parent, mutable.HashSet.empty[Int])
      }

      children.foreach(child => {
        if (!parentDep.contains(child)) {
          parentDep.put(child, mutable.HashSet.empty[Int])
        }
        val parentSet = parentDep(child)
        parentSet.add(parent)
      })
    })

    parentDep
  }

  private def decreaseBatchNum(curBatchNum: Array[Int],
                               candUidSet: mutable.HashSet[Int],
                               subQueries: Array[PlanOperator],
                               cacheManagers: Array[SubQueryCacheManager],
                               execOrder: Array[Int],
                               queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                               parentDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
                               isInQP: Boolean):
  (Array[Int], Int) = {

    val uidToDec =
      candUidSet.map(uid => {
        (uid, computeIncrementability(curBatchNum, uid, subQueries,
          cacheManagers, execOrder, queryDependency, parentDependency, isInQP))
      }).foldRight((-1, Double.MaxValue))((A, B) => {
        if (A._2 < B._2) A
        else B
      })._1

    cacheManagers.foreach(_.clearCache())

    curBatchNum(uidToDec) -= 1
    (curBatchNum, uidToDec)
  }

  private def computeIncrementability(
              curBatchNum: Array[Int], uid: Int,
              subQueries: Array[PlanOperator],
              cacheManagers: Array[SubQueryCacheManager],
              execOrder: Array[Int],
              queryDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
              parentDependency: mutable.HashMap[Int, mutable.HashSet[Int]],
              isInQP: Boolean): Double = {
    val collectPerOPFinalWork = false
    val collectPerOPCardMap = false

    val (oldTotalWork, oldFinalWork) =
      CostEstimater.estimatePlanGraphCost(subQueries, cacheManagers, curBatchNum, execOrder,
        queryDependency, parentDependency, collectPerOPFinalWork, collectPerOPCardMap, isInQP)

    curBatchNum(uid) -= 1
    val (newTotalWork, newFinalWork) =
      CostEstimater.estimatePlanGraphCost(subQueries, cacheManagers, curBatchNum, execOrder,
        queryDependency, parentDependency, collectPerOPFinalWork, collectPerOPCardMap, isInQP)
    curBatchNum(uid) += 1

    val totalDiff = oldTotalWork - newTotalWork
    val finalDiff = oldFinalWork.zip(newFinalWork).map(pair => {
      val oldOne = pair._1
      val newOne = pair._2
      newOne - oldOne
    }).sum

    val incrementability = finalDiff/totalDiff

    if (isInQP && !includesNonIncrementableOP(subQueries(uid), true)) {
      incrementability + 1.0
    } else {
      incrementability
    }
  }

  private def includesNonIncrementableOP(op: PlanOperator,
                                         isSubQueryRoot: Boolean): Boolean = {
    val isRoot = false
    var include = false
    op match {
      case _: AggOperator if !isSubQueryRoot =>
        include = true
      case _ =>
        op.childOps.foreach(child => {
         include |= includesNonIncrementableOP(child, isRoot)
        })
    }
    include
  }

  private def genClusters(fullQidSet: mutable.HashSet[Int],
                          qidToUids: mutable.HashMap[Int, mutable.HashSet[Int]]):
  mutable.HashSet[mutable.HashSet[Int]] = {

    val cluster = mutable.HashSet.empty[mutable.HashSet[Int]]
    fullQidSet.foreach(qid => {
      cluster.add(mutable.HashSet(qid))
    })

    val setToMerge = mutable.HashSet.empty[mutable.HashSet[Int]]
    fullQidSet.foreach(qid => {

      cluster.foreach(clusterSet => {
        if (clusterSet.exists(clusterID => {
          qidToUids(qid).intersect(qidToUids(clusterID)).nonEmpty
        })) {
          setToMerge.add(clusterSet)
        }
      })

      setToMerge.foreach(cluster.remove)
      val newSet = mutable.HashSet.empty[Int]
      setToMerge.foreach(oneSet => {
        oneSet.foreach(newSet.add)
      })
      cluster.add(newSet)

      setToMerge.clear()

    })

    cluster
  }

  private def findSubQueryHelper(op: PlanOperator,
                             subQueries: ArrayBuffer[PlanOperator],
                             visited: mutable.HashSet[PlanOperator]): Unit = {
    if (op.parentOps.length > 1 && !visited.contains(op)) {
      subQueries.append(op)
      visited.add(op)
    }

    op.childOps.foreach(findSubQueryHelper(_, subQueries, visited))
  }

  private def generateName(op: PlanOperator, uid: Int): Unit = {
    val qidSet = op.getQidSet

    val qidBuf = new StringBuffer()
    for (idx <- qidSet.indices) {
      qidBuf.append("_")
      qidBuf.append(qidSet(idx))
    }
    val qidStr = qidBuf.toString

    val qName = s"Q$uid$qidStr"

    op.setSubQueryName(qName)
    op.setUID(uid)
  }

  private def collectDependency(subQueries: Array[PlanOperator]):
  mutable.HashMap[Int, mutable.HashSet[Int]] = {
    val queryDependency = mutable.HashMap.empty[Int, mutable.HashSet[Int]]

    subQueries.foreach(subQuery => {
      val dependency = mutable.HashSet.empty[Int]
      val isRoot = true
      collectDependencyHelper(subQuery, dependency, isRoot)
      queryDependency.put(subQuery.subQueryUID, dependency)
    })

    queryDependency
  }

  private def collectDependencyHelper(subQuery: PlanOperator,
                                     dependency: mutable.HashSet[Int],
                                     isRoot: Boolean): Unit = {
    if (subQuery.parentOps.length > 1 && !isRoot) {
     dependency.add(subQuery.subQueryUID)
    } else {
      val isRoot = false
      subQuery.childOps.foreach(child => {
        collectDependencyHelper(child, dependency, isRoot)
      })
    }
  }

  private def ConventionalMQO(queryGraph: QueryGraph): QueryGraph = {
    val sigToOperator = mutable.HashMap.empty[String, Array[PlanOperator]]
    val isSameQuery = false

    queryGraph.qidToQuery.foreach(pair => {
      val q = pair._2
      genSignature(q)
      mergeSubExpression(q, sigToOperator, isSameQuery)
    })

    queryGraph
  }

  private def UnshareMQO(queryGraph: QueryGraph,
                         batchFinalWork: mutable.HashMap[Int, Double]): QueryGraph = {
    // Collect the perOP cardMap
    var newQueryGraph = collectPerOPCardMap(queryGraph)
    val candSubqueries = findUnShareCandidateSubQueries(newQueryGraph)

    var unshareEstimationTime: Long = 0L
    var unsharePhysicalTime: Long = 0L

    candSubqueries.foreach(subQuery => {
      var start = System.nanoTime()
      val (nodeSet, cluster) = CostEstimater.UnshareOneSubQuery(subQuery)
      unshareEstimationTime += (System.nanoTime() - start)/1000000

      if (nodeSet.nonEmpty) {
        start = System.nanoTime()
        newQueryGraph =
          unShareSubQueryPhysically(subQuery, nodeSet, cluster, newQueryGraph, batchFinalWork)
        unsharePhysicalTime += (System.nanoTime() - start)/1000000
      }
    })

    println(s"UnShare Estimation time ${unshareEstimationTime}ms")
    println(s"UnShare Physical time ${unsharePhysicalTime}ms")

    newQueryGraph
  }

  private def unShareSubQueryPhysically(subQuery: PlanOperator,
                                 nodeSet: mutable.HashSet[PlanOperator],
                                 cluster: mutable.HashSet[mutable.HashSet[Int]],
                                 queryGraph: QueryGraph,
                                 batchFinalWork: mutable.HashMap[Int, Double]): QueryGraph = {
    val subQueryToBatchNum = genSubQueryToBatchNumMapping(queryGraph)
    val newSubQueries = breakIntoMultiSubQueries(subQuery, nodeSet, cluster)
    val batchNum = subQueryToBatchNum(subQuery)

    // Find new batch numbers
    subQueryToBatchNum.remove(subQuery)
    newSubQueries.foreach(newSubQuery => {
      if (newSubQuery.parentOps.length > 1) { // An independent subquery
        subQueryToBatchNum.put(newSubQuery, batchNum)
      } else { // Merged into an old subquery
        val oldSubQuery = findRootSubQuery(newSubQuery)
        val oldBatchNum = subQueryToBatchNum(oldSubQuery)
        subQueryToBatchNum.put(oldSubQuery, math.max(oldBatchNum, batchNum))
      }
    })

    // Rebuild dependencies
    val newQueryGraph = findSubQueries(queryGraph)
    incExecutionPace(newQueryGraph, batchFinalWork, subQueryToBatchNum)
  }

  private def incExecutionPace(queryGraph: QueryGraph,
                               batchFinalWork: mutable.HashMap[Int, Double],
                               subQueryToBatchNum: mutable.HashMap[PlanOperator, Int]):
  QueryGraph = {
    val subQueries = queryGraph.subQueries
    val numSubQ = subQueries.length
    val cachedBatchNum = new Array[Int](numSubQ)
    for (uid <- 0 until numSubQ) {
      cachedBatchNum(uid) = subQueryToBatchNum(subQueries(uid))
    }

    val isInQP = false
    decideNonUniformPace(queryGraph, batchFinalWork, cachedBatchNum, isInQP)._1
  }

  private def findRootSubQuery(node: PlanOperator): PlanOperator = {
    var root: PlanOperator = node
    while (!root.isInstanceOf[RootOperator]) {
      root = root.parentOps(0)
    }
    root
  }

  private def genSubQueryToBatchNumMapping(queryGraph: QueryGraph):
  mutable.HashMap[PlanOperator, Int] = {
    val subQueries = queryGraph.subQueries
    val subQueryToBatchNum = mutable.HashMap.empty[PlanOperator, Int]
    for (uid <- subQueries.indices) {
      subQueryToBatchNum.put(subQueries(uid),
        queryGraph.numBatches(uid))
    }
    subQueryToBatchNum
  }

  private def breakIntoMultiSubQueries(subQuery: PlanOperator,
                                       nodeSet: mutable.HashSet[PlanOperator],
                                       cluster: mutable.HashSet[mutable.HashSet[Int]]):
  mutable.HashSet[PlanOperator] = {

    val isRoot = true
    val parentNode: PlanOperator = null
    detachSubQuery(subQuery, parentNode, isRoot)

    val newSubQuerySet = mutable.HashSet.empty[PlanOperator]
    cluster.foreach(qidGroup => {
      val newSubQuery = genSubQueryWithQidGroup(subQuery, fromSetQid(qidGroup), isRoot, nodeSet)
      newSubQuerySet.add(newSubQuery)
    })

    newSubQuerySet
  }

  private def detachSubQuery(curNode: PlanOperator,
                             parentNode: PlanOperator,
                             isSubQueryRoot: Boolean): Unit = {
    val isRoot = false
    if (isSubQueryRoot) {
      val parentOps = curNode.parentOps
      parentOps.foreach(parent => {
        for (childIdx <- parent.childOps.indices) {
          if (parent.childOps(childIdx) == curNode) {
            parent.childOps(childIdx) = Optimizer.dummyOperator
          }
        }
      })
      curNode.childOps.foreach(detachSubQuery(_, curNode, isRoot))
    } else if (curNode.parentOps.length > 1) {
      val newParentOps = new ArrayBuffer[PlanOperator]()
      curNode.parentOps.foreach(parent => {
        if (parent != parentNode) newParentOps.append(parent)
      })
      curNode.setParents(newParentOps.toArray)
    } else {
      curNode.childOps.foreach(detachSubQuery(_, curNode, isRoot))
    }

  }

  private def genSubQueryWithQidGroup(op: PlanOperator,
                                      qidGroup: Array[Int],
                                      isSubQueryRoot: Boolean,
                                      nodeSet: mutable.HashSet[PlanOperator]): PlanOperator = {

    if (!nodeSet.contains(op)) return op

    val isRoot = false
    var newOP: PlanOperator = null
    op match {
      case selOP: SelectOperator if selOP.selectSet.intersect(qidGroup).isEmpty =>
        newOP = genSubQueryWithQidGroup(selOP.childOps(0), qidGroup, isRoot, nodeSet)

      case _ =>
        newOP = op.copy()
        newOP.setQidSet(qidGroup)

        val newChildOPArray = op.childOps.map(genSubQueryWithQidGroup(_, qidGroup, isRoot, nodeSet))
        setChildren(newOP, newChildOPArray)
        newOP.setParents(Array.empty[PlanOperator])

        op match {
          case selOP: SelectOperator =>
            val newSelSet = selOP.selectSet.intersect(qidGroup)
            newOP.asInstanceOf[SelectOperator].setSelectSet(newSelSet)
          case _ =>
        }
    }

    if (isSubQueryRoot) {
      val parentOPs = op.parentOps
      parentOPs.foreach(parent => {
        // TODO: This is buggy
        if (parent.getQidSet.intersect(qidGroup).nonEmpty) {
          for (childIdx <- parent.childOps.indices) {
            if (parent.childOps(childIdx) == dummyOperator) {
              parent.childOps(childIdx) = newOP
            }
          }
          newOP.parentOps = newOP.parentOps ++ Array(parent)
        }
      })
    }

    newOP
  }

  private def setChildren(op: PlanOperator, childOPArray: Array[PlanOperator]): Unit = {
    op.setChildren(childOPArray)
    childOPArray.foreach(child => {
      child.parentOps = child.parentOps ++ Array(op)
    })
  }

  private def fromArrayQid(qidArray: Array[Int]): mutable.HashSet[Int] = {
    val newSet = mutable.HashSet.empty[Int]
    qidArray.foreach(newSet.add)
    newSet
  }

  private def fromSetQid(qidSet: mutable.HashSet[Int]): Array[Int] = {
    val arrayBuf = mutable.ArrayBuffer.empty[Int]
    qidSet.foreach(arrayBuf.append(_))
    arrayBuf.toArray
  }

  private def collectPerOPCardMap(queryGraph: QueryGraph): QueryGraph = {
    val subQueries = queryGraph.subQueries
    val numSubQ = subQueries.length
    val queryDependency = queryGraph.queryDependency
    val finalBatchNums = queryGraph.numBatches

    val execOrder = fromQueryDepToExecOrder(queryDependency, numSubQ)
    val cacheManagers = new Array[SubQueryCacheManager](numSubQ)
    for (uid <- 0 until numSubQ) {
      val uidArray = new Array[Int](0)
      val cacheMap = mutable.HashMap.empty[String, CachedCostInfo]
      val enableCache = false
      cacheManagers(uid) = SubQueryCacheManager(uid, uidArray, cacheMap, enableCache)
    }

    val collectPerOPFinalWork = false
    val collectPerOPCardMap = true
    val isInQP = false
    val parentDependency = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    CostEstimater.estimatePlanGraphCost(subQueries, cacheManagers,
      finalBatchNums, execOrder, queryDependency, parentDependency,
      collectPerOPFinalWork, collectPerOPCardMap, isInQP)

    queryGraph
  }

  private def findUnShareCandidateSubQueries(queryGraph: QueryGraph):
  mutable.HashSet[PlanOperator] = {
    val subQueries = queryGraph.subQueries
    val queryDependency = queryGraph.queryDependency

    val rootUidSet = mutable.HashSet.empty[Int]
    subQueries.zipWithIndex.filter(pair => {
      pair._1 match {
        case _: RootOperator => true
        case _ => false
      }
    }).foreach(pair => {
      rootUidSet.add(pair._2)
    })

    // Only consider the first-layer subqueries
    val candidateUids = mutable.HashSet.empty[Int]
    val parentDep = fromQueryDepToParentDep(queryDependency)
    for (uid <- subQueries.indices) {
      val parentUids = parentDep(uid)
      if (parentUids.nonEmpty && parentUids.forall(rootUidSet.contains)) {
        candidateUids.add(uid)
      }
    }

    // Filter out queries that do not include join and agg
    val candSubQueries = mutable.HashSet.empty[PlanOperator]
    candidateUids.foreach(uid => {
      val subQuery = subQueries(uid)
      val isRoot = true
      if (involveJoinOrAgg(subQuery, isRoot)) {
        candSubQueries.add(subQuery)
      }
    })

    candSubQueries
  }

  private def involveJoinOrAgg(op: PlanOperator, isSubQueryRoot: Boolean): Boolean = {
    if (op.parentOps.length > 1 && !isSubQueryRoot) return false

    val isRoot = false
    op match {
      case _: JoinOperator => true
      case _: AggOperator => true
      case _ =>
        var ret = false
        op.childOps.foreach(child => {
          ret |= involveJoinOrAgg(child, isRoot)
        })
        ret
    }

  }

  private def assignOPId(op: PlanOperator, startId: Int): Int = {
    op.id = startId
    var id = op.id
    op.childOps.foreach(child => {
      id = assignOPId(child, id + 1)
    })
    id
  }

  private def genSignature(op: PlanOperator): Unit = {
    genSignatureHelper(op)
  }

  private def genSignatureHelper(op: PlanOperator): String = {
    val childOPSignatures = op.childOps.map(genSignatureHelper)
    val signature = op.genSigStr(childOPSignatures)

    signature
  }

  private val unSupportedOPSet = HashSet("left_semi", "left_anti", "left_outer", "cross")

  // TODO: this could be buggy when query text (not just the join) includes the key words
  private def containUnsupportedOP(signature: String): Boolean = {
    unSupportedOPSet.exists(signature.contains(_))
  }

  private def mergeSubExpression(otherOP: PlanOperator,
                                 sigToOP: mutable.HashMap[String, Array[PlanOperator]],
                                 isSameQuery: Boolean):
  Unit = {
    val signature = otherOP.signature
    val hasUnspportedOP = containUnsupportedOP(signature)

    if (sigToOP.contains(signature) && !hasUnspportedOP) {
      // Find valid operators
      val existingOPArray = sigToOP(signature)
      val validOPArray =
        existingOPArray.filter(existingOP => {
          !containTree(existingOP, otherOP) && !containTree(otherOP, existingOP)
        }).filter(existingOP => {
          var pass = true
          val existingQidSet = existingOP.getQidSet
          val otherQidSet = otherOP.getQidSet
          if (existingQidSet.length == 1 && otherQidSet.length == 1) {
            if (existingQidSet(0) == otherQidSet(0)) pass = false
          }
          pass || isSameQuery
        }).map(existingOP => {
          val benefit = CostEstimater.shareWithMatBenefit(existingOP, otherOP, isSameQuery)
          (existingOP, benefit)
        })
        .filter(_._2 > 0.0)

      if (validOPArray.nonEmpty) {
        val bestShareOP =
          validOPArray.foldRight((null: PlanOperator, 0.0))((pairA, pairB) => {
            if (pairA._2 > pairB._2) pairA
            else pairB
          })._1

        val newOP =
          if (isSameQuery) genMergedOperatorForSameQuery(bestShareOP, otherOP)
          else genMergedOperatorForDiffQuery(bestShareOP, otherOP)

        // Insert newOP to the mult-hashmap and remove the old one
        val arrayBuffer = new ArrayBuffer[PlanOperator]()
        existingOPArray.foreach(oneOP => {
          if (oneOP != bestShareOP) {
            arrayBuffer.append(oneOP)
          }
        })
        arrayBuffer.append(newOP)

        sigToOP.put(signature, arrayBuffer.toArray)
      } else {

        val arrayBuffer = new ArrayBuffer[PlanOperator]()
        existingOPArray.foreach(existingOP => {
          arrayBuffer.append(existingOP)
        })
        arrayBuffer.append(otherOP)
        sigToOP.put(signature, arrayBuffer.toArray)

        otherOP.childOps.foreach(mergeSubExpression(_, sigToOP, isSameQuery))
      }
    } else {
      if (!hasUnspportedOP) sigToOP.put(signature, Array(otherOP))
      otherOP.childOps.foreach(mergeSubExpression(_, sigToOP, isSameQuery))
    }
  }

  private def genMergedOperatorForSameQuery(op: PlanOperator,
                                            otherOP: PlanOperator): PlanOperator = {

    val sigToState = mutable.HashMap.empty[String, (SubsumeState.Value, SelectOperator)]
    val newOp = mergeOpForSameQueryHelper(op, otherOP, sigToState)

    // newOP has included the parent/child info of op
    // so here we only include otherOP's info
    newOp.setParents(op.parentOps ++ otherOP.parentOps)
    otherOP.parentOps.foreach(parent => { setNewChild(parent, otherOP, newOp) })

    // TODO: this is buggy, only works for TPC-H
    sigToState.foreach(pair => {

      val subsumeState = pair._2._1
      val selectOperator = pair._2._2

      subsumeState match {
        case SubsumeState.SubSet =>
          // Pull up the select operator and add them to the parents except the last one
          for (i <- newOp.parentOps.indices) {
            val parent = newOp.parentOps(i)
            if (i < newOp.parentOps.length -1) {
              if (!parent.isInstanceOf[SelectOperator]) {
                insertSelectBetween(parent, selectOperator, newOp)
              }
            }
          }

        case SubsumeState.SuperSet =>
          val parent = newOp.parentOps(newOp.parentOps.length - 1)
          insertSelectBetween(parent, selectOperator, newOp)
      }

    })

    newOp
  }

  def mergeOpForSameQueryHelper(op: PlanOperator,
              otherOP: PlanOperator,
              sigToState: mutable.HashMap[String, (SubsumeState.Value, SelectOperator)]):
  PlanOperator = {

    var newOP: PlanOperator = null

    if (op.isInstanceOf[SelectOperator] && otherOP.isInstanceOf[SelectOperator]) {

      val leftChild = op.childOps(0)
      val rightChild = otherOP.childOps(0)

      val leftSelect = op.asInstanceOf[SelectOperator]
      val rightSelect = otherOP.asInstanceOf[SelectOperator]

      val state = findSubumeState(leftSelect, rightSelect)
      state match {

        case SubsumeState.SubSet =>
          sigToState.put(leftSelect.signature,
            (state, leftSelect.copy().asInstanceOf[SelectOperator]))
          newOP = replaceOperator(leftSelect, rightSelect)

        case SubsumeState.SuperSet =>
          sigToState.put(rightSelect.signature,
            (state, rightSelect.copy().asInstanceOf[SelectOperator]))
          newOP = leftSelect

        case SubsumeState.SameSet =>
          val qid = leftSelect.getQidSet(0)
          val minFinalWork = math.min(leftSelect.getFinalWork(qid).getOrElse(-1.0),
            rightSelect.getFinalWork(qid).getOrElse(-1.0))
          if (minFinalWork > 0) leftSelect.setFinalWork(qid, minFinalWork)
          newOP = leftSelect

        case SubsumeState.IntersectSet =>
          System.err.println(s"Not supported intersectset: $leftSelect, $rightSelect")
          System.exit(1)
      }

      mergeOpForSameQueryHelper(leftChild, rightChild, sigToState)

    } else if (op.isInstanceOf[SelectOperator]) {

      val leftChild = op.childOps(0)
      sigToState.put(op.signature,
        (SubsumeState.SubSet, op.copy().asInstanceOf[SelectOperator]))
      removeOperator(op)
      newOP = mergeOpForSameQueryHelper(leftChild, otherOP, sigToState)

    } else if (otherOP.isInstanceOf[SelectOperator]) {

      val rightChild = otherOP.childOps(0)
      sigToState.put(otherOP.signature,
        (SubsumeState.SubSet, otherOP.copy().asInstanceOf[SelectOperator]))
      newOP = mergeOpForSameQueryHelper(op, rightChild, sigToState)

    } else {

      op.childOps.zip(otherOP.childOps).map(pair => {
        val thisOP = pair._1
        val thatOP = pair._2
        mergeOpForSameQueryHelper(thisOP, thatOP, sigToState)
      })

      val qid = op.getQidSet(0)
      val minFinalWork = math.min(op.getFinalWork(qid).getOrElse(-1.0),
            otherOP.getFinalWork(qid).getOrElse(-1.0))
      if (minFinalWork >= 0) op.setFinalWork(qid, minFinalWork)

      newOP = op
    }

    newOP
  }

  def findSubumeState(op: SelectOperator,
                              otherOP: SelectOperator): SubsumeState.Value = {
    val predSet = mutable.HashSet.empty[Predicate]
    val otherPredSet = mutable.HashSet.empty[Predicate]

    op.getPredicates.foreach(predSet.add)
    otherOP.getPredicates.foreach(otherPredSet.add)

    val subSet = isSubset(predSet, otherPredSet)
    val superSet = isSubset(otherPredSet, predSet)

    if (subSet && superSet) SubsumeState.SameSet
    else if (subSet) SubsumeState.SubSet
    else if (superSet) SubsumeState.SuperSet
    else SubsumeState.IntersectSet
  }

  private def isSubset(predSet: mutable.HashSet[Predicate],
                        otherPredSet: mutable.HashSet[Predicate]): Boolean = {
    var subset = true
    otherPredSet.foreach(pred => {
      if (!predSet.contains(pred)) subset = false
    })

    subset
  }


  private def insertSelectBetween(parent: PlanOperator,
                                  selectOperator: SelectOperator,
                                  child: PlanOperator): Unit = {
    setNewParent(child, parent, selectOperator)

    selectOperator.setChildren(Array(child))
    selectOperator.setParents(Array(parent))

    setNewChild(parent, child, selectOperator)
  }

  private def genMergedOperatorForDiffQuery(op: PlanOperator,
                                            otherOP: PlanOperator): PlanOperator = {
    val newOP = mergeOpForDiffQueryHelper(op, otherOP)
    newOP
  }

  def removeParent(child: PlanOperator, parent: PlanOperator): Unit = {
    val newParents = ArrayBuffer.empty[PlanOperator]
    var findOne = false
    child.parentOps.foreach(oldParent => {
      if (oldParent != parent) newParents.append(oldParent)
      else findOne = true
    })
    child.setParents(newParents.toArray)
    if (!findOne) System.err.println("Did not find parent")
  }

  def mergeOpForDiffQueryHelper(op: PlanOperator,
                                otherOP: PlanOperator): PlanOperator = {
    if (op == otherOP) return op

    var newOP: PlanOperator = null

    if (op.isInstanceOf[SelectOperator] && otherOP.isInstanceOf[SelectOperator]) {

      val leftChild = op.childOps(0)
      val rightChild = otherOP.childOps(0)

      val leftSelect = op.asInstanceOf[SelectOperator]
      val rightSelect = otherOP.asInstanceOf[SelectOperator]

      val state = findSubumeState(leftSelect, rightSelect)
      newOP =
        if (state == SubsumeState.SameSet) {
          mergeSelectSet(leftSelect, rightSelect)
          mergeQidSet(op, otherOP)
        } else {
          mergeQidSet(op, otherOP)
          mergeQidSet(otherOP, op)
          insertAfter(op, otherOP)
        }

      newOP.setParents(newOP.parentOps ++ otherOP.parentOps)
      otherOP.parentOps.foreach(setNewChild(_, otherOP, newOP))

      removeParent(rightChild, otherOP)
      mergeOpForDiffQueryHelper(leftChild, rightChild)

    } else if (op.isInstanceOf[SelectOperator]) {

      newOP = op
      mergeQidSet(op, otherOP)

      newOP.setParents(op.parentOps ++ otherOP.parentOps)
      otherOP.parentOps.foreach(setNewChild(_, otherOP, newOP))
      otherOP.setParents(Array.empty[PlanOperator])

      mergeOpForDiffQueryHelper(op.childOps(0), otherOP)

    } else if (otherOP.isInstanceOf[SelectOperator]) {

      val rightChild = otherOP.childOps(0)
      mergeQidSet(otherOP, op)
      newOP = insertBefore(op, otherOP)

      newOP.setParents(newOP.parentOps ++ otherOP.parentOps)
      otherOP.parentOps.foreach(setNewChild(_, otherOP, newOP))

      removeParent(rightChild, otherOP)
      mergeOpForDiffQueryHelper(op, rightChild)

    } else {

      newOP = mergeQidSet(op, otherOP)
      newOP.setParents(newOP.parentOps ++ otherOP.parentOps)
      otherOP.parentOps.foreach(setNewChild(_, otherOP, newOP))

      op.childOps.zip(otherOP.childOps).map(pair => {
        val thisOP = pair._1
        val thatOP = pair._2
        removeParent(thatOP, otherOP)
        mergeOpForDiffQueryHelper(thisOP, thatOP)
      })

    }

    newOP
  }

  def mergeQidSet(left: PlanOperator,
                  right: PlanOperator): PlanOperator = {
    val qidSet = new mutable.HashSet[Int]()
    left.getQidSet.foreach(qidSet.add)
    right.getQidSet.foreach(qidSet.add)
    left.setQidSet(qidSet.toArray)

    right.getQidSet.foreach(qid => {
      right.getFinalWork(qid) match {
        case Some(finalWork) => left.setFinalWork(qid, finalWork)
        case None =>
      }
    })

    left
  }

  def mergeSelectSet(left: SelectOperator,
                             right: SelectOperator): PlanOperator = {
    val selectSet = new mutable.HashSet[Int]()
    left.getSelectSet.foreach(selectSet.add)
    right.getSelectSet.foreach(selectSet.add)
    left.setSelectSet(selectSet.toArray)
    left
  }

  // For operators with a single child
  private def removeOperator(op: PlanOperator): Unit = {
    val opChild = op.childOps
    val opParent = op.parentOps

    opChild(0).setParents(opParent)
    opParent.foreach(setNewChild(_, op, opChild(0)))
  }

  private def replaceOperator(left: PlanOperator, right: PlanOperator): PlanOperator = {
    val newOP = right.copy()

    newOP.setParents(left.parentOps)
    newOP.setChildren(left.childOps)

    setNewParent(left.childOps(0), left, newOP)
    left.parentOps.foreach(setNewChild(_, left, newOP))

    newOP
  }

  private def insertAfter(left: PlanOperator, right: PlanOperator): PlanOperator = {
    val newChild = right.copy()

    val leftChild = left.childOps
    left.setChildren(Array(newChild))

    newChild.setChildren(leftChild)
    newChild.setParents(Array(left))

    setNewParent(leftChild(0), left, newChild)

    left
  }

  private def insertBefore(left: PlanOperator, right: PlanOperator): PlanOperator = {
    val newParent = right.copy()

    val leftParent = left.parentOps
    left.setParents(Array(newParent))

    newParent.setChildren(Array(left))
    newParent.setParents(leftParent)

    leftParent.foreach(setNewChild(_, left, newParent))

    newParent
  }

  def setNewParent(child: PlanOperator,
                           oldParent: PlanOperator,
                           newParent: PlanOperator): Unit = {
    var parentIdx = -1
    child.parentOps.zipWithIndex.foreach(pair => {
      val parent = pair._1
      val idx = pair._2

      if (parent == oldParent) parentIdx = idx
    })
    child.parentOps(parentIdx) = newParent
  }

  def setNewChild(parent: PlanOperator,
                          oldChild: PlanOperator,
                          newChild: PlanOperator): Unit = {
    if (parent == dummyOperator) return
    var childIdx = -1
    parent.childOps.zipWithIndex.foreach(pair => {
      val child = pair._1
      val idx = pair._2

      if (child == oldChild) childIdx = idx
    })
    parent.childOps(childIdx) = newChild
  }

  private def containTree(op: PlanOperator, tree: PlanOperator): Boolean = {

    var isContain = false

    if (op == tree) {
      isContain = true
    } else {
      op.childOps.foreach(child => {
        isContain = containTree(child, tree)
      })
    }

    isContain
  }

  private def involveJoin(op: PlanOperator): Boolean = {
    var hasJoin = false

    op match {
      case _: JoinOperator =>
        hasJoin = true
      case _ =>
        op.childOps.foreach(child => {
          if (involveJoin(child)) hasJoin = true
        })
    }

    hasJoin
  }

  private def genDummyOperator(): PlanOperator = {
    val qidSet = Array(0)
    val outputAttrs = mutable.HashSet.empty[String]
    val referencedAttrs = mutable.HashSet.empty[String]
    val aliasAttrs = mutable.HashMap.empty[String, String]
    val dfStr = ""

    new DummyOperator(qidSet, outputAttrs,
      referencedAttrs, aliasAttrs, dfStr)
  }

}
