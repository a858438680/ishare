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

object SubsumeState extends Enumeration {
  type SubsumeState = Value

  val SubSet = Value("Subset")
  val SuperSet = Value("SuperSet")
  val SameSet = Value("SameSet")
  val IntersectSet = Value("IntersectSet")
}

object Optimizer {

  val dummyOperator = genDummyOperator()

  def initializeOptimizer(predFile: String): Unit = {
    Catalog.initCatalog(predFile)
  }

  def OptimizeUsingSQP(queryGraph: QueryGraph): QueryGraph = {

    val batchFinalWork = mutable.HashMap.empty[Int, Double]
    queryGraph.qidToQuery.foreach(pair => {
      val qid = pair._1
      val query = pair._2
      batchFinalWork.put(qid, getBatchFinalWork(query))
    })

    queryGraph.fullQidSet.foreach(qid => {
      val newQuery = OptimizeOneQuery(queryGraph.qidToQuery(qid))
      queryGraph.qidToQuery.put(qid, newQuery)
    })

    UnshareMQO(ConventionalMQO(queryGraph))

    val nonUniform = true
    decideExecutionPace(findSubQueries(queryGraph), batchFinalWork, nonUniform)
  }

  def OptimizeUsingBatchMQO(queryGraph: QueryGraph): QueryGraph = {

    val batchFinalWork = mutable.HashMap.empty[Int, Double]
    queryGraph.qidToQuery.foreach(pair => {
      val qid = pair._1
      val query = pair._2
      batchFinalWork.put(qid, getBatchFinalWork(query))
    })

    queryGraph.fullQidSet.foreach(qid => {
      val newQuery = OptimizeOneQuery(queryGraph.qidToQuery(qid))
      queryGraph.qidToQuery.put(qid, newQuery)
    })

    ConventionalMQO(queryGraph)

    val nonUniform = false
    decideExecutionPace(findSubQueries(queryGraph), batchFinalWork, nonUniform)
  }

  def OptimizedWithoutSharing(queryGraph: QueryGraph): QueryGraph = {

    val batchFinalWork = mutable.HashMap.empty[Int, Double]
    queryGraph.qidToQuery.foreach(pair => {
      val qid = pair._1
      val query = pair._2
      batchFinalWork.put(qid, getBatchFinalWork(query))
    })

    val nonUniform = false
    decideExecutionPace(findSubQueries(queryGraph), batchFinalWork, nonUniform)
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

  private def getBatchFinalWork(op: PlanOperator): Double = {
    val subQueries = Array(op)
    val batchNums = Array(1)
    val queryDependency = mutable.HashMap(0 -> mutable.HashSet.empty[Int])
    CostEstimater.estimatePlanGraphCost(subQueries, batchNums, queryDependency)._1
  }

  private def decideExecutionPace(queryGraph: QueryGraph,
                                  batchFinalWork: mutable.HashMap[Int, Double],
                                  nonUniform: Boolean): QueryGraph = {
    if (nonUniform) {
      System.err.println("Not Supported Nonuniform pace yet")
      System.exit(1)
    }

    decideUniformPace(queryGraph, batchFinalWork)
  }

  private def decideUniformPace(queryGraph: QueryGraph,
                                batchFinalWork: mutable.HashMap[Int, Double]): QueryGraph = {
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

    cluster.foreach(qidCluster => {
      for (idx <- 0 until numSubQ) newBatchNums(idx) = 0

      val maxBatchNum = Catalog.getMaxBatchNum
      var curBatchNum = maxBatchNum
      var meetConstraint = true

      while (meetConstraint && curBatchNum >= 1) {
        qidCluster.foreach(qid => {
          qidToUids(qid).foreach(uid => newBatchNums(uid) = curBatchNum)
        })

        val (_, finalWork) =
          CostEstimater.estimatePlanGraphCost(subQueries, newBatchNums, queryDependency)
        val qidToFinalWork =
          CostEstimater.buildQidFinalWork(qidToUids, finalWork)

        meetConstraint =
          !qidToFinalWork.exists(pair => {
            val qid = pair._1
            val curFinalWork = pair._2
            curFinalWork > finalWorkConstraints(qid)
          })

        curBatchNum -= 1
      }

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

    QueryGraph(queryGraph.qidToQuery, queryGraph.qidToConstraints, queryGraph.fullQidSet,
      queryGraph.subQueries, queryGraph.qidToUids, queryGraph.uidtoQid,
      queryGraph.queryDependency, finalWorkConstraints, uidOrderBuf.toArray, finalBatchNums)
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

  private def UnshareMQO(queryGraph: QueryGraph): QueryGraph = {
    queryGraph
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
          newOP = leftSelect

        case SubsumeState.IntersectSet =>
          System.err.println("Not supported intersectset")
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

  private def removeParent(child: PlanOperator, parent: PlanOperator): Unit = {
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
