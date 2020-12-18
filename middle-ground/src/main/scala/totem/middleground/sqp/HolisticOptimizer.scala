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

object HolisticOptimizer {

  def OptimizeUsingHolistic(queryGraph: QueryGraph): QueryGraph = {

    // Step 1: Optimize a single query first
    queryGraph.fullQidSet.foreach(qid => {
      val newQuery = Optimizer.OptimizeOneQuery(queryGraph.qidToQuery(qid))
      queryGraph.qidToQuery.put(qid, newQuery)
    })

    // Step 2: Extract SPJ subplans
    val spjMap = mutable.HashMap.empty[Int, mutable.HashSet[PlanOperator]]
    val parentMap = mutable.HashMap.empty[Int, mutable.HashSet[PlanOperator]]
    queryGraph.qidToQuery.foreach(pair => {
      val qid = pair._1
      val query = pair._2
      Utils.findSPJSubquery(query, qid, spjMap, parentMap)
    })

    // Step 3: Breaking a subplan into multiple individual joins
    val joinMapping = mutable.HashMap.empty[Int, mutable.HashSet[PlanOperator]]
    spjMap.foreach(pair => {
      val qid = pair._1
      val spjSet = pair._2
      breakIntoIndividualJoins(qid, spjSet, joinMapping)
    })

    // Step 4: MQO for SPJ subplans
    val joinGroups = groupingIndividualJoins(joinMapping)
    while (joinGroups.exists(_._2.size >= 2)) {
      shareOneJoin(joinMapping, joinGroups, queryGraph)
    }

    // Step 5: construct the subplan for unshared joins


    null
  }

  private def shareOneJoin (
              joinMapping: mutable.HashMap[Int, mutable.HashSet[PlanOperator]],
              joinGroups: mutable.HashMap[String, mutable.HashSet[PlanOperator]],
              queryGraph: QueryGraph): Unit = {
    var minSavedCost = Double.MaxValue
    var sharedJoinCond: String = ""

    var oldPlan = mutable.HashSet.empty[PlanOperator]
    var sharedQueries = mutable.HashSet.empty[Int]
    var sharedPlan = mutable.HashSet.empty[PlanOperator]

    // Find the candidate joins to share
    joinGroups.foreach(pair => {
      val joinCond = pair._1
      val joinSet = pair._2
      val quadruplet = computeSavedCost(joinSet, queryGraph)
      if (quadruplet._1 <= minSavedCost) {
        sharedJoinCond = joinCond
        minSavedCost = quadruplet._1
        oldPlan = quadruplet._2
        sharedQueries = quadruplet._3
        sharedPlan = quadruplet._4
      }
    })

    // Perform the sharing
    joinGroups.remove(sharedJoinCond)
    if (minSavedCost > 0) {
      performSharing(joinMapping, oldPlan, sharedQueries, sharedPlan)
    }
  }

  private def computeSavedCost(joinSet: mutable.HashSet[PlanOperator],
                               queryGraph: QueryGraph):
  (Double, mutable.HashSet[PlanOperator],
    mutable.HashSet[Int], mutable.HashSet[PlanOperator]) = {

    // Find the set of shared queries
    val sharedQueries = mutable.HashSet.empty[Int]
    joinSet.foreach(oneJoin => {
     sharedQueries.add(oneJoin.getQidSet(0))
    })

    // Find spanning plan and build oldPlan
    val oldPlan = findSpanningPlan(joinSet)

    // Generate a shared plan
    val newPlan = genSharedPlan(oldPlan, joinSet)

    // Compute cost for shared and unshared plan
    val oldCost = Optimizer.estimateQueryGraphCost(
      fromPlanGraphToQueryGraph(oldPlan, queryGraph))
    oldPlan.foreach(_.setParents(Array.empty[PlanOperator]))

    val newCost = Optimizer.estimateQueryGraphCost(
      fromPlanGraphToQueryGraph(newPlan, queryGraph))
    newPlan.foreach(_.setParents(Array.empty[PlanOperator]))

    val savedCost = oldCost - newCost

    (savedCost, oldPlan, sharedQueries, newPlan)
  }

  private def fromPlanGraphToQueryGraph(planGraph: mutable.HashSet[PlanOperator],
                                        queryGraph: QueryGraph): QueryGraph = {
    val fullQidSet = mutable.HashSet.empty[Int]
    planGraph.foreach(_.getQidSet.foreach(fullQidSet.add))

    val qidToConstraints = mutable.HashMap.empty[Int, Double]
    queryGraph.qidToConstraints.foreach(pair => {
      val qid = pair._1
      val constraint = pair._2

      if (fullQidSet.contains(qid)) qidToConstraints.put(qid, constraint)
    })

    // Generate queries
    val qidToQuery = mutable.HashMap.empty[Int, PlanOperator]
    fullQidSet.foreach(qid => {
      val rootOp = Utils.genRootOperator(qid)
      planGraph.foreach(op => {
        if (op.getQidSet.contains(qid)) {
          rootOp.setChildren(Array[PlanOperator](op))
        }
      })

      qidToQuery.put(qid, rootOp)
    })

    // Set parents for actual Ops
    planGraph.foreach(op => {
      val parentOps = new Array[PlanOperator](op.getQidSet.length)
      op.getQidSet.zipWithIndex.foreach(pair => {
        val qid = pair._1
        val idx = pair._2
        parentOps(idx) = qidToQuery(qid)
      })
    })

    val qidToFinalWork = mutable.HashMap.empty[Int, Double]
    val qidToUids = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    val uidToQid = mutable.HashMap.empty[Int, Int]
    val queries = Array.empty[PlanOperator]
    val queryDependency = mutable.HashMap.empty[Int, mutable.HashSet[Int]]
    val schedulingOrder = Array.empty[Int]
    val numBatches = Array.empty[Int]
    QueryGraph(qidToQuery, qidToConstraints, fullQidSet,
      queries, qidToUids, uidToQid, queryDependency, qidToFinalWork, schedulingOrder, numBatches)
  }

  private def genSharedPlan(oldPlan: mutable.HashSet[PlanOperator],
                            joinSet: mutable.HashSet[PlanOperator]):
  mutable.HashSet[PlanOperator] = {

    // copyPlanGraph
    val opToId = mutable.HashMap.empty[PlanOperator, Int]
    val idToOp = mutable.HashMap.empty[Int, PlanOperator]
    val visited = mutable.HashSet.empty[PlanOperator]
    oldPlan.foreach(trackTopologyHelper(_, opToId, idToOp, visited))

    val (newPlan, idToNewOp) = copyPlanGraph(oldPlan, opToId, idToOp)

    // generate new Joins we need to merge
    val newJoinSet = mutable.HashSet.empty[PlanOperator]
    joinSet.foreach(joinOp => {
      val id = opToId(joinOp)
      newJoinSet.add(idToNewOp(id))
    })

    // Now let's merge the joins
    val newSharedPlan = mutable.HashSet.empty[PlanOperator]
    var mergeTarget: PlanOperator = null
    newJoinSet.foreach(joinCand => {
      if (mergeTarget == null) mergeTarget = joinCand
      else {
        mergeOneJoinToAnother(mergeTarget, joinCand)
      }
    })
    newPlan.foreach(rootOp => {
      if (!newJoinSet.contains(rootOp) || rootOp == mergeTarget) {
        newSharedPlan.add(rootOp)
      }
    })

    newSharedPlan
  }

  private def mergeOneJoinToAnother(thisJoin: PlanOperator,
                                    otherJoin: PlanOperator): PlanOperator = {
    Optimizer.mergeQidSet(thisJoin, otherJoin)

    thisJoin.childOps.zipWithIndex.foreach(pair => {
      val childOp = pair._1
      val idx = pair._2
      val otherChildOp = otherJoin.childOps(idx)

      if (childOp.isInstanceOf[SelectOperator] || childOp.isInstanceOf[ScanOperator]) {

        Optimizer.removeParent(otherChildOp, otherJoin)
        Optimizer.mergeOpForDiffQueryHelper(childOp, otherChildOp)

      } else if (childOp.isInstanceOf[JoinOperator]) {

        assert(childOp == otherChildOp)
        Optimizer.removeParent(otherChildOp, otherJoin)

      } else {
        System.err.println(s"Operator ${childOp} should not be found in a SPJ subquery")
        System.exit(0)
      }

    })

    thisJoin
  }

  private def copyPlanGraph(planGraph: mutable.HashSet[PlanOperator],
                            opToId: mutable.HashMap[PlanOperator, Int],
                            idToOp: mutable.HashMap[Int, PlanOperator]):
  (mutable.HashSet[PlanOperator], mutable.HashMap[Int, PlanOperator]) = {

    // copy actual OPs
    val idToNewOp = mutable.HashMap.empty[Int, PlanOperator]
    idToOp.foreach(pair => {
      val id = pair._1
      val op = pair._2

      idToNewOp.put(id, op.copy())
    })

    // Now, let's connect them
    idToNewOp.foreach(pair => {
      val id = pair._1
      val newOP = pair._2
      val op = idToOp(id)

      val newChildOps = op.childOps.map(childOp => {
        val childId = opToId(childOp)
        idToNewOp(childId)
      })

      val newParentOps = op.parentOps.map(parentOp => {
        val parentId = opToId(parentOp)
        idToNewOp(parentId)
      })

      newOP.setChildren(newChildOps)
      newOP.setParents(newParentOps)
    })

    val newPlanGraph = mutable.HashSet.empty[PlanOperator]
    planGraph.foreach(oldOp => {
      val id = opToId(oldOp)
      val newOp = idToNewOp(id)
      newPlanGraph.add(newOp)
    })

    (newPlanGraph, idToNewOp)
  }

  private def trackTopologyHelper(op: PlanOperator,
                                  opToId: mutable.HashMap[PlanOperator, Int],
                                  idToOp: mutable.HashMap[Int, PlanOperator],
                                  visited: mutable.HashSet[PlanOperator]): Unit = {
    val id = opToId.size
    opToId.put(op, id)
    idToOp.put(id, op)
    visited.add(op)
    (op.parentOps ++ op.childOps).foreach(newOp => {
      if (!visited.contains(newOp)) {
        trackTopologyHelper(newOp, opToId, idToOp, visited)
      }
    })
  }

  private def findSpanningPlan(joinSet: mutable.HashSet[PlanOperator]):
  mutable.HashSet[PlanOperator] = {
    val oldPlan = mutable.HashSet.empty[PlanOperator]
    val visited = mutable.HashSet.empty[PlanOperator]
    joinSet.foreach(spanPlan(_, oldPlan, visited))

    oldPlan
  }

  private def spanPlan(op: PlanOperator,
                       oldPlan: mutable.HashSet[PlanOperator],
                       visited: mutable.HashSet[PlanOperator]): Unit = {
    visited.add(op)
    if (op.parentOps.isEmpty) oldPlan.add(op)

    (op.parentOps ++ op.childOps).foreach(newOp => {
      if (!visited.contains(newOp)) {
        spanPlan(newOp, oldPlan, visited)
      }
    })

  }

  private def performSharing(joinMapping: mutable.HashMap[Int, mutable.HashSet[PlanOperator]],
                             oldPlan: mutable.HashMap[Int, PlanOperator],
                             sharedQueries: mutable.HashSet[Int],
                             sharedPlan: mutable.HashMap[Int, PlanOperator]): Unit = {

  }

  private def groupingIndividualJoins(
              joinMapping: mutable.HashMap[Int, mutable.HashSet[PlanOperator]]):
  mutable.HashMap[String, mutable.HashSet[PlanOperator]] = {

    val joinGroups = mutable.HashMap.empty[String, mutable.HashSet[PlanOperator]]

    joinMapping.foreach(pair => {
      pair._2.foreach(joinOP => {
        val joinCond = joinOP.asInstanceOf[JoinOperator].getJoinCondition
        val joinSet = joinGroups.getOrElseUpdate(joinCond, mutable.HashSet.empty[PlanOperator])
        joinSet.add(joinOP)
      })
    })

    joinGroups.filter(pair => {
      pair._2.size >= 2
    })
  }

  private def breakIntoIndividualJoins(
              qid: Int,
              spjSet: mutable.HashSet[PlanOperator],
              joinMapping: mutable.HashMap[Int, mutable.HashSet[PlanOperator]]): Unit = {
    val joinSet = joinMapping.getOrElseUpdate(qid, mutable.HashSet.empty[PlanOperator])
    spjSet.foreach(subplan => {
      breakOneSubPlan(subplan, joinSet)
    })
  }

  private def breakOneSubPlan(planOperator: PlanOperator,
                              joinSet: mutable.HashSet[PlanOperator]): Unit = {
    val baseMapping = findBaseRelation(planOperator)
    generateJoins(planOperator, baseMapping, joinSet)
  }

  private def findBaseRelation(planOperator: PlanOperator):
  mutable.HashMap[String, PlanOperator] = {

    val baseMapping = mutable.HashMap.empty[String, PlanOperator]
    findBaseRelationHelper(planOperator, baseMapping)
    baseMapping
  }

  private def findBaseRelationHelper(planOperator: PlanOperator,
                                     baseMapping: mutable.HashMap[String, PlanOperator]): Unit = {
    planOperator match {
      case _: SelectOperator =>
        val tblName = extractTableName(planOperator)
        baseMapping.put(tblName, planOperator)

      case _: ScanOperator =>
        val tblName = extractTableName(planOperator)
        baseMapping.put(tblName, planOperator)

      case _ =>
        findBaseRelationHelper(planOperator, baseMapping)
    }
  }

  private def extractTableName(planOperator: PlanOperator): String = {
    var curOp = planOperator
    while (curOp.isInstanceOf[SelectOperator]) {
      curOp = curOp.childOps(0)
    }
    curOp.asInstanceOf[ScanOperator].getTableName
  }

  private def generateJoins(planOperator: PlanOperator,
                            baseMapping: mutable.HashMap[String, PlanOperator],
                            joinSet: mutable.HashSet[PlanOperator]): Unit = {
    genJoinHelper(planOperator, baseMapping, joinSet)
  }

  private def genJoinHelper(planOperator: PlanOperator,
                            baseMapping: mutable.HashMap[String, PlanOperator],
                            joinSet: mutable.HashSet[PlanOperator]): Unit = {

    planOperator match {
      case joinOp: JoinOperator =>
        val condSet = parseJoinCondition(joinOp.getJoinCondition)
        condSet.foreach(keyPair => {
          joinSet.add(constructJoin(joinOp.getQidSet, keyPair, baseMapping))
        })

        planOperator.childOps.foreach(genJoinHelper(_, baseMapping, joinSet))

      case _ =>
    }

  }

  private def parseJoinCondition(joinCondition: String): mutable.HashSet[(String, String)] = {
    val joinCondSet = mutable.HashSet.empty[(String, String)]

    joinCondition.split("and").foreach(singleJoin => {
      val joinKeys = singleJoin.split("===")
      assert(joinKeys.length == 2)
      val keyPair = orderJoinKeys(joinKeys)
      joinCondSet.add(keyPair)
    })

    joinCondSet
  }

  private def orderJoinKeys(joinKeys: Array[String]): (String, String) = {
    val tblA = Catalog.getTableNameFromKey(joinKeys(0))
    val tblB = Catalog.getTableNameFromKey(joinKeys(1))
    if (tblA.compareTo(tblB) <= 0) (joinKeys(0), joinKeys(1))
    else (joinKeys(1), joinKeys(0))
  }

  private def constructJoin(qidSet: Array[Int],
                            keyPair: (String, String),
                            baseMapping: mutable.HashMap[String, PlanOperator]): PlanOperator = {
    val outputAttrs = mutable.HashSet.empty[String]
    val referenceAttrs = mutable.HashSet[String](keyPair._1, keyPair._2)
    val aliasAttrs = mutable.HashMap.empty[String, String]
    val dfStr = ""
    val joinKey = Array[String](keyPair._1, keyPair._2)
    val joinOP = "==="
    val joinType = "inner"
    val postFilter = ""

    val newJoinOp = new JoinOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs,
      dfStr, joinKey, joinOP, joinType, postFilter)
    val tblLeft = Catalog.getTableNameFromKey(keyPair._1)
    val tblRight = Catalog.getTableNameFromKey(keyPair._2)

    val baseLeft = baseMapping(tblLeft)
    val baseRight = baseMapping(tblRight)

    val newLeft = copySubplan(baseLeft)
    val newRight = copySubplan(baseRight)

    newJoinOp.setChildren(Array[PlanOperator](newLeft, newRight))
    newJoinOp.setParents(Array.empty[PlanOperator])
    newLeft.setParents(Array[PlanOperator](newJoinOp))
    newRight.setParents(Array[PlanOperator](newJoinOp))

    newJoinOp
  }

  private def copySubplan(planOperator: PlanOperator): PlanOperator = {
    val newOp = planOperator.copy()
    val newChildOp = planOperator.childOps.map(copySubplan)
    newChildOp.foreach(op => {
      op.setParents(Array[PlanOperator](newOp))
    })
    newOp.setChildren(newChildOp)
    newOp
  }

}
