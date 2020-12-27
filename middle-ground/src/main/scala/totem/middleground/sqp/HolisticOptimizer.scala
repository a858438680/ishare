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

  var isPaceconf = false

  def holisticOptimize(queryGraph: QueryGraph,
                       isSWOpt: Boolean,
                       usePaceConf: Boolean,
                       batchFinalWork: mutable.HashMap[Int, Double]): QueryGraph = {
    val start = System.nanoTime()
    isPaceconf = usePaceConf

    // Step 1: Optimize a single query first
    queryGraph.fullQidSet.foreach(qid => {
      val newQuery = Optimizer.OptimizeOneQuery(queryGraph.qidToQuery(qid))
      queryGraph.qidToQuery.put(qid, newQuery)
    })

    val finalSPJSet =
      if (!isSWOpt) {

        println("Optimizing using AJoin")

        // Step 2: Extract SPJ subplans
        val spjQidSet = mutable.HashSet.empty[Int]
        val spjMap = mutable.HashMap.empty[Int, PlanOperator]
        val parentMap = mutable.HashMap.empty[Int, PlanOperator]
        queryGraph.qidToQuery.foreach(pair => {
          val qid = pair._1
          val query = pair._2
          Utils.findSPJSubquery(query, qid, spjQidSet, spjMap, parentMap)
        })

        // Step 3: Breaking a subplan into multiple individual joins
        val joinMapping = mutable.HashMap.empty[Int, mutable.HashSet[PlanOperator]]
        spjMap.foreach(pair => {
          val qid = pair._1
          val spjSubtree = pair._2
          breakIntoIndividualJoins(qid, spjSubtree, joinMapping)
        })

        // Step 4: MQO for SPJ subplans
        val joinGroups = groupingIndividualJoins(joinMapping, queryGraph, batchFinalWork)
        while (joinGroups.nonEmpty) {
          shareOneJoin(joinMapping, joinGroups, queryGraph, batchFinalWork)
        }

        val joinOrderTime = (System.nanoTime() - start) / 1000000
        println(s"Join Ordering Time: ${joinOrderTime}ms")

        // Step 5: construct the subplan for unshared joins
        val genSPJMapping = mutable.HashMap.empty[Int, PlanOperator]
        joinMapping.foreach(pair => {
          val qid = pair._1
          val perQueryJoinSet = pair._2

          // val perQueryJoinMap = mutable.HashMap.empty[Int, PlanOperator]
          // val sortedJoin = mutable.ArrayBuffer.empty[PlanOperator]
          // perQueryJoinSet.foreach(sortedJoin.append(_))
          // sortedJoin.sortWith((A, B) => {
          //   A.toString > B.toString
          // }).zipWithIndex.foreach(pair => {
          //   perQueryJoinMap.put(pair._2, pair._1)
          // })
          // val perQueryJoinNum = perQueryJoinMap.size

          // var targetOp: PlanOperator = null
          // var targetIdx: Int = 0
          // while (perQueryJoinMap.nonEmpty) {
          //   var found = false
          //   targetOp = null
          //   for (idx <- 0 until perQueryJoinNum) {
          //     if (perQueryJoinMap.contains(idx)) {
          //       if (targetOp == null) {
          //         targetIdx = idx
          //         targetOp = perQueryJoinMap(idx)
          //       } else {
          //         val op = perQueryJoinMap(idx)
          //         found |= replaceOldOp(targetOp, op)
          //       }
          //     }
          //   }
          //   assert(found || perQueryJoinMap.size == 1)
          //   perQueryJoinMap.remove(targetIdx)
          // }

          val isShare = false
          var targetOp: PlanOperator = null
          while (perQueryJoinSet.nonEmpty) {
            var found = false
            targetOp = null
            perQueryJoinSet.foreach(op => {
              if (targetOp == null) targetOp = op
              else found |= replaceOldOp(targetOp, op, isShare)
            })
            assert(found || perQueryJoinSet.size == 1)
            perQueryJoinSet.remove(targetOp)
          }

          if (!targetOp.isInstanceOf[RootOperator]) {
            val rootOp = Utils.genRootOperator(qid)
            rootOp.setChildren(Array[PlanOperator](targetOp))
            targetOp.setParents(targetOp.parentOps ++ Array[PlanOperator](rootOp))
            targetOp = rootOp
          }
          genSPJMapping.put(qid, targetOp)
        })

        // Step 6: generate new df string
        // Fortunately, df strings of joins are not important,
        // we do not need to generate them

        // Step 7: link the generated SPJ graph back to non-SPJ parts
        assert(genSPJMapping.size == parentMap.size)
        genSPJMapping.foreach(pair => {
          val qid = pair._1
          val fakeParent = pair._2
          val spjSubTree = fakeParent.childOps(0)
          val parentOp = parentMap(qid)
          val oldSPJSubtree = spjMap(qid)

          var childIdx = -1
          parentOp.childOps.zipWithIndex.foreach(pair => {
            val child = pair._1
            val idx = pair._2
            if (child == oldSPJSubtree) childIdx = idx
          })
          assert(childIdx >= 0)
          parentOp.childOps(childIdx) = spjSubTree

          var parentIdx = -1
          spjSubTree.parentOps.zipWithIndex.foreach(pair => {
            val parent = pair._1
            val idx = pair._2
            if (parent == fakeParent) parentIdx = idx
          })
          assert(parentIdx >= 0)
          spjSubTree.parentOps(parentIdx) = parentOp
        })

        spjQidSet
      } else {
        mutable.HashSet.empty[Int]
      }

    // Step 8: MQO for non-SPJ parts
    val sharedQueries = mutable.HashSet.empty[Int]
    queryGraph.qidToQuery.foreach(pair => {Optimizer.genSignature(pair._2)})
    val sigMap = populateSigMap(finalSPJSet, queryGraph, sharedQueries)
    if (!isSWOpt) {
      println(s"Shared queries in join ordering phase: ${Utils.intSetToString(sharedQueries)}")
    }
    val sortedQid = mutable.ArrayBuffer.empty[Int]
    val remainingQueries = mutable.HashSet.empty[Int]
    queryGraph.fullQidSet.foreach(qid => {
      if (!sharedQueries.contains(qid)) {
        sortedQid.append(qid)
        remainingQueries.add(qid)
      }
    })
    sortedQid.toArray.sortWith(_ > _).foreach(qid => {
      val query = queryGraph.qidToQuery(qid)
      remainingQueries.remove(qid)
      println(s"Optimizing query $qid," +
        s"Remaining queries ${Utils.intSetToString(remainingQueries)}")
      mergeSubQuery(query, sigMap, qid, sharedQueries, queryGraph, batchFinalWork)
      sharedQueries.add(qid)
    })

    val totalTime = (System.nanoTime() - start)/1000000
    println(s"Holistic Optimization Time: ${totalTime}ms")

    queryGraph
  }

  private def mergeSubQuery(
              otherOP: PlanOperator,
              sigMap: mutable.HashMap[String, mutable.HashSet[PlanOperator]],
              qid: Int,
              sharedQueries: mutable.HashSet[Int],
              queryGraph: QueryGraph,
              batchFinalWork: mutable.HashMap[Int, Double]): Unit = {

    val signature = otherOP.signature
    val hasUnspportedOP = Optimizer.containUnsupportedOP(signature)

    if (sigMap.contains(signature) && !hasUnspportedOP) {
      // Find valid operators
      val existingOPSet = sigMap(signature)
      val validOPSet =
        existingOPSet.filter(existingOP => {
          !Optimizer.containTree(existingOP, otherOP) &&
            !Optimizer.containTree(otherOP, existingOP)
        }).filter(existingOP => {
          var pass = true
          val existingQidSet = existingOP.getQidSet
          val otherQidSet = otherOP.getQidSet
          if (existingQidSet.length == 1 && otherQidSet.length == 1) {
            if (existingQidSet(0) == otherQidSet(0)) pass = false
          }
          pass
        }).map(existingOP => {
          val benefit =
            holisticSharingBenefit(existingOP, otherOP, qid,
              sharedQueries, queryGraph, batchFinalWork)
          (existingOP, benefit)
        })
        .filter(_._2 > 0.0)

      if (validOPSet.nonEmpty) {
        val bestShareOP =
          validOPSet.foldRight((null: PlanOperator, 0.0))((pairA, pairB) => {
            if (pairA._2 > pairB._2) pairA
            else pairB
          })._1

        val newOP = Optimizer.genMergedOperatorForDiffQuery(bestShareOP, otherOP)

        // Insert newOP to the mult-hashmap and remove the old one
        existingOPSet.remove(bestShareOP)
        existingOPSet.add(newOP)

        newOP.childOps.foreach(addSignature(sigMap, _))

      } else {

        existingOPSet.add(otherOP)
        otherOP.childOps.foreach(mergeSubQuery(_, sigMap, qid,
          sharedQueries, queryGraph, batchFinalWork))
      }
    } else {
      if (!hasUnspportedOP) sigMap.put(signature, mutable.HashSet[PlanOperator](otherOP))
      otherOP.childOps.foreach(mergeSubQuery(_, sigMap, qid,
        sharedQueries, queryGraph, batchFinalWork))
    }
  }

  private def addSignature(sigMap: mutable.HashMap[String, mutable.HashSet[PlanOperator]],
                           planOperator: PlanOperator): Unit = {
    val signature = planOperator.signature
    if (sigMap.contains(signature)) sigMap(signature).add(planOperator)
    else sigMap.put(signature, mutable.HashSet[PlanOperator](planOperator))

    planOperator.childOps.foreach(addSignature(sigMap, _))
  }

  private def holisticSharingBenefit(existingOp: PlanOperator,
                                     otherOp: PlanOperator,
                                     curQid: Int,
                                     sharedQueries: mutable.HashSet[Int],
                                     queryGraph: QueryGraph,
                                     batchFinalWork: mutable.HashMap[Int, Double]): Double = {
    val oldPlan = mutable.HashSet.empty[PlanOperator]
    queryGraph.qidToQuery.foreach(pair => {
      val qid = pair._1
      val query = pair._2

      if (sharedQueries.contains(qid) || qid == curQid) oldPlan.add(query)
    })

    // copyPlanGraph
    val opToId = mutable.HashMap.empty[PlanOperator, Int]
    val idToOp = mutable.HashMap.empty[Int, PlanOperator]
    val visited = mutable.HashSet.empty[PlanOperator]
    oldPlan.foreach(trackTopologyHelper(_, opToId, idToOp, visited))

    val (newPlan, idToNewOp) = copyPlanGraph(oldPlan, opToId, idToOp)
    if (!opToId.contains(existingOp)) {
      val a = 0
    }
    val newExistingOp = idToNewOp(opToId(existingOp))
    val newOtherOp = idToNewOp(opToId(otherOp))

    // compute the cost of not sharing
    val addRootOp = false
    val noShareCost = Optimizer.estimateQueryGraphCost(
      fromPlanGraphToQueryGraph(newPlan, queryGraph, addRootOp), batchFinalWork, isPaceconf)

    Optimizer.genMergedOperatorForDiffQuery(newExistingOp, newOtherOp)
    val shareCost = Optimizer.estimateQueryGraphCost(
      fromPlanGraphToQueryGraph(newPlan, queryGraph, addRootOp), batchFinalWork, isPaceconf)

    noShareCost - shareCost
  }

  private def populateSigMap(spjQidSet: mutable.HashSet[Int],
                             queryGraph: QueryGraph,
                             sharedQuery: mutable.HashSet[Int]):
  mutable.HashMap[String, mutable.HashSet[PlanOperator]] = {

    val sigMap = mutable.HashMap.empty[String, mutable.HashSet[PlanOperator]]
    val visited = mutable.HashSet.empty[PlanOperator]

    queryGraph.qidToQuery.foreach(pair => {
      val qid = pair._1
      val query = pair._2
      if (spjQidSet.contains(qid) && hasSharedJoin(query)) {
        sharedQuery.add(qid)
        populateSigMapHelper(query, sigMap, visited)
      }
    })

    sigMap
  }

  private def populateSigMapHelper(op: PlanOperator,
                                   sigMap: mutable.HashMap[String, mutable.HashSet[PlanOperator]],
                                   visited: mutable.HashSet[PlanOperator]): Unit = {
    if (!visited.contains(op)) {
      val signature = op.signature
      val opSet = sigMap.getOrElseUpdate(signature, mutable.HashSet.empty[PlanOperator])
      opSet.add(op)
      visited.add(op)
    }

  }

  private def selectScanOnly(op: PlanOperator): Boolean = {
    var only = true
    if (op.isInstanceOf[SelectOperator] || op.isInstanceOf[ScanOperator]) {
      op.childOps.foreach(only &= selectScanOnly(_))
    } else {
      only = false
    }
    only
  }

  private def hasSharedJoin(op: PlanOperator): Boolean = {
    var hasShare = false

    if (op.parentOps.length >= 2) hasShare = true
    else op.childOps.foreach(hasShare |= hasSharedJoin(_))

    hasShare
  }

  private def shareOneJoin (
              joinMapping: mutable.HashMap[Int, mutable.HashSet[PlanOperator]],
              joinGroups: mutable.HashMap[String, mutable.HashSet[mutable.HashSet[PlanOperator]]],
              queryGraph: QueryGraph,
              batchFinalWork: mutable.HashMap[Int, Double]): Unit = {
    var maxSavedCost = Double.MinValue
    var sharedJoinCond: String = ""
    var winnerJoinSet: mutable.HashSet[PlanOperator] = null

    var oldPlan = mutable.HashSet.empty[PlanOperator]
    var sharedQueries = mutable.HashSet.empty[Int]
    var sharedPlan = mutable.HashSet.empty[PlanOperator]

    // Find the candidate joins to share
    joinGroups.foreach(pair => {
      val joinCond = pair._1
      val joinSuperSet = pair._2
      joinSuperSet.foreach(joinSet => {
        val quadruplet = computeSavedCost(joinSet, queryGraph, batchFinalWork)
        if (quadruplet._1 > maxSavedCost) {
          sharedJoinCond = joinCond
          winnerJoinSet = joinSet
          maxSavedCost = quadruplet._1
          oldPlan = quadruplet._2
          sharedQueries = quadruplet._3
          sharedPlan = quadruplet._4
        }
      })
    })

    // Perform the sharing
    joinGroups(sharedJoinCond).remove(winnerJoinSet)
    if (joinGroups(sharedJoinCond).isEmpty) joinGroups.remove(sharedJoinCond)
    if (maxSavedCost > 0) {
      maintainJoinMapping(joinMapping, oldPlan, sharedQueries, sharedPlan)
      maintainJoinGroups(joinGroups)
    }
  }

  private def computeSavedCost(joinSet: mutable.HashSet[PlanOperator],
                               queryGraph: QueryGraph,
                               batchFinalWork: mutable.HashMap[Int, Double]):
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
    val addRootOp = true
    val oldCost = Optimizer.estimateQueryGraphCost(
      fromPlanGraphToQueryGraph(oldPlan, queryGraph, addRootOp), batchFinalWork, isPaceconf)
    oldPlan.foreach(_.setParents(Array.empty[PlanOperator]))

    val newCost = Optimizer.estimateQueryGraphCost(
      fromPlanGraphToQueryGraph(newPlan, queryGraph, addRootOp), batchFinalWork, isPaceconf)
    newPlan.foreach(_.setParents(Array.empty[PlanOperator]))

    val savedCost = oldCost - newCost

    (savedCost, oldPlan, sharedQueries, newPlan)
  }

  private def fromPlanGraphToQueryGraph(planGraph: mutable.HashSet[PlanOperator],
                                        queryGraph: QueryGraph,
                                        addRootOp: Boolean): QueryGraph = {
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
    if (addRootOp) {
      fullQidSet.foreach(qid => {
        val rootOp = Utils.genRootOperator(qid)
        planGraph.foreach(op => {
          if (op.getQidSet.contains(qid)) {
            assert(rootOp.childOps.isEmpty)
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
        op.setParents(parentOps)
      })
    } else {
      planGraph.foreach(op => {
        qidToQuery.put(op.getQidSet(0), op)
      })
    }

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
    if (visited.contains(op)) return

    val id = opToId.size
    opToId.put(op, id)
    idToOp.put(id, op)
    visited.add(op)
    (op.parentOps ++ op.childOps).foreach(newOp => {
      trackTopologyHelper(newOp, opToId, idToOp, visited)
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
    if (visited.contains(op)) return

    visited.add(op)
    if (op.parentOps.isEmpty) oldPlan.add(op)

    (op.parentOps ++ op.childOps).foreach(newOp => {
      spanPlan(newOp, oldPlan, visited)
    })

  }

  private def maintainJoinGroups(
              joinGroups: mutable.HashMap[String, mutable.HashSet[mutable.HashSet[PlanOperator]]])
  : Unit = {
    val nonMatchGroups =
      mutable.HashMap.empty[String, mutable.HashSet[mutable.HashSet[PlanOperator]]]

    joinGroups.foreach(pair => {
      val joinCond = pair._1
      val joinSuperSet = pair._2
      joinSuperSet.foreach(joinSet => {
        var nonMatch = false
        var targetOp: PlanOperator = null
        joinSet.foreach(op => {
          if (targetOp == null) targetOp = op
          else nonMatch |= nonMatchOps(targetOp, op)
        })
        if (nonMatch) {
          val nonMatchSet =
            nonMatchGroups.getOrElseUpdate(joinCond,
              mutable.HashSet.empty[mutable.HashSet[PlanOperator]])
          nonMatchSet.add(joinSet)
        }
      })
    })

    nonMatchGroups.foreach(pair => {
      val joinCond = pair._1
      val nonMatchJoinSuperSet = pair._2
      val joinSuperSet = joinGroups(joinCond)
      nonMatchJoinSuperSet.foreach(joinSuperSet.remove)
      if (joinSuperSet.isEmpty) joinGroups.remove(joinCond)
    })
  }

  private def nonMatchOps(opA: PlanOperator, opB: PlanOperator): Boolean = {
    if (opA.isInstanceOf[JoinOperator] && opB.isInstanceOf[JoinOperator]) {
      val joinA = opA.asInstanceOf[JoinOperator].getJoinCondition
      val joinB = opB.asInstanceOf[JoinOperator].getJoinCondition
      if (joinA.compareTo(joinB) == 0) {
        var nonMatch = false
        opA.childOps.zip(opB.childOps).foreach(pair => {
          nonMatch |= nonMatchOps(pair._1, pair._2)
        })
        nonMatch
      } else true
    } else if (!opA.isInstanceOf[JoinOperator] && !opB.isInstanceOf[JoinOperator]) {
      val tblA = extractTableName(opA)
      val tblB = extractTableName(opB)
      tblA.compareTo(tblB) != 0
    } else true
  }

  // Maintain join mapping
  private def maintainJoinMapping(
                             joinMapping: mutable.HashMap[Int, mutable.HashSet[PlanOperator]],
                             oldPlan: mutable.HashSet[PlanOperator],
                             sharedQueries: mutable.HashSet[Int],
                             sharedPlan: mutable.HashSet[PlanOperator]): Unit = {
    val sharedOp = findSharedOp(sharedQueries, sharedPlan)

    // Remove old operators in joinMapping
    oldPlan.foreach(op => {
      assert(op.getQidSet.length == 1)
      val qid = op.getQidSet(0)
      if (sharedQueries.contains(qid)) {
        joinMapping(qid).remove(op)
      }
    })

    val isShare = true
    // replace oldOp
    sharedOp.getQidSet.foreach(qid => {
      val perQueryJoinSet = joinMapping(qid)
      var found = false
      perQueryJoinSet.foreach(op => {
        found |= replaceOldOp(sharedOp, op, isShare)
      })

      if (!found) { // No more joins to share
        val rootOp = Utils.genRootOperator(qid)
        rootOp.setChildren(Array[PlanOperator](sharedOp))
        sharedOp.setParents(sharedOp.parentOps ++ Array[PlanOperator](rootOp))
        assert(perQueryJoinSet.isEmpty)
        perQueryJoinSet.add(rootOp)
      }
    })
  }

  private def replaceOldOp(newOp: PlanOperator,
                           op: PlanOperator,
                           isShare: Boolean): Boolean = {
    val idxSet = mutable.HashSet.empty[Int]
    op.childOps.zipWithIndex.foreach(childPair => {
      val child = childPair._1
      val idx = childPair._2
      if (newOp.childOps.exists(newOpChild => {
        !nonMatchOps(newOpChild, child)
      })) {
        idxSet.add(idx)
      }
    })

    val (newCopyOp, newCopyOpParents) =
      if (isShare) (newOp, newOp.parentOps)
      else {
        val newOpParents = newOp.parentOps
        newOp.setParents(Array.empty[PlanOperator])

        val newPlanGraph = mutable.HashSet[PlanOperator](newOp)
        val opToId = mutable.HashMap.empty[PlanOperator, Int]
        val idToOp = mutable.HashMap.empty[Int, PlanOperator]
        val visited = mutable.HashSet.empty[PlanOperator]
        newPlanGraph.foreach(trackTopologyHelper(_, opToId, idToOp, visited))

        val (_, idToCopyOp) = copyPlanGraph(newPlanGraph, opToId, idToOp)
        (idToCopyOp(opToId(newOp)), newOpParents)
      }

    assert(idxSet.size < 2)
    idxSet.foreach(idx => {
      op.childOps(idx) = newCopyOp
      newCopyOp.setParents(newCopyOpParents ++ Array[PlanOperator](op))
    })

    if (!isShare) newOp.setParents(newCopyOpParents)
    idxSet.size == 1
  }

  private def findSharedOp(sharedQueries: mutable.HashSet[Int],
                           sharedPlan: mutable.HashSet[PlanOperator]):
  PlanOperator = {

    var sharedOp: PlanOperator = null
    sharedPlan.foreach(op => {
      if (op.getQidSet.exists(sharedQueries.contains)) {
        assert(sharedOp == null)
        sharedOp = op
      }
    })

    sharedOp
  }

  // private def findDirectTables(op: PlanOperator): mutable.HashSet[String] = {
  //   val impactedTableSet = mutable.HashSet.empty[String]
  //   op.childOps.foreach(child => {
  //     if (!child.isInstanceOf[JoinOperator]) impactedTableSet.add(extractTableName(child))
  //   })

  //   assert(impactedTableSet.nonEmpty)
  //   impactedTableSet
  // }

  private def groupingIndividualJoins(
              joinMapping: mutable.HashMap[Int, mutable.HashSet[PlanOperator]],
              queryGraph: QueryGraph,
              batchFinalWork: mutable.HashMap[Int, Double]):
  mutable.HashMap[String, mutable.HashSet[mutable.HashSet[PlanOperator]]] = {

    val joinGroups = mutable.HashMap.empty[String, mutable.HashSet[mutable.HashSet[PlanOperator]]]

    joinMapping.foreach(pair => {
      pair._2.foreach(joinOP => {
        val joinCond = joinOP.asInstanceOf[JoinOperator].getJoinCondition
        val joinSuperSet = joinGroups.getOrElseUpdate(joinCond,
          mutable.HashSet.empty[mutable.HashSet[PlanOperator]])
        insertIntoJoinSet(joinOP, joinSuperSet, queryGraph, batchFinalWork)
      })
    })

    val filterdJoinGroups =
      mutable.HashMap.empty[String, mutable.HashSet[mutable.HashSet[PlanOperator]]]
    joinGroups.foreach(pair => {
      val joinCond = pair._1
      val newJoinSuperSet = pair._2.filter(_.size >= 2)
      if (newJoinSuperSet.nonEmpty) filterdJoinGroups.put(joinCond, newJoinSuperSet)
    })
    filterdJoinGroups

  }

  private def insertIntoJoinSet(
              joinOp: PlanOperator,
              joinSuperSet: mutable.HashSet[mutable.HashSet[PlanOperator]],
              queryGraph: QueryGraph,
              batchFinalWork: mutable.HashMap[Int, Double]): Unit = {
    var winningSet: mutable.HashSet[PlanOperator] = null
    var maxSavedCost = Double.MinValue
    joinSuperSet.foreach(joinSet => {
      joinSet.add(joinOp)
      val savedCost = computeSavedCost(joinSet, queryGraph, batchFinalWork)._1
      if (savedCost > maxSavedCost) {
        winningSet = joinSet
        maxSavedCost = savedCost
      }
      joinSet.remove(joinOp)
    })

    if (maxSavedCost < 0) joinSuperSet.add(mutable.HashSet[PlanOperator](joinOp))
    else winningSet.add(joinOp)
  }

  private def breakIntoIndividualJoins(
              qid: Int,
              spjSubtree: PlanOperator,
              joinMapping: mutable.HashMap[Int, mutable.HashSet[PlanOperator]]): Unit = {
    val joinSet = joinMapping.getOrElseUpdate(qid, mutable.HashSet.empty[PlanOperator])
    breakOneSubPlan(spjSubtree, joinSet)
  }

  private def breakOneSubPlan(spjSubtree: PlanOperator,
                              joinSet: mutable.HashSet[PlanOperator]): Unit = {
    val baseMapping = findAllBaseRelations(spjSubtree)
    genJoinHelper(spjSubtree, baseMapping, joinSet)
  }

  private def findAllBaseRelations(planOperator: PlanOperator):
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
        planOperator.childOps.foreach(findBaseRelationHelper(_, baseMapping))
    }
  }

  private def extractTableName(planOperator: PlanOperator): String = {
    var curOp = planOperator
    while (curOp.isInstanceOf[SelectOperator]) {
      curOp = curOp.childOps(0)
    }
    curOp.asInstanceOf[ScanOperator].getTableName
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
      val joinKeys = singleJoin.split("===").map(_.trim)
      assert(joinKeys.length == 2)
      val keyPair = orderJoinKeys(joinKeys)
      joinCondSet.add(keyPair)
    })

    joinCondSet
  }

  private def orderJoinKeys(joinKeys: Array[String]): (String, String) = {
    val tblA = Catalog.getTableNameFromKey(Parser.extractRawColomn(joinKeys(0)))
    val tblB = Catalog.getTableNameFromKey(Parser.extractRawColomn(joinKeys(1)))
    if (tblA.compareTo(tblB) <= 0) (joinKeys(0), joinKeys(1))
    else (joinKeys(1), joinKeys(0))
  }

  private def constructJoin(qidSet: Array[Int],
                            keyPair: (String, String),
                            baseMapping: mutable.HashMap[String, PlanOperator]): PlanOperator = {
    val outputAttrs = mutable.HashSet.empty[String]
    val referenceAttrs = mutable.HashSet[String](Parser.extractRawColomn(keyPair._1),
      Parser.extractRawColomn(keyPair._2))
    val aliasAttrs = mutable.HashMap.empty[String, String]
    val dfStr = ""
    val joinKey = Array[String](keyPair._1, keyPair._2)
    val joinOP = "==="
    val joinType = "inner"
    val postFilter = ""

    val newJoinOp = new JoinOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs,
      dfStr, joinKey, joinOP, joinType, postFilter)
    val tblLeft = Catalog.getTableNameFromKey(Parser.extractRawColomn(keyPair._1))
    val tblRight = Catalog.getTableNameFromKey(Parser.extractRawColomn(keyPair._2))

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
