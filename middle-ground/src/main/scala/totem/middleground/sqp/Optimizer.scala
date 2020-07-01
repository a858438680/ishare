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

  def OptimizeMultiQuery(multiQuery: Array[PlanOperator]): Array[PlanOperator] = {

    val newMultiQuery = new Array[PlanOperator](multiQuery.length)
    multiQuery.zipWithIndex.foreach(pair => {
      val op = pair._1
      val idx = pair._2
      newMultiQuery(idx) = OptimizeOneQuery(op)
    })

    UnshareMQO(ConventionalMQO(newMultiQuery))

  }

  def OptimizeOneQuery(op: PlanOperator): PlanOperator = {
    genSignature(op)

    val sigToOperator = mutable.HashMap.empty[String, Array[PlanOperator]]
    val isSameQuery = true

    mergeSubExpression(op, sigToOperator, isSameQuery)

    op
  }

  private def ConventionalMQO(multiQuery: Array[PlanOperator]): Array[PlanOperator] = {
    val sigToOperator = mutable.HashMap.empty[String, Array[PlanOperator]]
    val isSameQuery = false

    multiQuery.foreach(q => {
      genSignature(q)
      mergeSubExpression(q, sigToOperator, isSameQuery)
    })

    multiQuery
  }

  private def UnshareMQO(multiQuery: Array[PlanOperator]): Array[PlanOperator] = {
    multiQuery
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

  private def mergeSubExpression(otherOP: PlanOperator,
                                 sigToOP: mutable.HashMap[String, Array[PlanOperator]],
                                 isSameQuery: Boolean):
  Unit = {
    val signature = otherOP.signature

    if (sigToOP.contains(signature)) {
      // Find valid operators
      val existingOPArray = sigToOP(signature)
      val validOPArray =
        existingOPArray.filter(existingOP => {
          !containTree(existingOP, otherOP) && !containTree(otherOP, existingOP)
        }).map(existingOP => {
          val benefit = CostEstimater.shareWithMatBenefit(existingOP, otherOP)
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
          if (oneOP != bestShareOP) arrayBuffer.append(oneOP)
        })
        arrayBuffer.append(newOP)

        sigToOP.put(signature, arrayBuffer.toArray)
      } else {

        val arrayBuffer = new ArrayBuffer[PlanOperator]()
        existingOPArray.foreach(arrayBuffer.append(_))
        arrayBuffer.append(otherOP)
        sigToOP.put(signature, arrayBuffer.toArray)

        otherOP.childOps.foreach(mergeSubExpression(_, sigToOP, isSameQuery))
      }
    } else {
      sigToOP.put(signature, Array(otherOP))
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

  private def mergeOpForSameQueryHelper(op: PlanOperator,
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
    val newOp = mergeOpForDiffQueryHelper(op, otherOP)

    // newOP should include the parent/child info for op
    // so here we only include otherOP's info
    newOp.setParents(op.parentOps ++ otherOP.parentOps)
    otherOP.parentOps.foreach(parent => { setNewChild(parent, otherOP, newOp) })

    newOp
  }

  def mergeOpForDiffQueryHelper(op: PlanOperator,
                                otherOP: PlanOperator): PlanOperator = {
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

      mergeOpForDiffQueryHelper(leftChild, rightChild)

    } else if (op.isInstanceOf[SelectOperator]) {

      newOP = op
      mergeQidSet(op, otherOP)
      mergeOpForDiffQueryHelper(op.childOps(0), otherOP)

    } else if (otherOP.isInstanceOf[SelectOperator]) {

      val rightChild = otherOP.childOps(0)
      mergeQidSet(otherOP, op)
      newOP = insertBefore(op, otherOP)

      mergeOpForDiffQueryHelper(op, rightChild)

    } else {

      newOP = mergeQidSet(op, otherOP)
      op.childOps.zip(otherOP.childOps).map(pair => {
        val thisOP = pair._1
        val thatOP = pair._2
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
    opParent.foreach(parent => {setNewChild(parent, op, opChild(0))})
  }

  private def replaceOperator(left: PlanOperator, right: PlanOperator): PlanOperator = {
    val newOP = right.copy()

    newOP.setParents(left.parentOps)
    newOP.setChildren(left.childOps)

    setNewParent(left.childOps(0), left, newOP)
    left.parentOps.foreach(parent => {
      setNewChild(parent, left, newOP)
    })

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

    leftParent.foreach(parent => {
      if (parent != dummyOperator) setNewChild(parent, left, newParent)
    })

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
