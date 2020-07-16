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

case class TypeAttr (attr: String,
                     typeStr: String)

abstract class PlanOperator (qidSet: Array[Int],
                             outputAttrs: mutable.HashSet[String],
                             referencedAttrs: mutable.HashSet[String],
                             aliasAttrs: mutable.HashMap[String, String],
                             dfStr: String) {

  var childOps: Array[PlanOperator] = new Array[PlanOperator](0)
  var parentOps: Array[PlanOperator] = new Array[PlanOperator](0)
  var id: Int = _
  var signature: String = ""
  var internalQidSet = qidSet
  var copyOP: PlanOperator = _

  // These attributes are for code generation
  var subQueryName: String = _
  var subQueryUID: Int = _
  var possibleOutputAttrs = mutable.HashSet.empty[TypeAttr]
  var requireOutputAttrs = mutable.HashSet.empty[TypeAttr]
  var requireOutputArray: Array[TypeAttr] = _

  // These attributes are for cost estimation
  val cardMap = mutable.HashMap.empty[Int, Double]
  val delCardMap = mutable.HashMap.empty[Int, Double]
  val offSetMap = mutable.HashMap.empty[Int, mutable.HashMap[Int, Double]]
  val delOffSetMap = mutable.HashMap.empty[Int, mutable.HashMap[Int, Double]]

  def setChildren(childOperators: Array[PlanOperator]): Unit = {
    this.childOps = childOperators
  }

  def setParents(parentOperators: Array[PlanOperator]): Unit = {
    this.parentOps = parentOperators
  }

  def setId(newId: Int): Unit = {
    this.id = newId
  }

  def setUID(newUID: Int): Unit = {
    this.subQueryUID = newUID
  }

  def setSubQueryName(subQueryName: String): Unit = {
    this.subQueryName = subQueryName
  }

  def setPossibleOutputAttrs(outputAtts: mutable.HashSet[TypeAttr]): Unit = {
    this.possibleOutputAttrs = outputAtts
  }

  def setRequiredOutputAttrs(outputAtts: mutable.HashSet[TypeAttr]): Unit = {
    this.requireOutputAttrs = outputAtts
  }

  def genRequiredOutputArray(): Unit = {
    this.requireOutputArray = this.requireOutputAttrs.toArray
  }

  def getOutputAttrs: mutable.HashSet[String] = outputAttrs
  def getReferencedAttrs: mutable.HashSet[String] = referencedAttrs
  def getAliasAttrs: mutable.HashMap[String, String] = aliasAttrs
  def getDFStr: String = dfStr

  def genSigStr(childSignatures: Array[String]): String

  def copy(): PlanOperator

  protected def basicCopy(planOperator: PlanOperator): PlanOperator = {
    planOperator.childOps = childOps
    planOperator.parentOps = parentOps
    planOperator.id = id
    planOperator.signature = signature
    planOperator.internalQidSet = internalQidSet

    planOperator
  }

  def resetCostInfo(): Unit

  protected def basicResetCostInfo(): Unit = {
    cardMap.clear()
    delCardMap.clear()
    offSetMap.clear()
    delOffSetMap.clear()
  }

  def getQidSet: Array[Int] = internalQidSet
  def setQidSet(newQidSet: Array[Int]): Unit = {
    this.internalQidSet = newQidSet
  }
}

class ScanOperator (qidSet: Array[Int],
                    outputAttrs: mutable.HashSet[String],
                    referenceAttrs: mutable.HashSet[String],
                    aliasAttrs: mutable.HashMap[String, String],
                    dfStr: String,
                    tableName: String)
  extends PlanOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr) {

  var batchIdx = 0

  override def toString: String = {
    val  qidSetStr = Utils.qidSetToString(internalQidSet)
    s"Scan $tableName $qidSetStr"
  }

  override def genSigStr(childSignatures: Array[String]): String = {
    signature = tableName
    signature
  }

  override def copy(): PlanOperator = {
    val newOP =
      new ScanOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr, tableName)
    super.basicCopy(newOP)
  }

  def getTableName: String = tableName

  override def resetCostInfo(): Unit = {
    super.basicResetCostInfo()
    batchIdx = 0
  }

}

class ProjectOperator (qidSet: Array[Int],
                      outputAttrs: mutable.HashSet[String],
                      referenceAttrs: mutable.HashSet[String],
                      aliasAttrs: mutable.HashMap[String, String],
                      dfStr: String)
  extends PlanOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr) {

  override def toString: String = {
    val qidSetStr = Utils.qidSetToString(internalQidSet)
    val aliasStr = Utils.aliasToString(aliasAttrs)
    val outputStr = Utils.outputToString(outputAttrs)

    s"Select Out: $outputStr Alias: $aliasStr $qidSetStr"
  }

  override def genSigStr(childSignatures: Array[String]): String = {
    val strBuf = new StringBuffer()
    strBuf.append(s"${childSignatures(0)}\n")
    strBuf.append(s"$dfStr")

    signature = strBuf.toString
    signature
  }

  override def copy(): PlanOperator = {
    val newOP =
      new ProjectOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr)
    super.basicCopy(newOP)
  }

  override def resetCostInfo(): Unit = {
    super.basicResetCostInfo()
  }

}

case class Predicate (left: String, op: String, right: String) {

  override def toString: String = {
    s"$left $op $right"
  }

}

class SelectOperator (qidSet: Array[Int],
                      outputAttrs: mutable.HashSet[String],
                      referenceAttrs: mutable.HashSet[String],
                      aliasAttrs: mutable.HashMap[String, String],
                      dfStr: String,
                      predicates: Array[Predicate],
                      var selectSet: Array[Int])
  extends PlanOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr) {

  override def toString: String = {
    val qidSetStr = Utils.qidSetToString(internalQidSet)
    val selSetStr = Utils.qidSetToString(selectSet)
    val predStr = Utils.predicatesToString(predicates)

    s"Filter $predStr $qidSetStr $selSetStr"
  }

  override def genSigStr(childSignatures: Array[String]): String = {
    signature = childSignatures(0)
    signature
  }

  def getPredicates: Array[Predicate] = predicates

  def getSelectSet: Array[Int] = selectSet

  def setSelectSet(selSet: Array[Int]): Unit = {
    this.selectSet = selSet
  }

  override def copy(): PlanOperator = {
    val newOP =
      new SelectOperator(qidSet, outputAttrs, referenceAttrs,
        aliasAttrs, dfStr, predicates, selectSet)
    super.basicCopy(newOP)
  }

  override def resetCostInfo(): Unit = {
    super.basicResetCostInfo()
  }

}

class JoinOperator (qidSet: Array[Int],
                    outputAttrs: mutable.HashSet[String],
                    referenceAttrs: mutable.HashSet[String],
                    aliasAttrs: mutable.HashMap[String, String],
                    dfStr: String,
                    joinKey: Array[String],
                    joinOP: String,
                    joinType: String,
                    postFilter: String)
  extends PlanOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr) {

  val innerStateSizeMap = mutable.HashMap.empty[Int, Double]
  val outerStateSizeMap = mutable.HashMap.empty[Int, Double]

  override def toString: String = {
    val qidSetStr = Utils.qidSetToString(internalQidSet)
    val joinCondition =
      if (postFilter == "") s"${joinKey(0)} $joinOP ${joinKey(1)}"
      else s"${joinKey(0)} $joinOP ${joinKey(1)} and ${postFilter}"

    s"Join $joinCondition $joinType $qidSetStr"
  }

  override def genSigStr(childSignatures: Array[String]): String = {
    val strBuf = new StringBuffer()
    val joinCondition =
      if (postFilter == "") s"${joinKey(0)} $joinOP ${joinKey(1)}"
      else s"${joinKey(0)} $joinOP ${joinKey(1)} and $postFilter"
    strBuf.append(s"${childSignatures(0)} ")
    strBuf.append(s" $joinCondition \n")
    strBuf.append(s"${joinType} ${childSignatures(1)}")

    signature = strBuf.toString
    signature
  }

  override def copy(): PlanOperator = {
    val newOP =
      new JoinOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs,
        dfStr, joinKey, joinOP, joinType, postFilter)
    super.basicCopy(newOP)
  }

  def getJoinKeys: Array[String] = joinKey

  def getJoinCondition: String = {
      if (postFilter == "") s"${joinKey(0)} $joinOP ${joinKey(1)}"
      else s"${joinKey(0)} $joinOP ${joinKey(1)} and ${postFilter}"
  }

  def getJoinType: String = joinType

  def getPostFilter(): String = postFilter

  override def resetCostInfo(): Unit = {
    super.basicResetCostInfo()
    innerStateSizeMap.clear()
    outerStateSizeMap.clear()
  }
}

class AggOperator (qidSet: Array[Int],
                 outputAttrs: mutable.HashSet[String],
                 referenceAttrs: mutable.HashSet[String],
                 aliasAttrs: mutable.HashMap[String, String],
                 dfStr: String,
                 aliasFunc: mutable.HashMap[String, String],
                 groupByAttrs: mutable.HashSet[String],
                 aggFunc: mutable.HashMap[String, String])
  extends PlanOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr) {

  var stateSize = 0.0

  override def toString: String = {
    val qidSetStr = Utils.qidSetToString(internalQidSet)
    val groupByStr = Utils.groupByToString(groupByAttrs)
    val aliasStr = Utils.aliasToString(aliasAttrs)

    s"Agg $groupByStr $aliasStr $qidSetStr"
  }

  override def genSigStr(childSignatures: Array[String]): String = {
    val strBuf = new StringBuffer()
    strBuf.append(s"${childSignatures(0)}\n")
    strBuf.append(s"${dfStr.trim}")

    signature = strBuf.toString
    signature
  }

  override def copy(): PlanOperator = {
    val newOP =
      new AggOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs,
        dfStr, aliasFunc, groupByAttrs, aggFunc)
    super.basicCopy(newOP)
  }

  override def resetCostInfo(): Unit = {
    super.basicResetCostInfo()
    stateSize = 0.0
  }

  def getGroupByAttrs: mutable.HashSet[String] = groupByAttrs
  def getAGGFuncDef: mutable.HashMap[String, String] = aggFunc
}

class RootOperator(qidSet: Array[Int],
                   outputAttrs: mutable.HashSet[String],
                   referenceAttrs: mutable.HashSet[String],
                   aliasAttrs: mutable.HashMap[String, String],
                   dfStr: String)
extends PlanOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr) {

  override def genSigStr(childSignatures: Array[String]): String = {
    signature = s"Root of Q${qidSet(0)}"
    signature
  }

  override def toString: String = {
    s"Root of Q${qidSet(0)}"
  }

  override def copy(): PlanOperator = {
    val newOP =
      new RootOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr)
    super.basicCopy(newOP)
  }

  override def resetCostInfo(): Unit = {
    super.basicResetCostInfo()
  }

}

class DummyOperator(qidSet: Array[Int],
                   outputAttrs: mutable.HashSet[String],
                   referenceAttrs: mutable.HashSet[String],
                   aliasAttrs: mutable.HashMap[String, String],
                   dfStr: String)
extends PlanOperator(qidSet, outputAttrs, referenceAttrs, aliasAttrs, dfStr) {

  override def genSigStr(childSignatures: Array[String]): String = {
    signature = s"Dummy"
    signature
  }

  override def toString: String = {
    s"Dummy"
  }

  override def copy(): PlanOperator = {
    this
  }

  override def resetCostInfo(): Unit = {
    super.basicResetCostInfo()
  }

}
