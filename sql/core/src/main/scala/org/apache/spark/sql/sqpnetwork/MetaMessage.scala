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

package org.apache.spark.sql.sqpnetwork

import org.apache.spark.sql.sqpmeta.SubQueryInfo

class MetaMessage (val uid: Int) extends Serializable {}

class PlanMessage (override val uid: Int, val baseQuery: Boolean)
  extends MetaMessage (uid) {

  private var subQueryInfo: SubQueryInfo = null

  def setSubQInfo(inputSubQInfo: SubQueryInfo): Unit = {
    this.subQueryInfo = inputSubQInfo
  }

  def getSubQInfo(): SubQueryInfo = {
    subQueryInfo
  }
}

class ExecMessage (override val uid: Int, val terminate: Boolean)
  extends MetaMessage (uid) {}

class StatMessage (override val uid: Int)
  extends MetaMessage (uid) {

  var batchID = 0
  var execTime = 0.0

  def setStateInfo(inputBatchID: Int, inputExecTime: Double): Unit = {
    batchID = inputBatchID
    execTime = inputExecTime
  }
}
