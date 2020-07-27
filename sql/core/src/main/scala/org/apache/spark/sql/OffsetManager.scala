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

package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.sql.execution.streaming.BaseStreamingSource

class OffsetManager {

  var endOffsetsMap = mutable.HashMap.empty[BaseStreamingSource, SlothOffset]

  def loadEndOffsets(inputEndOffsets: Map[BaseStreamingSource, String]): Unit = {

    import SlothOffsetUtils._

    inputEndOffsets.foreach(pair => {
      endOffsetsMap.put(pair._1, jsonToOffset(pair._2))
    })

  }

  def constructNewData(batchId: Int, batchNum: Int): Map[BaseStreamingSource, String] = {

    endOffsetsMap.map(pair => {
      val source = pair._1
      val endOffSet = pair._2
      val offset = endOffSet.getOffsetByIndex(batchNum, batchId + 1)

      (source, SlothOffsetUtils.offsetToJSON(offset))
    }).toMap

  }

}
