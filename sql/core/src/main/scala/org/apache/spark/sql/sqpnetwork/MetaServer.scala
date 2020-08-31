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

package org.apache.spark.sql.sqpnetwork

import java.io.{BufferedInputStream, ObjectInputStream, ObjectOutputStream}
import java.net.{ServerSocket, Socket}

import org.apache.spark.sql.sqpmeta.SubQueryInfo

class MetaServer (numSubQ: Int, port: Int) {

  private val ss = new ServerSocket(port)
  private val socketArray = new Array[Socket](numSubQ)
  private val inputStreamArray = new Array[ObjectInputStream](numSubQ)
  private val outputStreamArray = new Array[ObjectOutputStream](numSubQ)
  private val execMessageArray = new Array[ExecMessage](numSubQ)
  private var queryInfo: Array[SubQueryInfo] = null

  def loadSharedQueryInfo (queryInfo: Array[SubQueryInfo]): Unit = {
    this.queryInfo = queryInfo
  }

  def startServer(): Unit = {
    // Set up server sockets and threads
    var socket: Socket = null
    var count = 0

    while (count < numSubQ) {
      socket = ss.accept()
      val outputStream = new ObjectOutputStream(socket.getOutputStream)
      val inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream))
      val uid = getUID(inputStream)

      socketArray(uid) = socket
      inputStreamArray(uid) = inputStream
      outputStreamArray(uid) = outputStream
      execMessageArray(uid) = new ExecMessage(uid, false, false)

      val planMessage = new PlanMessage(uid, baseQuery = true)
      planMessage.setSubQInfo(queryInfo(uid))
      writePlanMessage(outputStream, planMessage)

      count += 1
    }
  }

  def startOneQuery(): Unit = {
    // Set up server sockets and threads
    var socket: Socket = null
    socket = ss.accept()
    val outputStream = new ObjectOutputStream(socket.getOutputStream)
    val inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream))
    val uid = getUID(inputStream)

    socketArray(uid) = socket
    inputStreamArray(uid) = inputStream
    outputStreamArray(uid) = outputStream
    execMessageArray(uid) = ExecMessage(uid, false, true)

    val planMessage = new PlanMessage(uid, baseQuery = true)
    planMessage.setSubQInfo(queryInfo(uid))
    writePlanMessage(outputStream, planMessage)
  }

  def startOneExecution(uid: Int, repair: Boolean): Unit = {
    val msg = ExecMessage(uid, false, repair)
    outputStreamArray(uid).writeObject(msg)
  }

  def terminateQuery(uid: Int): Unit = {
    outputStreamArray(uid).writeObject(new ExecMessage(uid, true, true))
  }

  def getStatMessage(uid: Int): StatMessage = {
    inputStreamArray(uid).readObject().asInstanceOf[StatMessage]
  }

  def stopOneQuery(uid: Int): Unit = {
    inputStreamArray(uid).close()
    outputStreamArray(uid).close()
    socketArray(uid).close()
  }

  def stopServerSocket(): Unit = {
    ss.close()
  }

  def stopServer(): Unit = {
    inputStreamArray.foreach(_.close())
    outputStreamArray.foreach(_.close())
    socketArray.foreach(_.close())
    ss.close()
  }

  private def getUID(objectInputStream: ObjectInputStream): Int = {
    objectInputStream.readObject().asInstanceOf[MetaMessage].uid
  }

  private def writePlanMessage(objectOutputStream: ObjectOutputStream,
                               planMessage: PlanMessage): Unit = {
    objectOutputStream.writeObject(planMessage)
  }
}

// scalastyle:off println
