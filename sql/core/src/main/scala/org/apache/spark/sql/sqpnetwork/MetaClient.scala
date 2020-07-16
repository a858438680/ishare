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

import java.io.{BufferedInputStream, ObjectInputStream, ObjectOutputStream}
import java.net.Socket

class MetaClient (address: String, port: Int, uid: Int) {

  private var socket: Socket = null
  private var inputStream: ObjectInputStream = null
  private var outputStream: ObjectOutputStream = null

  def startClient(): Unit = {
    var isConnected = false
    while (!isConnected) {
      try {
        socket = new Socket(address, port)
        isConnected = socket.isConnected
      } catch {
        case _: Throwable =>
      }
    }

    inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream))
    outputStream = new ObjectOutputStream(socket.getOutputStream)
    outputStream.writeObject(new MetaMessage(uid))
  }

  def getPlanMessage(): PlanMessage = {
    inputStream.readObject().asInstanceOf[PlanMessage]
  }

  def getExecMessage(): ExecMessage = {

    while (true) {
      val execMessage = inputStream.readObject()
      if (execMessage.isInstanceOf[ExecMessage]) return execMessage.asInstanceOf[ExecMessage]
    }

    null
  }

  def reportStatMessage(msg: StatMessage): Unit = {
    outputStream.writeObject(msg)
  }

  def stopClient(): Unit = {
    inputStream.close()
    outputStream.close()
    socket.close()
  }

}
