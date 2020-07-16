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

class NetworkTester (address: String, port: Int) {

  private val numBatchArray = Array(2, 5)

  def runNetworkTester(): Unit = {
    val clients = new Array[ClientThread](2)
    for (uid <- 0 until 2) {
      clients(uid) = new ClientThread(address, port, uid, numBatchArray(uid))
      println(s"Start Client $uid")
      clients(uid).start()
    }

    println(s"Start Server")
    val server = new ServerThread(2, port, numBatchArray)
    server.start()

    clients.foreach(_.join())
    server.join()
  }
}

class ServerThread (numSubQ: Int, port: Int,
                    numBatchArray: Array[Int]) extends Thread {

  private val server = new MetaServer(numSubQ, port)
  private val batchIDArray = new Array[Int](numBatchArray.length)
  private val executed = new Array[Boolean](numBatchArray.length)

  for (idx <- batchIDArray.indices) batchIDArray(idx) = 0

  override def run(): Unit = {
    server.startServer()

    for (step <- 1 until 101) {
      val progress = step.toDouble/100.0
      for (uid <- batchIDArray.indices) {
        val threshold = (batchIDArray(uid) + 1).toDouble/numBatchArray(uid).toDouble
        if (progress >= threshold) {
          server.startOneExecution(uid)
          executed(uid) = true
          batchIDArray(uid) += 1
        }
      }

      for (uid <- batchIDArray.indices) {
        if (executed(uid)) {
          executed(uid) = false
          server.getStatMessage(uid)
        }
      }
    }

    server.stopServer()
  }

}

class ClientThread (address: String, port: Int,
                    uid: Int, numBatch: Int) extends Thread {

  private val client = new MetaClient(address, port, uid)

  override def run(): Unit = {
    client.startClient()

    client.getPlanMessage()
    println(s"Client $uid receives the plan message")

    for (batchID <- 0 until numBatch) {
      client.getExecMessage()

      val msg = new StatMessage(uid)
      msg.setStateInfo(batchID, 0.0)
      client.reportStatMessage(msg)
    }

    client.stopClient()
  }

}

object NetworkTester {

  def main(args: Array[String]): Unit = {
    val address = "localhost"
    val port = 8887
    val networkTester = new NetworkTester(address, port)
    networkTester.runNetworkTester()
  }
}
