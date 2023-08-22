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

package org.apache.spark.rpc.netty

import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.network.client.TransportClient
import org.apache.spark.rpc._

class NettyRpcEnvSuite extends RpcEnvSuite with MockitoSugar {

  override def createRpcEnv(
      conf: SparkConf,
      name: String,
      port: Int,
      clientMode: Boolean = false): RpcEnv = {
    val config = RpcEnvConfig(conf, "test", "localhost", "localhost", port,
      new SecurityManager(conf), 0, clientMode)
    new NettyRpcEnvFactory().create(config)
  }

  test("non-existent endpoint") {
    val uri = RpcEndpointAddress(env.address, "nonexist-endpoint").toString
    val e = intercept[SparkException] {
      env.setupEndpointRef(env.address, "nonexist-endpoint")
    }
    assert(e.getCause.isInstanceOf[RpcEndpointNotFoundException])
    assert(e.getCause.getMessage.contains(uri))
  }

  test("advertise address different from bind address") {
    val sparkConf = new SparkConf()
    val config = RpcEnvConfig(sparkConf, "test", "localhost", "example.com", 0,
      new SecurityManager(sparkConf), 0, false)
    val env = new NettyRpcEnvFactory().create(config)
    try {
      assert(env.address.hostPort.startsWith("example.com:"))
    } finally {
      env.shutdown()
    }
  }

  test("RequestMessage serialization") {
    def assertRequestMessageEquals(expected: RequestMessage, actual: RequestMessage): Unit = {
      assert(expected.senderAddress === actual.senderAddress)
      assert(expected.receiver === actual.receiver)
      assert(expected.content === actual.content)
    }

    val nettyEnv = env.asInstanceOf[NettyRpcEnv]
    val client = mock[TransportClient]
    val senderAddress = RpcAddress("locahost", 12345)
    val receiverAddress = RpcEndpointAddress("localhost", 54321, "test")
    val receiver = new NettyRpcEndpointRef(nettyEnv.conf, receiverAddress, nettyEnv)

    val msg = new RequestMessage(senderAddress, receiver, "foo")
    assertRequestMessageEquals(
      msg,
      RequestMessage(nettyEnv, client, msg.serialize(nettyEnv)))

    val msg2 = new RequestMessage(null, receiver, "foo")
    assertRequestMessageEquals(
      msg2,
      RequestMessage(nettyEnv, client, msg2.serialize(nettyEnv)))

    val msg3 = new RequestMessage(senderAddress, receiver, null)
    assertRequestMessageEquals(
      msg3,
      RequestMessage(nettyEnv, client, msg3.serialize(nettyEnv)))
  }
}
