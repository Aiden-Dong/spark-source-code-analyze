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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.internal.SQLConf

/**
 * Test cases for the builder pattern of [[SparkSession]].
 */
class SparkSessionBuilderSuite extends SparkFunSuite with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    // This suite should not interfere with the other test suites.
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearDefaultSession()
  }

  test("create with config options and propagate them to SparkContext and SparkSession") {
    val session = SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", value = false)
      .config("some-config", "v2")
      .getOrCreate()
    assert(session.sparkContext.conf.get("some-config") == "v2")
    assert(session.conf.get("some-config") == "v2")
  }

  test("use global default session") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.builder().getOrCreate() == session)
  }

  test("sets default and active session") {
    assert(SparkSession.getDefaultSession == None)
    assert(SparkSession.getActiveSession == None)
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.getDefaultSession == Some(session))
    assert(SparkSession.getActiveSession == Some(session))
  }

  test("get active or default session") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.active == session)
    SparkSession.clearActiveSession()
    assert(SparkSession.active == session)
    SparkSession.clearDefaultSession()
    intercept[IllegalStateException](SparkSession.active)
    session.stop()
  }

  test("config options are propagated to existing SparkSession") {
    val session1 = SparkSession.builder().master("local").config("spark-config1", "a").getOrCreate()
    assert(session1.conf.get("spark-config1") == "a")
    val session2 = SparkSession.builder().config("spark-config1", "b").getOrCreate()
    assert(session1 == session2)
    assert(session1.conf.get("spark-config1") == "b")
  }

  test("use session from active thread session and propagate config options") {
    val defaultSession = SparkSession.builder().master("local").getOrCreate()
    val activeSession = defaultSession.newSession()
    SparkSession.setActiveSession(activeSession)
    val session = SparkSession.builder().config("spark-config2", "a").getOrCreate()

    assert(activeSession != defaultSession)
    assert(session == activeSession)
    assert(session.conf.get("spark-config2") == "a")
    assert(session.sessionState.conf == SQLConf.get)
    assert(SQLConf.get.getConfString("spark-config2") == "a")
    SparkSession.clearActiveSession()

    assert(SparkSession.builder().getOrCreate() == defaultSession)
  }

  test("create a new session if the default session has been stopped") {
    val defaultSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setDefaultSession(defaultSession)
    defaultSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != defaultSession)
  }

  test("create a new session if the active thread session has been stopped") {
    val activeSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setActiveSession(activeSession)
    activeSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != activeSession)
  }

  test("create SparkContext first then SparkSession") {
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val sparkContext2 = new SparkContext(conf)
    val session = SparkSession.builder().config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext == sparkContext2)
    // We won't update conf for existing `SparkContext`
    assert(!sparkContext2.conf.contains("key2"))
    assert(sparkContext2.conf.get("key1") == "value1")
  }

  test("create SparkContext first then pass context to SparkSession") {
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val newSC = new SparkContext(conf)
    val session = SparkSession.builder().sparkContext(newSC).config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext == newSC)
    assert(session.sparkContext.conf.get("key1") == "value1")
    // If the created sparkContext is passed through the Builder's API sparkContext,
    // the conf of this sparkContext will not contain the conf set through the API config.
    assert(!session.sparkContext.conf.contains("key2"))
    assert(session.sparkContext.conf.get("spark.app.name") == "test")
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(session.sessionState.newHadoopConf().get("hive.in.test") == "true")
    assert(session.sparkContext.hadoopConfiguration.get("hive.in.test") == "true")
  }

  test("SPARK-15991: Set global Hadoop conf") {
    val session = SparkSession.builder().master("local").getOrCreate()
    val mySpecialKey = "my.special.key.15991"
    val mySpecialValue = "msv"
    try {
      session.sparkContext.hadoopConfiguration.set(mySpecialKey, mySpecialValue)
      assert(session.sessionState.newHadoopConf().get(mySpecialKey) == mySpecialValue)
    } finally {
      session.sparkContext.hadoopConfiguration.unset(mySpecialKey)
    }
  }
}
