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

package org.apache.spark.scheduler

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{RpcUtils, SerializableBuffer}

class CoarseGrainedSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext
    with Eventually {

  test("serialized task larger than max RPC message size") {
    val conf = new SparkConf
    conf.set("spark.rpc.message.maxSize", "1")
    conf.set("spark.default.parallelism", "1")
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val buffer = new SerializableBuffer(java.nio.ByteBuffer.allocate(2 * frameSize))
    val larger = sc.parallelize(Seq(buffer))
    val thrown = intercept[SparkException] {
      larger.collect()
    }
    assert(thrown.getMessage.contains("using broadcast variables for large values"))
    val smaller = sc.parallelize(1 to 4).collect()
    assert(smaller.size === 4)
  }

  test("compute max number of concurrent tasks can be launched") {
    val conf = new SparkConf()
      .setMaster("local-cluster[4, 3, 1024]")
      .setAppName("test")
    sc = new SparkContext(conf)
    eventually(timeout(10.seconds)) {
      // Ensure all executors have been launched.
      assert(sc.getExecutorIds().length == 4)
    }
    assert(sc.maxNumConcurrentTasks() == 12)
  }

  test("compute max number of concurrent tasks can be launched when spark.task.cpus > 1") {
    val conf = new SparkConf()
      .set("spark.task.cpus", "2")
      .setMaster("local-cluster[4, 3, 1024]")
      .setAppName("test")
    sc = new SparkContext(conf)
    eventually(timeout(10.seconds)) {
      // Ensure all executors have been launched.
      assert(sc.getExecutorIds().length == 4)
    }
    // Each executor can only launch one task since `spark.task.cpus` is 2.
    assert(sc.maxNumConcurrentTasks() == 4)
  }

  test("compute max number of concurrent tasks can be launched when some executors are busy") {
    val conf = new SparkConf()
      .set("spark.task.cpus", "2")
      .setMaster("local-cluster[4, 3, 1024]")
      .setAppName("test")
    sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10, 4).mapPartitions { iter =>
      Thread.sleep(5000)
      iter
    }
    var taskStarted = new AtomicBoolean(false)
    var taskEnded = new AtomicBoolean(false)
    val listener = new SparkListener() {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        taskStarted.set(true)
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskEnded.set(true)
      }
    }

    try {
      sc.addSparkListener(listener)
      eventually(timeout(10.seconds)) {
        // Ensure all executors have been launched.
        assert(sc.getExecutorIds().length == 4)
      }

      // Submit a job to trigger some tasks on active executors.
      testSubmitJob(sc, rdd)

      eventually(timeout(10.seconds)) {
        // Ensure some tasks have started and no task finished, so some executors must be busy.
        assert(taskStarted.get() == true)
        assert(taskEnded.get() == false)
        // Assert we count in slots on both busy and free executors.
        assert(sc.maxNumConcurrentTasks() == 4)
      }
    } finally {
      sc.removeSparkListener(listener)
    }
  }

  private def testSubmitJob(sc: SparkContext, rdd: RDD[Int]): Unit = {
    sc.submitJob(
      rdd,
      (iter: Iterator[Int]) => iter.toArray,
      0 until rdd.partitions.length,
      { case (_, _) => return }: (Int, Array[Int]) => Unit,
      { return }
    )
  }
}
