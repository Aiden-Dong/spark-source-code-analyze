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

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Shuffle 系统的可拔插接口
 * 在 SparkEnv 中，基于 spark.shuffle.manager 设置，在driver和每个executor上创建 ShuffleManager.
 * driver 用它注册shuffle，executor（或在driver中本地运行的任务）基于shuffleManager来读取或者写入数据。
 *
 * NOTE: 这将由SparkEnv实例化，因此其构造函数可以使用SparkConf和isDriver作为参数。
 */
private[spark] trait ShuffleManager {

  /**
   * 通过ShuffleDependency 来注册获取一个ShuffleHandler
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  // 获取给定分区的写入器。由MapTask在Executor上调用。
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /****
   * 为一系列的reduce分区 [startPartition, endPartition) 获取一个读取器。
   * 由reduce任务在执行器上调用。
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
   * 从ShuffleManager中移除shuffle的元数据。
   * @return true if the metadata removed successfully, otherwise false.
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * 返回一个能够根据block坐标检索shuffle block 数据的解析器。
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}
