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
package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{DYN_ALLOCATION_MAX_EXECUTORS, DYN_ALLOCATION_MIN_EXECUTORS, EXECUTOR_INSTANCES}
import org.apache.spark.util.Utils

private[spark] object SchedulerBackendUtils {
  val DEFAULT_NUMBER_EXECUTORS = 2

  /**
   * 获取初始的目标执行器数量取决于是否启用了动态分配。
   * 如果不使用动态分配，则获取用户请求的执行器数量。
   * 这段代码强调了在不同的执行模式下获取初始执行器数量的方法。
   */
  def getInitialTargetExecutorNumber(conf: SparkConf, numExecutors: Int = DEFAULT_NUMBER_EXECUTORS): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {    // 如果已经开启了动态分配
      val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)                 // 最小的Executor 数量
      val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)   // 初始化的Executor 数量
      val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)                 // 最大的Executor数量

      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      conf.get(EXECUTOR_INSTANCES).getOrElse(numExecutors)
    }
  }
}
