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

package org.apache.spark.sql.execution.exchange

import java.util.{HashMap => JHashMap, Map => JMap}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{MapOutputStatistics, ShuffleDependency, SimpleFutureAction}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ShuffledRowRDD, SparkPlan}

/**
 * 这段文字描述了一个用于决定SparkSQL中数据在各个stage之间如何 shuffle 的协调器。
 * 目前，该协调器的工作是确定需要从一个或多个stage获取shuffle数据的stage的后shuffle分区数。
 *
 * 这段文字描述了一个协调器（coordinator）的构造函数，它有三个参数：numExchanges、targetPostShuffleInputSize 和 minNumPostShufflePartitions。
 *   - numExchanges 用于指示将有多少个 [[ShuffleExchangeExec]] 注册到该协调器。
 *       因此，在开始执行任何实际工作之前，我们可以确保已经得到了预期数量的 [[ShuffleExchangeExec]]。
 *   - targetPostShuffleInputSize 是后洗牌分区输入数据大小的目标大小。通过这个参数，我们可以估算出后洗牌分区的数量。
 *     这个参数可以通过 spark.sql.adaptive.shuffle.targetPostShuffleInputSize 进行配置。
 *   - minNumPostShufflePartitions 是一个可选参数。如果定义了该参数，协调器将尽力确保至少有 minNumPostShufflePartitions 个后洗牌分区。
 *
 * 这段文字描述了协调器（coordinator）的工作流程，如下所示：
 *
 *  - 在执行 [[SparkPlan]] 之前，对于一个 [[ShuffleExchangeExec]] 操作符，如果有一个 [[ExchangeCoordinator]] 被分配给它，
 *    它会在 doPrepare 方法中将自己注册到该协调器。
 *
 *  - 一旦我们开始执行物理计划，注册到该协调器的 [[ShuffleExchangeExec]] 将调用 postShuffleRDD 方法来获取相应的 [[ShuffledRowRDD]]。
 *    如果该协调器已经决定了如何shuffle数据，那么该 [[ShuffleExchangeExec]] 将立即得到其对应的后洗牌 [[ShuffledRowRDD]]。
 *
 * - 如果该协调器尚未决定如何shuffle数据，它将要求这些已注册的 [[ShuffleExchangeExec]] 提交它们的前洗牌阶段。
 *   然后，基于前shuffle分区的大小统计信息，该协调器将确定后shuffle分区的数量，并在必要时将多个前洗牌分区与连续索引打包到一个后shuffle分区中。
 *
 * - 最后，该协调器将为所有已注册的 [[ShuffleExchangeExec]] 创建后洗牌 [[ShuffledRowRDD]]。
 *    因此，当一个 [[ShuffleExchangeExec]] 调用 postShuffleRDD 方法时，该协调器可以查找到相应的 [[RDD]]。
 *
 * 该策略用于确定后shuffle分区的数量，具体步骤如下:
 * 1. 首先，我们有一个用于后shuffle分区的目标输入大小（即targetPostShuffleInputSize）。
 * 2. 一旦我们获得了与已注册的 [[ShuffleExchangeExec]] 相关的前shuffle阶段的大小统计信息，我们将对这些统计信息进行一次遍历。
 * 3. 在遍历过程中，我们将连续索引的前shuffle分区打包到一个单独的后shuffle分区，直到添加另一个前shuffle分区会导致后shuffle分区的大小超过目标大小为止。
 * 4. 换句话说，我们会不断将前shuffle分区合并成后shuflle分区，直到满足以下条件之一：
 *     4.1 合并下一个前shuffle分区会导致后shuffle分区的大小超过目标大小 targetPostShuffleInputSize。
 *     4.2 已经处理了所有前shuffle分区。
 *     4.3 通过这种方式，我们可以动态地确定后shuffle分区的数量，以便尽可能地满足目标输入大小要求。这有助于优化shuffle操作的性能和资源使用。
 *
 * 例如，我们有两个stage，具有以下pre-shuffle分区大小的统计数据 :
 *    stage 1: [100 MB, 20 MB, 100 MB, 10MB, 30 MB]
 *    stage 2: [10 MB,  10 MB, 70 MB,  5 MB, 5 MB]
 * 假设目标的输入大小是128MB，那么我们将有四个洗牌后的分区:
 * which are:
 *  - post-shuffle partition 0: pre-shuffle partition 0 (size 110 MB)
 *  - post-shuffle partition 1: pre-shuffle partition 1 (size 30 MB)
 *  - post-shuffle partition 2: pre-shuffle partition 2 (size 170 MB)
 *  - post-shuffle partition 3: pre-shuffle partition 3 and 4 (size 50 MB)
 */
class ExchangeCoordinator(
    advisoryTargetPostShuffleInputSize: Long,                     // 目标设置的优化大小
    minNumPostShufflePartitions: Option[Int] = None)              // 最低的shuffle分区数
  extends Logging {

  // 注册进来的 ShuffleExchange 操作算子， 上游可能会有多个依赖（JOIN的情况）
  private[this] val exchanges = ArrayBuffer[ShuffleExchangeExec]()

  // 在这里使用lazy val，以便我们可以注意到对这个类的错误使用，
  // 例如，在第一次调用 postShuffleRDD 之前，应该先注册所有的交换。
  // 如果在 postShuffleRDD 调用后注册了新的交换，doEstimationIfNecessary中的assert(exchanges.length == numExchanges)会失败。
  // 这段代码强调了在何时注册交换以避免错误使用的必要性。
  private[this] lazy val numExchanges = exchanges.size

  // 这个映射用于查找Exchange操作符的洗牌后ShuffledRowRDD。
  private[this] lazy val postShuffleRDDs: JMap[ShuffleExchangeExec, ShuffledRowRDD] = new JHashMap[ShuffleExchangeExec, ShuffledRowRDD](numExchanges)

  // 一个布尔值，指示此coorditor是否已经决定了如何shuffle数据。
  // 这个变量只会在doEstimationIfNecessary中更新，该方法受到同步保护。
  @volatile private[this] var estimated: Boolean = false

  /**
   * Registers a [[ShuffleExchangeExec]] operator to this coordinator. This method is only allowed
   * to be called in the `doPrepare` method of a [[ShuffleExchangeExec]] operator.
   */
  @GuardedBy("this")
  def registerExchange(exchange: ShuffleExchangeExec): Unit = synchronized {
    exchanges += exchange
  }

  def isEstimated: Boolean = estimated

  /**
   * ****************************
   * 在此 coordinator 中注册一个 [[ShuffleExchangeExec]] 操作符。
   * 此方法只允许在[[ShuffleExchangeExec]]操作符的doPrepare方法中调用。
   *
   * 此方法中基于 Map Shuffle 的统计数据，计算目标Shuffle的每个分区取数分区位置
   */
  def estimatePartitionStartIndices(mapOutputStatistics: Array[MapOutputStatistics]): Array[Int] = {

    // 每个post-shuffle分区的数据量上限
    val targetPostShuffleInputSize = minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        // 当前所有的 MapTask 总的输入数据大小
        val totalPostShuffleInputSize = mapOutputStatistics.map(_.bytesByPartitionId.sum).sum

        // 计算基于最少的分区数得到每个分区数据量上限
        val maxPostShuffleInputSize = math.max(math.ceil(totalPostShuffleInputSize / numPartitions.toDouble).toLong, 16)
        math.min(maxPostShuffleInputSize, advisoryTargetPostShuffleInputSize)

      case None => advisoryTargetPostShuffleInputSize
    }

    logInfo(s"advisoryTargetPostShuffleInputSize: $advisoryTargetPostShuffleInputSize, targetPostShuffleInputSize $targetPostShuffleInputSize.")

    // 确保我们为这些阶段获得相同数量的 pre-shuffle partition.
    val distinctNumPreShufflePartitions = mapOutputStatistics.map(stats => stats.bytesByPartitionId.length).distinct
    // 我们期望的预洗牌分区数量是单值的原因是，当我们添加Exchange操作时，我们使用一个静态配置（spark.sql.shuffle.partitions）来设置预shuffle分区数量（即map输出分区数量），
    // 即使两个输入RDD具有不同数量的分区，它们将具有相同数量的预shuffle分区（即map输出分区）。
    // 这段代码解释了为什么我们期望预洗牌分区的数量是单值的原因。

    assert(distinctNumPreShufflePartitions.length == 1,
      "There should be only one distinct value of the number pre-shuffle partitions among registered Exchange operator.")

    // 获取分区数
    val numPreShufflePartitions = distinctNumPreShufflePartitions.head

    // 记录目标分区的每一个分区从pre-shuffle 分区定位的起始索引
    val partitionStartIndices = ArrayBuffer[Int]()

    // partitionStartIndices的第一个元素始终为0。这是因为第一个分区的起始索引始终是0。
    partitionStartIndices += 0
    var postShuffleInputSize = 0L

    var i = 0
    while (i < numPreShufflePartitions) {
      var nextShuffleInputSize = 0L       // 当前分区的数据量大小
      var j = 0
      while (j < mapOutputStatistics.length) {   // 从所有的 pre-shuffle stage 中找到当前计算的分区， 获取计算数据量
        nextShuffleInputSize += mapOutputStatistics(j).bytesByPartitionId(i)
        j += 1
      }

      // If including the nextShuffleInputSize would exceed the target partition size,
      // then start a new partition.
      if (i > 0 && postShuffleInputSize + nextShuffleInputSize > targetPostShuffleInputSize) {
        partitionStartIndices += i
        // reset postShuffleInputSize.
        postShuffleInputSize = nextShuffleInputSize
      } else postShuffleInputSize += nextShuffleInputSize

      i += 1
    }

    partitionStartIndices.toArray
  }

  @GuardedBy("this")
  private def doEstimationIfNecessary(): Unit = synchronized {

    if (!estimated) {
      // Make sure we have the expected number of registered Exchange operators.
      // 上游可能会有
      assert(exchanges.length == numExchanges)

      val newPostShuffleRDDs = new JHashMap[ShuffleExchangeExec, ShuffledRowRDD](numExchanges)

      // Submit all map stages
      val shuffleDependencies = ArrayBuffer[ShuffleDependency[Int, InternalRow, InternalRow]]()
      val submittedStageFutures = ArrayBuffer[SimpleFutureAction[MapOutputStatistics]]()
      var i = 0
      while (i < numExchanges) {
        val exchange = exchanges(i)
        val shuffleDependency = exchange.prepareShuffleDependency()  // 获取当前shuffleDependency
        shuffleDependencies += shuffleDependency
        if (shuffleDependency.rdd.partitions.length != 0) {
          // submitMapStage does not accept RDD with 0 partition.
          // So, we will not submit this dependency.
          submittedStageFutures += exchange.sqlContext.sparkContext.submitMapStage(shuffleDependency)
        }
        i += 1
      }

      // Wait for the finishes of those submitted map stages.
      val mapOutputStatistics = new Array[MapOutputStatistics](submittedStageFutures.length)
      var j = 0
      while (j < submittedStageFutures.length) {
        // This call is a blocking call. If the stage has not finished, we will wait at here.
        mapOutputStatistics(j) = submittedStageFutures(j).get()
        j += 1
      }

      // If we have mapOutputStatistics.length < numExchange, it is because we do not submit
      // a stage when the number of partitions of this dependency is 0.
      assert(mapOutputStatistics.length <= numExchanges)

      // Now, we estimate partitionStartIndices. partitionStartIndices.length will be the
      // number of post-shuffle partitions.
      val partitionStartIndices =
        if (mapOutputStatistics.length == 0) {
          Array.empty[Int]
        } else {
          estimatePartitionStartIndices(mapOutputStatistics)
        }

      var k = 0
      while (k < numExchanges) {
        val exchange = exchanges(k)
        val rdd =
          exchange.preparePostShuffleRDD(shuffleDependencies(k), Some(partitionStartIndices))
        newPostShuffleRDDs.put(exchange, rdd)

        k += 1
      }

      // Finally, we set postShuffleRDDs and estimated.
      assert(postShuffleRDDs.isEmpty)
      assert(newPostShuffleRDDs.size() == numExchanges)
      postShuffleRDDs.putAll(newPostShuffleRDDs)
      estimated = true
    }
  }

  def postShuffleRDD(exchange: ShuffleExchangeExec): ShuffledRowRDD = {
    doEstimationIfNecessary()

    if (!postShuffleRDDs.containsKey(exchange)) {
      throw new IllegalStateException(
        s"The given $exchange is not registered in this coordinator.")
    }

    postShuffleRDDs.get(exchange)
  }

  override def toString: String = {
    s"coordinator[target post-shuffle partition size: $advisoryTargetPostShuffleInputSize]"
  }
}
