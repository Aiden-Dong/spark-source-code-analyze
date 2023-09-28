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

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/***
 * ***********************************************************************
 * 从其他节点的块存储中请求并读取[startPartition，endPartition)范围内的分区
 * 以获取shuffle数据。
 * ***********************************************************************
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  // 读取此Reduce任务的合并键值。
  override def read(): Iterator[Product2[K, C]] = {

    // TODO 1  : 从 Shuffle 数据源端拉取数据，构建 Iterator
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,   // Shuffle Client
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),  // 返回每个ShuffleId 对应的数据文件位置
      serializerManager.wrapStream,
      // Note: 当没有提供后缀时，我们使用getSizeAsMb以保持向后兼容性。
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),   // spark.reducer.maxBlocksInFlightPerAddress
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),  // spark.maxRemoteBlockSizeFetchToMem
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true))

    val serializerInstance = dep.serializer.newInstance()

    // 为每个流创建一个键/值迭代器。
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: 下面的asKeyValueIterator将键/值迭代器包装在NextIterator中。
      // NextIterator确保在所有记录都被读取后调用底层InputStream的close()方法。
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // 更新每个读取的记录的Task上下文指标。.
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // 这里必须使用可中断的迭代器，以支持任务取消。
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    // TODO 2 : 如果存在聚合函数， 则使用聚合函数聚合
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // 我们正在读取已经合并的值。
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // 我们不知道值的类型，但也不关心——依赖项应该已经确保与此聚合器兼容，
        // 该聚合器将把值类型转换为组合类型C。
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // 如果定义了排序顺序，则对输出进行排序。
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter = new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)

        sorter.insertAll(aggregatedIter)

        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })

        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}
