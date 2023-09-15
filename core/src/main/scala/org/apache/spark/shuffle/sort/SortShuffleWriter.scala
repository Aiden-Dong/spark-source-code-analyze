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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

/****
 * [[SortShuffleWriter]] 是 Apache Spark 中的一个组件，用于在 Shuffle 过程中将数据进行排序并写入磁盘。
 *
 * [[SortShuffleWriter]] 的主要作用是在 Shuffle 阶段将分区数据进行排序，然后将排序后的数据写入磁盘，以便在后续阶段进行合并和汇总操作。
 *  - 排序有助于提高后续的聚合操作效率，因为排序后的数据可以更快速地执行合并等操作。
 *  - 分区数据的排序： 首先，对每个分区的数据进行排序，通常采用外部排序算法，以确保数据有序。
 *
 * 具体来说，[[SortShuffleWriter]] 的主要步骤包括 :
 *  - 分区数据的排序： 首先，对每个分区的数据进行排序，通常采用外部排序算法，以确保数据有序。
 *  - 数据写入磁盘： 排序后的数据将被写入磁盘的临时文件中。这些临时文件将用于后续的 Shuffle 阶段。
 *  - 元数据记录： 记录元数据，如每个分区的数据所在的文件位置、偏移量等信息，以便后续的 Shuffle 阶段可以正确地读取和合并数据。
 *  - 溢写和合并： 如果数据量很大，可能需要进行溢写，将排序后的数据分批写入多个临时文件。后续的 Shuffle 阶段会将这些文件进行合并，以生成最终的结果。
 */
private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency                     // ShuffleDependency
  private val blockManager = SparkEnv.get.blockManager    // BlockManager
  private var sorter: ExternalSorter[K, V, _] = null      // 外部排序器

  //  我们是否在停止过程中？
  //  因为ShuffleMapTask可以先调用stop()，成功返回true，然后如果它们遇到异常，可能会再次调用stop()，但这次成功返回false。
  //  因此，我们要确保我们不会尝试两次删除文件等操作。
  private var stopping = false
  private var mapStatus: MapStatus = null
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /***
   * 将数据写出去
   * @param records
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {

    sorter = if (dep.mapSideCombine) {
      // 如果需要combine 操作，提供了key 排序跟key 合并类
      new ExternalSorter[K, V, C](context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // 如果不需要combine操作
      // 在这种情况下，我们既不向排序器传递聚合器，也不向排序器传递排序，因为我们不关心每个分区中的键是否已排序；
      // 这将在reduce方面完成
      new ExternalSorter[K, V, V](context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    
    sorter.insertAll(records)

    // 不必费心将打开合并输出文件的时间包含在 ShuffleWriter 时间中,
    // 因为它只打开一个文件，所以通常速度太快而无法准确测量 (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)    // shuffle_{shuffleId}_{mapId}_0.data
    val tmp = Utils.tempFileWith(output)

    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  /***
   * 判断是否支持 BypassMergeSortShuffleHandle
   * 当前不能设置  combine, 并且 rdd 的分区数少于 spark.shuffle.sort.bypassMergeThreshold
   */
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      false
    } else {
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
