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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._

/**
 *
 * 在基于排序的 Shuffle 中，传入的record会根据它们的目标分区ID进行排序，然后写入单个map输出文件。
 *
 * Reducers 会获取该文件的有序区域，以便读取属于它们的map输出部分。
 * 在map输出数据过大无法放入内存的情况下，输出的已排序子集可以被溢写到磁盘，
 * 并且这些磁盘文件会被合并以生成最终的输出文件。
 *
 * 基于排序的Shuffle有两种不同的写入路径，用于生成其map输出文件。:
 *
 *  - 序列化排序: 当以下三个条件都满足时使用：:
 *    1. Shuffle依赖未指定聚合或要求输出有序.
 *    2. Shuffle序列化器支持重新定位序列化值（目前由KryoSerializer和Spark SQL的自定义序列化器支持）。
 *    3. Shuffle生成的输出分区少于16777216个。
 *  - 反序列化排序: 用于处理所有其他情况。
 *
 * -----------------------
 * 序列化排序 mode
 * -----------------------
 *
 * 在序列化排序模式下，传入的Record在传递给ShuffleWriter后立即进行序列化，并在排序过程中以序列化形式进行缓冲。这个写入路径实现了几种优化：
 *
 *  - 它的排序操作基于序列化的二进制数据，而不是Java对象，这减少了内存消耗和垃圾回收开销。
 *    这种优化需要记录序列化器具有某些属性，以允许排序序列化的Record而无需进行反序列化。
 *    有关更多详细信息，请参见首次提出并实施这种优化的 SPARK-4550。
 *
 *  - 它使用了一个专门的高缓存效率的排序器([[ShuffleExternalSorter]])，该Sorter对压缩Record指针和分区ID的数组进行排序。
 *    通过在排序数组中每个记录仅使用8字节的空间，可以将更多的数组数据放入缓存中。
 *
 *  - 溢写合并过程操作的是属于同一分区的序列化记录块，在合并过程中不需要对记录进行反序列化。
 *
 *  - 当溢写压缩编解码器支持压缩数据的串联时，溢写合并过程仅仅将序列化和压缩的溢写分区串联起来，生成最终的输出分区。
 *    这允许使用高效的数据复制方法，例如NIO的 transferTo，并且避免在合并过程中分配解压缩或复制缓冲区。
 *
 * For more details on these optimizations, see SPARK-7081.
 */
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  // 校验是否支持 Shuffle 溢写操作
  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  // ShuffleId -> ShuffleMapFile 的Map
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  // 需要指出这个工具的作用
  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)


  /***
   * 获取一个用于传递给Task的[[ShuffleHandle]]。
   *
   * @param shuffleId   ShuffleDependency 的 shuffleId
   * @param numMaps     ShuffleDependency 的 shuffleId
   * @param dependency  Rdd 对应的 ShuffleDependency
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,  //
      numMaps: Int,    // 输出文件的数量
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // 当前不能设置  combine, 并且 rdd 的分区数少于 spark.shuffle.sort.bypassMergeThreshold
      // 如果分区数少于 spark.shuffle.sort.bypassMergeThreshold，并且我们不需要在map阶段进行combine操作，
      // 那么直接写入 numPartitions 个文件，然后在最后将它们拼接在一起。这样可以避免序列化和反序列化两次，
      // 以合并溢写文件，这在正常的代码路径中会发生。不足之处是一次打开多个文件，从而在内存中分配更多缓冲区。
      new BypassMergeSortShuffleHandle[K, V](shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // 否则，尝试以序列化的形式缓存映射输出，因为这样更高效：
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // 否则，以非序列化的形式缓存映射输出。:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /***
   * 返回一个 Shuffle 数据的读取器
   * @param handle
   * @param startPartition 起始分区
   * @param endPartition   结束分区
   * @param context        任务上下文
   * @return
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }


  /***
   * 为给定的分区获取一个ShuffleWriter。通过MapTask 在Executor 上调用
   * @param handle ShuffleDependency 注册得到的 ShuffleHandler 对象
   * @param mapId 用于写出的分区文件
   * @param context 任务上下文
   * @return
   */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    val env = SparkEnv.get
    handle match {
      // SerializedShuffleHandle
      // 基于序列化对象的排序器
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf)
        // BypassMergeSortShuffleHandle
        // 每个分区一个文件的shuffle写工具
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          bypassMergeSortHandle,
          mapId,
          context,
          env.conf)
        // SortShuffleWriter
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object SortShuffleManager extends Logging {

  /**
   * The maximum number of shuffle output partitions that SortShuffleManager supports when
   * buffering map outputs in a serialized form. This is an extreme defensive programming measure,
   * since it's extremely unlikely that a single shuffle produces over 16 million output partitions.
   * */
  val MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
    PackedRecordPointer.MAXIMUM_PARTITION_ID + 1

  /**
   * 这是一个帮助方法，用于确定一个shuffle操作是否应该使用优化的序列化shfulle路径，
   * 还是应该回退到原始路径，该原始路径是针对非序列化对象进行操作的。
   */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions

    // 判断当前序列化对象是否支持在任务执行期间重新定位
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.mapSideCombine) {
      // 是否进行combine操作
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because we need to do " +
        s"map-side aggregation")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      // 分区数是否大于 16777216
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
 */
private[spark] class SerializedShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * bypass merge sort shuffle path.
 */
private[spark] class BypassMergeSortShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}
