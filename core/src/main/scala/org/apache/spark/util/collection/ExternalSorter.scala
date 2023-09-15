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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter}

/**
 * 对一些 K-V 进行排序和可能的合并，以产生K-C。
 * 首先使用一个分区器将Key分组到不同的分区，然后可选地在每个分区内使用自定义比较器对Key进行排序。
 * 可以输出一个分区文件，每个分区都有不同的字节范围，适用于洗牌获取。
 *
 * 如果没有开启 Combine 操作， 那么 K 类型必须与V类型相同。
 * 我们会在最后将对象进行强制类型转换。
 *
 * 注意：尽管 [[ExternalSorter]] 是一个相当通用的排序器，但是它的一些配置与其他Shuffle排序器公用（例如，它的块压缩由spark.shuffle.compress控制）。
 * 如果 [[ExternalSorter]] 在其他非shuffle上下文中被使用，而我们可能想要使用不同的配置设置，我们可能需要重新考虑这一点。
 *
 * @param aggregator 可选的合并器（Aggregator），其中包含用于合并数据的组合函数。
 * @param partitioner 可选的分区器；如果提供了，按分区ID和Key进行排序。
 * @param ordering 可选的排序规则，用于在每个分区内对键进行排序；应该是一个完全有序的规则
 * @param serializer 在溢出到磁盘时使用的序列化器。
 *
 * 请注意，如果提供了排序规则，我们将始终使用它进行排序，因此只在您确实希望输出key被排序时才提供它。
 * 例如，在没有map端合并的映射任务中，您可以将排序规则传递为None，以避免额外的排序。
 * 另一方面，如果您确实想要进行合并，拥有一个排序规则比没有更有效率。
 *
 * 用户与这个类的交互方式如下：
 *
 * 1. 实例化 [[ExternalSorter]].
 * 2. 调用 insertAll() 插入所有的数据 .
 * 3. 请求一个iterator()以遍历已排序/聚合的记录.
 *     - or -
 *    调用 writePartitionedFile() 来创建一个包含已排序/聚合输出的文件，这个文件可以在Spark的排序shuffle中使用。
 *
 * 从高层次来看，这个类的内部工作方式如下：:
 *
 *  - 我们重复地填充内存数据缓冲区，根据需要使用PartitionedAppendOnlyMap（如果要按键合并）或PartitionedPairBuffer（如果不需要合并）。
 *    在这些缓冲区内，我们按分区ID然后可能也按key对元素进行排序。
 *    为了避免对每个key多次调用[[Partitioner]]，我们将分区ID与每条record一起存储。
 *
 *  - 当每个缓冲区达到我们的内存限制时，我们将其溢出到文件中。这个文件首先按分区ID进行排序，如果我们要进行聚合，可能还会按key或hash(key)进行第二次排序。
 *    对于每个文件，我们跟踪内存中每个分区中有多少对象，这样我们就不必为每个元素写出分区ID。
 *
 *  - 当用户请求迭代器或文件输出时，溢出的文件会被合并，以及任何剩余的内存数据，使用上面定义的相同排序顺序（除非排序和聚合都被禁用）。
 *  如果我们需要按键进行聚合，我们要么使用来自排序参数的完全排序，要么读取具有相同哈希码的键并将它们进行比较以合并值。
 *
 *  - 用户应该在最后调用stop()方法来删除所有中间文件。.
 */
private[spark] class ExternalSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
  with Logging {

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)  // 分区数
  private val shouldPartition = numPartitions > 1

  // 基于数据key获取所在的分区数
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val blockManager = SparkEnv.get.blockManager                   // blockmanager
  private val diskBlockManager = blockManager.diskBlockManager
  private val serializerManager = SparkEnv.get.serializerManager         // 序列化器
  private val serInstance = serializer.newInstance()

  // 用于Shuffle文件的写缓冲区
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // 从序列化器读取/写入时的对象批次大小。
  // 对象被分批写入，每个批次使用自己的序列化流。这可以减少在反序列化流时构建的引用跟踪映射的大小。
  // 注意：将这个值设置得太低可能会导致在序列化时出现过多的拷贝，因为一些序列化器通过在对象数量翻倍时进行增长和拷贝来增加内部数据结构的大小。
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  // 数据的内存缓冲池， 满了将进行spill 操作
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]           // 如果数据需要 combine 操作，我们则将数据放入 map 结构中
  @volatile private var buffer = new PartitionedPairBuffer[K, C]           // 如果数据不需要 combine 操作， 我们则将数据放入buff 数据中

  // 总共序列化到磁盘的字节数
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // 内存使用峰值
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  @volatile private var isShuffleSort: Boolean = true
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  @volatile private var readingIterator: SpillableIterator = null

  // 用于对键K进行排序，以在分区内允许聚合或排序的比较器。如果用户没有提供完全排序，它可以是一个哈希码的部分排序。
  // （部分排序意味着相等的键具有comparator.compare(k, k) = 0，但一些非相等的键也有这个特性，因此我们需要进行后续的处理来找到真正相等的键）。
  // 请注意，如果没有提供聚合器和排序规则，我们会忽略这一点。
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // 关于一个溢出文件的信息。
  // 包括由于我们定期重置其流而由序列化器写入的"批次"的字节大小，以及每个分区中的元素数量，用于在合并时高效地跟踪分区。
  private[this] case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: Array[Long],
    elementsPerPartition: Array[Long])

  private val spills = new ArrayBuffer[SpilledFile]

  // 溢出小文件的数量
  private[spark] def numSpills: Int = spills.size

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // 是否需要合并
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {
      // 如果需要进行与合并， 则直接在内存中进行合并处理
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null

      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }

      while (records.hasNext) {
        addElementsRead()    // 记录数据读取
        kv = records.next()  // 提取出 kv 数据记录, key -> (partitinId, recordKey)
        map.changeValue((getPartition(kv._1), kv._1), update)
        maybeSpillCollection(usingMap = true)
      }
    } else {
      while (records.hasNext) {
        addElementsRead()         // 记录数据读取
        val kv = records.next()   //
        // 将(partitionId, recordKey) -> record 插入到有序数组
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      estimatedSize = map.estimateSize()   // 计算当前数据大小
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    spills += spillFile
  }

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  override protected[this] def forceSpill(): Boolean = {
    if (isShuffleSort) {
      false
    } else {
      assert(readingIterator != null)
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        map = null
        buffer = null
      }
      isSpilled
    }
  }

  /**
   * Spill contents of in-memory iterator to a temporary file on disk.
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator)
      : SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // 记录每个分区写入的数据
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions, s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        inMemoryIterator.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (success) {
        writer.close()
      } else {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }

  /******
   * 合并一系列已排序的文件，提供一个分区迭代器，然后在每个分区内提供元素迭代器。
   * 这可用于要么写出一个新文件，要么将数据返回给用户。
   *
   * 返回一个迭代器，按分区分组的所有写入到此对象的数据。
   * 对于每个分区，我们有一个迭代器来遍历其内容，并且期望按顺序访问它们（不能跳到下一个分区而不读取前一个分区）。
   * 保证按分区ID的顺序返回每个分区的键值对。
   */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {

    // 得到所有 Spill 文件的迭代器
    val readers = spills.map(new SpillReader(_))

    // 得到文件的迭代器
    val inMemBuffered = inMemory.buffered

    // 迭代所有的分区
    (0 until numPartitions).iterator.map { p =>
      // 获取内存数据在当前分区的迭代器
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)

      // 拼接所有的当前分区的磁盘数据迭代器跟内存数据迭代器
      // 对于 SpillFile 时打开所有的文件并定位到当前分区的位置
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)

      if (aggregator.isDefined) {   // 表示如果开启合并功能
        // 在分区之间执行部分聚合。
        (p, mergeWithAggregation(iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * 使用给定的键比较器，合并排序一系列(K, C)迭代器。.
   */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] =
  {
    // 提供了 head 方法的迭代器
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)

    type Iter = BufferedIterator[Product2[K, C]]

    // 他把迭代器作为元素
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // 比较返回头部元素最小的迭代器
      override def compare(x: Iter, y: Iter): Int = comparator.compare(y.head._1, x.head._1)
    })

    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true

    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = !heap.isEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()   // 取出迭代器， 当前迭代器的首部元素已经是所有元素中最小的了
        val firstPair = firstBuf.next() // 拿到头部元素
        if (firstBuf.hasNext) {         // 如果当前迭代器中还有元素，则继续放置到比较队列中
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  /**
   * 合并一系列(K, C)迭代器，通过对每个键聚合值，假设每个迭代器都按照给定的比较器按键排序。
   * 如果比较器不是完全排序（例如，当我们按哈希码对对象进行排序时，不同的键可能比较相等，尽管它们实际上不相等），我们仍然通过对所有比较相等的键进行相等性测试来合并它们。
   */
  private def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],   // 当前所有的待合并的数据
      mergeCombiners: (C, C) => C,                // combine 操作
      comparator: Comparator[K],                  // key 比较器, 默认hashcode
      totalOrder: Boolean)                       // 是否排序
      : Iterator[Product2[K, C]] =
  {
    if (!totalOrder) {
      // 我们只有部分排序，例如按哈希码比较键，这意味着多个不同的键可能被排序视为相等。
      // 为了处理这个情况，我们需要一次读取所有按排序视为相等的键，并进行比较。
      new Iterator[Iterator[Product2[K, C]]] {

        // 归并排序
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1
          // 表示如果key 相同
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {  // 当前key 跟前一个key相同
                combiners(i) = mergeCombiners(combiners(i), pair._2) // 合并结果数
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)
        }
      }.flatMap(i => i)
    } else {
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  /**
   * 用于按分区读取溢出文件的内部类。期望按顺序请求所有分区。
   */
  private[this] class SpillReader(spill: SpilledFile) {
    // 序列化器批量偏移量；大小将为batchSize.length + 1。
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // 跟踪我们所在的分区和批处理流。
    // 这些将是我们将要读取的下一个元素的索引。
    // 我们还将存储最后一次读取的分区，以便readNextPartition()可以确定它来自哪个分区。
    var partitionId = 0
    var indexInPartition = 0L
    var batchId = 0
    var indexInBatch = 0
    var lastPartitionId = 0

    skipToNextPartition()

    // 中间文件和从一个批处理精确读取的反序列化器流。
    // 这可以防止高级流的预取和其他任意行为。
    var fileStream: FileInputStream = null
    var deserializeStream = nextBatchStream()  // Also sets fileStream

    var nextItem: (K, C) = null
    var finished = false

    /** 构建一个只从下一批次中读取的流。 */
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchId < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchId)
        fileStream = new FileInputStream(spill.file)
        fileStream.getChannel.position(start)  // 定向数据读取位置
        batchId += 1

        val end = batchOffsets(batchId)  // 当前分区的截至位置

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))

        val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
        serInstance.deserializeStream(wrappedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Update partitionId if we have reached the end of our current partition, possibly skipping
     * empty partitions on the way.
     */
    private def skipToNextPartition() {
      while (partitionId < numPartitions &&
          indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    /**
     * 从反序列化流中返回下一个(K, C)对，并更新partitionId、indexInPartition、indexInBatch等以匹配其位置。
     * 如果当前批次已耗尽，则构建下一批次的流并从中读取。如果没有剩余的键值对，则返回null。
     */
    private def readNextItem(): (K, C) = {
      if (finished || deserializeStream == null) {
        return null
      }
      // 读取数据集
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      lastPartitionId = partitionId
      // Start reading the next batch if we're done with this one
      indexInBatch += 1
      // 如果当前批次数据读取数超过限制， 则重新开启一个批次来执行
      if (indexInBatch == serializerBatchSize) {
        indexInBatch = 0
        deserializeStream = nextBatchStream()
      }
      // Update the partition location of the element we're reading
      indexInPartition += 1
      skipToNextPartition()
      // If we've finished reading the last partition, remember that we're done
      if (partitionId == numPartitions) {
        finished = true
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      (k, c)
    }

    var nextPartitionToRead = 0

    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    def cleanup() {
      batchId = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      if (ds != null) {
        ds.close()
      }
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map.
   * If this iterator is forced spill to disk to release memory when there is not enough memory,
   * it returns pairs from an on-disk map.
   */
  def destructiveIterator(memoryIterator: Iterator[((Int, K), C)]): Iterator[((Int, K), C)] = {
    if (isShuffleSort) {
      memoryIterator
    } else {
      readingIterator = new SpillableIterator(memoryIterator)
      readingIterator
    }
  }

  /**
   * 返回一个迭代器，按分区分组并由请求的聚合器聚合的所有写入到此对象的数据。
   * 对于每个分区，我们有一个迭代器来遍历其内容，并且期望按顺序访问它们（不能跳到下一个分区而不读取前一个分区）。
   * 保证按分区ID的顺序返回每个分区的键值对。
   *
   * 目前，我们只在一个步骤中合并所有溢出的文件，但可以修改以支持分层合并。
   * Exposed for testing.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined

    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer

    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (!ordering.isDefined) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(None)))
      } else {
        // We do need to sort by both partition ID and key
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      // Merge spilled and in-memory data
      // 返回内存数据的迭代器
      val memIter = destructiveIterator(collection.partitionedDestructiveSortedIterator(comparator))
      merge(spills, memIter)
    }
  }

  /**
   * Return an iterator over all the data written to this object, aggregated by our aggregator.
   */
  def iterator: Iterator[Product2[K, C]] = {
    isShuffleSort = false
    partitionedIterator.flatMap(pair => pair._2)
  }

  /**
   * 将添加到此ExternalSorter的所有数据写入磁盘存储中的文件。
   * 被 [[org.apache.spark.shuffle.sort.SortShuffleWriter]] 调用.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @return 文件每个分区的长度数组，以字节为单位（由映射输出跟踪器使用）。
   */
  def writePartitionedFile(
      blockId: BlockId,
      outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)
    val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
      context.taskMetrics().shuffleWriteMetrics)

    if (spills.isEmpty) {
      // Case where we only have in-memory data
      val collection = if (aggregator.isDefined) map else buffer
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        val segment = writer.commitAndGet()
        lengths(partitionId) = segment.length
      }
    } else {
      // 我们必须执行合并排序；按分区获取一个迭代器，然后直接写入所有数据。
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          val segment = writer.commitAndGet()
          lengths(id) = segment.length
        }
      }
    }

    writer.close()
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    lengths
  }

  def stop(): Unit = {
    spills.foreach(s => s.file.delete())
    spills.clear()
    forceSpillFiles.foreach(s => s.file.delete())
    forceSpillFiles.clear()
    if (map != null || buffer != null) {
      map = null // So that the memory can be garbage-collected
      buffer = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   *
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  private def groupByPartition(data: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
   */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }

  private[this] class SpillableIterator(var upstream: Iterator[((Int, K), C)])
    extends Iterator[((Int, K), C)] {

    private val SPILL_LOCK = new Object()

    private var nextUpstream: Iterator[((Int, K), C)] = null

    private var cur: ((Int, K), C) = readNext()

    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) {
        false
      } else {
        val inMemoryIterator = new WritablePartitionedIterator {
          private[this] var cur = if (upstream.hasNext) upstream.next() else null

          def writeNext(writer: DiskBlockObjectWriter): Unit = {
            writer.write(cur._1._2, cur._2)
            cur = if (upstream.hasNext) upstream.next() else null
          }

          def hasNext(): Boolean = cur != null

          def nextPartition(): Int = cur._1._1
        }
        logInfo(s"Task ${context.taskAttemptId} force spilling in-memory map to disk and " +
          s" it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
        forceSpillFiles += spillFile
        val spillReader = new SpillReader(spillFile)
        nextUpstream = (0 until numPartitions).iterator.flatMap { p =>
          val iterator = spillReader.readNextPartition()
          iterator.map(cur => ((p, cur._1), cur._2))
        }
        hasSpilled = true
        true
      }
    }

    def readNext(): ((Int, K), C) = SPILL_LOCK.synchronized {
      if (nextUpstream != null) {
        upstream = nextUpstream
        nextUpstream = null
      }
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null
      }
    }

    override def hasNext(): Boolean = cur != null

    override def next(): ((Int, K), C) = {
      val r = cur
      cur = readNext()
      r
    }
  }
}
