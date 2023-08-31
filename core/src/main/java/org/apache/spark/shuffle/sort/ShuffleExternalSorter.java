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

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * 一个专门为基于排序的shuffle而设计的外部排序器。.
 * <p>
 *
 * 传入的 record 被追加到数据页中。
 * 当所有record都被插入（或者当前线程的shuffle内存限制达到）时，内存中的record将根据它们的分区ID进行排序（使用 {@link ShuffleInMemorySorter}）。
 * 然后，排序后的记录将被写入单个输出文件（如果发生溢出，则写入多个文件）。
 * 输出文件的格式与由{@link org.apache.spark.shuffle.sort.SortShuffleWriter}写入
 * 最终输出文件的格式相同：每个输出分区的记录都以单个序列化、压缩的流的形式写入，可以使用新的解压缩和反序列化流进行读取。
 * <p>
 *
 * 与{@link org.apache.spark.util.collection.ExternalSorter}不同，这个排序器不会合并其溢出文件。
 * 相反，这个合并操作是在{@link UnsafeShuffleWriter}中执行的，它使用了一个专门的合并过程，避免了额外的序列化/反序列化。
 */
final class ShuffleExternalSorter extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ShuffleExternalSorter.class);

  @VisibleForTesting
  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

  private final int numPartitions;                        // 要写出的分区数
  private final TaskMemoryManager taskMemoryManager;       // Task 内存管理器
  private final BlockManager blockManager;                 // 数据块管理工具
  private final TaskContext taskContext;                   // 任务描述器
  private final ShuffleWriteMetrics writeMetrics;          // shuffle 写的指标工具

  // 当内存中有这么多元素时，强制该排序器进行溢出。
  private final int numElementsForSpillThreshold;

  // 在使用DiskBlockObjectWriter写入溢出时要使用的缓冲区大小
  // spark.shuffle.file.buffer
  // 32K 默认
  private final int fileBufferSizeBytes;

  // 在将排序后的record写入磁盘文件时要使用的缓冲区大小。
  // spark.shuffle.spill.diskWriteBufferSize
  // 1MB
  private final int diskWriteBufferSize;

  /**
   * 内存池
   * 保存被排序记录的内存页。在溢出时，这个列表中的页会被释放，尽管从原则上讲我们可以在溢出之间循环利用这些页
   * （另一方面，如果我们在TaskMemoryManager中维护了一个可重用的页池，这可能是不必要的）。
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  /**
   * 目前为止这个排序器使用的最大内存峰值，以字节为单位
   **/
  private long peakMemoryUsedBytes;

  // These variables are reset after spilling:
  @Nullable private ShuffleInMemorySorter inMemSorter;  //  内存shuffle排序器
  @Nullable private MemoryBlock currentPage = null;     // 当前正在使用的内存块
  private long pageCursor = -1;               // 页的当前内存位置

  ShuffleExternalSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetrics writeMetrics) {
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = memoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.numPartitions = numPartitions;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSizeBytes = (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;

    this.numElementsForSpillThreshold = (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());

    this.writeMetrics = writeMetrics;

    this.inMemSorter = new ShuffleInMemorySorter(this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true));

    this.peakMemoryUsedBytes = getMemoryUsage();

    this.diskWriteBufferSize = (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
  }

  /**
   * 对内存中的记录进行排序，并将排序后的记录写入磁盘文件。这个方法不会释放排序的数据结构。
   *
   * @param isLastFile 如果为true，这表示我们正在写入最终的输出文件，并且写入的字节应该计入洗牌溢出指标而不是洗牌写入指标。
   */
  private void writeSortedFile(boolean isLastFile) {

    final ShuffleWriteMetrics writeMetricsToUse;

    if (isLastFile) {
      // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
      writeMetricsToUse = writeMetrics;
    } else {
      // We're spilling, so bytes written should be counted towards spill rather than write.
      // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
      // them towards shuffle bytes written.
      writeMetricsToUse = new ShuffleWriteMetrics();
    }

    // 将 inMemSorter 排序并返回一个迭代器
    final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords = inMemSorter.getSortedIterator();

    // 对象拷贝到writeBuffer用于写磁盘
    final byte[] writeBuffer = new byte[diskWriteBufferSize];

    // 因为这个输出将在shuffle期间被读取，所以它的压缩编解码器必须由spark.shuffle.compress控制
    // 而不是由spark.shuffle.spill.compress控制，因此我们需要在这里使用createTempShuffleBlock
    // 更多细节请参见SPARK-3426。
    final Tuple2<TempShuffleBlockId, File> spilledFileInfo = blockManager.diskBlockManager().createTempShuffleBlock();
    final File file = spilledFileInfo._2();
    final TempShuffleBlockId blockId = spilledFileInfo._1();


    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    final SerializerInstance ser = DummySerializerInstance.INSTANCE;

    final DiskBlockObjectWriter writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse);

    int currentPartition = -1;
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    // 遍历有序数据集
    while (sortedRecords.hasNext()) {
      sortedRecords.loadNext();

      // 获取写分区
      final int partition = sortedRecords.packedRecordPointer.getPartitionId();
      assert (partition >= currentPartition);

      if (partition != currentPartition) {
        // Switch to the new partition
        if (currentPartition != -1) {
          final FileSegment fileSegment = writer.commitAndGet();
          spillInfo.partitionLengths[currentPartition] = fileSegment.length();  // 记录每个分区的长度
        }
        currentPartition = partition;
      }
      // 基于指针对象，获取到对象
      final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
      final Object recordPage = taskMemoryManager.getPage(recordPointer);
      final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
      // 数据大小
      int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);

      long recordReadPosition = recordOffsetInPage + uaoSize; // skip over record length
      // 将数据写出到文件
      while (dataRemaining > 0) {
        final int toTransfer = Math.min(diskWriteBufferSize, dataRemaining);
        Platform.copyMemory(recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);

        writer.write(writeBuffer, 0, toTransfer);
        recordReadPosition += toTransfer;
        dataRemaining -= toTransfer;
      }

      // 记录数据写
      writer.recordWritten();
    }

    final FileSegment committedSegment = writer.commitAndGet();
    writer.close();
    // If `writeSortedFile()` was called from `closeAndGetSpills()` and no records were inserted,
    // then the file might be empty. Note that it might be better to avoid calling
    // writeSortedFile() in that case.
    if (currentPartition != -1) {
      spillInfo.partitionLengths[currentPartition] = committedSegment.length();
      spills.add(spillInfo);
    }

    if (!isLastFile) {  // i.e. this is a spill file
      // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when records
      // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
      // relies on its `recordWritten()` method being called in order to trigger periodic updates to
      // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
      // counter at a higher-level, then the in-progress metrics for records written and bytes
      // written would get out of sync.
      //
      // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
      // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
      // metrics to the true write metrics here. The reason for performing this copying is so that
      // we can avoid reporting spilled bytes as shuffle write bytes.
      //
      // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
      // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
      // This means that this IO time is not accounted for anywhere; SPARK-3577 will fix this.
      writeMetrics.incRecordsWritten(writeMetricsToUse.recordsWritten());
      taskContext.taskMetrics().incDiskBytesSpilled(writeMetricsToUse.bytesWritten());
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spills.size(),
      spills.size() > 1 ? " times" : " time");

    // 将inMemSorter 数据刷写到一个新的文件，刷写的文件信息记录到 SpillInfo 对象中
    writeSortedFile(false);
    final long spillSize = freeMemory();

    // 清空 inMemSorter
    inMemSorter.reset();
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // pages, we might not be able to get memory for the pointer array.
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    return spillSize;
  }

  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  public void cleanupResources() {
    freeMemory();
    if (inMemSorter != null) {
      inMemSorter.free();
      inMemSorter = null;
    }
    for (SpillInfo spill : spills) {
      if (spill.file.exists() && !spill.file.delete()) {
        logger.error("Unable to delete spill file {}", spill.file.getPath());
      }
    }
  }

  /********8
   * 检查是否有足够的空间将额外的记录插入排序指针数组中，并在需要额外空间时扩展数组。
   * 如果无法获得所需的空间，则会将内存中的数据溢出到磁盘。
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        array = allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        // The pointer array is too big to fix in a single page, spill.
        spill();
        return;
      } catch (SparkOutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        freeArray(array);
      } else {
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * 为了插入额外的记录而分配更多的内存。这将从内存管理器请求额外的内存，如果无法获取所请求的内存，则会进行溢出操作。
   *
   * @param required 数据页中所需的空间，以字节为单位，包括用于存储记录大小的空间。
   *                 。
   */
  private void acquireNewPageIfNecessary(int required) {
    // 如果当前没有分配页，或者分配的内存页剩余空间不足，则分配一个新的页
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
      // TODO: try to find space in previous pages
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * 将一个数据record写入到输入流中
   * @param recordBase 序列化对象
   * @param recordOffset 数据位置
   * @param length 数据大小
   * @param partitionId 要写入的分区
   */
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
    throws IOException {

    // for tests
    assert(inMemSorter != null);

    // 如果内存数据量过多， 则直接spill 出去
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " + numElementsForSpillThreshold);
      spill();
    }

    // inMemSorter 内存准备
    growPointerArrayIfNecessary();

    // 获取 UAO 对象大小
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();

    // 需求内存总大小 = 序列化对象长度 + UAO 对象大小
    final int required = length + uaoSize;

    // 检查内存页是否满足条件，不满足条件则准备一个新的内存页
    acquireNewPageIfNecessary(required);

    assert(currentPage != null);

    // 拿到底层的存储对象
    final Object base = currentPage.getBaseObject();

    // 拿到要写入的地址
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);

    // 将UAO 放置到内存页中
    UnsafeAlignedOffset.putSize(base, pageCursor, length);
    pageCursor += uaoSize;
    // 将序列化对象放置到内存页中
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;

    inMemSorter.insertRecord(recordAddress, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (inMemSorter != null) {
      // Do not count the final file towards the spill count.
      writeSortedFile(true);
      freeMemory();
      inMemSorter.free();
      inMemSorter = null;
    }
    return spills.toArray(new SpillInfo[spills.size()]);
  }

}
