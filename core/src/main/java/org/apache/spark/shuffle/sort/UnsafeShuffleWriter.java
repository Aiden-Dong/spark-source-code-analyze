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
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;
import org.apache.spark.internal.config.package$;

/*****
 *  [[UnsafeShuffleWriter]] 是 Apache Spark 中用于执行 Shuffle 操作的一部分。
 *  这个类利用了Spark的Tungsten执行引擎和Unsafe内存操作来尽量减少Shuffle过程中的内存拷贝和序列化开销。
 *
 *  这里是 [[UnsafeShuffleWriter]] 的基本工作原理：
 *
 *  1. MapTask 的 Shuffle 写入阶段：
 *       - 在Map任务执行过程中，如果需要进行Shuffle，Map任务会将自己的输出数据分成多个分区，然后将每个分区的数据写入[[UnsafeShuffleWriter]]中。
 *       - [[UnsafeShuffleWriter]] 使用基于内存的数据结构来存储数据，而不是直接写入磁盘。这有助于减少磁盘写入操作的频率。
 *  2. 合并和溢写 :
 *       - 当 "UnsafeShuffleWriter" 中的某个分区的数据量达到一定阈值时，它会触发一个合并操作，将多个小的内存分区合并成一个更大的内存分区。
 *         这有助于减少内存碎片和提高内存的利用率。
 *       - 当内存分区无法容纳更多数据时，"UnsafeShuffleWriter" 会将部分数据写入磁盘，以释放内存空间。
 *  3. Sort 和 Write:
 *       - 在每个内存分区中，数据会按照指定的排序顺序进行排序，以便在后续的Reduce任务中进行更高效的合并操作。
 *       - 排序后的数据会被写入磁盘文件，这些文件的位置信息会被保存以供后续的Reduce任务使用。
 *
 *   总的来说，"UnsafeShuffleWriter"利用内存数据结构、排序和合并技术以及最小化磁盘操作的策略来加速Shuffle过程，从而提高Spark作业的性能。
 *   不过需要注意的是，虽然"UnsafeShuffleWriter"能够带来性能上的提升，但它也可能会对内存资源造成一定的压力，特别是在处理大规模数据时。
 */
@Private
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting
  static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;                        // 数据块管理工具
  private final IndexShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;                  // 内存管理
  private final SerializerInstance serializer;
  private final Partitioner partitioner;                          // 分区类
  private final ShuffleWriteMetrics writeMetrics;                 // 指标类工具
  private final int shuffleId;                                    // shuffle dependency 注册的 shuffleId
  private final int mapId;                                        // 当前的任务ID
  private final TaskContext taskContext;                          // 任务描述上下文
  private final SparkConf sparkConf;                              // Spark 配置
  private final boolean transferToEnabled;                        // 是否开启 stransferTo 进行文件拷贝
  private final int initialSortBufferSize;                        // 初始化用户排序的 buffer 大小  spark.shuffle.sort.initialBufferSize
  private final int inputBufferSizeInBytes;                       // spark.shuffle.file.buffer
  private final int outputBufferSizeInBytes;                      // spark.shuffle.unsafe.file.output.buffer

  @Nullable private MapStatus mapStatus;
  @Nullable private ShuffleExternalSorter sorter;                // 主要用于排序写的工具类
  private long peakMemoryUsedBytes = 0;

  // 用户获取序列化对象的工具类
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;    // 将对象序列化的流工具

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  private class CloseAndFlushShieldOutputStream extends CloseShieldOutputStream {

    CloseAndFlushShieldOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public void flush() {
      // do nothing
    }
  }

  public UnsafeShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf) throws IOException {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
        "UnsafeShuffleWriter can only be used for shuffles with at most " +
        SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
        " reduce partitions");
    }
    this.blockManager = blockManager;
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);  // 是否开启 stransferTo
    this.initialSortBufferSize = sparkConf.getInt("spark.shuffle.sort.initialBufferSize", DEFAULT_INITIAL_SORT_BUFFER_SIZE);
    this.inputBufferSizeInBytes = (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.outputBufferSizeInBytes = (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    open();
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * This convenience method should only be called in test code.
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    try {
      while (records.hasNext()) {
        // 迭代数据，将数据序列化后，写入排序器中
        insertRecordIntoSorter(records.next());
      }
      //
      closeAndWriteOutput();
      success = true;
    } finally {
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                         "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() {
    assert (sorter == null);
    sorter = new ShuffleExternalSorter(
      memoryManager,
      blockManager,
      taskContext,
      initialSortBufferSize,
      partitioner.numPartitions(),
      sparkConf,
      writeMetrics);
    serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);

    // 更新当前的内存使用情况
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    // spill 并且返回所有的spill 文件
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    sorter = null;
    final long[] partitionLengths;
    // 获取最终输出文件
    final File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    // 拿到临时写文件
    final File tmp = Utils.tempFileWith(output);

    try {
      try {
        partitionLengths = mergeSpills(spills, tmp);
      } finally {
        for (SpillInfo spill : spills) {
          if (spill.file.exists() && ! spill.file.delete()) {
            logger.error("Error while deleting spill file {}", spill.file.getPath());
          }
        }
      }
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  /***
   * 将数据写入到排序器中
   * @param record
   * @throws IOException
   */
  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);  // 获取分区号

    serBuffer.reset();  // 清空流

    // 将对象序列化成字节
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    // 序列化后的对象大小
    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    // 将数据写到排序器中
    sorter.insertRecord(serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * 将spills文件合并写出到目标文件
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpills(SpillInfo[] spills, File outputFile) throws IOException {

    // 是否开启 shuffle 压缩
    final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
    // 获取压缩实例
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    // 是否开启快速合并
    final boolean fastMergeEnabled = sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);

    // 是否支持快速压缩 : 没有开启压缩 || 或者压缩类型是 snappy / lz4 等
    final boolean fastMergeIsSupported = !compressionEnabled || CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    // 是否开启加密 spark.io.encryption.enabled
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();

    try {
      if (spills.length == 0) {
        new FileOutputStream(outputFile).close(); // Create an empty file
        return new long[partitioner.numPartitions()];
      } else if (spills.length == 1) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        Files.move(spills[0].file, outputFile);
        return spills[0].partitionLengths;
      } else {
        final long[] partitionLengths;
        // There are multiple spills to merge, so none of these spill files' lengths were counted
        // towards our shuffle write count or shuffle write time. If we use the slow merge path,
        // then the final output file's size won't necessarily be equal to the sum of the spill
        // files' sizes. To guard against this case, we look at the output file's actual size when
        // computing shuffle bytes written.
        //
        // We allow the individual merge methods to report their own IO times since different merge
        // strategies use different IO techniques.  We count IO during merge towards the shuffle
        // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
        // branch in ExternalSorter.
        if (fastMergeEnabled && fastMergeIsSupported) {
          // Compression is disabled or we are using an IO compression codec that supports
          // decompression of concatenated compressed streams, so we can perform a fast spill merge
          // that doesn't need to interpret the spilled bytes.
          if (transferToEnabled && !encryptionEnabled) {
            logger.debug("Using transferTo-based fast merge");
            partitionLengths = mergeSpillsWithTransferTo(spills, outputFile);
          } else {
            logger.debug("Using fileStream-based fast merge");
            partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null);
          }
        } else {
          logger.debug("Using slow merge");
          partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
        }
        // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
        // in-memory records, we write out the in-memory records to a file but do not count that
        // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
        // to be counted as shuffle write, but this will lead to double-counting of the final
        // SpillInfo's bytes.
        writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
        writeMetrics.incBytesWritten(outputFile.length());
        return partitionLengths;
      }
    } catch (IOException e) {
      if (outputFile.exists() && !outputFile.delete()) {
        logger.error("Unable to delete output file {}", outputFile.getPath());
      }
      throw e;
    }
  }

  /**
   * 使用Java FileStreams合并溢出文件。
   * 相对于基于NIO的合并方式，{@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[], File)}，
   * 这种代码路径通常较慢，主要用于IO压缩编解码器不支持压缩数据的串联、启用了加密或用户显式禁用了{@code transferTo}以解决内核错误的情况。
   * 此代码路径在溢出的各个分区大小较小且 {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo} 方法执行许多小的磁盘IO操作的情况下，也可能更快。
   * 在这些情况下，使用大缓冲区来进行输入和输出文件有助于减少磁盘IO次数，从而加快文件合并速度。
   *
   * @param spills the spills to merge.
   * @param outputFile the file to write the merged data to.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithFileStream(
      SpillInfo[] spills,
      File outputFile,
      @Nullable CompressionCodec compressionCodec) throws IOException {
    assert (spills.length >= 2);

    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];

    // 拿到对应的输入流
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    // 最终写出文件流
    final OutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile), outputBufferSizeInBytes);
    // 使用计数输出流，以避免在每个分区写入后关闭底层文件并向文件系统请求其大小。
    final CountingOutputStream mergedFileOutputStream = new CountingOutputStream(bos);

    boolean threwException = true;
    try {
      // 输入文件
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] = new NioBufferedFileInputStream(spills[i].file, inputBufferSizeInBytes);
      }
      // 遍历所有的分区
      for (int partition = 0; partition < numPartitions; partition++) {
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // Shield the underlying output stream from close() and flush() calls, so that we can close
        // the higher level streams to make sure all data is really flushed and internal state is
        // cleaned.
        // 基于计数器的写流
        OutputStream partitionOutput = new CloseAndFlushShieldOutputStream(new TimeTrackingOutputStream(writeMetrics, mergedFileOutputStream));

        partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);

        if (compressionCodec != null) {
          partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
        }

        for (int i = 0; i < spills.length; i++) {
          // 拿到当前SpillFile 的当前分区的数据长度
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          if (partitionLengthInSpill > 0) {
            InputStream partitionInputStream = new LimitedInputStream(spillInputStreams[i], partitionLengthInSpill, false);
            try {
              partitionInputStream = blockManager.serializerManager().wrapForEncryption(partitionInputStream);
              if (compressionCodec != null) {
                partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
              }
              // 将SpillFile的当前分区写出到目标文件
              ByteStreams.copy(partitionInputStream, partitionOutput);
            } finally {
              partitionInputStream.close();
            }
          }
        }
        partitionOutput.flush();
        partitionOutput.close();

        // 记录每个分区的数据长度
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      Closeables.close(mergedFileOutputStream, threwException);
    }
    return partitionLengths;
  }

  /**
   * 使用NIO的transferTo合并溢出文件，将溢出分区的字节串联起来。
   * 只有在IO压缩编解码器和序列化器支持串联序列化流时，这种方式才是安全的。
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithTransferTo(SpillInfo[] spills, File outputFile) throws IOException {
    assert (spills.length >= 2);
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];  // 记录每个分区的数据长度
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    final long[] spillInputChannelPositions = new long[spills.length];
    FileChannel mergedFileOutputChannel = null;

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();

      long bytesWrittenToMergedFile = 0;
      // 遍历每个分区
      for (int partition = 0; partition < numPartitions; partition++) {
        // 迭代每个Spill文件，将每个Spill文件的对应分区属于写出到目标分区
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          final FileChannel spillInputChannel = spillInputChannels[i];
          final long writeStartTime = System.nanoTime();
          Utils.copyFileStreamNIO(
            spillInputChannel,
            mergedFileOutputChannel,
            spillInputChannelPositions[i],
            partitionLengthInSpill);
          spillInputChannelPositions[i] += partitionLengthInSpill;
          writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          bytesWrittenToMergedFile += partitionLengthInSpill;
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
          "Current position " + mergedFileOutputChannel.position() + " does not equal expected " +
            "position " + bytesWrittenToMergedFile + " after transferTo. Please check your kernel" +
            " version to see if it is 2.6.32, as there is a kernel bug which will lead to " +
            "unexpected behavior when using transferTo. You can set spark.file.transferTo=false " +
            "to disable this NIO feature."
        );
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (int i = 0; i < spills.length; i++) {
        assert(spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
      Closeables.close(mergedFileOutputChannel, threwException);
    }
    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }
}
