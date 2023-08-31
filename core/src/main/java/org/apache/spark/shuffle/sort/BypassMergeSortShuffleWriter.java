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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.annotation.Nullable;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

/***********************************************************
 *
 * 这个类的作用是在基于排序的shuffle操作中，为hash式shuffle提供备用的执行路径。
 * 这个写入路径将传入的record写入单独的文件，每个reduce分区一个文件，然后将这些分区的文件连接起来形成一个单一的输出文件。
 * 提供给reduce使用。
 *
 * Record并不在内存中缓冲。它是一种特殊的写出格式，写出内容可以通过{@link org.apache.spark.shuffle.IndexShuffleBlockResolver} 来提供/消费。
 * <p>
 * 对于包含大量reduce分区的shuffle操作来说，这个写入路径效率较低，
 * 因为它会同时为所有分区打开独立的序列化器和文件流。
 *
 * 因此，{@link SortShuffleManager} 仅在以下情况下选择这个写入路径：
 * <ul>
 *    <li>不存在排序的情况,</li>
 *    <li>没有指定聚合器</li>
 *    <li>分区数量少于  <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * 这段代码曾经是 {@link org.apache.spark.util.collection.ExternalSorter} 的一部分，但为了降低代码复杂性，它被重构到自己的类中
 * 有关详细信息，请参阅 SPARK-7855。
 * <p>
 *
 * 曾有提议完全移除这段代码路径；有关详细信息，请参阅 SPARK-6026。
 */
final class BypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

  private final int fileBufferSize;           // Shuffle 写Buffer 大小
  private final boolean transferToEnabled;    // 文件传输过程中是否开启 NIO transferTO 方法
  private final int numPartitions;              // 总共要写出的分区数
  private final BlockManager blockManager;
  private final Partitioner partitioner;        // 当前RDD的写出分区器
  private final ShuffleWriteMetrics writeMetrics;  // shuffle 写指标统计
  private final int shuffleId;                  // shuffleId
  private final int mapId;                      // 当前操作RDD的读取分区号
  private final Serializer serializer;          //  数据序列化方法
  private final IndexShuffleBlockResolver shuffleBlockResolver;

  /** Array of file writers, one for each partition */
  private DiskBlockObjectWriter[] partitionWriters;      // 分区写工具， 每个分区对应一个分区写文件
  private FileSegment[] partitionWriterSegments;
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;

  /**
   * 我们正在停止过程中吗？
   * 因为map任务可以先调用stop()，成功返回true，然后如果它们遇到异常，可能会再次调用stop()，但这次成功返回false。
   * 因此，我们希望确保我们不会尝试两次删除文件等操作。
   */
  private boolean stopping = false;

  BypassMergeSortShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      BypassMergeSortShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf conf) {

    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();    // 获取当前的ShuffleDependency
    this.mapId = mapId;                                             // 分区号
    this.shuffleId = dep.shuffleId();                               // shuffle ID
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.serializer = dep.serializer();
    this.shuffleBlockResolver = shuffleBlockResolver;
  }

  /***
   *
   * @param records 数据迭代器
   * @throws IOException
   */
  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    if (!records.hasNext()) {
      partitionLengths = new long[numPartitions];
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      return;
    }
    final SerializerInstance serInstance = serializer.newInstance();   // 创建一个序列化实例

    final long openStartTime = System.nanoTime();   // 记录当前时间

    partitionWriters = new DiskBlockObjectWriter[numPartitions];
    partitionWriterSegments = new FileSegment[numPartitions];

    for (int i = 0; i < numPartitions; i++) {

      // 对于每个分区， 创建一个分区文件块
      final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile = blockManager.diskBlockManager().createTempShuffleBlock();

      final File file = tempShuffleBlockIdPlusFile._2();

      final BlockId blockId = tempShuffleBlockIdPlusFile._1();

      // 通过BlockManager 封装了一个磁盘写入工具 [[org.apache.spark.storage.DiskBlockObjectWriter]]
      partitionWriters[i] = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
    }
    // 创建要写入的文件和创建磁盘写入器都涉及与磁盘的交互，当我们打开许多文件时，总体上可能需要很长时间
    // 因此应将其包括在shuffle写入时间中。
    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

    // 遍历迭代数据记录，将记录内容写出
    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final K key = record._1();
      // 找到对应的写文件， 数据写出
      partitionWriters[partitioner.getPartition(key)].write(key, record._2());
    }

    // 将每个文件都提交上
    for (int i = 0; i < numPartitions; i++) {
      final DiskBlockObjectWriter writer = partitionWriters[i];
      partitionWriterSegments[i] = writer.commitAndGet();
      writer.close();
    }

    // 获取一个ShuffleBlock 写出文件
    File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    File tmp = Utils.tempFileWith(output);
    try {
      // 将每个分区文件合并写到目标文件
      partitionLengths = writePartitionedFile(tmp);

      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedFile(File outputFile) throws IOException {
    // Track location of the partition starts in the output file
    final long[] lengths = new long[numPartitions];
    if (partitionWriters == null) {
      // We were passed an empty iterator
      return lengths;
    }

    final FileOutputStream out = new FileOutputStream(outputFile, true);
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    try {
      for (int i = 0; i < numPartitions; i++) {
        final File file = partitionWriterSegments[i].file();
        if (file.exists()) {
          final FileInputStream in = new FileInputStream(file);
          boolean copyThrewException = true;
          try {
            lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
            copyThrewException = false;
          } finally {
            Closeables.close(in, copyThrewException);
          }
          if (!file.delete()) {
            logger.error("Unable to delete file for partition {}", i);
          }
        }
      }
      threwException = false;
    } finally {
      Closeables.close(out, threwException);
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
    partitionWriters = null;
    return lengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              File file = writer.revertPartialWritesAndClose();
              if (!file.delete()) {
                logger.error("Error while deleting file {}", file.getAbsolutePath());
              }
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}
