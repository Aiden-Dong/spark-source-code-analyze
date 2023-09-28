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

package org.apache.spark.storage

import java.io.{InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/**
 * 这是一个迭代器，用于获取多个数据块。对于本地块，它从本地块管理器中获取。
 * 对于远程块，它使用提供的BlockTransferService进行获取。
 *
 * 这会创建一个由(BlockID，InputStream)Tuple 组成的迭代器，以便调用者可以在接收到数据块时以流水线方式处理它们。
 * 实现中对远程获取进行了调节，以确保不超过maxBytesInFlight，以避免使用过多内存。
 *
 * @param context [[TaskContext]], 用来更新指标
 * @param shuffleClient [[ShuffleClient]] 用来获取远程数据块
 * @param blockManager [[BlockManager]] 用来读取本地块
 * @param blocksByAddress  以[[BlockManagerId]]分组的要获取的块列表。
 *                         对于每个块，我们还需要大小（以字节为单位的长字段）以限制内存使用。
 *                         请注意，已经排除了大小为零的块，这是在[[MapOutputTracker.convertMapStatuses]]中发生的。
 *
 * @param streamWrapper 包装返回的输入流的函数。这允许你在读取输入流之前或之后执行一些操作，例如解压缩数据或者对数据进行其他的处理。.
 * @param maxBytesInFlight (spark.reducer.maxSizeInFlight)
 *                         在任何给定时刻可以获取的远程块的最大大小（以字节为单位）。
 *                         这是为了限制在内存中使用的远程块的大小，以防止内存耗尽。.
 * @param maxReqsInFlight (spark.reducer.maxReqsInFlight)
 *                        在任何给定时刻可以获取的远程块的最大数量。
 *                        这是为了限制同时进行的远程请求的数量，以避免过多的网络请求。
 * @param maxBlocksInFlightPerAddress (spark.reducer.maxBlocksInFlightPerAddress)
 *                                    在给定的(远程主机:端口)上的任何给定时刻可以获取的shuffle块的最大数量。
 *                                    这是为了限制同时从特定远程主机获取的shuffle块的数量，以管理内存和网络资源的使用。
 * @param maxReqSizeShuffleToMem  (spark.maxRemoteBlockSizeFetchToMem)
 *                                可以被Shuffle到内存的请求的最大大小（以字节为单位）。
 *                                这个设置通常用于控制每个Shuffle请求的大小，以便有效地使用内存资源，并防止一次请求占用过多的内存。.
 * @param detectCorrupt (spark.shuffle.detectCorrupt)
 *                      这个设置用于确定是否检测已获取的数据块中是否存在任何损坏。
 *                      当启用时，Spark会对接收的数据块进行校验和验证，以确保数据的完整性。
 *                      如果数据块损坏，Spark将会抛出异常或采取其他适当的操作来处理问题。
 *                      这可以帮助提高数据的可靠性，但可能会增加一些额外的计算开销。.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * 这个参数表示要获取的数据块的总数。通常，它应该等于[[blocksByAddress]]中数据块的总数。
   * 在blocksByAddress中，已经过滤掉了大小为零的数据块，所以[[numBlocksToFetch]]应该反映出要获取的有效数据块的总数。
   *
   * 这个参数应该等于 localBlocks.size + remoteBlocks.size。
   * 这表示要获取的总块数，包括本地块和远程块。
   */
  private[this] var numBlocksToFetch = 0

  /**
   * 这个参数表示调用者已经处理的块数。
   * 当 [[numBlocksProcessed]]==[[numBlocksToFetch]]时，表示迭代器已经耗尽，所有块都已处理。
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  // 本地的数据块结合， 过滤掉大小为零的数据块
  private[this] val localBlocks = scala.collection.mutable.LinkedHashSet[BlockId]()

  // 远程的数据块结合， 过滤掉大小为零的数据块
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * 这是一个用于保存结果的队列。
   * 它将由 [[org.apache.spark.network.BlockTransferService]] 提供的异步模型转换成同步模型（迭代器）。
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * 正在处理的当前[[FetchResult]]。
   * 我们跟踪这个信息，以便在处理当前缓冲区时出现运行时异常时释放当前缓冲区。
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * 要发出的获取请求队列
   * 我们将逐渐从中获取请求，以确保在飞行中的字节数不超过maxBytesInFlight。
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * Queue of fetch requests which could not be issued the first time they were dequeued.
   * 首次出队列列时无法发出的获取请求队列。
   * These requests are tried again when the fetch constraints are satisfied.
   * 当满足获取约束时，将再次尝试这些请求。
   *
   * 这是要发出的获取请求队列
   * 我们将逐渐从中获取请求，以确保在飞行中的字节数不超过maxBytesInFlight。
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  // 我们请求的当前在传输中的字节数
  private[this] var bytesInFlight = 0L

  // 当前正在传输中的请求数量
  private[this] var reqsInFlight = 0

  // 当前每个主机:端口正在传输中的块数量。
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * 这些无法成功解压缩的块，用于确保对这些损坏的块最多重试一次。
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * 这个迭代器是否仍然处于活动状态。
   * 如果 isZombie 为 true，回调接口将不再将获取的块放入 results 中。
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * 用于存储用于Shuffle远程大块的文件的集合。
   * 在清理时，将删除此集合中的文件。这是防止磁盘文件泄漏的一层保护。
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = {
    // we never need to do any encryption or decryption here, regardless of configs, because that
    // is handled at another layer in the code.  When encryption is enabled, shuffle data is written
    // to disk encrypted in the first place, and sent over the network still encrypted.
    new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
  }

  override def registerTempFileToClean(file: DownloadFile): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.path())
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val remainingBlocks = new HashSet[String]() ++= sizeMap.keys
    val blockIds = req.blocks.map(_._1.toString)
    val address = req.address

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            remainingBlocks -= blockId
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
              remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }

    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    if (req.size > maxReqSizeShuffleToMem) {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {

    // 确保远程请求的长度最多为 maxBytesInFlight/5。
    // 保持长度小于maxBytesInFlight的原因是允许从最多5个节点并行获取，而不是在一个节点上读取输出时阻塞。
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)

    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize
      + ", maxBlocksInFlightPerAddress: " + maxBlocksInFlightPerAddress)

    // 将本地块和远程块进行拆分。
    // 进一步将远程块拆分成大小最多为 maxBytesInFlight 的 FetchRequest，以限制在传输中的数据量。
    val remoteRequests = new ArrayBuffer[FetchRequest]

    for ((address, blockInfos) <- blocksByAddress) {
      if (address.executorId == blockManager.blockManagerId.executorId) {

        // 本地数据块
        blockInfos.find(_._2 <= 0) match {
          case Some((blockId, size)) if size < 0 =>
            throw new BlockException(blockId, "Negative block size " + size)
          case Some((blockId, size)) if size == 0 =>
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          case None => // do nothing.
        }
        localBlocks ++= blockInfos.map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {

        // 远程数据块
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          } else if (size == 0) {
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          } else {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          }

          if (curRequestSize >= targetRequestSize ||
              curBlocks.size >= maxBlocksInFlightPerAddress) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            logDebug(s"Creating fetch request of $curRequestSize at $address "
              + s"with ${curBlocks.size} blocks")
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks including ${localBlocks.size}" +
        s" local blocks and ${remoteBlocks.size} remote blocks")
    remoteRequests
  }

  /**
   * 在获取远程块的同时获取本地块。
   * 这是可以的，因为在创建输入流时，ManagedBuffer的内存是懒加载分配的，因此我们在内存中跟踪的只是ManagedBuffer的引用本身。
   */
  private[this] def fetchLocalBlocks() {
    logDebug(s"Start fetching local blocks: ${localBlocks.mkString(", ")}")
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId,  buf.size(), buf, false))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener[Unit](_ => cleanup())

    // Split local and remote blocks.
    // remoteRequests 包含请求的远程数据块集合
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // 这个方法用于发送初始的数据块请求，最多可以发送maxBytesInFlight字节的请求数据。
    // 这是一个流式传输的操作，它会持续发送请求直到达到指定的字节限制。
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * 获取下一个（BlockId，InputStream）。
   *
   * 如果任务失败，每个InputStream下面的ManagedBuffers将由注册在TaskCompletionListener中的cleanup()方法释放。
   * 然而，调用方应该在不再需要这些InputStreams时尽快关闭()它们，以便尽早释放内存。
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // 取出下一个获取的结果并尝试解压缩以检测数据是否损坏，如果数据损坏，则再次获取一次。
    // 如果第二次获取仍然损坏，则抛出FailureFetchResult异常，以便可以重试前一阶段。
    // 对于本地的 Shuffle 块，第一次出现 IOException 就抛出 FailureFetchResult 异常。
    // 这段代码的作用是确保获取的数据是完整和正确的，如果出现问题，可以进行适当的处理和重试。
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      result = results.take()  // 取出一个数据来
      val stopFetchWait = System.currentTimeMillis()
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
        case r @ SuccessFetchResult(blockId, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          if (!localBlocks.contains(blockId)) {
            bytesInFlight -= size
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          if (buf.size == 0) {
            // We will never legitimately receive a zero-size block. All blocks with zero records
            // have zero size and all zero-size blocks have no records (and hence should never
            // have been requested in the first place). This statement relies on behaviors of the
            // shuffle writers, which are guaranteed by the following test cases:
            //
            // - BypassMergeSortShuffleWriterSuite: "write with some empty partitions"
            // - UnsafeShuffleWriterSuite: "writeEmptyIterator"
            // - DiskBlockObjectWriterSuite: "commit() and close() without ever opening or writing"
            //
            // There is not an explicit test for SortShuffleWriter but the underlying APIs that
            // uses are shared by the UnsafeShuffleWriter (both writers use DiskBlockObjectWriter
            // which returns a zero-size from commitAndGet() in case no records were written
            // since the last call.
            val msg = s"Received a zero-size buffer for block $blockId from $address " +
              s"(expectedApproxSize = $size, isNetworkReqDone=$isNetworkReqDone)"
            throwFetchFailedException(blockId, address, new IOException(msg))
          }

          val in = try {
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              logError("Failed to create input stream from local block", e)
              buf.release()
              throwFetchFailedException(blockId, address, e)
          }
          var isStreamCopied: Boolean = false
          try {
            input = streamWrapper(blockId, in)
            // Only copy the stream if it's wrapped by compression or encryption, also the size of
            // block is small (the decompressed block is smaller than maxBytesInFlight)
            if (detectCorrupt && !input.eq(in) && size < maxBytesInFlight / 3) {
              isStreamCopied = true
              val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage tne potential increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              Utils.copyStream(input, out, closeStreams = true)
              input = out.toChunkedByteBuffer.toInputStream(dispose = true)
            }
          } catch {
            case e: IOException =>
              buf.release()
              if (buf.isInstanceOf[FileSegmentManagedBuffer] || corruptedBlocks.contains(blockId)) {
                throwFetchFailedException(blockId, address, e)
              } else {
                logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                corruptedBlocks += blockId
                fetchRequests += FetchRequest(address, Array((blockId, size)))
                result = null
              }
          } finally {
            // TODO: release the buf here to free memory earlier
            if (isStreamCopied) {
              in.close()
            }
          }

        case FailureFetchResult(blockId, address, e) =>
          throwFetchFailedException(blockId, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId, new BufferReleasingInputStream(input, this))
  }

  private def fetchUpToMaxBytes(): Unit = {
    // 发送最多 maxBytesInFlight 的获取请求.
    // 如果您无法立即从远程主机获取数据，请推迟该请求，直到下次可以处理该请求。

    // 如果可能，处理任何未完成的延迟获取请求。
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // 如果可能的话，处理任何常规的获取请求.
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        send(remoteAddress, request)
      }
    }

    // 向远程请求发送数据
    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      sendRequest(request)
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }

    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   * @param buf `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}
