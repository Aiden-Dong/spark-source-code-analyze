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

package org.apache.spark.sql.execution.streaming.continuous

import java.io.{BufferedReader, InputStreamReader, IOException}
import java.net.Socket
import java.sql.Timestamp
import java.util.{Calendar, List => JList}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.json4s.{DefaultFormats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{ContinuousRecordEndpoint, ContinuousRecordPartitionOffset, GetRecord}
import org.apache.spark.sql.execution.streaming.sources.TextSocketReader
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputPartitionReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.RpcUtils


/**
 * A ContinuousReader that reads text lines through a TCP socket, designed only for tutorials and
 * debugging. This ContinuousReader will *not* work in production applications due to multiple
 * reasons, including no support for fault recovery.
 *
 * The driver maintains a socket connection to the host-port, keeps the received messages in
 * buckets and serves the messages to the executors via a RPC endpoint.
 */
class TextSocketContinuousReader(options: DataSourceOptions) extends ContinuousReader with Logging {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  private val host: String = options.get("host").get()
  private val port: Int = options.get("port").get().toInt

  assert(SparkSession.getActiveSession.isDefined)
  private val spark = SparkSession.getActiveSession.get
  private val numPartitions = spark.sparkContext.defaultParallelism

  @GuardedBy("this")
  private var socket: Socket = _

  @GuardedBy("this")
  private var readThread: Thread = _

  @GuardedBy("this")
  private val buckets = Seq.fill(numPartitions)(new ListBuffer[(String, Timestamp)])

  @GuardedBy("this")
  private var currentOffset: Int = -1

  private var startOffset: TextSocketOffset = _

  private val recordEndpoint = new ContinuousRecordEndpoint(buckets, this)
  @volatile private var endpointRef: RpcEndpointRef = _

  initialize()

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    assert(offsets.length == numPartitions)
    val offs = offsets
      .map(_.asInstanceOf[ContinuousRecordPartitionOffset])
      .sortBy(_.partitionId)
      .map(_.offset)
      .toList
    TextSocketOffset(offs)
  }

  override def deserializeOffset(json: String): Offset = {
    TextSocketOffset(Serialization.read[List[Int]](json))
  }

  override def setStartOffset(offset: java.util.Optional[Offset]): Unit = {
    this.startOffset = offset
      .orElse(TextSocketOffset(List.fill(numPartitions)(0)))
      .asInstanceOf[TextSocketOffset]
    recordEndpoint.setStartOffsets(startOffset.offsets)
  }

  override def getStartOffset: Offset = startOffset

  override def readSchema(): StructType = {
    if (includeTimestamp) {
      TextSocketReader.SCHEMA_TIMESTAMP
    } else {
      TextSocketReader.SCHEMA_REGULAR
    }
  }

  override def planInputPartitions(): JList[InputPartition[InternalRow]] = {

    val endpointName = s"TextSocketContinuousReaderEndpoint-${java.util.UUID.randomUUID()}"
    endpointRef = recordEndpoint.rpcEnv.setupEndpoint(endpointName, recordEndpoint)

    val offsets = startOffset match {
      case off: TextSocketOffset => off.offsets
      case off =>
        throw new IllegalArgumentException(
          s"invalid offset type ${off.getClass} for TextSocketContinuousReader")
    }

    if (offsets.size != numPartitions) {
      throw new IllegalArgumentException(
        s"The previous run contained ${offsets.size} partitions, but" +
          s" $numPartitions partitions are currently configured. The numPartitions option" +
          " cannot be changed.")
    }

    startOffset.offsets.zipWithIndex.map {
      case (offset, i) =>
        TextSocketContinuousInputPartition(
          endpointName, i, offset, includeTimestamp): InputPartition[InternalRow]
    }.asJava

  }

  override def commit(end: Offset): Unit = synchronized {
    val endOffset = end match {
      case off: TextSocketOffset => off
      case _ => throw new IllegalArgumentException(s"TextSocketContinuousReader.commit()" +
        s"received an offset ($end) that did not originate with an instance of this class")
    }

    endOffset.offsets.zipWithIndex.foreach {
      case (offset, partition) =>
        val max = startOffset.offsets(partition) + buckets(partition).size
        if (offset > max) {
          throw new IllegalStateException("Invalid offset " + offset + " to commit" +
          " for partition " + partition + ". Max valid offset: " + max)
        }
        val n = offset - startOffset.offsets(partition)
        buckets(partition).trimStart(n)
    }
    startOffset = endOffset
    recordEndpoint.setStartOffsets(startOffset.offsets)
  }

  /** Stop this source. */
  override def stop(): Unit = synchronized {
    if (socket != null) {
      try {
        // Unfortunately, BufferedReader.readLine() cannot be interrupted, so the only way to
        // stop the readThread is to close the socket.
        socket.close()
      } catch {
        case e: IOException =>
      }
      socket = null
    }
    if (endpointRef != null) recordEndpoint.rpcEnv.stop(endpointRef)
  }

  private def initialize(): Unit = synchronized {
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    // Thread continuously reads from a socket and inserts data into buckets
    readThread = new Thread(s"TextSocketContinuousReader($host, $port)") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          while (true) {
            val line = reader.readLine()
            if (line == null) {
              // End of file reached
              logWarning(s"Stream closed by $host:$port")
              return
            }
            TextSocketContinuousReader.this.synchronized {
              currentOffset += 1
              val newData = (line,
                Timestamp.valueOf(
                  TextSocketReader.DATE_FORMAT.format(Calendar.getInstance().getTime()))
              )
              buckets(currentOffset % numPartitions) += newData
            }
          }
        } catch {
          case e: IOException =>
        }
      }
    }

    readThread.start()
  }

  override def toString: String = s"TextSocketContinuousReader[host: $host, port: $port]"

  private def includeTimestamp: Boolean = options.getBoolean("includeTimestamp", false)

}

/**
 * Continuous text socket input partition.
 */
case class TextSocketContinuousInputPartition(
    driverEndpointName: String,
    partitionId: Int,
    startOffset: Int,
    includeTimestamp: Boolean)
extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new TextSocketContinuousInputPartitionReader(driverEndpointName, partitionId, startOffset,
      includeTimestamp)
}

/**
 * Continuous text socket input partition reader.
 *
 * Polls the driver endpoint for new records.
 */
class TextSocketContinuousInputPartitionReader(
    driverEndpointName: String,
    partitionId: Int,
    startOffset: Int,
    includeTimestamp: Boolean)
  extends ContinuousInputPartitionReader[InternalRow] {

  private val endpoint = RpcUtils.makeDriverRef(
    driverEndpointName,
    SparkEnv.get.conf,
    SparkEnv.get.rpcEnv)

  private var currentOffset = startOffset
  private var current: Option[InternalRow] = None

  override def next(): Boolean = {
    try {
      current = getRecord
      while (current.isEmpty) {
        Thread.sleep(100)
        current = getRecord
      }
      currentOffset += 1
    } catch {
      case _: InterruptedException =>
        // Someone's trying to end the task; just let them.
        return false
    }
    true
  }

  override def get(): InternalRow = {
    current.get
  }

  override def close(): Unit = {}

  override def getOffset: PartitionOffset =
    ContinuousRecordPartitionOffset(partitionId, currentOffset)

  private def getRecord: Option[InternalRow] =
    endpoint.askSync[Option[InternalRow]](GetRecord(
      ContinuousRecordPartitionOffset(partitionId, currentOffset))).map(rec =>
      if (includeTimestamp) {
        rec
      } else {
        InternalRow(rec.get(0, TextSocketReader.SCHEMA_TIMESTAMP)
          .asInstanceOf[(String, Timestamp)]._1)
      }
    )
}

case class TextSocketOffset(offsets: List[Int]) extends Offset {
  private implicit val formats = Serialization.formats(NoTypeHints)
  override def json: String = Serialization.write(offsets)
}
