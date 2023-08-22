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

package org.apache.spark.scheduler

import java.io._
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.{CompressionCodec, LZ4CompressionCodec}
import org.apache.spark.util.{JsonProtocol, JsonProtocolSuite, Utils}

/**
 * Test whether ReplayListenerBus replays events from logs correctly.
 */
class ReplayListenerSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {
  private val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  private var testDir: File = _

  before {
    testDir = Utils.createTempDir()
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Simple replay") {
    val logFilePath = getFilePath(testDir, "events.txt")
    val fstream = fileSystem.create(logFilePath)
    val writer = new PrintWriter(fstream)
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", None,
      125L, "Mickey", None)
    val applicationEnd = SparkListenerApplicationEnd(1000L)
    // scalastyle:off println
    writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationStart))))
    writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationEnd))))
    // scalastyle:on println
    writer.close()

    val conf = EventLoggingListenerSuite.getLoggingConf(logFilePath)
    val logData = fileSystem.open(logFilePath)
    val eventMonster = new EventMonster(conf)
    try {
      val replayer = new ReplayListenerBus()
      replayer.addListener(eventMonster)
      replayer.replay(logData, logFilePath.toString)
    } finally {
      logData.close()
    }
    assert(eventMonster.loggedEvents.size === 2)
    assert(eventMonster.loggedEvents(0) === JsonProtocol.sparkEventToJson(applicationStart))
    assert(eventMonster.loggedEvents(1) === JsonProtocol.sparkEventToJson(applicationEnd))
  }

  /**
   * Test replaying compressed spark history file that internally throws an EOFException.  To
   * avoid sensitivity to the compression specifics the test forces an EOFException to occur
   * while reading bytes from the underlying stream (such as observed in actual history files
   * in some cases) and forces specific failure handling.  This validates correctness in both
   * cases when maybeTruncated is true or false.
   */
  test("Replay compressed inprogress log file succeeding on partial read") {
    val buffered = new ByteArrayOutputStream
    val codec = new LZ4CompressionCodec(new SparkConf())
    val compstream = codec.compressedOutputStream(buffered)
    Utils.tryWithResource(new PrintWriter(compstream)) { writer =>

      val applicationStart = SparkListenerApplicationStart("AppStarts", None,
        125L, "Mickey", None)
      val applicationEnd = SparkListenerApplicationEnd(1000L)

      // scalastyle:off println
      writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationStart))))
      writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationEnd))))
      // scalastyle:on println
    }

    val logFilePath = getFilePath(testDir, "events.lz4.inprogress")
    val bytes = buffered.toByteArray
    Utils.tryWithResource(fileSystem.create(logFilePath)) { fstream =>
      fstream.write(bytes, 0, buffered.size)
    }

    // Read the compressed .inprogress file and verify only first event was parsed.
    val conf = EventLoggingListenerSuite.getLoggingConf(logFilePath)
    val replayer = new ReplayListenerBus()

    val eventMonster = new EventMonster(conf)
    replayer.addListener(eventMonster)

    // Verify the replay returns the events given the input maybe truncated.
    val logData = EventLoggingListener.openEventLog(logFilePath, fileSystem)
    Utils.tryWithResource(new EarlyEOFInputStream(logData, buffered.size - 10)) { failingStream =>
      replayer.replay(failingStream, logFilePath.toString, true)

      assert(eventMonster.loggedEvents.size === 1)
      assert(failingStream.didFail)
    }

    // Verify the replay throws the EOF exception since the input may not be truncated.
    val logData2 = EventLoggingListener.openEventLog(logFilePath, fileSystem)
    Utils.tryWithResource(new EarlyEOFInputStream(logData2, buffered.size - 10)) { failingStream2 =>
      intercept[EOFException] {
        replayer.replay(failingStream2, logFilePath.toString, false)
      }
    }
  }

  test("Replay incompatible event log") {
    val logFilePath = getFilePath(testDir, "incompatible.txt")
    val fstream = fileSystem.create(logFilePath)
    val writer = new PrintWriter(fstream)
    val applicationStart = SparkListenerApplicationStart("Incompatible App", None,
      125L, "UserUsingIncompatibleVersion", None)
    val applicationEnd = SparkListenerApplicationEnd(1000L)
    // scalastyle:off println
    writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationStart))))
    writer.println("""{"Event":"UnrecognizedEventOnlyForTest","Timestamp":1477593059313}""")
    writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationEnd))))
    // scalastyle:on println
    writer.close()

    val conf = EventLoggingListenerSuite.getLoggingConf(logFilePath)
    val logData = fileSystem.open(logFilePath)
    val eventMonster = new EventMonster(conf)
    try {
      val replayer = new ReplayListenerBus()
      replayer.addListener(eventMonster)
      replayer.replay(logData, logFilePath.toString)
    } finally {
      logData.close()
    }
    assert(eventMonster.loggedEvents.size === 2)
    assert(eventMonster.loggedEvents(0) === JsonProtocol.sparkEventToJson(applicationStart))
    assert(eventMonster.loggedEvents(1) === JsonProtocol.sparkEventToJson(applicationEnd))
  }

  // This assumes the correctness of EventLoggingListener
  test("End-to-end replay") {
    testApplicationReplay()
  }

  // This assumes the correctness of EventLoggingListener
  test("End-to-end replay with compression") {
    CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codec =>
      testApplicationReplay(Some(codec))
    }
  }


  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  /**
   * Test end-to-end replaying of events.
   *
   * This test runs a few simple jobs with event logging enabled, and compares each emitted
   * event to the corresponding event replayed from the event logs. This test makes the
   * assumption that the event logging behavior is correct (tested in a separate suite).
   */
  private def testApplicationReplay(codecName: Option[String] = None) {
    val logDir = new File(testDir.getAbsolutePath, "test-replay")
    // Here, it creates `Path` from the URI instead of the absolute path for the explicit file
    // scheme so that the string representation of this `Path` has leading file scheme correctly.
    val logDirPath = new Path(logDir.toURI)
    fileSystem.mkdirs(logDirPath)

    val conf = EventLoggingListenerSuite.getLoggingConf(logDirPath, codecName)
    sc = new SparkContext("local-cluster[2,1,1024]", "Test replay", conf)

    // Run a few jobs
    sc.parallelize(1 to 100, 1).count()
    sc.parallelize(1 to 100, 2).map(i => (i, i)).count()
    sc.parallelize(1 to 100, 3).map(i => (i, i)).groupByKey().count()
    sc.parallelize(1 to 100, 4).map(i => (i, i)).groupByKey().persist().count()
    sc.stop()

    // Prepare information needed for replay
    val applications = fileSystem.listStatus(logDirPath)
    assert(applications != null && applications.size > 0)
    val eventLog = applications.sortBy(_.getModificationTime).last
    assert(!eventLog.isDirectory)

    // Replay events
    val logData = EventLoggingListener.openEventLog(eventLog.getPath(), fileSystem)
    val eventMonster = new EventMonster(conf)
    try {
      val replayer = new ReplayListenerBus()
      replayer.addListener(eventMonster)
      replayer.replay(logData, eventLog.getPath().toString)
    } finally {
      logData.close()
    }

    // Verify the same events are replayed in the same order
    assert(sc.eventLogger.isDefined)
    val originalEvents = sc.eventLogger.get.loggedEvents
    val replayedEvents = eventMonster.loggedEvents
    originalEvents.zip(replayedEvents).foreach { case (e1, e2) =>
      // Don't compare the JSON here because accumulators in StageInfo may be out of order
      JsonProtocolSuite.assertEquals(
        JsonProtocol.sparkEventFromJson(e1), JsonProtocol.sparkEventFromJson(e2))
    }
  }

  private def getFilePath(dir: File, fileName: String): Path = {
    assert(dir.isDirectory)
    val path = new File(dir, fileName).getAbsolutePath
    new Path(path)
  }

  /**
   * A simple listener that buffers all the events it receives.
   *
   * The event buffering functionality must be implemented within EventLoggingListener itself.
   * This is because of the following race condition: the event may be mutated between being
   * processed by one listener and being processed by another. Thus, in order to establish
   * a fair comparison between the original events and the replayed events, both functionalities
   * must be implemented within one listener (i.e. the EventLoggingListener).
   *
   * This child listener inherits only the event buffering functionality, but does not actually
   * log the events.
   */
  private class EventMonster(conf: SparkConf)
    extends EventLoggingListener("test", None, new URI("testdir"), conf) {

    override def start() { }

  }

  /*
   * This is a dummy input stream that wraps another input stream but ends prematurely when
   * reading at the specified position, throwing an EOFExeption.
   */
  private class EarlyEOFInputStream(in: InputStream, failAtPos: Int) extends InputStream {
    private val countDown = new AtomicInteger(failAtPos)

    def didFail: Boolean = countDown.get == 0

    @throws[IOException]
    override def read(): Int = {
      if (countDown.get == 0) {
        throw new EOFException("Stream ended prematurely")
      }
      countDown.decrementAndGet()
      in.read()
    }

    override def close(): Unit = in.close()
  }
}
