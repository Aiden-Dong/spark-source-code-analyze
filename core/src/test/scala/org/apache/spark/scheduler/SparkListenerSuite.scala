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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.Semaphore

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.mockito.Mockito
import org.scalatest.Matchers

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config._
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.{ResetSystemProperties, RpcUtils}

class SparkListenerSuite extends SparkFunSuite with LocalSparkContext with Matchers
  with ResetSystemProperties {

  import LiveListenerBus._

  /** Length of time to wait while draining listener events. */
  val WAIT_TIMEOUT_MILLIS = 10000

  val jobCompletionTime = 1421191296660L

  private val mockSparkContext: SparkContext = Mockito.mock(classOf[SparkContext])
  private val mockMetricsSystem: MetricsSystem = Mockito.mock(classOf[MetricsSystem])

  private def numDroppedEvents(bus: LiveListenerBus): Long = {
    bus.metrics.metricRegistry.counter(s"queue.$SHARED_QUEUE.numDroppedEvents").getCount
  }

  private def sharedQueueSize(bus: LiveListenerBus): Int = {
    bus.metrics.metricRegistry.getGauges().get(s"queue.$SHARED_QUEUE.size").getValue()
      .asInstanceOf[Int]
  }

  private def eventProcessingTimeCount(bus: LiveListenerBus): Long = {
    bus.metrics.metricRegistry.timer(s"queue.$SHARED_QUEUE.listenerProcessingTime").getCount()
  }

  test("don't call sc.stop in listener") {
    sc = new SparkContext("local", "SparkListenerSuite", new SparkConf())
    val listener = new SparkContextStoppingListener(sc)

    sc.listenerBus.addToSharedQueue(listener)
    sc.listenerBus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    sc.stop()

    assert(listener.sparkExSeen)
  }

  test("basic creation and shutdown of LiveListenerBus") {
    val conf = new SparkConf()
    val counter = new BasicJobCounter
    val bus = new LiveListenerBus(conf)

    // Metrics are initially empty.
    assert(bus.metrics.numEventsPosted.getCount === 0)
    assert(numDroppedEvents(bus) === 0)
    assert(bus.queuedEvents.size === 0)
    assert(eventProcessingTimeCount(bus) === 0)

    // Post five events:
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded)) }

    // Five messages should be marked as received and queued, but no messages should be posted to
    // listeners yet because the the listener bus hasn't been started.
    assert(bus.metrics.numEventsPosted.getCount === 5)
    assert(bus.queuedEvents.size === 5)

    // Add the counter to the bus after messages have been queued for later delivery.
    bus.addToSharedQueue(counter)
    assert(counter.count === 0)

    // Starting listener bus should flush all buffered events
    bus.start(mockSparkContext, mockMetricsSystem)
    Mockito.verify(mockMetricsSystem).registerSource(bus.metrics)
    bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(counter.count === 5)
    assert(sharedQueueSize(bus) === 0)
    assert(eventProcessingTimeCount(bus) === 5)

    // After the bus is started, there should be no more queued events.
    assert(bus.queuedEvents === null)

    // After listener bus has stopped, posting events should not increment counter
    bus.stop()
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded)) }
    assert(counter.count === 5)
    assert(eventProcessingTimeCount(bus) === 5)

    // Listener bus must not be started twice
    intercept[IllegalStateException] {
      val bus = new LiveListenerBus(conf)
      bus.start(mockSparkContext, mockMetricsSystem)
      bus.start(mockSparkContext, mockMetricsSystem)
    }

    // ... or stopped before starting
    intercept[IllegalStateException] {
      val bus = new LiveListenerBus(conf)
      bus.stop()
    }
  }

  test("bus.stop() waits for the event queue to completely drain") {
    @volatile var drained = false

    // When Listener has started
    val listenerStarted = new Semaphore(0)

    // Tells the listener to stop blocking
    val listenerWait = new Semaphore(0)

    // When stopper has started
    val stopperStarted = new Semaphore(0)

    // When stopper has returned
    val stopperReturned = new Semaphore(0)

    class BlockingListener extends SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        listenerStarted.release()
        listenerWait.acquire()
        drained = true
      }
    }
    val bus = new LiveListenerBus(new SparkConf())
    val blockingListener = new BlockingListener

    bus.addToSharedQueue(blockingListener)
    bus.start(mockSparkContext, mockMetricsSystem)
    bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded))

    listenerStarted.acquire()
    // Listener should be blocked after start
    assert(!drained)

    new Thread("ListenerBusStopper") {
      override def run() {
        stopperStarted.release()
        // stop() will block until notify() is called below
        bus.stop()
        stopperReturned.release()
      }
    }.start()

    stopperStarted.acquire()
    // Listener should remain blocked after stopper started
    assert(!drained)

    // unblock Listener to let queue drain
    listenerWait.release()
    stopperReturned.acquire()
    assert(drained)
  }

  test("metrics for dropped listener events") {
    val bus = new LiveListenerBus(new SparkConf().set(LISTENER_BUS_EVENT_QUEUE_CAPACITY, 1))

    val listenerStarted = new Semaphore(0)
    val listenerWait = new Semaphore(0)

    bus.addToSharedQueue(new SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        listenerStarted.release()
        listenerWait.acquire()
      }
    })

    bus.start(mockSparkContext, mockMetricsSystem)

    // Post a message to the listener bus and wait for processing to begin:
    bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded))
    listenerStarted.acquire()
    assert(sharedQueueSize(bus) === 0)
    assert(numDroppedEvents(bus) === 0)

    // If we post an additional message then it should remain in the queue because the listener is
    // busy processing the first event:
    bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded))
    assert(sharedQueueSize(bus) === 1)
    assert(numDroppedEvents(bus) === 0)

    // The queue is now full, so any additional events posted to the listener will be dropped:
    bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded))
    assert(sharedQueueSize(bus) === 1)
    assert(numDroppedEvents(bus) === 1)

    // Allow the the remaining events to be processed so we can stop the listener bus:
    listenerWait.release(2)
    bus.stop()
  }

  test("basic creation of StageInfo") {
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.map(_.toString)
    rdd2.setName("Target RDD")
    rdd2.count()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    listener.stageInfos.size should be {1}
    val (stageInfo, taskInfoMetrics) = listener.stageInfos.head
    stageInfo.rddInfos.size should be {2}
    stageInfo.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo.rddInfos.exists(_.name == "Target RDD") should be {true}
    stageInfo.numTasks should be {4}
    stageInfo.submissionTime should be ('defined)
    stageInfo.completionTime should be ('defined)
    taskInfoMetrics.length should be {4}
  }

  test("basic creation of StageInfo with shuffle") {
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.filter(_ % 2 == 0).map(i => (i, i))
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd1.setName("Un")
    rdd2.setName("Deux")
    rdd3.setName("Trois")

    rdd1.count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be {1}
    val stageInfo1 = listener.stageInfos.keys.find(_.stageId == 0).get
    stageInfo1.rddInfos.size should be {1} // ParallelCollectionRDD
    stageInfo1.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo1.rddInfos.exists(_.name == "Un") should be {true}
    listener.stageInfos.clear()

    rdd2.count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be {1}
    val stageInfo2 = listener.stageInfos.keys.find(_.stageId == 1).get
    stageInfo2.rddInfos.size should be {3}
    stageInfo2.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo2.rddInfos.exists(_.name == "Deux") should be {true}
    listener.stageInfos.clear()

    rdd3.count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be {2} // Shuffle map stage + result stage
    val stageInfo3 = listener.stageInfos.keys.find(_.stageId == 3).get
    stageInfo3.rddInfos.size should be {1} // ShuffledRDD
    stageInfo3.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo3.rddInfos.exists(_.name == "Trois") should be {true}
  }

  test("StageInfo with fewer tasks than partitions") {
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.map(_.toString)
    sc.runJob(rdd2, (items: Iterator[String]) => items.size, Seq(0, 1))

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    listener.stageInfos.size should be {1}
    val (stageInfo, _) = listener.stageInfos.head
    stageInfo.numTasks should be {2}
    stageInfo.rddInfos.size should be {2}
    stageInfo.rddInfos.forall(_.numPartitions == 4) should be {true}
  }

  test("local metrics") {
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    sc.addSparkListener(new StatsReportListener)
    // just to make sure some of the tasks and their deserialization take a noticeable
    // amount of time
    val slowDeserializable = new SlowDeserializable
    val w = { i: Int =>
      if (i == 0) {
        Thread.sleep(100)
        slowDeserializable.use()
      }
      i
    }

    val numSlices = 16
    val d = sc.parallelize(0 to 10000, numSlices).map(w)
    d.count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be (1)

    val d2 = d.map { i => w(i) -> i * 2 }.setName("shuffle input 1")
    val d3 = d.map { i => w(i) -> (0 to (i % 5)) }.setName("shuffle input 2")
    val d4 = d2.cogroup(d3, numSlices).map { case (k, (v1, v2)) =>
      (w(k), (v1.size, v2.size))
    }
    d4.setName("A Cogroup")
    d4.collectAsMap()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be (4)
    listener.stageInfos.foreach { case (stageInfo, taskInfoMetrics) =>
      /**
       * Small test, so some tasks might take less than 1 millisecond, but average should be greater
       * than 0 ms.
       */
      checkNonZeroAvg(
        taskInfoMetrics.map(_._2.executorRunTime),
        stageInfo + " executorRunTime")
      checkNonZeroAvg(
        taskInfoMetrics.map(_._2.executorDeserializeTime),
        stageInfo + " executorDeserializeTime")

      /* Test is disabled (SEE SPARK-2208)
      if (stageInfo.rddInfos.exists(_.name == d4.name)) {
        checkNonZeroAvg(
          taskInfoMetrics.map(_._2.shuffleReadMetrics.get.fetchWaitTime),
          stageInfo + " fetchWaitTime")
      }
      */

      taskInfoMetrics.foreach { case (taskInfo, taskMetrics) =>
        taskMetrics.resultSize should be > (0L)
        if (stageInfo.rddInfos.exists(info => info.name == d2.name || info.name == d3.name)) {
          assert(taskMetrics.shuffleWriteMetrics.bytesWritten > 0L)
        }
        if (stageInfo.rddInfos.exists(_.name == d4.name)) {
          assert(taskMetrics.shuffleReadMetrics.totalBlocksFetched == 2 * numSlices)
          assert(taskMetrics.shuffleReadMetrics.localBlocksFetched == 2 * numSlices)
          assert(taskMetrics.shuffleReadMetrics.remoteBlocksFetched == 0)
          assert(taskMetrics.shuffleReadMetrics.remoteBytesRead == 0L)
        }
      }
    }
  }

  test("onTaskGettingResult() called when result fetched remotely") {
    val conf = new SparkConf().set("spark.rpc.message.maxSize", "1")
    sc = new SparkContext("local", "SparkListenerSuite", conf)
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    // Make a task whose result is larger than the RPC message size
    val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
    assert(maxRpcMessageSize === 1024 * 1024)
    val result = sc.parallelize(Seq(1), 1)
      .map { x => 1.to(maxRpcMessageSize).toArray }
      .reduce { case (x, y) => x }
    assert(result === 1.to(maxRpcMessageSize).toArray)

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    val TASK_INDEX = 0
    assert(listener.startedTasks.contains(TASK_INDEX))
    assert(listener.startedGettingResultTasks.contains(TASK_INDEX))
    assert(listener.endedTasks.contains(TASK_INDEX))
  }

  test("onTaskGettingResult() not called when result sent directly") {
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    // Make a task whose result is larger than the RPC message size
    val result = sc.parallelize(Seq(1), 1).map(2 * _).reduce { case (x, y) => x }
    assert(result === 2)

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    val TASK_INDEX = 0
    assert(listener.startedTasks.contains(TASK_INDEX))
    assert(listener.startedGettingResultTasks.isEmpty)
    assert(listener.endedTasks.contains(TASK_INDEX))
  }

  test("onTaskEnd() should be called for all started tasks, even after job has been killed") {
    sc = new SparkContext("local", "SparkListenerSuite")
    val WAIT_TIMEOUT_MILLIS = 10000
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    val numTasks = 10
    val f = sc.parallelize(1 to 10000, numTasks).map { i => Thread.sleep(10); i }.countAsync()
    // Wait until one task has started (because we want to make sure that any tasks that are started
    // have corresponding end events sent to the listener).
    var finishTime = System.currentTimeMillis + WAIT_TIMEOUT_MILLIS
    listener.synchronized {
      var remainingWait = finishTime - System.currentTimeMillis
      while (listener.startedTasks.isEmpty && remainingWait > 0) {
        listener.wait(remainingWait)
        remainingWait = finishTime - System.currentTimeMillis
      }
      assert(!listener.startedTasks.isEmpty)
    }

    f.cancel()

    // Ensure that onTaskEnd is called for all started tasks.
    finishTime = System.currentTimeMillis + WAIT_TIMEOUT_MILLIS
    listener.synchronized {
      var remainingWait = finishTime - System.currentTimeMillis
      while (listener.endedTasks.size < listener.startedTasks.size && remainingWait > 0) {
        listener.wait(finishTime - System.currentTimeMillis)
        remainingWait = finishTime - System.currentTimeMillis
      }
      assert(listener.endedTasks.size === listener.startedTasks.size)
    }
  }

  test("SparkListener moves on if a listener throws an exception") {
    val badListener = new BadListener
    val jobCounter1 = new BasicJobCounter
    val jobCounter2 = new BasicJobCounter
    val bus = new LiveListenerBus(new SparkConf())

    // Propagate events to bad listener first
    bus.addToSharedQueue(badListener)
    bus.addToSharedQueue(jobCounter1)
    bus.addToSharedQueue(jobCounter2)
    bus.start(mockSparkContext, mockMetricsSystem)

    // Post events to all listeners, and wait until the queue is drained
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded)) }
    bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    // The exception should be caught, and the event should be propagated to other listeners
    assert(jobCounter1.count === 5)
    assert(jobCounter2.count === 5)
  }

  test("registering listeners via spark.extraListeners") {
    val listeners = Seq(
      classOf[ListenerThatAcceptsSparkConf],
      classOf[FirehoseListenerThatAcceptsSparkConf],
      classOf[BasicJobCounter])
    val conf = new SparkConf().setMaster("local").setAppName("test")
      .set(EXTRA_LISTENERS, listeners.map(_.getName))
    sc = new SparkContext(conf)
    sc.listenerBus.listeners.asScala.count(_.isInstanceOf[BasicJobCounter]) should be (1)
    sc.listenerBus.listeners.asScala
      .count(_.isInstanceOf[ListenerThatAcceptsSparkConf]) should be (1)
    sc.listenerBus.listeners.asScala
      .count(_.isInstanceOf[FirehoseListenerThatAcceptsSparkConf]) should be (1)
  }

  test("add and remove listeners to/from LiveListenerBus queues") {
    val bus = new LiveListenerBus(new SparkConf(false))
    val counter1 = new BasicJobCounter()
    val counter2 = new BasicJobCounter()
    val counter3 = new BasicJobCounter()

    bus.addToSharedQueue(counter1)
    bus.addToStatusQueue(counter2)
    bus.addToStatusQueue(counter3)
    assert(bus.activeQueues() === Set(SHARED_QUEUE, APP_STATUS_QUEUE))
    assert(bus.findListenersByClass[BasicJobCounter]().size === 3)

    bus.removeListener(counter1)
    assert(bus.activeQueues() === Set(APP_STATUS_QUEUE))
    assert(bus.findListenersByClass[BasicJobCounter]().size === 2)

    bus.removeListener(counter2)
    assert(bus.activeQueues() === Set(APP_STATUS_QUEUE))
    assert(bus.findListenersByClass[BasicJobCounter]().size === 1)

    bus.removeListener(counter3)
    assert(bus.activeQueues().isEmpty)
    assert(bus.findListenersByClass[BasicJobCounter]().isEmpty)
  }

  Seq(true, false).foreach { throwInterruptedException =>
    val suffix = if (throwInterruptedException) "throw interrupt" else "set Thread interrupted"
    test(s"interrupt within listener is handled correctly: $suffix") {
      val conf = new SparkConf(false)
        .set(LISTENER_BUS_EVENT_QUEUE_CAPACITY, 5)
      val bus = new LiveListenerBus(conf)
      val counter1 = new BasicJobCounter()
      val counter2 = new BasicJobCounter()
      val interruptingListener1 = new InterruptingListener(throwInterruptedException)
      val interruptingListener2 = new InterruptingListener(throwInterruptedException)
      bus.addToSharedQueue(counter1)
      bus.addToSharedQueue(interruptingListener1)
      bus.addToStatusQueue(counter2)
      bus.addToEventLogQueue(interruptingListener2)
      assert(bus.activeQueues() === Set(SHARED_QUEUE, APP_STATUS_QUEUE, EVENT_LOG_QUEUE))
      assert(bus.findListenersByClass[BasicJobCounter]().size === 2)
      assert(bus.findListenersByClass[InterruptingListener]().size === 2)

      bus.start(mockSparkContext, mockMetricsSystem)

      // after we post one event, both interrupting listeners should get removed, and the
      // event log queue should be removed
      bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded))
      bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
      assert(bus.activeQueues() === Set(SHARED_QUEUE, APP_STATUS_QUEUE))
      assert(bus.findListenersByClass[BasicJobCounter]().size === 2)
      assert(bus.findListenersByClass[InterruptingListener]().size === 0)
      assert(counter1.count === 1)
      assert(counter2.count === 1)

      // posting more events should be fine, they'll just get processed from the OK queue.
      (0 until 5).foreach { _ => bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded)) }
      bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
      assert(counter1.count === 6)
      assert(counter2.count === 6)

      // Make sure stopping works -- this requires putting a poison pill in all active queues, which
      // would fail if our interrupted queue was still active, as its queue would be full.
      bus.stop()
    }
  }

  /**
   * Assert that the given list of numbers has an average that is greater than zero.
   */
  private def checkNonZeroAvg(m: Traversable[Long], msg: String) {
    assert(m.sum / m.size.toDouble > 0.0, msg)
  }

  /**
   * A simple listener that saves all task infos and task metrics.
   */
  private class SaveStageAndTaskInfo extends SparkListener {
    val stageInfos = mutable.Map[StageInfo, Seq[(TaskInfo, TaskMetrics)]]()
    var taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()

    override def onTaskEnd(task: SparkListenerTaskEnd) {
      val info = task.taskInfo
      val metrics = task.taskMetrics
      if (info != null && metrics != null) {
        taskInfoMetrics += ((info, metrics))
      }
    }

    override def onStageCompleted(stage: SparkListenerStageCompleted) {
      stageInfos(stage.stageInfo) = taskInfoMetrics
      taskInfoMetrics = mutable.Buffer.empty
    }
  }

  /**
   * A simple listener that saves the task indices for all task events.
   */
  private class SaveTaskEvents extends SparkListener {
    val startedTasks = new mutable.HashSet[Int]()
    val startedGettingResultTasks = new mutable.HashSet[Int]()
    val endedTasks = new mutable.HashSet[Int]()

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
      startedTasks += taskStart.taskInfo.index
      notify()
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      endedTasks += taskEnd.taskInfo.index
      notify()
    }

    override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
      startedGettingResultTasks += taskGettingResult.taskInfo.index
    }
  }

  /**
   * A simple listener that throws an exception on job end.
   */
  private class BadListener extends SparkListener {
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { throw new Exception }
  }

  /**
   * A simple listener that interrupts on job end.
   */
  private class InterruptingListener(val throwInterruptedException: Boolean) extends SparkListener {
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      if (throwInterruptedException) {
        throw new InterruptedException("got interrupted")
      } else {
        Thread.currentThread().interrupt()
      }
    }
  }
}

// These classes can't be declared inside of the SparkListenerSuite class because we don't want
// their constructors to contain references to SparkListenerSuite:

/**
 * A simple listener that counts the number of jobs observed.
 */
private class BasicJobCounter extends SparkListener {
  var count = 0
  override def onJobEnd(job: SparkListenerJobEnd): Unit = count += 1
}

/**
 * A simple listener that tries to stop SparkContext.
 */
private class SparkContextStoppingListener(val sc: SparkContext) extends SparkListener {
  @volatile var sparkExSeen = false
  override def onJobEnd(job: SparkListenerJobEnd): Unit = {
    try {
      sc.stop()
    } catch {
      case se: SparkException =>
        sparkExSeen = true
    }
  }
}

private class ListenerThatAcceptsSparkConf(conf: SparkConf) extends SparkListener {
  var count = 0
  override def onJobEnd(job: SparkListenerJobEnd): Unit = count += 1
}

private class FirehoseListenerThatAcceptsSparkConf(conf: SparkConf) extends SparkFirehoseListener {
  var count = 0
  override def onEvent(event: SparkListenerEvent): Unit = event match {
    case job: SparkListenerJobEnd => count += 1
    case _ =>
  }
}

private class SlowDeserializable extends Externalizable {

  override def writeExternal(out: ObjectOutput): Unit = { }

  override def readExternal(in: ObjectInput): Unit = Thread.sleep(1)

  def use(): Unit = { }
}
