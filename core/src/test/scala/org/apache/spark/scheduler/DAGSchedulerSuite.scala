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

import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.annotation.meta.param
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.config
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.shuffle.{FetchFailedException, MetadataFetchFailedException}
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockManagerMaster}
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, CallSite, LongAccumulator, Utils}

class DAGSchedulerEventProcessLoopTester(dagScheduler: DAGScheduler)
  extends DAGSchedulerEventProcessLoop(dagScheduler) {

  override def post(event: DAGSchedulerEvent): Unit = {
    try {
      // Forward event to `onReceive` directly to avoid processing event asynchronously.
      onReceive(event)
    } catch {
      case NonFatal(e) => onError(e)
    }
  }

  override def onError(e: Throwable): Unit = {
    logError("Error in DAGSchedulerEventLoop: ", e)
    dagScheduler.stop()
    throw e
  }

}

class MyCheckpointRDD(
    sc: SparkContext,
    numPartitions: Int,
    dependencies: List[Dependency[_]],
    locations: Seq[Seq[String]] = Nil,
    @(transient @param) tracker: MapOutputTrackerMaster = null,
    indeterminate: Boolean = false)
  extends MyRDD(sc, numPartitions, dependencies, locations, tracker, indeterminate) {

  // Allow doCheckpoint() on this RDD.
  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    Iterator.empty
}

/**
 * An RDD for passing to DAGScheduler. These RDDs will use the dependencies and
 * preferredLocations (if any) that are passed to them. They are deliberately not executable
 * so we can test that DAGScheduler does not try to execute RDDs locally.
 *
 * Optionally, one can pass in a list of locations to use as preferred locations for each task,
 * and a MapOutputTrackerMaster to enable reduce task locality. We pass the tracker separately
 * because, in this test suite, it won't be the same as sc.env.mapOutputTracker.
 */
class MyRDD(
    sc: SparkContext,
    numPartitions: Int,
    dependencies: List[Dependency[_]],
    locations: Seq[Seq[String]] = Nil,
    @(transient @param) tracker: MapOutputTrackerMaster = null,
    indeterminate: Boolean = false)
  extends RDD[(Int, Int)](sc, dependencies) with Serializable {

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")

  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i
  }).toArray

  override protected def getOutputDeterministicLevel = {
    if (indeterminate) DeterministicLevel.INDETERMINATE else super.getOutputDeterministicLevel
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    if (locations.isDefinedAt(partition.index)) {
      locations(partition.index)
    } else if (tracker != null && dependencies.size == 1 &&
        dependencies(0).isInstanceOf[ShuffleDependency[_, _, _]]) {
      // If we have only one shuffle dependency, use the same code path as ShuffledRDD for locality
      val dep = dependencies(0).asInstanceOf[ShuffleDependency[_, _, _]]
      tracker.getPreferredLocationsForShuffle(dep, partition.index)
    } else {
      Nil
    }
  }

  override def toString: String = "DAGSchedulerSuiteRDD " + id
}

class DAGSchedulerSuiteDummyException extends Exception

class DAGSchedulerSuite extends SparkFunSuite with LocalSparkContext with TimeLimits {

  import DAGSchedulerSuite._

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  val conf = new SparkConf
  /** Set of TaskSets the DAGScheduler has requested executed. */
  val taskSets = scala.collection.mutable.Buffer[TaskSet]()

  /** Stages for which the DAGScheduler has called TaskScheduler.cancelTasks(). */
  val cancelledStages = new HashSet[Int]()

  val taskScheduler = new TaskScheduler() {
    override def schedulingMode: SchedulingMode = SchedulingMode.FIFO
    override def rootPool: Pool = new Pool("", schedulingMode, 0, 0)
    override def start() = {}
    override def stop() = {}
    override def executorHeartbeatReceived(
        execId: String,
        accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
        blockManagerId: BlockManagerId): Boolean = true
    override def submitTasks(taskSet: TaskSet) = {
      // normally done by TaskSetManager
      taskSet.tasks.foreach(_.epoch = mapOutputTracker.getEpoch)
      taskSets += taskSet
    }
    override def cancelTasks(stageId: Int, interruptThread: Boolean) {
      cancelledStages += stageId
    }
    override def killTaskAttempt(
      taskId: Long, interruptThread: Boolean, reason: String): Boolean = false
    override def killAllTaskAttempts(
      stageId: Int, interruptThread: Boolean, reason: String): Unit = {}
    override def setDAGScheduler(dagScheduler: DAGScheduler) = {}
    override def defaultParallelism() = 2
    override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
    override def workerRemoved(workerId: String, host: String, message: String): Unit = {}
    override def applicationAttemptId(): Option[String] = None
  }

  /** Length of time to wait while draining listener events. */
  val WAIT_TIMEOUT_MILLIS = 10000
  val sparkListener = new SparkListener() {
    val submittedStageInfos = new HashSet[StageInfo]
    val successfulStages = new HashSet[Int]
    val failedStages = new ArrayBuffer[Int]
    val stageByOrderOfExecution = new ArrayBuffer[Int]
    val endedTasks = new HashSet[Long]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
      submittedStageInfos += stageSubmitted.stageInfo
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
      val stageInfo = stageCompleted.stageInfo
      stageByOrderOfExecution += stageInfo.stageId
      if (stageInfo.failureReason.isEmpty) {
        successfulStages += stageInfo.stageId
      } else {
        failedStages += stageInfo.stageId
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      endedTasks += taskEnd.taskInfo.taskId
    }
  }

  var mapOutputTracker: MapOutputTrackerMaster = null
  var broadcastManager: BroadcastManager = null
  var securityMgr: SecurityManager = null
  var scheduler: DAGScheduler = null
  var dagEventProcessLoopTester: DAGSchedulerEventProcessLoop = null

  /**
   * Set of cache locations to return from our mock BlockManagerMaster.
   * Keys are (rdd ID, partition ID). Anything not present will return an empty
   * list of cache locations silently.
   */
  val cacheLocations = new HashMap[(Int, Int), Seq[BlockManagerId]]
  // stub out BlockManagerMaster.getLocations to use our cacheLocations
  val blockManagerMaster = new BlockManagerMaster(null, conf, true) {
      override def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
        blockIds.map {
          _.asRDDId.map(id => (id.rddId -> id.splitIndex)).flatMap(key => cacheLocations.get(key)).
            getOrElse(Seq())
        }.toIndexedSeq
      }
      override def removeExecutor(execId: String) {
        // don't need to propagate to the driver, which we don't have
      }
    }

  /** The list of results that DAGScheduler has collected. */
  val results = new HashMap[Int, Any]()
  var failure: Exception = _
  val jobListener = new JobListener() {
    override def taskSucceeded(index: Int, result: Any) = results.put(index, result)
    override def jobFailed(exception: Exception) = { failure = exception }
  }

  /** A simple helper class for creating custom JobListeners */
  class SimpleListener extends JobListener {
    val results = new HashMap[Int, Any]
    var failure: Exception = null
    override def taskSucceeded(index: Int, result: Any): Unit = results.put(index, result)
    override def jobFailed(exception: Exception): Unit = { failure = exception }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    init(new SparkConf())
  }

  private def init(testConf: SparkConf): Unit = {
    sc = new SparkContext("local[2]", "DAGSchedulerSuite", testConf)
    sparkListener.submittedStageInfos.clear()
    sparkListener.successfulStages.clear()
    sparkListener.failedStages.clear()
    sparkListener.endedTasks.clear()
    failure = null
    sc.addSparkListener(sparkListener)
    taskSets.clear()
    cancelledStages.clear()
    cacheLocations.clear()
    results.clear()
    securityMgr = new SecurityManager(conf)
    broadcastManager = new BroadcastManager(true, conf, securityMgr)
    mapOutputTracker = new MapOutputTrackerMaster(conf, broadcastManager, true) {
      override def sendTracker(message: Any): Unit = {
        // no-op, just so we can stop this to avoid leaking threads
      }
    }
    scheduler = new DAGScheduler(
      sc,
      taskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)
  }

  override def afterEach(): Unit = {
    try {
      scheduler.stop()
      dagEventProcessLoopTester.stop()
      mapOutputTracker.stop()
      broadcastManager.stop()
    } finally {
      super.afterEach()
    }
  }

  override def afterAll() {
    super.afterAll()
  }

  /**
   * Type of RDD we use for testing. Note that we should never call the real RDD compute methods.
   * This is a pair RDD type so it can always be used in ShuffleDependencies.
   */
  type PairOfIntsRDD = RDD[(Int, Int)]

  /**
   * Process the supplied event as if it were the top of the DAGScheduler event queue, expecting
   * the scheduler not to exit.
   *
   * After processing the event, submit waiting stages as is done on most iterations of the
   * DAGScheduler event loop.
   */
  private def runEvent(event: DAGSchedulerEvent) {
    dagEventProcessLoopTester.post(event)
  }

  /**
   * When we submit dummy Jobs, this is the compute function we supply. Except in a local test
   * below, we do not expect this function to ever be executed; instead, we will return results
   * directly through CompletionEvents.
   */
  private val jobComputeFunc = (context: TaskContext, it: Iterator[(_)]) =>
    it.next.asInstanceOf[Tuple2[_, _]]._1

  /** Send the given CompletionEvent messages for the tasks in the TaskSet. */
  private def complete(taskSet: TaskSet, results: Seq[(TaskEndReason, Any)]) {
    assert(taskSet.tasks.size >= results.size)
    for ((result, i) <- results.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(makeCompletionEvent(taskSet.tasks(i), result._1, result._2))
      }
    }
  }

  private def completeWithAccumulator(
      accumId: Long,
      taskSet: TaskSet,
      results: Seq[(TaskEndReason, Any)]) {
    assert(taskSet.tasks.size >= results.size)
    for ((result, i) <- results.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(makeCompletionEvent(
          taskSet.tasks(i),
          result._1,
          result._2,
          Seq(AccumulatorSuite.createLongAccum("", initValue = 1, id = accumId))))
      }
    }
  }

  /** Submits a job to the scheduler and returns the job id. */
  private def submit(
      rdd: RDD[_],
      partitions: Array[Int],
      func: (TaskContext, Iterator[_]) => _ = jobComputeFunc,
      listener: JobListener = jobListener,
      properties: Properties = null): Int = {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, func, partitions, CallSite("", ""), listener, properties))
    jobId
  }

  /** Submits a map stage to the scheduler and returns the job id. */
  private def submitMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      listener: JobListener = jobListener): Int = {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(MapStageSubmitted(jobId, shuffleDep, CallSite("", ""), listener))
    jobId
  }

  /** Sends TaskSetFailed to the scheduler. */
  private def failed(taskSet: TaskSet, message: String) {
    runEvent(TaskSetFailed(taskSet, message, None))
  }

  /** Sends JobCancelled to the DAG scheduler. */
  private def cancel(jobId: Int) {
    runEvent(JobCancelled(jobId, None))
  }

  test("[SPARK-3353] parent stage should have lower stage id") {
    sparkListener.stageByOrderOfExecution.clear()
    sc.parallelize(1 to 10).map(x => (x, x)).reduceByKey(_ + _, 4).count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.stageByOrderOfExecution.length === 2)
    assert(sparkListener.stageByOrderOfExecution(0) < sparkListener.stageByOrderOfExecution(1))
  }

  /**
   * This test ensures that DAGScheduler build stage graph correctly.
   *
   * Suppose you have the following DAG:
   *
   * [A] <--(s_A)-- [B] <--(s_B)-- [C] <--(s_C)-- [D]
   *             \                /
   *               <-------------
   *
   * Here, RDD B has a shuffle dependency on RDD A, and RDD C has shuffle dependency on both
   * B and A. The shuffle dependency IDs are numbers in the DAGScheduler, but to make the example
   * easier to understand, let's call the shuffled data from A shuffle dependency ID s_A and the
   * shuffled data from B shuffle dependency ID s_B.
   *
   * Note: [] means an RDD, () means a shuffle dependency.
   */
  test("[SPARK-13902] Ensure no duplicate stages are created") {
    val rddA = new MyRDD(sc, 1, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(1))
    val s_A = shuffleDepA.shuffleId

    val rddB = new MyRDD(sc, 1, List(shuffleDepA), tracker = mapOutputTracker)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(1))
    val s_B = shuffleDepB.shuffleId

    val rddC = new MyRDD(sc, 1, List(shuffleDepA, shuffleDepB), tracker = mapOutputTracker)
    val shuffleDepC = new ShuffleDependency(rddC, new HashPartitioner(1))
    val s_C = shuffleDepC.shuffleId

    val rddD = new MyRDD(sc, 1, List(shuffleDepC), tracker = mapOutputTracker)

    submit(rddD, Array(0))

    assert(scheduler.shuffleIdToMapStage.size === 3)
    assert(scheduler.activeJobs.size === 1)

    val mapStageA = scheduler.shuffleIdToMapStage(s_A)
    val mapStageB = scheduler.shuffleIdToMapStage(s_B)
    val mapStageC = scheduler.shuffleIdToMapStage(s_C)
    val finalStage = scheduler.activeJobs.head.finalStage

    assert(mapStageA.parents.isEmpty)
    assert(mapStageB.parents === List(mapStageA))
    assert(mapStageC.parents === List(mapStageA, mapStageB))
    assert(finalStage.parents === List(mapStageC))

    complete(taskSets(0), Seq((Success, makeMapStatus("hostA", 1))))
    complete(taskSets(1), Seq((Success, makeMapStatus("hostA", 1))))
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", 1))))
    complete(taskSets(3), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("All shuffle files on the slave should be cleaned up when slave lost") {
    // reset the test context with the right shuffle service config
    afterEach()
    val conf = new SparkConf()
    conf.set(config.SHUFFLE_SERVICE_ENABLED.key, "true")
    conf.set("spark.files.fetchFailure.unRegisterOutputOnHost", "true")
    init(conf)
    runEvent(ExecutorAdded("exec-hostA1", "hostA"))
    runEvent(ExecutorAdded("exec-hostA2", "hostA"))
    runEvent(ExecutorAdded("exec-hostB", "hostB"))
    val firstRDD = new MyRDD(sc, 3, Nil)
    val firstShuffleDep = new ShuffleDependency(firstRDD, new HashPartitioner(3))
    val firstShuffleId = firstShuffleDep.shuffleId
    val shuffleMapRdd = new MyRDD(sc, 3, List(firstShuffleDep))
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(3))
    val secondShuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    // map stage1 completes successfully, with one task on each executor
    complete(taskSets(0), Seq(
      (Success,
        MapStatus(BlockManagerId("exec-hostA1", "hostA", 12345), Array.fill[Long](1)(2))),
      (Success,
        MapStatus(BlockManagerId("exec-hostA2", "hostA", 12345), Array.fill[Long](1)(2))),
      (Success, makeMapStatus("hostB", 1))
    ))
    // map stage2 completes successfully, with one task on each executor
    complete(taskSets(1), Seq(
      (Success,
        MapStatus(BlockManagerId("exec-hostA1", "hostA", 12345), Array.fill[Long](1)(2))),
      (Success,
        MapStatus(BlockManagerId("exec-hostA2", "hostA", 12345), Array.fill[Long](1)(2))),
      (Success, makeMapStatus("hostB", 1))
    ))
    // make sure our test setup is correct
    val initialMapStatus1 = mapOutputTracker.shuffleStatuses(firstShuffleId).mapStatuses
    //  val initialMapStatus1 = mapOutputTracker.mapStatuses.get(0).get
    assert(initialMapStatus1.count(_ != null) === 3)
    assert(initialMapStatus1.map{_.location.executorId}.toSet ===
      Set("exec-hostA1", "exec-hostA2", "exec-hostB"))

    val initialMapStatus2 = mapOutputTracker.shuffleStatuses(secondShuffleId).mapStatuses
    //  val initialMapStatus1 = mapOutputTracker.mapStatuses.get(0).get
    assert(initialMapStatus2.count(_ != null) === 3)
    assert(initialMapStatus2.map{_.location.executorId}.toSet ===
      Set("exec-hostA1", "exec-hostA2", "exec-hostB"))

    // reduce stage fails with a fetch failure from one host
    complete(taskSets(2), Seq(
      (FetchFailed(BlockManagerId("exec-hostA2", "hostA", 12345), firstShuffleId, 0, 0, "ignored"),
        null)
    ))

    // Here is the main assertion -- make sure that we de-register
    // the map outputs for both map stage from both executors on hostA

    val mapStatus1 = mapOutputTracker.shuffleStatuses(firstShuffleId).mapStatuses
    assert(mapStatus1.count(_ != null) === 1)
    assert(mapStatus1(2).location.executorId === "exec-hostB")
    assert(mapStatus1(2).location.host === "hostB")

    val mapStatus2 = mapOutputTracker.shuffleStatuses(secondShuffleId).mapStatuses
    assert(mapStatus2.count(_ != null) === 1)
    assert(mapStatus2(2).location.executorId === "exec-hostB")
    assert(mapStatus2(2).location.host === "hostB")
  }

  test("zero split job") {
    var numResults = 0
    var failureReason: Option[Exception] = None
    val fakeListener = new JobListener() {
      override def taskSucceeded(partition: Int, value: Any): Unit = numResults += 1
      override def jobFailed(exception: Exception): Unit = {
        failureReason = Some(exception)
      }
    }
    val jobId = submit(new MyRDD(sc, 0, Nil), Array(), listener = fakeListener)
    assert(numResults === 0)
    cancel(jobId)
    assert(failureReason.isDefined)
    assert(failureReason.get.getMessage() === "Job 0 cancelled ")
  }

  test("run trivial job") {
    submit(new MyRDD(sc, 1, Nil), Array(0))
    complete(taskSets(0), List((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("run trivial job w/ dependency") {
    val baseRdd = new MyRDD(sc, 1, Nil)
    val finalRdd = new MyRDD(sc, 1, List(new OneToOneDependency(baseRdd)))
    submit(finalRdd, Array(0))
    complete(taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("equals and hashCode AccumulableInfo") {
    val accInfo1 = new AccumulableInfo(
      1, Some("a1"), Some("delta1"), Some("val1"), internal = true, countFailedValues = false)
    val accInfo2 = new AccumulableInfo(
      1, Some("a1"), Some("delta1"), Some("val1"), internal = false, countFailedValues = false)
    val accInfo3 = new AccumulableInfo(
      1, Some("a1"), Some("delta1"), Some("val1"), internal = false, countFailedValues = false)
    assert(accInfo1 !== accInfo2)
    assert(accInfo2 === accInfo3)
    assert(accInfo2.hashCode() === accInfo3.hashCode())
  }

  test("cache location preferences w/ dependency") {
    val baseRdd = new MyRDD(sc, 1, Nil).cache()
    val finalRdd = new MyRDD(sc, 1, List(new OneToOneDependency(baseRdd)))
    cacheLocations(baseRdd.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    submit(finalRdd, Array(0))
    val taskSet = taskSets(0)
    assertLocations(taskSet, Seq(Seq("hostA", "hostB")))
    complete(taskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("regression test for getCacheLocs") {
    val rdd = new MyRDD(sc, 3, Nil).cache()
    cacheLocations(rdd.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    cacheLocations(rdd.id -> 1) =
      Seq(makeBlockManagerId("hostB"), makeBlockManagerId("hostC"))
    cacheLocations(rdd.id -> 2) =
      Seq(makeBlockManagerId("hostC"), makeBlockManagerId("hostD"))
    val locs = scheduler.getCacheLocs(rdd).map(_.map(_.host))
    assert(locs === Seq(Seq("hostA", "hostB"), Seq("hostB", "hostC"), Seq("hostC", "hostD")))
  }

  /**
   * This test ensures that if a particular RDD is cached, RDDs earlier in the dependency chain
   * are not computed. It constructs the following chain of dependencies:
   * +---+ shuffle +---+    +---+    +---+
   * | A |<--------| B |<---| C |<---| D |
   * +---+         +---+    +---+    +---+
   * Here, B is derived from A by performing a shuffle, C has a one-to-one dependency on B,
   * and D similarly has a one-to-one dependency on C. If none of the RDDs were cached, this
   * set of RDDs would result in a two stage job: one ShuffleMapStage, and a ResultStage that
   * reads the shuffled data from RDD A. This test ensures that if C is cached, the scheduler
   * doesn't perform a shuffle, and instead computes the result using a single ResultStage
   * that reads C's cached data.
   */
  test("getMissingParentStages should consider all ancestor RDDs' cache statuses") {
    val rddA = new MyRDD(sc, 1, Nil)
    val rddB = new MyRDD(sc, 1, List(new ShuffleDependency(rddA, new HashPartitioner(1))),
      tracker = mapOutputTracker)
    val rddC = new MyRDD(sc, 1, List(new OneToOneDependency(rddB))).cache()
    val rddD = new MyRDD(sc, 1, List(new OneToOneDependency(rddC)))
    cacheLocations(rddC.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    submit(rddD, Array(0))
    assert(scheduler.runningStages.size === 1)
    // Make sure that the scheduler is running the final result stage.
    // Because C is cached, the shuffle map stage to compute A does not need to be run.
    assert(scheduler.runningStages.head.isInstanceOf[ResultStage])
  }

  test("avoid exponential blowup when getting preferred locs list") {
    // Build up a complex dependency graph with repeated zip operations, without preferred locations
    var rdd: RDD[_] = new MyRDD(sc, 1, Nil)
    (1 to 30).foreach(_ => rdd = rdd.zip(rdd))
    // getPreferredLocs runs quickly, indicating that exponential graph traversal is avoided.
    failAfter(10 seconds) {
      val preferredLocs = scheduler.getPreferredLocs(rdd, 0)
      // No preferred locations are returned.
      assert(preferredLocs.length === 0)
    }
  }

  test("unserializable task") {
    val unserializableRdd = new MyRDD(sc, 1, Nil) {
      class UnserializableClass
      val unserializable = new UnserializableClass
    }
    submit(unserializableRdd, Array(0))
    assert(failure.getMessage.startsWith(
      "Job aborted due to stage failure: Task not serializable:"))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("trivial job failure") {
    submit(new MyRDD(sc, 1, Nil), Array(0))
    failed(taskSets(0), "some failure")
    assert(failure.getMessage === "Job aborted due to stage failure: some failure")
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("trivial job cancellation") {
    val rdd = new MyRDD(sc, 1, Nil)
    val jobId = submit(rdd, Array(0))
    cancel(jobId)
    assert(failure.getMessage === s"Job $jobId cancelled ")
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("job cancellation no-kill backend") {
    // make sure that the DAGScheduler doesn't crash when the TaskScheduler
    // doesn't implement killTask()
    val noKillTaskScheduler = new TaskScheduler() {
      override def schedulingMode: SchedulingMode = SchedulingMode.FIFO
      override def rootPool: Pool = new Pool("", schedulingMode, 0, 0)
      override def start(): Unit = {}
      override def stop(): Unit = {}
      override def submitTasks(taskSet: TaskSet): Unit = {
        taskSets += taskSet
      }
      override def cancelTasks(stageId: Int, interruptThread: Boolean) {
        throw new UnsupportedOperationException
      }
      override def killTaskAttempt(
          taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
        throw new UnsupportedOperationException
      }
      override def killAllTaskAttempts(
          stageId: Int, interruptThread: Boolean, reason: String): Unit = {
        throw new UnsupportedOperationException
      }
      override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {}
      override def defaultParallelism(): Int = 2
      override def executorHeartbeatReceived(
          execId: String,
          accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
          blockManagerId: BlockManagerId): Boolean = true
      override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
      override def workerRemoved(workerId: String, host: String, message: String): Unit = {}
      override def applicationAttemptId(): Option[String] = None
    }
    val noKillScheduler = new DAGScheduler(
      sc,
      noKillTaskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(noKillScheduler)
    val jobId = submit(new MyRDD(sc, 1, Nil), Array(0))
    cancel(jobId)
    // Because the job wasn't actually cancelled, we shouldn't have received a failure message.
    assert(failure === null)

    // When the task set completes normally, state should be correctly updated.
    complete(taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.isEmpty)
    assert(sparkListener.successfulStages.contains(0))
  }

  test("run trivial shuffle") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 1)),
      (Success, makeMapStatus("hostB", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    complete(taskSets(1), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("run trivial shuffle with fetch failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", reduceRdd.partitions.length)),
      (Success, makeMapStatus("hostB", reduceRdd.partitions.length))))
    // the 2nd ResultTask failed
    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"), null)))
    // this will get called
    // blockManagerMaster.removeExecutor("exec-hostA")
    // ask the scheduler to try it again
    scheduler.resubmitFailedStages()
    // have the 2nd attempt pass
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", reduceRdd.partitions.length))))
    // we can see both result blocks now
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))
    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  private val shuffleFileLossTests = Seq(
    ("slave lost with shuffle service", SlaveLost("", false), true, false),
    ("worker lost with shuffle service", SlaveLost("", true), true, true),
    ("worker lost without shuffle service", SlaveLost("", true), false, true),
    ("executor failure with shuffle service", ExecutorKilled, true, false),
    ("executor failure without shuffle service", ExecutorKilled, false, true))

  for ((eventDescription, event, shuffleServiceOn, expectFileLoss) <- shuffleFileLossTests) {
    val maybeLost = if (expectFileLoss) {
      "lost"
    } else {
      "not lost"
    }
    test(s"shuffle files $maybeLost when $eventDescription") {
      // reset the test context with the right shuffle service config
      afterEach()
      val conf = new SparkConf()
      conf.set(config.SHUFFLE_SERVICE_ENABLED.key, shuffleServiceOn.toString)
      init(conf)
      assert(sc.env.blockManager.externalShuffleServiceEnabled == shuffleServiceOn)

      val shuffleMapRdd = new MyRDD(sc, 2, Nil)
      val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
      val shuffleId = shuffleDep.shuffleId
      val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
      submit(reduceRdd, Array(0))
      complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))))
      runEvent(ExecutorLost("exec-hostA", event))
      if (expectFileLoss) {
        intercept[MetadataFetchFailedException] {
          mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0)
        }
      } else {
        assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
          HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
      }
    }
  }

  // Helper function to validate state when creating tests for task failures
  private def checkStageId(stageId: Int, attempt: Int, stageAttempt: TaskSet) {
    assert(stageAttempt.stageId === stageId)
    assert(stageAttempt.stageAttemptId == attempt)
  }

  // Helper functions to extract commonly used code in Fetch Failure test cases
  private def setupStageAbortTest(sc: SparkContext) {
    sc.listenerBus.addToSharedQueue(new EndListener())
    ended = false
    jobResult = null
  }

  // Create a new Listener to confirm that the listenerBus sees the JobEnd message
  // when we abort the stage. This message will also be consumed by the EventLoggingListener
  // so this will propagate up to the user.
  var ended = false
  var jobResult : JobResult = null

  class EndListener extends SparkListener {
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      jobResult = jobEnd.jobResult
      ended = true
    }
  }

  /**
   * Common code to get the next stage attempt, confirm it's the one we expect, and complete it
   * successfully.
   *
   * @param stageId - The current stageId
   * @param attemptIdx - The current attempt count
   * @param numShufflePartitions - The number of partitions in the next stage
   */
  private def completeShuffleMapStageSuccessfully(
      stageId: Int,
      attemptIdx: Int,
      numShufflePartitions: Int): Unit = {
    val stageAttempt = taskSets.last
    checkStageId(stageId, attemptIdx, stageAttempt)
    complete(stageAttempt, stageAttempt.tasks.zipWithIndex.map {
      case (task, idx) =>
        (Success, makeMapStatus("host" + ('A' + idx).toChar, numShufflePartitions))
    }.toSeq)
  }

  /**
   * Common code to get the next stage attempt, confirm it's the one we expect, and complete it
   * with all FetchFailure.
   *
   * @param stageId - The current stageId
   * @param attemptIdx - The current attempt count
   * @param shuffleDep - The shuffle dependency of the stage with a fetch failure
   */
  private def completeNextStageWithFetchFailure(
      stageId: Int,
      attemptIdx: Int,
      shuffleDep: ShuffleDependency[_, _, _]): Unit = {
    val stageAttempt = taskSets.last
    checkStageId(stageId, attemptIdx, stageAttempt)
    complete(stageAttempt, stageAttempt.tasks.zipWithIndex.map { case (task, idx) =>
      (FetchFailed(makeBlockManagerId("hostA"), shuffleDep.shuffleId, 0, idx, "ignored"), null)
    }.toSeq)
  }

  /**
   * Common code to get the next result stage attempt, confirm it's the one we expect, and
   * complete it with a success where we return 42.
   *
   * @param stageId - The current stageId
   * @param attemptIdx - The current attempt count
   */
  private def completeNextResultStageWithSuccess(
      stageId: Int,
      attemptIdx: Int,
      partitionToResult: Int => Int = _ => 42): Unit = {
    val stageAttempt = taskSets.last
    checkStageId(stageId, attemptIdx, stageAttempt)
    assert(scheduler.stageIdToStage(stageId).isInstanceOf[ResultStage])
    val taskResults = stageAttempt.tasks.zipWithIndex.map { case (task, idx) =>
      (Success, partitionToResult(idx))
    }
    complete(stageAttempt, taskResults.toSeq)
  }

  /**
   * In this test, we simulate a job where many tasks in the same stage fail. We want to show
   * that many fetch failures inside a single stage attempt do not trigger an abort
   * on their own, but only when there are enough failing stage attempts.
   */
  test("Single stage fetch failure should not abort the stage.") {
    setupStageAbortTest(sc)

    val parts = 8
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, (0 until parts).toArray)

    completeShuffleMapStageSuccessfully(0, 0, numShufflePartitions = parts)

    completeNextStageWithFetchFailure(1, 0, shuffleDep)

    // Resubmit and confirm that now all is well
    scheduler.resubmitFailedStages()

    assert(scheduler.runningStages.nonEmpty)
    assert(!ended)

    // Complete stage 0 and then stage 1 with a "42"
    completeShuffleMapStageSuccessfully(0, 1, numShufflePartitions = parts)
    completeNextResultStageWithSuccess(1, 1)

    // Confirm job finished successfully
    sc.listenerBus.waitUntilEmpty(1000)
    assert(ended === true)
    assert(results === (0 until parts).map { idx => idx -> 42 }.toMap)
    assertDataStructuresEmpty()
  }

  /**
   * In this test we simulate a job failure where the first stage completes successfully and
   * the second stage fails due to a fetch failure. Multiple successive fetch failures of a stage
   * trigger an overall job abort to avoid endless retries.
   */
  test("Multiple consecutive stage fetch failures should lead to job being aborted.") {
    setupStageAbortTest(sc)

    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    for (attempt <- 0 until scheduler.maxConsecutiveStageAttempts) {
      // Complete all the tasks for the current attempt of stage 0 successfully
      completeShuffleMapStageSuccessfully(0, attempt, numShufflePartitions = 2)

      // Now we should have a new taskSet, for a new attempt of stage 1.
      // Fail all these tasks with FetchFailure
      completeNextStageWithFetchFailure(1, attempt, shuffleDep)

      // this will trigger a resubmission of stage 0, since we've lost some of its
      // map output, for the next iteration through the loop
      scheduler.resubmitFailedStages()

      if (attempt < scheduler.maxConsecutiveStageAttempts - 1) {
        assert(scheduler.runningStages.nonEmpty)
        assert(!ended)
      } else {
        // Stage should have been aborted and removed from running stages
        assertDataStructuresEmpty()
        sc.listenerBus.waitUntilEmpty(1000)
        assert(ended)
        jobResult match {
          case JobFailed(reason) =>
            assert(reason.getMessage.contains("ResultStage 1 () has failed the maximum"))
          case other => fail(s"expected JobFailed, not $other")
        }
      }
    }
  }

  /**
   * In this test, we create a job with two consecutive shuffles, and simulate 2 failures for each
   * shuffle fetch. In total In total, the job has had four failures overall but not four failures
   * for a particular stage, and as such should not be aborted.
   */
  test("Failures in different stages should not trigger an overall abort") {
    setupStageAbortTest(sc)

    val shuffleOneRdd = new MyRDD(sc, 2, Nil).cache()
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, new HashPartitioner(2))
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne), tracker = mapOutputTracker).cache()
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, new HashPartitioner(1))
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo), tracker = mapOutputTracker)
    submit(finalRdd, Array(0))

    // In the first two iterations, Stage 0 succeeds and stage 1 fails. In the next two iterations,
    // stage 2 fails.
    for (attempt <- 0 until scheduler.maxConsecutiveStageAttempts) {
      // Complete all the tasks for the current attempt of stage 0 successfully
      completeShuffleMapStageSuccessfully(0, attempt, numShufflePartitions = 2)

      if (attempt < scheduler.maxConsecutiveStageAttempts / 2) {
        // Now we should have a new taskSet, for a new attempt of stage 1.
        // Fail all these tasks with FetchFailure
        completeNextStageWithFetchFailure(1, attempt, shuffleDepOne)
      } else {
        completeShuffleMapStageSuccessfully(1, attempt, numShufflePartitions = 1)

        // Fail stage 2
        completeNextStageWithFetchFailure(2,
          attempt - scheduler.maxConsecutiveStageAttempts / 2, shuffleDepTwo)
      }

      // this will trigger a resubmission of stage 0, since we've lost some of its
      // map output, for the next iteration through the loop
      scheduler.resubmitFailedStages()
    }

    completeShuffleMapStageSuccessfully(0, 4, numShufflePartitions = 2)
    completeShuffleMapStageSuccessfully(1, 4, numShufflePartitions = 1)

    // Succeed stage2 with a "42"
    completeNextResultStageWithSuccess(2, scheduler.maxConsecutiveStageAttempts / 2)

    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  /**
   * In this test we demonstrate that only consecutive failures trigger a stage abort. A stage may
   * fail multiple times, succeed, then fail a few more times (because its run again by downstream
   * dependencies). The total number of failed attempts for one stage will go over the limit,
   * but that doesn't matter, since they have successes in the middle.
   */
  test("Non-consecutive stage failures don't trigger abort") {
    setupStageAbortTest(sc)

    val shuffleOneRdd = new MyRDD(sc, 2, Nil).cache()
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, new HashPartitioner(2))
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne), tracker = mapOutputTracker).cache()
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, new HashPartitioner(1))
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo), tracker = mapOutputTracker)
    submit(finalRdd, Array(0))

    // First, execute stages 0 and 1, failing stage 1 up to MAX-1 times.
    for (attempt <- 0 until scheduler.maxConsecutiveStageAttempts - 1) {
      // Make each task in stage 0 success
      completeShuffleMapStageSuccessfully(0, attempt, numShufflePartitions = 2)

      // Now we should have a new taskSet, for a new attempt of stage 1.
      // Fail these tasks with FetchFailure
      completeNextStageWithFetchFailure(1, attempt, shuffleDepOne)

      scheduler.resubmitFailedStages()

      // Confirm we have not yet aborted
      assert(scheduler.runningStages.nonEmpty)
      assert(!ended)
    }

    // Rerun stage 0 and 1 to step through the task set
    completeShuffleMapStageSuccessfully(0, 3, numShufflePartitions = 2)
    completeShuffleMapStageSuccessfully(1, 3, numShufflePartitions = 1)

    // Fail stage 2 so that stage 1 is resubmitted when we call scheduler.resubmitFailedStages()
    completeNextStageWithFetchFailure(2, 0, shuffleDepTwo)

    scheduler.resubmitFailedStages()

    // Rerun stage 0 to step through the task set
    completeShuffleMapStageSuccessfully(0, 4, numShufflePartitions = 2)

    // Now again, fail stage 1 (up to MAX_FAILURES) but confirm that this doesn't trigger an abort
    // since we succeeded in between.
    completeNextStageWithFetchFailure(1, 4, shuffleDepOne)

    scheduler.resubmitFailedStages()

    // Confirm we have not yet aborted
    assert(scheduler.runningStages.nonEmpty)
    assert(!ended)

    // Next, succeed all and confirm output
    // Rerun stage 0 + 1
    completeShuffleMapStageSuccessfully(0, 5, numShufflePartitions = 2)
    completeShuffleMapStageSuccessfully(1, 5, numShufflePartitions = 1)

    // Succeed stage 2 and verify results
    completeNextResultStageWithSuccess(2, 1)

    assertDataStructuresEmpty()
    sc.listenerBus.waitUntilEmpty(1000)
    assert(ended === true)
    assert(results === Map(0 -> 42))
  }

  test("trivial shuffle with multiple fetch failures") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", reduceRdd.partitions.length)),
      (Success, makeMapStatus("hostB", reduceRdd.partitions.length))))
    // The MapOutputTracker should know about both map output locations.
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(1))

    // The second ResultTask fails, with a fetch failure for the output from the second mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 1, 1, "ignored"),
      null))
    // The SparkListener should not receive redundant failure events.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.size == 1)
  }

  test("Retry all the tasks on a resubmitted attempt of a barrier stage caused by FetchFailure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", reduceRdd.partitions.length)),
      (Success, makeMapStatus("hostB", reduceRdd.partitions.length))))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq(0, 1)))

    scheduler.resubmitFailedStages()
    // Complete the map stage.
    completeShuffleMapStageSuccessfully(0, 1, numShufflePartitions = 2)

    // Complete the result stage.
    completeNextResultStageWithSuccess(1, 1)

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assertDataStructuresEmpty()
  }

  test("Retry all the tasks on a resubmitted attempt of a barrier stage caused by TaskKilled") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", reduceRdd.partitions.length))))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq(1)))

    // The second map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      TaskKilled("test"),
      null))
    assert(sparkListener.failedStages === Seq(0))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq(0, 1)))

    scheduler.resubmitFailedStages()
    // Complete the map stage.
    completeShuffleMapStageSuccessfully(0, 1, numShufflePartitions = 2)

    // Complete the result stage.
    completeNextResultStageWithSuccess(1, 0)

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assertDataStructuresEmpty()
  }

  test("Fail the job if a barrier ResultTask failed") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
      .barrier()
      .mapPartitions(iter => iter)
    submit(reduceRdd, Array(0, 1))

    // Complete the map stage.
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostA", 2))))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))

    // The first ResultTask fails
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      TaskKilled("test"),
      null))

    // Assert the stage has been cancelled.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(failure.getMessage.startsWith("Job aborted due to stage failure: Could not recover " +
      "from a failed barrier ResultStage."))
  }

  /**
   * This tests the case where another FetchFailed comes in while the map stage is getting
   * re-run.
   */
  test("late fetch failures don't cause multiple concurrent attempts for the same map stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    val mapStageId = 0
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == mapStageId)
    }

    // The map stage should have been submitted.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 1)

    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))
    // The MapOutputTracker should know about both map output locations.
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 1).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(1))

    // Trigger resubmission of the failed map stage.
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    // Another attempt for the map stage should have been submitted, resulting in 2 total attempts.
    assert(countSubmittedMapStageAttempts() === 2)

    // The second ResultTask fails, with a fetch failure for the output from the second mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId, 1, 1, "ignored"),
      null))

    // Another ResubmitFailedStages event should not result in another attempt for the map
    // stage being run concurrently.
    // NOTE: the actual ResubmitFailedStages may get called at any time during this, but it
    // shouldn't effect anything -- our calling it just makes *SURE* it gets called between the
    // desired event and our check.
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 2)

  }

  /**
   * This tests the case where a late FetchFailed comes in after the map stage has finished getting
   * retried and a new reduce stage starts running.
   */
  test("extremely late fetch failures don't cause multiple concurrent attempts for " +
    "the same stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    def countSubmittedReduceStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == 1)
    }
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == 0)
    }

    // The map stage should have been submitted.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 1)

    // Complete the map stage.
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))

    // The reduce stage should have been submitted.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedReduceStageAttempts() === 1)

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null))

    // Trigger resubmission of the failed map stage and finish the re-started map task.
    runEvent(ResubmitFailedStages)
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", 1))))

    // Because the map stage finished, another attempt for the reduce stage should have been
    // submitted, resulting in 2 total attempts for each the map and the reduce stage.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 2)
    assert(countSubmittedReduceStageAttempts() === 2)

    // A late FetchFailed arrives from the second task in the original reduce stage.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId, 1, 1, "ignored"),
      null))

    // Running ResubmitFailedStages shouldn't result in any more attempts for the map stage, because
    // the FetchFailed should have been ignored
    runEvent(ResubmitFailedStages)

    // The FetchFailed from the original reduce stage should be ignored.
    assert(countSubmittedMapStageAttempts() === 2)
  }

  test("task events always posted in speculation / when stage is killed") {
    val baseRdd = new MyRDD(sc, 4, Nil)
    val finalRdd = new MyRDD(sc, 4, List(new OneToOneDependency(baseRdd)))
    submit(finalRdd, Array(0, 1, 2, 3))

    // complete two tasks
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0), Success, 42,
      Seq.empty, createFakeTaskInfoWithId(0)))
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1), Success, 42,
      Seq.empty, createFakeTaskInfoWithId(1)))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    // verify stage exists
    assert(scheduler.stageIdToStage.contains(0))
    assert(sparkListener.endedTasks.size == 2)

    // finish other 2 tasks
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(2), Success, 42,
      Seq.empty, createFakeTaskInfoWithId(2)))
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(3), Success, 42,
      Seq.empty, createFakeTaskInfoWithId(3)))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.endedTasks.size == 4)

    // verify the stage is done
    assert(!scheduler.stageIdToStage.contains(0))

    // Stage should be complete. Finish one other Successful task to simulate what can happen
    // with a speculative task and make sure the event is sent out
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(3), Success, 42,
      Seq.empty, createFakeTaskInfoWithId(5)))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.endedTasks.size == 5)

    // make sure non successful tasks also send out event
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(3), UnknownReason, 42,
      Seq.empty, createFakeTaskInfoWithId(6)))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.endedTasks.size == 6)
  }

  test("ignore late map task completions") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    // pretend we were told hostA went away
    val oldEpoch = mapOutputTracker.getEpoch
    runEvent(ExecutorLost("exec-hostA", ExecutorKilled))
    val newEpoch = mapOutputTracker.getEpoch
    assert(newEpoch > oldEpoch)

    // now start completing some tasks in the shuffle map stage, under different hosts
    // and epochs, and make sure scheduler updates its state correctly
    val taskSet = taskSets(0)
    val shuffleStage = scheduler.stageIdToStage(taskSet.stageId).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage.numAvailableOutputs === 0)

    // should be ignored for being too old
    runEvent(makeCompletionEvent(
      taskSet.tasks(0),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.size)))
    assert(shuffleStage.numAvailableOutputs === 0)

    // should work because it's a non-failed host (so the available map outputs will increase)
    runEvent(makeCompletionEvent(
      taskSet.tasks(0),
      Success,
      makeMapStatus("hostB", reduceRdd.partitions.size)))
    assert(shuffleStage.numAvailableOutputs === 1)

    // should be ignored for being too old
    runEvent(makeCompletionEvent(
      taskSet.tasks(0),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.size)))
    assert(shuffleStage.numAvailableOutputs === 1)

    // should work because it's a new epoch, which will increase the number of available map
    // outputs, and also finish the stage
    taskSet.tasks(1).epoch = newEpoch
    runEvent(makeCompletionEvent(
      taskSet.tasks(1),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.size)))
    assert(shuffleStage.numAvailableOutputs === 2)
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostB"), makeBlockManagerId("hostA")))

    // finish the next stage normally, which completes the job
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("run shuffle with map stage failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    // Fail the map stage.  This should cause the entire job to fail.
    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)
    assert(failure.getMessage === s"Job aborted due to stage failure: $stageFailureMessage")

    // Listener bus should get told about the map stage failing, but not the reduce stage
    // (since the reduce stage hasn't been started yet).
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.toSet === Set(0))

    assertDataStructuresEmpty()
  }

  /**
   * Run two jobs, with a shared dependency.  We simulate a fetch failure in the second job, which
   * requires regenerating some outputs of the shared dependency.  One key aspect of this test is
   * that the second job actually uses a different stage for the shared dependency (a "skipped"
   * stage).
   */
  test("shuffle fetch failure in a reused shuffle dependency") {
    // Run the first job successfully, which creates one shuffle dependency

    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    completeShuffleMapStageSuccessfully(0, 0, 2)
    completeNextResultStageWithSuccess(1, 0)
    assert(results === Map(0 -> 42, 1 -> 42))
    assertDataStructuresEmpty()

    // submit another job w/ the shared dependency, and have a fetch failure
    val reduce2 = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduce2, Array(0, 1))
    // Note that the stage numbering here is only b/c the shared dependency produces a new, skipped
    // stage.  If instead it reused the existing stage, then this would be stage 2
    completeNextStageWithFetchFailure(3, 0, shuffleDep)
    scheduler.resubmitFailedStages()

    // the scheduler now creates a new task set to regenerate the missing map output, but this time
    // using a different stage, the "skipped" one

    // SPARK-9809 -- this stage is submitted without a task for each partition (because some of
    // the shuffle map output is still available from stage 0); make sure we've still got internal
    // accumulators setup
    assert(scheduler.stageIdToStage(2).latestInfo.taskMetrics != null)
    completeShuffleMapStageSuccessfully(2, 0, 2)
    completeNextResultStageWithSuccess(3, 1, idx => idx + 1234)
    assert(results === Map(0 -> 1234, 1 -> 1235))

    assertDataStructuresEmpty()
  }

  /**
   * This test runs a three stage job, with a fetch failure in stage 1.  but during the retry, we
   * have completions from both the first & second attempt of stage 1.  So all the map output is
   * available before we finish any task set for stage 1.  We want to make sure that we don't
   * submit stage 2 until the map output for stage 1 is registered
   */
  test("don't submit stage until its dependencies map outputs are registered (SPARK-5259)") {
    val firstRDD = new MyRDD(sc, 3, Nil)
    val firstShuffleDep = new ShuffleDependency(firstRDD, new HashPartitioner(3))
    val firstShuffleId = firstShuffleDep.shuffleId
    val shuffleMapRdd = new MyRDD(sc, 3, List(firstShuffleDep))
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))

    // things start out smoothly, stage 0 completes with no issues
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostB", shuffleMapRdd.partitions.length)),
      (Success, makeMapStatus("hostB", shuffleMapRdd.partitions.length)),
      (Success, makeMapStatus("hostA", shuffleMapRdd.partitions.length))
    ))

    // then one executor dies, and a task fails in stage 1
    runEvent(ExecutorLost("exec-hostA", ExecutorKilled))
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(null, firstShuffleId, 2, 0, "Fetch failed"),
      null))

    // so we resubmit stage 0, which completes happily
    scheduler.resubmitFailedStages()
    val stage0Resubmit = taskSets(2)
    assert(stage0Resubmit.stageId == 0)
    assert(stage0Resubmit.stageAttemptId === 1)
    val task = stage0Resubmit.tasks(0)
    assert(task.partitionId === 2)
    runEvent(makeCompletionEvent(
      task,
      Success,
      makeMapStatus("hostC", shuffleMapRdd.partitions.length)))

    // now here is where things get tricky : we will now have a task set representing
    // the second attempt for stage 1, but we *also* have some tasks for the first attempt for
    // stage 1 still going
    val stage1Resubmit = taskSets(3)
    assert(stage1Resubmit.stageId == 1)
    assert(stage1Resubmit.stageAttemptId === 1)
    assert(stage1Resubmit.tasks.length === 3)

    // we'll have some tasks finish from the first attempt, and some finish from the second attempt,
    // so that we actually have all stage outputs, though no attempt has completed all its
    // tasks
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(0),
      Success,
      makeMapStatus("hostC", reduceRdd.partitions.length)))
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(1),
      Success,
      makeMapStatus("hostC", reduceRdd.partitions.length)))
    // late task finish from the first attempt
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(2),
      Success,
      makeMapStatus("hostB", reduceRdd.partitions.length)))

    // What should happen now is that we submit stage 2.  However, we might not see an error
    // b/c of DAGScheduler's error handling (it tends to swallow errors and just log them).  But
    // we can check some conditions.
    // Note that the really important thing here is not so much that we submit stage 2 *immediately*
    // but that we don't end up with some error from these interleaved completions.  It would also
    // be OK (though sub-optimal) if stage 2 simply waited until the resubmission of stage 1 had
    // all its tasks complete

    // check that we have all the map output for stage 0 (it should have been there even before
    // the last round of completions from stage 1, but just to double check it hasn't been messed
    // up) and also the newly available stage 1
    val stageToReduceIdxs = Seq(
      0 -> (0 until 3),
      1 -> (0 until 1)
    )
    for {
      (stage, reduceIdxs) <- stageToReduceIdxs
      reduceIdx <- reduceIdxs
    } {
      // this would throw an exception if the map status hadn't been registered
      val statuses = mapOutputTracker.getMapSizesByExecutorId(stage, reduceIdx)
      // really we should have already thrown an exception rather than fail either of these
      // asserts, but just to be extra defensive let's double check the statuses are OK
      assert(statuses != null)
      assert(statuses.nonEmpty)
    }

    // and check that stage 2 has been submitted
    assert(taskSets.size == 5)
    val stage2TaskSet = taskSets(4)
    assert(stage2TaskSet.stageId == 2)
    assert(stage2TaskSet.stageAttemptId == 0)
  }

  /**
   * We lose an executor after completing some shuffle map tasks on it.  Those tasks get
   * resubmitted, and when they finish the job completes normally
   */
  test("register map outputs correctly after ExecutorLost and task Resubmitted") {
    val firstRDD = new MyRDD(sc, 3, Nil)
    val firstShuffleDep = new ShuffleDependency(firstRDD, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 5, List(firstShuffleDep))
    submit(reduceRdd, Array(0))

    // complete some of the tasks from the first stage, on one host
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.length)))
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.length)))

    // now that host goes down
    runEvent(ExecutorLost("exec-hostA", ExecutorKilled))

    // so we resubmit those tasks
    runEvent(makeCompletionEvent(taskSets(0).tasks(0), Resubmitted, null))
    runEvent(makeCompletionEvent(taskSets(0).tasks(1), Resubmitted, null))

    // now complete everything on a different host
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostB", reduceRdd.partitions.length)),
      (Success, makeMapStatus("hostB", reduceRdd.partitions.length)),
      (Success, makeMapStatus("hostB", reduceRdd.partitions.length))
    ))

    // now we should submit stage 1, and the map output from stage 0 should be registered

    // check that we have all the map output for stage 0
    (0 until reduceRdd.partitions.length).foreach { reduceIdx =>
      val statuses = mapOutputTracker.getMapSizesByExecutorId(0, reduceIdx)
      // really we should have already thrown an exception rather than fail either of these
      // asserts, but just to be extra defensive let's double check the statuses are OK
      assert(statuses != null)
      assert(statuses.nonEmpty)
    }

    // and check that stage 1 has been submitted
    assert(taskSets.size == 2)
    val stage1TaskSet = taskSets(1)
    assert(stage1TaskSet.stageId == 1)
    assert(stage1TaskSet.stageAttemptId == 0)
  }

  /**
   * Makes sure that failures of stage used by multiple jobs are correctly handled.
   *
   * This test creates the following dependency graph:
   *
   * shuffleMapRdd1     shuffleMapRDD2
   *        |     \        |
   *        |      \       |
   *        |       \      |
   *        |        \     |
   *   reduceRdd1    reduceRdd2
   *
   * We start both shuffleMapRdds and then fail shuffleMapRdd1.  As a result, the job listeners for
   * reduceRdd1 and reduceRdd2 should both be informed that the job failed.  shuffleMapRDD2 should
   * also be cancelled, because it is only used by reduceRdd2 and reduceRdd2 cannot complete
   * without shuffleMapRdd1.
   */
  test("failure of stage used by two jobs") {
    val shuffleMapRdd1 = new MyRDD(sc, 2, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(2))
    val shuffleMapRdd2 = new MyRDD(sc, 2, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(2))

    val reduceRdd1 = new MyRDD(sc, 2, List(shuffleDep1), tracker = mapOutputTracker)
    val reduceRdd2 = new MyRDD(sc, 2, List(shuffleDep1, shuffleDep2), tracker = mapOutputTracker)

    // We need to make our own listeners for this test, since by default submit uses the same
    // listener for all jobs, and here we want to capture the failure for each job separately.
    class FailureRecordingJobListener() extends JobListener {
      var failureMessage: String = _
      override def taskSucceeded(index: Int, result: Any) {}
      override def jobFailed(exception: Exception): Unit = { failureMessage = exception.getMessage }
    }
    val listener1 = new FailureRecordingJobListener()
    val listener2 = new FailureRecordingJobListener()

    submit(reduceRdd1, Array(0, 1), listener = listener1)
    submit(reduceRdd2, Array(0, 1), listener = listener2)

    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)

    assert(cancelledStages.toSet === Set(0, 2))

    // Make sure the listeners got told about both failed stages.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.successfulStages.isEmpty)
    assert(sparkListener.failedStages.toSet === Set(0, 2))

    assert(listener1.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assert(listener2.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assertDataStructuresEmpty()
  }

  def checkJobPropertiesAndPriority(taskSet: TaskSet, expected: String, priority: Int): Unit = {
    assert(taskSet.properties != null)
    assert(taskSet.properties.getProperty("testProperty") === expected)
    assert(taskSet.priority === priority)
  }

  def launchJobsThatShareStageAndCancelFirst(): ShuffleDependency[Int, Int, Nothing] = {
    val baseRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep1 = new ShuffleDependency(baseRdd, new HashPartitioner(1))
    val intermediateRdd = new MyRDD(sc, 1, List(shuffleDep1))
    val shuffleDep2 = new ShuffleDependency(intermediateRdd, new HashPartitioner(1))
    val finalRdd1 = new MyRDD(sc, 1, List(shuffleDep2))
    val finalRdd2 = new MyRDD(sc, 1, List(shuffleDep2))
    val job1Properties = new Properties()
    val job2Properties = new Properties()
    job1Properties.setProperty("testProperty", "job1")
    job2Properties.setProperty("testProperty", "job2")

    // Run jobs 1 & 2, both referencing the same stage, then cancel job1.
    // Note that we have to submit job2 before we cancel job1 to have them actually share
    // *Stages*, and not just shuffle dependencies, due to skipped stages (at least until
    // we address SPARK-10193.)
    val jobId1 = submit(finalRdd1, Array(0), properties = job1Properties)
    val jobId2 = submit(finalRdd2, Array(0), properties = job2Properties)
    assert(scheduler.activeJobs.nonEmpty)
    val testProperty1 = scheduler.jobIdToActiveJob(jobId1).properties.getProperty("testProperty")

    // remove job1 as an ActiveJob
    cancel(jobId1)

    // job2 should still be running
    assert(scheduler.activeJobs.nonEmpty)
    val testProperty2 = scheduler.jobIdToActiveJob(jobId2).properties.getProperty("testProperty")
    assert(testProperty1 != testProperty2)
    // NB: This next assert isn't necessarily the "desired" behavior; it's just to document
    // the current behavior.  We've already submitted the TaskSet for stage 0 based on job1, but
    // even though we have cancelled that job and are now running it because of job2, we haven't
    // updated the TaskSet's properties.  Changing the properties to "job2" is likely the more
    // correct behavior.
    val job1Id = 0  // TaskSet priority for Stages run with "job1" as the ActiveJob
    checkJobPropertiesAndPriority(taskSets(0), "job1", job1Id)
    complete(taskSets(0), Seq((Success, makeMapStatus("hostA", 1))))

    shuffleDep1
  }

  /**
   * Makes sure that tasks for a stage used by multiple jobs are submitted with the properties of a
   * later, active job if they were previously run under a job that is no longer active
   */
  test("stage used by two jobs, the first no longer active (SPARK-6880)") {
    launchJobsThatShareStageAndCancelFirst()

    // The next check is the key for SPARK-6880.  For the stage which was shared by both job1 and
    // job2 but never had any tasks submitted for job1, the properties of job2 are now used to run
    // the stage.
    checkJobPropertiesAndPriority(taskSets(1), "job2", 1)

    complete(taskSets(1), Seq((Success, makeMapStatus("hostA", 1))))
    assert(taskSets(2).properties != null)
    complete(taskSets(2), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assert(scheduler.activeJobs.isEmpty)

    assertDataStructuresEmpty()
  }

  /**
   * Makes sure that tasks for a stage used by multiple jobs are submitted with the properties of a
   * later, active job if they were previously run under a job that is no longer active, even when
   * there are fetch failures
   */
  test("stage used by two jobs, some fetch failures, and the first job no longer active " +
    "(SPARK-6880)") {
    val shuffleDep1 = launchJobsThatShareStageAndCancelFirst()
    val job2Id = 1  // TaskSet priority for Stages run with "job2" as the ActiveJob

    // lets say there is a fetch failure in this task set, which makes us go back and
    // run stage 0, attempt 1
    complete(taskSets(1), Seq(
      (FetchFailed(makeBlockManagerId("hostA"), shuffleDep1.shuffleId, 0, 0, "ignored"), null)))
    scheduler.resubmitFailedStages()

    // stage 0, attempt 1 should have the properties of job2
    assert(taskSets(2).stageId === 0)
    assert(taskSets(2).stageAttemptId === 1)
    checkJobPropertiesAndPriority(taskSets(2), "job2", job2Id)

    // run the rest of the stages normally, checking that they have the correct properties
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", 1))))
    checkJobPropertiesAndPriority(taskSets(3), "job2", job2Id)
    complete(taskSets(3), Seq((Success, makeMapStatus("hostA", 1))))
    checkJobPropertiesAndPriority(taskSets(4), "job2", job2Id)
    complete(taskSets(4), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assert(scheduler.activeJobs.isEmpty)

    assertDataStructuresEmpty()
  }

  /**
   * In this test, we run a map stage where one of the executors fails but we still receive a
   * "zombie" complete message from a task that ran on that executor. We want to make sure the
   * stage is resubmitted so that the task that ran on the failed executor is re-executed, and
   * that the stage is only marked as finished once that task completes.
   */
  test("run trivial shuffle with out-of-band executor failure and retry") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))
    // Tell the DAGScheduler that hostA was lost.
    runEvent(ExecutorLost("exec-hostA", ExecutorKilled))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 1)),
      (Success, makeMapStatus("hostB", 1))))

    // At this point, no more tasks are running for the stage (and the TaskSetManager considers the
    // stage complete), but the tasks that ran on HostA need to be re-run, so the DAGScheduler
    // should re-submit the stage with one task (the task that originally ran on HostA).
    assert(taskSets.size === 2)
    assert(taskSets(1).tasks.size === 1)

    // Make sure that the stage that was re-submitted was the ShuffleMapStage (not the reduce
    // stage, which shouldn't be run until all of the tasks in the ShuffleMapStage complete on
    // alive executors).
    assert(taskSets(1).tasks(0).isInstanceOf[ShuffleMapTask])

    // have hostC complete the resubmitted task
    complete(taskSets(1), Seq((Success, makeMapStatus("hostC", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))

    // Make sure that the reduce stage was now submitted.
    assert(taskSets.size === 3)
    assert(taskSets(2).tasks(0).isInstanceOf[ResultTask[_, _]])

    // Complete the reduce stage.
    complete(taskSets(2), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("recursive shuffle failures") {
    val shuffleOneRdd = new MyRDD(sc, 2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, new HashPartitioner(2))
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne), tracker = mapOutputTracker)
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, new HashPartitioner(1))
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo), tracker = mapOutputTracker)
    submit(finalRdd, Array(0))
    // have the first stage complete normally
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))
    // have the second stage complete normally
    complete(taskSets(1), Seq(
      (Success, makeMapStatus("hostA", 1)),
      (Success, makeMapStatus("hostC", 1))))
    // fail the third stage because hostA went down
    complete(taskSets(2), Seq(
      (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0, "ignored"), null)))
    // TODO assert this:
    // blockManagerMaster.removeExecutor("exec-hostA")
    // have DAGScheduler try again
    scheduler.resubmitFailedStages()
    complete(taskSets(3), Seq((Success, makeMapStatus("hostA", 2))))
    complete(taskSets(4), Seq((Success, makeMapStatus("hostA", 1))))
    complete(taskSets(5), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("cached post-shuffle") {
    val shuffleOneRdd = new MyRDD(sc, 2, Nil).cache()
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, new HashPartitioner(2))
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne), tracker = mapOutputTracker).cache()
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, new HashPartitioner(1))
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo), tracker = mapOutputTracker)
    submit(finalRdd, Array(0))
    cacheLocations(shuffleTwoRdd.id -> 0) = Seq(makeBlockManagerId("hostD"))
    cacheLocations(shuffleTwoRdd.id -> 1) = Seq(makeBlockManagerId("hostC"))
    // complete stage 0
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))
    // complete stage 1
    complete(taskSets(1), Seq(
      (Success, makeMapStatus("hostA", 1)),
      (Success, makeMapStatus("hostB", 1))))
    // pretend stage 2 failed because hostA went down
    complete(taskSets(2), Seq(
      (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0, "ignored"), null)))
    // TODO assert this:
    // blockManagerMaster.removeExecutor("exec-hostA")
    // DAGScheduler should notice the cached copy of the second shuffle and try to get it rerun.
    scheduler.resubmitFailedStages()
    assertLocations(taskSets(3), Seq(Seq("hostD")))
    // allow hostD to recover
    complete(taskSets(3), Seq((Success, makeMapStatus("hostD", 1))))
    complete(taskSets(4), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("misbehaved accumulator should not crash DAGScheduler and SparkContext") {
    val acc = new LongAccumulator {
      override def add(v: java.lang.Long): Unit = throw new DAGSchedulerSuiteDummyException
      override def add(v: Long): Unit = throw new DAGSchedulerSuiteDummyException
    }
    sc.register(acc)

    // Run this on executors
    sc.parallelize(1 to 10, 2).foreach { item => acc.add(1) }

    // Make sure we can still run commands
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("misbehaved accumulator should not impact other accumulators") {
    val bad = new LongAccumulator {
      override def merge(other: AccumulatorV2[java.lang.Long, java.lang.Long]): Unit = {
        throw new DAGSchedulerSuiteDummyException
      }
    }
    sc.register(bad, "bad")
    val good = sc.longAccumulator("good")

    sc.parallelize(1 to 10, 2).foreach { item =>
      bad.add(1)
      good.add(1)
    }

    // This is to ensure the `bad` accumulator did fail to update its value
    assert(bad.value == 0L)
    // Should be able to update the "good" accumulator
    assert(good.value == 10L)
  }

  /**
   * The job will be failed on first task throwing a DAGSchedulerSuiteDummyException.
   *  Any subsequent task WILL throw a legitimate java.lang.UnsupportedOperationException.
   *  If multiple tasks, there exists a race condition between the SparkDriverExecutionExceptions
   *  and their differing causes as to which will represent result for job...
   */
  test("misbehaved resultHandler should not crash DAGScheduler and SparkContext") {
    val e = intercept[SparkDriverExecutionException] {
      // Number of parallelized partitions implies number of tasks of job
      val rdd = sc.parallelize(1 to 10, 2)
      sc.runJob[Int, Int](
        rdd,
        (context: TaskContext, iter: Iterator[Int]) => iter.size,
        // For a robust test assertion, limit number of job tasks to 1; that is,
        // if multiple RDD partitions, use id of any one partition, say, first partition id=0
        Seq(0),
        (part: Int, result: Int) => throw new DAGSchedulerSuiteDummyException)
    }
    assert(e.getCause.isInstanceOf[DAGSchedulerSuiteDummyException])

    // Make sure we can still run commands on our SparkContext
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("getPartitions exceptions should not crash DAGScheduler and SparkContext (SPARK-8606)") {
    val e1 = intercept[DAGSchedulerSuiteDummyException] {
      val rdd = new MyRDD(sc, 2, Nil) {
        override def getPartitions: Array[Partition] = {
          throw new DAGSchedulerSuiteDummyException
        }
      }
      rdd.reduceByKey(_ + _, 1).count()
    }

    // Make sure we can still run commands
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("getPreferredLocations errors should not crash DAGScheduler and SparkContext (SPARK-8606)") {
    val e1 = intercept[SparkException] {
      val rdd = new MyRDD(sc, 2, Nil) {
        override def getPreferredLocations(split: Partition): Seq[String] = {
          throw new DAGSchedulerSuiteDummyException
        }
      }
      rdd.count()
    }
    assert(e1.getMessage.contains(classOf[DAGSchedulerSuiteDummyException].getName))

    // Make sure we can still run commands
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("accumulator not calculated for resubmitted result stage") {
    // just for register
    val accum = AccumulatorSuite.createLongAccum("a")
    val finalRdd = new MyRDD(sc, 1, Nil)
    submit(finalRdd, Array(0))
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))

    assert(accum.value === 1)
    assertDataStructuresEmpty()
  }

  test("accumulator not calculated for resubmitted task in result stage") {
    val accum = AccumulatorSuite.createLongAccum("a")
    val finalRdd = new MyRDD(sc, 2, Nil)
    submit(finalRdd, Array(0, 1))
    // finish the first task
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    // verify stage exists
    assert(scheduler.stageIdToStage.contains(0))

    // finish the first task again (simulate a speculative task or a resubmitted task)
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))

    // The accumulator should only be updated once.
    assert(accum.value === 1)

    runEvent(makeCompletionEvent(taskSets(0).tasks(1), Success, 42))
    assertDataStructuresEmpty()
  }

  test("accumulators are updated on exception failures and task killed") {
    val acc1 = AccumulatorSuite.createLongAccum("ingenieur")
    val acc2 = AccumulatorSuite.createLongAccum("boulanger")
    val acc3 = AccumulatorSuite.createLongAccum("agriculteur")
    assert(AccumulatorContext.get(acc1.id).isDefined)
    assert(AccumulatorContext.get(acc2.id).isDefined)
    assert(AccumulatorContext.get(acc3.id).isDefined)
    val accUpdate1 = new LongAccumulator
    accUpdate1.metadata = acc1.metadata
    accUpdate1.setValue(15)
    val accUpdate2 = new LongAccumulator
    accUpdate2.metadata = acc2.metadata
    accUpdate2.setValue(13)
    val accUpdate3 = new LongAccumulator
    accUpdate3.metadata = acc3.metadata
    accUpdate3.setValue(18)

    val accumUpdates1 = Seq(accUpdate1, accUpdate2)
    val accumInfo1 = accumUpdates1.map(AccumulatorSuite.makeInfo)
    val exceptionFailure = new ExceptionFailure(
      new SparkException("fondue?"),
      accumInfo1).copy(accums = accumUpdates1)
    submit(new MyRDD(sc, 1, Nil), Array(0))
    runEvent(makeCompletionEvent(taskSets.head.tasks.head, exceptionFailure, "result"))

    assert(AccumulatorContext.get(acc1.id).get.value === 15L)
    assert(AccumulatorContext.get(acc2.id).get.value === 13L)

    val accumUpdates2 = Seq(accUpdate3)
    val accumInfo2 = accumUpdates2.map(AccumulatorSuite.makeInfo)

    val taskKilled = new TaskKilled( "test", accumInfo2, accums = accumUpdates2)
    runEvent(makeCompletionEvent(taskSets.head.tasks.head, taskKilled, "result"))

    assert(AccumulatorContext.get(acc3.id).get.value === 18L)
  }

  test("reduce tasks should be placed locally with map output") {
    // Create a shuffleMapRdd with 1 partition
    val shuffleMapRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA")))

    // Reducer should run on the same host that map task ran
    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(Seq("hostA")))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("reduce task locality preferences should only include machines with largest map outputs") {
    val numMapTasks = 4
    // Create a shuffleMapRdd with more partitions
    val shuffleMapRdd = new MyRDD(sc, numMapTasks, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))

    val statuses = (1 to numMapTasks).map { i =>
      (Success, makeMapStatus("host" + i, 1, (10*i).toByte))
    }
    complete(taskSets(0), statuses)

    // Reducer should prefer the last 3 hosts as they have 20%, 30% and 40% of data
    val hosts = (1 to numMapTasks).map(i => "host" + i).reverse.take(numMapTasks - 1)

    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(hosts))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("stages with both narrow and shuffle dependencies use narrow ones for locality") {
    // Create an RDD that has both a shuffle dependency and a narrow dependency (e.g. for a join)
    val rdd1 = new MyRDD(sc, 1, Nil)
    val rdd2 = new MyRDD(sc, 1, Nil, locations = Seq(Seq("hostB")))
    val shuffleDep = new ShuffleDependency(rdd1, new HashPartitioner(1))
    val narrowDep = new OneToOneDependency(rdd2)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep, narrowDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA")))

    // Reducer should run where RDD 2 has preferences, even though it also has a shuffle dep
    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(Seq("hostB")))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("Spark exceptions should include call site in stack trace") {
    val e = intercept[SparkException] {
      sc.parallelize(1 to 10, 2).map { _ => throw new RuntimeException("uh-oh!") }.count()
    }

    // Does not include message, ONLY stack trace.
    val stackTraceString = Utils.exceptionString(e)

    // should actually include the RDD operation that invoked the method:
    assert(stackTraceString.contains("org.apache.spark.rdd.RDD.count"))

    // should include the FunSuite setup:
    assert(stackTraceString.contains("org.scalatest.FunSuite"))
  }

  test("catch errors in event loop") {
    // this is a test of our testing framework -- make sure errors in event loop don't get ignored

    // just run some bad event that will throw an exception -- we'll give a null TaskEndReason
    val rdd1 = new MyRDD(sc, 1, Nil)
    submit(rdd1, Array(0))
    intercept[Exception] {
      complete(taskSets(0), Seq(
        (null, makeMapStatus("hostA", 1))))
    }
  }

  test("simple map stage submission") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a map stage by itself
    submitMapStage(shuffleDep)
    assert(results.size === 0)  // No results yet
    completeShuffleMapStageSuccessfully(0, 0, 1)
    assert(results.size === 1)
    results.clear()
    assertDataStructuresEmpty()

    // Submit a reduce job that depends on this map stage; it should directly do the reduce
    submit(reduceRdd, Array(0))
    completeNextResultStageWithSuccess(2, 0)
    assert(results === Map(0 -> 42))
    results.clear()
    assertDataStructuresEmpty()

    // Check that if we submit the map stage again, no tasks run
    submitMapStage(shuffleDep)
    assert(results.size === 1)
    assertDataStructuresEmpty()
  }

  test("map stage submission with reduce stage also depending on the data") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)

    // Submit the map stage by itself
    submitMapStage(shuffleDep)

    // Submit a reduce job that depends on this map stage
    submit(reduceRdd, Array(0))

    // Complete tasks for the map stage
    completeShuffleMapStageSuccessfully(0, 0, 1)
    assert(results.size === 1)
    results.clear()

    // Complete tasks for the reduce stage
    completeNextResultStageWithSuccess(1, 0)
    assert(results === Map(0 -> 42))
    results.clear()
    assertDataStructuresEmpty()

    // Check that if we submit the map stage again, no tasks run
    submitMapStage(shuffleDep)
    assert(results.size === 1)
    assertDataStructuresEmpty()
  }

  test("map stage submission with fetch failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a map stage by itself
    submitMapStage(shuffleDep)
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", reduceRdd.partitions.length)),
      (Success, makeMapStatus("hostB", reduceRdd.partitions.length))))
    assert(results.size === 1)
    results.clear()
    assertDataStructuresEmpty()

    // Submit a reduce job that depends on this map stage, but where one reduce will fail a fetch
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"), null)))
    // Ask the scheduler to try it again; TaskSet 2 will rerun the map task that we couldn't fetch
    // from, then TaskSet 3 will run the reduce stage
    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", reduceRdd.partitions.length))))
    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    results.clear()
    assertDataStructuresEmpty()

    // Run another reduce job without a failure; this should just work
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(4), Seq(
      (Success, 44),
      (Success, 45)))
    assert(results === Map(0 -> 44, 1 -> 45))
    results.clear()
    assertDataStructuresEmpty()

    // Resubmit the map stage; this should also just work
    submitMapStage(shuffleDep)
    assert(results.size === 1)
    results.clear()
    assertDataStructuresEmpty()
  }

  /**
   * In this test, we have three RDDs with shuffle dependencies, and we submit map stage jobs
   * that are waiting on each one, as well as a reduce job on the last one. We test that all of
   * these jobs complete even if there are some fetch failures in both shuffles.
   */
  test("map stage submission with multiple shared stages and failures") {
    val rdd1 = new MyRDD(sc, 2, Nil)
    val dep1 = new ShuffleDependency(rdd1, new HashPartitioner(2))
    val rdd2 = new MyRDD(sc, 2, List(dep1), tracker = mapOutputTracker)
    val dep2 = new ShuffleDependency(rdd2, new HashPartitioner(2))
    val rdd3 = new MyRDD(sc, 2, List(dep2), tracker = mapOutputTracker)

    val listener1 = new SimpleListener
    val listener2 = new SimpleListener
    val listener3 = new SimpleListener

    submitMapStage(dep1, listener1)
    submitMapStage(dep2, listener2)
    submit(rdd3, Array(0, 1), listener = listener3)

    // Complete the first stage
    assert(taskSets(0).stageId === 0)
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", rdd1.partitions.length)),
      (Success, makeMapStatus("hostB", rdd1.partitions.length))))
    assert(mapOutputTracker.getMapSizesByExecutorId(dep1.shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    assert(listener1.results.size === 1)

    // When attempting the second stage, show a fetch failure
    assert(taskSets(1).stageId === 1)
    complete(taskSets(1), Seq(
      (Success, makeMapStatus("hostA", rdd2.partitions.length)),
      (FetchFailed(makeBlockManagerId("hostA"), dep1.shuffleId, 0, 0, "ignored"), null)))
    scheduler.resubmitFailedStages()
    assert(listener2.results.size === 0)    // Second stage listener should not have a result yet

    // Stage 0 should now be running as task set 2; make its task succeed
    assert(taskSets(2).stageId === 0)
    complete(taskSets(2), Seq(
      (Success, makeMapStatus("hostC", rdd2.partitions.length))))
    assert(mapOutputTracker.getMapSizesByExecutorId(dep1.shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))
    assert(listener2.results.size === 0)    // Second stage listener should still not have a result

    // Stage 1 should now be running as task set 3; make its first task succeed
    assert(taskSets(3).stageId === 1)
    complete(taskSets(3), Seq(
      (Success, makeMapStatus("hostB", rdd2.partitions.length)),
      (Success, makeMapStatus("hostD", rdd2.partitions.length))))
    assert(mapOutputTracker.getMapSizesByExecutorId(dep2.shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostB"), makeBlockManagerId("hostD")))
    assert(listener2.results.size === 1)

    // Finally, the reduce job should be running as task set 4; make it see a fetch failure,
    // then make it run again and succeed
    assert(taskSets(4).stageId === 2)
    complete(taskSets(4), Seq(
      (Success, 52),
      (FetchFailed(makeBlockManagerId("hostD"), dep2.shuffleId, 0, 0, "ignored"), null)))
    scheduler.resubmitFailedStages()

    // TaskSet 5 will rerun stage 1's lost task, then TaskSet 6 will rerun stage 2
    assert(taskSets(5).stageId === 1)
    complete(taskSets(5), Seq(
      (Success, makeMapStatus("hostE", rdd2.partitions.length))))
    complete(taskSets(6), Seq(
      (Success, 53)))
    assert(listener3.results === Map(0 -> 52, 1 -> 53))
    assertDataStructuresEmpty()
  }

  test("Trigger mapstage's job listener in submitMissingTasks") {
    val rdd1 = new MyRDD(sc, 2, Nil)
    val dep1 = new ShuffleDependency(rdd1, new HashPartitioner(2))
    val rdd2 = new MyRDD(sc, 2, List(dep1), tracker = mapOutputTracker)
    val dep2 = new ShuffleDependency(rdd2, new HashPartitioner(2))

    val listener1 = new SimpleListener
    val listener2 = new SimpleListener

    submitMapStage(dep1, listener1)
    submitMapStage(dep2, listener2)

    // Complete the stage0.
    assert(taskSets(0).stageId === 0)
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", rdd1.partitions.length)),
      (Success, makeMapStatus("hostB", rdd1.partitions.length))))
    assert(mapOutputTracker.getMapSizesByExecutorId(dep1.shuffleId, 0).map(_._1).toSet ===
        HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    assert(listener1.results.size === 1)

    // When attempting stage1, trigger a fetch failure.
    assert(taskSets(1).stageId === 1)
    complete(taskSets(1), Seq(
      (Success, makeMapStatus("hostC", rdd2.partitions.length)),
      (FetchFailed(makeBlockManagerId("hostA"), dep1.shuffleId, 0, 0, "ignored"), null)))
    scheduler.resubmitFailedStages()
    // Stage1 listener should not have a result yet
    assert(listener2.results.size === 0)

    // Speculative task succeeded in stage1.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1),
      Success,
      makeMapStatus("hostD", rdd2.partitions.length)))
    // stage1 listener still should not have a result, though there's no missing partitions
    // in it. Because stage1 has been failed and is not inside `runningStages` at this moment.
    assert(listener2.results.size === 0)

    // Stage0 should now be running as task set 2; make its task succeed
    assert(taskSets(2).stageId === 0)
    complete(taskSets(2), Seq(
      (Success, makeMapStatus("hostC", rdd2.partitions.length))))
    assert(mapOutputTracker.getMapSizesByExecutorId(dep1.shuffleId, 0).map(_._1).toSet ===
        Set(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))

    // After stage0 is finished, stage1 will be submitted and found there is no missing
    // partitions in it. Then listener got triggered.
    assert(listener2.results.size === 1)
    assertDataStructuresEmpty()
  }

  /**
   * In this test, we run a map stage where one of the executors fails but we still receive a
   * "zombie" complete message from that executor. We want to make sure the stage is not reported
   * as done until all tasks have completed.
   *
   * Most of the functionality in this test is tested in "run trivial shuffle with out-of-band
   * executor failure and retry".  However, that test uses ShuffleMapStages that are followed by
   * a ResultStage, whereas in this test, the ShuffleMapStage is tested in isolation, without a
   * ResultStage after it.
   */
  test("map stage submission with executor failure late map task completions") {
    val shuffleMapRdd = new MyRDD(sc, 3, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))

    submitMapStage(shuffleDep)

    val oldTaskSet = taskSets(0)
    runEvent(makeCompletionEvent(oldTaskSet.tasks(0), Success, makeMapStatus("hostA", 2)))
    assert(results.size === 0)    // Map stage job should not be complete yet

    // Pretend host A was lost. This will cause the TaskSetManager to resubmit task 0, because it
    // completed on hostA.
    val oldEpoch = mapOutputTracker.getEpoch
    runEvent(ExecutorLost("exec-hostA", ExecutorKilled))
    val newEpoch = mapOutputTracker.getEpoch
    assert(newEpoch > oldEpoch)

    // Suppose we also get a completed event from task 1 on the same host; this should be ignored
    runEvent(makeCompletionEvent(oldTaskSet.tasks(1), Success, makeMapStatus("hostA", 2)))
    assert(results.size === 0)    // Map stage job should not be complete yet

    // A completion from another task should work because it's a non-failed host
    runEvent(makeCompletionEvent(oldTaskSet.tasks(2), Success, makeMapStatus("hostB", 2)))

    // At this point, no more tasks are running for the stage (and the TaskSetManager considers
    // the stage complete), but the task that ran on hostA needs to be re-run, so the map stage
    // shouldn't be marked as complete, and the DAGScheduler should re-submit the stage.
    assert(results.size === 0)
    assert(taskSets.size === 2)

    // Now complete tasks in the second task set
    val newTaskSet = taskSets(1)
    // 2 tasks should have been re-submitted, for tasks 0 and 1 (which ran on hostA).
    assert(newTaskSet.tasks.size === 2)
    // Complete task 0 from the original task set (i.e., not hte one that's currently active).
    // This should still be counted towards the job being complete (but there's still one
    // outstanding task).
    runEvent(makeCompletionEvent(newTaskSet.tasks(0), Success, makeMapStatus("hostB", 2)))
    assert(results.size === 0)

    // Complete the final task, from the currently active task set.  There's still one
    // running task, task 0 in the currently active stage attempt, but the success of task 0 means
    // the DAGScheduler can mark the stage as finished.
    runEvent(makeCompletionEvent(newTaskSet.tasks(1), Success, makeMapStatus("hostB", 2)))
    assert(results.size === 1)    // Map stage job should now finally be complete
    assertDataStructuresEmpty()

    // Also test that a reduce stage using this shuffled data can immediately run
    val reduceRDD = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    results.clear()
    submit(reduceRDD, Array(0, 1))
    complete(taskSets(2), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    results.clear()
    assertDataStructuresEmpty()
  }

  /**
   * Checks the DAGScheduler's internal logic for traversing an RDD DAG by making sure that
   * getShuffleDependencies correctly returns the direct shuffle dependencies of a particular
   * RDD. The test creates the following RDD graph (where n denotes a narrow dependency and s
   * denotes a shuffle dependency):
   *
   * A <------------s---------,
   *                           \
   * B <--s-- C <--s-- D <--n---`-- E
   *
   * Here, the direct shuffle dependency of C is just the shuffle dependency on B. The direct
   * shuffle dependencies of E are the shuffle dependency on A and the shuffle dependency on C.
   */
  test("getShuffleDependencies correctly returns only direct shuffle parents") {
    val rddA = new MyRDD(sc, 2, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(1))
    val rddB = new MyRDD(sc, 2, Nil)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(1))
    val rddC = new MyRDD(sc, 1, List(shuffleDepB))
    val shuffleDepC = new ShuffleDependency(rddC, new HashPartitioner(1))
    val rddD = new MyRDD(sc, 1, List(shuffleDepC))
    val narrowDepD = new OneToOneDependency(rddD)
    val rddE = new MyRDD(sc, 1, List(shuffleDepA, narrowDepD), tracker = mapOutputTracker)

    assert(scheduler.getShuffleDependencies(rddA) === Set())
    assert(scheduler.getShuffleDependencies(rddB) === Set())
    assert(scheduler.getShuffleDependencies(rddC) === Set(shuffleDepB))
    assert(scheduler.getShuffleDependencies(rddD) === Set(shuffleDepC))
    assert(scheduler.getShuffleDependencies(rddE) === Set(shuffleDepA, shuffleDepC))
  }

  test("SPARK-17644: After one stage is aborted for too many failed attempts, subsequent stages" +
    "still behave correctly on fetch failures") {
    // Runs a job that always encounters a fetch failure, so should eventually be aborted
    def runJobWithPersistentFetchFailure: Unit = {
      val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 2).map(x => (x, 1)).groupByKey()
      val shuffleHandle =
        rdd1.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle
      rdd1.map {
        case (x, _) if (x == 1) =>
          throw new FetchFailedException(
            BlockManagerId("1", "1", 1), shuffleHandle.shuffleId, 0, 0, "test")
        case (x, _) => x
      }.count()
    }

    // Runs a job that encounters a single fetch failure but succeeds on the second attempt
    def runJobWithTemporaryFetchFailure: Unit = {
      val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 2).map(x => (x, 1)).groupByKey()
      val shuffleHandle =
        rdd1.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle
      rdd1.map {
        case (x, _) if (x == 1) && FailThisAttempt._fail.getAndSet(false) =>
          throw new FetchFailedException(
            BlockManagerId("1", "1", 1), shuffleHandle.shuffleId, 0, 0, "test")
      }
    }

    failAfter(10.seconds) {
      val e = intercept[SparkException] {
        runJobWithPersistentFetchFailure
      }
      assert(e.getMessage.contains("org.apache.spark.shuffle.FetchFailedException"))
    }

    // Run a second job that will fail due to a fetch failure.
    // This job will hang without the fix for SPARK-17644.
    failAfter(10.seconds) {
      val e = intercept[SparkException] {
        runJobWithPersistentFetchFailure
      }
      assert(e.getMessage.contains("org.apache.spark.shuffle.FetchFailedException"))
    }

    failAfter(10.seconds) {
      try {
        runJobWithTemporaryFetchFailure
      } catch {
        case e: Throwable => fail("A job with one fetch failure should eventually succeed")
      }
    }
  }

  test("[SPARK-19263] DAGScheduler should not submit multiple active tasksets," +
      " even with late completions from earlier stage attempts") {
    // Create 3 RDDs with shuffle dependencies on each other: rddA <--- rddB <--- rddC
    val rddA = new MyRDD(sc, 2, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(2))
    val shuffleIdA = shuffleDepA.shuffleId

    val rddB = new MyRDD(sc, 2, List(shuffleDepA), tracker = mapOutputTracker)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(2))

    val rddC = new MyRDD(sc, 2, List(shuffleDepB), tracker = mapOutputTracker)

    submit(rddC, Array(0, 1))

    // Complete both tasks in rddA.
    assert(taskSets(0).stageId === 0 && taskSets(0).stageAttemptId === 0)
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostA", 2))))

    // Fetch failed for task(stageId=1, stageAttemptId=0, partitionId=0) running on hostA
    // and task(stageId=1, stageAttemptId=0, partitionId=1) is still running.
    assert(taskSets(1).stageId === 1 && taskSets(1).stageAttemptId === 0)
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleIdA, 0, 0,
        "Fetch failure of task: stageId=1, stageAttempt=0, partitionId=0"),
      result = null))

    // Both original tasks in rddA should be marked as failed, because they ran on the
    // failed hostA, so both should be resubmitted. Complete them on hostB successfully.
    scheduler.resubmitFailedStages()
    assert(taskSets(2).stageId === 0 && taskSets(2).stageAttemptId === 1
      && taskSets(2).tasks.size === 2)
    complete(taskSets(2), Seq(
      (Success, makeMapStatus("hostB", 2)),
      (Success, makeMapStatus("hostB", 2))))

    // Complete task(stageId=1, stageAttemptId=0, partitionId=1) running on failed hostA
    // successfully. The success should be ignored because the task started before the
    // executor failed, so the output may have been lost.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1), Success, makeMapStatus("hostA", 2)))

    // task(stageId=1, stageAttemptId=1, partitionId=1) should be marked completed when
    // task(stageId=1, stageAttemptId=0, partitionId=1) finished
    // ideally we would verify that but no way to get into task scheduler to verify

    // Both tasks in rddB should be resubmitted, because none of them has succeeded truly.
    // Complete the task(stageId=1, stageAttemptId=1, partitionId=0) successfully.
    // Task(stageId=1, stageAttemptId=1, partitionId=1) of this new active stage attempt
    // is still running.
    assert(taskSets(3).stageId === 1 && taskSets(3).stageAttemptId === 1
      && taskSets(3).tasks.size === 2)
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(0), Success, makeMapStatus("hostB", 2)))

    // At this point there should be no active task set for stageId=1 and we need
    // to resubmit because the output from (stageId=1, stageAttemptId=0, partitionId=1)
    // was ignored due to executor failure
    assert(taskSets.size === 5)
    assert(taskSets(4).stageId === 1 && taskSets(4).stageAttemptId === 2
      && taskSets(4).tasks.size === 1)

    // Complete task(stageId=1, stageAttempt=2, partitionId=1) successfully.
    runEvent(makeCompletionEvent(
      taskSets(4).tasks(0), Success, makeMapStatus("hostB", 2)))

    // Now the ResultStage should be submitted, because all of the tasks of rddB have
    // completed successfully on alive executors.
    assert(taskSets.size === 6 && taskSets(5).tasks(0).isInstanceOf[ResultTask[_, _]])
    complete(taskSets(5), Seq(
      (Success, 1),
      (Success, 1)))
  }

  test("task end event should have updated accumulators (SPARK-20342)") {
    val tasks = 10

    val accumId = new AtomicLong()
    val foundCount = new AtomicLong()
    val listener = new SparkListener() {
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
        event.taskInfo.accumulables.find(_.id == accumId.get).foreach { _ =>
          foundCount.incrementAndGet()
        }
      }
    }
    sc.addSparkListener(listener)

    // Try a few times in a loop to make sure. This is not guaranteed to fail when the bug exists,
    // but it should at least make the test flaky. If the bug is fixed, this should always pass.
    (1 to 10).foreach { i =>
      foundCount.set(0L)

      val accum = sc.longAccumulator(s"accum$i")
      accumId.set(accum.id)

      sc.parallelize(1 to tasks, tasks).foreach { _ =>
        accum.add(1L)
      }
      sc.listenerBus.waitUntilEmpty(1000)
      assert(foundCount.get() === tasks)
    }
  }

  test("Barrier task failures from the same stage attempt don't trigger multiple stage retries") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    val mapStageId = 0
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == mapStageId)
    }

    // The map stage should have been submitted.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 1)

    // The first map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0),
      TaskKilled("test"),
      null))
    assert(sparkListener.failedStages === Seq(0))

    // The second map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      TaskKilled("test"),
      null))

    // Trigger resubmission of the failed map stage.
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    // Another attempt for the map stage should have been submitted, resulting in 2 total attempts.
    assert(countSubmittedMapStageAttempts() === 2)
  }

  test("Barrier task failures from a previous stage attempt don't trigger stage retry") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    val mapStageId = 0
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == mapStageId)
    }

    // The map stage should have been submitted.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 1)

    // The first map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0),
      TaskKilled("test"),
      null))
    assert(sparkListener.failedStages === Seq(0))

    // Trigger resubmission of the failed map stage.
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    // Another attempt for the map stage should have been submitted, resulting in 2 total attempts.
    assert(countSubmittedMapStageAttempts() === 2)

    // The second map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      TaskKilled("test"),
      null))

    // The second map task failure doesn't trigger stage retry.
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 2)
  }

  test("SPARK-23207: retry all the succeeding stages when the map stage is indeterminate") {
    val shuffleMapRdd1 = new MyRDD(sc, 2, Nil, indeterminate = true)

    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(2))
    val shuffleId1 = shuffleDep1.shuffleId
    val shuffleMapRdd2 = new MyRDD(sc, 2, List(shuffleDep1), tracker = mapOutputTracker)

    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(2))
    val shuffleId2 = shuffleDep2.shuffleId
    val finalRdd = new MyRDD(sc, 2, List(shuffleDep2), tracker = mapOutputTracker)

    submit(finalRdd, Array(0, 1))

    // Finish the first shuffle map stage.
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))
    assert(mapOutputTracker.findMissingPartitions(shuffleId1) === Some(Seq.empty))

    // Finish the second shuffle map stage.
    complete(taskSets(1), Seq(
      (Success, makeMapStatus("hostC", 2)),
      (Success, makeMapStatus("hostD", 2))))
    assert(mapOutputTracker.findMissingPartitions(shuffleId2) === Some(Seq.empty))

    // The first task of the final stage failed with fetch failure
    runEvent(makeCompletionEvent(
      taskSets(2).tasks(0),
      FetchFailed(makeBlockManagerId("hostC"), shuffleId2, 0, 0, "ignored"),
      null))

    val failedStages = scheduler.failedStages.toSeq
    assert(failedStages.length == 2)
    // Shuffle blocks of "hostC" is lost, so first task of the `shuffleMapRdd2` needs to retry.
    assert(failedStages.collect {
      case stage: ShuffleMapStage if stage.shuffleDep.shuffleId == shuffleId2 => stage
    }.head.findMissingPartitions() == Seq(0))
    // The result stage is still waiting for its 2 tasks to complete
    assert(failedStages.collect {
      case stage: ResultStage => stage
    }.head.findMissingPartitions() == Seq(0, 1))

    scheduler.resubmitFailedStages()

    // The first task of the `shuffleMapRdd2` failed with fetch failure
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId1, 0, 0, "ignored"),
      null))

    // The job should fail because Spark can't rollback the shuffle map stage.
    assert(failure != null && failure.getMessage.contains("Spark cannot rollback"))
  }

  private def assertResultStageFailToRollback(mapRdd: MyRDD): Unit = {
    val shuffleDep = new ShuffleDependency(mapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val finalRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)

    submit(finalRdd, Array(0, 1))

    completeShuffleMapStageSuccessfully(taskSets.length - 1, 0, numShufflePartitions = 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))

    // Finish the first task of the result stage
    runEvent(makeCompletionEvent(
      taskSets.last.tasks(0), Success, 42,
      Seq.empty, createFakeTaskInfoWithId(0)))

    // Fail the second task with FetchFailed.
    runEvent(makeCompletionEvent(
      taskSets.last.tasks(1),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null))

    // The job should fail because Spark can't rollback the result stage.
    assert(failure != null && failure.getMessage.contains("Spark cannot rollback"))
  }

  test("SPARK-23207: cannot rollback a result stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil, indeterminate = true)
    assertResultStageFailToRollback(shuffleMapRdd)
  }

  test("SPARK-23207: local checkpoint fail to rollback (checkpointed before)") {
    val shuffleMapRdd = new MyCheckpointRDD(sc, 2, Nil, indeterminate = true)
    shuffleMapRdd.localCheckpoint()
    shuffleMapRdd.doCheckpoint()
    assertResultStageFailToRollback(shuffleMapRdd)
  }

  test("SPARK-23207: local checkpoint fail to rollback (checkpointing now)") {
    val shuffleMapRdd = new MyCheckpointRDD(sc, 2, Nil, indeterminate = true)
    shuffleMapRdd.localCheckpoint()
    assertResultStageFailToRollback(shuffleMapRdd)
  }

  private def assertResultStageNotRollbacked(mapRdd: MyRDD): Unit = {
    val shuffleDep = new ShuffleDependency(mapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val finalRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)

    submit(finalRdd, Array(0, 1))

    completeShuffleMapStageSuccessfully(taskSets.length - 1, 0, numShufflePartitions = 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))

    // Finish the first task of the result stage
    runEvent(makeCompletionEvent(
      taskSets.last.tasks(0), Success, 42,
      Seq.empty, createFakeTaskInfoWithId(0)))

    // Fail the second task with FetchFailed.
    runEvent(makeCompletionEvent(
      taskSets.last.tasks(1),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null))

    assert(failure == null, "job should not fail")
    val failedStages = scheduler.failedStages.toSeq
    assert(failedStages.length == 2)
    // Shuffle blocks of "hostA" is lost, so first task of the `shuffleMapRdd2` needs to retry.
    assert(failedStages.collect {
      case stage: ShuffleMapStage if stage.shuffleDep.shuffleId == shuffleId => stage
    }.head.findMissingPartitions() == Seq(0))
    // The first task of result stage remains completed.
    assert(failedStages.collect {
      case stage: ResultStage => stage
    }.head.findMissingPartitions() == Seq(1))
  }

  test("SPARK-23207: reliable checkpoint can avoid rollback (checkpointed before)") {
    sc.setCheckpointDir(Utils.createTempDir().getCanonicalPath)
    val shuffleMapRdd = new MyCheckpointRDD(sc, 2, Nil, indeterminate = true)
    shuffleMapRdd.checkpoint()
    shuffleMapRdd.doCheckpoint()
    assertResultStageNotRollbacked(shuffleMapRdd)
  }

  test("SPARK-23207: reliable checkpoint fail to rollback (checkpointing now)") {
    sc.setCheckpointDir(Utils.createTempDir().getCanonicalPath)
    val shuffleMapRdd = new MyCheckpointRDD(sc, 2, Nil, indeterminate = true)
    shuffleMapRdd.checkpoint()
    assertResultStageFailToRollback(shuffleMapRdd)
  }

  /**
   * Assert that the supplied TaskSet has exactly the given hosts as its preferred locations.
   * Note that this checks only the host and not the executor ID.
   */
  private def assertLocations(taskSet: TaskSet, hosts: Seq[Seq[String]]) {
    assert(hosts.size === taskSet.tasks.size)
    for ((taskLocs, expectedLocs) <- taskSet.tasks.map(_.preferredLocations).zip(hosts)) {
      assert(taskLocs.map(_.host).toSet === expectedLocs.toSet)
    }
  }

  private def assertDataStructuresEmpty(): Unit = {
    assert(scheduler.activeJobs.isEmpty)
    assert(scheduler.failedStages.isEmpty)
    assert(scheduler.jobIdToActiveJob.isEmpty)
    assert(scheduler.jobIdToStageIds.isEmpty)
    assert(scheduler.stageIdToStage.isEmpty)
    assert(scheduler.runningStages.isEmpty)
    assert(scheduler.shuffleIdToMapStage.isEmpty)
    assert(scheduler.waitingStages.isEmpty)
    assert(scheduler.outputCommitCoordinator.isEmpty)
  }

  // Nothing in this test should break if the task info's fields are null, but
  // OutputCommitCoordinator requires the task info itself to not be null.
  private def createFakeTaskInfo(): TaskInfo = {
    val info = new TaskInfo(0, 0, 0, 0L, "", "", TaskLocality.ANY, false)
    info.finishTime = 1
    info
  }

  private def createFakeTaskInfoWithId(taskId: Long): TaskInfo = {
    val info = new TaskInfo(taskId, 0, 0, 0L, "", "", TaskLocality.ANY, false)
    info.finishTime = 1
    info
  }

  private def makeCompletionEvent(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      extraAccumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty,
      taskInfo: TaskInfo = createFakeTaskInfo()): CompletionEvent = {
    val accumUpdates = reason match {
      case Success => task.metrics.accumulators()
      case ef: ExceptionFailure => ef.accums
      case tk: TaskKilled => tk.accums
      case _ => Seq.empty
    }
    CompletionEvent(task, reason, result, accumUpdates ++ extraAccumUpdates, taskInfo)
  }
}

object DAGSchedulerSuite {
  def makeMapStatus(host: String, reduces: Int, sizes: Byte = 2): MapStatus =
    MapStatus(makeBlockManagerId(host), Array.fill[Long](reduces)(sizes))

  def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)
}

object FailThisAttempt {
  val _fail = new AtomicBoolean(true)
}
