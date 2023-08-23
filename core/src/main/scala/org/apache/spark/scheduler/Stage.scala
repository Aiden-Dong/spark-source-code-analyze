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

import scala.collection.mutable.HashSet

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * stage 是一组并行task，这些task都有计算相同的函数(位于RDD中)，并且作为 Spark Job的一部分运行。
 * 这些task具有相同的shuffle依赖关系。
 * DAGSheduler 按照shuffle的边界将RDD图拆分成若干Stage，然后 DAGScheduler 按照 拓扑顺序运行这些Stage。
 *
 * Stage 的实现有两种 :
 * 1. ShuffleMapStage: 它的结果数据作为其他Stage的输入。
 * 2. ResultStage: 它的task通过在RDD上运行函数直接计算（例如 count()、save() 等）。
 * 对于ShuffleMapStage，我们还会追踪每个输出分区所在的节点。
 *
 * 每个stage还有一个firstJobId，用于标识首次提交该阶段的作业。
 * 当使用 FIFO 调度时，这允许较早作业的阶段被优先计算或在失败后更快地恢复。
 *
 * 最后，由于故障恢复的需要，单个stage可以重试多次。
 * 在这种情况下，stage对象将跟踪多个 StageInfo 对象，以便传递给Listener或 Web UI。
 * 最新的 StageInfo 可通过 latestInfo 访问。
 *
 * @param id StageId , 唯一表示一个Stage
 * @param rdd
 *   该stage运行的RDD：对于ShuffleMapStage，它是我们在上面运行shuffle操作以前的map task RDD
 *   而对于ResultStage，它是我们在其中运行了一个操作的目标RDD。
 * @param numTasks stage中的总task数；特别是对于ResultStage，可能不需要计算所有partition，例如用于first()、lookup() 和 take() 等操作。
 * @param parents 该Stage依赖的上游Stage(通过 ShuffleDependency 依赖)
 * @param firstJobId 当前stage所属的JobId.
 * @param callSite  与该stage相关联的用户程序位置：对于ShuffleMapStage，是目标RDD的创建位置；对于ResultStage，是调用操作的位置。
 */
private[scheduler] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite)
  extends Logging {

  val numPartitions = rdd.partitions.length

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]

  /** The ID to use for the next new attempt for this stage. */
  private var nextAttemptId: Int = 0

  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  /**
   * Pointer to the [[StageInfo]] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   */
  private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

  /**
   * Set of stage attempt IDs that have failed. We keep track of these failures in order to avoid
   * endless retries if a stage keeps failing.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   */
  val failedAttemptIds = new HashSet[Int]

  private[scheduler] def clearFailures() : Unit = {
    failedAttemptIds.clear()
  }

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences)
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  def findMissingPartitions(): Seq[Int]
}
