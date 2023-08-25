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

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages 是执行 DAG 中产生用于Shuffle数据的中间阶段。
 * 它们恰好出现在每次 shuffle 操作之前，可能在此之前包含多个流水线操作（例如map和filter）。
* 在执行时，它们保存映射输出文件，稍后可以由 reduce 任务获取。shuffleDep 字段描述了每个阶段所属的洗牌操作，而诸如 outputLocs 和 numAvailableOutputs 之类的变量跟踪有多少映射输出已准备就绪。
ShuffleMapStages 也可以作为作业通过 DAGScheduler.submitMapStage 独立提交。对于这种阶段，提交它们的 ActiveJobs 在 mapStageJobs 中被跟踪。请注意，可能会有多个 ActiveJobs 尝试计算相同的洗牌映射阶段。
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 */
private[spark] class ShuffleMapStage(
    id: Int,                // 表示当前 Stage 的 ID
    rdd: RDD[_],            // 当前Shuffle操作的依赖RDD
    numTasks: Int,          // 当前 Stage 的Task数量，为目标RDD的分区数
    parents: List[Stage],   // 当前Stage 上游依赖的Stage
    firstJobId: Int,        // 当前作业ID
    callSite: CallSite,     //
    val shuffleDep: ShuffleDependency[_, _, _],   // 当前依赖方式
    mapOutputTrackerMaster: MapOutputTrackerMaster)
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /**
   * Partitions that either haven't yet been computed, or that were computed on an executor
   * that has since been lost, so should be re-computed.  This variable is used by the
   * DAGScheduler to determine when a stage has completed. Task successes in both the active
   * attempt for the stage or in earlier attempts for this stage can cause paritition ids to get
   * removed from pendingPartitions. As a result, this variable may be inconsistent with the pending
   * tasks in the TaskSetManager for the active attempt for the stage (the partitions stored here
   * will always be a subset of the partitions that the TaskSetManager thinks are pending).
   */
  val pendingPartitions = new HashSet[Int]

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   */
  def numAvailableOutputs: Int = mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  override def findMissingPartitions(): Seq[Int] = {
    mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
  }
}
