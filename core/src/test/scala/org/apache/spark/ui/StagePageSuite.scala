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

package org.apache.spark.ui

import java.util.Locale
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{AccumulableInfo => UIAccumulableInfo, StageData, StageStatus}
import org.apache.spark.status.config._
import org.apache.spark.ui.jobs.{ApiHelper, StagePage, StagesTab, TaskPagedTable}

class StagePageSuite extends SparkFunSuite with LocalSparkContext {

  private val peakExecutionMemory = 10

  test("ApiHelper.COLUMN_TO_INDEX should match headers of the task table") {
    val conf = new SparkConf(false).set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
    val statusStore = AppStatusStore.createLiveStore(conf)
    try {
      val stageData = new StageData(
        status = StageStatus.ACTIVE,
        stageId = 1,
        attemptId = 1,
        numTasks = 1,
        numActiveTasks = 1,
        numCompleteTasks = 1,
        numFailedTasks = 1,
        numKilledTasks = 1,
        numCompletedIndices = 1,

        executorRunTime = 1L,
        executorCpuTime = 1L,
        submissionTime = None,
        firstTaskLaunchedTime = None,
        completionTime = None,
        failureReason = None,

        inputBytes = 1L,
        inputRecords = 1L,
        outputBytes = 1L,
        outputRecords = 1L,
        shuffleReadBytes = 1L,
        shuffleReadRecords = 1L,
        shuffleWriteBytes = 1L,
        shuffleWriteRecords = 1L,
        memoryBytesSpilled = 1L,
        diskBytesSpilled = 1L,

        name = "stage1",
        description = Some("description"),
        details = "detail",
        schedulingPool = "pool1",

        rddIds = Seq(1),
        accumulatorUpdates = Seq(new UIAccumulableInfo(0L, "acc", None, "value")),
        tasks = None,
        executorSummary = None,
        killedTasksSummary = Map.empty
      )
      val taskTable = new TaskPagedTable(
        stageData,
        basePath = "/a/b/c",
        currentTime = 0,
        pageSize = 10,
        sortColumn = "Index",
        desc = false,
        store = statusStore
      )
      val columnNames = (taskTable.headers \ "th" \ "a").map(_.child(1).text).toSet
      assert(columnNames === ApiHelper.COLUMN_TO_INDEX.keySet)
    } finally {
      statusStore.close()
    }
  }

  test("peak execution memory should displayed") {
    val html = renderStagePage().toString().toLowerCase(Locale.ROOT)
    val targetString = "peak execution memory"
    assert(html.contains(targetString))
  }

  test("SPARK-10543: peak execution memory should be per-task rather than cumulative") {
    val html = renderStagePage().toString().toLowerCase(Locale.ROOT)
    // verify min/25/50/75/max show task value not cumulative values
    assert(html.contains(s"<td>$peakExecutionMemory.0 b</td>" * 5))
  }

  /**
   * Render a stage page started with the given conf and return the HTML.
   * This also runs a dummy stage to populate the page with useful content.
   */
  private def renderStagePage(): Seq[Node] = {
    val conf = new SparkConf(false).set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
    val statusStore = AppStatusStore.createLiveStore(conf)
    val listener = statusStore.listener.get

    try {
      val tab = mock(classOf[StagesTab], RETURNS_SMART_NULLS)
      when(tab.store).thenReturn(statusStore)

      val request = mock(classOf[HttpServletRequest])
      when(tab.conf).thenReturn(conf)
      when(tab.appName).thenReturn("testing")
      when(tab.headerTabs).thenReturn(Seq.empty)
      when(request.getParameter("id")).thenReturn("0")
      when(request.getParameter("attempt")).thenReturn("0")
      val page = new StagePage(tab, statusStore)

      // Simulate a stage in job progress listener
      val stageInfo = new StageInfo(0, 0, "dummy", 1, Seq.empty, Seq.empty, "details")
      // Simulate two tasks to test PEAK_EXECUTION_MEMORY correctness
      (1 to 2).foreach {
        taskId =>
          val taskInfo = new TaskInfo(taskId, taskId, 0, 0, "0", "localhost", TaskLocality.ANY,
            false)
          listener.onStageSubmitted(SparkListenerStageSubmitted(stageInfo))
          listener.onTaskStart(SparkListenerTaskStart(0, 0, taskInfo))
          taskInfo.markFinished(TaskState.FINISHED, System.currentTimeMillis())
          val taskMetrics = TaskMetrics.empty
          taskMetrics.incPeakExecutionMemory(peakExecutionMemory)
          listener.onTaskEnd(SparkListenerTaskEnd(0, 0, "result", Success, taskInfo, taskMetrics))
      }
      listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
      page.render(request)
    } finally {
      statusStore.close()
    }
  }

}
