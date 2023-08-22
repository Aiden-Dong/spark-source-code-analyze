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

package org.apache.spark.sql.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.sql.{functions, AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.execution.{QueryExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources.{CreateTable, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameCallbackSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  import functions._

  test("execute callback functions when a DataFrame action finished successfully") {
    val metrics = ArrayBuffer.empty[(String, QueryExecution, Long)]
    val listener = new QueryExecutionListener {
      // Only test successful case here, so no need to implement `onFailure`
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        metrics += ((funcName, qe, duration))
      }
    }
    spark.listenerManager.register(listener)

    val df = Seq(1 -> "a").toDF("i", "j")
    df.select("i").collect()
    df.filter($"i" > 0).count()

    assert(metrics.length == 2)

    assert(metrics(0)._1 == "collect")
    assert(metrics(0)._2.analyzed.isInstanceOf[Project])
    assert(metrics(0)._3 > 0)

    assert(metrics(1)._1 == "count")
    assert(metrics(1)._2.analyzed.isInstanceOf[Aggregate])
    assert(metrics(1)._3 > 0)

    spark.listenerManager.unregister(listener)
  }

  testQuietly("execute callback functions when a DataFrame action failed") {
    val metrics = ArrayBuffer.empty[(String, QueryExecution, Exception)]
    val listener = new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        metrics += ((funcName, qe, exception))
      }

      // Only test failed case here, so no need to implement `onSuccess`
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {}
    }
    spark.listenerManager.register(listener)

    val errorUdf = udf[Int, Int] { _ => throw new RuntimeException("udf error") }
    val df = sparkContext.makeRDD(Seq(1 -> "a")).toDF("i", "j")

    val e = intercept[SparkException](df.select(errorUdf($"i")).collect())

    assert(metrics.length == 1)
    assert(metrics(0)._1 == "collect")
    assert(metrics(0)._2.analyzed.isInstanceOf[Project])
    assert(metrics(0)._3.getMessage == e.getMessage)

    spark.listenerManager.unregister(listener)
  }

  test("get numRows metrics by callback") {
    val metrics = ArrayBuffer.empty[Long]
    val listener = new QueryExecutionListener {
      // Only test successful case here, so no need to implement `onFailure`
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        val metric = qe.executedPlan match {
          case w: WholeStageCodegenExec => w.child.longMetric("numOutputRows")
          case other => other.longMetric("numOutputRows")
        }
        metrics += metric.value
      }
    }
    spark.listenerManager.register(listener)

    val df = Seq(1 -> "a").toDF("i", "j").groupBy("i").count()
    df.collect()
    df.collect()
    Seq(1 -> "a", 2 -> "a").toDF("i", "j").groupBy("i").count().collect()

    assert(metrics.length == 3)
    assert(metrics(0) === 1)
    assert(metrics(1) === 1)
    assert(metrics(2) === 2)

    spark.listenerManager.unregister(listener)
  }

  // TODO: Currently some LongSQLMetric use -1 as initial value, so if the accumulator is never
  // updated, we can filter it out later.  However, when we aggregate(sum) accumulator values at
  // driver side for SQL physical operators, these -1 values will make our result smaller.
  // A easy fix is to create a new SQLMetric(including new MetricValue, MetricParam, etc.), but we
  // can do it later because the impact is just too small (1048576 tasks for 1 MB).
  ignore("get size metrics by callback") {
    val metrics = ArrayBuffer.empty[Long]
    val listener = new QueryExecutionListener {
      // Only test successful case here, so no need to implement `onFailure`
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        metrics += qe.executedPlan.longMetric("dataSize").value
        val bottomAgg = qe.executedPlan.children(0).children(0)
        metrics += bottomAgg.longMetric("dataSize").value
      }
    }
    spark.listenerManager.register(listener)

    val sparkListener = new SaveInfoListener
    spark.sparkContext.addSparkListener(sparkListener)

    val df = (1 to 100).map(i => i -> i.toString).toDF("i", "j")
    df.groupBy("i").count().collect()

    def getPeakExecutionMemory(stageId: Int): Long = {
      val peakMemoryAccumulator = sparkListener.getCompletedStageInfos(stageId).accumulables
        .filter(_._2.name == InternalAccumulator.PEAK_EXECUTION_MEMORY)

      assert(peakMemoryAccumulator.size == 1)
      peakMemoryAccumulator.head._2.value.get.asInstanceOf[Long]
    }

    assert(sparkListener.getCompletedStageInfos.length == 2)
    val bottomAggDataSize = getPeakExecutionMemory(0)
    val topAggDataSize = getPeakExecutionMemory(1)

    // For this simple case, the peakExecutionMemory of a stage should be the data size of the
    // aggregate operator, as we only have one memory consuming operator per stage.
    assert(metrics.length == 2)
    assert(metrics(0) == topAggDataSize)
    assert(metrics(1) == bottomAggDataSize)

    spark.listenerManager.unregister(listener)
  }

  test("execute callback functions for DataFrameWriter") {
    val commands = ArrayBuffer.empty[(String, LogicalPlan)]
    val exceptions = ArrayBuffer.empty[(String, Exception)]
    val listener = new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        exceptions += funcName -> exception
      }

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        commands += funcName -> qe.logical
      }
    }
    spark.listenerManager.register(listener)

    withTempPath { path =>
      spark.range(10).write.format("json").save(path.getCanonicalPath)
      assert(commands.length == 1)
      assert(commands.head._1 == "save")
      assert(commands.head._2.isInstanceOf[InsertIntoHadoopFsRelationCommand])
      assert(commands.head._2.asInstanceOf[InsertIntoHadoopFsRelationCommand]
        .fileFormat.isInstanceOf[JsonFileFormat])
    }

    withTable("tab") {
      sql("CREATE TABLE tab(i long) using parquet") // adds commands(1) via onSuccess
      spark.range(10).write.insertInto("tab")
      assert(commands.length == 3)
      assert(commands(2)._1 == "insertInto")
      assert(commands(2)._2.isInstanceOf[InsertIntoTable])
      assert(commands(2)._2.asInstanceOf[InsertIntoTable].table
        .asInstanceOf[UnresolvedRelation].tableIdentifier.table == "tab")
    }
    // exiting withTable adds commands(3) via onSuccess (drops tab)

    withTable("tab") {
      spark.range(10).select($"id", $"id" % 5 as "p").write.partitionBy("p").saveAsTable("tab")
      assert(commands.length == 5)
      assert(commands(4)._1 == "saveAsTable")
      assert(commands(4)._2.isInstanceOf[CreateTable])
      assert(commands(4)._2.asInstanceOf[CreateTable].tableDesc.partitionColumnNames == Seq("p"))
    }

    withTable("tab") {
      sql("CREATE TABLE tab(i long) using parquet")
      val e = intercept[AnalysisException] {
        spark.range(10).select($"id", $"id").write.insertInto("tab")
      }
      assert(exceptions.length == 1)
      assert(exceptions.head._1 == "insertInto")
      assert(exceptions.head._2 == e)
    }
  }
}
