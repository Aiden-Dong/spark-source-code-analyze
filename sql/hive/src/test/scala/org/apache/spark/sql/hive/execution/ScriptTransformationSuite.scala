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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, TaskContext, TestUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest, UnaryExecNode}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types.StringType

class ScriptTransformationSuite extends SparkPlanTest with TestHiveSingleton {
  import spark.implicits._

  private val noSerdeIOSchema = HiveScriptIOSchema(
    inputRowFormat = Seq.empty,
    outputRowFormat = Seq.empty,
    inputSerdeClass = None,
    outputSerdeClass = None,
    inputSerdeProps = Seq.empty,
    outputSerdeProps = Seq.empty,
    recordReaderClass = None,
    recordWriterClass = None,
    schemaLess = false
  )

  private val serdeIOSchema = noSerdeIOSchema.copy(
    inputSerdeClass = Some(classOf[LazySimpleSerDe].getCanonicalName),
    outputSerdeClass = Some(classOf[LazySimpleSerDe].getCanonicalName)
  )

  test("cat without SerDe") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    checkAnswer(
      rowsDf,
      (child: SparkPlan) => new ScriptTransformationExec(
        input = Seq(rowsDf.col("a").expr),
        script = "cat",
        output = Seq(AttributeReference("a", StringType)()),
        child = child,
        ioschema = noSerdeIOSchema
      ),
      rowsDf.collect())
  }

  test("cat with LazySimpleSerDe") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    checkAnswer(
      rowsDf,
      (child: SparkPlan) => new ScriptTransformationExec(
        input = Seq(rowsDf.col("a").expr),
        script = "cat",
        output = Seq(AttributeReference("a", StringType)()),
        child = child,
        ioschema = serdeIOSchema
      ),
      rowsDf.collect())
  }

  test("script transformation should not swallow errors from upstream operators (no serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    val e = intercept[TestFailedException] {
      checkAnswer(
        rowsDf,
        (child: SparkPlan) => new ScriptTransformationExec(
          input = Seq(rowsDf.col("a").expr),
          script = "cat",
          output = Seq(AttributeReference("a", StringType)()),
          child = ExceptionInjectingOperator(child),
          ioschema = noSerdeIOSchema
        ),
        rowsDf.collect())
    }
    assert(e.getMessage().contains("intentional exception"))
  }

  test("script transformation should not swallow errors from upstream operators (with serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    val e = intercept[TestFailedException] {
      checkAnswer(
        rowsDf,
        (child: SparkPlan) => new ScriptTransformationExec(
          input = Seq(rowsDf.col("a").expr),
          script = "cat",
          output = Seq(AttributeReference("a", StringType)()),
          child = ExceptionInjectingOperator(child),
          ioschema = serdeIOSchema
        ),
        rowsDf.collect())
    }
    assert(e.getMessage().contains("intentional exception"))
  }

  test("SPARK-14400 script transformation should fail for bad script command") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")

    val e = intercept[SparkException] {
      val plan =
        new ScriptTransformationExec(
          input = Seq(rowsDf.col("a").expr),
          script = "some_non_existent_command",
          output = Seq(AttributeReference("a", StringType)()),
          child = rowsDf.queryExecution.sparkPlan,
          ioschema = serdeIOSchema)
      SparkPlanTest.executePlan(plan, hiveContext)
    }
    assert(e.getMessage.contains("Subprocess exited with status"))
  }

  test("SPARK-24339 verify the result after pruning the unused columns") {
    val rowsDf = Seq(
      ("Bob", 16, 176),
      ("Alice", 32, 164),
      ("David", 60, 192),
      ("Amy", 24, 180)).toDF("name", "age", "height")

    checkAnswer(
      rowsDf,
      (child: SparkPlan) => new ScriptTransformationExec(
        input = Seq(rowsDf.col("name").expr),
        script = "cat",
        output = Seq(AttributeReference("name", StringType)()),
        child = child,
        ioschema = serdeIOSchema
      ),
      rowsDf.select("name").collect())
  }
}

private case class ExceptionInjectingOperator(child: SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().map { x =>
      assert(TaskContext.get() != null) // Make sure that TaskContext is defined.
      Thread.sleep(1000) // This sleep gives the external process time to start.
      throw new IllegalArgumentException("intentional exception")
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}
