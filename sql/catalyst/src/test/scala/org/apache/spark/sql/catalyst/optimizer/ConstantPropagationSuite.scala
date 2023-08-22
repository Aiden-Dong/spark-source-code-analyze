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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

/**
 * Unit tests for constant propagation in expressions.
 */
class ConstantPropagationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
        Batch("ConstantPropagation", FixedPoint(10),
          ConstantPropagation,
          ConstantFolding,
          BooleanSimplification) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  private val columnA = 'a
  private val columnB = 'b
  private val columnC = 'c

  test("basic test") {
    val query = testRelation
      .select(columnA)
      .where(columnA === Add(columnB, Literal(1)) && columnB === Literal(10))

    val correctAnswer =
      testRelation
        .select(columnA)
        .where(columnA === Literal(11) && columnB === Literal(10)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("with combination of AND and OR predicates") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Add(columnB, Literal(1)) &&
          columnB === Literal(10) &&
          (columnA === Add(columnC, Literal(3)) || columnB === columnC))
      .analyze

    val correctAnswer =
      testRelation
        .select(columnA)
        .where(
          columnA === Literal(11) &&
            columnB === Literal(10) &&
            (Literal(11) === Add(columnC, Literal(3)) || Literal(10) === columnC))
        .analyze

    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("equality predicates outside a `NOT` can be propagated within a `NOT`") {
    val query = testRelation
      .select(columnA)
      .where(Not(columnA === Add(columnB, Literal(1))) && columnB === Literal(10))
      .analyze

    val correctAnswer =
      testRelation
        .select(columnA)
        .where(Not(columnA === Literal(11)) && columnB === Literal(10))
        .analyze

    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("equality predicates inside a `NOT` should not be picked for propagation") {
    val query = testRelation
      .select(columnA)
      .where(Not(columnB === Literal(10)) && columnA === Add(columnB, Literal(1)))
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("equality predicates outside a `OR` can be propagated within a `OR`") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Literal(2) &&
          (columnA === Add(columnB, Literal(3)) || columnB === Literal(9)))
      .analyze

    val correctAnswer = testRelation
      .select(columnA)
      .where(
        columnA === Literal(2) &&
          (Literal(2) === Add(columnB, Literal(3)) || columnB === Literal(9)))
      .analyze

    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("equality predicates inside a `OR` should not be picked for propagation") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Add(columnB, Literal(2)) &&
          (columnA === Add(columnB, Literal(3)) || columnB === Literal(9)))
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("equality operator not immediate child of root `AND` should not be used for propagation") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Literal(0) &&
          ((columnB === columnA) === (columnB === Literal(0))))
      .analyze

    val correctAnswer = testRelation
      .select(columnA)
      .where(
        columnA === Literal(0) &&
          ((columnB === Literal(0)) === (columnB === Literal(0))))
      .analyze

    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("conflicting equality predicates") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Literal(1) && columnA === Literal(2) && columnB === Add(columnA, Literal(3)))

    val correctAnswer = testRelation
      .select(columnA)
      .where(columnA === Literal(1) && columnA === Literal(2) && columnB === Literal(5)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }
}
