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

package org.apache.spark.sql.execution.datasources.orc

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._

import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgument}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * A test suite that tests Apache ORC filter API based filter pushdown optimization.
 * OrcFilterSuite and HiveOrcFilterSuite is logically duplicated to provide the same test coverage.
 * The difference are the packages containing 'Predicate' and 'SearchArgument' classes.
 * - OrcFilterSuite uses 'org.apache.orc.storage.ql.io.sarg' package.
 * - HiveOrcFilterSuite uses 'org.apache.hadoop.hive.ql.io.sarg' package.
 */
class OrcFilterSuite extends OrcTest with SharedSQLContext {

  private def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      checker: (SearchArgument) => Unit): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct
    val query = df
      .select(output.map(e => Column(e)): _*)
      .where(Column(predicate))

    var maybeRelation: Option[HadoopFsRelation] = None
    val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan.collect {
      case PhysicalOperation(_, filters, LogicalRelation(orcRelation: HadoopFsRelation, _, _, _)) =>
        maybeRelation = Some(orcRelation)
        filters
    }.flatten.reduceLeftOption(_ && _)
    assert(maybeAnalyzedPredicate.isDefined, "No filter is analyzed from the given query")

    val (_, selectedFilters, _) =
      DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate.toSeq)
    assert(selectedFilters.nonEmpty, "No filter is pushed down")

    val maybeFilter = OrcFilters.createFilter(query.schema, selectedFilters)
    assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $selectedFilters")
    checker(maybeFilter.get)
  }

  private def checkFilterPredicate
      (predicate: Predicate, filterOperator: PredicateLeaf.Operator)
      (implicit df: DataFrame): Unit = {
    def checkComparisonOperator(filter: SearchArgument) = {
      val operator = filter.getLeaves.asScala
      assert(operator.map(_.getOperator).contains(filterOperator))
    }
    checkFilterPredicate(df, predicate, checkComparisonOperator)
  }

  private def checkFilterPredicate
      (predicate: Predicate, stringExpr: String)
      (implicit df: DataFrame): Unit = {
    def checkLogicalOperator(filter: SearchArgument) = {
      assert(filter.toString == stringExpr)
    }
    checkFilterPredicate(df, predicate, checkLogicalOperator)
  }

  private def checkNoFilterPredicate
      (predicate: Predicate)
      (implicit df: DataFrame): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct
    val query = df
      .select(output.map(e => Column(e)): _*)
      .where(Column(predicate))

    var maybeRelation: Option[HadoopFsRelation] = None
    val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan.collect {
      case PhysicalOperation(_, filters, LogicalRelation(orcRelation: HadoopFsRelation, _, _, _)) =>
        maybeRelation = Some(orcRelation)
        filters
    }.flatten.reduceLeftOption(_ && _)
    assert(maybeAnalyzedPredicate.isDefined, "No filter is analyzed from the given query")

    val (_, selectedFilters, _) =
      DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate.toSeq)
    assert(selectedFilters.nonEmpty, "No filter is pushed down")

    val maybeFilter = OrcFilters.createFilter(query.schema, selectedFilters)
    assert(maybeFilter.isEmpty, s"Could generate filter predicate for $selectedFilters")
  }

  test("filter pushdown - integer") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - long") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - float") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - double") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - string") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(i.toString))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === "1", PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> "1", PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < "2", PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > "3", PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= "1", PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= "4", PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal("1") === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal("1") <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal("2") > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal("3") < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal("1") >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal("4") <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - boolean") {
    withOrcDataFrame((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === true, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> true, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < true, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > false, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= false, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= false, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(false) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(false) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(false) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(true) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(true) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(true) <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - decimal") {
    withOrcDataFrame((1 to 4).map(i => Tuple1.apply(BigDecimal.valueOf(i)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === BigDecimal.valueOf(1), PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> BigDecimal.valueOf(1), PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < BigDecimal.valueOf(2), PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > BigDecimal.valueOf(3), PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= BigDecimal.valueOf(1), PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= BigDecimal.valueOf(4), PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(
        Literal(BigDecimal.valueOf(1)) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(1)) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(2)) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(3)) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(1)) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(4)) <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - timestamp") {
    val timeString = "2015-08-20 14:57:00"
    val timestamps = (1 to 4).map { i =>
      val milliseconds = Timestamp.valueOf(timeString).getTime + i * 3600
      new Timestamp(milliseconds)
    }
    withOrcDataFrame(timestamps.map(Tuple1(_))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === timestamps(0), PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> timestamps(0), PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < timestamps(1), PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > timestamps(2), PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= timestamps(0), PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= timestamps(3), PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(timestamps(0)) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(timestamps(0)) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(timestamps(1)) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(timestamps(2)) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(timestamps(0)) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(timestamps(3)) <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - combinations with logical operators") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate(
        '_1.isNotNull,
        "leaf-0 = (IS_NULL _1), expr = (not leaf-0)"
      )
      checkFilterPredicate(
        '_1 =!= 1,
        "leaf-0 = (IS_NULL _1), leaf-1 = (EQUALS _1 1), expr = (and (not leaf-0) (not leaf-1))"
      )
      checkFilterPredicate(
        !('_1 < 4),
        "leaf-0 = (IS_NULL _1), leaf-1 = (LESS_THAN _1 4), expr = (and (not leaf-0) (not leaf-1))"
      )
      checkFilterPredicate(
        '_1 < 2 || '_1 > 3,
        "leaf-0 = (LESS_THAN _1 2), leaf-1 = (LESS_THAN_EQUALS _1 3), " +
          "expr = (or leaf-0 (not leaf-1))"
      )
      checkFilterPredicate(
        '_1 < 2 && '_1 > 3,
        "leaf-0 = (IS_NULL _1), leaf-1 = (LESS_THAN _1 2), leaf-2 = (LESS_THAN_EQUALS _1 3), " +
          "expr = (and (not leaf-0) leaf-1 (not leaf-2))"
      )
    }
  }

  test("filter pushdown - date") {
    val dates = Seq("2017-08-18", "2017-08-19", "2017-08-20", "2017-08-21").map { day =>
      Date.valueOf(day)
    }
    withOrcDataFrame(dates.map(Tuple1(_))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === dates(0), PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> dates(0), PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < dates(1), PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > dates(2), PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= dates(0), PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= dates(3), PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(dates(0)) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(dates(0)) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(dates(1)) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(dates(2)) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(dates(0)) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(dates(3)) <= '_1, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("no filter pushdown - non-supported types") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes(StandardCharsets.UTF_8)
    }
    // ArrayType
    withOrcDataFrame((1 to 4).map(i => Tuple1(Array(i)))) { implicit df =>
      checkNoFilterPredicate('_1.isNull)
    }
    // BinaryType
    withOrcDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkNoFilterPredicate('_1 <=> 1.b)
    }
    // MapType
    withOrcDataFrame((1 to 4).map(i => Tuple1(Map(i -> i)))) { implicit df =>
      checkNoFilterPredicate('_1.isNotNull)
    }
  }

  test("SPARK-12218 Converting conjunctions into ORC SearchArguments") {
    import org.apache.spark.sql.sources._
    // The `LessThan` should be converted while the `StringContains` shouldn't
    val schema = new StructType(
      Array(
        StructField("a", IntegerType, nullable = true),
        StructField("b", StringType, nullable = true)))
    assertResult("leaf-0 = (LESS_THAN a 10), expr = leaf-0") {
      OrcFilters.createFilter(schema, Array(
        LessThan("a", 10),
        StringContains("b", "prefix")
      )).get.toString
    }

    // The `LessThan` should be converted while the whole inner `And` shouldn't
    assertResult("leaf-0 = (LESS_THAN a 10), expr = leaf-0") {
      OrcFilters.createFilter(schema, Array(
        LessThan("a", 10),
        Not(And(
          GreaterThan("a", 1),
          StringContains("b", "prefix")
        ))
      )).get.toString
    }
  }
}
