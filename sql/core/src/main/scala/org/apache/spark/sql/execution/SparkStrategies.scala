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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{execution, AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.MemoryPlanV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StructType

/**
 * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
 * with the query planner and is not designed to be stable across spark releases.  Developers
 * writing libraries should instead consider using the stable APIs provided in
 * [[org.apache.spark.sql.sources]]
 */
abstract class SparkStrategy extends GenericStrategy[SparkPlan] {

  override protected def planLater(plan: LogicalPlan): SparkPlan = PlanLater(plan)
}

// 物理计划占位符
case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SparkPlanner =>

  /**
   * Plans special cases of limit operators.
   */
  object SpecialLimits extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(rootPlan) => rootPlan match {
        case Limit(IntegerLiteral(limit), Sort(order, true, child))
            if limit < conf.topKSortFallbackThreshold =>
          TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child)))
            if limit < conf.topKSortFallbackThreshold =>
          TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), child) =>
          CollectLimitExec(limit, planLater(child)) :: Nil
        case other => planLater(other) :: Nil
      }
      case Limit(IntegerLiteral(limit), Sort(order, true, child))
          if limit < conf.topKSortFallbackThreshold =>
        TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
      case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child)))
          if limit < conf.topKSortFallbackThreshold =>
        TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  /**
   * Select the proper physical plan for join based on joining keys and size of logical plan.
   *
   * At first, uses the [[ExtractEquiJoinKeys]] pattern to find joins where at least some of the
   * predicates can be evaluated by matching join keys. If found, join implementations are chosen
   * with the following precedence:
   *
   * - Broadcast hash join (BHJ):
   *     BHJ is not supported for full outer join. For right outer join, we only can broadcast the
   *     left side. For left outer, left semi, left anti and the internal join type ExistenceJoin,
   *     we only can broadcast the right side. For inner like join, we can broadcast both sides.
   *     Normally, BHJ can perform faster than the other join algorithms when the broadcast side is
   *     small. However, broadcasting tables is a network-intensive operation. It could cause OOM
   *     or perform worse than the other join algorithms, especially when the build/broadcast side
   *     is big.
   *
   *     For the supported cases, users can specify the broadcast hint (e.g. the user applied the
   *     [[org.apache.spark.sql.functions.broadcast()]] function to a DataFrame) and session-based
   *     [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold to adjust whether BHJ is used and
   *     which join side is broadcast.
   *
   *     1) Broadcast the join side with the broadcast hint, even if the size is larger than
   *     [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]. If both sides have the hint (only when the type
   *     is inner like join), the side with a smaller estimated physical size will be broadcast.
   *     2) Respect the [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold and broadcast the side
   *     whose estimated physical size is smaller than the threshold. If both sides are below the
   *     threshold, broadcast the smaller side. If neither is smaller, BHJ is not used.
   *
   * - Shuffle hash join: if the average size of a single partition is small enough to build a hash
   *     table.
   *
   * - Sort merge: if the matching join keys are sortable.
   *
   * If there is no joining keys, Join implementations are chosen with the following precedence:
   * - BroadcastNestedLoopJoin (BNLJ):
   *     BNLJ supports all the join types but the impl is OPTIMIZED for the following scenarios:
   *     For right outer join, the left side is broadcast. For left outer, left semi, left anti
   *     and the internal join type ExistenceJoin, the right side is broadcast. For inner like
   *     joins, either side is broadcast.
   *
   *     Like BHJ, users still can specify the broadcast hint and session-based
   *     [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold to impact which side is broadcast.
   *
   *     1) Broadcast the join side with the broadcast hint, even if the size is larger than
   *     [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]. If both sides have the hint (i.e., just for
   *     inner-like join), the side with a smaller estimated physical size will be broadcast.
   *     2) Respect the [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold and broadcast the side
   *     whose estimated physical size is smaller than the threshold. If both sides are below the
   *     threshold, broadcast the smaller side. If neither is smaller, BNLJ is not used.
   *
   * - CartesianProduct: for inner like join, CartesianProduct is the fallback option.
   *
   * - BroadcastNestedLoopJoin (BNLJ):
   *     For the other join types, BNLJ is the fallback option. Here, we just pick the broadcast
   *     side with the broadcast hint. If neither side has a hint, we broadcast the side with
   *     the smaller estimated physical size.
   */
  object JoinSelection extends Strategy with PredicateHelper {

    /**
     * 匹配输出应该足够小以用于广播连接的计划
     */
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
    }

    /**
     * 匹配单个分区应足够小以构建哈希表的计划。
     * 注意：这假设分区数是固定的，如果是动态的则需要额外的工作。
     */
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
    }

    /**
     * 返回计划a是否比计划b小得多（3倍）。
     * 构建哈希映射的成本比排序高，我们只应在一个表比另一个表小得多的情况下构建哈希映射。
     * 由于我们没有行数的统计数据，因此在这里使用字节大小作为估算。
     */
    private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
    }

    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }

    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }

    private def broadcastSide(
        canBuildLeft: Boolean,    // 左表是否可以广播
        canBuildRight: Boolean,   // 右表是否可以广播
        left: LogicalPlan,        // 左表
        right: LogicalPlan        // 右表
   ): BuildSide = {               // 返回广播类型
      def smallerSide =
        if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft

      if (canBuildRight && canBuildLeft) {
        // 如果左表右表都可以广播的情况下，只广播一个表， 较小的一张表
        smallerSide
      } else if (canBuildRight) {
        // 广播右表
        BuildRight
      } else if (canBuildLeft) {
        // 广播左表
        BuildLeft
      } else {
        // 否则广播小表
        smallerSide
      }
    }

    private def canBroadcastByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
      : Boolean = {
      // 判断左表是否可以广播
      val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
      // 判断右表是否可以广播
      val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
      buildLeft || buildRight
    }

    private def broadcastSideByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
      : BuildSide = {
      val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
      val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
      broadcastSide(buildLeft, buildRight, left, right)
    }

    private def canBroadcastBySizes(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
      : Boolean = {
      val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
      val buildRight = canBuildRight(joinType) && canBroadcast(right)
      buildLeft || buildRight
    }

    private def broadcastSideBySizes(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
      : BuildSide = {
      val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
      val buildRight = canBuildRight(joinType) && canBroadcast(right)
      broadcastSide(buildLeft, buildRight, left, right)
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      // --- BroadcastHashJoin --------------------------------------------------------------------

      // 基于用户标识的广播条件判断
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBroadcastByHints(joinType, left, right) =>              // 校验是否可以基于 Hints 标识，判断是否可以广播
        val buildSide = broadcastSideByHints(joinType, left, right)   // 返回广播形式
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, buildSide, condition, planLater(left), planLater(right)))    // 使用 BoradCastHashJoinExec 机型处理

      // 基于表大小的广播条件判断
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBroadcastBySizes(joinType, left, right) =>      // 校验是否有满足广播条件的小表
        val buildSide = broadcastSideBySizes(joinType, left, right)
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, buildSide, condition, planLater(left), planLater(right)))  // 使用 BraodCastHashJoinExec 进行JOIN 处理

      // --- ShuffledHashJoin ---------------------------------------------------------------------
        // spark.sql.join.preferSortMergeJoin
        // left join 等情况下， 右表是否较小，并且比左表小3倍以上
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildRight(joinType) && canBuildLocalHashMap(right)
           && muchSmaller(right, left) ||
           !RowOrdering.isOrderable(leftKeys) => // left key 类型是不可排序类型
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

      // spark.sql.join.preferSortMergeJoin
      // right join 等情况下， 左表是否较小，并且比右表小3倍以上
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildLeft(joinType) && canBuildLocalHashMap(left)
           && muchSmaller(left, right) ||
           !RowOrdering.isOrderable(leftKeys) =>  // left key 类型是不可排序类型
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

      // --- SortMergeJoin ------------------------------------------------------------
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if RowOrdering.isOrderable(leftKeys) =>  // left key 是可排序类型
        joins.SortMergeJoinExec(
          leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

      // --- Without joining keys ------------------------------------------------------------
      // 基于用户表示的广播条件校验
      case j @ logical.Join(left, right, joinType, condition)
          if canBroadcastByHints(joinType, left, right) =>
        val buildSide = broadcastSideByHints(joinType, left, right)
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

       // 基于配置大小的广播条件校验
      case j @ logical.Join(left, right, joinType, condition)
          if canBroadcastBySizes(joinType, left, right) =>
        val buildSide = broadcastSideBySizes(joinType, left, right)
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

       //
      case logical.Join(left, right, _: InnerLike, condition) =>  // 笛卡尔JOIN
        joins.CartesianProductExec(planLater(left), planLater(right), condition) :: Nil

      // 其他类型
      case logical.Join(left, right, joinType, condition) =>
        val buildSide = broadcastSide(
          left.stats.hints.broadcast, right.stats.hints.broadcast, left, right)
        // This join could be very slow or OOM
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

      // --- Cases where this strategy does not apply ---------------------------------------------

      case _ => Nil
    }
  }

  /**
   * Used to plan streaming aggregation queries that are computed incrementally as part of a
   * [[StreamingQuery]]. Currently this rule is injected into the planner
   * on-demand, only when planning in a [[org.apache.spark.sql.execution.streaming.StreamExecution]]
   */
  object StatefulAggregationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case _ if !plan.isStreaming => Nil

      case EventTimeWatermark(columnName, delay, child) =>
        EventTimeWatermarkExec(columnName, delay, planLater(child)) :: Nil

      case PhysicalAggregation(
        namedGroupingExpressions, aggregateExpressions, rewrittenResultExpressions, child) =>

        if (aggregateExpressions.exists(PythonUDF.isGroupedAggPandasUDF)) {
          throw new AnalysisException(
            "Streaming aggregation doesn't support group aggregate pandas UDF")
        }

        val stateVersion = conf.getConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION)

        aggregate.AggUtils.planStreamingAggregation(
          namedGroupingExpressions,
          aggregateExpressions.map(expr => expr.asInstanceOf[AggregateExpression]),
          rewrittenResultExpressions,
          stateVersion,
          planLater(child))

      case _ => Nil
    }
  }

  /**
   * Us ed to plan the streaming deduplicate operator.
   */
  object StreamingDeduplicationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Deduplicate(keys, child) if child.isStreaming =>
        StreamingDeduplicateExec(keys, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  /**
   * Used to plan the streaming global limit operator for streams in append mode.
   * We need to check for either a direct Limit or a Limit wrapped in a ReturnAnswer operator,
   * following the example of the SpecialLimits Strategy above.
   * Streams with limit in Append mode use the stateful StreamingGlobalLimitExec.
   * Streams with limit in Complete mode use the stateless CollectLimitExec operator.
   * Limit is unsupported for streams in Update mode.
   */
  case class StreamingGlobalLimitStrategy(outputMode: OutputMode) extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(rootPlan) => rootPlan match {
        case Limit(IntegerLiteral(limit), child)
            if plan.isStreaming && outputMode == InternalOutputModes.Append =>
          StreamingGlobalLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
        case _ => Nil
      }
      case Limit(IntegerLiteral(limit), child)
          if plan.isStreaming && outputMode == InternalOutputModes.Append =>
        StreamingGlobalLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
      case _ => Nil
    }
  }

  object StreamingJoinStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
          if left.isStreaming && right.isStreaming =>

          new StreamingSymmetricHashJoinExec(
            leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

        case Join(left, right, _, _) if left.isStreaming && right.isStreaming =>
          throw new AnalysisException(
            "Stream-stream join without equality predicate is not supported", plan = Some(plan))

        case _ => Nil
      }
    }
  }

  /**
   * Used to plan the aggregate operator for expressions based on the AggregateFunction2 interface.
   */
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>
        val aggregateExpressions = aggExpressions.map(expr =>
          expr.asInstanceOf[AggregateExpression])

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children.toSet).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our `RewriteDistinctAggregates` should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
              "Spark user mailing list.")
        }

        val aggregateOperator =
          if (functionsWithDistinct.isEmpty) {
            aggregate.AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child))
          } else {
            aggregate.AggUtils.planAggregateWithOneDistinct(
              groupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              planLater(child))
          }

        aggregateOperator

      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if aggExpressions.forall(expr => expr.isInstanceOf[PythonUDF]) =>
        val udfExpressions = aggExpressions.map(expr => expr.asInstanceOf[PythonUDF])

        Seq(execution.python.AggregateInPandasExec(
          groupingExpressions,
          udfExpressions,
          resultExpressions,
          planLater(child)))

      case PhysicalAggregation(_, _, _, _) =>
        // If cannot match the two cases above, then it's an error
        throw new AnalysisException(
          "Cannot use a mixture of aggregate function and group aggregate pandas UDF")

      case _ => Nil
    }
  }

  object Window extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalWindow(
        WindowFunctionType.SQL, windowExprs, partitionSpec, orderSpec, child) =>
        execution.window.WindowExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case PhysicalWindow(
        WindowFunctionType.Python, windowExprs, partitionSpec, orderSpec, child) =>
        execution.python.WindowInPandasExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  protected lazy val singleRowRdd = sparkContext.parallelize(Seq(InternalRow()), 1)

  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryTableScanExec(_, filters, mem)) :: Nil
      case _ => Nil
    }
  }

  /**
   * This strategy is just for explaining `Dataset/DataFrame` created by `spark.readStream`.
   * It won't affect the execution, because `StreamingRelation` will be replaced with
   * `StreamingExecutionRelation` in `StreamingQueryManager` and `StreamingExecutionRelation` will
   * be replaced with the real relation using the `Source` in `StreamExecution`.
   */
  object StreamingRelationStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case s: StreamingRelation =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case s: StreamingExecutionRelation =>
        StreamingRelationExec(s.toString, s.output) :: Nil
      case s: StreamingRelationV2 =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case _ => Nil
    }
  }

  /**
   * Strategy to convert [[FlatMapGroupsWithState]] logical operator to physical operator
   * in streaming plans. Conversion for batch plans is handled by [[BasicOperators]].
   */
  object FlatMapGroupsWithStateStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case FlatMapGroupsWithState(
        func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, stateEnc, outputMode, _,
        timeout, child) =>
        val stateVersion = conf.getConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION)
        val execPlan = FlatMapGroupsWithStateExec(
          func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, None, stateEnc, stateVersion,
          outputMode, timeout, batchTimestampMs = None, eventTimeWatermark = None, planLater(child))
        execPlan :: Nil
      case _ =>
        Nil
    }
  }

  /**
   * Strategy to convert EvalPython logical operator to physical operator.
   */
  object PythonEvals extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ArrowEvalPython(udfs, output, child) =>
        ArrowEvalPythonExec(udfs, output, planLater(child)) :: Nil
      case BatchEvalPython(udfs, output, child) =>
        BatchEvalPythonExec(udfs, output, planLater(child)) :: Nil
      case _ =>
        Nil
    }
  }

  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case d: DataWritingCommand => DataWritingCommandExec(d, planLater(d.query)) :: Nil
      case r: RunnableCommand => ExecutedCommandExec(r) :: Nil

      case MemoryPlan(sink, output) =>
        val encoder = RowEncoder(sink.schema)
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy())) :: Nil
      case MemoryPlanV2(sink, output) =>
        val encoder = RowEncoder(StructType.fromAttributes(output))
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy())) :: Nil

      case logical.Distinct(child) =>
        throw new IllegalStateException(
          "logical distinct operator should have been replaced by aggregate in the optimizer")
      case logical.Intersect(left, right, false) =>
        throw new IllegalStateException(
          "logical intersect  operator should have been replaced by semi-join in the optimizer")
      case logical.Intersect(left, right, true) =>
        throw new IllegalStateException(
          "logical intersect operator should have been replaced by union, aggregate" +
            " and generate operators in the optimizer")
      case logical.Except(left, right, false) =>
        throw new IllegalStateException(
          "logical except operator should have been replaced by anti-join in the optimizer")
      case logical.Except(left, right, true) =>
        throw new IllegalStateException(
          "logical except (all) operator should have been replaced by union, aggregate" +
            " and generate operators in the optimizer")

      case logical.DeserializeToObject(deserializer, objAttr, child) =>
        execution.DeserializeToObjectExec(deserializer, objAttr, planLater(child)) :: Nil
      case logical.SerializeFromObject(serializer, child) =>
        execution.SerializeFromObjectExec(serializer, planLater(child)) :: Nil
      case logical.MapPartitions(f, objAttr, child) =>
        execution.MapPartitionsExec(f, objAttr, planLater(child)) :: Nil
      case logical.MapPartitionsInR(f, p, b, is, os, objAttr, child) =>
        execution.MapPartitionsExec(
          execution.r.MapPartitionsRWrapper(f, p, b, is, os), objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInR(f, p, b, is, os, key, value, grouping, data, objAttr, child) =>
        execution.FlatMapGroupsInRExec(f, p, b, is, os, key, value, grouping,
          data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInPandas(grouping, func, output, child) =>
        execution.python.FlatMapGroupsInPandasExec(grouping, func, output, planLater(child)) :: Nil
      case logical.MapElements(f, _, _, objAttr, child) =>
        execution.MapElementsExec(f, objAttr, planLater(child)) :: Nil
      case logical.AppendColumns(f, _, _, in, out, child) =>
        execution.AppendColumnsExec(f, in, out, planLater(child)) :: Nil
      case logical.AppendColumnsWithObject(f, childSer, newSer, child) =>
        execution.AppendColumnsWithObjectExec(f, childSer, newSer, planLater(child)) :: Nil
      case logical.MapGroups(f, key, value, grouping, data, objAttr, child) =>
        execution.MapGroupsExec(f, key, value, grouping, data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsWithState(
          f, key, value, grouping, data, output, _, _, _, timeout, child) =>
        execution.MapGroupsExec(
          f, key, value, grouping, data, output, timeout, planLater(child)) :: Nil
      case logical.CoGroup(f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr, left, right) =>
        execution.CoGroupExec(
          f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr,
          planLater(left), planLater(right)) :: Nil

      case logical.Repartition(numPartitions, shuffle, child) =>
        if (shuffle) {
          ShuffleExchangeExec(RoundRobinPartitioning(numPartitions), planLater(child)) :: Nil
        } else {
          execution.CoalesceExec(numPartitions, planLater(child)) :: Nil
        }
      case logical.Sort(sortExprs, global, child) =>
        execution.SortExec(sortExprs, global, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.ProjectExec(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.FilterExec(condition, planLater(child)) :: Nil
      case f: logical.TypedFilter =>
        execution.FilterExec(f.typedCondition(f.deserializer), planLater(f.child)) :: Nil
      case e @ logical.Expand(_, _, child) =>
        execution.ExpandExec(e.projections, e.output, planLater(child)) :: Nil
      case logical.Sample(lb, ub, withReplacement, seed, child) =>
        execution.SampleExec(lb, ub, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data, _) =>
        LocalTableScanExec(output, data) :: Nil
      case logical.LocalLimit(IntegerLiteral(limit), child) =>
        execution.LocalLimitExec(limit, planLater(child)) :: Nil
      case logical.GlobalLimit(IntegerLiteral(limit), child) =>
        execution.GlobalLimitExec(limit, planLater(child)) :: Nil
      case logical.Union(unionChildren) =>
        execution.UnionExec(unionChildren.map(planLater)) :: Nil
      case g @ logical.Generate(generator, _, outer, _, _, child) =>
        execution.GenerateExec(
          generator, g.requiredChildOutput, outer,
          g.qualifiedGeneratorOutput, planLater(child)) :: Nil
      case _: logical.OneRowRelation =>
        execution.RDDScanExec(Nil, singleRowRdd, "OneRowRelation") :: Nil
      case r: logical.Range =>
        execution.RangeExec(r) :: Nil
      case r: logical.RepartitionByExpression =>
        exchange.ShuffleExchangeExec(r.partitioning, planLater(r.child)) :: Nil
      case ExternalRDD(outputObjAttr, rdd) => ExternalRDDScanExec(outputObjAttr, rdd) :: Nil
      case r: LogicalRDD =>
        RDDScanExec(r.output, r.rdd, "ExistingRDD", r.outputPartitioning, r.outputOrdering) :: Nil
      case h: ResolvedHint => planLater(h.child) :: Nil
      case _ => Nil
    }
  }
}
