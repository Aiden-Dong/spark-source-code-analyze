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

package org.apache.spark.sql

import scala.collection.JavaConverters._

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.api.java.function._
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CreateStruct}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.expressions.ReduceAggregator
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

/**
 * :: Experimental ::
 * A [[Dataset]] has been logically grouped by a user specified grouping key.  Users should not
 * construct a [[KeyValueGroupedDataset]] directly, but should instead call `groupByKey` on
 * an existing [[Dataset]].
 *
 * @since 2.0.0
 */
@Experimental
@InterfaceStability.Evolving
class KeyValueGroupedDataset[K, V] private[sql](
    kEncoder: Encoder[K],
    vEncoder: Encoder[V],
    @transient val queryExecution: QueryExecution,
    private val dataAttributes: Seq[Attribute],
    private val groupingAttributes: Seq[Attribute]) extends Serializable {

  // Similar to [[Dataset]], we turn the passed in encoder to `ExpressionEncoder` explicitly.
  private implicit val kExprEnc = encoderFor(kEncoder)
  private implicit val vExprEnc = encoderFor(vEncoder)

  private def logicalPlan = queryExecution.analyzed
  private def sparkSession = queryExecution.sparkSession

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the type of the key has been mapped to the
   * specified type. The mapping of key columns to the type follows the same rules as `as` on
   * [[Dataset]].
   *
   * @since 1.6.0
   */
  def keyAs[L : Encoder]: KeyValueGroupedDataset[L, V] =
    new KeyValueGroupedDataset(
      encoderFor[L],
      vExprEnc,
      queryExecution,
      dataAttributes,
      groupingAttributes)

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the given function `func` has been applied
   * to the data. The grouping key is unchanged by this.
   *
   * {{{
   *   // Create values grouped by key from a Dataset[(K, V)]
   *   ds.groupByKey(_._1).mapValues(_._2) // Scala
   * }}}
   *
   * @since 2.1.0
   */
  def mapValues[W : Encoder](func: V => W): KeyValueGroupedDataset[K, W] = {
    val withNewData = AppendColumns(func, dataAttributes, logicalPlan)
    val projected = Project(withNewData.newColumns ++ groupingAttributes, withNewData)
    val executed = sparkSession.sessionState.executePlan(projected)

    new KeyValueGroupedDataset(
      encoderFor[K],
      encoderFor[W],
      executed,
      withNewData.newColumns,
      groupingAttributes)
  }

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the given function `func` has been applied
   * to the data. The grouping key is unchanged by this.
   *
   * {{{
   *   // Create Integer values grouped by String key from a Dataset<Tuple2<String, Integer>>
   *   Dataset<Tuple2<String, Integer>> ds = ...;
   *   KeyValueGroupedDataset<String, Integer> grouped =
   *     ds.groupByKey(t -> t._1, Encoders.STRING()).mapValues(t -> t._2, Encoders.INT());
   * }}}
   *
   * @since 2.1.0
   */
  def mapValues[W](func: MapFunction[V, W], encoder: Encoder[W]): KeyValueGroupedDataset[K, W] = {
    implicit val uEnc = encoder
    mapValues { (v: V) => func.call(v) }
  }

  /**
   * Returns a [[Dataset]] that contains each unique key. This is equivalent to doing mapping
   * over the Dataset to extract the keys and then running a distinct operation on those.
   *
   * @since 1.6.0
   */
  def keys: Dataset[K] = {
    Dataset[K](
      sparkSession,
      Distinct(
        Project(groupingAttributes, logicalPlan)))
  }

  /**
   * (Scala-specific)
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an iterator containing elements of an arbitrary type which will be returned
   * as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def flatMapGroups[U : Encoder](f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] = {
    Dataset[U](
      sparkSession,
      MapGroups(
        f,
        groupingAttributes,
        dataAttributes,
        logicalPlan))
  }

  /**
   * (Java-specific)
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an iterator containing elements of an arbitrary type which will be returned
   * as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def flatMapGroups[U](f: FlatMapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] = {
    flatMapGroups((key, data) => f.call(key, data.asJava).asScala)(encoder)
  }

  /**
   * (Scala-specific)
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an element of arbitrary type which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def mapGroups[U : Encoder](f: (K, Iterator[V]) => U): Dataset[U] = {
    val func = (key: K, it: Iterator[V]) => Iterator(f(key, it))
    flatMapGroups(func)
  }

  /**
   * (Java-specific)
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an element of arbitrary type which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def mapGroups[U](f: MapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] = {
    mapGroups((key, data) => f.call(key, data.asJava))(encoder)
  }

  /**
   * ::Experimental::
   * (Scala-specific)
   * Applies the given function to each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by the function.
   * For a static batch Dataset, the function will be invoked once per group. For a streaming
   * Dataset, the function will be invoked for each group repeatedly in every trigger, and
   * updates to each group's state will be saved across invocations.
   * See [[org.apache.spark.sql.streaming.GroupState]] for more details.
   *
   * @tparam S The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U The type of the output objects. Must be encodable to Spark SQL types.
   * @param func Function to be called on every group.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def mapGroupsWithState[S: Encoder, U: Encoder](
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    val flatMapFunc = (key: K, it: Iterator[V], s: GroupState[S]) => Iterator(func(key, it, s))
    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        flatMapFunc.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        OutputMode.Update,
        isMapGroupsWithState = true,
        GroupStateTimeout.NoTimeout,
        child = logicalPlan))
  }

  /**
   * ::Experimental::
   * (Scala-specific)
   * Applies the given function to each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by the function.
   * For a static batch Dataset, the function will be invoked once per group. For a streaming
   * Dataset, the function will be invoked for each group repeatedly in every trigger, and
   * updates to each group's state will be saved across invocations.
   * See [[org.apache.spark.sql.streaming.GroupState]] for more details.
   *
   * @tparam S The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U The type of the output objects. Must be encodable to Spark SQL types.
   * @param func Function to be called on every group.
   * @param timeoutConf Timeout configuration for groups that do not receive data for a while.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def mapGroupsWithState[S: Encoder, U: Encoder](
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    val flatMapFunc = (key: K, it: Iterator[V], s: GroupState[S]) => Iterator(func(key, it, s))
    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        flatMapFunc.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        OutputMode.Update,
        isMapGroupsWithState = true,
        timeoutConf,
        child = logicalPlan))
  }

  /**
   * ::Experimental::
   * (Java-specific)
   * Applies the given function to each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by the function.
   * For a static batch Dataset, the function will be invoked once per group. For a streaming
   * Dataset, the function will be invoked for each group repeatedly in every trigger, and
   * updates to each group's state will be saved across invocations.
   * See `GroupState` for more details.
   *
   * @tparam S The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U The type of the output objects. Must be encodable to Spark SQL types.
   * @param func          Function to be called on every group.
   * @param stateEncoder  Encoder for the state type.
   * @param outputEncoder Encoder for the output type.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def mapGroupsWithState[S, U](
      func: MapGroupsWithStateFunction[K, V, S, U],
      stateEncoder: Encoder[S],
      outputEncoder: Encoder[U]): Dataset[U] = {
    mapGroupsWithState[S, U](
      (key: K, it: Iterator[V], s: GroupState[S]) => func.call(key, it.asJava, s)
    )(stateEncoder, outputEncoder)
  }

  /**
   * ::Experimental::
   * (Java-specific)
   * Applies the given function to each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by the function.
   * For a static batch Dataset, the function will be invoked once per group. For a streaming
   * Dataset, the function will be invoked for each group repeatedly in every trigger, and
   * updates to each group's state will be saved across invocations.
   * See `GroupState` for more details.
   *
   * @tparam S The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U The type of the output objects. Must be encodable to Spark SQL types.
   * @param func          Function to be called on every group.
   * @param stateEncoder  Encoder for the state type.
   * @param outputEncoder Encoder for the output type.
   * @param timeoutConf   Timeout configuration for groups that do not receive data for a while.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def mapGroupsWithState[S, U](
      func: MapGroupsWithStateFunction[K, V, S, U],
      stateEncoder: Encoder[S],
      outputEncoder: Encoder[U],
      timeoutConf: GroupStateTimeout): Dataset[U] = {
    mapGroupsWithState[S, U](timeoutConf)(
      (key: K, it: Iterator[V], s: GroupState[S]) => func.call(key, it.asJava, s)
    )(stateEncoder, outputEncoder)
  }

  /**
   * ::Experimental::
   * (Scala-specific)
   * Applies the given function to each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by the function.
   * For a static batch Dataset, the function will be invoked once per group. For a streaming
   * Dataset, the function will be invoked for each group repeatedly in every trigger, and
   * updates to each group's state will be saved across invocations.
   * See `GroupState` for more details.
   *
   * @tparam S The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U The type of the output objects. Must be encodable to Spark SQL types.
   * @param func Function to be called on every group.
   * @param outputMode The output mode of the function.
   * @param timeoutConf Timeout configuration for groups that do not receive data for a while.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Update) {
      throw new IllegalArgumentException("The output mode of function should be append or update")
    }
    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        func.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        outputMode,
        isMapGroupsWithState = false,
        timeoutConf,
        child = logicalPlan))
  }

  /**
   * ::Experimental::
   * (Java-specific)
   * Applies the given function to each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by the function.
   * For a static batch Dataset, the function will be invoked once per group. For a streaming
   * Dataset, the function will be invoked for each group repeatedly in every trigger, and
   * updates to each group's state will be saved across invocations.
   * See `GroupState` for more details.
   *
   * @tparam S The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U The type of the output objects. Must be encodable to Spark SQL types.
   * @param func          Function to be called on every group.
   * @param outputMode    The output mode of the function.
   * @param stateEncoder  Encoder for the state type.
   * @param outputEncoder Encoder for the output type.
   * @param timeoutConf   Timeout configuration for groups that do not receive data for a while.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def flatMapGroupsWithState[S, U](
      func: FlatMapGroupsWithStateFunction[K, V, S, U],
      outputMode: OutputMode,
      stateEncoder: Encoder[S],
      outputEncoder: Encoder[U],
      timeoutConf: GroupStateTimeout): Dataset[U] = {
    val f = (key: K, it: Iterator[V], s: GroupState[S]) => func.call(key, it.asJava, s).asScala
    flatMapGroupsWithState[S, U](outputMode, timeoutConf)(f)(stateEncoder, outputEncoder)
  }

  /**
   * (Scala-specific)
   * Reduces the elements of each group of data using the specified binary function.
   * The given function must be commutative and associative or the result may be non-deterministic.
   *
   * @since 1.6.0
   */
  def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = {
    val vEncoder = encoderFor[V]
    val aggregator: TypedColumn[V, V] = new ReduceAggregator[V](f)(vEncoder).toColumn
    agg(aggregator)
  }

  /**
   * (Java-specific)
   * Reduces the elements of each group of data using the specified binary function.
   * The given function must be commutative and associative or the result may be non-deterministic.
   *
   * @since 1.6.0
   */
  def reduceGroups(f: ReduceFunction[V]): Dataset[(K, V)] = {
    reduceGroups(f.call _)
  }

  /**
   * Internal helper function for building typed aggregations that return tuples.  For simplicity
   * and code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   */
  protected def aggUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val encoders = columns.map(_.encoder)
    val namedColumns =
      columns.map(_.withInputType(vExprEnc, dataAttributes).named)
    val keyColumn = if (kExprEnc.flat) {
      assert(groupingAttributes.length == 1)
      groupingAttributes.head
    } else {
      Alias(CreateStruct(groupingAttributes), "key")()
    }
    val aggregate = Aggregate(groupingAttributes, keyColumn +: namedColumns, logicalPlan)
    val execution = new QueryExecution(sparkSession, aggregate)

    new Dataset(
      sparkSession,
      execution,
      ExpressionEncoder.tuple(kExprEnc +: encoders))
  }

  /**
   * Computes the given aggregation, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing this aggregation over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1](col1: TypedColumn[V, U1]): Dataset[(K, U1)] =
    aggUntyped(col1).asInstanceOf[Dataset[(K, U1)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2]): Dataset[(K, U1, U2)] =
    aggUntyped(col1, col2).asInstanceOf[Dataset[(K, U1, U2)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2, U3](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3]): Dataset[(K, U1, U2, U3)] =
    aggUntyped(col1, col2, col3).asInstanceOf[Dataset[(K, U1, U2, U3)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2, U3, U4](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4]): Dataset[(K, U1, U2, U3, U4)] =
    aggUntyped(col1, col2, col3, col4).asInstanceOf[Dataset[(K, U1, U2, U3, U4)]]

  /**
   * Returns a [[Dataset]] that contains a tuple with each key and the number of items present
   * for that key.
   *
   * @since 1.6.0
   */
  def count(): Dataset[(K, Long)] = agg(functions.count("*").as(ExpressionEncoder[Long]()))

  /**
   * (Scala-specific)
   * Applies the given function to each cogrouped data.  For each unique group, the function will
   * be passed the grouping key and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`.  The function can return an iterator containing elements of an
   * arbitrary type which will be returned as a new [[Dataset]].
   *
   * @since 1.6.0
   */
  def cogroup[U, R : Encoder](
      other: KeyValueGroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): Dataset[R] = {
    implicit val uEncoder = other.vExprEnc
    Dataset[R](
      sparkSession,
      CoGroup(
        f,
        this.groupingAttributes,
        other.groupingAttributes,
        this.dataAttributes,
        other.dataAttributes,
        this.logicalPlan,
        other.logicalPlan))
  }

  /**
   * (Java-specific)
   * Applies the given function to each cogrouped data.  For each unique group, the function will
   * be passed the grouping key and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`.  The function can return an iterator containing elements of an
   * arbitrary type which will be returned as a new [[Dataset]].
   *
   * @since 1.6.0
   */
  def cogroup[U, R](
      other: KeyValueGroupedDataset[K, U],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] = {
    cogroup(other)((key, left, right) => f.call(key, left.asJava, right.asJava).asScala)(encoder)
  }

  override def toString: String = {
    val builder = new StringBuilder
    val kFields = kExprEnc.schema.map {
      case f => s"${f.name}: ${f.dataType.simpleString(2)}"
    }
    val vFields = vExprEnc.schema.map {
      case f => s"${f.name}: ${f.dataType.simpleString(2)}"
    }
    builder.append("KeyValueGroupedDataset: [key: [")
    builder.append(kFields.take(2).mkString(", "))
    if (kFields.length > 2) {
      builder.append(" ... " + (kFields.length - 2) + " more field(s)")
    }
    builder.append("], value: [")
    builder.append(vFields.take(2).mkString(", "))
    if (vFields.length > 2) {
      builder.append(" ... " + (vFields.length - 2) + " more field(s)")
    }
    builder.append("]]").toString()
  }
}
