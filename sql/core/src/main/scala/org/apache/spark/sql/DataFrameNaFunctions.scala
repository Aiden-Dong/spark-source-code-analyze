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

import java.{lang => jl}
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
 * Functionality for working with missing data in `DataFrame`s.
 *
 * @since 1.3.1
 */
@InterfaceStability.Stable
final class DataFrameNaFunctions private[sql](df: DataFrame) {

  /**
   * Returns a new `DataFrame` that drops rows containing any null or NaN values.
   *
   * @since 1.3.1
   */
  def drop(): DataFrame = drop("any", df.columns)

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN values.
   *
   * If `how` is "any", then drop rows containing any null or NaN values.
   * If `how` is "all", then drop rows only if every column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String): DataFrame = drop(how, df.columns)

  /**
   * Returns a new `DataFrame` that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(cols: Array[String]): DataFrame = drop(cols.toSeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(cols: Seq[String]): DataFrame = drop(cols.size, cols)

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Array[String]): DataFrame = drop(how, cols.toSeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Seq[String]): DataFrame = {
    how.toLowerCase(Locale.ROOT) match {
      case "any" => drop(cols.size, cols)
      case "all" => drop(1, cols)
      case _ => throw new IllegalArgumentException(s"how ($how) must be 'any' or 'all'")
    }
  }

  /**
   * Returns a new `DataFrame` that drops rows containing
   * less than `minNonNulls` non-null and non-NaN values.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int): DataFrame = drop(minNonNulls, df.columns)

  /**
   * Returns a new `DataFrame` that drops rows containing
   * less than `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Array[String]): DataFrame = drop(minNonNulls, cols.toSeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing less than
   * `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Seq[String]): DataFrame = {
    // Filtering condition:
    // only keep the row if it has at least `minNonNulls` non-null and non-NaN values.
    val predicate = AtLeastNNonNulls(minNonNulls, cols.map(name => df.resolve(name)))
    df.filter(Column(predicate))
  }

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in numeric columns with `value`.
   *
   * @since 2.2.0
   */
  def fill(value: Long): DataFrame = fill(value, df.columns)

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in numeric columns with `value`.
   * @since 1.3.1
   */
  def fill(value: Double): DataFrame = fill(value, df.columns)

  /**
   * Returns a new `DataFrame` that replaces null values in string columns with `value`.
   *
   * @since 1.3.1
   */
  def fill(value: String): DataFrame = fill(value, df.columns)

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   *
   * @since 2.2.0
   */
  def fill(value: Long, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 2.2.0
   */
  def fill(value: Long, cols: Seq[String]): DataFrame = fillValue(value, cols)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Seq[String]): DataFrame = fillValue(value, cols)


  /**
   * Returns a new `DataFrame` that replaces null values in specified string columns.
   * If a specified column is not a string column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: String, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values in
   * specified string columns. If a specified column is not a string column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: String, cols: Seq[String]): DataFrame = fillValue(value, cols)

  /**
   * Returns a new `DataFrame` that replaces null values in boolean columns with `value`.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean): DataFrame = fill(value, df.columns)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values in specified
   * boolean columns. If a specified column is not a boolean column, it is ignored.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean, cols: Seq[String]): DataFrame = fillValue(value, cols)

  /**
   * Returns a new `DataFrame` that replaces null values in specified boolean columns.
   * If a specified column is not a boolean column, it is ignored.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean, cols: Array[String]): DataFrame = fill(value, cols.toSeq)


  /**
   * Returns a new `DataFrame` that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type:
   * `Integer`, `Long`, `Float`, `Double`, `String`, `Boolean`.
   * Replacement values are cast to the column data type.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *   df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));
   * }}}
   *
   * @since 1.3.1
   */
  def fill(valueMap: java.util.Map[String, Any]): DataFrame = fillMap(valueMap.asScala.toSeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type: `Int`, `Long`, `Float`, `Double`, `String`, `Boolean`.
   * Replacement values are cast to the column data type.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   df.na.fill(Map(
   *     "A" -> "unknown",
   *     "B" -> 1.0
   *   ))
   * }}}
   *
   * @since 1.3.1
   */
  def fill(valueMap: Map[String, Any]): DataFrame = fillMap(valueMap.toSeq)

  /**
   * Replaces values matching keys in `replacement` map with the corresponding values.
   *
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
   *   df.na.replace("height", ImmutableMap.of(1.0, 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
   *   df.na.replace("name", ImmutableMap.of("UNKNOWN", "unnamed"));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
   *   df.na.replace("*", ImmutableMap.of("UNKNOWN", "unnamed"));
   * }}}
   *
   * @param col name of the column to apply the value replacement. If `col` is "*",
   *            replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](col: String, replacement: java.util.Map[T, T]): DataFrame = {
    replace[T](col, replacement.asScala.toMap)
  }

  /**
   * Replaces values matching keys in `replacement` map with the corresponding values.
   *
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
   *   df.na.replace(new String[] {"height", "weight"}, ImmutableMap.of(1.0, 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
   *   df.na.replace(new String[] {"firstname", "lastname"}, ImmutableMap.of("UNKNOWN", "unnamed"));
   * }}}
   *
   * @param cols list of columns to apply the value replacement. If `col` is "*",
   *             replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](cols: Array[String], replacement: java.util.Map[T, T]): DataFrame = {
    replace(cols.toSeq, replacement.asScala.toMap)
  }

  /**
   * (Scala-specific) Replaces values matching keys in `replacement` map.
   *
   * {{{
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
   *   df.na.replace("height", Map(1.0 -> 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
   *   df.na.replace("name", Map("UNKNOWN" -> "unnamed"));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
   *   df.na.replace("*", Map("UNKNOWN" -> "unnamed"));
   * }}}
   *
   * @param col name of the column to apply the value replacement. If `col` is "*",
   *            replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](col: String, replacement: Map[T, T]): DataFrame = {
    if (col == "*") {
      replace0(df.columns, replacement)
    } else {
      replace0(Seq(col), replacement)
    }
  }

  /**
   * (Scala-specific) Replaces values matching keys in `replacement` map.
   *
   * {{{
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
   *   df.na.replace("height" :: "weight" :: Nil, Map(1.0 -> 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
   *   df.na.replace("firstname" :: "lastname" :: Nil, Map("UNKNOWN" -> "unnamed"));
   * }}}
   *
   * @param cols list of columns to apply the value replacement. If `col` is "*",
   *             replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = replace0(cols, replacement)

  private def replace0[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = {
    if (replacement.isEmpty || cols.isEmpty) {
      return df
    }

    // Convert the NumericType in replacement map to DoubleType,
    // while leaving StringType, BooleanType and null untouched.
    val replacementMap: Map[_, _] = replacement.map {
      case (k, v: String) => (k, v)
      case (k, v: Boolean) => (k, v)
      case (k: String, null) => (k, null)
      case (k: Boolean, null) => (k, null)
      case (k, null) => (convertToDouble(k), null)
      case (k, v) => (convertToDouble(k), convertToDouble(v))
    }

    // targetColumnType is either DoubleType, StringType or BooleanType,
    // depending on the type of first key in replacement map.
    // Only fields of targetColumnType will perform replacement.
    val targetColumnType = replacement.head._1 match {
      case _: jl.Double | _: jl.Float | _: jl.Integer | _: jl.Long => DoubleType
      case _: jl.Boolean => BooleanType
      case _: String => StringType
    }

    val columnEquals = df.sparkSession.sessionState.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      val shouldReplace = cols.exists(colName => columnEquals(colName, f.name))
      if (f.dataType.isInstanceOf[NumericType] && targetColumnType == DoubleType && shouldReplace) {
        replaceCol(f, replacementMap)
      } else if (f.dataType == targetColumnType && shouldReplace) {
        replaceCol(f, replacementMap)
      } else {
        df.col(f.name)
      }
    }
    df.select(projections : _*)
  }

  private def fillMap(values: Seq[(String, Any)]): DataFrame = {
    // Error handling
    values.foreach { case (colName, replaceValue) =>
      // Check column name exists
      df.resolve(colName)

      // Check data type
      replaceValue match {
        case _: jl.Double | _: jl.Float | _: jl.Integer | _: jl.Long | _: jl.Boolean | _: String =>
          // This is good
        case _ => throw new IllegalArgumentException(
          s"Unsupported value type ${replaceValue.getClass.getName} ($replaceValue).")
      }
    }

    val columnEquals = df.sparkSession.sessionState.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      values.find { case (k, _) => columnEquals(k, f.name) }.map { case (_, v) =>
        v match {
          case v: jl.Float => fillCol[Float](f, v)
          case v: jl.Double => fillCol[Double](f, v)
          case v: jl.Long => fillCol[Long](f, v)
          case v: jl.Integer => fillCol[Integer](f, v)
          case v: jl.Boolean => fillCol[Boolean](f, v.booleanValue())
          case v: String => fillCol[String](f, v)
        }
      }.getOrElse(df.col(f.name))
    }
    df.select(projections : _*)
  }

  /**
   * Returns a [[Column]] expression that replaces null value in `col` with `replacement`.
   */
  private def fillCol[T](col: StructField, replacement: T): Column = {
    val quotedColName = "`" + col.name + "`"
    val colValue = col.dataType match {
      case DoubleType | FloatType =>
        nanvl(df.col(quotedColName), lit(null)) // nanvl only supports these types
      case _ => df.col(quotedColName)
    }
    coalesce(colValue, lit(replacement).cast(col.dataType)).as(col.name)
  }

  /**
   * Returns a [[Column]] expression that replaces value matching key in `replacementMap` with
   * value in `replacementMap`, using [[CaseWhen]].
   *
   * TODO: This can be optimized to use broadcast join when replacementMap is large.
   */
  private def replaceCol(col: StructField, replacementMap: Map[_, _]): Column = {
    val keyExpr = df.col(col.name).expr
    def buildExpr(v: Any) = Cast(Literal(v), keyExpr.dataType)
    val branches = replacementMap.flatMap { case (source, target) =>
      Seq(buildExpr(source), buildExpr(target))
    }.toSeq
    new Column(CaseKeyWhen(keyExpr, branches :+ keyExpr)).as(col.name)
  }

  private def convertToDouble(v: Any): Double = v match {
    case v: Float => v.toDouble
    case v: Double => v
    case v: Long => v.toDouble
    case v: Int => v.toDouble
    case v => throw new IllegalArgumentException(
      s"Unsupported value type ${v.getClass.getName} ($v).")
  }

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric, string columns. If a specified column is not a numeric, string
   * or boolean column it is ignored.
   */
  private def fillValue[T](value: T, cols: Seq[String]): DataFrame = {
    // the fill[T] which T is  Long/Double,
    // should apply on all the NumericType Column, for example:
    // val input = Seq[(java.lang.Integer, java.lang.Double)]((null, 164.3)).toDF("a","b")
    // input.na.fill(3.1)
    // the result is (3,164.3), not (null, 164.3)
    val targetType = value match {
      case _: Double | _: Long => NumericType
      case _: String => StringType
      case _: Boolean => BooleanType
      case _ => throw new IllegalArgumentException(
        s"Unsupported value type ${value.getClass.getName} ($value).")
    }

    val columnEquals = df.sparkSession.sessionState.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      val typeMatches = (targetType, f.dataType) match {
        case (NumericType, dt) => dt.isInstanceOf[NumericType]
        case (StringType, dt) => dt == StringType
        case (BooleanType, dt) => dt == BooleanType
        case _ =>
          throw new IllegalArgumentException(s"$targetType is not matched at fillValue")
      }
      // Only fill if the column is part of the cols list.
      if (typeMatches && cols.exists(col => columnEquals(f.name, col))) {
        fillCol[T](f, value)
      } else {
        df.col(f.name)
      }
    }
    df.select(projections : _*)
  }
}
