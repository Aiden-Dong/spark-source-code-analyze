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
import scala.language.implicitConversions

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

private[sql] object Column {

  def apply(colName: String): Column = new Column(colName)

  def apply(expr: Expression): Column = new Column(expr)

  def unapply(col: Column): Option[Expression] = Some(col.expr)

  private[sql] def generateAlias(e: Expression): String = {
    e match {
      case a: AggregateExpression if a.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
        a.aggregateFunction.toString
      case expr => toPrettySQL(expr)
    }
  }
}

/**
 * A [[Column]] where an [[Encoder]] has been given for the expected input and return type.
 * To create a [[TypedColumn]], use the `as` function on a [[Column]].
 *
 * @tparam T The input type expected for this expression.  Can be `Any` if the expression is type
 *           checked by the analyzer instead of the compiler (i.e. `expr("sum(...)")`).
 * @tparam U The output type of this column.
 *
 * @since 1.6.0
 */
@InterfaceStability.Stable
class TypedColumn[-T, U](
    expr: Expression,
    private[sql] val encoder: ExpressionEncoder[U])
  extends Column(expr) {

  /**
   * Inserts the specific input type and schema into any expressions that are expected to operate
   * on a decoded object.
   */
  private[sql] def withInputType(
      inputEncoder: ExpressionEncoder[_],
      inputAttributes: Seq[Attribute]): TypedColumn[T, U] = {
    val unresolvedDeserializer = UnresolvedDeserializer(inputEncoder.deserializer, inputAttributes)
    val newExpr = expr transform {
      case ta: TypedAggregateExpression if ta.inputDeserializer.isEmpty =>
        ta.withInputInfo(
          deser = unresolvedDeserializer,
          cls = inputEncoder.clsTag.runtimeClass,
          schema = inputEncoder.schema)
    }
    new TypedColumn[T, U](newExpr, encoder)
  }

  /**
   * Gives the [[TypedColumn]] a name (alias).
   * If the current `TypedColumn` has metadata associated with it, this metadata will be propagated
   * to the new column.
   *
   * @group expr_ops
   * @since 2.0.0
   */
  override def name(alias: String): TypedColumn[T, U] =
    new TypedColumn[T, U](super.name(alias).expr, encoder)

}

/**
 * A column that will be computed based on the data in a `DataFrame`.
 *
 * A new column can be constructed based on the input columns present in a DataFrame:
 *
 * {{{
 *   df("columnName")            // On a specific `df` DataFrame.
 *   col("columnName")           // A generic column not yet associated with a DataFrame.
 *   col("columnName.field")     // Extracting a struct field
 *   col("`a.column.with.dots`") // Escape `.` in column names.
 *   $"columnName"               // Scala short hand for a named column.
 * }}}
 *
 * [[Column]] objects can be composed to form complex expressions:
 *
 * {{{
 *   $"a" + 1
 *   $"a" === $"b"
 * }}}
 *
 * @note The internal Catalyst expression can be accessed via [[expr]], but this method is for
 * debugging purposes only and can change in any future Spark releases.
 *
 * @groupname java_expr_ops Java-specific expression operators
 * @groupname expr_ops Expression operators
 * @groupname df_ops DataFrame functions
 * @groupname Ungrouped Support functions for DataFrames
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
class Column(val expr: Expression) extends Logging {

  def this(name: String) = this(name match {
    case "*" => UnresolvedStar(None)
    case _ if name.endsWith(".*") =>
      val parts = UnresolvedAttribute.parseAttributeName(name.substring(0, name.length - 2))
      UnresolvedStar(Some(parts))
    case _ => UnresolvedAttribute.quotedString(name)
  })

  override def toString: String = toPrettySQL(expr)

  override def equals(that: Any): Boolean = that match {
    case that: Column => that.expr.equals(this.expr)
    case _ => false
  }

  override def hashCode: Int = this.expr.hashCode()

  /** Creates a column based on the given expression. */
  private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  /**
   * Returns the expression for this column either with an existing or auto assigned name.
   */
  private[sql] def named: NamedExpression = expr match {
    // Wrap UnresolvedAttribute with UnresolvedAlias, as when we resolve UnresolvedAttribute, we
    // will remove intermediate Alias for ExtractValue chain, and we need to alias it again to
    // make it a NamedExpression.
    case u: UnresolvedAttribute => UnresolvedAlias(u)

    case u: UnresolvedExtractValue => UnresolvedAlias(u)

    case expr: NamedExpression => expr

    // Leave an unaliased generator with an empty list of names since the analyzer will generate
    // the correct defaults after the nested expression's type has been resolved.
    case g: Generator => MultiAlias(g, Nil)

    case func: UnresolvedFunction => UnresolvedAlias(func, Some(Column.generateAlias))

    // If we have a top level Cast, there is a chance to give it a better alias, if there is a
    // NamedExpression under this Cast.
    case c: Cast =>
      c.transformUp {
        case c @ Cast(_: NamedExpression, _, _) => UnresolvedAlias(c)
      } match {
        case ne: NamedExpression => ne
        case _ => Alias(expr, toPrettySQL(expr))()
      }

    case a: AggregateExpression if a.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
      UnresolvedAlias(a, Some(Column.generateAlias))

    // Wait until the struct is resolved. This will generate a nicer looking alias.
    case struct: CreateNamedStructLike => UnresolvedAlias(struct)

    case expr: Expression => Alias(expr, toPrettySQL(expr))()
  }

  /**
   * Provides a type hint about the expected return value of this column.  This information can
   * be used by operations such as `select` on a [[Dataset]] to automatically convert the
   * results into the correct JVM types.
   * @since 1.6.0
   */
  def as[U : Encoder]: TypedColumn[Any, U] = new TypedColumn[Any, U](expr, encoderFor[U])

  /**
   * Extracts a value or values from a complex type.
   * The following types of extraction are supported:
   * <ul>
   * <li>Given an Array, an integer ordinal can be used to retrieve a single value.</li>
   * <li>Given a Map, a key of the correct type can be used to retrieve an individual value.</li>
   * <li>Given a Struct, a string fieldName can be used to extract that field.</li>
   * <li>Given an Array of Structs, a string fieldName can be used to extract filed
   *    of every struct in that array, and return an Array of fields.</li>
   * </ul>
   * @group expr_ops
   * @since 1.4.0
   */
  def apply(extraction: Any): Column = withExpr {
    UnresolvedExtractValue(expr, lit(extraction).expr)
  }

  /**
   * Unary minus, i.e. negate the expression.
   * {{{
   *   // Scala: select the amount column and negates all values.
   *   df.select( -df("amount") )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.select( negate(col("amount") );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def unary_- : Column = withExpr { UnaryMinus(expr) }

  /**
   * Inversion of boolean expression, i.e. NOT.
   * {{{
   *   // Scala: select rows that are not active (isActive === false)
   *   df.filter( !df("isActive") )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( not(df.col("isActive")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def unary_! : Column = withExpr { Not(expr) }

  /**
   * Equality test.
   * {{{
   *   // Scala:
   *   df.filter( df("colA") === df("colB") )
   *
   *   // Java
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").equalTo(col("colB")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def === (other: Any): Column = withExpr {
    val right = lit(other).expr
    if (this.expr == right) {
      logWarning(
        s"Constructing trivially true equals predicate, '${this.expr} = $right'. " +
          "Perhaps you need to use aliases.")
    }
    EqualTo(expr, right)
  }

  /**
   * Equality test.
   * {{{
   *   // Scala:
   *   df.filter( df("colA") === df("colB") )
   *
   *   // Java
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").equalTo(col("colB")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def equalTo(other: Any): Column = this === other

  /**
   * Inequality test.
   * {{{
   *   // Scala:
   *   df.select( df("colA") =!= df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").notEqual(col("colB")) );
   * }}}
   *
   * @group expr_ops
   * @since 2.0.0
    */
  def =!= (other: Any): Column = withExpr{ Not(EqualTo(expr, lit(other).expr)) }

  /**
   * Inequality test.
   * {{{
   *   // Scala:
   *   df.select( df("colA") !== df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").notEqual(col("colB")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
    */
  @deprecated("!== does not have the same precedence as ===, use =!= instead", "2.0.0")
  def !== (other: Any): Column = this =!= other

  /**
   * Inequality test.
   * {{{
   *   // Scala:
   *   df.select( df("colA") !== df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").notEqual(col("colB")) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def notEqual(other: Any): Column = withExpr { Not(EqualTo(expr, lit(other).expr)) }

  /**
   * Greater than.
   * {{{
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") > 21 )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   people.select( people.col("age").gt(21) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def > (other: Any): Column = withExpr { GreaterThan(expr, lit(other).expr) }

  /**
   * Greater than.
   * {{{
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") > lit(21) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   people.select( people.col("age").gt(21) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def gt(other: Any): Column = this > other

  /**
   * Less than.
   * {{{
   *   // Scala: The following selects people younger than 21.
   *   people.select( people("age") < 21 )
   *
   *   // Java:
   *   people.select( people.col("age").lt(21) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def < (other: Any): Column = withExpr { LessThan(expr, lit(other).expr) }

  /**
   * Less than.
   * {{{
   *   // Scala: The following selects people younger than 21.
   *   people.select( people("age") < 21 )
   *
   *   // Java:
   *   people.select( people.col("age").lt(21) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def lt(other: Any): Column = this < other

  /**
   * Less than or equal to.
   * {{{
   *   // Scala: The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= 21 )
   *
   *   // Java:
   *   people.select( people.col("age").leq(21) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def <= (other: Any): Column = withExpr { LessThanOrEqual(expr, lit(other).expr) }

  /**
   * Less than or equal to.
   * {{{
   *   // Scala: The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= 21 )
   *
   *   // Java:
   *   people.select( people.col("age").leq(21) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def leq(other: Any): Column = this <= other

  /**
   * Greater than or equal to an expression.
   * {{{
   *   // Scala: The following selects people age 21 or older than 21.
   *   people.select( people("age") >= 21 )
   *
   *   // Java:
   *   people.select( people.col("age").geq(21) )
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def >= (other: Any): Column = withExpr { GreaterThanOrEqual(expr, lit(other).expr) }

  /**
   * Greater than or equal to an expression.
   * {{{
   *   // Scala: The following selects people age 21 or older than 21.
   *   people.select( people("age") >= 21 )
   *
   *   // Java:
   *   people.select( people.col("age").geq(21) )
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def geq(other: Any): Column = this >= other

  /**
   * Equality test that is safe for null values.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def <=> (other: Any): Column = withExpr {
    val right = lit(other).expr
    if (this.expr == right) {
      logWarning(
        s"Constructing trivially true equals predicate, '${this.expr} <=> $right'. " +
          "Perhaps you need to use aliases.")
    }
    EqualNullSafe(expr, right)
  }

  /**
   * Equality test that is safe for null values.
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def eqNullSafe(other: Any): Column = this <=> other

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions.
   * If otherwise is not defined at the end, null is returned for unmatched conditions.
   *
   * {{{
   *   // Example: encoding gender string column into integer.
   *
   *   // Scala:
   *   people.select(when(people("gender") === "male", 0)
   *     .when(people("gender") === "female", 1)
   *     .otherwise(2))
   *
   *   // Java:
   *   people.select(when(col("gender").equalTo("male"), 0)
   *     .when(col("gender").equalTo("female"), 1)
   *     .otherwise(2))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def when(condition: Column, value: Any): Column = this.expr match {
    case CaseWhen(branches, None) =>
      withExpr { CaseWhen(branches :+ ((condition.expr, lit(value).expr))) }
    case CaseWhen(branches, Some(_)) =>
      throw new IllegalArgumentException(
        "when() cannot be applied once otherwise() is applied")
    case _ =>
      throw new IllegalArgumentException(
        "when() can only be applied on a Column previously generated by when() function")
  }

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions.
   * If otherwise is not defined at the end, null is returned for unmatched conditions.
   *
   * {{{
   *   // Example: encoding gender string column into integer.
   *
   *   // Scala:
   *   people.select(when(people("gender") === "male", 0)
   *     .when(people("gender") === "female", 1)
   *     .otherwise(2))
   *
   *   // Java:
   *   people.select(when(col("gender").equalTo("male"), 0)
   *     .when(col("gender").equalTo("female"), 1)
   *     .otherwise(2))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def otherwise(value: Any): Column = this.expr match {
    case CaseWhen(branches, None) =>
      withExpr { CaseWhen(branches, Option(lit(value).expr)) }
    case CaseWhen(branches, Some(_)) =>
      throw new IllegalArgumentException(
        "otherwise() can only be applied once on a Column previously generated by when()")
    case _ =>
      throw new IllegalArgumentException(
        "otherwise() can only be applied on a Column previously generated by when()")
  }

  /**
   * True if the current column is between the lower bound and upper bound, inclusive.
   *
   * @group java_expr_ops
   * @since 1.4.0
   */
  def between(lowerBound: Any, upperBound: Any): Column = {
    (this >= lowerBound) && (this <= upperBound)
  }

  /**
   * True if the current expression is NaN.
   *
   * @group expr_ops
   * @since 1.5.0
   */
  def isNaN: Column = withExpr { IsNaN(expr) }

  /**
   * True if the current expression is null.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def isNull: Column = withExpr { IsNull(expr) }

  /**
   * True if the current expression is NOT null.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def isNotNull: Column = withExpr { IsNotNull(expr) }

  /**
   * Boolean OR.
   * {{{
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people.col("inSchool").or(people.col("isEmployed")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def || (other: Any): Column = withExpr { Or(expr, lit(other).expr) }

  /**
   * Boolean OR.
   * {{{
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people.col("inSchool").or(people.col("isEmployed")) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def or(other: Column): Column = this || other

  /**
   * Boolean AND.
   * {{{
   *   // Scala: The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") && people("isEmployed") )
   *
   *   // Java:
   *   people.select( people.col("inSchool").and(people.col("isEmployed")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def && (other: Any): Column = withExpr { And(expr, lit(other).expr) }

  /**
   * Boolean AND.
   * {{{
   *   // Scala: The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") && people("isEmployed") )
   *
   *   // Java:
   *   people.select( people.col("inSchool").and(people.col("isEmployed")) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def and(other: Column): Column = this && other

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // Scala: The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").plus(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def + (other: Any): Column = withExpr { Add(expr, lit(other).expr) }

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // Scala: The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").plus(people.col("weight")) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def plus(other: Any): Column = this + other

  /**
   * Subtraction. Subtract the other expression from this expression.
   * {{{
   *   // Scala: The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").minus(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def - (other: Any): Column = withExpr { Subtract(expr, lit(other).expr) }

  /**
   * Subtraction. Subtract the other expression from this expression.
   * {{{
   *   // Scala: The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").minus(people.col("weight")) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def minus(other: Any): Column = this - other

  /**
   * Multiplication of this expression and another expression.
   * {{{
   *   // Scala: The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").multiply(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def * (other: Any): Column = withExpr { Multiply(expr, lit(other).expr) }

  /**
   * Multiplication of this expression and another expression.
   * {{{
   *   // Scala: The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").multiply(people.col("weight")) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def multiply(other: Any): Column = this * other

  /**
   * Division this expression by another expression.
   * {{{
   *   // Scala: The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").divide(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def / (other: Any): Column = withExpr { Divide(expr, lit(other).expr) }

  /**
   * Division this expression by another expression.
   * {{{
   *   // Scala: The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").divide(people.col("weight")) );
   * }}}
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def divide(other: Any): Column = this / other

  /**
   * Modulo (a.k.a. remainder) expression.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def % (other: Any): Column = withExpr { Remainder(expr, lit(other).expr) }

  /**
   * Modulo (a.k.a. remainder) expression.
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def mod(other: Any): Column = this % other

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   *
   * Note: Since the type of the elements in the list are inferred only during the run time,
   * the elements will be "up-casted" to the most common type for comparison.
   * For eg:
   *   1) In the case of "Int vs String", the "Int" will be up-casted to "String" and the
   * comparison will look like "String vs String".
   *   2) In the case of "Float vs Double", the "Float" will be up-casted to "Double" and the
   * comparison will look like "Double vs Double"
   *
   * @group expr_ops
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def isin(list: Any*): Column = withExpr { In(expr, list.map(lit(_).expr)) }

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the provided collection.
   *
   * Note: Since the type of the elements in the collection are inferred only during the run time,
   * the elements will be "up-casted" to the most common type for comparison.
   * For eg:
   *   1) In the case of "Int vs String", the "Int" will be up-casted to "String" and the
   * comparison will look like "String vs String".
   *   2) In the case of "Float vs Double", the "Float" will be up-casted to "Double" and the
   * comparison will look like "Double vs Double"
   *
   * @group expr_ops
   * @since 2.4.0
   */
  def isInCollection(values: scala.collection.Iterable[_]): Column = isin(values.toSeq: _*)

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the provided collection.
   *
   * Note: Since the type of the elements in the collection are inferred only during the run time,
   * the elements will be "up-casted" to the most common type for comparison.
   * For eg:
   *   1) In the case of "Int vs String", the "Int" will be up-casted to "String" and the
   * comparison will look like "String vs String".
   *   2) In the case of "Float vs Double", the "Float" will be up-casted to "Double" and the
   * comparison will look like "Double vs Double"
   *
   * @group java_expr_ops
   * @since 2.4.0
   */
  def isInCollection(values: java.lang.Iterable[_]): Column = isInCollection(values.asScala)

  /**
   * SQL like expression. Returns a boolean column based on a SQL LIKE match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def like(literal: String): Column = withExpr { Like(expr, lit(literal).expr) }

  /**
   * SQL RLIKE expression (LIKE with Regex). Returns a boolean column based on a regex
   * match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def rlike(literal: String): Column = withExpr { RLike(expr, lit(literal).expr) }

  /**
   * An expression that gets an item at position `ordinal` out of an array,
   * or gets a value by key `key` in a `MapType`.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def getItem(key: Any): Column = withExpr { UnresolvedExtractValue(expr, Literal(key)) }

  /**
   * An expression that gets a field by name in a `StructType`.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def getField(fieldName: String): Column = withExpr {
    UnresolvedExtractValue(expr, Literal(fieldName))
  }

  /**
   * An expression that returns a substring.
   * @param startPos expression for the starting position.
   * @param len expression for the length of the substring.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def substr(startPos: Column, len: Column): Column = withExpr {
    Substring(expr, startPos.expr, len.expr)
  }

  /**
   * An expression that returns a substring.
   * @param startPos starting position.
   * @param len length of the substring.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def substr(startPos: Int, len: Int): Column = withExpr {
    Substring(expr, lit(startPos).expr, lit(len).expr)
  }

  /**
   * Contains the other element. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def contains(other: Any): Column = withExpr { Contains(expr, lit(other).expr) }

  /**
   * String starts with. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def startsWith(other: Column): Column = withExpr { StartsWith(expr, lit(other).expr) }

  /**
   * String starts with another string literal. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def startsWith(literal: String): Column = this.startsWith(lit(literal))

  /**
   * String ends with. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def endsWith(other: Column): Column = withExpr { EndsWith(expr, lit(other).expr) }

  /**
   * String ends with another string literal. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def endsWith(literal: String): Column = this.endsWith(lit(literal))

  /**
   * Gives the column an alias. Same as `as`.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".alias("colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def alias(alias: String): Column = name(alias)

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column.  If this not desired, use `as` with explicitly empty metadata.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def as(alias: String): Column = name(alias)

  /**
   * (Scala-specific) Assigns the given aliases to the results of a table generating function.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select(explode($"myMap").as("key" :: "value" :: Nil))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def as(aliases: Seq[String]): Column = withExpr { MultiAlias(expr, aliases) }

  /**
   * Assigns the given aliases to the results of a table generating function.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select(explode($"myMap").as("key" :: "value" :: Nil))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def as(aliases: Array[String]): Column = withExpr { MultiAlias(expr, aliases) }

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as('colB))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column.  If this not desired, use `as` with explicitly empty metadata.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def as(alias: Symbol): Column = name(alias.name)

  /**
   * Gives the column an alias with metadata.
   * {{{
   *   val metadata: Metadata = ...
   *   df.select($"colA".as("colB", metadata))
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def as(alias: String, metadata: Metadata): Column = withExpr {
    Alias(expr, alias)(explicitMetadata = Some(metadata))
  }

  /**
   * Gives the column a name (alias).
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".name("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column.  If this not desired, use `as` with explicitly empty metadata.
   *
   * @group expr_ops
   * @since 2.0.0
   */
  def name(alias: String): Column = withExpr {
    expr match {
      case ne: NamedExpression => Alias(expr, alias)(explicitMetadata = Some(ne.metadata))
      case other => Alias(other, alias)()
    }
  }

  /**
   * Casts the column to a different data type.
   * {{{
   *   // Casts colA to IntegerType.
   *   import org.apache.spark.sql.types.IntegerType
   *   df.select(df("colA").cast(IntegerType))
   *
   *   // equivalent to
   *   df.select(df("colA").cast("int"))
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def cast(to: DataType): Column = withExpr { Cast(expr, to) }

  /**
   * Casts the column to a different data type, using the canonical string representation
   * of the type. The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`,
   * `float`, `double`, `decimal`, `date`, `timestamp`.
   * {{{
   *   // Casts colA to integer.
   *   df.select(df("colA").cast("int"))
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def cast(to: String): Column = cast(CatalystSqlParser.parseDataType(to))

  /**
   * Returns a sort expression based on the descending order of the column.
   * {{{
   *   // Scala
   *   df.sort(df("age").desc)
   *
   *   // Java
   *   df.sort(df.col("age").desc());
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def desc: Column = withExpr { SortOrder(expr, Descending) }

  /**
   * Returns a sort expression based on the descending order of the column,
   * and null values appear before non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order and null values appearing first.
   *   df.sort(df("age").desc_nulls_first)
   *
   *   // Java
   *   df.sort(df.col("age").desc_nulls_first());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def desc_nulls_first: Column = withExpr { SortOrder(expr, Descending, NullsFirst, Set.empty) }

  /**
   * Returns a sort expression based on the descending order of the column,
   * and null values appear after non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order and null values appearing last.
   *   df.sort(df("age").desc_nulls_last)
   *
   *   // Java
   *   df.sort(df.col("age").desc_nulls_last());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def desc_nulls_last: Column = withExpr { SortOrder(expr, Descending, NullsLast, Set.empty) }

  /**
   * Returns a sort expression based on ascending order of the column.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order.
   *   df.sort(df("age").asc)
   *
   *   // Java
   *   df.sort(df.col("age").asc());
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def asc: Column = withExpr { SortOrder(expr, Ascending) }

  /**
   * Returns a sort expression based on ascending order of the column,
   * and null values return before non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order and null values appearing first.
   *   df.sort(df("age").asc_nulls_first)
   *
   *   // Java
   *   df.sort(df.col("age").asc_nulls_first());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def asc_nulls_first: Column = withExpr { SortOrder(expr, Ascending, NullsFirst, Set.empty) }

  /**
   * Returns a sort expression based on ascending order of the column,
   * and null values appear after non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order and null values appearing last.
   *   df.sort(df("age").asc_nulls_last)
   *
   *   // Java
   *   df.sort(df.col("age").asc_nulls_last());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def asc_nulls_last: Column = withExpr { SortOrder(expr, Ascending, NullsLast, Set.empty) }

  /**
   * Prints the expression to the console for debugging purposes.
   *
   * @group df_ops
   * @since 1.3.0
   */
  def explain(extended: Boolean): Unit = {
    // scalastyle:off println
    if (extended) {
      println(expr)
    } else {
      println(expr.sql)
    }
    // scalastyle:on println
  }

  /**
   * Compute bitwise OR of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseOR($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def bitwiseOR(other: Any): Column = withExpr { BitwiseOr(expr, lit(other).expr) }

  /**
   * Compute bitwise AND of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseAND($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def bitwiseAND(other: Any): Column = withExpr { BitwiseAnd(expr, lit(other).expr) }

  /**
   * Compute bitwise XOR of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseXOR($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def bitwiseXOR(other: Any): Column = withExpr { BitwiseXor(expr, lit(other).expr) }

  /**
   * Defines a windowing column.
   *
   * {{{
   *   val w = Window.partitionBy("name").orderBy("id")
   *   df.select(
   *     sum("price").over(w.rangeBetween(Window.unboundedPreceding, 2)),
   *     avg("price").over(w.rowsBetween(Window.currentRow, 4))
   *   )
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def over(window: expressions.WindowSpec): Column = window.withAggregate(this)

  /**
   * Defines an empty analytic clause. In this case the analytic function is applied
   * and presented for all rows in the result set.
   *
   * {{{
   *   df.select(
   *     sum("price").over(),
   *     avg("price").over()
   *   )
   * }}}
   *
   * @group expr_ops
   * @since 2.0.0
   */
  def over(): Column = over(Window.spec)

}


/**
 * A convenient class used for constructing schema.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
class ColumnName(name: String) extends Column(name) {

  /**
   * Creates a new `StructField` of type boolean.
   * @since 1.3.0
   */
  def boolean: StructField = StructField(name, BooleanType)

  /**
   * Creates a new `StructField` of type byte.
   * @since 1.3.0
   */
  def byte: StructField = StructField(name, ByteType)

  /**
   * Creates a new `StructField` of type short.
   * @since 1.3.0
   */
  def short: StructField = StructField(name, ShortType)

  /**
   * Creates a new `StructField` of type int.
   * @since 1.3.0
   */
  def int: StructField = StructField(name, IntegerType)

  /**
   * Creates a new `StructField` of type long.
   * @since 1.3.0
   */
  def long: StructField = StructField(name, LongType)

  /**
   * Creates a new `StructField` of type float.
   * @since 1.3.0
   */
  def float: StructField = StructField(name, FloatType)

  /**
   * Creates a new `StructField` of type double.
   * @since 1.3.0
   */
  def double: StructField = StructField(name, DoubleType)

  /**
   * Creates a new `StructField` of type string.
   * @since 1.3.0
   */
  def string: StructField = StructField(name, StringType)

  /**
   * Creates a new `StructField` of type date.
   * @since 1.3.0
   */
  def date: StructField = StructField(name, DateType)

  /**
   * Creates a new `StructField` of type decimal.
   * @since 1.3.0
   */
  def decimal: StructField = StructField(name, DecimalType.USER_DEFAULT)

  /**
   * Creates a new `StructField` of type decimal.
   * @since 1.3.0
   */
  def decimal(precision: Int, scale: Int): StructField =
    StructField(name, DecimalType(precision, scale))

  /**
   * Creates a new `StructField` of type timestamp.
   * @since 1.3.0
   */
  def timestamp: StructField = StructField(name, TimestampType)

  /**
   * Creates a new `StructField` of type binary.
   * @since 1.3.0
   */
  def binary: StructField = StructField(name, BinaryType)

  /**
   * Creates a new `StructField` of type array.
   * @since 1.3.0
   */
  def array(dataType: DataType): StructField = StructField(name, ArrayType(dataType))

  /**
   * Creates a new `StructField` of type map.
   * @since 1.3.0
   */
  def map(keyType: DataType, valueType: DataType): StructField =
    map(MapType(keyType, valueType))

  def map(mapType: MapType): StructField = StructField(name, mapType)

  /**
   * Creates a new `StructField` of type struct.
   * @since 1.3.0
   */
  def struct(fields: StructField*): StructField = struct(StructType(fields))

  /**
   * Creates a new `StructField` of type struct.
   * @since 1.3.0
   */
  def struct(structType: StructType): StructField = StructField(name, structType)
}
