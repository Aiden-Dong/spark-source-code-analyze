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

package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the basic expression abstract classes in Catalyst.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * An expression in Catalyst.
 *
 * If an expression wants to be exposed in the function registry (so users can call it with
 * "name(arguments...)", the concrete implementation must be a case class whose constructor
 * arguments are all Expressions types. See [[Substring]] for an example.
 *
 * There are a few important traits:
 *
 * - [[Nondeterministic]]: an expression that is not deterministic.
 * - [[Unevaluable]]: an expression that is not supposed to be evaluated.
 * - [[CodegenFallback]]: an expression that does not have code gen implemented and falls back to
 *                        interpreted mode.
 *
 * - [[LeafExpression]]: an expression that has no child.
 * - [[UnaryExpression]]: an expression that has one child.
 * - [[BinaryExpression]]: an expression that has two children.
 * - [[TernaryExpression]]: an expression that has three children.
 * - [[BinaryOperator]]: a special case of [[BinaryExpression]] that requires two children to have
 *                       the same output data type.
 *
 */
abstract class Expression extends TreeNode[Expression] {

  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed.
   *
   * The following conditions are used to determine suitability for constant folding:
   *  - A [[Coalesce]] is foldable if all of its children are foldable
   *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
   *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
   *  - A [[Literal]] is foldable
   *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
   */
  def foldable: Boolean = false

  /**
   * Returns true when the current expression always return the same result for fixed inputs from
   * children.
   *
   * Note that this means that an expression should be considered as non-deterministic if:
   * - it relies on some mutable internal state, or
   * - it relies on some implicit input that is not part of the children expression list.
   * - it has non-deterministic child or children.
   * - it assumes the input satisfies some certain condition via the child operator.
   *
   * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
   * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
   */
  lazy val deterministic: Boolean = children.forall(_.deterministic)

  def nullable: Boolean

  def references: AttributeSet = AttributeSet(children.flatMap(_.references.iterator))

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: InternalRow = null): Any

  /**
   * Returns an [[ExprCode]], that contains the Java source code to generate the result of
   * evaluating the expression on an input row.
   *
   * @param ctx a [[CodegenContext]]
   * @return [[ExprCode]]
   */
  def genCode(ctx: CodegenContext): ExprCode = {
    ctx.subExprEliminationExprs.get(this).map { subExprState =>
      // This expression is repeated which means that the code to evaluate it has already been added
      // as a function before. In that case, we just re-use it.
      ExprCode(ctx.registerComment(this.toString), subExprState.isNull, subExprState.value)
    }.getOrElse {
      val isNull = ctx.freshName("isNull")
      val value = ctx.freshName("value")
      val eval = doGenCode(ctx, ExprCode(
        JavaCode.isNullVariable(isNull),
        JavaCode.variable(value, dataType)))
      reduceCodeSize(ctx, eval)
      if (eval.code.toString.nonEmpty) {
        // Add `this` in the comment.
        eval.copy(code = ctx.registerComment(this.toString) + eval.code)
      } else {
        eval
      }
    }
  }

  private def reduceCodeSize(ctx: CodegenContext, eval: ExprCode): Unit = {
    // TODO: support whole stage codegen too
    if (eval.code.length > 1024 && ctx.INPUT_ROW != null && ctx.currentVars == null) {
      val setIsNull = if (!eval.isNull.isInstanceOf[LiteralValue]) {
        val globalIsNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "globalIsNull")
        val localIsNull = eval.isNull
        eval.isNull = JavaCode.isNullGlobal(globalIsNull)
        s"$globalIsNull = $localIsNull;"
      } else {
        ""
      }

      val javaType = CodeGenerator.javaType(dataType)
      val newValue = ctx.freshName("value")

      val funcName = ctx.freshName(nodeName)
      val funcFullName = ctx.addNewFunction(funcName,
        s"""
           |private $javaType $funcName(InternalRow ${ctx.INPUT_ROW}) {
           |  ${eval.code}
           |  $setIsNull
           |  return ${eval.value};
           |}
           """.stripMargin)

      eval.value = JavaCode.variable(newValue, dataType)
      eval.code = code"$javaType $newValue = $funcFullName(${ctx.INPUT_ROW});"
    }
  }

  /**
   * Returns Java source code that can be compiled to evaluate this expression.
   * The default behavior is to call the eval method of the expression. Concrete expression
   * implementations should override this to do actual code generation.
   *
   * @param ctx a [[CodegenContext]]
   * @param ev an [[ExprCode]] with unique terms.
   * @return an [[ExprCode]] containing the Java source code to generate the given expression
   */
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode

  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and input data types checking passed, and `false` if it still contains any unresolved
   * placeholders or has data types mismatch.
   * Implementations of expressions should override this if the resolution of this type of
   * expression involves more than just the resolution of its children and type checking.
   */
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Returns an expression where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but removes cosmetic variations (case sensitivity, ordering for
   * commutative operations, etc.)  See [[Canonicalize]] for more details.
   *
   * `deterministic` expressions where `this.canonicalized == other.canonicalized` will always
   * evaluate to the same result.
   */
  lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    Canonicalize.execute(withNewChildren(canonicalizedChildren))
  }

  /**
   * Returns true when two expressions will always compute the same result, even if they differ
   * cosmetically (i.e. capitalization of names in attributes may be different).
   *
   * See [[Canonicalize]] for more details.
   */
  def semanticEquals(other: Expression): Boolean =
    deterministic && other.deterministic && canonicalized == other.canonicalized

  /**
   * Returns a `hashCode` for the calculation performed by this expression. Unlike the standard
   * `hashCode`, an attempt has been made to eliminate cosmetic differences.
   *
   * See [[Canonicalize]] for more details.
   */
  def semanticHash(): Int = canonicalized.hashCode()

  /**
   * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
   * or returns a `TypeCheckResult` with an error message if invalid.
   * Note: it's not valid to call this method until `childrenResolved == true`.
   */
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /**
   * Returns a user-facing string representation of this expression's name.
   * This should usually match the name of the function in SQL.
   */
  def prettyName: String = nodeName.toLowerCase(Locale.ROOT)

  protected def flatArguments: Iterator[Any] = productIterator.flatMap {
    case t: Traversable[_] => t
    case single => single :: Nil
  }

  // Marks this as final, Expression.verboseString should never be called, and thus shouldn't be
  // overridden by concrete classes.
  final override def verboseString: String = simpleString

  override def simpleString: String = toString

  override def toString: String = prettyName + Utils.truncatedString(
    flatArguments.toSeq, "(", ", ", ")")

  /**
   * Returns SQL representation of this expression.  For expressions extending [[NonSQLExpression]],
   * this method may return an arbitrary user facing string.
   */
  def sql: String = {
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }
}


/**
 * An expression that cannot be evaluated. Some expressions don't live past analysis or optimization
 * time (e.g. Star). This trait is used by those expressions.
 */
trait Unevaluable extends Expression {

  final override def eval(input: InternalRow = null): Any =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
}


/**
 * An expression that gets replaced at runtime (currently by the optimizer) into a different
 * expression for evaluation. This is mainly used to provide compatibility with other databases.
 * For example, we use this to support "nvl" by replacing it with "coalesce".
 *
 * A RuntimeReplaceable should have the original parameters along with a "child" expression in the
 * case class constructor, and define a normal constructor that accepts only the original
 * parameters. For an example, see [[Nvl]]. To make sure the explain plan and expression SQL
 * works correctly, the implementation should also override flatArguments method and sql method.
 */
trait RuntimeReplaceable extends UnaryExpression with Unevaluable {
  override def nullable: Boolean = child.nullable
  override def foldable: Boolean = child.foldable
  override def dataType: DataType = child.dataType
  // As this expression gets replaced at optimization with its `child" expression,
  // two `RuntimeReplaceable` are considered to be semantically equal if their "child" expressions
  // are semantically equal.
  override lazy val canonicalized: Expression = child.canonicalized
}


/**
 * Expressions that don't have SQL representation should extend this trait.  Examples are
 * `ScalaUDF`, `ScalaUDAF`, and object expressions like `MapObjects` and `Invoke`.
 */
trait NonSQLExpression extends Expression {
  final override def sql: String = {
    transform {
      case a: Attribute => new PrettyAttribute(a)
      case a: Alias => PrettyAttribute(a.sql, a.dataType)
    }.toString
  }
}


/**
 * An expression that is nondeterministic.
 */
trait Nondeterministic extends Expression {
  final override lazy val deterministic: Boolean = false
  final override def foldable: Boolean = false

  @transient
  private[this] var initialized = false

  /**
   * Initializes internal states given the current partition index and mark this as initialized.
   * Subclasses should override [[initializeInternal()]].
   */
  final def initialize(partitionIndex: Int): Unit = {
    initializeInternal(partitionIndex)
    initialized = true
  }

  protected def initializeInternal(partitionIndex: Int): Unit

  /**
   * @inheritdoc
   * Throws an exception if [[initialize()]] is not called yet.
   * Subclasses should override [[evalInternal()]].
   */
  final override def eval(input: InternalRow = null): Any = {
    require(initialized,
      s"Nondeterministic expression ${this.getClass.getName} should be initialized before eval.")
    evalInternal(input)
  }

  protected def evalInternal(input: InternalRow): Any
}

/**
 * An expression that contains mutable state. A stateful expression is always non-deterministic
 * because the results it produces during evaluation are not only dependent on the given input
 * but also on its internal state.
 *
 * The state of the expressions is generally not exposed in the parameter list and this makes
 * comparing stateful expressions problematic because similar stateful expressions (with the same
 * parameter list) but with different internal state will be considered equal. This is especially
 * problematic during tree transformations. In order to counter this the `fastEquals` method for
 * stateful expressions only returns `true` for the same reference.
 *
 * A stateful expression should never be evaluated multiple times for a single row. This should
 * only be a problem for interpreted execution. This can be prevented by creating fresh copies
 * of the stateful expression before execution, these can be made using the `freshCopy` function.
 */
trait Stateful extends Nondeterministic {
  /**
   * Return a fresh uninitialized copy of the stateful expression.
   */
  def freshCopy(): Stateful

  /**
   * Only the same reference is considered equal.
   */
  override def fastEquals(other: TreeNode[_]): Boolean = this eq other
}

/**
 * A leaf expression, i.e. one without any child expressions.
 */
abstract class LeafExpression extends Expression {

  override final def children: Seq[Expression] = Nil
}


/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
abstract class UnaryExpression extends Expression {

  def child: Expression

  override final def children: Seq[Expression] = child :: Nil

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  /**
   * Default behavior of evaluation according to the default nullability of UnaryExpression.
   * If subclass of UnaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input: Any): Any =
    sys.error(s"UnaryExpressions must override either eval or nullSafeEval")

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * As an example, the following does a boolean inversion (i.e. NOT).
   * {{{
   *   defineCodeGen(ctx, ev, c => s"!($c)")
   * }}}
   *
   * @param f function that accepts a variable name and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = ${f(eval)};"
    })
  }

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * @param f function that accepts the non-null evaluation result name of child and returns Java
   *          code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    val childGen = child.genCode(ctx)
    val resultCode = f(childGen.value)

    if (nullable) {
      val nullSafeEval = ctx.nullSafeExec(child.nullable, childGen.isNull)(resultCode)
      ev.copy(code = code"""
        ${childGen.code}
        boolean ${ev.isNull} = ${childGen.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${childGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class BinaryExpression extends Expression {

  def left: Expression
  def right: Expression

  override final def children: Seq[Expression] = Seq(left, right)

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.value} = ${f(eval1, eval2)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val resultCode = f(leftGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(left.nullable, leftGen.isNull) {
          rightGen.code + ctx.nullSafeExec(right.nullable, rightGen.isNull) {
            s"""
              ${ev.isNull} = false; // resultCode could change nullability.
              $resultCode
            """
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}


/**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to be of the same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes {

  /**
   * Expected input type from both left/right child expressions, similar to the
   * [[ImplicitCastInputTypes]] trait.
   */
  def inputType: AbstractDataType

  def symbol: String

  def sqlOperator: String = symbol

  override def toString: String = s"($left $symbol $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType)

  override def checkInputDataTypes(): TypeCheckResult = {
    // First check whether left and right have the same type, then check if the type is acceptable.
    if (!left.dataType.sameType(right.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${left.dataType.catalogString} and ${right.dataType.catalogString}).")
    } else if (!inputType.acceptsType(left.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"'$sql' requires ${inputType.simpleString} type," +
        s" not ${left.dataType.catalogString}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def sql: String = s"(${left.sql} $sqlOperator ${right.sql})"
}


object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
}

/**
 * An expression with three inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class TernaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of TernaryExpression.
   * If subclass of TernaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val value1 = exprs(0).eval(input)
    if (value1 != null) {
      val value2 = exprs(1).eval(input)
      if (value2 != null) {
        val value3 = exprs(2).eval(input)
        if (value3 != null) {
          return nullSafeEval(value1, value2, value3)
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of TernaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    sys.error(s"TernaryExpressions must override either eval or nullSafeEval")

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts three variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3) => {
      s"${ev.value} = ${f(eval1, eval2, eval3)};"
    })
  }

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 3 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    val leftGen = children(0).genCode(ctx)
    val midGen = children(1).genCode(ctx)
    val rightGen = children(2).genCode(ctx)
    val resultCode = f(leftGen.value, midGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(children(0).nullable, leftGen.isNull) {
          midGen.code + ctx.nullSafeExec(children(1).nullable, midGen.isNull) {
            rightGen.code + ctx.nullSafeExec(children(2).nullable, rightGen.isNull) {
              s"""
                ${ev.isNull} = false; // resultCode could change nullability.
                $resultCode
              """
            }
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${midGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * A trait resolving nullable, containsNull, valueContainsNull flags of the output date type.
 * This logic is usually utilized by expressions combining data from multiple child expressions
 * of non-primitive types (e.g. [[CaseWhen]]).
 */
trait ComplexTypeMergingExpression extends Expression {

  /**
   * A collection of data types used for resolution the output type of the expression. By default,
   * data types of all child expressions. The collection must not be empty.
   */
  @transient
  lazy val inputTypesForMerging: Seq[DataType] = children.map(_.dataType)

  def dataTypeCheck: Unit = {
    require(
      inputTypesForMerging.nonEmpty,
      "The collection of input data types must not be empty.")
    require(
      TypeCoercion.haveSameType(inputTypesForMerging),
      "All input types must be the same except nullable, containsNull, valueContainsNull flags." +
        s" The input types found are\n\t${inputTypesForMerging.mkString("\n\t")}")
  }

  override def dataType: DataType = {
    dataTypeCheck
    inputTypesForMerging.reduceLeft(TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(_, _).get)
  }
}

/**
 * Common base trait for user-defined functions, including UDF/UDAF/UDTF of different languages
 * and Hive function wrappers.
 */
trait UserDefinedExpression
