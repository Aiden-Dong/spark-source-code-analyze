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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.LogicalPlanStats
import org.apache.spark.sql.types.StructType


abstract class LogicalPlan
  extends QueryPlan[LogicalPlan]
  with AnalysisHelper
  with LogicalPlanStats
  with QueryPlanConstraints
  with Logging {

  /** Returns true if this subtree has data from a streaming data source. */
  def isStreaming: Boolean = children.exists(_.isStreaming == true)

  override def verboseStringWithSuffix: String = {
    super.verboseString + statsCache.map(", " + _.toString).getOrElse("")
  }

  /**
   * 返回该计划可以计算的最大行数。
   *
   * 任何可以将Limit推迟的运算符都应该重写这个函数（例如，Union）。
   * 任何可以通过推迟Limit的运算的运算符都应该重写这个函数（例如，Project）。
   */
  def maxRows: Option[Long] = None

  /**
   * 返回该计划在每个分区上可能计算的最大行数。.
   */
  def maxRowsPerPartition: Option[Long] = maxRows

  /**
   * 如果此表达式及其所有子表达式都已解析为特定模式，则返回true
   * 如果仍包含任何未解析的占位符，则返回false。
   * LogicalPlan的实现可以重写此方法
   * 例如 org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation 返回false
   **/
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * 如果此查询计划的所有子项都已解析，则返回true.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * 将给定的模式解析为此查询计划中的具体[[Attribute]]引用。
   * 此函数应仅在已分析的计划上调用，
   * 因为对于未解析的[[Attribute]]，它将抛出[[AnalysisException]]。
   */
  def resolve(schema: StructType, resolver: Resolver): Seq[Attribute] = {
    schema.map { field =>
      resolve(field.name :: Nil, resolver).map {
        case a: AttributeReference => a
        case _ => sys.error(s"can not handle nested schema yet...  plan $this")
      }.getOrElse {
        throw new AnalysisException(
          s"Unable to resolve ${field.name} given [${output.map(_.name).mkString(", ")}]")
      }
    }
  }

  private[this] lazy val childAttributes = AttributeSeq(children.flatMap(_.output))

  private[this] lazy val outputAttributes = AttributeSeq(output)

  /**
   * 可选地使用此LogicalPlan的所有子节点的输入将给定的字符串解析为[[NamedExpression]]。
   * 属性表示为以下形式的字符串：[scope].AttributeName.[nested].[fields]...。
   */
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    childAttributes.resolve(nameParts, resolver)

  /**
   * 可选地使用此LogicalPlan的输出将给定的字符串解析为[[NamedExpression]]
   * 属性表示为以下形式的字符串：[scope].AttributeName.[nested].[fields]...
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    outputAttributes.resolve(nameParts, resolver)

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   */
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    outputAttributes.resolve(UnresolvedAttribute.parseAttributeName(name), resolver)
  }

  /**
   * Refreshes (or invalidates) any metadata/data cached in the plan recursively.
   */
  def refresh(): Unit = children.foreach(_.refresh())

  /**
   * Returns the output ordering that this plan generates.
   */
  def outputOrdering: Seq[SortOrder] = Nil
}

/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan {
  override final def children: Seq[LogicalPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet

  /** Leaf nodes that can survive analysis must define their own statistics. */
  def computeStats(): Statistics = throw new UnsupportedOperationException
}

/**
 * A logical plan node with single child.
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override final def children: Seq[LogicalPlan] = child :: Nil

  /**
   * Generates an additional set of aliased constraints by replacing the original constraint
   * expressions with the corresponding alias
   */
  protected def getAliasedConstraints(projectList: Seq[NamedExpression]): Set[Expression] = {
    var allConstraints = child.constraints.asInstanceOf[Set[Expression]]
    projectList.foreach {
      case a @ Alias(l: Literal, _) =>
        allConstraints += EqualNullSafe(a.toAttribute, l)
      case a @ Alias(e, _) =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints -- child.constraints
  }

  override protected def validConstraints: Set[Expression] = child.constraints
}

/**
 * A logical plan node with a left and right child.
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override final def children: Seq[LogicalPlan] = Seq(left, right)
}

abstract class OrderPreservingUnaryNode extends UnaryNode {
  override final def outputOrdering: Seq[SortOrder] = child.outputOrdering
}
