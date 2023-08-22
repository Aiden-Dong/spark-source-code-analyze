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

import java.util.{Objects, UUID}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types._

object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  private[expressions] val jvmId = UUID.randomUUID()
  def newExprId: ExprId = ExprId(curId.getAndIncrement(), jvmId)
  def unapply(expr: NamedExpression): Option[(String, DataType)] = Some((expr.name, expr.dataType))
}

/**
 * A globally unique id for a given named expression.
 * Used to identify which attribute output by a relation is being
 * referenced in a subsequent computation.
 *
 * The `id` field is unique within a given JVM, while the `uuid` is used to uniquely identify JVMs.
 */
case class ExprId(id: Long, jvmId: UUID) {

  override def equals(other: Any): Boolean = other match {
    case ExprId(id, jvmId) => this.id == id && this.jvmId == jvmId
    case _ => false
  }

  override def hashCode(): Int = id.hashCode()

}

object ExprId {
  def apply(id: Long): ExprId = ExprId(id, NamedExpression.jvmId)
}

/**
 * An [[Expression]] that is named.
 */
trait NamedExpression extends Expression {

  /** We should never fold named expressions in order to not remove the alias. */
  override def foldable: Boolean = false

  def name: String
  def exprId: ExprId

  /**
   * Returns a dot separated fully qualified name for this attribute.  Given that there can be
   * multiple qualifiers, it is possible that there are other possible way to refer to this
   * attribute.
   */
  def qualifiedName: String = (qualifier :+ name).mkString(".")

  /**
   * Optional qualifier for the expression.
   * Qualifier can also contain the fully qualified information, for e.g, Sequence of string
   * containing the database and the table name
   *
   * For now, since we do not allow using original table name to qualify a column name once the
   * table is aliased, this can only be:
   *
   * 1. Empty Seq: when an attribute doesn't have a qualifier,
   *    e.g. top level attributes aliased in the SELECT clause, or column from a LocalRelation.
   * 2. Seq with a Single element: either the table name or the alias name of the table.
   * 3. Seq with 2 elements: database name and table name
   */
  def qualifier: Seq[String]

  def toAttribute: Attribute

  /** Returns the metadata when an expression is a reference to another expression with metadata. */
  def metadata: Metadata = Metadata.empty

  /** Returns a copy of this expression with a new `exprId`. */
  def newInstance(): NamedExpression

  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _ => ""
      }
    } else {
      ""
    }
}

abstract class Attribute extends LeafExpression with NamedExpression with NullIntolerant {

  override def references: AttributeSet = AttributeSet(this)

  def withNullability(newNullability: Boolean): Attribute
  def withQualifier(newQualifier: Seq[String]): Attribute
  def withName(newName: String): Attribute
  def withMetadata(newMetadata: Metadata): Attribute

  override def toAttribute: Attribute = this
  def newInstance(): Attribute

}

/**
 * Used to assign a new name to a computation.
 * For example the SQL expression "1 + 1 AS a" could be represented as follows:
 *  Alias(Add(Literal(1), Literal(1)), "a")()
 *
 * Note that exprId and qualifiers are in a separate parameter list because
 * we only pattern match on child and name.
 *
 * @param child The computation being performed
 * @param name The name to be associated with the result of computing [[child]].
 * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this
 *               alias. Auto-assigned if left blank.
 * @param qualifier An optional Seq of string that can be used to refer to this attribute in a
 *                  fully qualified way. Consider the examples tableName.name, subQueryAlias.name.
 *                  tableName and subQueryAlias are possible qualifiers.
 * @param explicitMetadata Explicit metadata associated with this alias that overwrites child's.
 */
case class Alias(child: Expression, name: String)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Seq[String] = Seq.empty,
    val explicitMetadata: Option[Metadata] = None)
  extends UnaryExpression with NamedExpression {

  // Alias(Generator, xx) need to be transformed into Generate(generator, ...)
  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !child.isInstanceOf[Generator]

  override def eval(input: InternalRow): Any = child.eval(input)

  /** Just a simple passthrough for code generation. */
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new IllegalStateException("Alias.doGenCode should not be called.")
  }

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def metadata: Metadata = {
    explicitMetadata.getOrElse {
      child match {
        case named: NamedExpression => named.metadata
        case _ => Metadata.empty
      }
    }
  }

  def newInstance(): NamedExpression =
    Alias(child, name)(qualifier = qualifier, explicitMetadata = explicitMetadata)

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable, metadata)(exprId, qualifier)
    } else {
      UnresolvedAttribute(name)
    }
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix$delaySuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: explicitMetadata :: Nil
  }

  override def hashCode(): Int = {
    val state = Seq(name, exprId, child, qualifier, explicitMetadata)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(other: Any): Boolean = other match {
    case a: Alias =>
      name == a.name && exprId == a.exprId && child == a.child && qualifier == a.qualifier &&
        explicitMetadata == a.explicitMetadata
    case _ => false
  }

  override def sql: String = {
    val qualifierPrefix = if (qualifier.nonEmpty) qualifier.mkString(".") + "." else ""
    s"${child.sql} AS $qualifierPrefix${quoteIdentifier(name)}"
  }
}

/**
 * A reference to an attribute produced by another operator in the tree.
 *
 * @param name The name of this attribute, should only be used during analysis or for debugging.
 * @param dataType The [[DataType]] of this attribute.
 * @param nullable True if null is a valid value for this attribute.
 * @param metadata The metadata of this attribute.
 * @param exprId A globally unique id used to check if different AttributeReferences refer to the
 *               same attribute.
 * @param qualifier An optional string that can be used to referred to this attribute in a fully
 *                  qualified way. Consider the examples tableName.name, subQueryAlias.name.
 *                  tableName and subQueryAlias are possible qualifiers.
 */
case class AttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Seq[String] = Seq.empty[String])
  extends Attribute with Unevaluable {

  // currently can only handle qualifier of length 2
  require(qualifier.length <= 2)
  /**
   * Returns true iff the expression id is the same for both attributes.
   */
  def sameRef(other: AttributeReference): Boolean = this.exprId == other.exprId

  override def equals(other: Any): Boolean = other match {
    case ar: AttributeReference =>
      name == ar.name && dataType == ar.dataType && nullable == ar.nullable &&
        metadata == ar.metadata && exprId == ar.exprId && qualifier == ar.qualifier
    case _ => false
  }

  override def semanticEquals(other: Expression): Boolean = other match {
    case ar: AttributeReference => sameRef(ar)
    case _ => false
  }

  override def semanticHash(): Int = {
    this.exprId.hashCode()
  }

  override def hashCode: Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + name.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + nullable.hashCode()
    h = h * 37 + metadata.hashCode()
    h = h * 37 + exprId.hashCode()
    h = h * 37 + qualifier.hashCode()
    h
  }

  override def newInstance(): AttributeReference =
    AttributeReference(name, dataType, nullable, metadata)(qualifier = qualifier)

  /**
   * Returns a copy of this [[AttributeReference]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): AttributeReference = {
    if (nullable == newNullability) {
      this
    } else {
      AttributeReference(name, dataType, newNullability, metadata)(exprId, qualifier)
    }
  }

  override def withName(newName: String): AttributeReference = {
    if (name == newName) {
      this
    } else {
      AttributeReference(newName, dataType, nullable, metadata)(exprId, qualifier)
    }
  }

  /**
   * Returns a copy of this [[AttributeReference]] with new qualifier.
   */
  override def withQualifier(newQualifier: Seq[String]): AttributeReference = {
    if (newQualifier == qualifier) {
      this
    } else {
      AttributeReference(name, dataType, nullable, metadata)(exprId, newQualifier)
    }
  }

  def withExprId(newExprId: ExprId): AttributeReference = {
    if (exprId == newExprId) {
      this
    } else {
      AttributeReference(name, dataType, nullable, metadata)(newExprId, qualifier)
    }
  }

  override def withMetadata(newMetadata: Metadata): Attribute = {
    AttributeReference(name, dataType, nullable, newMetadata)(exprId, qualifier)
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: Nil
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  override def toString: String = s"$name#${exprId.id}$typeSuffix$delaySuffix"

  // Since the expression id is not in the first constructor it is missing from the default
  // tree string.
  override def simpleString: String = s"$name#${exprId.id}: ${dataType.simpleString}"

  override def sql: String = {
    val qualifierPrefix = if (qualifier.nonEmpty) qualifier.mkString(".") + "." else ""
    s"$qualifierPrefix${quoteIdentifier(name)}"
  }
}

/**
 * A place holder used when printing expressions without debugging information such as the
 * expression id or the unresolved indicator.
 */
case class PrettyAttribute(
    name: String,
    dataType: DataType = NullType)
  extends Attribute with Unevaluable {

  def this(attribute: Attribute) = this(attribute.name, attribute match {
    case a: AttributeReference => a.dataType
    case a: PrettyAttribute => a.dataType
    case _ => NullType
  })

  override def toString: String = name
  override def sql: String = toString

  override def withNullability(newNullability: Boolean): Attribute =
    throw new UnsupportedOperationException
  override def newInstance(): Attribute = throw new UnsupportedOperationException
  override def withQualifier(newQualifier: Seq[String]): Attribute =
    throw new UnsupportedOperationException
  override def withName(newName: String): Attribute = throw new UnsupportedOperationException
  override def withMetadata(newMetadata: Metadata): Attribute =
    throw new UnsupportedOperationException
  override def qualifier: Seq[String] = throw new UnsupportedOperationException
  override def exprId: ExprId = throw new UnsupportedOperationException
  override def nullable: Boolean = true
}

/**
 * A place holder used to hold a reference that has been resolved to a field outside of the current
 * plan. This is used for correlated subqueries.
 */
case class OuterReference(e: NamedExpression)
  extends LeafExpression with NamedExpression with Unevaluable {
  override def dataType: DataType = e.dataType
  override def nullable: Boolean = e.nullable
  override def prettyName: String = "outer"

  override def name: String = e.name
  override def qualifier: Seq[String] = e.qualifier
  override def exprId: ExprId = e.exprId
  override def toAttribute: Attribute = e.toAttribute
  override def newInstance(): NamedExpression = OuterReference(e.newInstance())
}

object VirtualColumn {
  // The attribute name used by Hive, which has different result than Spark, deprecated.
  val hiveGroupingIdName: String = "grouping__id"
  val groupingIdName: String = "spark_grouping_id"
  val groupingIdAttribute: UnresolvedAttribute = UnresolvedAttribute(groupingIdName)
}
