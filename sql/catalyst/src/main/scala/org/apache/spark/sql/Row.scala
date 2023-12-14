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
import scala.util.hashing.MurmurHash3

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType

/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
object Row {
  /**
   * 这个方法可以用于在schema匹配中从[[Row]]对象中提取字段。例如：
   * {{{
   * import org.apache.spark.sql._
   *
   * val pairs = sql("SELECT key, value FROM src").rdd.map {
   *   case Row(key: Int, value: String) =>
   *     key -> value
   * }
   * }}}
   */
  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row.toSeq)

  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): Row = new GenericRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a `Seq` of values.
   */
  def fromSeq(values: Seq[Any]): Row = new GenericRow(values.toArray)

  def fromTuple(tuple: Product): Row = fromSeq(tuple.productIterator.toSeq)

  /**
   * Merge multiple rows into a single row, one after another.
   */
  def merge(rows: Row*): Row = {
    // TODO: Improve the performance of this if used in performance critical part.
    new GenericRow(rows.flatMap(_.toSeq).toArray)
  }

  /** Returns an empty row. */
  val empty = apply()
}


/**
 * 表示来自关系运算符的输出中的一行。允许通过序数进行通用访问，对于原始类型将产生装箱开销，以及本机原始类型访问。
 * 使用本机原始类型接口检索null值是无效的，而是用户必须在尝试检索可能为null的值之前检查isNullAt。
 *
 * 可以使用 `RowFactory.create()`(Java) 或者是 `Row.apply()`(scala) 来创建一个新的Row
 *
 * 可以通过提供字段值来构造[[Row]]对象。 例如:
 * {{{
 *     import org.apache.spark.sql._
 *
 *     // 直接通过多个值来创建 Row 对象
 *     Row(value1, value2, value3, ...)
 *     // 通过 Seq 对象来创建 Row 对象
 *     Row.fromSeq(Seq(value1, value2, ...))
 * }}}
 *
 * 可以通过序数进行通用访问来访问行的值，对于原始类型将产生装箱开销，
 * 也可以进行本机原始类型访问。
 * 通用访问的例子是：
 * {{{
 *     import org.apache.spark.sql._
 *
 *     val row = Row(1, true, "a string", null)
 *     // row: Row = [1,true,a string,null]
 *     val firstValue = row(0)    // 通过序号访问
 *     // firstValue: Any = 1
 *     val fourthValue = row(3)   // 通过序号访问
 *     // fourthValue: Any = null
 * }}}
 *
 * 对于本机原始类型访问，使用本机原始类型接口检索null值是无效的，而是用户必须在尝试检索可能为null的值之前检查isNullAt。
 * 本机原始类型访问的例子是：
 * {{{
 *     val firstValue = row.getInt(0)
 *     // firstValue: Int = 1
 *     val isNull = row.isNullAt(3)      // 判断是否为空
 *     // isNull: Boolean = true
 * }}}
 *
 * 在Scala中，可以在模式匹配中提取[[Row]]对象中的字段。示例：
 * {{{
 *     import org.apache.spark.sql._
 *
 *     val pairs = sql("SELECT key, value FROM src").rdd.map {
 *       case Row(key: Int, value: String) =>
 *         key -> value
 *     }
 * }}}
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
trait Row extends Serializable {
  // Row 的元素数量
  def size: Int = length

  // Row 的元素数量
  def length: Int

  def schema: StructType = null

  /**
   * 返回位于i处的数值，如果当前的数值是null,则直接返回null.
   * 以下是Spark SQL类型和返回类型之间的映射：
   *
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> java.math.BigDecimal
   *
   *   DateType -> java.sql.Date
   *   TimestampType -> java.sql.Timestamp
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def apply(i: Int): Any = get(i)

  def get(i: Int): Any

  /** Checks whether the value at position i is null. */
  def isNullAt(i: Int): Boolean = get(i) == null

  /**
   * Returns the value at position i as a primitive boolean.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getBoolean(i: Int): Boolean = getAnyValAs[Boolean](i)

  /**
   * Returns the value at position i as a primitive byte.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getByte(i: Int): Byte = getAnyValAs[Byte](i)

  /**
   * Returns the value at position i as a primitive short.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getShort(i: Int): Short = getAnyValAs[Short](i)

  /**
   * Returns the value at position i as a primitive int.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getInt(i: Int): Int = getAnyValAs[Int](i)

  /**
   * Returns the value at position i as a primitive long.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getLong(i: Int): Long = getAnyValAs[Long](i)

  /**
   * Returns the value at position i as a primitive float.
   * Throws an exception if the type mismatches or if the value is null.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getFloat(i: Int): Float = getAnyValAs[Float](i)

  /**
   * Returns the value at position i as a primitive double.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getDouble(i: Int): Double = getAnyValAs[Double](i)

  /**
   * Returns the value at position i as a String object.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getString(i: Int): String = getAs[String](i)

  /**
   * Returns the value at position i of decimal type as java.math.BigDecimal.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getDecimal(i: Int): java.math.BigDecimal = getAs[java.math.BigDecimal](i)

  /**
   * Returns the value at position i of date type as java.sql.Date.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getDate(i: Int): java.sql.Date = getAs[java.sql.Date](i)

  /**
   * Returns the value at position i of date type as java.sql.Timestamp.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getTimestamp(i: Int): java.sql.Timestamp = getAs[java.sql.Timestamp](i)

  /**
   * Returns the value at position i of array type as a Scala Seq.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getSeq[T](i: Int): Seq[T] = getAs[Seq[T]](i)

  /**
   * Returns the value at position i of array type as `java.util.List`.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getList[T](i: Int): java.util.List[T] =
    getSeq[T](i).asJava

  /**
   * Returns the value at position i of map type as a Scala Map.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getMap[K, V](i: Int): scala.collection.Map[K, V] = getAs[Map[K, V]](i)

  /**
   * Returns the value at position i of array type as a `java.util.Map`.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getJavaMap[K, V](i: Int): java.util.Map[K, V] =
    getMap[K, V](i).asJava

  /**
   * Returns the value at position i of struct type as a [[Row]] object.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getStruct(i: Int): Row = getAs[Row](i)

  /**
   * Returns the value at position i.
   * For primitive types if value is null it returns 'zero value' specific for primitive
   * ie. 0 for Int - use isNullAt to ensure that value is not null
   *
   * @throws ClassCastException when data type does not match.
   */
  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  /**
   * Returns the value of a given fieldName.
   * For primitive types if value is null it returns 'zero value' specific for primitive
   * ie. 0 for Int - use isNullAt to ensure that value is not null
   *
   * @throws UnsupportedOperationException when schema is not defined.
   * @throws IllegalArgumentException when fieldName do not exist.
   * @throws ClassCastException when data type does not match.
   */
  def getAs[T](fieldName: String): T = getAs[T](fieldIndex(fieldName))

  /**
   * Returns the index of a given field name.
   *
   * @throws UnsupportedOperationException when schema is not defined.
   * @throws IllegalArgumentException when a field `name` does not exist.
   */
  def fieldIndex(name: String): Int = {
    throw new UnsupportedOperationException("fieldIndex on a Row without schema is undefined.")
  }

  /**
   * Returns a Map consisting of names and values for the requested fieldNames
   * For primitive types if value is null it returns 'zero value' specific for primitive
   * ie. 0 for Int - use isNullAt to ensure that value is not null
   *
   * @throws UnsupportedOperationException when schema is not defined.
   * @throws IllegalArgumentException when fieldName do not exist.
   * @throws ClassCastException when data type does not match.
   */
  def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] = {
    fieldNames.map { name =>
      name -> getAs[T](name)
    }.toMap
  }

  override def toString: String = s"[${this.mkString(",")}]"

  /**
   * Make a copy of the current [[Row]] object.
   */
  def copy(): Row

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    val len = length
    var i = 0
    while (i < len) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[Row]) return false
    val other = o.asInstanceOf[Row]

    if (other eq null) return false

    if (length != other.length) {
      return false
    }

    var i = 0
    while (i < length) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = get(i)
        val o2 = other.get(i)
        o1 match {
          case b1: Array[Byte] =>
            if (!o2.isInstanceOf[Array[Byte]] ||
                !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
              return false
            }
          case f1: Float if java.lang.Float.isNaN(f1) =>
            if (!o2.isInstanceOf[Float] || ! java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
              return false
            }
          case d1: Double if java.lang.Double.isNaN(d1) =>
            if (!o2.isInstanceOf[Double] || ! java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
              return false
            }
          case d1: java.math.BigDecimal if o2.isInstanceOf[java.math.BigDecimal] =>
            if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
              return false
            }
          case _ => if (o1 != o2) {
            return false
          }
        }
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    // Using Scala's Seq hash code implementation.
    var n = 0
    var h = MurmurHash3.seqSeed
    val len = length
    while (n < len) {
      h = MurmurHash3.mix(h, apply(n).##)
      n += 1
    }
    MurmurHash3.finalizeHash(h, n)
  }

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
   */
  def toSeq: Seq[Any] = {
    val n = length
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      values.update(i, get(i))
      i += 1
    }
    values.toSeq
  }

  /** Displays all elements of this sequence in a string (without a separator). */
  def mkString: String = toSeq.mkString

  /** Displays all elements of this sequence in a string using a separator string. */
  def mkString(sep: String): String = toSeq.mkString(sep)

  /**
   * Displays all elements of this traversable or iterator in a string using
   * start, end, and separator strings.
   */
  def mkString(start: String, sep: String, end: String): String = toSeq.mkString(start, sep, end)

  /**
   * Returns the value at position i.
   *
   * @throws UnsupportedOperationException when schema is not defined.
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  private def getAnyValAs[T <: AnyVal](i: Int): T =
    if (isNullAt(i)) throw new NullPointerException(s"Value at index $i is null")
    else getAs[T](i)
}
