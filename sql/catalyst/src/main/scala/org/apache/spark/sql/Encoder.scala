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

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.types._


/**
 * :: 实验性的 ::
 *  用于在JVM中的对象（类型为T）与内部的Spark SQL表示之间进行转换。
 *
 * == Scala ==
 * Encoders(编码器)通常通过 `SparkSession` 的隐式转换自动创建，也可以通过在[[Encoders]]上调用静态方法来显式创建。
 * {{{
 *   import spark.implicits._
 *   val ds = Seq(1, 2, 3).toDS() // implicitly provided (spark.implicits.newIntEncoder)
 * }}}
 *
 * == Java ==
 * Encoders 通过在[[Encoders]]上调用静态方法来指定。
 *
 * {{{
 *   List<String> data = Arrays.asList("abc", "abc", "xyz");
 *   Dataset<String> ds = context.createDataset(data, Encoders.STRING());
 * }}}
 *
 * Encoders 可以组合成元组：
 *
 * {{{
 *   Encoder<Tuple2<Integer, String>> encoder2 = Encoders.tuple(Encoders.INT(), Encoders.STRING());
 *   List<Tuple2<Integer, String>> data2 = Arrays.asList(new scala.Tuple2(1, "a");
 *   Dataset<Tuple2<Integer, String>> ds2 = context.createDataset(data2, encoder2);
 * }}}
 *
 * 或者从Java Beans构造：
 *
 * {{{
 *   Encoders.bean(MyClass.class);
 * }}}
 *
 * == 实现 ==
 *  - Encoders 不需要是线程安全的，因此如果它们重用内部缓冲区以提高性能，则不需要使用锁来防止并发访问
 *
 * @since 1.6.0
 */
@Experimental
@InterfaceStability.Evolving
@implicitNotFound("Unable to find encoder for type ${T}. An implicit Encoder[${T}] is needed to " +
  "store ${T} instances in a Dataset. Primitive types (Int, String, etc) and Product types (case " +
  "classes) are supported by importing spark.implicits._  Support for serializing other types " +
  "will be added in future releases.")
trait Encoder[T] extends Serializable {

  // 返回将此类型对象编码为 Row 的 schema
  def schema: StructType

  // 类型标签
  def clsTag: ClassTag[T]
}
