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

package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * 这是一个基类，用于表示子RDD的每个分区都依赖于父RDD的少数分区的依赖关系。
 * 窄依赖关系允许进行流水线式执行。
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
 *  表示对Shuffle阶段输出的依赖关系。需要注意，在进行shuffle的情况下，RDD是临时的，因为我们在executor端不需要它
 *
 * @param _rdd 上游依赖的 RDD
 * @param partitioner 用于进行shuffle分区的分区器
 * @param serializer rdd 数据序列化工具 spark.serializer 参数可以用于指定，如果不指定，将使用默认序列化器
 * @param keyOrdering 用于对key进行排序的比较器
 * @param aggregator map/reduce 端聚合器
 * @param mapSideCombine 是否进行map端聚合
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],     // 上游的RDD
    val partitioner: Partitioner,                              // 上游RDD算完后的分区方式
    val serializer: Serializer = SparkEnv.get.serializer,      // RDD 的序列化工具
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  if (mapSideCombine) {
    require(aggregator.isDefined, "Map-side combine without Aggregator specified!")
  }

  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName

  private[spark] val combinerClassName: Option[String] = Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  // 生成一个ShuffleId
  val shuffleId: Int = _rdd.context.newShuffleId()

  // shufffle 操作句柄信息
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.length, this)

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}


/**
 * :: DeveloperApi :: 像类似map/flatmap相关的依赖关系
 * 表示父RDD和子RDD之间分区之间的一对一依赖关系。.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * 表示父RDD和子RDD之间分区范围之间的一对一依赖关系, 再Union 场景中使用
 * @param rdd 依赖的RDD
 * @param inStart 在父RDD中范围的起始位置。
 * @param outStart 在子RDD中范围的起始位置。
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
