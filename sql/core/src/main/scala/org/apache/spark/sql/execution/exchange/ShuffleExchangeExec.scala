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

package org.apache.spark.sql.execution.exchange

import java.util.Random
import java.util.function.Supplier

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}

/****************************************************
 * 执行一个将产生所需 newPartitioning 的 Shuffle 。
 * @param newPartitioning 分区分布
 * @param child 子查询树
 */
case class ShuffleExchangeExec(
    var newPartitioning: Partitioning,
    child: SparkPlan,
    @transient coordinator: Option[ExchangeCoordinator]) extends Exchange {

  // NOTE: 在序列化和反序列化之后，coordinator可能为空。,

  // 统计数据大小指标
  override lazy val metrics = Map("dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))

  // 节点名称
  override def nodeName: String = {
    val extraInfo = coordinator match {
      case Some(exchangeCoordinator) =>
        s"(coordinator id: ${System.identityHashCode(exchangeCoordinator)})"
      case _ => ""
    }

    val simpleNodeName = "Exchange"
    s"$simpleNodeName$extraInfo"
  }

  // 新的分区分布
  override def outputPartitioning: Partitioning = newPartitioning

  // 序列化器
  private val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  override protected def doPrepare(): Unit = {
    // 如果需要[[ExchangeCoordinator]]，我们在执行prepare操作时将此Exchange操作符注册到ExchangeCoordinator。
    // 重要的是确保我们在执行前注册此操作符，而不是在构造函数中注册，
    // 因为在转换物理计划时可能会创建新的Exchange操作符实例（然后ExchangeCoordinator将持有不需要的Exchange的引用）。
    // 因此，我们应该在执行计划之前只在即将开始执行时调用registerExchange。这段代码强调了在何时注册Exchange操作符以确保协调工作的正确性。
    coordinator match {
      case Some(exchangeCoordinator) => exchangeCoordinator.registerExchange(this)
      case _ =>
    }
  }

  /**
   * 返回一个[[ShuffleDependency]]，该 dependency 将根据newPartitioning中定义的分区方案，对其子级的行进行分区。
   * 返回的[[ShuffleDependency]]的这些分区将成为Shuffle的输入。
   */
  private[exchange] def prepareShuffleDependency() : ShuffleDependency[Int, InternalRow, InternalRow] = {
    ShuffleExchangeExec.prepareShuffleDependency(
      child.execute(), child.output, newPartitioning, serializer)
  }

  /**
   * 返回一个表示Shuffle后数据集的[[ShuffledRowRDD]]。
   * 这个[[ShuffledRowRDD]]是基于给定的[[ShuffleDependency]]和可选的分区起始索引数组创建的。
   * 如果定义了这个可选数组，返回的[[ShuffledRowRDD]]将根据该数组的索引获取Shuffle前的分区。
   */
  private[exchange] def preparePostShuffleRDD(
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    // 如果提供了一个分区起始索引数组，我们需要使用这个数组来创建ShuffledRowRDD。
    // 此外，我们需要更新newPartitioning来更新洗牌后的分区数量。
    // 这段代码强调了在特定情况下更新ShuffledRowRDD和newPartitioning的必要性。
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)
    }
    new ShuffledRowRDD(shuffleDependency, specifiedPartitionStartIndices)
  }

  /**
   * 缓存创建的ShuffleRowRDD，以便我们可以重复使用它。.
   */
  private var cachedShuffleRDD: ShuffledRowRDD = null

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    // 如果多个计划使用相同的计划，则返回相同的ShuffleRowRDD。
    // 这可以用来避免重复创建相同的ShuffleRowRDD实例，以提高性能和减少资源消耗。
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = coordinator match {
        case Some(exchangeCoordinator) =>
          // 存在 coordinator 的情况下，使用 coordinator 构造产生 ShuffleRDD
          val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
          assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
          shuffleRDD
        case _ =>
          // 默认情况下，没有定义 Coordinator 的默认构造器
          val shuffleDependency = prepareShuffleDependency()
          preparePostShuffleRDD(shuffleDependency)
      }
    }
    cachedShuffleRDD
  }
}

object ShuffleExchangeExec {
  def apply(newPartitioning: Partitioning, child: SparkPlan): ShuffleExchangeExec = {
    ShuffleExchangeExec(newPartitioning, child, coordinator = Option.empty[ExchangeCoordinator])
  }

  /**
   * 确定在将记录发送到Shuffle之前是否必须进行防御性拷贝。Spark的多个Shuffle组件会在内存中缓冲反序列化的Java对象。
   * Shuffle代码假定对象是不可变的，因此不执行自己的防御性拷贝。但是，在Spark SQL中，操作符的迭代器返回相同的可变Row对象。
   * 为了正确洗牌这些操作符的输出，我们需要在将记录发送到Shuffle之前执行我们自己的拷贝。
   * 这种拷贝代价高昂，因此我们尽量避免它。这个方法封装了选择何时进行拷贝的逻辑。
   * 从长远来看，我们可能希望将这个逻辑推到核心的Shuffle API中，这样我们就不必依赖于此处对核心内部的了解。
   *
   * 关于这个问题的更多讨论，请参见SPARK-2967、SPARK-4479和SPARK-7375。
   *
   * @param partitioner the partitioner for the shuffle
   * @return true if rows should be copied before being shuffled, false otherwise
   */
  private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
    // 请注意，尽管我们只使用分区器的numPartitions字段，但我们要求传递分区器而不是直接传递分区数，以防止出现某些特殊情况，
    // 其中使用numPartitions分区构造的分区器可能会输出较少的分区（例如，像RangePartitioner）。
    // 这段代码强调了为了应对潜在的特殊情况，需要传递分区器而不是分区数。
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    val numParts = partitioner.numPartitions
    if (sortBasedShuffleOn) {
      if (numParts <= bypassMergeThreshold) {
        // 如果我们使用原始的SortShuffleManager并且输出分区的数量足够小，那么Spark会退回到基于哈希的洗牌写路径，该路径不会缓冲反序列化的记录。
        // 请注意，如果我们解决SPARK-6026并删除此绕过路径，我们将不得不删除这种情况。
        // 这段代码强调了Spark在某些情况下会选择不同的洗牌写入路径，具体取决于分区数量。
        false
      } else if (numParts <= SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties and the number of partitions doesn't exceed the limitation. If this
        // optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, and the
        // serializer in Spark SQL always satisfy the properties, so we only need to check whether
        // the number of partitions exceeds the limitation.
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory, so we must
        // copy.
        true
      }
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  /**
   * ************************************************************
   * 返回一个[[ShuffleDependency]]，该依赖将根据newPartitioning中定义的分区方案对其子级的行进行分区。
   * 返回的ShuffleDependency的这些分区将成为Shuffle的输入。
   *
   * @param rdd 上游依赖RDD
   * @param outputAttributes 输出字段
   * @param newPartitioning 新的分区信息
   * @param serializer 序列化工具
   * *******************************************************
   */
  def prepareShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer): ShuffleDependency[Int, InternalRow, InternalRow] = {

    // 获取分区器
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(_, n) =>
        new Partitioner {
          override def numPartitions: Int = n
          // 对于HashPartitioning，分区键已经是有效的分区ID，
          // 因为我们使用HashPartitioning.partitionIdExpression来生成分区键。
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // 在内部，RangePartitioner在RDD上运行一个作业来对键进行采样以计算分区边界。
        // 为了获取准确的样本，我们需要复制可变的键。
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val mutablePair = new MutablePair[InternalRow, Null]()
          iter.map(row => mutablePair.update(row.copy(), null))
        }
        implicit val ordering = new LazilyGeneratedOrdering(sortingExpressions, outputAttributes)

        new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }

    // 定义获取分区方法
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      case h: HashPartitioning =>
        val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
        row => projection(row).getInt(0)
      case RangePartitioning(_, _) | SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1


    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {

      // 必须确保生成的RoundRobinPartitioning是确定性的，否则重试任务可能会输出不同的行，从而导致数据丢失。
      // 目前，我们遵循了最直接的方法，在分区之前执行本地排序。
      // 请注意，如果新的分区方案只有1个分区，我们就不执行本地排序，在这种情况下，所有输出行都进入同一个分区。
      // 这段代码强调了确保 RoundRobinPartitioning 的确定性以防止数据丢失的必要性。
      val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
        rdd.mapPartitionsInternal { iter =>
          val recordComparatorSupplier = new Supplier[RecordComparator] {
            override def get: RecordComparator = new RecordBinaryComparator()
          }
          // 用于比较行哈希码的比较器，它应该始终是整数（Integer）。
          val prefixComparator = PrefixComparators.LONG
          val canUseRadixSort = SparkEnv.get.conf.get(SQLConf.RADIX_SORT_ENABLED)
          // 前缀计算器生成行的哈希码作为前缀，因此当输入行从有限范围中选择列值时，可以减少前缀相等的概率。
          val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
            private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
            override def computePrefix(row: InternalRow):
            UnsafeExternalRowSorter.PrefixComputer.Prefix = {
              // The hashcode generated from the binary form of a [[UnsafeRow]] should not be null.
              result.isNull = false
              result.value = row.hashCode()
              result
            }
          }
          val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

          val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
            StructType.fromAttributes(outputAttributes),
            recordComparatorSupplier,
            prefixComparator,
            prefixComputer,
            pageSize,
            canUseRadixSort)
          sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
        }
      } else {
        rdd
      }

      // 如果我们不对输入进行排序，Round-Robin函数是有序敏感的.
      val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition
      if (needToCopyObjectsBeforeShuffle(part)) {
        newRdd.mapPartitionsWithIndexInternal((_, iter) => {
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }, isOrderSensitive = isOrderSensitive)
      } else {
        newRdd.mapPartitionsWithIndexInternal((_, iter) => {
          val getPartitionKey = getPartitionKeyExtractor()        //获取key分区器
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }, isOrderSensitive = isOrderSensitive)
      }
    }

    // 现在，我们手动创建一个ShuffleDependency。
    // 因为rddWithPartitionIds中的数据对是（partitionId，row）的形式，而且每个partitionId都在期望的范围内[0, part.numPartitions-1]。
    // 这个ShuffleDependency的分区器是 PartitionIdPassthrough。
    val dependency = new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer)

    dependency
  }
}
