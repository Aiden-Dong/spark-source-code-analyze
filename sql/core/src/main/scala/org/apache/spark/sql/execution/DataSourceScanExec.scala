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

package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

trait DataSourceScanExec extends LeafExecNode with CodegenSupport {
  val relation: BaseRelation
  val tableIdentifier: Option[TableIdentifier]

  protected val nodeNamePrefix: String = ""

  override val nodeName: String = {
    s"Scan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  // Metadata that describes more details of this scan.
  protected def metadata: Map[String, String]

  override def simpleString: String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), 100)
    }
    val metadataStr = Utils.truncatedString(metadataEntries, " ", ", ", "")
    s"$nodeNamePrefix$nodeName${Utils.truncatedString(output, "[", ",", "]")}$metadataStr"
  }

  override def verboseString: String = redact(super.verboseString)

  override def treeString(verbose: Boolean, addSuffix: Boolean): String = {
    redact(super.treeString(verbose, addSuffix))
  }

  /**
   * Shorthand for calling redactString() without specifying redacting rules
   */
  private def redact(text: String): String = {
    Utils.redact(sqlContext.sessionState.conf.stringRedactionPattern, text)
  }
}

/** Physical plan node for scanning data from a relation. */
case class RowDataSourceScanExec(
    fullOutput: Seq[Attribute],
    requiredColumnsIndex: Seq[Int],
    filters: Set[Filter],
    handledFilters: Set[Filter],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec {

  def output: Seq[Attribute] = requiredColumnsIndex.map(fullOutput)

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map( r => {
        numOutputRows += 1
        proj(r)
      })
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // PhysicalRDD always just has one input
    val input = ctx.addMutableState("scala.collection.Iterator", "input", v => s"$v = inputs[0];")
    val exprRows = output.zipWithIndex.map{ case (a, i) =>
      BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, columnsRowInput).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }

  override val metadata: Map[String, String] = {
    val markedFilters = for (filter <- filters) yield {
      if (handledFilters.contains(filter)) s"*$filter" else s"$filter"
    }
    Map(
      "ReadSchema" -> output.toStructType.catalogString,
      "PushedFilters" -> markedFilters.mkString("[", ", ", "]"))
  }

  // Don't care about `rdd` and `tableIdentifier` when canonicalizing.
  override def doCanonicalize(): SparkPlan =
    copy(
      fullOutput.map(QueryPlan.normalizeExprId(_, fullOutput)),
      rdd = null,
      tableIdentifier = None)
}

/**
 * Physical plan node for scanning data from HadoopFsRelations.
 *
 * @param relation The file-based relation to scan.
 * @param output Output attributes of the scan, including data attributes and partition attributes.
 * @param requiredSchema Required schema of the underlying relation, excluding partition columns.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param optionalBucketSet Bucket ids for bucket pruning
 * @param dataFilters Filters on non-partition columns.
 * @param tableIdentifier identifier for the table in the metastore.
 */
case class FileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with ColumnarBatchScan  {

  // Note that some vals referring the file-based relation are lazy intentionally
  // so that this plan can be canonicalized on executor side too. See SPARK-23731.
  override lazy val supportsBatch: Boolean = relation.fileFormat.supportBatch(
    relation.sparkSession, StructType.fromAttributes(output))

  override lazy val needsUnsafeRowConversion: Boolean = {
    if (relation.fileFormat.isInstanceOf[ParquetSource]) {
      SparkSession.getActiveSession.get.sessionState.conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
  }

  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf)

  @transient private lazy val selectedPartitions: Seq[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret = relation.location.listFiles(partitionFilters, dataFilters)
    val timeTakenMs = ((System.nanoTime() - startTime) + optimizerMetadataTimeNs) / 1000 / 1000

    metrics("numFiles").add(ret.map(_.files.size.toLong).sum)
    metrics("metadataTime").add(timeTakenMs)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics("numFiles") :: metrics("metadataTime") :: Nil)

    ret
  }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    val bucketSpec = if (relation.sparkSession.sessionState.conf.bucketingEnabled) {
      relation.bucketSpec
    } else {
      None
    }
    bucketSpec match {
      case Some(spec) =>
        // For bucketed columns:
        // -----------------------
        // `HashPartitioning` would be used only when:
        // 1. ALL the bucketing columns are being read from the table
        //
        // For sorted columns:
        // ---------------------
        // Sort ordering should be used when ALL these criteria's match:
        // 1. `HashPartitioning` is being used
        // 2. A prefix (or all) of the sort columns are being read from the table.
        //
        // Sort ordering would be over the prefix subset of `sort columns` being read
        // from the table.
        // eg.
        // Assume (col0, col2, col3) are the columns read from the table
        // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
        // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
        // above

        def toAttribute(colName: String): Option[Attribute] =
          output.find(_.name == colName)

        val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
        if (bucketColumns.size == spec.bucketColumnNames.size) {
          val partitioning = HashPartitioning(bucketColumns, spec.numBuckets)
          val sortColumns =
            spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)

          val sortOrder = if (sortColumns.nonEmpty) {
            // In case of bucketing, its possible to have multiple files belonging to the
            // same bucket in a given relation. Each of these files are locally sorted
            // but those files combined together are not globally sorted. Given that,
            // the RDD partition will not be sorted even if the relation has sort columns set
            // Current solution is to check if all the buckets have a single file in it

            val files = selectedPartitions.flatMap(partition => partition.files)
            val bucketToFilesGrouping =
              files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
            val singleFilePartitions = bucketToFilesGrouping.forall(p => p._2.length <= 1)

            if (singleFilePartitions) {
              // TODO Currently Spark does not support writing columns sorting in descending order
              // so using Ascending order. This can be fixed in future
              sortColumns.map(attribute => SortOrder(attribute, Ascending))
            } else {
              Nil
            }
          } else {
            Nil
          }
          (partitioning, sortOrder)
        } else {
          (UnknownPartitioning(0), Nil)
        }
      case _ =>
        (UnknownPartitioning(0), Nil)
    }
  }

  @transient
  private val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
  logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName + seqToString(location.rootPaths)
    val metadata =
      FMap(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsBatch.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedDownFilters),
        "Location" -> locationDesc)
    val withOptPartitionCount =
      relation.partitionSchemaOption.map { _ =>
        metadata + ("PartitionCount" -> selectedPartitions.size.toString)
      } getOrElse {
        metadata
      }

    val withSelectedBucketsCount = relation.bucketSpec.map { spec =>
      val numSelectedBuckets = optionalBucketSet.map { b =>
        b.cardinality()
      } getOrElse {
        spec.numBuckets
      }
      withOptPartitionCount + ("SelectedBucketsCount" ->
        s"$numSelectedBuckets out of ${spec.numBuckets}")
    } getOrElse {
      withOptPartitionCount
    }

    withSelectedBucketsCount
  }

  private lazy val inputRDD: RDD[InternalRow] = {
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    relation.bucketSpec match {
      case Some(bucketing) if relation.sparkSession.sessionState.conf.bucketingEnabled =>
        createBucketedReadRDD(bucketing, readFile, selectedPartitions, relation)
      case _ =>
        createNonBucketedReadRDD(readFile, selectedPartitions, relation)
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files"),
      "metadataTime" -> SQLMetrics.createMetric(sparkContext, "metadata time (ms)"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))

  protected override def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      // in the case of fallback, this batched scan should never fail because of:
      // 1) only primitive types are supported
      // 2) the number of columns should be smaller than spark.sql.codegen.maxFields
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      val numOutputRows = longMetric("numOutputRows")

      if (needsUnsafeRowConversion) {
        inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
          val proj = UnsafeProjection.create(schema)
          proj.initialize(index)
          iter.map( r => {
            numOutputRows += 1
            proj(r)
          })
        }
      } else {
        inputRDD.map { r =>
          numOutputRows += 1
          r
        }
      }
    }
  }

  override val nodeNamePrefix: String = "File"

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createNonBucketedReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          val hosts = getBlockHosts(getBlockLocations(f), 0, f.getLen)
          PartitionedFile(p.values, f.getPath.toUri.toString, 0, f.getLen, hosts)
        }
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(new Path(f.filePath).getName)
          .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
      }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
      FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Nil))
    }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {

    // spark.sql.files.maxPartitionBytes
    val defaultMaxSplitBytes = fsRelation.sparkSession.sessionState.conf.filesMaxPartitionBytes
    // spark.sql.files.openCostInBytes
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    // spark.default.parallelism
    val defaultParallelism = fsRelation.sparkSession.sparkContext.defaultParallelism
    //  计算预估总的字节大小

    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    // 计算预估的分片大小
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))

    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        // 获取当前文件的所有的Block
        val blockLocations = getBlockLocations(file)

        if (fsRelation.fileFormat.isSplitable(
            fsRelation.sparkSession, fsRelation.options, file.getPath)) {
          // 如果当前文件可以被分割
          // 将按照分割计算大小切割文件
          (0L until file.getLen by maxSplitBytes).map { offset =>
            // 剩余的文件大小
            val remaining = file.getLen - offset

            val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining  // 当前split的长度
            val hosts = getBlockHosts(blockLocations, offset, size) // 当前Split所在的host
            PartitionedFile(partition.values, file.getPath.toUri.toString, offset, size, hosts)
          }
        } else {
          val hosts = getBlockHosts(blockLocations, 0, file.getLen)
          Seq(PartitionedFile(
            partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts))
        }
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    // Block 按照大小进行排序，用于合并小文件

    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition =
          FilePartition(partitions.size, currentFiles.toArray.toSeq) // Copy to a new Array.
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // 合并小的分区
    splitFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()

    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(
      blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }

  override def doCanonicalize(): FileSourceScanExec = {
    FileSourceScanExec(
      relation,
      output.map(QueryPlan.normalizeExprId(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(partitionFilters, output),
      optionalBucketSet,
      QueryPlan.normalizePredicates(dataFilters, output),
      None)
  }
}
