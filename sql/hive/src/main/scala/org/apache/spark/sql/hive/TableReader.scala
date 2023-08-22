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

package org.apache.spark.sql.hive

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants._
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, RDD, UnionRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A trait for subclasses that handle table scans.
 */
private[hive] sealed trait TableReader {
  def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow]

  def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow]
}


/**
 * Helper class for scanning tables stored in Hadoop - e.g., to read Hive tables that reside in the
 * data warehouse directory.
 */
private[hive]
class HadoopTableReader(
    @transient private val attributes: Seq[Attribute],
    @transient private val partitionKeys: Seq[Attribute],
    @transient private val tableDesc: TableDesc,
    @transient private val sparkSession: SparkSession,
    hadoopConf: Configuration)
  extends TableReader with CastSupport with Logging {

  // Hadoop honors "mapreduce.job.maps" as hint,
  // but will ignore when mapreduce.jobtracker.address is "local".
  // https://hadoop.apache.org/docs/r2.6.5/hadoop-mapreduce-client/hadoop-mapreduce-client-core/
  // mapred-default.xml
  //
  // In order keep consistency with Hive, we will let it be 0 in local mode also.
  private val _minSplitsPerRDD = if (sparkSession.sparkContext.isLocal) {
    0 // will splitted based on block by default.
  } else {
    math.max(hadoopConf.getInt("mapreduce.job.maps", 1),
      sparkSession.sparkContext.defaultMinPartitions)
  }

  SparkHadoopUtil.get.appendS3AndSparkHadoopConfigurations(
    sparkSession.sparkContext.conf, hadoopConf)

  private val _broadcastedHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

  override def conf: SQLConf = sparkSession.sessionState.conf

  override def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow] =
    makeRDDForTable(
      hiveTable,
      Utils.classForName(tableDesc.getSerdeClassName).asInstanceOf[Class[Deserializer]],
      filterOpt = None)

  /**
   * 基于无分区表创建 HadoopRDD
   *
   * @param hiveTable hive 表信息.
   * @param deserializerClass 序列化与反序列化相关类.
   * @param filterOpt If defined, then the filter is used to reject files contained in the data
   *                  directory being read. If None, then all files are accepted.
   */
  def makeRDDForTable(
      hiveTable: HiveTable,
      deserializerClass: Class[_ <: Deserializer],
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    assert(!hiveTable.isPartitioned,
      "makeRDDForTable() cannot be called on a partitioned table, since input formats may " +
      "differ across partitions. Use makeRDDForPartitionedTable() instead.")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val localTableDesc = tableDesc                       // 表的相关描述信息
    val broadcastedHadoopConf = _broadcastedHadoopConf   // hadoop 配置

    val tablePath = hiveTable.getPath                     // 获取hive 表的相关路径
    val inputPathStr = applyFilterIfNeeded(tablePath, filterOpt)    // 输入路径


    // 表对应的 InputFormat
    val ifc = hiveTable.getInputFormatClass.asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]

    val hadoopRDD = createHadoopRdd(localTableDesc, inputPathStr, ifc)

    val attrsWithIndex = attributes.zipWithIndex
    val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

    val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = deserializerClass.newInstance()
      deserializer.initialize(hconf, localTableDesc.getProperties)
      HadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow, deserializer)
    }

    deserializedHadoopRDD
  }

  override def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow] = {
    val partitionToDeserializer = partitions.map(part =>
      (part, part.getDeserializer.getClass.asInstanceOf[Class[Deserializer]])).toMap
    makeRDDForPartitionedTable(partitionToDeserializer, filterOpt = None)
  }

  /**
   * 对查询中指定的每个分区键创建一个HadoopRDD。请注意，对于磁盘上的Hive表，将为每个使用'PARTITION BY'指定的分区键创建一个数据目录。
   * 每个分区创建一个Hadoop RDD 以后，将会产生多个HadoopRDD, 上层将使用UnionRDD 封装返回
   */
  def makeRDDForPartitionedTable(
      partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]],
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    // 将不存在的路径排除掉
    def verifyPartitionPath(
        partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]]):
        Map[HivePartition, Class[_ <: Deserializer]] = {
      if (!sparkSession.sessionState.conf.verifyPartitionPath) {
        partitionToDeserializer
      } else {
        val existPathSet = collection.mutable.Set[String]()
        val pathPatternSet = collection.mutable.Set[String]()
        partitionToDeserializer.filter {
          case (partition, partDeserializer) =>
            def updateExistPathSetByPathPattern(pathPatternStr: String) {
              val pathPattern = new Path(pathPatternStr)
              val fs = pathPattern.getFileSystem(hadoopConf)
              val matches = fs.globStatus(pathPattern)
              matches.foreach(fileStatus => existPathSet += fileStatus.getPath.toString)
            }
            // convert  /demo/data/year/month/day  to  /demo/data/*/*/*/
            def getPathPatternByPath(parNum: Int, tempPath: Path): String = {
              var path = tempPath
              for (i <- (1 to parNum)) path = path.getParent
              val tails = (1 to parNum).map(_ => "*").mkString("/", "/", "/")
              path.toString + tails
            }

            val partPath = partition.getDataLocation
            val partNum = Utilities.getPartitionDesc(partition).getPartSpec.size()
            val pathPatternStr = getPathPatternByPath(partNum, partPath)
            if (!pathPatternSet.contains(pathPatternStr)) {
              pathPatternSet += pathPatternStr
              updateExistPathSetByPathPattern(pathPatternStr)
            }
            existPathSet.contains(partPath.toString)
        }
      }
    }

    val hivePartitionRDDs = verifyPartitionPath(partitionToDeserializer)
      .map { case (partition, partDeserializer) =>
        // 遍历每个存在的路径

      val partDesc = Utilities.getPartitionDesc(partition)
      val partPath = partition.getDataLocation  // 拿到分区对应的目录

      val inputPathStr = applyFilterIfNeeded(partPath, filterOpt)      // 输入路径
      val ifc = partDesc.getInputFileFormatClass.asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]  // InputFormat

      val partSpec = partDesc.getPartSpec
      val partProps = partDesc.getProperties

      val partColsDelimited: String = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
      val partCols = partColsDelimited.trim().split("/").toSeq
      val partValues = if (partSpec == null) {
        Array.fill(partCols.size)(new String)
      } else {
        partCols.map(col => new String(partSpec.get(col))).toArray
      }

      val broadcastedHiveConf = _broadcastedHadoopConf
      val localDeserializer = partDeserializer
      val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

      // Splits all attributes into two groups, partition key attributes and those that are not.
      // Attached indices indicate the position of each attribute in the output schema.
      val (partitionKeyAttrs, nonPartitionKeyAttrs) =
        attributes.zipWithIndex.partition { case (attr, _) =>
          partitionKeys.contains(attr)
        }

      def fillPartitionKeys(rawPartValues: Array[String], row: InternalRow): Unit = {
        partitionKeyAttrs.foreach { case (attr, ordinal) =>
          val partOrdinal = partitionKeys.indexOf(attr)
          row(ordinal) = cast(Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
        }
      }

      // Fill all partition keys to the given MutableRow object
      fillPartitionKeys(partValues, mutableRow)

      val tableProperties = tableDesc.getProperties

      // Create local references so that the outer object isn't serialized.
      val localTableDesc = tableDesc

      // 基于每个输入路径创建一个HadoopRDD
      createHadoopRdd(localTableDesc, inputPathStr, ifc)
        .mapPartitions { iter =>
        val hconf = broadcastedHiveConf.value.value
        val deserializer = localDeserializer.newInstance()
        // SPARK-13709: For SerDes like AvroSerDe, some essential information (e.g. Avro schema
        // information) may be defined in table properties. Here we should merge table properties
        // and partition properties before initializing the deserializer. Note that partition
        // properties take a higher priority here. For example, a partition may have a different
        // SerDe as the one defined in table properties.
        val props = new Properties(tableProperties)
        partProps.asScala.foreach {
          case (key, value) => props.setProperty(key, value)
        }
        deserializer.initialize(hconf, props)
        // get the table deserializer
        val tableSerDe = localTableDesc.getDeserializerClass.newInstance()
        tableSerDe.initialize(hconf, localTableDesc.getProperties)

        // 将 RDD[Write] 转变为 RDD[InternalRow]
        HadoopTableReader.fillObject(iter, deserializer, nonPartitionKeyAttrs, mutableRow, tableSerDe)
      }
    }.toSeq

    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.size == 0) {
      new EmptyRDD[InternalRow](sparkSession.sparkContext)
    } else {
      // 使用 UnionRDD 合并 HadoopRDD
      new UnionRDD(hivePartitionRDDs(0).context, hivePartitionRDDs)
    }
  }

  /**
   * If `filterOpt` is defined, then it will be used to filter files from `path`. These files are
   * returned in a single, comma-separated string.
   */
  private def applyFilterIfNeeded(path: Path, filterOpt: Option[PathFilter]): String = {
    filterOpt match {
      case Some(filter) =>
        val fs = path.getFileSystem(hadoopConf)
        val filteredFiles = fs.listStatus(path, filter).map(_.getPath.toString)
        filteredFiles.mkString(",")
      case None => path.toString
    }
  }


  /***
   * 创建一个HadoopRDD
   * @param tableDesc                 表的相关描述信息
   * @param path                      数据路径
   * @param inputFormatClass          InputFormat
   * @return
   */
  private def createHadoopRdd(
    tableDesc: TableDesc,
    path: String,
    inputFormatClass: Class[InputFormat[Writable, Writable]]): RDD[Writable] = {

    // mapreduce.input.fileinputformat.inputdir : path
    val initializeJobConfFunc = HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc) _

    // 创建一个
    val rdd = new HadoopRDD(
      sparkSession.sparkContext,
      _broadcastedHadoopConf.asInstanceOf[Broadcast[SerializableConfiguration]],
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }
}

private[hive] object HiveTableUtil {

  // copied from PlanUtils.configureJobPropertiesForStorageHandler(tableDesc)
  // that calls Hive.get() which tries to access metastore, but it's not valid in runtime
  // it would be fixed in next version of hive but till then, we should use this instead
  def configureJobPropertiesForStorageHandler(
      tableDesc: TableDesc, conf: Configuration, input: Boolean) {
    val property = tableDesc.getProperties.getProperty(META_TABLE_STORAGE)
    val storageHandler =
      org.apache.hadoop.hive.ql.metadata.HiveUtils.getStorageHandler(conf, property)
    if (storageHandler != null) {
      val jobProperties = new java.util.LinkedHashMap[String, String]
      if (input) {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      } else {
        storageHandler.configureOutputJobProperties(tableDesc, jobProperties)
      }
      if (!jobProperties.isEmpty) {
        tableDesc.setJobProperties(jobProperties)
      }
    }
  }
}

private[hive] object HadoopTableReader extends HiveInspectors with Logging {
  /**
   * Curried. After given an argument for 'path', the resulting JobConf => Unit closure is used to
   * instantiate a HadoopRDD.
   */
  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc)(jobConf: JobConf) {
    FileInputFormat.setInputPaths(jobConf, Seq[Path](new Path(path)): _*)
    if (tableDesc != null) {
      HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, true)
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
  }

  /**
   * Transform all given raw `Writable`s into `Row`s.
   *
   * @param iterator Iterator of all `Writable`s to be transformed
   * @param rawDeser The `Deserializer` associated with the input `Writable`
   * @param nonPartitionKeyAttrs Attributes that should be filled together with their corresponding
   *                             positions in the output schema
   * @param mutableRow A reusable `MutableRow` that should be filled
   * @param tableDeser Table Deserializer
   * @return An `Iterator[Row]` transformed from `iterator`
   */
  def fillObject(
      iterator: Iterator[Writable],
      rawDeser: Deserializer,
      nonPartitionKeyAttrs: Seq[(Attribute, Int)],
      mutableRow: InternalRow,
      tableDeser: Deserializer): Iterator[InternalRow] = {

    val soi = if (rawDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
      rawDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
    } else {
      ObjectInspectorConverters.getConvertedOI(
        rawDeser.getObjectInspector,
        tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
    }

    logDebug(soi.toString)

    val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map { case (attr, ordinal) =>
      soi.getStructFieldRef(attr.name) -> ordinal
    }.toArray.unzip

    /**
     * Builds specific unwrappers ahead of time according to object inspector
     * types to avoid pattern matching and branching costs per row.
     */
    val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
      _.getFieldObjectInspector match {
        case oi: BooleanObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
        case oi: ByteObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
        case oi: ShortObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
        case oi: IntObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
        case oi: LongObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
        case oi: FloatObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
        case oi: DoubleObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
        case oi: HiveVarcharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveCharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveDecimalObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, HiveShim.toCatalystDecimal(oi, value))
        case oi: TimestampObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(oi.getPrimitiveJavaObject(value)))
        case oi: DateObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setInt(ordinal, DateTimeUtils.fromJavaDate(oi.getPrimitiveJavaObject(value)))
        case oi: BinaryObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, oi.getPrimitiveJavaObject(value))
        case oi =>
          val unwrapper = unwrapperFor(oi)
          (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
      }
    }

    val converter = ObjectInspectorConverters.getConverter(rawDeser.getObjectInspector, soi)

    // Map each tuple to a row object
    iterator.map { value =>
      val raw = converter.convert(rawDeser.deserialize(value))
      var i = 0
      val length = fieldRefs.length
      while (i < length) {
        val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
        if (fieldValue == null) {
          mutableRow.setNullAt(fieldOrdinals(i))
        } else {
          unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
        }
        i += 1
      }

      mutableRow: InternalRow
    }
  }
}
