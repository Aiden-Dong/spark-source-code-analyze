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

package org.apache.spark.sql.catalyst.catalog

import java.net.URI
import java.util.Date

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Cast, ExprId, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


/**
 * A function defined in the catalog.
 *
 * @param identifier name of the function
 * @param className fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
 * @param resources resource types and Uris used by the function
 */
case class CatalogFunction(
    identifier: FunctionIdentifier,
    className: String,
    resources: Seq[FunctionResource])


/**
 * Storage format, used to describe how a partition or a table is stored.
 */
case class CatalogStorageFormat(
    locationUri: Option[URI],
    inputFormat: Option[String],
    outputFormat: Option[String],
    serde: Option[String],
    compressed: Boolean,
    properties: Map[String, String]) {

  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("Storage(", ", ", ")")
  }

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    locationUri.foreach(l => map.put("Location", l.toString))
    serde.foreach(map.put("Serde Library", _))
    inputFormat.foreach(map.put("InputFormat", _))
    outputFormat.foreach(map.put("OutputFormat", _))
    if (compressed) map.put("Compressed", "")
    CatalogUtils.maskCredentials(properties) match {
      case props if props.isEmpty => // No-op
      case props =>
        map.put("Storage Properties", props.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]"))
    }
    map
  }
}

object CatalogStorageFormat {
  /** Empty storage format for default values and copies. */
  val empty = CatalogStorageFormat(locationUri = None, inputFormat = None,
    outputFormat = None, serde = None, compressed = false, properties = Map.empty)
}

/**
 * A partition (Hive style) defined in the catalog.
 *
 * @param spec partition spec values indexed by column name
 * @param storage storage format of the partition
 * @param parameters some parameters for the partition
 * @param createTime creation time of the partition, in milliseconds
 * @param lastAccessTime last access time, in milliseconds
 * @param stats optional statistics (number of rows, total size, etc.)
 */
case class CatalogTablePartition(
    spec: CatalogTypes.TablePartitionSpec,
    storage: CatalogStorageFormat,
    parameters: Map[String, String] = Map.empty,
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    stats: Option[CatalogStatistics] = None) {

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    map.put("Partition Values", s"[$specString]")
    map ++= storage.toLinkedHashMap
    if (parameters.nonEmpty) {
      map.put("Partition Parameters", s"{${parameters.map(p => p._1 + "=" + p._2).mkString(", ")}}")
    }
    map.put("Created Time", new Date(createTime).toString)
    val lastAccess = {
      if (-1 == lastAccessTime) "UNKNOWN" else new Date(lastAccessTime).toString
    }
    map.put("Last Access", lastAccess)
    stats.foreach(s => map.put("Partition Statistics", s.simpleString))
    map
  }

  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("CatalogPartition(\n\t", "\n\t", ")")
  }

  /** Readable string representation for the CatalogTablePartition. */
  def simpleString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }

  /** Return the partition location, assuming it is specified. */
  def location: URI = storage.locationUri.getOrElse {
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    throw new AnalysisException(s"Partition [$specString] did not specify locationUri")
  }

  /**
   * Given the partition schema, returns a row with that schema holding the partition values.
   */
  def toRow(partitionSchema: StructType, defaultTimeZondId: String): InternalRow = {
    val caseInsensitiveProperties = CaseInsensitiveMap(storage.properties)
    val timeZoneId = caseInsensitiveProperties.getOrElse(
      DateTimeUtils.TIMEZONE_OPTION, defaultTimeZondId)
    InternalRow.fromSeq(partitionSchema.map { field =>
      val partValue = if (spec(field.name) == ExternalCatalogUtils.DEFAULT_PARTITION_NAME) {
        null
      } else {
        spec(field.name)
      }
      Cast(Literal(partValue), field.dataType, Option(timeZoneId)).eval()
    })
  }
}


/**
 * A container for bucketing information.
 * Bucketing is a technology for decomposing data sets into more manageable parts, and the number
 * of buckets is fixed so it does not fluctuate with data.
 *
 * @param numBuckets number of buckets.
 * @param bucketColumnNames the names of the columns that used to generate the bucket id.
 * @param sortColumnNames the names of the columns that used to sort data in each bucket.
 */
case class BucketSpec(
    numBuckets: Int,
    bucketColumnNames: Seq[String],
    sortColumnNames: Seq[String]) {
  def conf: SQLConf = SQLConf.get

  if (numBuckets <= 0 || numBuckets > conf.bucketingMaxBuckets) {
    throw new AnalysisException(
      s"Number of buckets should be greater than 0 but less than or equal to " +
        s"bucketing.maxBuckets (`${conf.bucketingMaxBuckets}`). Got `$numBuckets`")
  }

  override def toString: String = {
    val bucketString = s"bucket columns: [${bucketColumnNames.mkString(", ")}]"
    val sortString = if (sortColumnNames.nonEmpty) {
      s", sort columns: [${sortColumnNames.mkString(", ")}]"
    } else {
      ""
    }
    s"$numBuckets buckets, $bucketString$sortString"
  }

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    mutable.LinkedHashMap[String, String](
      "Num Buckets" -> numBuckets.toString,
      "Bucket Columns" -> bucketColumnNames.map(quoteIdentifier).mkString("[", ", ", "]"),
      "Sort Columns" -> sortColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
    )
  }
}

/**
 * A table defined in the catalog.
 *
 * Note that Hive's metastore also tracks skewed columns. We should consider adding that in the
 * future once we have a better understanding of how we want to handle skewed columns.
 *
 * @param provider the name of the data source provider for this table, e.g. parquet, json, etc.
 *                 Can be None if this table is a View, should be "hive" for hive serde tables.
 * @param unsupportedFeatures is a list of string descriptions of features that are used by the
 *        underlying table but not supported by Spark SQL yet.
 * @param tracksPartitionsInCatalog whether this table's partition metadata is stored in the
 *                                  catalog. If false, it is inferred automatically based on file
 *                                  structure.
 * @param schemaPreservesCase Whether or not the schema resolved for this table is case-sensitive.
 *                           When using a Hive Metastore, this flag is set to false if a case-
 *                           sensitive schema was unable to be read from the table properties.
 *                           Used to trigger case-sensitive schema inference at query time, when
 *                           configured.
 * @param ignoredProperties is a list of table properties that are used by the underlying table
 *                          but ignored by Spark SQL yet.
 * @param createVersion records the version of Spark that created this table metadata. The default
 *                      is an empty string. We expect it will be read from the catalog or filled by
 *                      ExternalCatalog.createTable. For temporary views, the value will be empty.
 */
case class CatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String] = None,
    partitionColumnNames: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    owner: String = "",
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    createVersion: String = "",
    properties: Map[String, String] = Map.empty,
    stats: Option[CatalogStatistics] = None,
    viewText: Option[String] = None,
    comment: Option[String] = None,
    unsupportedFeatures: Seq[String] = Seq.empty,
    tracksPartitionsInCatalog: Boolean = false,
    schemaPreservesCase: Boolean = true,
    ignoredProperties: Map[String, String] = Map.empty) {

  import CatalogTable._

  /**
   * schema of this table's partition columns
   */
  def partitionSchema: StructType = {
    val partitionFields = schema.takeRight(partitionColumnNames.length)
    assert(partitionFields.map(_.name) == partitionColumnNames)

    StructType(partitionFields)
  }

  /**
   * schema of this table's data columns
   */
  def dataSchema: StructType = {
    val dataFields = schema.dropRight(partitionColumnNames.length)
    StructType(dataFields)
  }

  /** Return the database this table was specified to belong to, assuming it exists. */
  def database: String = identifier.database.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify database")
  }

  /** Return the table location, assuming it is specified. */
  def location: URI = storage.locationUri.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify locationUri")
  }

  /** Return the fully qualified name of this table, assuming the database was specified. */
  def qualifiedName: String = identifier.unquotedString

  /**
   * Return the default database name we use to resolve a view, should be None if the CatalogTable
   * is not a View or created by older versions of Spark(before 2.2.0).
   */
  def viewDefaultDatabase: Option[String] = properties.get(VIEW_DEFAULT_DATABASE)

  /**
   * Return the output column names of the query that creates a view, the column names are used to
   * resolve a view, should be empty if the CatalogTable is not a View or created by older versions
   * of Spark(before 2.2.0).
   */
  def viewQueryColumnNames: Seq[String] = {
    for {
      numCols <- properties.get(VIEW_QUERY_OUTPUT_NUM_COLUMNS).toSeq
      index <- 0 until numCols.toInt
    } yield properties.getOrElse(
      s"$VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX$index",
      throw new AnalysisException("Corrupted view query output column names in catalog: " +
        s"$numCols parts expected, but part $index is missing.")
    )
  }

  /** Syntactic sugar to update a field in `storage`. */
  def withNewStorage(
      locationUri: Option[URI] = storage.locationUri,
      inputFormat: Option[String] = storage.inputFormat,
      outputFormat: Option[String] = storage.outputFormat,
      compressed: Boolean = false,
      serde: Option[String] = storage.serde,
      properties: Map[String, String] = storage.properties): CatalogTable = {
    copy(storage = CatalogStorageFormat(
      locationUri, inputFormat, outputFormat, serde, compressed, properties))
  }


  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    val tableProperties = properties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
    val partitionColumns = partitionColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")

    identifier.database.foreach(map.put("Database", _))
    map.put("Table", identifier.table)
    if (owner != null && owner.nonEmpty) map.put("Owner", owner)
    map.put("Created Time", new Date(createTime).toString)
    map.put("Last Access", new Date(lastAccessTime).toString)
    map.put("Created By", "Spark " + createVersion)
    map.put("Type", tableType.name)
    provider.foreach(map.put("Provider", _))
    bucketSpec.foreach(map ++= _.toLinkedHashMap)
    comment.foreach(map.put("Comment", _))
    if (tableType == CatalogTableType.VIEW) {
      viewText.foreach(map.put("View Text", _))
      viewDefaultDatabase.foreach(map.put("View Default Database", _))
      if (viewQueryColumnNames.nonEmpty) {
        map.put("View Query Output Columns", viewQueryColumnNames.mkString("[", ", ", "]"))
      }
    }

    if (properties.nonEmpty) map.put("Table Properties", tableProperties)
    stats.foreach(s => map.put("Statistics", s.simpleString))
    map ++= storage.toLinkedHashMap
    if (tracksPartitionsInCatalog) map.put("Partition Provider", "Catalog")
    if (partitionColumnNames.nonEmpty) map.put("Partition Columns", partitionColumns)
    if (schema.nonEmpty) map.put("Schema", schema.treeString)

    map
  }

  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("CatalogTable(\n", "\n", ")")
  }

  /** Readable string representation for the CatalogTable. */
  def simpleString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }
}

object CatalogTable {
  val VIEW_DEFAULT_DATABASE = "view.default.database"
  val VIEW_QUERY_OUTPUT_PREFIX = "view.query.out."
  val VIEW_QUERY_OUTPUT_NUM_COLUMNS = VIEW_QUERY_OUTPUT_PREFIX + "numCols"
  val VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX = VIEW_QUERY_OUTPUT_PREFIX + "col."
}

/**
 * This class of statistics is used in [[CatalogTable]] to interact with metastore.
 * We define this new class instead of directly using [[Statistics]] here because there are no
 * concepts of attributes or broadcast hint in catalog.
 */
case class CatalogStatistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    colStats: Map[String, CatalogColumnStat] = Map.empty) {

  /**
   * Convert [[CatalogStatistics]] to [[Statistics]], and match column stats to attributes based
   * on column names.
   */
  def toPlanStats(planOutput: Seq[Attribute], cboEnabled: Boolean): Statistics = {
    if (cboEnabled && rowCount.isDefined) {
      val attrStats = AttributeMap(planOutput
        .flatMap(a => colStats.get(a.name).map(a -> _.toPlanStat(a.name, a.dataType))))
      // Estimate size as number of rows * row size.
      val size = EstimationUtils.getOutputSize(planOutput, rowCount.get, attrStats)
      Statistics(sizeInBytes = size, rowCount = rowCount, attributeStats = attrStats)
    } else {
      // When CBO is disabled or the table doesn't have other statistics, we apply the size-only
      // estimation strategy and only propagate sizeInBytes in statistics.
      Statistics(sizeInBytes = sizeInBytes)
    }
  }

  /** Readable string representation for the CatalogStatistics. */
  def simpleString: String = {
    val rowCountString = if (rowCount.isDefined) s", ${rowCount.get} rows" else ""
    s"$sizeInBytes bytes$rowCountString"
  }
}

/**
 * This class of statistics for a column is used in [[CatalogTable]] to interact with metastore.
 */
case class CatalogColumnStat(
    distinctCount: Option[BigInt] = None,
    min: Option[String] = None,
    max: Option[String] = None,
    nullCount: Option[BigInt] = None,
    avgLen: Option[Long] = None,
    maxLen: Option[Long] = None,
    histogram: Option[Histogram] = None) {

  /**
   * Returns a map from string to string that can be used to serialize the column stats.
   * The key is the name of the column and name of the field (e.g. "colName.distinctCount"),
   * and the value is the string representation for the value.
   * min/max values are stored as Strings. They can be deserialized using
   * [[CatalogColumnStat.fromExternalString]].
   *
   * As part of the protocol, the returned map always contains a key called "version".
   * Any of the fields that are null (None) won't appear in the map.
   */
  def toMap(colName: String): Map[String, String] = {
    val map = new scala.collection.mutable.HashMap[String, String]
    map.put(s"${colName}.${CatalogColumnStat.KEY_VERSION}", "1")
    distinctCount.foreach { v =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_DISTINCT_COUNT}", v.toString)
    }
    nullCount.foreach { v =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_NULL_COUNT}", v.toString)
    }
    avgLen.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_AVG_LEN}", v.toString) }
    maxLen.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MAX_LEN}", v.toString) }
    min.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MIN_VALUE}", v) }
    max.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MAX_VALUE}", v) }
    histogram.foreach { h =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_HISTOGRAM}", HistogramSerializer.serialize(h))
    }
    map.toMap
  }

  /** Convert [[CatalogColumnStat]] to [[ColumnStat]]. */
  def toPlanStat(
      colName: String,
      dataType: DataType): ColumnStat =
    ColumnStat(
      distinctCount = distinctCount,
      min = min.map(CatalogColumnStat.fromExternalString(_, colName, dataType)),
      max = max.map(CatalogColumnStat.fromExternalString(_, colName, dataType)),
      nullCount = nullCount,
      avgLen = avgLen,
      maxLen = maxLen,
      histogram = histogram)
}

object CatalogColumnStat extends Logging {

  // List of string keys used to serialize CatalogColumnStat
  val KEY_VERSION = "version"
  private val KEY_DISTINCT_COUNT = "distinctCount"
  private val KEY_MIN_VALUE = "min"
  private val KEY_MAX_VALUE = "max"
  private val KEY_NULL_COUNT = "nullCount"
  private val KEY_AVG_LEN = "avgLen"
  private val KEY_MAX_LEN = "maxLen"
  private val KEY_HISTOGRAM = "histogram"

  /**
   * Converts from string representation of data type to the corresponding Catalyst data type.
   */
  def fromExternalString(s: String, name: String, dataType: DataType): Any = {
    dataType match {
      case BooleanType => s.toBoolean
      case DateType => DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(s))
      case TimestampType => DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(s))
      case ByteType => s.toByte
      case ShortType => s.toShort
      case IntegerType => s.toInt
      case LongType => s.toLong
      case FloatType => s.toFloat
      case DoubleType => s.toDouble
      case _: DecimalType => Decimal(s)
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case BinaryType | StringType => null
      case _ =>
        throw new AnalysisException("Column statistics deserialization is not supported for " +
          s"column $name of data type: $dataType.")
    }
  }

  /**
   * Converts the given value from Catalyst data type to string representation of external
   * data type.
   */
  def toExternalString(v: Any, colName: String, dataType: DataType): String = {
    val externalValue = dataType match {
      case DateType => DateTimeUtils.toJavaDate(v.asInstanceOf[Int])
      case TimestampType => DateTimeUtils.toJavaTimestamp(v.asInstanceOf[Long])
      case BooleanType | _: IntegralType | FloatType | DoubleType => v
      case _: DecimalType => v.asInstanceOf[Decimal].toJavaBigDecimal
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case _ =>
        throw new AnalysisException("Column statistics serialization is not supported for " +
          s"column $colName of data type: $dataType.")
    }
    externalValue.toString
  }


  /**
   * Creates a [[CatalogColumnStat]] object from the given map.
   * This is used to deserialize column stats from some external storage.
   * The serialization side is defined in [[CatalogColumnStat.toMap]].
   */
  def fromMap(
    table: String,
    colName: String,
    map: Map[String, String]): Option[CatalogColumnStat] = {

    try {
      Some(CatalogColumnStat(
        distinctCount = map.get(s"${colName}.${KEY_DISTINCT_COUNT}").map(v => BigInt(v.toLong)),
        min = map.get(s"${colName}.${KEY_MIN_VALUE}"),
        max = map.get(s"${colName}.${KEY_MAX_VALUE}"),
        nullCount = map.get(s"${colName}.${KEY_NULL_COUNT}").map(v => BigInt(v.toLong)),
        avgLen = map.get(s"${colName}.${KEY_AVG_LEN}").map(_.toLong),
        maxLen = map.get(s"${colName}.${KEY_MAX_LEN}").map(_.toLong),
        histogram = map.get(s"${colName}.${KEY_HISTOGRAM}").map(HistogramSerializer.deserialize)
      ))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to parse column statistics for column ${colName} in table $table", e)
        None
    }
  }
}


case class CatalogTableType private(name: String)
object CatalogTableType {
  val EXTERNAL = new CatalogTableType("EXTERNAL")
  val MANAGED = new CatalogTableType("MANAGED")
  val VIEW = new CatalogTableType("VIEW")
}


/**
 * A database defined in the catalog.
 */
case class CatalogDatabase(
    name: String,
    description: String,
    locationUri: URI,
    properties: Map[String, String])


object CatalogTypes {
  /**
   * Specifications of a table partition. Mapping column name to column value.
   */
  type TablePartitionSpec = Map[String, String]

  /**
   * Initialize an empty spec.
   */
  lazy val emptyTablePartitionSpec: TablePartitionSpec = Map.empty[String, String]
}

/**
 * A placeholder for a table relation, which will be replaced by concrete relation like
 * `LogicalRelation` or `HiveTableRelation`, during analysis.
 */
case class UnresolvedCatalogRelation(tableMeta: CatalogTable) extends LeafNode {
  assert(tableMeta.identifier.database.isDefined)
  override lazy val resolved: Boolean = false
  override def output: Seq[Attribute] = Nil
}

/**
 * A `LogicalPlan` that represents a hive table.
 *
 * TODO: remove this after we completely make hive as a data source.
 */
case class HiveTableRelation(
    tableMeta: CatalogTable,
    dataCols: Seq[AttributeReference],
    partitionCols: Seq[AttributeReference]) extends LeafNode with MultiInstanceRelation {
  assert(tableMeta.identifier.database.isDefined)
  assert(tableMeta.partitionSchema.sameType(partitionCols.toStructType))
  assert(tableMeta.dataSchema.sameType(dataCols.toStructType))

  // The partition column should always appear after data columns.
  override def output: Seq[AttributeReference] = dataCols ++ partitionCols

  def isPartitioned: Boolean = partitionCols.nonEmpty

  override def doCanonicalize(): HiveTableRelation = copy(
    tableMeta = tableMeta.copy(
      storage = CatalogStorageFormat.empty,
      createTime = -1
    ),
    dataCols = dataCols.zipWithIndex.map {
      case (attr, index) => attr.withExprId(ExprId(index))
    },
    partitionCols = partitionCols.zipWithIndex.map {
      case (attr, index) => attr.withExprId(ExprId(index + dataCols.length))
    }
  )

  override def computeStats(): Statistics = {
    tableMeta.stats.map(_.toPlanStats(output, conf.cboEnabled)).getOrElse {
      throw new IllegalStateException("table stats must be specified.")
    }
  }

  override def newInstance(): HiveTableRelation = copy(
    dataCols = dataCols.map(_.newInstance()),
    partitionCols = partitionCols.map(_.newInstance()))
}
