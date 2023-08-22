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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * An abstract class that represents [[FileIndex]]s that are aware of partitioned tables.
 * It provides the necessary methods to parse partition data based on a set of files.
 *
 * @param parameters as set of options to control partition discovery
 * @param userSpecifiedSchema an optional user specified schema that will be use to provide
 *                            types for the discovered partitions
 */
abstract class PartitioningAwareFileIndex(
    sparkSession: SparkSession,
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    fileStatusCache: FileStatusCache = NoopCache) extends FileIndex with Logging {
  import PartitioningAwareFileIndex.BASE_PATH_PARAM

  /** Returns the specification of the partitions inferred from the data. */
  def partitionSpec(): PartitionSpec

  override def partitionSchema: StructType = partitionSpec().partitionColumns

  protected val hadoopConf: Configuration =
    sparkSession.sessionState.newHadoopConfWithOptions(parameters)

  protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus]

  protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]]

  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val selectedPartitions = if (partitionSpec().partitionColumns.isEmpty) {
      PartitionDirectory(InternalRow.empty, allFiles().filter(f => isDataPath(f.getPath))) :: Nil
    } else {
      prunePartitions(partitionFilters, partitionSpec()).map {
        case PartitionPath(values, path) =>
          val files: Seq[FileStatus] = leafDirToChildrenFiles.get(path) match {
            case Some(existingDir) =>
              // Directory has children files in it, return them
              existingDir.filter(f => isDataPath(f.getPath))

            case None =>
              // Directory does not exist, or has no children files
              Nil
          }
          PartitionDirectory(values, files)
      }
    }
    logTrace("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))
    selectedPartitions
  }

  /** Returns the list of files that will be read when scanning this relation. */
  override def inputFiles: Array[String] =
    allFiles().map(_.getPath.toUri.toString).toArray

  override def sizeInBytes: Long = allFiles().map(_.getLen).sum

  def allFiles(): Seq[FileStatus] = {
    if (partitionSpec().partitionColumns.isEmpty) {
      // For each of the root input paths, get the list of files inside them
      rootPaths.flatMap { path =>
        // Make the path qualified (consistent with listLeafFiles and listLeafFilesInParallel).
        val fs = path.getFileSystem(hadoopConf)
        val qualifiedPathPre = fs.makeQualified(path)
        val qualifiedPath: Path = if (qualifiedPathPre.isRoot && !qualifiedPathPre.isAbsolute) {
          // SPARK-17613: Always append `Path.SEPARATOR` to the end of parent directories,
          // because the `leafFile.getParent` would have returned an absolute path with the
          // separator at the end.
          new Path(qualifiedPathPre, Path.SEPARATOR)
        } else {
          qualifiedPathPre
        }

        // There are three cases possible with each path
        // 1. The path is a directory and has children files in it. Then it must be present in
        //    leafDirToChildrenFiles as those children files will have been found as leaf files.
        //    Find its children files from leafDirToChildrenFiles and include them.
        // 2. The path is a file, then it will be present in leafFiles. Include this path.
        // 3. The path is a directory, but has no children files. Do not include this path.

        leafDirToChildrenFiles.get(qualifiedPath)
          .orElse { leafFiles.get(qualifiedPath).map(Array(_)) }
          .getOrElse(Array.empty)
      }
    } else {
      leafFiles.values.toSeq
    }
  }

  protected def inferPartitioning(): PartitionSpec = {
    // We use leaf dirs containing data files to discover the schema.
    val leafDirs = leafDirToChildrenFiles.filter { case (_, files) =>
      files.exists(f => isDataPath(f.getPath))
    }.keys.toSeq

    val caseInsensitiveOptions = CaseInsensitiveMap(parameters)
    val timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
      .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)
    val inferredPartitionSpec = PartitioningUtils.parsePartitions(
      leafDirs,
      typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled,
      basePaths = basePaths,
      timeZoneId = timeZoneId)
    userSpecifiedSchema match {
      case Some(userProvidedSchema) if userProvidedSchema.nonEmpty =>
        val userPartitionSchema =
          combineInferredAndUserSpecifiedPartitionSchema(inferredPartitionSpec)

        // we need to cast into the data type that user specified.
        def castPartitionValuesToUserSchema(row: InternalRow) = {
          InternalRow((0 until row.numFields).map { i =>
            val dt = inferredPartitionSpec.partitionColumns.fields(i).dataType
            Cast(
              Literal.create(row.get(i, dt), dt),
              userPartitionSchema.fields(i).dataType,
              Option(timeZoneId)).eval()
          }: _*)
        }

        PartitionSpec(userPartitionSchema, inferredPartitionSpec.partitions.map { part =>
          part.copy(values = castPartitionValuesToUserSchema(part.values))
        })
      case _ =>
        inferredPartitionSpec
    }
  }

  private def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[PartitionPath] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }
      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, " +
          s"pruned ${if (total == 0) "0" else s"$percentPruned%"} partitions."
      }

      selected
    } else {
      partitions
    }
  }

  /**
   * Contains a set of paths that are considered as the base dirs of the input datasets.
   * The partitioning discovery logic will make sure it will stop when it reaches any
   * base path.
   *
   * By default, the paths of the dataset provided by users will be base paths.
   * Below are three typical examples,
   * Case 1) `spark.read.parquet("/path/something=true/")`: the base path will be
   * `/path/something=true/`, and the returned DataFrame will not contain a column of `something`.
   * Case 2) `spark.read.parquet("/path/something=true/a.parquet")`: the base path will be
   * still `/path/something=true/`, and the returned DataFrame will also not contain a column of
   * `something`.
   * Case 3) `spark.read.parquet("/path/")`: the base path will be `/path/`, and the returned
   * DataFrame will have the column of `something`.
   *
   * Users also can override the basePath by setting `basePath` in the options to pass the new base
   * path to the data source.
   * For example, `spark.read.option("basePath", "/path/").parquet("/path/something=true/")`,
   * and the returned DataFrame will have the column of `something`.
   */
  private def basePaths: Set[Path] = {
    parameters.get(BASE_PATH_PARAM).map(new Path(_)) match {
      case Some(userDefinedBasePath) =>
        val fs = userDefinedBasePath.getFileSystem(hadoopConf)
        if (!fs.isDirectory(userDefinedBasePath)) {
          throw new IllegalArgumentException(s"Option '$BASE_PATH_PARAM' must be a directory")
        }
        Set(fs.makeQualified(userDefinedBasePath))

      case None =>
        rootPaths.map { path =>
          // Make the path qualified (consistent with listLeafFiles and listLeafFilesInParallel).
          val qualifiedPath = path.getFileSystem(hadoopConf).makeQualified(path)
          if (leafFiles.contains(qualifiedPath)) qualifiedPath.getParent else qualifiedPath }.toSet
    }
  }

  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }

  /**
   * In the read path, only managed tables by Hive provide the partition columns properly when
   * initializing this class. All other file based data sources will try to infer the partitioning,
   * and then cast the inferred types to user specified dataTypes if the partition columns exist
   * inside `userSpecifiedSchema`, otherwise we can hit data corruption bugs like SPARK-18510, or
   * inconsistent data types as reported in SPARK-21463.
   * @param spec A partition inference result
   * @return The PartitionSchema resolved from inference and cast according to `userSpecifiedSchema`
   */
  private def combineInferredAndUserSpecifiedPartitionSchema(spec: PartitionSpec): StructType = {
    val equality = sparkSession.sessionState.conf.resolver
    val resolved = spec.partitionColumns.map { partitionField =>
      // SPARK-18510: try to get schema from userSpecifiedSchema, otherwise fallback to inferred
      userSpecifiedSchema.flatMap(_.find(f => equality(f.name, partitionField.name))).getOrElse(
        partitionField)
    }
    StructType(resolved)
  }
}

object PartitioningAwareFileIndex {
  val BASE_PATH_PARAM = "basePath"
}
