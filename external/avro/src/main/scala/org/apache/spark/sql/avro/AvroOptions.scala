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

package org.apache.spark.sql.avro

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

/**
 * Options for Avro Reader and Writer stored in case insensitive manner.
 */
class AvroOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    @transient val conf: Configuration) extends Logging with Serializable {

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf)
  }

  /**
   * Optional schema provided by an user in JSON format.
   */
  val schema: Option[String] = parameters.get("avroSchema")

  /**
   * Top level record name in write result, which is required in Avro spec.
   * See https://avro.apache.org/docs/1.8.2/spec.html#schema_record .
   * Default value is "topLevelRecord"
   */
  val recordName: String = parameters.getOrElse("recordName", "topLevelRecord")

  /**
   * Record namespace in write result. Default value is "".
   * See Avro spec for details: https://avro.apache.org/docs/1.8.2/spec.html#schema_record .
   */
  val recordNamespace: String = parameters.getOrElse("recordNamespace", "")

  /**
   * The `ignoreExtension` option controls ignoring of files without `.avro` extensions in read.
   * If the option is enabled, all files (with and without `.avro` extension) are loaded.
   * If the option is not set, the Hadoop's config `avro.mapred.ignore.inputs.without.extension`
   * is taken into account. If the former one is not set too, file extensions are ignored.
   */
  val ignoreExtension: Boolean = {
    val ignoreFilesWithoutExtensionByDefault = false
    val ignoreFilesWithoutExtension = conf.getBoolean(
      AvroFileFormat.IgnoreFilesWithoutExtensionProperty,
      ignoreFilesWithoutExtensionByDefault)

    parameters
      .get("ignoreExtension")
      .map(_.toBoolean)
      .getOrElse(!ignoreFilesWithoutExtension)
  }

  /**
   * The `compression` option allows to specify a compression codec used in write.
   * Currently supported codecs are `uncompressed`, `snappy`, `deflate`, `bzip2` and `xz`.
   * If the option is not set, the `spark.sql.avro.compression.codec` config is taken into
   * account. If the former one is not set too, the `snappy` codec is used by default.
   */
  val compression: String = {
    parameters.get("compression").getOrElse(SQLConf.get.avroCompressionCodec)
  }
}
