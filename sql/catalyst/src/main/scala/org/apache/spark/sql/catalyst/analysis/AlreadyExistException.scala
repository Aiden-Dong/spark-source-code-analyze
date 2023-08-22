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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec

/**
 * Thrown by a catalog when an item already exists. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
class DatabaseAlreadyExistsException(db: String)
  extends AnalysisException(s"Database '$db' already exists")

class TableAlreadyExistsException(db: String, table: String)
  extends AnalysisException(s"Table or view '$table' already exists in database '$db'")

class TempTableAlreadyExistsException(table: String)
  extends AnalysisException(s"Temporary view '$table' already exists")

class PartitionAlreadyExistsException(db: String, table: String, spec: TablePartitionSpec)
  extends AnalysisException(
    s"Partition already exists in table '$table' database '$db':\n" + spec.mkString("\n"))

class PartitionsAlreadyExistException(db: String, table: String, specs: Seq[TablePartitionSpec])
  extends AnalysisException(
    s"The following partitions already exists in table '$table' database '$db':\n"
      + specs.mkString("\n===\n"))

class FunctionAlreadyExistsException(db: String, func: String)
  extends AnalysisException(s"Function '$func' already exists in database '$db'")
