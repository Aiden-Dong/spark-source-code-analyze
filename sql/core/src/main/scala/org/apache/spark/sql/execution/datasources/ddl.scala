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

import java.util.Locale

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand}
import org.apache.spark.sql.types._

/**
 * Create a table and optionally insert some data into it. Note that this plan is unresolved and
 * has to be replaced by the concrete implementations during analysis.
 *
 * @param tableDesc the metadata of the table to be created.
 * @param mode the data writing mode
 * @param query an optional logical plan representing data to write into the created table.
 */
case class CreateTable(
    tableDesc: CatalogTable,
    mode: SaveMode,
    query: Option[LogicalPlan]) extends LogicalPlan {
  assert(tableDesc.provider.isDefined, "The table to be created must have a provider.")

  if (query.isEmpty) {
    assert(
      mode == SaveMode.ErrorIfExists || mode == SaveMode.Ignore,
      "create table without data insertion can only use ErrorIfExists or Ignore as SaveMode.")
  }

  override def children: Seq[LogicalPlan] = query.toSeq
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = false
}

/**
 * Create or replace a local/global temporary view with given data source.
 */
case class CreateTempViewUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    replace: Boolean,
    global: Boolean,
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  if (tableIdent.database.isDefined) {
    throw new AnalysisException(
      s"Temporary view '$tableIdent' should not have specified a database")
  }

  override def argString: String = {
    s"[tableIdent:$tableIdent " +
      userSpecifiedSchema.map(_ + " ").getOrElse("") +
      s"replace:$replace " +
      s"provider:$provider " +
      CatalogUtils.maskCredentials(options)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (provider.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, " +
        "you can't use it with CREATE TEMP VIEW USING")
    }

    val dataSource = DataSource(
      sparkSession,
      userSpecifiedSchema = userSpecifiedSchema,
      className = provider,
      options = options)

    val catalog = sparkSession.sessionState.catalog
    val viewDefinition = Dataset.ofRows(
      sparkSession, LogicalRelation(dataSource.resolveRelation())).logicalPlan

    if (global) {
      catalog.createGlobalTempView(tableIdent.table, viewDefinition, replace)
    } else {
      catalog.createTempView(tableIdent.table, viewDefinition, replace)
    }

    Seq.empty[Row]
  }
}

case class RefreshTable(tableIdent: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Refresh the given table's metadata. If this table is cached as an InMemoryRelation,
    // drop the original cached version and make the new version cached lazily.
    sparkSession.catalog.refreshTable(tableIdent.quotedString)
    Seq.empty[Row]
  }
}

case class RefreshResource(path: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.refreshByPath(path)
    Seq.empty[Row]
  }
}
