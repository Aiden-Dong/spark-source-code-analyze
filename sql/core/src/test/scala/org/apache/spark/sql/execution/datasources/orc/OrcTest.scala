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

package org.apache.spark.sql.execution.datasources.orc

import java.io.File

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf.ORC_IMPLEMENTATION
import org.apache.spark.sql.test.SQLTestUtils

/**
 * OrcTest
 *   -> OrcSuite
 *       -> OrcSourceSuite
 *       -> HiveOrcSourceSuite
 *   -> OrcQueryTests
 *       -> OrcQuerySuite
 *       -> HiveOrcQuerySuite
 *   -> OrcPartitionDiscoveryTest
 *       -> OrcPartitionDiscoverySuite
 *       -> HiveOrcPartitionDiscoverySuite
 *   -> OrcFilterSuite
 *   -> HiveOrcFilterSuite
 */
abstract class OrcTest extends QueryTest with SQLTestUtils with BeforeAndAfterAll {
  import testImplicits._

  val orcImp: String = "native"

  private var originalConfORCImplementation = "native"

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    originalConfORCImplementation = spark.conf.get(ORC_IMPLEMENTATION)
    spark.conf.set(ORC_IMPLEMENTATION.key, orcImp)
  }

  protected override def afterAll(): Unit = {
    spark.conf.set(ORC_IMPLEMENTATION.key, originalConfORCImplementation)
    super.afterAll()
  }

  /**
   * Writes `data` to a Orc file, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withOrcFile[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: String => Unit): Unit = {
    withTempPath { file =>
      sparkContext.parallelize(data).toDF().write.orc(file.getCanonicalPath)
      f(file.getCanonicalPath)
    }
  }

  /**
   * Writes `data` to a Orc file and reads it back as a `DataFrame`,
   * which is then passed to `f`. The Orc file will be deleted after `f` returns.
   */
  protected def withOrcDataFrame[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: DataFrame => Unit): Unit = {
    withOrcFile(data)(path => f(spark.read.orc(path)))
  }

  /**
   * Writes `data` to a Orc file, reads it back as a `DataFrame` and registers it as a
   * temporary table named `tableName`, then call `f`. The temporary table together with the
   * Orc file will be dropped/deleted after `f` returns.
   */
  protected def withOrcTable[T <: Product: ClassTag: TypeTag]
      (data: Seq[T], tableName: String)
      (f: => Unit): Unit = {
    withOrcDataFrame(data) { df =>
      df.createOrReplaceTempView(tableName)
      withTempView(tableName)(f)
    }
  }

  protected def makeOrcFile[T <: Product: ClassTag: TypeTag](
      data: Seq[T], path: File): Unit = {
    data.toDF().write.mode(SaveMode.Overwrite).orc(path.getCanonicalPath)
  }

  protected def makeOrcFile[T <: Product: ClassTag: TypeTag](
      df: DataFrame, path: File): Unit = {
    df.write.mode(SaveMode.Overwrite).orc(path.getCanonicalPath)
  }

  protected def checkPredicatePushDown(df: DataFrame, numRows: Int, predicate: String): Unit = {
    withTempPath { file =>
      // It needs to repartition data so that we can have several ORC files
      // in order to skip stripes in ORC.
      df.repartition(numRows).write.orc(file.getCanonicalPath)
      val actual = stripSparkFilter(spark.read.orc(file.getCanonicalPath).where(predicate)).count()
      assert(actual < numRows)
    }
  }
}
