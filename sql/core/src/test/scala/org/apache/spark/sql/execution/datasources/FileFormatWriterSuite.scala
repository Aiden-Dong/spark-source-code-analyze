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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSQLContext

class FileFormatWriterSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("empty file should be skipped while write to file") {
    withTempPath { path =>
      spark.range(100).repartition(10).where("id = 50").write.parquet(path.toString)
      val partFiles = path.listFiles()
        .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      assert(partFiles.length === 2)
    }
  }

  test("SPARK-22252: FileFormatWriter should respect the input query schema") {
    withTable("t1", "t2", "t3", "t4") {
      spark.range(1).select('id as 'col1, 'id as 'col2).write.saveAsTable("t1")
      spark.sql("select COL1, COL2 from t1").write.saveAsTable("t2")
      checkAnswer(spark.table("t2"), Row(0, 0))

      // Test picking part of the columns when writing.
      spark.range(1).select('id, 'id as 'col1, 'id as 'col2).write.saveAsTable("t3")
      spark.sql("select COL1, COL2 from t3").write.saveAsTable("t4")
      checkAnswer(spark.table("t4"), Row(0, 0))
    }
  }
}
