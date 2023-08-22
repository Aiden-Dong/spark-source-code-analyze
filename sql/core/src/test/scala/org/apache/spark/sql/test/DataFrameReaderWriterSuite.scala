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

package org.apache.spark.sql.test

import java.io.File
import java.util.Locale
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


object LastOptions {

  var parameters: Map[String, String] = null
  var schema: Option[StructType] = null
  var saveMode: SaveMode = null

  def clear(): Unit = {
    parameters = null
    schema = null
    saveMode = null
  }
}

/** Dummy provider. */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  case class FakeRelation(sqlContext: SQLContext) extends BaseRelation {
    override def schema: StructType = StructType(Seq(StructField("a", StringType)))
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType
    ): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = Some(schema)
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
    ): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = None
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = None
    LastOptions.saveMode = mode
    FakeRelation(sqlContext)
  }
}

/** Dummy provider with only RelationProvider and CreatableRelationProvider. */
class DefaultSourceWithoutUserSpecifiedSchema
  extends RelationProvider
  with CreatableRelationProvider {

  case class FakeRelation(sqlContext: SQLContext) extends BaseRelation {
    override def schema: StructType = StructType(Seq(StructField("a", StringType)))
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    FakeRelation(sqlContext)
  }
}

object MessageCapturingCommitProtocol {
  val commitMessages = new ConcurrentLinkedQueue[TaskCommitMessage]()
}

class MessageCapturingCommitProtocol(jobId: String, path: String)
    extends HadoopMapReduceCommitProtocol(jobId, path) {

  // captures commit messages for testing
  override def onTaskCommit(msg: TaskCommitMessage): Unit = {
    MessageCapturingCommitProtocol.commitMessages.offer(msg)
  }
}


class DataFrameReaderWriterSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {
  import testImplicits._

  private val userSchema = new StructType().add("s", StringType)
  private val userSchemaString = "s STRING"
  private val textSchema = new StructType().add("value", StringType)
  private val data = Seq("1", "2", "3")
  private val dir = Utils.createTempDir(namePrefix = "input").getCanonicalPath

  before {
    Utils.deleteRecursively(new File(dir))
  }

  test("writeStream cannot be called on non-streaming datasets") {
    val e = intercept[AnalysisException] {
      spark.read
        .format("org.apache.spark.sql.test")
        .load()
        .writeStream
        .start()
    }
    Seq("'writeStream'", "only", "streaming Dataset/DataFrame").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }

  test("resolve default source") {
    spark.read
      .format("org.apache.spark.sql.test")
      .load()
      .write
      .format("org.apache.spark.sql.test")
      .save()
  }

  test("resolve default source without extending SchemaRelationProvider") {
    spark.read
      .format("org.apache.spark.sql.test.DefaultSourceWithoutUserSpecifiedSchema")
      .load()
      .write
      .format("org.apache.spark.sql.test.DefaultSourceWithoutUserSpecifiedSchema")
      .save()
  }

  test("resolve full class") {
    spark.read
      .format("org.apache.spark.sql.test.DefaultSource")
      .load()
      .write
      .format("org.apache.spark.sql.test")
      .save()
  }

  test("options") {
    val map = new java.util.HashMap[String, String]
    map.put("opt3", "3")

    val df = spark.read
        .format("org.apache.spark.sql.test")
        .option("opt1", "1")
        .options(Map("opt2" -> "2"))
        .options(map)
        .load()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")

    LastOptions.clear()

    df.write
      .format("org.apache.spark.sql.test")
      .option("opt1", "1")
      .options(Map("opt2" -> "2"))
      .options(map)
      .save()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")
  }

  test("save mode") {
    val df = spark.read
      .format("org.apache.spark.sql.test")
      .load()

    df.write
      .format("org.apache.spark.sql.test")
      .mode(SaveMode.ErrorIfExists)
      .save()
    assert(LastOptions.saveMode === SaveMode.ErrorIfExists)
  }

  test("test path option in load") {
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .load("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("path") == "/test")

    LastOptions.clear()
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 55)
      .load()

    assert(LastOptions.parameters("intOpt") == "55")
    assert(!LastOptions.parameters.contains("path"))

    LastOptions.clear()
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 54)
      .load("/test", "/test1", "/test2")

    assert(LastOptions.parameters("intOpt") == "54")
    assert(!LastOptions.parameters.contains("path"))
  }

  test("test different data types for options") {
    val df = spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .load("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")

    LastOptions.clear()
    df.write
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .save("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")
  }

  test("check jdbc() does not support partitioning, bucketBy or sortBy") {
    val df = spark.read.text(Utils.createTempDir(namePrefix = "text").getCanonicalPath)

    var w = df.write.partitionBy("value")
    var e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "partitioning").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }

    w = df.write.bucketBy(2, "value")
    e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "does not support bucketBy right now").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }

    w = df.write.sortBy("value")
    e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("sortBy must be used together with bucketBy").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }

    w = df.write.bucketBy(2, "value").sortBy("value")
    e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "does not support bucketBy and sortBy right now").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }

  test("prevent all column partitioning") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      intercept[AnalysisException] {
        spark.range(10).write.format("parquet").mode("overwrite").partitionBy("id").save(path)
      }
      intercept[AnalysisException] {
        spark.range(10).write.format("csv").mode("overwrite").partitionBy("id").save(path)
      }
    }
  }

  test("load API") {
    spark.read.format("org.apache.spark.sql.test").load()
    spark.read.format("org.apache.spark.sql.test").load(dir)
    spark.read.format("org.apache.spark.sql.test").load(dir, dir, dir)
    spark.read.format("org.apache.spark.sql.test").load(Seq(dir, dir): _*)
    Option(dir).map(spark.read.format("org.apache.spark.sql.test").load)
  }

  test("write path implements onTaskCommit API correctly") {
    withSQLConf(
        "spark.sql.sources.commitProtocolClass" ->
          classOf[MessageCapturingCommitProtocol].getCanonicalName) {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        MessageCapturingCommitProtocol.commitMessages.clear()
        spark.range(10).repartition(10).write.mode("overwrite").parquet(path)
        assert(MessageCapturingCommitProtocol.commitMessages.size() == 10)
      }
    }
  }

  test("read a data source that does not extend SchemaRelationProvider") {
    val dfReader = spark.read
      .option("from", "1")
      .option("TO", "10")
      .format("org.apache.spark.sql.sources.SimpleScanSource")

    // when users do not specify the schema
    checkAnswer(dfReader.load(), spark.range(1, 11).toDF())

    // when users specify the schema
    val inputSchema = new StructType().add("s", IntegerType, nullable = false)
    val e = intercept[AnalysisException] { dfReader.schema(inputSchema).load() }
    assert(e.getMessage.contains(
      "org.apache.spark.sql.sources.SimpleScanSource does not allow user-specified schemas"))
  }

  test("read a data source that does not extend RelationProvider") {
    val dfReader = spark.read
      .option("from", "1")
      .option("TO", "10")
      .option("option_with_underscores", "someval")
      .option("option.with.dots", "someval")
      .format("org.apache.spark.sql.sources.AllDataTypesScanSource")

    // when users do not specify the schema
    val e = intercept[AnalysisException] { dfReader.load() }
    assert(e.getMessage.contains("A schema needs to be specified when using"))

    // when users specify the schema
    val inputSchema = new StructType().add("s", StringType, nullable = false)
    assert(dfReader.schema(inputSchema).load().count() == 10)
  }

  test("text - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).write.mode(SaveMode.Overwrite).text(dir)
    testRead(spark.read.text(dir), data, textSchema)

    // Reader, without user specified schema
    testRead(spark.read.text(), Seq.empty, textSchema)
    testRead(spark.read.text(dir, dir, dir), data ++ data ++ data, textSchema)
    testRead(spark.read.text(Seq(dir, dir): _*), data ++ data, textSchema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.text).get, data, textSchema)

    // Reader, with user specified schema, should just apply user schema on the file data
    testRead(spark.read.schema(userSchema).text(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).text(dir), data, userSchema)
    testRead(spark.read.schema(userSchema).text(dir, dir), data ++ data, userSchema)
    testRead(spark.read.schema(userSchema).text(Seq(dir, dir): _*), data ++ data, userSchema)
  }

  test("textFile - API and behavior regarding schema") {
    spark.createDataset(data).write.mode(SaveMode.Overwrite).text(dir)

    // Reader, without user specified schema
    testRead(spark.read.textFile().toDF(), Seq.empty, textSchema)
    testRead(spark.read.textFile(dir).toDF(), data, textSchema)
    testRead(spark.read.textFile(dir, dir).toDF(), data ++ data, textSchema)
    testRead(spark.read.textFile(Seq(dir, dir): _*).toDF(), data ++ data, textSchema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.text).get, data, textSchema)

    // Reader, with user specified schema, should just apply user schema on the file data
    val e = intercept[AnalysisException] { spark.read.schema(userSchema).textFile() }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains(
      "user specified schema not supported"))
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(dir) }
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(dir, dir) }
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(Seq(dir, dir): _*) }
  }

  test("csv - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).csv(dir)
    val df = spark.read.csv(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    val message = intercept[AnalysisException] {
      testRead(spark.read.csv(), Seq.empty, schema)
    }.getMessage
    assert(message.contains("Unable to infer schema for CSV. It must be specified manually."))

    testRead(spark.read.csv(dir), data, schema)
    testRead(spark.read.csv(dir, dir), data ++ data, schema)
    testRead(spark.read.csv(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.csv).get, data, schema)

    // Reader, with user specified schema, should just apply user schema on the file data
    testRead(spark.read.schema(userSchema).csv(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).csv(dir), data, userSchema)
    testRead(spark.read.schema(userSchema).csv(dir, dir), data ++ data, userSchema)
    testRead(spark.read.schema(userSchema).csv(Seq(dir, dir): _*), data ++ data, userSchema)
  }

  test("json - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).json(dir)
    val df = spark.read.json(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    intercept[AnalysisException] {
      testRead(spark.read.json(), Seq.empty, schema)
    }
    testRead(spark.read.json(dir), data, schema)
    testRead(spark.read.json(dir, dir), data ++ data, schema)
    testRead(spark.read.json(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.json).get, data, schema)

    // Reader, with user specified schema, data should be nulls as schema in file different
    // from user schema
    val expData = Seq[String](null, null, null)
    testRead(spark.read.schema(userSchema).json(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).json(dir), expData, userSchema)
    testRead(spark.read.schema(userSchema).json(dir, dir), expData ++ expData, userSchema)
    testRead(spark.read.schema(userSchema).json(Seq(dir, dir): _*), expData ++ expData, userSchema)
  }

  test("parquet - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).parquet(dir)
    val df = spark.read.parquet(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    intercept[AnalysisException] {
      testRead(spark.read.parquet(), Seq.empty, schema)
    }
    testRead(spark.read.parquet(dir), data, schema)
    testRead(spark.read.parquet(dir, dir), data ++ data, schema)
    testRead(spark.read.parquet(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.parquet).get, data, schema)

    // Reader, with user specified schema, data should be nulls as schema in file different
    // from user schema
    val expData = Seq[String](null, null, null)
    testRead(spark.read.schema(userSchema).parquet(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).parquet(dir), expData, userSchema)
    testRead(spark.read.schema(userSchema).parquet(dir, dir), expData ++ expData, userSchema)
    testRead(
      spark.read.schema(userSchema).parquet(Seq(dir, dir): _*), expData ++ expData, userSchema)
  }

  test("orc - API and behavior regarding schema") {
    withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "native") {
      // Writer
      spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).orc(dir)
      val df = spark.read.orc(dir)
      checkAnswer(df, spark.createDataset(data).toDF())
      val schema = df.schema

      // Reader, without user specified schema
      intercept[AnalysisException] {
        testRead(spark.read.orc(), Seq.empty, schema)
      }
      testRead(spark.read.orc(dir), data, schema)
      testRead(spark.read.orc(dir, dir), data ++ data, schema)
      testRead(spark.read.orc(Seq(dir, dir): _*), data ++ data, schema)
      // Test explicit calls to single arg method - SPARK-16009
      testRead(Option(dir).map(spark.read.orc).get, data, schema)

      // Reader, with user specified schema, data should be nulls as schema in file different
      // from user schema
      val expData = Seq[String](null, null, null)
      testRead(spark.read.schema(userSchema).orc(), Seq.empty, userSchema)
      testRead(spark.read.schema(userSchema).orc(dir), expData, userSchema)
      testRead(spark.read.schema(userSchema).orc(dir, dir), expData ++ expData, userSchema)
      testRead(
        spark.read.schema(userSchema).orc(Seq(dir, dir): _*), expData ++ expData, userSchema)
    }
  }

  test("column nullability and comment - write and then read") {
    withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "native") {
      Seq("json", "orc", "parquet", "csv").foreach { format =>
        val schema = StructType(
          StructField("cl1", IntegerType, nullable = false).withComment("test") ::
            StructField("cl2", IntegerType, nullable = true) ::
            StructField("cl3", IntegerType, nullable = true) :: Nil)
        val row = Row(3, null, 4)
        val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

        val tableName = "tab"
        withTable(tableName) {
          df.write.format(format).mode("overwrite").saveAsTable(tableName)
          // Verify the DDL command result: DESCRIBE TABLE
          checkAnswer(
            sql(s"desc $tableName").select("col_name", "comment").where($"comment" === "test"),
            Row("cl1", "test") :: Nil)
          // Verify the schema
          val expectedFields = schema.fields.map(f => f.copy(nullable = true))
          assert(spark.table(tableName).schema == schema.copy(fields = expectedFields))
        }
      }
    }
  }

  test("SPARK-17230: write out results of decimal calculation") {
    val df = spark.range(99, 101)
      .selectExpr("id", "cast(id as long) * cast('1.0' as decimal(38, 18)) as num")
    df.write.mode(SaveMode.Overwrite).parquet(dir)
    val df2 = spark.read.parquet(dir)
    checkAnswer(df2, df)
  }

  private def testRead(
      df: => DataFrame,
      expectedResult: Seq[String],
      expectedSchema: StructType): Unit = {
    checkAnswer(df, spark.createDataset(expectedResult).toDF())
    assert(df.schema === expectedSchema)
  }

  test("saveAsTable with mode Append should not fail if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Append).saveAsTable("same_name")
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable with mode Append should not fail if the table already exists " +
    "and a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        val format = spark.sessionState.conf.defaultDataSourceName
        sql(s"CREATE TABLE same_name(id LONG) USING $format")
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Append).saveAsTable("same_name")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        checkAnswer(spark.table("default.same_name"), spark.range(20).toDF())
      }
    }
  }

  test("saveAsTable with mode ErrorIfExists should not fail if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.ErrorIfExists).saveAsTable("same_name")
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable with mode Overwrite should not drop the temp view if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Overwrite).saveAsTable("same_name")
        assert(spark.sessionState.catalog.getTempView("same_name").isDefined)
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable with mode Overwrite should not fail if the table already exists " +
    "and a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        sql("CREATE TABLE same_name(id LONG) USING parquet")
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Overwrite).saveAsTable("same_name")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        checkAnswer(spark.table("default.same_name"), spark.range(20).toDF())
      }
    }
  }

  test("saveAsTable with mode Ignore should create the table if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Ignore).saveAsTable("same_name")
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("SPARK-18510: use user specified types for partition columns in file sources") {
    import org.apache.spark.sql.functions.udf
    withTempDir { src =>
      val createArray = udf { (length: Long) =>
        for (i <- 1 to length.toInt) yield i.toString
      }
      spark.range(4).select(createArray('id + 1) as 'ex, 'id, 'id % 4 as 'part).coalesce(1).write
        .partitionBy("part", "id")
        .mode("overwrite")
        .parquet(src.toString)
      // Specify a random ordering of the schema, partition column in the middle, etc.
      // Also let's say that the partition columns are Strings instead of Longs.
      // partition columns should go to the end
      val schema = new StructType()
        .add("id", StringType)
        .add("ex", ArrayType(StringType))
      val df = spark.read
        .schema(schema)
        .format("parquet")
        .load(src.toString)

      assert(df.schema.toList === List(
        StructField("ex", ArrayType(StringType)),
        StructField("part", IntegerType), // inferred partitionColumn dataType
        StructField("id", StringType))) // used user provided partitionColumn dataType

      checkAnswer(
        df,
        // notice how `part` is ordered before `id`
        Row(Array("1"), 0, "0") :: Row(Array("1", "2"), 1, "1") ::
          Row(Array("1", "2", "3"), 2, "2") :: Row(Array("1", "2", "3", "4"), 3, "3") :: Nil
      )
    }
  }

  test("SPARK-18899: append to a bucketed table using DataFrameWriter with mismatched bucketing") {
    withTable("t") {
      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.bucketBy(2, "i").saveAsTable("t")
      val e = intercept[AnalysisException] {
        Seq(3 -> "c").toDF("i", "j").write.bucketBy(3, "i").mode("append").saveAsTable("t")
      }
      assert(e.message.contains("Specified bucketing does not match that of the existing table"))
    }
  }

  test("SPARK-18912: number of columns mismatch for non-file-based data source table") {
    withTable("t") {
      sql("CREATE TABLE t USING org.apache.spark.sql.test.DefaultSource")

      val e = intercept[AnalysisException] {
        Seq(1 -> "a").toDF("a", "b").write
          .format("org.apache.spark.sql.test.DefaultSource")
          .mode("append").saveAsTable("t")
      }
      assert(e.message.contains("The column number of the existing table"))
    }
  }

  test("SPARK-18913: append to a table with special column names") {
    withTable("t") {
      Seq(1 -> "a").toDF("x.x", "y.y").write.saveAsTable("t")
      Seq(2 -> "b").toDF("x.x", "y.y").write.mode("append").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("SPARK-16848: table API throws an exception for user specified schema") {
    withTable("t") {
      val schema = StructType(StructField("a", StringType) :: Nil)
      val e = intercept[AnalysisException] {
        spark.read.schema(schema).table("t")
      }.getMessage
      assert(e.contains("User specified schema not supported with `table`"))
    }
  }

  test("SPARK-20431: Specify a schema by using a DDL-formatted string") {
    spark.createDataset(data).write.mode(SaveMode.Overwrite).text(dir)
    testRead(spark.read.schema(userSchemaString).text(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchemaString).text(dir), data, userSchema)
    testRead(spark.read.schema(userSchemaString).text(dir, dir), data ++ data, userSchema)
    testRead(spark.read.schema(userSchemaString).text(Seq(dir, dir): _*), data ++ data, userSchema)
  }

  test("SPARK-20460 Check name duplication in buckets") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        var errorMsg = intercept[AnalysisException] {
          Seq((1, 1)).toDF("col", c0).write.bucketBy(2, c0, c1).saveAsTable("t")
        }.getMessage
        assert(errorMsg.contains("Found duplicate column(s) in the bucket definition"))

        errorMsg = intercept[AnalysisException] {
          Seq((1, 1)).toDF("col", c0).write.bucketBy(2, "col").sortBy(c0, c1).saveAsTable("t")
        }.getMessage
        assert(errorMsg.contains("Found duplicate column(s) in the sort definition"))
      }
    }
  }

  test("SPARK-20460 Check name duplication in schema") {
    def checkWriteDataColumnDuplication(
        format: String, colName0: String, colName1: String, tempDir: File): Unit = {
      val errorMsg = intercept[AnalysisException] {
        Seq((1, 1)).toDF(colName0, colName1).write.format(format).mode("overwrite")
          .save(tempDir.getAbsolutePath)
      }.getMessage
      assert(errorMsg.contains("Found duplicate column(s) when inserting into"))
    }

    def checkReadUserSpecifiedDataColumnDuplication(
        df: DataFrame, format: String, colName0: String, colName1: String, tempDir: File): Unit = {
      val testDir = Utils.createTempDir(tempDir.getAbsolutePath)
      df.write.format(format).mode("overwrite").save(testDir.getAbsolutePath)
      val errorMsg = intercept[AnalysisException] {
        spark.read.format(format).schema(s"$colName0 INT, $colName1 INT")
          .load(testDir.getAbsolutePath)
      }.getMessage
      assert(errorMsg.contains("Found duplicate column(s) in the data schema:"))
    }

    def checkReadPartitionColumnDuplication(
        format: String, colName0: String, colName1: String, tempDir: File): Unit = {
      val testDir = Utils.createTempDir(tempDir.getAbsolutePath)
      Seq(1).toDF("col").write.format(format).mode("overwrite")
        .save(s"${testDir.getAbsolutePath}/$colName0=1/$colName1=1")
      val errorMsg = intercept[AnalysisException] {
        spark.read.format(format).load(testDir.getAbsolutePath)
      }.getMessage
      assert(errorMsg.contains("Found duplicate column(s) in the partition schema:"))
    }

    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        withTempDir { src =>
          // Check CSV format
          checkWriteDataColumnDuplication("csv", c0, c1, src)
          checkReadUserSpecifiedDataColumnDuplication(
            Seq((1, 1)).toDF("c0", "c1"), "csv", c0, c1, src)
          // If `inferSchema` is true, a CSV format is duplicate-safe (See SPARK-16896)
          var testDir = Utils.createTempDir(src.getAbsolutePath)
          Seq("a,a", "1,1").toDF().coalesce(1).write.mode("overwrite").text(testDir.getAbsolutePath)
          val df = spark.read.format("csv").option("inferSchema", true).option("header", true)
            .load(testDir.getAbsolutePath)
          checkAnswer(df, Row(1, 1))
          checkReadPartitionColumnDuplication("csv", c0, c1, src)

          // Check JSON format
          checkWriteDataColumnDuplication("json", c0, c1, src)
          checkReadUserSpecifiedDataColumnDuplication(
            Seq((1, 1)).toDF("c0", "c1"), "json", c0, c1, src)
          // Inferred schema cases
          testDir = Utils.createTempDir(src.getAbsolutePath)
          Seq(s"""{"$c0":3, "$c1":5}""").toDF().write.mode("overwrite")
            .text(testDir.getAbsolutePath)
          val errorMsg = intercept[AnalysisException] {
            spark.read.format("json").option("inferSchema", true).load(testDir.getAbsolutePath)
          }.getMessage
          assert(errorMsg.contains("Found duplicate column(s) in the data schema:"))
          checkReadPartitionColumnDuplication("json", c0, c1, src)

          // Check Parquet format
          checkWriteDataColumnDuplication("parquet", c0, c1, src)
          checkReadUserSpecifiedDataColumnDuplication(
            Seq((1, 1)).toDF("c0", "c1"), "parquet", c0, c1, src)
          checkReadPartitionColumnDuplication("parquet", c0, c1, src)
        }
      }
    }
  }

  test("Insert overwrite table command should output correct schema: basic") {
    withTable("tbl", "tbl2") {
      withView("view1") {
        val df = spark.range(10).toDF("id")
        df.write.format("parquet").saveAsTable("tbl")
        spark.sql("CREATE VIEW view1 AS SELECT id FROM tbl")
        spark.sql("CREATE TABLE tbl2(ID long) USING parquet")
        spark.sql("INSERT OVERWRITE TABLE tbl2 SELECT ID FROM view1")
        val identifier = TableIdentifier("tbl2")
        val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
        val expectedSchema = StructType(Seq(StructField("ID", LongType, true)))
        assert(spark.read.parquet(location).schema == expectedSchema)
        checkAnswer(spark.table("tbl2"), df)
      }
    }
  }

  test("Insert overwrite table command should output correct schema: complex") {
    withTable("tbl", "tbl2") {
      withView("view1") {
        val df = spark.range(10).map(x => (x, x.toInt, x.toInt)).toDF("col1", "col2", "col3")
        df.write.format("parquet").saveAsTable("tbl")
        spark.sql("CREATE VIEW view1 AS SELECT * FROM tbl")
        spark.sql("CREATE TABLE tbl2(COL1 long, COL2 int, COL3 int) USING parquet PARTITIONED " +
          "BY (COL2) CLUSTERED BY (COL3) INTO 3 BUCKETS")
        spark.sql("INSERT OVERWRITE TABLE tbl2 SELECT COL1, COL2, COL3 FROM view1")
        val identifier = TableIdentifier("tbl2")
        val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
        val expectedSchema = StructType(Seq(
          StructField("COL1", LongType, true),
          StructField("COL3", IntegerType, true),
          StructField("COL2", IntegerType, true)))
        assert(spark.read.parquet(location).schema == expectedSchema)
        checkAnswer(spark.table("tbl2"), df)
      }
    }
  }

  test("Create table as select command should output correct schema: basic") {
    withTable("tbl", "tbl2") {
      withView("view1") {
        val df = spark.range(10).toDF("id")
        df.write.format("parquet").saveAsTable("tbl")
        spark.sql("CREATE VIEW view1 AS SELECT id FROM tbl")
        spark.sql("CREATE TABLE tbl2 USING parquet AS SELECT ID FROM view1")
        val identifier = TableIdentifier("tbl2")
        val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
        val expectedSchema = StructType(Seq(StructField("ID", LongType, true)))
        assert(spark.read.parquet(location).schema == expectedSchema)
        checkAnswer(spark.table("tbl2"), df)
      }
    }
  }

  test("Create table as select command should output correct schema: complex") {
    withTable("tbl", "tbl2") {
      withView("view1") {
        val df = spark.range(10).map(x => (x, x.toInt, x.toInt)).toDF("col1", "col2", "col3")
        df.write.format("parquet").saveAsTable("tbl")
        spark.sql("CREATE VIEW view1 AS SELECT * FROM tbl")
        spark.sql("CREATE TABLE tbl2 USING parquet PARTITIONED BY (COL2) " +
          "CLUSTERED BY (COL3) INTO 3 BUCKETS AS SELECT COL1, COL2, COL3 FROM view1")
        val identifier = TableIdentifier("tbl2")
        val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
        val expectedSchema = StructType(Seq(
          StructField("COL1", LongType, true),
          StructField("COL3", IntegerType, true),
          StructField("COL2", IntegerType, true)))
        assert(spark.read.parquet(location).schema == expectedSchema)
        checkAnswer(spark.table("tbl2"), df)
      }
    }
  }

  test("use Spark jobs to list files") {
    withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "1") {
      withTempDir { dir =>
        val jobDescriptions = new ConcurrentLinkedQueue[String]()
        val jobListener = new SparkListener {
          override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
            jobDescriptions.add(jobStart.properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
          }
        }
        sparkContext.addSparkListener(jobListener)
        try {
          spark.range(0, 3).map(i => (i, i))
            .write.partitionBy("_1").mode("overwrite").parquet(dir.getCanonicalPath)
          // normal file paths
          checkDatasetUnorderly(
            spark.read.parquet(dir.getCanonicalPath).as[(Long, Long)],
            0L -> 0L, 1L -> 1L, 2L -> 2L)
          sparkContext.listenerBus.waitUntilEmpty(10000)
          assert(jobDescriptions.asScala.toList.exists(
            _.contains("Listing leaf files and directories for 3 paths")))
        } finally {
          sparkContext.removeSparkListener(jobListener)
        }
      }
    }
  }
}
