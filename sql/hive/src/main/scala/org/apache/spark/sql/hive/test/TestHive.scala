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

package org.apache.spark.sql.hive.test

import java.io.File
import java.net.URI
import java.util.{Set => JavaSet}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, ExternalCatalogWithListener}
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.command.CacheTableCommand
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf, WithTestConf}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.util.{ShutdownHookManager, Utils}

// SPARK-3729: Test key required to check for initialization errors with config.
object TestHive
  extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set(SQLConf.CODEGEN_FALLBACK.key, "false")
        .set("spark.sql.hive.metastore.barrierPrefixes",
          "org.apache.spark.sql.hive.execution.PairSerDe")
        .set("spark.sql.warehouse.dir", TestHiveContext.makeWarehouseDir().toURI.getPath)
        // SPARK-8910
        .set("spark.ui.enabled", "false")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
        // Disable ConvertToLocalRelation for better test coverage. Test cases built on
        // LocalRelation will exercise the optimization rules better by disabling it as
        // this rule may potentially block testing of other optimization rules such as
        // ConstantPropagation etc.
        .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)))


case class TestHiveVersion(hiveClient: HiveClient)
  extends TestHiveContext(TestHive.sparkContext, hiveClient)


private[hive] class TestHiveExternalCatalog(
    conf: SparkConf,
    hadoopConf: Configuration,
    hiveClient: Option[HiveClient] = None)
  extends HiveExternalCatalog(conf, hadoopConf) with Logging {

  override lazy val client: HiveClient =
    hiveClient.getOrElse {
      HiveUtils.newClientForMetadata(conf, hadoopConf)
    }
}


private[hive] class TestHiveSharedState(
    sc: SparkContext,
    hiveClient: Option[HiveClient] = None)
  extends SharedState(sc) {

  override lazy val externalCatalog: ExternalCatalogWithListener = {
    new ExternalCatalogWithListener(new TestHiveExternalCatalog(
      sc.conf,
      sc.hadoopConfiguration,
      hiveClient))
  }
}


/**
 * A locally running test instance of Spark's Hive execution engine.
 *
 * Data from [[testTables]] will be automatically loaded whenever a query is run over those tables.
 * Calling [[reset]] will delete all tables and other state in the database, leaving the database
 * in a "clean" state.
 *
 * TestHive is singleton object version of this class because instantiating multiple copies of the
 * hive metastore seems to lead to weird non-deterministic failures.  Therefore, the execution of
 * test cases that rely on TestHive must be serialized.
 */
class TestHiveContext(
    @transient override val sparkSession: TestHiveSparkSession)
  extends SQLContext(sparkSession) {

  /**
   * If loadTestTables is false, no test tables are loaded. Note that this flag can only be true
   * when running in the JVM, i.e. it needs to be false when calling from Python.
   */
  def this(sc: SparkContext, loadTestTables: Boolean = true) {
    this(new TestHiveSparkSession(HiveUtils.withHiveExternalCatalog(sc), loadTestTables))
  }

  def this(sc: SparkContext, hiveClient: HiveClient) {
    this(new TestHiveSparkSession(HiveUtils.withHiveExternalCatalog(sc),
      hiveClient,
      loadTestTables = false))
  }

  override def newSession(): TestHiveContext = {
    new TestHiveContext(sparkSession.newSession())
  }

  def setCacheTables(c: Boolean): Unit = {
    sparkSession.setCacheTables(c)
  }

  def getHiveFile(path: String): File = {
    sparkSession.getHiveFile(path)
  }

  def loadTestTable(name: String): Unit = {
    sparkSession.loadTestTable(name)
  }

  def reset(): Unit = {
    sparkSession.reset()
  }

}

/**
 * A [[SparkSession]] used in [[TestHiveContext]].
 *
 * @param sc SparkContext
 * @param existingSharedState optional [[SharedState]]
 * @param parentSessionState optional parent [[SessionState]]
 * @param loadTestTables if true, load the test tables. They can only be loaded when running
 *                       in the JVM, i.e when calling from Python this flag has to be false.
 */
private[hive] class TestHiveSparkSession(
    @transient private val sc: SparkContext,
    @transient private val existingSharedState: Option[TestHiveSharedState],
    @transient private val parentSessionState: Option[SessionState],
    private val loadTestTables: Boolean)
  extends SparkSession(sc) with Logging { self =>

  def this(sc: SparkContext, loadTestTables: Boolean) {
    this(
      sc,
      existingSharedState = None,
      parentSessionState = None,
      loadTestTables)
  }

  def this(sc: SparkContext, hiveClient: HiveClient, loadTestTables: Boolean) {
    this(
      sc,
      existingSharedState = Some(new TestHiveSharedState(sc, Some(hiveClient))),
      parentSessionState = None,
      loadTestTables)
  }

  SparkSession.setDefaultSession(this)
  SparkSession.setActiveSession(this)

  { // set the metastore temporary configuration
    val metastoreTempConf = HiveUtils.newTemporaryConfiguration(useInMemoryDerby = false) ++ Map(
      ConfVars.METASTORE_INTEGER_JDO_PUSHDOWN.varname -> "true",
      // scratch directory used by Hive's metastore client
      ConfVars.SCRATCHDIR.varname -> TestHiveContext.makeScratchDir().toURI.toString,
      ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname -> "1") ++
      // After session cloning, the JDBC connect string for a JDBC metastore should not be changed.
      existingSharedState.map { state =>
        val connKey =
          state.sparkContext.hadoopConfiguration.get(ConfVars.METASTORECONNECTURLKEY.varname)
        ConfVars.METASTORECONNECTURLKEY.varname -> connKey
      }

    metastoreTempConf.foreach { case (k, v) =>
      sc.hadoopConfiguration.set(k, v)
    }
  }

  assume(sc.conf.get(CATALOG_IMPLEMENTATION) == "hive")

  @transient
  override lazy val sharedState: TestHiveSharedState = {
    existingSharedState.getOrElse(new TestHiveSharedState(sc))
  }

  @transient
  override lazy val sessionState: SessionState = {
    new TestHiveSessionStateBuilder(this, parentSessionState).build()
  }

  lazy val metadataHive: HiveClient = {
    sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client.newSession()
  }

  override def newSession(): TestHiveSparkSession = {
    new TestHiveSparkSession(sc, Some(sharedState), None, loadTestTables)
  }

  override def cloneSession(): SparkSession = {
    val result = new TestHiveSparkSession(
      sparkContext,
      Some(sharedState),
      Some(sessionState),
      loadTestTables)
    result.sessionState // force copy of SessionState
    result
  }

  private var cacheTables: Boolean = false

  def setCacheTables(c: Boolean): Unit = {
    cacheTables = c
  }

  // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
  // without restarting the JVM.
  System.clearProperty("spark.hostPort")

  // For some hive test case which contain ${system:test.tmp.dir}
  System.setProperty("test.tmp.dir", Utils.createTempDir().toURI.getPath)

  /** The location of the compiled hive distribution */
  lazy val hiveHome = envVarToFile("HIVE_HOME")

  /** The location of the hive source code. */
  lazy val hiveDevHome = envVarToFile("HIVE_DEV_HOME")

  /**
   * Returns the value of specified environmental variable as a [[java.io.File]] after checking
   * to ensure it exists
   */
  private def envVarToFile(envVar: String): Option[File] = {
    Option(System.getenv(envVar)).map(new File(_))
  }

  val hiveFilesTemp = File.createTempFile("catalystHiveFiles", "")
  hiveFilesTemp.delete()
  hiveFilesTemp.mkdir()
  ShutdownHookManager.registerShutdownDeleteDir(hiveFilesTemp)

  def getHiveFile(path: String): File = {
    new File(Thread.currentThread().getContextClassLoader.getResource(path).getFile)
  }

  private def quoteHiveFile(path : String) = if (Utils.isWindows) {
    getHiveFile(path).getPath.replace('\\', '/')
  } else {
    getHiveFile(path).getPath
  }

  def getWarehousePath(): String = {
    val tempConf = new SQLConf
    sc.conf.getAll.foreach { case (k, v) => tempConf.setConfString(k, v) }
    tempConf.warehousePath
  }

  val describedTable = "DESCRIBE (\\w+)".r

  case class TestTable(name: String, commands: (() => Unit)*)

  protected[hive] implicit class SqlCmd(sql: String) {
    def cmd: () => Unit = {
      () => new TestHiveQueryExecution(sql).hiveResultString(): Unit
    }
  }

  /**
   * A list of test tables and the DDL required to initialize them.  A test table is loaded on
   * demand when a query are run against it.
   */
  @transient
  lazy val testTables = new mutable.HashMap[String, TestTable]()

  def registerTestTable(testTable: TestTable): Unit = {
    testTables += (testTable.name -> testTable)
  }

  if (loadTestTables) {
    // The test tables that are defined in the Hive QTestUtil.
    // /itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
    // https://github.com/apache/hive/blob/branch-0.13/data/scripts/q_test_init.sql
    @transient
    val hiveQTestUtilTables: Seq[TestTable] = Seq(
      TestTable("src",
        "CREATE TABLE src (key INT, value STRING)".cmd,
        s"LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/kv1.txt")}' INTO TABLE src".cmd),
      TestTable("src1",
        "CREATE TABLE src1 (key INT, value STRING)".cmd,
        s"LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/kv3.txt")}' INTO TABLE src1".cmd),
      TestTable("srcpart", () => {
        "CREATE TABLE srcpart (key INT, value STRING) PARTITIONED BY (ds STRING, hr STRING)"
          .cmd.apply()
        for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- Seq("11", "12")) {
          s"""
             |LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/kv1.txt")}'
             |OVERWRITE INTO TABLE srcpart PARTITION (ds='$ds',hr='$hr')
          """.stripMargin.cmd.apply()
        }
      }),
      TestTable("srcpart1", () => {
        "CREATE TABLE srcpart1 (key INT, value STRING) PARTITIONED BY (ds STRING, hr INT)"
          .cmd.apply()
        for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- 11 to 12) {
          s"""
             |LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/kv1.txt")}'
             |OVERWRITE INTO TABLE srcpart1 PARTITION (ds='$ds',hr='$hr')
          """.stripMargin.cmd.apply()
        }
      }),
      TestTable("src_thrift", () => {
        import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer
        import org.apache.hadoop.mapred.{SequenceFileInputFormat, SequenceFileOutputFormat}
        import org.apache.thrift.protocol.TBinaryProtocol

        s"""
           |CREATE TABLE src_thrift(fake INT)
           |ROW FORMAT SERDE '${classOf[ThriftDeserializer].getName}'
           |WITH SERDEPROPERTIES(
           |  'serialization.class'='org.apache.spark.sql.hive.test.Complex',
           |  'serialization.format'='${classOf[TBinaryProtocol].getName}'
           |)
           |STORED AS
           |INPUTFORMAT '${classOf[SequenceFileInputFormat[_, _]].getName}'
           |OUTPUTFORMAT '${classOf[SequenceFileOutputFormat[_, _]].getName}'
        """.stripMargin.cmd.apply()

        s"""
           |LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/complex.seq")}'
           |INTO TABLE src_thrift
        """.stripMargin.cmd.apply()
      }),
      TestTable("serdeins",
        s"""CREATE TABLE serdeins (key INT, value STRING)
           |ROW FORMAT SERDE '${classOf[LazySimpleSerDe].getCanonicalName}'
           |WITH SERDEPROPERTIES ('field.delim'='\\t')
         """.stripMargin.cmd,
        "INSERT OVERWRITE TABLE serdeins SELECT * FROM src".cmd),
      TestTable("episodes",
        s"""CREATE TABLE episodes (title STRING, air_date STRING, doctor INT)
           |STORED AS avro
           |TBLPROPERTIES (
           |  'avro.schema.literal'='{
           |    "type": "record",
           |    "name": "episodes",
           |    "namespace": "testing.hive.avro.serde",
           |    "fields": [
           |      {
           |          "name": "title",
           |          "type": "string",
           |          "doc": "episode title"
           |      },
           |      {
           |          "name": "air_date",
           |          "type": "string",
           |          "doc": "initial date"
           |      },
           |      {
           |          "name": "doctor",
           |          "type": "int",
           |          "doc": "main actor playing the Doctor in episode"
           |      }
           |    ]
           |  }'
           |)
         """.stripMargin.cmd,
        s"""
           |LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/episodes.avro")}'
           |INTO TABLE episodes
         """.stripMargin.cmd
      ),
      // THIS TABLE IS NOT THE SAME AS THE HIVE TEST TABLE episodes_partitioned AS DYNAMIC
      // PARTITIONING IS NOT YET SUPPORTED
      TestTable("episodes_part",
        s"""CREATE TABLE episodes_part (title STRING, air_date STRING, doctor INT)
           |PARTITIONED BY (doctor_pt INT)
           |STORED AS avro
           |TBLPROPERTIES (
           |  'avro.schema.literal'='{
           |    "type": "record",
           |    "name": "episodes",
           |    "namespace": "testing.hive.avro.serde",
           |    "fields": [
           |      {
           |          "name": "title",
           |          "type": "string",
           |          "doc": "episode title"
           |      },
           |      {
           |          "name": "air_date",
           |          "type": "string",
           |          "doc": "initial date"
           |      },
           |      {
           |          "name": "doctor",
           |          "type": "int",
           |          "doc": "main actor playing the Doctor in episode"
           |      }
           |    ]
           |  }'
           |)
         """.stripMargin.cmd,
        // WORKAROUND: Required to pass schema to SerDe for partitioned tables.
        // TODO: Pass this automatically from the table to partitions.
        s"""
           |ALTER TABLE episodes_part SET SERDEPROPERTIES (
           |  'avro.schema.literal'='{
           |    "type": "record",
           |    "name": "episodes",
           |    "namespace": "testing.hive.avro.serde",
           |    "fields": [
           |      {
           |          "name": "title",
           |          "type": "string",
           |          "doc": "episode title"
           |      },
           |      {
           |          "name": "air_date",
           |          "type": "string",
           |          "doc": "initial date"
           |      },
           |      {
           |          "name": "doctor",
           |          "type": "int",
           |          "doc": "main actor playing the Doctor in episode"
           |      }
           |    ]
           |  }'
           |)
          """.stripMargin.cmd,
        s"""
          INSERT OVERWRITE TABLE episodes_part PARTITION (doctor_pt=1)
          SELECT title, air_date, doctor FROM episodes
        """.cmd
        ),
      TestTable("src_json",
        s"""CREATE TABLE src_json (json STRING) STORED AS TEXTFILE
         """.stripMargin.cmd,
        s"LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/json.txt")}' INTO TABLE src_json".cmd)
    )

    hiveQTestUtilTables.foreach(registerTestTable)
  }

  private val loadedTables = new collection.mutable.HashSet[String]

  def getLoadedTables: collection.mutable.HashSet[String] = loadedTables

  def loadTestTable(name: String) {
    if (!(loadedTables contains name)) {
      // Marks the table as loaded first to prevent infinite mutually recursive table loading.
      loadedTables += name
      logDebug(s"Loading test table $name")
      val createCmds =
        testTables.get(name).map(_.commands).getOrElse(sys.error(s"Unknown test table $name"))

      // test tables are loaded lazily, so they may be loaded in the middle a query execution which
      // has already set the execution id.
      if (sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY) == null) {
        // We don't actually have a `QueryExecution` here, use a fake one instead.
        SQLExecution.withNewExecutionId(this, new QueryExecution(this, OneRowRelation())) {
          createCmds.foreach(_())
        }
      } else {
        createCmds.foreach(_())
      }

      if (cacheTables) {
        new SQLContext(self).cacheTable(name)
      }
    }
  }

  /**
   * Records the UDFs present when the server starts, so we can delete ones that are created by
   * tests.
   */
  protected val originalUDFs: JavaSet[String] = FunctionRegistry.getFunctionNames

  /**
   * Resets the test instance by deleting any table, view, temp view, and UDF that have been created
   */
  def reset() {
    try {
      // HACK: Hive is too noisy by default.
      org.apache.log4j.LogManager.getCurrentLoggers.asScala.foreach { log =>
        val logger = log.asInstanceOf[org.apache.log4j.Logger]
        if (!logger.getName.contains("org.apache.spark")) {
          logger.setLevel(org.apache.log4j.Level.WARN)
        }
      }

      // Clean out the Hive warehouse between each suite
      val warehouseDir = new File(new URI(sparkContext.conf.get("spark.sql.warehouse.dir")).getPath)
      Utils.deleteRecursively(warehouseDir)
      warehouseDir.mkdir()

      sharedState.cacheManager.clearCache()
      loadedTables.clear()
      sessionState.catalog.reset()
      metadataHive.reset()

      // HDFS root scratch dir requires the write all (733) permission. For each connecting user,
      // an HDFS scratch dir: ${hive.exec.scratchdir}/<username> is created, with
      // ${hive.scratch.dir.permission}. To resolve the permission issue, the simplest way is to
      // delete it. Later, it will be re-created with the right permission.
      val hadoopConf = sessionState.newHadoopConf()
      val location = new Path(hadoopConf.get(ConfVars.SCRATCHDIR.varname))
      val fs = location.getFileSystem(hadoopConf)
      fs.delete(location, true)

      // Some tests corrupt this value on purpose, which breaks the RESET call below.
      sessionState.conf.setConfString("fs.defaultFS", new File(".").toURI.toString)
      // It is important that we RESET first as broken hooks that might have been set could break
      // other sql exec here.
      metadataHive.runSqlHive("RESET")
      // For some reason, RESET does not reset the following variables...
      // https://issues.apache.org/jira/browse/HIVE-9004
      metadataHive.runSqlHive("set hive.table.parameters.default=")
      // Lots of tests fail if we do not change the partition whitelist from the default.
      metadataHive.runSqlHive("set hive.metastore.partition.name.whitelist.pattern=.*")

      sessionState.catalog.setCurrentDatabase("default")
    } catch {
      case e: Exception =>
        logError("FATAL ERROR: Failed to reset TestDB state.", e)
    }
  }

}


private[hive] class TestHiveQueryExecution(
    sparkSession: TestHiveSparkSession,
    logicalPlan: LogicalPlan)
  extends QueryExecution(sparkSession, logicalPlan) with Logging {

  def this(sparkSession: TestHiveSparkSession, sql: String) {
    this(sparkSession, sparkSession.sessionState.sqlParser.parsePlan(sql))
  }

  def this(sql: String) {
    this(TestHive.sparkSession, sql)
  }

  override lazy val analyzed: LogicalPlan = {
    val describedTables = logical match {
      case CacheTableCommand(tbl, _, _) => tbl.table :: Nil
      case _ => Nil
    }

    // Make sure any test tables referenced are loaded.
    val referencedTables =
      describedTables ++
        logical.collect { case UnresolvedRelation(tableIdent) => tableIdent.table }
    val resolver = sparkSession.sessionState.conf.resolver
    val referencedTestTables = sparkSession.testTables.keys.filter { testTable =>
      referencedTables.exists(resolver(_, testTable))
    }
    logDebug(s"Query references test tables: ${referencedTestTables.mkString(", ")}")
    referencedTestTables.foreach(sparkSession.loadTestTable)
    // Proceed with analysis.
    sparkSession.sessionState.analyzer.executeAndCheck(logical)
  }
}


private[hive] object TestHiveContext {

  /**
   * A map used to store all confs that need to be overridden in sql/hive unit tests.
   */
  val overrideConfs: Map[String, String] =
    Map(
      // Fewer shuffle partitions to speed up testing.
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    )

  def makeWarehouseDir(): File = {
    val warehouseDir = Utils.createTempDir(namePrefix = "warehouse")
    warehouseDir.delete()
    warehouseDir
  }

  def makeScratchDir(): File = {
    val scratchDir = Utils.createTempDir(namePrefix = "scratch")
    scratchDir.delete()
    scratchDir
  }

}

private[sql] class TestHiveSessionStateBuilder(
    session: SparkSession,
    state: Option[SessionState])
  extends HiveSessionStateBuilder(session, state)
  with WithTestConf {

  override def overrideConfs: Map[String, String] = TestHiveContext.overrideConfs

  override def createQueryExecution: (LogicalPlan) => QueryExecution = { plan =>
    new TestHiveQueryExecution(session.asInstanceOf[TestHiveSparkSession], plan)
  }

  override protected def newBuilder: NewBuilder = new TestHiveSessionStateBuilder(_, _)
}
