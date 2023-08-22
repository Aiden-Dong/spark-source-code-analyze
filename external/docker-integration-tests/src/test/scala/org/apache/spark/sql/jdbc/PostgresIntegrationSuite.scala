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

package org.apache.spark.sql.jdbc

import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{ArrayType, DecimalType, FloatType, ShortType}
import org.apache.spark.tags.DockerTest

@DockerTest
class PostgresIntegrationSuite extends DockerJDBCIntegrationSuite {
  override val db = new DatabaseOnDocker {
    override val imageName = "postgres:9.4.5"
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=postgres&password=rootpass"
    override def getStartupProcessName: Option[String] = None
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.setCatalog("foo")
    conn.prepareStatement("CREATE TYPE enum_type AS ENUM ('d1', 'd2')").executeUpdate()
    conn.prepareStatement("CREATE TABLE bar (c0 text, c1 integer, c2 double precision, c3 bigint, "
      + "c4 bit(1), c5 bit(10), c6 bytea, c7 boolean, c8 inet, c9 cidr, "
      + "c10 integer[], c11 text[], c12 real[], c13 numeric(2,2)[], c14 enum_type, "
      + "c15 float4, c16 smallint)").executeUpdate()
    conn.prepareStatement("INSERT INTO bar VALUES ('hello', 42, 1.25, 123456789012345, B'0', "
      + "B'1000100101', E'\\\\xDEADBEEF', true, '172.16.0.42', '192.168.0.0/16', "
      + """'{1, 2}', '{"a", null, "b"}', '{0.11, 0.22}', '{0.11, 0.22}', 'd1', 1.01, 1)"""
    ).executeUpdate()
    conn.prepareStatement("INSERT INTO bar VALUES (null, null, null, null, null, "
      + "null, null, null, null, null, "
      + "null, null, null, null, null, null, null)"
    ).executeUpdate()

    conn.prepareStatement("CREATE TABLE ts_with_timezone " +
      "(id integer, tstz TIMESTAMP WITH TIME ZONE, ttz TIME WITH TIME ZONE)")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO ts_with_timezone VALUES " +
      "(1, TIMESTAMP WITH TIME ZONE '2016-08-12 10:22:31.949271-07', " +
      "TIME WITH TIME ZONE '17:22:31.949271+00')")
      .executeUpdate()

    conn.prepareStatement("CREATE TABLE st_with_array (c0 uuid, c1 inet, c2 cidr," +
      "c3 json, c4 jsonb, c5 uuid[], c6 inet[], c7 cidr[], c8 json[], c9 jsonb[])")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO st_with_array VALUES ( " +
      "'0a532531-cdf1-45e3-963d-5de90b6a30f1', '172.168.22.1', '192.168.100.128/25', " +
      """'{"a": "foo", "b": "bar"}', '{"a": 1, "b": 2}', """ +
      "ARRAY['7be8aaf8-650e-4dbb-8186-0a749840ecf2'," +
      "'205f9bfc-018c-4452-a605-609c0cfad228']::uuid[], ARRAY['172.16.0.41', " +
      "'172.16.0.42']::inet[], ARRAY['192.168.0.0/24', '10.1.0.0/16']::cidr[], " +
      """ARRAY['{"a": "foo", "b": "bar"}', '{"a": 1, "b": 2}']::json[], """ +
      """ARRAY['{"a": 1, "b": 2, "c": 3}']::jsonb[])"""
    )
      .executeUpdate()
  }

  test("Type mapping for various types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bar", new Properties)
    val rows = df.collect().sortBy(_.toString())
    assert(rows.length == 2)
    // Test the types, and values using the first row.
    val types = rows(0).toSeq.map(x => x.getClass)
    assert(types.length == 17)
    assert(classOf[String].isAssignableFrom(types(0)))
    assert(classOf[java.lang.Integer].isAssignableFrom(types(1)))
    assert(classOf[java.lang.Double].isAssignableFrom(types(2)))
    assert(classOf[java.lang.Long].isAssignableFrom(types(3)))
    assert(classOf[java.lang.Boolean].isAssignableFrom(types(4)))
    assert(classOf[Array[Byte]].isAssignableFrom(types(5)))
    assert(classOf[Array[Byte]].isAssignableFrom(types(6)))
    assert(classOf[java.lang.Boolean].isAssignableFrom(types(7)))
    assert(classOf[String].isAssignableFrom(types(8)))
    assert(classOf[String].isAssignableFrom(types(9)))
    assert(classOf[Seq[Int]].isAssignableFrom(types(10)))
    assert(classOf[Seq[String]].isAssignableFrom(types(11)))
    assert(classOf[Seq[Double]].isAssignableFrom(types(12)))
    assert(classOf[Seq[BigDecimal]].isAssignableFrom(types(13)))
    assert(classOf[String].isAssignableFrom(types(14)))
    assert(classOf[java.lang.Float].isAssignableFrom(types(15)))
    assert(classOf[java.lang.Short].isAssignableFrom(types(16)))
    assert(rows(0).getString(0).equals("hello"))
    assert(rows(0).getInt(1) == 42)
    assert(rows(0).getDouble(2) == 1.25)
    assert(rows(0).getLong(3) == 123456789012345L)
    assert(!rows(0).getBoolean(4))
    // BIT(10)'s come back as ASCII strings of ten ASCII 0's and 1's...
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](5),
      Array[Byte](49, 48, 48, 48, 49, 48, 48, 49, 48, 49)))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](6),
      Array[Byte](0xDE.toByte, 0xAD.toByte, 0xBE.toByte, 0xEF.toByte)))
    assert(rows(0).getBoolean(7))
    assert(rows(0).getString(8) == "172.16.0.42")
    assert(rows(0).getString(9) == "192.168.0.0/16")
    assert(rows(0).getSeq(10) == Seq(1, 2))
    assert(rows(0).getSeq(11) == Seq("a", null, "b"))
    assert(rows(0).getSeq(12).toSeq == Seq(0.11f, 0.22f))
    assert(rows(0).getSeq(13) == Seq("0.11", "0.22").map(BigDecimal(_).bigDecimal))
    assert(rows(0).getString(14) == "d1")
    assert(rows(0).getFloat(15) == 1.01f)
    assert(rows(0).getShort(16) == 1)

    // Test reading null values using the second row.
    assert(0.until(16).forall(rows(1).isNullAt(_)))
  }

  test("Basic write test") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bar", new Properties)
    // Test only that it doesn't crash.
    df.write.jdbc(jdbcUrl, "public.barcopy", new Properties)
    // Test that written numeric type has same DataType as input
    assert(sqlContext.read.jdbc(jdbcUrl, "public.barcopy", new Properties).schema(13).dataType ==
      ArrayType(DecimalType(2, 2), true))
    // Test write null values.
    df.select(df.queryExecution.analyzed.output.map { a =>
      Column(Literal.create(null, a.dataType)).as(a.name)
    }: _*).write.jdbc(jdbcUrl, "public.barcopy2", new Properties)
  }

  test("Creating a table with shorts and floats") {
    sqlContext.createDataFrame(Seq((1.0f, 1.toShort)))
      .write.jdbc(jdbcUrl, "shortfloat", new Properties)
    val schema = sqlContext.read.jdbc(jdbcUrl, "shortfloat", new Properties).schema
    assert(schema(0).dataType == FloatType)
    assert(schema(1).dataType == ShortType)
  }

  test("SPARK-20557: column type TIMESTAMP with TIME ZONE and TIME with TIME ZONE " +
    "should be recognized") {
    // When using JDBC to read the columns of TIMESTAMP with TIME ZONE and TIME with TIME ZONE
    // the actual types are java.sql.Types.TIMESTAMP and java.sql.Types.TIME
    val dfRead = sqlContext.read.jdbc(jdbcUrl, "ts_with_timezone", new Properties)
    val rows = dfRead.collect()
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types(1).equals("class java.sql.Timestamp"))
    assert(types(2).equals("class java.sql.Timestamp"))
  }

  test("SPARK-22291: Conversion error when transforming array types of " +
    "uuid, inet and cidr to StingType in PostgreSQL") {
    val df = sqlContext.read.jdbc(jdbcUrl, "st_with_array", new Properties)
    val rows = df.collect()
    assert(rows(0).getString(0) == "0a532531-cdf1-45e3-963d-5de90b6a30f1")
    assert(rows(0).getString(1) == "172.168.22.1")
    assert(rows(0).getString(2) == "192.168.100.128/25")
    assert(rows(0).getString(3) == "{\"a\": \"foo\", \"b\": \"bar\"}")
    assert(rows(0).getString(4) == "{\"a\": 1, \"b\": 2}")
    assert(rows(0).getSeq(5) == Seq("7be8aaf8-650e-4dbb-8186-0a749840ecf2",
      "205f9bfc-018c-4452-a605-609c0cfad228"))
    assert(rows(0).getSeq(6) == Seq("172.16.0.41", "172.16.0.42"))
    assert(rows(0).getSeq(7) == Seq("192.168.0.0/24", "10.1.0.0/16"))
    assert(rows(0).getSeq(8) == Seq("""{"a": "foo", "b": "bar"}""", """{"a": 1, "b": 2}"""))
    assert(rows(0).getSeq(9) == Seq("""{"a": 1, "b": 2, "c": 3}"""))
  }
}
