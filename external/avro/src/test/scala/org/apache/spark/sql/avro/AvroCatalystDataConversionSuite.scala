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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.types._

class AvroCatalystDataConversionSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def roundTripTest(data: Literal): Unit = {
    val avroType = SchemaConverters.toAvroType(data.dataType, data.nullable)
    checkResult(data, avroType.toString, data.eval())
  }

  private def checkResult(data: Literal, schema: String, expected: Any): Unit = {
    checkEvaluation(
      AvroDataToCatalyst(CatalystDataToAvro(data), schema),
      prepareExpectedResult(expected))
  }

  private def assertFail(data: Literal, schema: String): Unit = {
    intercept[java.io.EOFException] {
      AvroDataToCatalyst(CatalystDataToAvro(data), schema).eval()
    }
  }

  private val testingTypes = Seq(
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType(8, 0),   // 32 bits decimal without fraction
    DecimalType(8, 4),   // 32 bits decimal
    DecimalType(16, 0),  // 64 bits decimal without fraction
    DecimalType(16, 11), // 64 bits decimal
    DecimalType(38, 0),
    DecimalType(38, 38),
    StringType,
    BinaryType)

  protected def prepareExpectedResult(expected: Any): Any = expected match {
    // Spark byte and short both map to avro int
    case b: Byte => b.toInt
    case s: Short => s.toInt
    case row: GenericInternalRow => InternalRow.fromSeq(row.values.map(prepareExpectedResult))
    case array: GenericArrayData => new GenericArrayData(array.array.map(prepareExpectedResult))
    case map: MapData =>
      val keys = new GenericArrayData(
        map.keyArray().asInstanceOf[GenericArrayData].array.map(prepareExpectedResult))
      val values = new GenericArrayData(
        map.valueArray().asInstanceOf[GenericArrayData].array.map(prepareExpectedResult))
      new ArrayBasedMapData(keys, values)
    case other => other
  }

  testingTypes.foreach { dt =>
    val seed = scala.util.Random.nextLong()
    test(s"single $dt with seed $seed") {
      val rand = new scala.util.Random(seed)
      val data = RandomDataGenerator.forType(dt, rand = rand).get.apply()
      val converter = CatalystTypeConverters.createToCatalystConverter(dt)
      val input = Literal.create(converter(data), dt)
      roundTripTest(input)
    }
  }

  for (_ <- 1 to 5) {
    val seed = scala.util.Random.nextLong()
    val rand = new scala.util.Random(seed)
    val schema = RandomDataGenerator.randomSchema(rand, 5, testingTypes)
    test(s"flat schema ${schema.catalogString} with seed $seed") {
      val data = RandomDataGenerator.randomRow(rand, schema)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      val input = Literal.create(converter(data), schema)
      roundTripTest(input)
    }
  }

  for (_ <- 1 to 5) {
    val seed = scala.util.Random.nextLong()
    val rand = new scala.util.Random(seed)
    val schema = RandomDataGenerator.randomNestedSchema(rand, 10, testingTypes)
    test(s"nested schema ${schema.catalogString} with seed $seed") {
      val data = RandomDataGenerator.randomRow(rand, schema)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      val input = Literal.create(converter(data), schema)
      roundTripTest(input)
    }
  }

  test("read int as string") {
    val data = Literal(1)
    val avroTypeJson =
      s"""
         |{
         |  "type": "string",
         |  "name": "my_string"
         |}
       """.stripMargin

    // When read int as string, avro reader is not able to parse the binary and fail.
    assertFail(data, avroTypeJson)
  }

  test("read string as int") {
    val data = Literal("abc")
    val avroTypeJson =
      s"""
         |{
         |  "type": "int",
         |  "name": "my_int"
         |}
       """.stripMargin

    // When read string data as int, avro reader is not able to find the type mismatch and read
    // the string length as int value.
    checkResult(data, avroTypeJson, 3)
  }

  test("read float as double") {
    val data = Literal(1.23f)
    val avroTypeJson =
      s"""
         |{
         |  "type": "double",
         |  "name": "my_double"
         |}
       """.stripMargin

    // When read float data as double, avro reader fails(trying to read 8 bytes while the data have
    // only 4 bytes).
    assertFail(data, avroTypeJson)
  }

  test("read double as float") {
    val data = Literal(1.23)
    val avroTypeJson =
      s"""
         |{
         |  "type": "float",
         |  "name": "my_float"
         |}
       """.stripMargin

    // avro reader reads the first 4 bytes of a double as a float, the result is totally undefined.
    checkResult(data, avroTypeJson, 5.848603E35f)
  }
}
