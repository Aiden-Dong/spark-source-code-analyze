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

package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkFunSuite

class CSVUtilsSuite extends SparkFunSuite {
  test("Can parse escaped characters") {
    assert(CSVUtils.toChar("""\t""") === '\t')
    assert(CSVUtils.toChar("""\r""") === '\r')
    assert(CSVUtils.toChar("""\b""") === '\b')
    assert(CSVUtils.toChar("""\f""") === '\f')
    assert(CSVUtils.toChar("""\"""") === '\"')
    assert(CSVUtils.toChar("""\'""") === '\'')
    assert(CSVUtils.toChar("""\u0000""") === '\u0000')
    assert(CSVUtils.toChar("""\\""") === '\\')
  }

  test("Does not accept delimiter larger than one character") {
    val exception = intercept[IllegalArgumentException]{
      CSVUtils.toChar("ab")
    }
    assert(exception.getMessage.contains("cannot be more than one character"))
  }

  test("Throws exception for unsupported escaped characters") {
    val exception = intercept[IllegalArgumentException]{
      CSVUtils.toChar("""\1""")
    }
    assert(exception.getMessage.contains("Unsupported special character for delimiter"))
  }

  test("string with one backward slash is prohibited") {
    val exception = intercept[IllegalArgumentException]{
      CSVUtils.toChar("""\""")
    }
    assert(exception.getMessage.contains("Single backslash is prohibited"))
  }

  test("output proper error message for empty string") {
    val exception = intercept[IllegalArgumentException]{
      CSVUtils.toChar("")
    }
    assert(exception.getMessage.contains("Delimiter cannot be empty string"))
  }
}
