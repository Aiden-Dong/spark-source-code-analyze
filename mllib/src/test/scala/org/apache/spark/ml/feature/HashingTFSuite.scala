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

package org.apache.spark.ml.feature

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.feature.{HashingTF => MLlibHashingTF}
import org.apache.spark.sql.Row
import org.apache.spark.util.Utils

class HashingTFSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._
  import HashingTFSuite.murmur3FeatureIdx

  test("params") {
    ParamsSuite.checkParams(new HashingTF)
  }

  test("hashingTF") {
    val numFeatures = 100
    // Assume perfect hash when computing expected features.
    def idx: Any => Int = murmur3FeatureIdx(numFeatures)
    val data = Seq(
      ("a a b b c d".split(" ").toSeq,
        Vectors.sparse(numFeatures,
          Seq((idx("a"), 2.0), (idx("b"), 2.0), (idx("c"), 1.0), (idx("d"), 1.0))))
    )

    val df = data.toDF("words", "expected")
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(numFeatures)
    val output = hashingTF.transform(df)
    val attrGroup = AttributeGroup.fromStructField(output.schema("features"))
    require(attrGroup.numAttributes === Some(numFeatures))

    testTransformer[(Seq[String], Vector)](df, hashingTF, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("applying binary term freqs") {
    val df = Seq((0, "a a b c c c".split(" ").toSeq)).toDF("id", "words")
    val n = 100
    val hashingTF = new HashingTF()
        .setInputCol("words")
        .setOutputCol("features")
        .setNumFeatures(n)
        .setBinary(true)
    val output = hashingTF.transform(df)
    val features = output.select("features").first().getAs[Vector](0)
    def idx: Any => Int = murmur3FeatureIdx(n)  // Assume perfect hash on input features
    val expected = Vectors.sparse(n,
      Seq((idx("a"), 1.0), (idx("b"), 1.0), (idx("c"), 1.0)))
    assert(features ~== expected absTol 1e-14)
  }

  test("read/write") {
    val t = new HashingTF()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setNumFeatures(10)
    testDefaultReadWrite(t)
  }

}

object HashingTFSuite {

  private[feature] def murmur3FeatureIdx(numFeatures: Int)(term: Any): Int = {
    Utils.nonNegativeMod(MLlibHashingTF.murmur3Hash(term), numFeatures)
  }

}
