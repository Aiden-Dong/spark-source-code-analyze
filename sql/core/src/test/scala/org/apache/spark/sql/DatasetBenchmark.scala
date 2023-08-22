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

package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Benchmark

/**
 * Benchmark for Dataset typed operations comparing with DataFrame and RDD versions.
 */
object DatasetBenchmark {

  case class Data(l: Long, s: String)

  def backToBackMapLong(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val rdd = spark.sparkContext.range(0, numRows)
    val ds = spark.range(0, numRows)
    val df = ds.toDF("l")
    val func = (l: Long) => l + 1

    val benchmark = new Benchmark("back-to-back map long", numRows)

    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.select($"l" + 1 as "l")
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = ds.as[Long]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackMap(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back map", numRows)
    val func = (d: Data) => Data(d.l + 1, d.s)

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.select($"l" + 1 as "l", $"s")
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = df.as[Data]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackFilterLong(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val rdd = spark.sparkContext.range(1, numRows)
    val ds = spark.range(1, numRows)
    val df = ds.toDF("l")
    val func = (l: Long) => l % 2L == 0L

    val benchmark = new Benchmark("back-to-back filter Long", numRows)

    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.filter(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.filter($"l" % 2L === 0L)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = ds.as[Long]
      var i = 0
      while (i < numChains) {
        res = res.filter(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackFilter(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back filter", numRows)
    val func = (d: Data, i: Int) => d.l % (100L + i) == 0L
    val funcs = 0.until(numChains).map { i =>
      (d: Data) => func(d, i)
    }

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.filter(funcs(i))
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.filter($"l" % (100L + i) === 0L)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = df.as[Data]
      var i = 0
      while (i < numChains) {
        res = res.filter(funcs(i))
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  object ComplexAggregator extends Aggregator[Data, Data, Long] {
    override def zero: Data = Data(0, "")

    override def reduce(b: Data, a: Data): Data = Data(b.l + a.l, "")

    override def finish(reduction: Data): Long = reduction.l

    override def merge(b1: Data, b2: Data): Data = Data(b1.l + b2.l, "")

    override def bufferEncoder: Encoder[Data] = Encoders.product[Data]

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  def aggregate(spark: SparkSession, numRows: Long): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("aggregate", numRows)

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD sum") { iter =>
      rdd.aggregate(0L)(_ + _.l, _ + _)
    }

    benchmark.addCase("DataFrame sum") { iter =>
      df.select(sum($"l")).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset sum using Aggregator") { iter =>
      df.as[Data].select(typed.sumLong((d: Data) => d.l)).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset complex Aggregator") { iter =>
      df.as[Data].select(ComplexAggregator.toColumn).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Dataset benchmark")
      .getOrCreate()

    val numRows = 100000000
    val numChains = 10

    val benchmark0 = backToBackMapLong(spark, numRows, numChains)
    val benchmark1 = backToBackMap(spark, numRows, numChains)
    val benchmark2 = backToBackFilterLong(spark, numRows, numChains)
    val benchmark3 = backToBackFilter(spark, numRows, numChains)
    val benchmark4 = aggregate(spark, numRows)

    /*
    OpenJDK 64-Bit Server VM 1.8.0_111-8u111-b14-2ubuntu0.16.04.2-b14 on Linux 4.4.0-47-generic
    Intel(R) Xeon(R) CPU E5-2667 v3 @ 3.20GHz
    back-to-back map long:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           1883 / 1892         53.1          18.8       1.0X
    DataFrame                                      502 /  642        199.1           5.0       3.7X
    Dataset                                        657 /  784        152.2           6.6       2.9X
    */
    benchmark0.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 3.10.0-327.18.2.el7.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    back-to-back map:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           3448 / 3646         29.0          34.5       1.0X
    DataFrame                                     2647 / 3116         37.8          26.5       1.3X
    Dataset                                       4781 / 5155         20.9          47.8       0.7X
    */
    benchmark1.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_121-8u121-b13-0ubuntu1.16.04.2-b13 on Linux 4.4.0-47-generic
    Intel(R) Xeon(R) CPU E5-2667 v3 @ 3.20GHz
    back-to-back filter Long:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                            846 / 1120        118.1           8.5       1.0X
    DataFrame                                      270 /  329        370.9           2.7       3.1X
    Dataset                                        545 /  789        183.5           5.4       1.6X
    */
    benchmark2.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 3.10.0-327.18.2.el7.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    back-to-back filter:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           1346 / 1618         74.3          13.5       1.0X
    DataFrame                                       59 /   72       1695.4           0.6      22.8X
    Dataset                                       2777 / 2805         36.0          27.8       0.5X
    */
    benchmark3.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.12.1
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    aggregate:                               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD sum                                       1913 / 1942         52.3          19.1       1.0X
    DataFrame sum                                   46 /   61       2157.7           0.5      41.3X
    Dataset sum using Aggregator                  4656 / 4758         21.5          46.6       0.4X
    Dataset complex Aggregator                    6636 / 7039         15.1          66.4       0.3X
    */
    benchmark4.run()
  }
}
