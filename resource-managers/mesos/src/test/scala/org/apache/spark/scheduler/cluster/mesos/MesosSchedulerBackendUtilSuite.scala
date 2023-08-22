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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.mesos.config

class MesosSchedulerBackendUtilSuite extends SparkFunSuite {

  test("ContainerInfo fails to parse invalid docker parameters") {
    val conf = new SparkConf()
    conf.set("spark.mesos.executor.docker.parameters", "a,b")
    conf.set("spark.mesos.executor.docker.image", "test")

    val containerInfo = MesosSchedulerBackendUtil.buildContainerInfo(
      conf)
    val params = containerInfo.getDocker.getParametersList

    assert(params.size() == 0)
  }

  test("ContainerInfo parses docker parameters") {
    val conf = new SparkConf()
    conf.set("spark.mesos.executor.docker.parameters", "a=1,b=2,c=3")
    conf.set("spark.mesos.executor.docker.image", "test")

    val containerInfo = MesosSchedulerBackendUtil.buildContainerInfo(
      conf)
    val params = containerInfo.getDocker.getParametersList
    assert(params.size() == 3)
    assert(params.get(0).getKey == "a")
    assert(params.get(0).getValue == "1")
    assert(params.get(1).getKey == "b")
    assert(params.get(1).getValue == "2")
    assert(params.get(2).getKey == "c")
    assert(params.get(2).getValue == "3")
  }
}
