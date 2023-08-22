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
package org.apache.spark.deploy.k8s

import org.apache.spark.{SparkConf, SparkFunSuite}

class KubernetesVolumeUtilsSuite extends SparkFunSuite {
  test("Parses hostPath volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.hostPath.volumeName.mount.path", "/path")
    sparkConf.set("test.hostPath.volumeName.mount.readOnly", "true")
    sparkConf.set("test.hostPath.volumeName.options.path", "/hostPath")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head.get
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly === true)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesHostPathVolumeConf] ===
      KubernetesHostPathVolumeConf("/hostPath"))
  }

  test("Parses persistentVolumeClaim volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.path", "/path")
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.readOnly", "true")
    sparkConf.set("test.persistentVolumeClaim.volumeName.options.claimName", "claimeName")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head.get
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly === true)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesPVCVolumeConf] ===
      KubernetesPVCVolumeConf("claimeName"))
  }

  test("Parses emptyDir volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.emptyDir.volumeName.mount.path", "/path")
    sparkConf.set("test.emptyDir.volumeName.mount.readOnly", "true")
    sparkConf.set("test.emptyDir.volumeName.options.medium", "medium")
    sparkConf.set("test.emptyDir.volumeName.options.sizeLimit", "5G")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head.get
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly === true)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesEmptyDirVolumeConf] ===
      KubernetesEmptyDirVolumeConf(Some("medium"), Some("5G")))
  }

  test("Parses emptyDir volume options can be optional") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.emptyDir.volumeName.mount.path", "/path")
    sparkConf.set("test.emptyDir.volumeName.mount.readOnly", "true")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head.get
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly === true)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesEmptyDirVolumeConf] ===
      KubernetesEmptyDirVolumeConf(None, None))
  }

  test("Defaults optional readOnly to false") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.hostPath.volumeName.mount.path", "/path")
    sparkConf.set("test.hostPath.volumeName.options.path", "/hostPath")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head.get
    assert(volumeSpec.mountReadOnly === false)
  }

  test("Gracefully fails on missing mount key") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.emptyDir.volumeName.mnt.path", "/path")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.isFailure === true)
    assert(volumeSpec.failed.get.getMessage === "emptyDir.volumeName.mount.path")
  }

  test("Gracefully fails on missing option key") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.hostPath.volumeName.mount.path", "/path")
    sparkConf.set("test.hostPath.volumeName.mount.readOnly", "true")
    sparkConf.set("test.hostPath.volumeName.options.pth", "/hostPath")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.isFailure === true)
    assert(volumeSpec.failed.get.getMessage === "hostPath.volumeName.options.path")
  }
}
