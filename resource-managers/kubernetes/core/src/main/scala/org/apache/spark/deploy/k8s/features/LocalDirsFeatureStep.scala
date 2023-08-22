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
package org.apache.spark.deploy.k8s.features

import java.nio.file.Paths
import java.util.UUID

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, VolumeBuilder, VolumeMountBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, KubernetesRoleSpecificConf, SparkPod}

private[spark] class LocalDirsFeatureStep(
    conf: KubernetesConf[_ <: KubernetesRoleSpecificConf],
    defaultLocalDir: String = s"/var/data/spark-${UUID.randomUUID}")
  extends KubernetesFeatureConfigStep {

  // Cannot use Utils.getConfiguredLocalDirs because that will default to the Java system
  // property - we want to instead default to mounting an emptydir volume that doesn't already
  // exist in the image.
  // We could make utils.getConfiguredLocalDirs opinionated about Kubernetes, as it is already
  // a bit opinionated about YARN and Mesos.
  private val resolvedLocalDirs = Option(conf.sparkConf.getenv("SPARK_LOCAL_DIRS"))
    .orElse(conf.getOption("spark.local.dir"))
    .getOrElse(defaultLocalDir)
    .split(",")

  override def configurePod(pod: SparkPod): SparkPod = {
    val localDirVolumes = resolvedLocalDirs
      .zipWithIndex
      .map { case (localDir, index) =>
        new VolumeBuilder()
          .withName(s"spark-local-dir-${index + 1}")
          .withNewEmptyDir()
          .endEmptyDir()
          .build()
      }
    val localDirVolumeMounts = localDirVolumes
      .zip(resolvedLocalDirs)
      .map { case (localDirVolume, localDirPath) =>
        new VolumeMountBuilder()
          .withName(localDirVolume.getName)
          .withMountPath(localDirPath)
          .build()
      }
    val podWithLocalDirVolumes = new PodBuilder(pod.pod)
      .editSpec()
        .addToVolumes(localDirVolumes: _*)
        .endSpec()
      .build()
    val containerWithLocalDirVolumeMounts = new ContainerBuilder(pod.container)
      .addNewEnv()
        .withName("SPARK_LOCAL_DIRS")
        .withValue(resolvedLocalDirs.mkString(","))
        .endEnv()
      .addToVolumeMounts(localDirVolumeMounts: _*)
      .build()
    SparkPod(podWithLocalDirVolumes, containerWithLocalDirVolumeMounts)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
