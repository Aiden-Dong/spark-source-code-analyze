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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._

import org.apache.spark.SparkException
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.config.{EXECUTOR_CLASS_PATH, EXECUTOR_JAVA_OPTIONS, EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD, PYSPARK_EXECUTOR_MEMORY}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

private[spark] class BasicExecutorFeatureStep(
    kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf])
  extends KubernetesFeatureConfigStep {

  // Consider moving some of these fields to KubernetesConf or KubernetesExecutorSpecificConf
  private val executorExtraClasspath = kubernetesConf.get(EXECUTOR_CLASS_PATH)
  private val executorContainerImage = kubernetesConf
    .get(EXECUTOR_CONTAINER_IMAGE)
    .getOrElse(throw new SparkException("Must specify the executor container image"))
  private val blockManagerPort = kubernetesConf
    .sparkConf
    .getInt("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT)

  private val executorPodNamePrefix = kubernetesConf.appResourceNamePrefix

  private val driverUrl = RpcEndpointAddress(
    kubernetesConf.get("spark.driver.host"),
    kubernetesConf.sparkConf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT),
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
  private val executorMemoryMiB = kubernetesConf.get(EXECUTOR_MEMORY)
  private val executorMemoryString = kubernetesConf.get(
    EXECUTOR_MEMORY.key, EXECUTOR_MEMORY.defaultValueString)

  private val memoryOverheadMiB = kubernetesConf
    .get(EXECUTOR_MEMORY_OVERHEAD)
    .getOrElse(math.max(
      (kubernetesConf.get(MEMORY_OVERHEAD_FACTOR) * executorMemoryMiB).toInt,
      MEMORY_OVERHEAD_MIN_MIB))
  private val executorMemoryWithOverhead = executorMemoryMiB + memoryOverheadMiB
  private val executorMemoryTotal = kubernetesConf.sparkConf
    .getOption(APP_RESOURCE_TYPE.key).map{ res =>
      val additionalPySparkMemory = res match {
        case "python" =>
          kubernetesConf.sparkConf
            .get(PYSPARK_EXECUTOR_MEMORY).map(_.toInt).getOrElse(0)
        case _ => 0
      }
    executorMemoryWithOverhead + additionalPySparkMemory
  }.getOrElse(executorMemoryWithOverhead)

  private val executorCores = kubernetesConf.sparkConf.getInt("spark.executor.cores", 1)
  private val executorCoresRequest =
    if (kubernetesConf.sparkConf.contains(KUBERNETES_EXECUTOR_REQUEST_CORES)) {
      kubernetesConf.get(KUBERNETES_EXECUTOR_REQUEST_CORES).get
    } else {
      executorCores.toString
    }
  private val executorLimitCores = kubernetesConf.get(KUBERNETES_EXECUTOR_LIMIT_CORES)

  override def configurePod(pod: SparkPod): SparkPod = {
    val name = s"$executorPodNamePrefix-exec-${kubernetesConf.roleSpecificConf.executorId}"

    // hostname must be no longer than 63 characters, so take the last 63 characters of the pod
    // name as the hostname.  This preserves uniqueness since the end of name contains
    // executorId
    val hostname = name.substring(Math.max(0, name.length - 63))
    val executorMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryTotal}Mi")
      .build()
    val executorCpuQuantity = new QuantityBuilder(false)
      .withAmount(executorCoresRequest)
      .build()
    val executorExtraClasspathEnv = executorExtraClasspath.map { cp =>
      new EnvVarBuilder()
        .withName(ENV_CLASSPATH)
        .withValue(cp)
        .build()
    }
    val executorExtraJavaOptionsEnv = kubernetesConf
      .get(EXECUTOR_JAVA_OPTIONS)
      .map { opts =>
        val subsOpts = Utils.substituteAppNExecIds(opts, kubernetesConf.appId,
          kubernetesConf.roleSpecificConf.executorId)
        val delimitedOpts = Utils.splitCommandString(subsOpts)
        delimitedOpts.zipWithIndex.map {
          case (opt, index) =>
            new EnvVarBuilder().withName(s"$ENV_JAVA_OPT_PREFIX$index").withValue(opt).build()
        }
      }.getOrElse(Seq.empty[EnvVar])
    val executorEnv = (Seq(
      (ENV_DRIVER_URL, driverUrl),
      (ENV_EXECUTOR_CORES, executorCores.toString),
      (ENV_EXECUTOR_MEMORY, executorMemoryString),
      (ENV_APPLICATION_ID, kubernetesConf.appId),
      // This is to set the SPARK_CONF_DIR to be /opt/spark/conf
      (ENV_SPARK_CONF_DIR, SPARK_CONF_DIR_INTERNAL),
      (ENV_EXECUTOR_ID, kubernetesConf.roleSpecificConf.executorId)) ++
      kubernetesConf.roleEnvs)
      .map(env => new EnvVarBuilder()
        .withName(env._1)
        .withValue(env._2)
        .build()
      ) ++ Seq(
      new EnvVarBuilder()
        .withName(ENV_EXECUTOR_POD_IP)
        .withValueFrom(new EnvVarSourceBuilder()
          .withNewFieldRef("v1", "status.podIP")
          .build())
        .build()
    ) ++ executorExtraJavaOptionsEnv ++ executorExtraClasspathEnv.toSeq
    val requiredPorts = Seq(
      (BLOCK_MANAGER_PORT_NAME, blockManagerPort))
      .map { case (name, port) =>
        new ContainerPortBuilder()
          .withName(name)
          .withContainerPort(port)
          .build()
      }

    val executorContainer = new ContainerBuilder(pod.container)
      .withName("executor")
      .withImage(executorContainerImage)
      .withImagePullPolicy(kubernetesConf.imagePullPolicy())
      .withNewResources()
        .addToRequests("memory", executorMemoryQuantity)
        .addToLimits("memory", executorMemoryQuantity)
        .addToRequests("cpu", executorCpuQuantity)
        .endResources()
      .addAllToEnv(executorEnv.asJava)
      .withPorts(requiredPorts.asJava)
      .addToArgs("executor")
      .build()
    val containerWithLimitCores = executorLimitCores.map { limitCores =>
      val executorCpuLimitQuantity = new QuantityBuilder(false)
        .withAmount(limitCores)
        .build()
      new ContainerBuilder(executorContainer)
        .editResources()
          .addToLimits("cpu", executorCpuLimitQuantity)
          .endResources()
        .build()
    }.getOrElse(executorContainer)
    val driverPod = kubernetesConf.roleSpecificConf.driverPod
    val ownerReference = driverPod.map(pod =>
      new OwnerReferenceBuilder()
        .withController(true)
        .withApiVersion(pod.getApiVersion)
        .withKind(pod.getKind)
        .withName(pod.getMetadata.getName)
        .withUid(pod.getMetadata.getUid)
        .build())
    val executorPod = new PodBuilder(pod.pod)
      .editOrNewMetadata()
        .withName(name)
        .withLabels(kubernetesConf.roleLabels.asJava)
        .withAnnotations(kubernetesConf.roleAnnotations.asJava)
        .addToOwnerReferences(ownerReference.toSeq: _*)
        .endMetadata()
      .editOrNewSpec()
        .withHostname(hostname)
        .withRestartPolicy("Never")
        .withNodeSelector(kubernetesConf.nodeSelector().asJava)
        .addToImagePullSecrets(kubernetesConf.imagePullSecrets(): _*)
        .endSpec()
      .build()

    SparkPod(executorPod, containerWithLimitCores)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
