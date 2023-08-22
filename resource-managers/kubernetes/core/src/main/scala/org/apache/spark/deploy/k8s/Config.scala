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

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigBuilder

private[spark] object Config extends Logging {

  val KUBERNETES_NAMESPACE =
    ConfigBuilder("spark.kubernetes.namespace")
      .doc("The namespace that will be used for running the driver and executor pods.")
      .stringConf
      .createWithDefault("default")

  val CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.container.image")
      .doc("Container image to use for Spark containers. Individual container types " +
        "(e.g. driver or executor) can also be configured to use different images if desired, " +
        "by setting the container type-specific image name.")
      .stringConf
      .createOptional

  val DRIVER_CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.driver.container.image")
      .doc("Container image to use for the driver.")
      .fallbackConf(CONTAINER_IMAGE)

  val EXECUTOR_CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.executor.container.image")
      .doc("Container image to use for the executors.")
      .fallbackConf(CONTAINER_IMAGE)

  val CONTAINER_IMAGE_PULL_POLICY =
    ConfigBuilder("spark.kubernetes.container.image.pullPolicy")
      .doc("Kubernetes image pull policy. Valid values are Always, Never, and IfNotPresent.")
      .stringConf
      .checkValues(Set("Always", "Never", "IfNotPresent"))
      .createWithDefault("IfNotPresent")

  val IMAGE_PULL_SECRETS =
    ConfigBuilder("spark.kubernetes.container.image.pullSecrets")
      .doc("Comma separated list of the Kubernetes secrets used " +
        "to access private image registries.")
      .stringConf
      .createOptional

  val KUBERNETES_AUTH_DRIVER_CONF_PREFIX =
      "spark.kubernetes.authenticate.driver"
  val KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX =
      "spark.kubernetes.authenticate.driver.mounted"
  val KUBERNETES_AUTH_CLIENT_MODE_PREFIX = "spark.kubernetes.authenticate"
  val OAUTH_TOKEN_CONF_SUFFIX = "oauthToken"
  val OAUTH_TOKEN_FILE_CONF_SUFFIX = "oauthTokenFile"
  val CLIENT_KEY_FILE_CONF_SUFFIX = "clientKeyFile"
  val CLIENT_CERT_FILE_CONF_SUFFIX = "clientCertFile"
  val CA_CERT_FILE_CONF_SUFFIX = "caCertFile"

  val KUBERNETES_SERVICE_ACCOUNT_NAME =
    ConfigBuilder(s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.serviceAccountName")
      .doc("Service account that is used when running the driver pod. The driver pod uses " +
        "this service account when requesting executor pods from the API server. If specific " +
        "credentials are given for the driver pod to use, the driver will favor " +
        "using those credentials instead.")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_LIMIT_CORES =
    ConfigBuilder("spark.kubernetes.driver.limit.cores")
      .doc("Specify the hard cpu limit for the driver pod")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_SUBMIT_CHECK =
    ConfigBuilder("spark.kubernetes.submitInDriver")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val KUBERNETES_EXECUTOR_LIMIT_CORES =
    ConfigBuilder("spark.kubernetes.executor.limit.cores")
      .doc("Specify the hard cpu limit for each executor pod")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_REQUEST_CORES =
    ConfigBuilder("spark.kubernetes.executor.request.cores")
      .doc("Specify the cpu request for each executor pod")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_POD_NAME =
    ConfigBuilder("spark.kubernetes.driver.pod.name")
      .doc("Name of the driver pod.")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_POD_NAME_PREFIX =
    ConfigBuilder("spark.kubernetes.executor.podNamePrefix")
      .doc("Prefix to use in front of the executor pod names.")
      .internal()
      .stringConf
      .createWithDefault("spark")

  val KUBERNETES_PYSPARK_PY_FILES =
    ConfigBuilder("spark.kubernetes.python.pyFiles")
      .doc("The PyFiles that are distributed via client arguments")
      .internal()
      .stringConf
      .createOptional

  val KUBERNETES_PYSPARK_MAIN_APP_RESOURCE =
    ConfigBuilder("spark.kubernetes.python.mainAppResource")
      .doc("The main app resource for pyspark jobs")
      .internal()
      .stringConf
      .createOptional

  val KUBERNETES_PYSPARK_APP_ARGS =
    ConfigBuilder("spark.kubernetes.python.appArgs")
      .doc("The app arguments for PySpark Jobs")
      .internal()
      .stringConf
      .createOptional

  val KUBERNETES_R_MAIN_APP_RESOURCE =
    ConfigBuilder("spark.kubernetes.r.mainAppResource")
      .doc("The main app resource for SparkR jobs")
      .internal()
      .stringConf
      .createOptional

  val KUBERNETES_R_APP_ARGS =
    ConfigBuilder("spark.kubernetes.r.appArgs")
      .doc("The app arguments for SparkR Jobs")
      .internal()
      .stringConf
      .createOptional

  val KUBERNETES_ALLOCATION_BATCH_SIZE =
    ConfigBuilder("spark.kubernetes.allocation.batch.size")
      .doc("Number of pods to launch at once in each round of executor allocation.")
      .intConf
      .checkValue(value => value > 0, "Allocation batch size should be a positive integer")
      .createWithDefault(5)

  val KUBERNETES_ALLOCATION_BATCH_DELAY =
    ConfigBuilder("spark.kubernetes.allocation.batch.delay")
      .doc("Time to wait between each round of executor allocation.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(value => value > 0, "Allocation batch delay must be a positive time value.")
      .createWithDefaultString("1s")

  val KUBERNETES_EXECUTOR_LOST_REASON_CHECK_MAX_ATTEMPTS =
    ConfigBuilder("spark.kubernetes.executor.lostCheck.maxAttempts")
      .doc("Maximum number of attempts allowed for checking the reason of an executor loss " +
        "before it is assumed that the executor failed.")
      .intConf
      .checkValue(value => value > 0, "Maximum attempts of checks of executor lost reason " +
        "must be a positive integer")
      .createWithDefault(10)

  val WAIT_FOR_APP_COMPLETION =
    ConfigBuilder("spark.kubernetes.submission.waitAppCompletion")
      .doc("In cluster mode, whether to wait for the application to finish before exiting the " +
        "launcher process.")
      .booleanConf
      .createWithDefault(true)

  val REPORT_INTERVAL =
    ConfigBuilder("spark.kubernetes.report.interval")
      .doc("Interval between reports of the current app status in cluster mode.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"Logging interval must be a positive time value.")
      .createWithDefaultString("1s")

  val KUBERNETES_EXECUTOR_API_POLLING_INTERVAL =
    ConfigBuilder("spark.kubernetes.executor.apiPollingInterval")
      .doc("Interval between polls against the Kubernetes API server to inspect the " +
        "state of executors.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"API server polling interval must be a" +
        " positive time value.")
      .createWithDefaultString("30s")

  val KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL =
    ConfigBuilder("spark.kubernetes.executor.eventProcessingInterval")
      .doc("Interval between successive inspection of executor events sent from the" +
        " Kubernetes API.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"Event processing interval must be a positive" +
        " time value.")
      .createWithDefaultString("1s")

  val MEMORY_OVERHEAD_FACTOR =
    ConfigBuilder("spark.kubernetes.memoryOverheadFactor")
      .doc("This sets the Memory Overhead Factor that will allocate memory to non-JVM jobs " +
        "which in the case of JVM tasks will default to 0.10 and 0.40 for non-JVM jobs")
      .doubleConf
      .checkValue(mem_overhead => mem_overhead >= 0 && mem_overhead < 1,
        "Ensure that memory overhead is a double between 0 --> 1.0")
      .createWithDefault(0.1)

  val PYSPARK_MAJOR_PYTHON_VERSION =
    ConfigBuilder("spark.kubernetes.pyspark.pythonVersion")
      .doc("This sets the major Python version. Either 2 or 3. (Python2 or Python3)")
      .stringConf
      .checkValue(pv => List("2", "3").contains(pv),
        "Ensure that major Python version is either Python2 or Python3")
      .createWithDefault("2")

  val APP_RESOURCE_TYPE =
    ConfigBuilder("spark.kubernetes.resource.type")
      .doc("This sets the resource type internally")
      .internal()
      .stringConf
      .createOptional

  val KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX =
    "spark.kubernetes.authenticate.submission"

  val KUBERNETES_NODE_SELECTOR_PREFIX = "spark.kubernetes.node.selector."

  val KUBERNETES_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label."
  val KUBERNETES_DRIVER_ANNOTATION_PREFIX = "spark.kubernetes.driver.annotation."
  val KUBERNETES_DRIVER_SECRETS_PREFIX = "spark.kubernetes.driver.secrets."
  val KUBERNETES_DRIVER_SECRET_KEY_REF_PREFIX = "spark.kubernetes.driver.secretKeyRef."
  val KUBERNETES_DRIVER_VOLUMES_PREFIX = "spark.kubernetes.driver.volumes."

  val KUBERNETES_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.executor.label."
  val KUBERNETES_EXECUTOR_ANNOTATION_PREFIX = "spark.kubernetes.executor.annotation."
  val KUBERNETES_EXECUTOR_SECRETS_PREFIX = "spark.kubernetes.executor.secrets."
  val KUBERNETES_EXECUTOR_SECRET_KEY_REF_PREFIX = "spark.kubernetes.executor.secretKeyRef."
  val KUBERNETES_EXECUTOR_VOLUMES_PREFIX = "spark.kubernetes.executor.volumes."

  val KUBERNETES_VOLUMES_HOSTPATH_TYPE = "hostPath"
  val KUBERNETES_VOLUMES_PVC_TYPE = "persistentVolumeClaim"
  val KUBERNETES_VOLUMES_EMPTYDIR_TYPE = "emptyDir"
  val KUBERNETES_VOLUMES_MOUNT_PATH_KEY = "mount.path"
  val KUBERNETES_VOLUMES_MOUNT_READONLY_KEY = "mount.readOnly"
  val KUBERNETES_VOLUMES_OPTIONS_PATH_KEY = "options.path"
  val KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY = "options.claimName"
  val KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY = "options.medium"
  val KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY = "options.sizeLimit"

  val KUBERNETES_DRIVER_ENV_PREFIX = "spark.kubernetes.driverEnv."
}
