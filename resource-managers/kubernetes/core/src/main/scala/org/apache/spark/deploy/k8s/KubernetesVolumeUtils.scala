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

import java.util.NoSuchElementException

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._

private[spark] object KubernetesVolumeUtils {
  /**
   * Extract Spark volume configuration properties with a given name prefix.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing with volume name as key and spec as value
   */
  def parseVolumesWithPrefix(
    sparkConf: SparkConf,
    prefix: String): Iterable[Try[KubernetesVolumeSpec[_ <: KubernetesVolumeSpecificConf]]] = {
    val properties = sparkConf.getAllWithPrefix(prefix).toMap

    getVolumeTypesAndNames(properties).map { case (volumeType, volumeName) =>
      val pathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_PATH_KEY"
      val readOnlyKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_MOUNT_READONLY_KEY"

      for {
        path <- properties.getTry(pathKey)
        volumeConf <- parseVolumeSpecificConf(properties, volumeType, volumeName)
      } yield KubernetesVolumeSpec(
        volumeName = volumeName,
        mountPath = path,
        mountReadOnly = properties.get(readOnlyKey).exists(_.toBoolean),
        volumeConf = volumeConf
      )
    }
  }

  /**
   * Get unique pairs of volumeType and volumeName,
   * assuming options are formatted in this way:
   * `volumeType`.`volumeName`.`property` = `value`
   * @param properties flat mapping of property names to values
   * @return Set[(volumeType, volumeName)]
   */
  private def getVolumeTypesAndNames(
    properties: Map[String, String]
  ): Set[(String, String)] = {
    properties.keys.flatMap { k =>
      k.split('.').toList match {
        case tpe :: name :: _ => Some((tpe, name))
        case _ => None
      }
    }.toSet
  }

  private def parseVolumeSpecificConf(
    options: Map[String, String],
    volumeType: String,
    volumeName: String): Try[KubernetesVolumeSpecificConf] = {
    volumeType match {
      case KUBERNETES_VOLUMES_HOSTPATH_TYPE =>
        val pathKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_PATH_KEY"
        for {
          path <- options.getTry(pathKey)
        } yield KubernetesHostPathVolumeConf(path)

      case KUBERNETES_VOLUMES_PVC_TYPE =>
        val claimNameKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY"
        for {
          claimName <- options.getTry(claimNameKey)
        } yield KubernetesPVCVolumeConf(claimName)

      case KUBERNETES_VOLUMES_EMPTYDIR_TYPE =>
        val mediumKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY"
        val sizeLimitKey = s"$volumeType.$volumeName.$KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY"
        Success(KubernetesEmptyDirVolumeConf(options.get(mediumKey), options.get(sizeLimitKey)))

      case _ =>
        Failure(new RuntimeException(s"Kubernetes Volume type `$volumeType` is not supported"))
    }
  }

  /**
   * Convenience wrapper to accumulate key lookup errors
   */
  implicit private class MapOps[A, B](m: Map[A, B]) {
    def getTry(key: A): Try[B] = {
      m
        .get(key)
        .fold[Try[B]](Failure(new NoSuchElementException(key.toString)))(Success(_))
    }
  }
}
