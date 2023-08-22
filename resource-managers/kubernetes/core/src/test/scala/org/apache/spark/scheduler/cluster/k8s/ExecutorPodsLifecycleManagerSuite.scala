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
package org.apache.spark.scheduler.cluster.k8s

import com.google.common.cache.CacheBuilder
import io.fabric8.kubernetes.api.model.{DoneablePod, Pod}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.scheduler.ExecutorExited
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsLifecycleManagerSuite extends SparkFunSuite with BeforeAndAfter {

  private var namedExecutorPods: mutable.Map[String, PodResource[Pod, DoneablePod]] = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var executorBuilder: KubernetesExecutorBuilder = _

  @Mock
  private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _
  private var eventHandlerUnderTest: ExecutorPodsLifecycleManager = _

  before {
    MockitoAnnotations.initMocks(this)
    val removedExecutorsCache = CacheBuilder.newBuilder().build[java.lang.Long, java.lang.Long]
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    namedExecutorPods = mutable.Map.empty[String, PodResource[Pod, DoneablePod]]
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq.empty[String])
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(any(classOf[String]))).thenAnswer(namedPodsAnswer())
    eventHandlerUnderTest = new ExecutorPodsLifecycleManager(
      new SparkConf(),
      executorBuilder,
      kubernetesClient,
      snapshotsStore,
      removedExecutorsCache)
    eventHandlerUnderTest.start(schedulerBackend)
  }

  test("When an executor reaches error states immediately, remove from the scheduler backend.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
    verify(namedExecutorPods(failedPod.getMetadata.getName)).delete()
  }

  test("Don't remove executors twice from Spark but remove from K8s repeatedly.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend, times(1)).doRemoveExecutor("1", expectedLossReason)
    verify(namedExecutorPods(failedPod.getMetadata.getName), times(2)).delete()
  }

  test("When the scheduler backend lists executor ids that aren't present in the cluster," +
    " remove those executors from Spark.") {
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq("1"))
    val msg = s"The executor with ID 1 was not found in the cluster but we didn't" +
      s" get a reason why. Marking the executor as failed. The executor may have been" +
      s" deleted but the driver missed the deletion event."
    val expectedLossReason = ExecutorExited(-1, exitCausedByApp = false, msg)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
  }

  private def exitReasonMessage(failedExecutorId: Int, failedPod: Pod): String = {
    s"""
       |The executor with id $failedExecutorId exited with exit code 1.
       |The API gave the following brief reason: ${failedPod.getStatus.getReason}
       |The API gave the following message: ${failedPod.getStatus.getMessage}
       |The API gave the following container statuses:
       |
       |${failedPod.getStatus.getContainerStatuses.asScala.map(_.toString).mkString("\n===\n")}
      """.stripMargin
  }

  private def namedPodsAnswer(): Answer[PodResource[Pod, DoneablePod]] = {
    new Answer[PodResource[Pod, DoneablePod]] {
      override def answer(invocation: InvocationOnMock): PodResource[Pod, DoneablePod] = {
        val podName = invocation.getArgumentAt(0, classOf[String])
        namedExecutorPods.getOrElseUpdate(
          podName, mock(classOf[PodResource[Pod, DoneablePod]]))
      }
    }
  }
}
