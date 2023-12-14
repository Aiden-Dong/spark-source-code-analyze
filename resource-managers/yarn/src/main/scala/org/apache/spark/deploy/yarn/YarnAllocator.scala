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

package org.apache.spark.deploy.yarn

import java.util.Collections
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef}
import org.apache.spark.scheduler.{ExecutorExited, ExecutorLossReason}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveLastAllocatedExecutorId
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * YarnAllocator 负责向 YARN ResourceManager请求 Container 并在 YARN满足这些请求时决定如何处理这些Container。
 *
 * 该类使用 YARN 的 AMRMClient API。我们与AMRMClient互动的方式有三种：
 *   - 让RM知道我们的资源需求，这会更新关于已请求Container的本地记录。
 *   - 调用"allocate"，该调用会将本地容器请求与RM同步，并返回YARN授予我们的任何容器。这也充当心跳。
 *   - 处理授予我们的容器，可能在其中启动执行器。
 *
 * 该类的公共方法是线程安全的。所有更改状态的方法都是同步的。这意味着多个线程可以同时调用这些方法而不会引发竞态条件或数据不一致问题。
 */
private[yarn] class YarnAllocator(
    driverUrl: String,
    driverRef: RpcEndpointRef,
    conf: YarnConfiguration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    securityMgr: SecurityManager,
    localResources: Map[String, LocalResource],
    resolver: SparkRackResolver,
    clock: Clock = new SystemClock)
  extends Logging {

  import YarnAllocator._

  // Visible for testing.
  val allocatedHostToContainersMap = new HashMap[String, collection.mutable.Set[ContainerId]]
  val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  // 这些是我们不再关心的Container。我们要么已经告诉RM释放它们，要么在下一个心跳时会这样做。
  // 容器会在RM告诉我们它们已完成后从这个映射中移除。
  private val releasedContainers = Collections.newSetFromMap[ContainerId](new ConcurrentHashMap[ContainerId, java.lang.Boolean])

  private val runningExecutors = Collections.newSetFromMap[String](new ConcurrentHashMap[String, java.lang.Boolean]())

  private val numExecutorsStarting = new AtomicInteger(0)

  /**
   * 用于生成每个Executor的唯一ID
   * 初始化executorIdCounter。
   * 当AM重新启动时，executorIdCounter将重置为0。然后新执行器的ID将从1开始，这将与已经在之前创建的执行器冲突。
   * 因此，我们应该通过从驱动程序获取最大的executorId来初始化executorIdCounter。
   * 这种executorId冲突的情况只出现在yarn客户端模式中，因此这是yarn客户端模式中的一个问题。有关更多详细信息，请参见jira。
   * 参见SPARK-12864。
   */
  private var executorIdCounter: Int = driverRef.askSync[Int](RetrieveLastAllocatedExecutorId)

  private[spark] val failureTracker = new FailureTracker(sparkConf, clock)

  private val allocatorBlacklistTracker = new YarnAllocatorBlacklistTracker(sparkConf, amClient, failureTracker)

  // 初始目标Executor的数量 :
  // 如果开启动态执行      ->  spark.dynamicAllocation.initialExecutors
  // 如果没有开启动态执行  ->  spark.executor.instances
  @volatile private var targetNumExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(sparkConf)


  // 待处理的Executor丢失原因请求 - 从ExecutorID映射到查询，以了解给定Executor为何丢失，然后回应请求者的列表。
  // 这个数据结构用于跟踪需要等待进一步查询的执行器丢失原因。
  private val pendingLossReasonRequests = new HashMap[String, mutable.Buffer[RpcCallContext]]

  //
  // 维护已释放的Executor的Loss原因，当从AM-RM调用获取执行器丢失原因时，它将被添加，并在查询此丢失原因后被移除。
  // 这用于记录已释放执行器的丢失原因，以供后续查询和处理。
  private val releasedExecutorLossReasons = new HashMap[String, ExecutorLossReason]

  // 跟踪哪个Container正在运行哪个Executor，以便稍后移除执行器。
  // 这对于测试时可见。这个功能用于跟踪Container和Executor之间的关系，以便在需要时正确移除Executor.
  private[yarn] val executorIdToContainer = new HashMap[String, Container]

  private var numUnexpectedContainerRelease = 0L
  private val containerIdToExecutorId = new HashMap[ContainerId, String]

  // Executor 内存大小
  protected val executorMemory = sparkConf.get(EXECUTOR_MEMORY).toInt

  // Executor Overhead 内存大小.
  protected val memoryOverhead: Int = sparkConf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN)).toInt

  // pyspark work 内存大小
  protected val pysparkWorkerMemory: Int = if (sparkConf.get(IS_PYTHON_APP)) {
    sparkConf.get(PYSPARK_EXECUTOR_MEMORY).map(_.toInt).getOrElse(0)
  } else {
    0
  }
  // Number of cores per executor.
  protected val executorCores = sparkConf.get(EXECUTOR_CORES)

  // 设置每个Executor的资源大小
  private[yarn] val resource = Resource.newInstance(executorMemory + memoryOverhead + pysparkWorkerMemory, executorCores)

  private val launcherPool = ThreadUtils.newDaemonCachedThreadPool("ContainerLauncher", sparkConf.get(CONTAINER_LAUNCH_MAX_THREADS))

  // For testing
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)

  private val labelExpression = sparkConf.get(EXECUTOR_NODE_LABEL_EXPRESSION)

  // 一个用于存储首选主机名和可能运行在其上的任务编号的映射。
  private var hostToLocalTaskCounts: Map[String, Int] = Map.empty

  // Number of tasks that have locality preferences in active stages
  private[yarn] var numLocalityAwareTasks: Int = 0

  // A container placement strategy based on pending tasks' locality preference
  private[yarn] val containerPlacementStrategy =
    new LocalityPreferredContainerPlacementStrategy(sparkConf, conf, resource, resolver)

  // 获得当前正在运行的 Executor 的数量
  def getNumExecutorsRunning: Int = runningExecutors.size()

  // 获得当前已经释放的Container的数量
  def getNumReleasedContainers: Int = releasedContainers.size()

  // 获取当前失败的Executor 数量
  def getNumExecutorsFailed: Int = failureTracker.numFailedExecutors

  def isAllNodeBlacklisted: Boolean = allocatorBlacklistTracker.isAllNodeBlacklisted

  // 一系列尚未得到满足的待处理Container请求。
  // 这表示正在等待YARN ResourceManager提供的容器请求的列表。
  def getPendingAllocate: Seq[ContainerRequest] = getPendingAtLocation(ANY_HOST)

  def numContainersPendingAllocate: Int = synchronized {
    getPendingAllocate.size
  }

  // 指定位置尚未得到满足的待处理容器请求的序列。
  // 这表示正在等待YARN ResourceManager提供的特定位置的容器请求的列表。
  private def getPendingAtLocation(location: String): Seq[ContainerRequest] = {
    amClient.getMatchingRequests(RM_REQUEST_PRIORITY, location, resource).asScala
      .flatMap(_.asScala)
      .toSeq
  }

  /**
   * 从ResourceManager请求所需数量的执行器，以达到所需的总数。这表示请求足够多的执行器，以达到所需的总数。.
   * 如果请求的总数小于当前正在运行的执行器数量，不会执行任何执行器的终止操作。
   * @param requestedTotal total number of containers requested
   * @param localityAwareTasks number of locality aware tasks to be used as container placement hint
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @param nodeBlacklist blacklisted nodes, which is passed in to avoid allocating new containers
   *                      on them. It will be used to update the application master's blacklist.
   * @return Whether the new requested total is different than the old value.
   */
  def requestTotalExecutorsWithPreferredLocalities(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int],
      nodeBlacklist: Set[String]): Boolean = synchronized {
    this.numLocalityAwareTasks = localityAwareTasks
    this.hostToLocalTaskCounts = hostToLocalTaskCount

    if (requestedTotal != targetNumExecutors) {
      logInfo(s"Driver requested a total number of $requestedTotal executor(s).")
      targetNumExecutors = requestedTotal
      allocatorBlacklistTracker.setSchedulerBlacklistedNodes(nodeBlacklist)
      true
    } else {
      false
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
   */
  def killExecutor(executorId: String): Unit = synchronized {
    executorIdToContainer.get(executorId) match {
      case Some(container) if !releasedContainers.contains(container.getId) =>
        internalReleaseContainer(container)
        runningExecutors.remove(executorId)
      case _ => logWarning(s"Attempted to kill unknown executor $executorId!")
    }
  }

  /*************
   * 请求资源的方式如下：如果YARN给予我们所有请求的资源，我们将拥有与maxExecutors相等的容器数量。
   * 处理YARN授予给我们的任何容器，可能在这些容器中启动执行器。
   * 这个过程必须同步，因为该方法中读取的变量可能会被其他方法修改。
   * ******************
   */
  def allocateResources(): Unit = synchronized {
    updateResourceRequests()  // 更新资源申请

    val progressIndicator = 0.1f
    // 获取分配返回信息
    val allocateResponse = amClient.allocate(progressIndicator)

    val allocatedContainers = allocateResponse.getAllocatedContainers()
    allocatorBlacklistTracker.setNumClusterNodes(allocateResponse.getNumClusterNodes)

    if (allocatedContainers.size > 0) {
      logDebug(("Allocated containers: %d. Current executor count: %d. " +
        "Launching executor count: %d. Cluster resources: %s.")
        .format(
          allocatedContainers.size,
          runningExecutors.size,
          numExecutorsStarting.get,
          allocateResponse.getAvailableResources))

      handleAllocatedContainers(allocatedContainers.asScala)
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))
      processCompletedContainers(completedContainers.asScala)
      logDebug("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, runningExecutors.size))
    }
  }

  /**
   * 更新我们将与RM同步的容器请求集
   * 基于我们当前运行的执行器数量和目标执行器数量。
   */
  def updateResourceRequests(): Unit = {
    val pendingAllocate = getPendingAllocate   // 当前申请没有获得的分配容器数
    val numPendingAllocate = pendingAllocate.size

    // 当前未被满足的目标executor数量
    // 这块是动态可变的
    val missing = targetNumExecutors - numPendingAllocate - numExecutorsStarting.get - runningExecutors.size

    logDebug(s"Updating resource requests, target: $targetNumExecutors, " +
      s"pending: $numPendingAllocate, running: ${runningExecutors.size}, " +
      s"executorsStarting: ${numExecutorsStarting.get}")

    // 标识仍然需要申请一部分的资源
    if (missing > 0) {
      logInfo(s"Will request $missing executor container(s), each with " +
        s"${resource.getVirtualCores} core(s) and " +
        s"${resource.getMemory} MB memory (including $memoryOverhead MB of overhead)")

      // 将待处理的容器请求分为三组：本地匹配列表，本地不匹配列表，非本地列表。
      // 考虑容器的放置情况时，将本地匹配的容器请求视为已分配的容器。
      // 对于本地不匹配和无本地偏好的容器请求，取消这些容器请求，因为所需的本地偏好已更改，需要重新计算容器的放置策略。
      val (localRequests, staleRequests, anyHostRequests) = splitPendingAllocationsByLocality(hostToLocalTaskCounts, pendingAllocate)

      // 取消不再需要的位置的“过时”请求。
      staleRequests.foreach { stale =>
        amClient.removeContainerRequest(stale)
      }
      val cancelledContainers = staleRequests.size
      if (cancelledContainers > 0) {
        logInfo(s"Canceled $cancelledContainers container request(s) (locality no longer needed)")
      }

      // 考虑可用的新容器数量和已取消的过时容器数量。
      val availableContainers = missing + cancelledContainers

      // to maximize locality, include requests with no locality preference that can be cancelled
      val potentialContainers = availableContainers + anyHostRequests.size

      val containerLocalityPreferences = containerPlacementStrategy.localityOfRequestedContainers(
        potentialContainers, numLocalityAwareTasks, hostToLocalTaskCounts,
          allocatedHostToContainersMap, localRequests)

      // 绑定
      val newLocalityRequests = new mutable.ArrayBuffer[ContainerRequest]
      containerLocalityPreferences.foreach {
        case ContainerLocalityPreferences(nodes, racks) if nodes != null =>

          newLocalityRequests += createContainerRequest(resource, nodes, racks)
        case _ =>
      }

      if (availableContainers >= newLocalityRequests.size) {
        // more containers are available than needed for locality, fill in requests for any host
        for (i <- 0 until (availableContainers - newLocalityRequests.size)) {
          newLocalityRequests += createContainerRequest(resource, null, null)
        }
      } else {
        val numToCancel = newLocalityRequests.size - availableContainers
        // cancel some requests without locality preferences to schedule more local containers
        anyHostRequests.slice(0, numToCancel).foreach { nonLocal =>
          amClient.removeContainerRequest(nonLocal)
        }
        if (numToCancel > 0) {
          logInfo(s"Canceled $numToCancel unlocalized container requests to resubmit with locality")
        }
      }

      // 增加资源列请求
      newLocalityRequests.foreach { request =>
        amClient.addContainerRequest(request)
      }

      if (log.isInfoEnabled()) {
        val (localized, anyHost) = newLocalityRequests.partition(_.getNodes() != null)
        if (anyHost.nonEmpty) {
          logInfo(s"Submitted ${anyHost.size} unlocalized container requests.")
        }
        localized.foreach { request =>
          logInfo(s"Submitted container request for host ${hostStr(request)}.")
        }
      }
    } else if (numPendingAllocate > 0 && missing < 0) {
      val numToCancel = math.min(numPendingAllocate, -missing)
      logInfo(s"Canceling requests for $numToCancel executor container(s) to have a new desired " +
        s"total $targetNumExecutors executors.")

      val matchingRequests = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, resource)
      if (!matchingRequests.isEmpty) {
        matchingRequests.iterator().next().asScala
          .take(numToCancel).foreach(amClient.removeContainerRequest)
      } else {
        logWarning("Expected to find pending requests, but found none.")
      }
    }
  }

  private def hostStr(request: ContainerRequest): String = {
    Option(request.getNodes) match {
      case Some(nodes) => nodes.asScala.mkString(",")
      case None => "Any"
    }
  }

  /**
   * Creates a container request, handling the reflection required to use YARN features that were
   * added in recent versions.
   */
  private def createContainerRequest(
      resource: Resource,
      nodes: Array[String],
      racks: Array[String]): ContainerRequest = {
    new ContainerRequest(resource, nodes, racks, RM_REQUEST_PRIORITY, true, labelExpression.orNull)
  }

  /**
   * 处理由RM授予的容器，通过在它们上启动执行器。
   * 由于YARN分配协议的工作方式，某些健康的竞争条件可能会导致YARN授予我们不再需要的容器。在这种情况下，我们会释放这些容器。
   *
   * Visible for testing.
   */
  def handleAllocatedContainers(allocatedContainers: Seq[Container]): Unit = {
    val containersToUse = new ArrayBuffer[Container](allocatedContainers.size)

    // Match incoming requests by host
    val remainingAfterHostMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- allocatedContainers) {
      matchContainerToRequest(allocatedContainer, allocatedContainer.getNodeId.getHost,
        containersToUse, remainingAfterHostMatches)
    }

    // Match remaining by rack
    val remainingAfterRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterHostMatches) {
      val rack = resolver.resolve(conf, allocatedContainer.getNodeId.getHost)
      matchContainerToRequest(allocatedContainer, rack, containersToUse,
        remainingAfterRackMatches)
    }

    // Assign remaining that are neither node-local nor rack-local
    val remainingAfterOffRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterRackMatches) {
      matchContainerToRequest(allocatedContainer, ANY_HOST, containersToUse,
        remainingAfterOffRackMatches)
    }

    if (!remainingAfterOffRackMatches.isEmpty) {
      logDebug(s"Releasing ${remainingAfterOffRackMatches.size} unneeded containers that were " +
        s"allocated to us")
      for (container <- remainingAfterOffRackMatches) {
        internalReleaseContainer(container)
      }
    }

    // 在已经分配的 container 上执行应用
    runAllocatedContainers(containersToUse)

    logInfo("Received %d containers from YARN, launching executors on %d of them."
      .format(allocatedContainers.size, containersToUse.size))
  }

  /**
   * Looks for requests for the given location that match the given container allocation. If it
   * finds one, removes the request so that it won't be submitted again. Places the container into
   * containersToUse or remaining.
   *
   * @param allocatedContainer container that was given to us by YARN
   * @param location resource name, either a node, rack, or *
   * @param containersToUse list of containers that will be used
   * @param remaining list of containers that will not be used
   */
  private def matchContainerToRequest(
      allocatedContainer: Container,
      location: String,
      containersToUse: ArrayBuffer[Container],
      remaining: ArrayBuffer[Container]): Unit = {
    // SPARK-6050: certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.
    val matchingResource = Resource.newInstance(allocatedContainer.getResource.getMemory,
          resource.getVirtualCores)
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      matchingResource)

    // Match the allocation to a request
    if (!matchingRequests.isEmpty) {
      val containerRequest = matchingRequests.get(0).iterator.next
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  // 在已经分配的 Container 上运行应用
  private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]): Unit = {
    for (container <- containersToUse) {
      executorIdCounter += 1
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      val executorId = executorIdCounter.toString
      assert(container.getResource.getMemory >= resource.getMemory)
      logInfo(s"Launching container $containerId on host $executorHostname " +
        s"for executor with ID $executorId")

      def updateInternalState(): Unit = synchronized {
        runningExecutors.add(executorId)
        numExecutorsStarting.decrementAndGet()
        executorIdToContainer(executorId) = container
        containerIdToExecutorId(container.getId) = executorId

        val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
          new HashSet[ContainerId])
        containerSet += containerId
        allocatedContainerToHostMap.put(containerId, executorHostname)
      }

      if (runningExecutors.size() < targetNumExecutors) {
        numExecutorsStarting.incrementAndGet()
        if (launchContainers) {
          launcherPool.execute(new Runnable {
            override def run(): Unit = {
              try {
                new ExecutorRunnable(
                  Some(container),
                  conf,
                  sparkConf,
                  driverUrl,
                  executorId,
                  executorHostname,
                  executorMemory,
                  executorCores,
                  appAttemptId.getApplicationId.toString,
                  securityMgr,
                  localResources
                ).run()
                updateInternalState()
              } catch {
                case e: Throwable =>
                  numExecutorsStarting.decrementAndGet()
                  if (NonFatal(e)) {
                    logError(s"Failed to launch executor $executorId on container $containerId", e)
                    // Assigned container should be released immediately
                    // to avoid unnecessary resource occupation.
                    amClient.releaseAssignedContainer(containerId)
                  } else {
                    throw e
                  }
              }
            }
          })
        } else {
          // For test only
          updateInternalState()
        }
      } else {
        logInfo(("Skip launching executorRunnable as running executors count: %d " +
          "reached target executors count: %d.").format(
          runningExecutors.size, targetNumExecutors))
      }
    }
  }

  // Visible for testing.
  private[yarn] def processCompletedContainers(completedContainers: Seq[ContainerStatus]): Unit = {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId
      val alreadyReleased = releasedContainers.remove(containerId)
      val hostOpt = allocatedContainerToHostMap.get(containerId)
      val onHostStr = hostOpt.map(host => s" on host: $host").getOrElse("")
      val exitReason = if (!alreadyReleased) {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        containerIdToExecutorId.get(containerId) match {
          case Some(executorId) => runningExecutors.remove(executorId)
          case None => logWarning(s"Cannot find executorId for container: ${containerId.toString}")
        }

        logInfo("Completed container %s%s (state: %s, exit status: %s)".format(
          containerId,
          onHostStr,
          completedContainer.getState,
          completedContainer.getExitStatus))
        // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
        // there are some exit status' we shouldn't necessarily count against us, but for
        // now I think its ok as none of the containers are expected to exit.
        val exitStatus = completedContainer.getExitStatus
        val (exitCausedByApp, containerExitReason) = exitStatus match {
          case ContainerExitStatus.SUCCESS =>
            (false, s"Executor for container $containerId exited because of a YARN event (e.g., " +
              "pre-emption) and not because of an error in the running job.")
          case ContainerExitStatus.PREEMPTED =>
            // Preemption is not the fault of the running tasks, since YARN preempts containers
            // merely to do resource sharing, and tasks that fail due to preempted executors could
            // just as easily finish on any other executor. See SPARK-8167.
            (false, s"Container ${containerId}${onHostStr} was preempted.")
          // Should probably still count memory exceeded exit codes towards task failures
          case VMEM_EXCEEDED_EXIT_CODE =>
            (true, memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              VMEM_EXCEEDED_PATTERN))
          case PMEM_EXCEEDED_EXIT_CODE =>
            (true, memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              PMEM_EXCEEDED_PATTERN))
          case _ =>
            // all the failures which not covered above, like:
            // disk failure, kill by app master or resource manager, ...
            allocatorBlacklistTracker.handleResourceAllocationFailure(hostOpt)
            (true, "Container marked as failed: " + containerId + onHostStr +
              ". Exit status: " + completedContainer.getExitStatus +
              ". Diagnostics: " + completedContainer.getDiagnostics)

        }
        if (exitCausedByApp) {
          logWarning(containerExitReason)
        } else {
          logInfo(containerExitReason)
        }
        ExecutorExited(exitStatus, exitCausedByApp, containerExitReason)
      } else {
        // If we have already released this container, then it must mean
        // that the driver has explicitly requested it to be killed
        ExecutorExited(completedContainer.getExitStatus, exitCausedByApp = false,
          s"Container $containerId exited from explicit termination request.")
      }

      for {
        host <- hostOpt
        containerSet <- allocatedHostToContainersMap.get(host)
      } {
        containerSet.remove(containerId)
        if (containerSet.isEmpty) {
          allocatedHostToContainersMap.remove(host)
        } else {
          allocatedHostToContainersMap.update(host, containerSet)
        }

        allocatedContainerToHostMap.remove(containerId)
      }

      containerIdToExecutorId.remove(containerId).foreach { eid =>
        executorIdToContainer.remove(eid)
        pendingLossReasonRequests.remove(eid) match {
          case Some(pendingRequests) =>
            // Notify application of executor loss reasons so it can decide whether it should abort
            pendingRequests.foreach(_.reply(exitReason))

          case None =>
            // We cannot find executor for pending reasons. This is because completed container
            // is processed before querying pending result. We should store it for later query.
            // This is usually happened when explicitly killing a container, the result will be
            // returned in one AM-RM communication. So query RPC will be later than this completed
            // container process.
            releasedExecutorLossReasons.put(eid, exitReason)
        }
        if (!alreadyReleased) {
          // The executor could have gone away (like no route to host, node failure, etc)
          // Notify backend about the failure of the executor
          numUnexpectedContainerRelease += 1
          driverRef.send(RemoveExecutor(eid, exitReason))
        }
      }
    }
  }

  /**
   * Register that some RpcCallContext has asked the AM why the executor was lost. Note that
   * we can only find the loss reason to send back in the next call to allocateResources().
   */
  private[yarn] def enqueueGetLossReasonRequest(
      eid: String,
      context: RpcCallContext): Unit = synchronized {
    if (executorIdToContainer.contains(eid)) {
      pendingLossReasonRequests
        .getOrElseUpdate(eid, new ArrayBuffer[RpcCallContext]) += context
    } else if (releasedExecutorLossReasons.contains(eid)) {
      // Executor is already released explicitly before getting the loss reason, so directly send
      // the pre-stored lost reason
      context.reply(releasedExecutorLossReasons.remove(eid).get)
    } else {
      logWarning(s"Tried to get the loss reason for non-existent executor $eid")
      context.sendFailure(
        new SparkException(s"Fail to find loss reason for non-existent executor $eid"))
    }
  }

  private def internalReleaseContainer(container: Container): Unit = {
    releasedContainers.add(container.getId())
    amClient.releaseAssignedContainer(container.getId())
  }

  private[yarn] def getNumUnexpectedContainerRelease = numUnexpectedContainerRelease

  private[yarn] def getNumPendingLossReasonRequests: Int = synchronized {
    pendingLossReasonRequests.size
  }

  /**
   * Split the pending container requests into 3 groups based on current localities of pending
   * tasks.
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @param pendingAllocations A sequence of pending allocation container request.
   * @return A tuple of 3 sequences, first is a sequence of locality matched container
   *         requests, second is a sequence of locality unmatched container requests, and third is a
   *         sequence of locality free container requests.
   */
  private def splitPendingAllocationsByLocality(hostToLocalTaskCount: Map[String, Int],
                                                pendingAllocations: Seq[ContainerRequest]
    ): (Seq[ContainerRequest], Seq[ContainerRequest], Seq[ContainerRequest]) = {
    val localityMatched = ArrayBuffer[ContainerRequest]()
    val localityUnMatched = ArrayBuffer[ContainerRequest]()
    val localityFree = ArrayBuffer[ContainerRequest]()

    val preferredHosts = hostToLocalTaskCount.keySet
    pendingAllocations.foreach { cr =>
      val nodes = cr.getNodes
      if (nodes == null) {
        localityFree += cr
      } else if (nodes.asScala.toSet.intersect(preferredHosts).nonEmpty) {
        localityMatched += cr
      } else {
        localityUnMatched += cr
      }
    }

    (localityMatched.toSeq, localityUnMatched.toSeq, localityFree.toSeq)
  }

}

private object YarnAllocator {
  val MEM_REGEX = "[0-9.]+ [KMG]B"
  val PMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX physical memory used")
  val VMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX virtual memory used")
  val VMEM_EXCEEDED_EXIT_CODE = -103
  val PMEM_EXCEEDED_EXIT_CODE = -104

  def memLimitExceededLogMessage(diagnostics: String, pattern: Pattern): String = {
    val matcher = pattern.matcher(diagnostics)
    val diag = if (matcher.find()) " " + matcher.group() + "." else ""
    s"Container killed by YARN for exceeding memory limits. $diag " +
      "Consider boosting spark.yarn.executor.memoryOverhead or " +
      "disabling yarn.nodemanager.vmem-check-enabled because of YARN-4714."
  }
}
