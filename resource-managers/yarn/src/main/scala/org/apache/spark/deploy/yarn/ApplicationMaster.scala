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

import java.io.{File, IOException}
import java.lang.reflect.{InvocationTargetException, Modifier}
import java.net.{URI, URL}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.collection.mutable.HashMap
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.commons.lang3.{StringUtils => ComStrUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.deploy.yarn.security.AMCredentialRenewer
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, YarnSchedulerBackend}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util._

/**
 * Spark On Yanrn 上的主节点应用程序.
 */
private[spark] class ApplicationMaster(args: ApplicationMasterArguments) extends Logging {

  private val isClusterMode = args.userClass != null     // cluster 模式标识

  // spark 配置
  private val sparkConf = new SparkConf()

  // 初始化配置
  if (args.propertiesFile != null) {
    Utils.getPropertiesFromFile(args.propertiesFile).foreach { case (k, v) =>
      sparkConf.set(k, v)
    }
  }

  private val securityMgr = new SecurityManager(sparkConf)

  // 指标管理
  private var metricsSystem: Option[MetricsSystem] = None

  // Spark应用程序主节点会根据系统属性设置每个配置项的值。这涵盖了两种情况：
  // SparkHadoopUtil类存储的默认配置。
  // 用户在集群模式下创建的新SparkConf对象。
  // 无论是默认配置还是用户创建的SparkConf对象，都会从系统属性中读取这些配置。
  sparkConf.getAll.foreach { case (k, v) =>
    sys.props(k) = v
  }

  // hadoop yarn 配置信息
  private val yarnConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(sparkConf))

  // 类加载器
  private val userClassLoader = {
    val classpath = Client.getUserClasspath(sparkConf)
    val urls = classpath.map { entry =>
      new URL("file:" + new File(entry.getPath()).getAbsolutePath())
    }

    if (isClusterMode) {
      // spark.driver.userClassPathFirst
      if (Client.isUserClassPathFirst(sparkConf, isDriver = true)) {
        new ChildFirstURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      } else {
        new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      }
    } else {
      new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
    }
  }

  private val credentialRenewer: Option[AMCredentialRenewer] = sparkConf.get(KEYTAB).map { _ =>
    new AMCredentialRenewer(sparkConf, yarnConf)
  }

  private val ugi = credentialRenewer match {
    case Some(cr) =>
      // Set the context class loader so that the token renewer has access to jars distributed
      // by the user.
      val currentLoader = Thread.currentThread().getContextClassLoader()
      Thread.currentThread().setContextClassLoader(userClassLoader)
      try {
        cr.start()
      } finally {
        Thread.currentThread().setContextClassLoader(currentLoader)
      }

    case _ =>
      SparkHadoopUtil.get.createSparkUser()
  }

  // yarn client
  private val client = doAsUser { new YarnRMClient() }

  // 默认情况下，Spark在YARN上的执行器数量为两倍（如果启用了动态分配，则为两倍的最大执行器数量），
  // 最低为3个。这些配置是通过系统属性设置的。

  private val maxNumExecutorFailures = {
    val effectiveNumExecutors =
      if (Utils.isDynamicAllocationEnabled(sparkConf)) {
        sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      } else {
        sparkConf.get(EXECUTOR_INSTANCES).getOrElse(0)
      }
    // By default, effectiveNumExecutors is Int.MaxValue if dynamic allocation is enabled.
    // We need avoid the integer overflow here.
    val defaultMaxNumExecutorFailures = math.max(3,
      if (effectiveNumExecutors > Int.MaxValue / 2) Int.MaxValue else (2 * effectiveNumExecutors))

    // spark.yarn.max.executor.failures
    sparkConf.get(MAX_EXECUTOR_FAILURES).getOrElse(defaultMaxNumExecutorFailures)
  }

  @volatile private var exitCode = 0                         // 退出码
  @volatile private var unregistered = false
  @volatile private var finished = false                     // 是否结束
  @volatile private var finalStatus = getDefaultFinalStatus  // 最终状态
  @volatile private var finalMsg: String = ""                // 最终信息
  @volatile private var userClassThread: Thread = _          // 用户class线程

  @volatile private var reporterThread: Thread = _          // 报告线程
  @volatile private var allocator: YarnAllocator = _        // yarn 资源分配器

  // A flag to check whether user has initialized spark context
  @volatile private var registered = false

  // Lock for controlling the allocator (heartbeat) thread.
  private val allocatorLock = new Object()    // 资源分配锁

  // 稳态心跳间隔。
  // 我们希望能够做出合理的响应，而不会对 RM 造成太多请求。
  private val heartbeatInterval = {    // 心跳检测时间间隔
    //确保在 YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS 过去之前发送进度.
    // yarn.am.liveness-monitor.expiry-interval-ms
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)
    // spark.yarn.scheduler.heartbeat.interval-ms
    math.max(0, math.min(expiryInterval / 2, sparkConf.get(RM_HEARTBEAT_INTERVAL)))
  }

  // 分配器轮询之前的初始等待间隔，以便在请求执行器时能够更快地启动.
  private val initialAllocationInterval = math.min(heartbeatInterval, sparkConf.get(INITIAL_HEARTBEAT_INTERVAL))

  // Next wait interval before allocator poll.
  private var nextAllocationInterval = initialAllocationInterval

  private var rpcEnv: RpcEnv = null

  // In cluster mode, used to tell the AM when the user's SparkContext has been initialized.
  private val sparkContextPromise = Promise[SparkContext]()

  // 加载客户端设置的本地化文件列表。
  // 这是在启动执行器时使用的，并在此处加载，以便这些配置不会污染集群模式下的 Web UI 环境页面
  private val localResources = doAsUser {
    logInfo("Preparing Local resources")
    val resources = HashMap[String, LocalResource]()

    def setupDistributedCache(
        file: String,
        rtype: LocalResourceType,
        timestamp: String,
        size: String,
        vis: String): Unit = {
      val uri = new URI(file)
      val amJarRsrc = Records.newRecord(classOf[LocalResource])
      amJarRsrc.setType(rtype)
      amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(vis))
      amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri))
      amJarRsrc.setTimestamp(timestamp.toLong)
      amJarRsrc.setSize(size.toLong)

      val fileName = Option(uri.getFragment()).getOrElse(new Path(uri).getName())
      resources(fileName) = amJarRsrc
    }

    val distFiles = sparkConf.get(CACHED_FILES)
    val fileSizes = sparkConf.get(CACHED_FILES_SIZES)
    val timeStamps = sparkConf.get(CACHED_FILES_TIMESTAMPS)
    val visibilities = sparkConf.get(CACHED_FILES_VISIBILITIES)
    val resTypes = sparkConf.get(CACHED_FILES_TYPES)

    for (i <- 0 to distFiles.size - 1) {
      val resType = LocalResourceType.valueOf(resTypes(i))
      setupDistributedCache(distFiles(i), resType, timeStamps(i).toString, fileSizes(i).toString,
      visibilities(i))
    }

    // Distribute the conf archive to executors.
    sparkConf.get(CACHED_CONF_ARCHIVE).foreach { path =>
      val uri = new URI(path)
      val fs = FileSystem.get(uri, yarnConf)
      val status = fs.getFileStatus(new Path(uri))
      // SPARK-16080: Make sure to use the correct name for the destination when distributing the
      // conf archive to executors.
      val destUri = new URI(uri.getScheme(), uri.getRawSchemeSpecificPart(), Client.LOCALIZED_CONF_DIR)

      setupDistributedCache(destUri.toString(), LocalResourceType.ARCHIVE,
        status.getModificationTime().toString, status.getLen.toString,
        LocalResourceVisibility.PRIVATE.name())
    }

    // Clean up the configuration so it doesn't show up in the Web UI (since it's really noisy).
    CACHE_CONFIGS.foreach { e =>
      sparkConf.remove(e)
      sys.props.remove(e.key)
    }

    resources.toMap
  }

  // Application 重试ID
  def getAttemptId(): ApplicationAttemptId = {
    client.getAttemptId()
  }

  final def run(): Int = {
    doAsUser {
      runImpl()
    }
    exitCode
  }

  private def runImpl(): Unit = {
    try {
      val appAttemptId = client.getAttemptId()

      var attemptID: Option[String] = None

      if (isClusterMode) {
        // Set the web ui port to be ephemeral for yarn so we don't conflict with
        // other spark processes running on the same box
        System.setProperty("spark.ui.port", "0")

        // Set the master and deploy mode property to match the requested mode.
        System.setProperty("spark.master", "yarn")
        System.setProperty("spark.submit.deployMode", "cluster")

        // Set this internal configuration if it is running on cluster mode, this
        // configuration will be checked in SparkContext to avoid misuse of yarn cluster mode.
        System.setProperty("spark.yarn.app.id", appAttemptId.getApplicationId().toString())

        attemptID = Option(appAttemptId.getAttemptId.toString)
      }

      new CallerContext("APPMASTER", sparkConf.get(APP_CALLER_CONTEXT),
        Option(appAttemptId.getApplicationId.toString), attemptID).setCurrentContext()

      logInfo("ApplicationAttemptId: " + appAttemptId)

      // This shutdown hook should run *after* the SparkContext is shut down.
      val priority = ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY - 1

      // 注册退出状态
      ShutdownHookManager.addShutdownHook(priority) { () =>
        val maxAppAttempts = client.getMaxRegAttempts(sparkConf, yarnConf)
        val isLastAttempt = client.getAttemptId().getAttemptId() >= maxAppAttempts

        if (!finished) {
          // The default state of ApplicationMaster is failed if it is invoked by shut down hook.
          // This behavior is different compared to 1.x version.
          // If user application is exited ahead of time by calling System.exit(N), here mark
          // this application as failed with EXIT_EARLY. For a good shutdown, user shouldn't call
          // System.exit(0) to terminate the application.
          // 设置作业结束状态
          finish(finalStatus, ApplicationMaster.EXIT_EARLY, "Shutdown hook called before final status was reported.")
        }

        if (!unregistered) {
          // we only want to unregister if we don't want the RM to retry
          if (finalStatus == FinalApplicationStatus.SUCCEEDED || isLastAttempt) {
            unregister(finalStatus, finalMsg)
            cleanupStagingDir()
          }
        }
      }

      if (isClusterMode) {
        runDriver()
      } else {
        runExecutorLauncher()
      }
    } catch {
      case e: Exception =>
        // catch everything else if not specifically handled
        logError("Uncaught exception: ", e)
        finish(FinalApplicationStatus.FAILED,
          ApplicationMaster.EXIT_UNCAUGHT_EXCEPTION,
          "Uncaught exception: " + StringUtils.stringifyException(e))
    } finally {
      try {
        metricsSystem.foreach { ms =>
          ms.report()
          ms.stop()
        }
      } catch {
        case e: Exception =>
          logWarning("Exception during stopping of the metric system: ", e)
      }
    }
  }

  /**
   * Set the default final application status for client mode to UNDEFINED to handle
   * if YARN HA restarts the application so that it properly retries. Set the final
   * status to SUCCEEDED in cluster mode to handle if the user calls System.exit
   * from the application code.
   */
  final def getDefaultFinalStatus(): FinalApplicationStatus = {
    if (isClusterMode) {
      FinalApplicationStatus.FAILED
    } else {
      FinalApplicationStatus.UNDEFINED
    }
  }

  /**
   * unregister is used to completely unregister the application from the ResourceManager.
   * This means the ResourceManager will not retry the application attempt on your behalf if
   * a failure occurred.
   */
  final def unregister(status: FinalApplicationStatus, diagnostics: String = null): Unit = {
    synchronized {
      if (registered && !unregistered) {
        logInfo(s"Unregistering ApplicationMaster with $status" +
          Option(diagnostics).map(msg => s" (diag message: $msg)").getOrElse(""))
        unregistered = true
        client.unregister(status, Option(diagnostics).getOrElse(""))
      }
    }
  }

  final def finish(status: FinalApplicationStatus, code: Int, msg: String = null): Unit = {
    synchronized {
      if (!finished) {
        val inShutdown = ShutdownHookManager.inShutdown()
        if (registered || !isClusterMode) {
          exitCode = code
          finalStatus = status
        } else {
          finalStatus = FinalApplicationStatus.FAILED
          exitCode = ApplicationMaster.EXIT_SC_NOT_INITED
        }
        logInfo(s"Final app status: $finalStatus, exitCode: $exitCode" +
          Option(msg).map(msg => s", (reason: $msg)").getOrElse(""))
        finalMsg = ComStrUtils.abbreviate(msg, sparkConf.get(AM_FINAL_MSG_LIMIT).toInt)
        finished = true
        if (!inShutdown && Thread.currentThread() != reporterThread && reporterThread != null) {
          logDebug("shutting down reporter thread")
          reporterThread.interrupt()
        }
        if (!inShutdown && Thread.currentThread() != userClassThread && userClassThread != null) {
          logDebug("shutting down user thread")
          userClassThread.interrupt()
        }
        if (!inShutdown) {
          credentialRenewer.foreach(_.stop())
        }
      }
    }
  }

  private def sparkContextInitialized(sc: SparkContext) = {
    sparkContextPromise.synchronized {
      // Notify runDriver function that SparkContext is available
      sparkContextPromise.success(sc)
      // Pause the user class thread in order to make proper initialization in runDriver function.
      sparkContextPromise.wait()
    }
  }

  private def resumeDriver(): Unit = {
    // When initialization in runDriver happened the user class thread has to be resumed.
    sparkContextPromise.synchronized {
      sparkContextPromise.notify()
    }
  }

  private def registerAM(
      host: String,
      port: Int,
      _sparkConf: SparkConf,
      uiAddress: Option[String]): Unit = {
    val appId = client.getAttemptId().getApplicationId().toString()
    val attemptId = client.getAttemptId().getAttemptId().toString()
    val historyAddress = ApplicationMaster
      .getHistoryServerAddress(_sparkConf, yarnConf, appId, attemptId)

    client.register(host, port, yarnConf, _sparkConf, uiAddress, historyAddress)
    registered = true
  }

  private def createAllocator(driverRef: RpcEndpointRef, _sparkConf: SparkConf): Unit = {
    val appId = client.getAttemptId().getApplicationId().toString()
    val driverUrl = RpcEndpointAddress(driverRef.address.host, driverRef.address.port,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString


    // 在初始化分配器之前，让我们提前记录有关执行器如何运行的信息。
    // 在初始化分配器之前，我们可以在日志中记录有关执行器如何运行的信息，以避免在每个启动的执行器上都打印出这些信息。
    // Question: 使用占位符来表示可能会变化的信息，比如执行器的ID。
    logInfo {
      val executorMemory = _sparkConf.get(EXECUTOR_MEMORY).toInt
      val executorCores = _sparkConf.get(EXECUTOR_CORES)
      val dummyRunner = new ExecutorRunnable(None, yarnConf, _sparkConf, driverUrl, "<executorId>",
        "<hostname>", executorMemory, executorCores, appId, securityMgr, localResources)
      dummyRunner.launchContextDebugInfo()
    }

    // 创建资源分配器
    allocator = client.createAllocator(
      yarnConf,
      _sparkConf,
      driverUrl,
      driverRef,
      securityMgr,
      localResources)

    credentialRenewer.foreach(_.setDriverRef(driverRef))

    // Initialize the AM endpoint *after* the allocator has been initialized. This ensures
    // that when the driver sends an initial executor request (e.g. after an AM restart),
    // the allocator is ready to service requests.
    rpcEnv.setupEndpoint("YarnAM", new AMEndpoint(rpcEnv, driverRef))

    // 分配资源
    allocator.allocateResources()
    val ms = MetricsSystem.createMetricsSystem("applicationMaster", sparkConf, securityMgr)
    val prefix = _sparkConf.get(YARN_METRICS_NAMESPACE).getOrElse(appId)
    ms.registerSource(new ApplicationMasterSource(prefix, allocator))
    ms.start()
    metricsSystem = Some(ms)
    reporterThread = launchReporterThread()
  }

  private def runDriver(): Unit = {
    addAmIpFilter(None)

    // 启动并执行用户的主函数
    userClassThread = startUserApplication()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    logInfo("Waiting for spark context initialization...")
    val totalWaitTime = sparkConf.get(AM_MAX_WAIT_TIME) // spark.yarn.am.waitTime
    try {
      val sc = ThreadUtils.awaitResult(sparkContextPromise.future, Duration(totalWaitTime, TimeUnit.MILLISECONDS))
      if (sc != null) {
        rpcEnv = sc.env.rpcEnv

        val userConf = sc.getConf
        val host = userConf.get("spark.driver.host")
        val port = userConf.get("spark.driver.port").toInt
        registerAM(host, port, userConf, sc.ui.map(_.webUrl))

        // RPC 消息通信
        val driverRef = rpcEnv.setupEndpointRef(RpcAddress(host, port), YarnSchedulerBackend.ENDPOINT_NAME)

        createAllocator(driverRef, userConf)
      } else {
        // Sanity check; should never happen in normal operation, since sc should only be null
        // if the user app did not create a SparkContext.
        throw new IllegalStateException("User did not initialize spark context!")
      }
      resumeDriver()
      userClassThread.join()
    } catch {
      case e: SparkException if e.getCause().isInstanceOf[TimeoutException] =>
        logError(
          s"SparkContext did not initialize after waiting for $totalWaitTime ms. " +
           "Please check earlier log output for errors. Failing the application.")
        finish(FinalApplicationStatus.FAILED,
          ApplicationMaster.EXIT_SC_NOT_INITED,
          "Timed out waiting for SparkContext.")
    } finally {
      resumeDriver()
    }
  }

  private def runExecutorLauncher(): Unit = {
    val hostname = Utils.localHostName
    val amCores = sparkConf.get(AM_CORES)
    rpcEnv = RpcEnv.create("sparkYarnAM", hostname, hostname, -1, sparkConf, securityMgr,
      amCores, true)

    // The client-mode AM doesn't listen for incoming connections, so report an invalid port.
    registerAM(hostname, -1, sparkConf, sparkConf.getOption("spark.driver.appUIAddress"))

    // The driver should be up and listening, so unlike cluster mode, just try to connect to it
    // with no waiting or retrying.
    val (driverHost, driverPort) = Utils.parseHostPort(args.userArgs(0))
    val driverRef = rpcEnv.setupEndpointRef(
      RpcAddress(driverHost, driverPort),
      YarnSchedulerBackend.ENDPOINT_NAME)
    addAmIpFilter(Some(driverRef))
    createAllocator(driverRef, sparkConf)

    // In client mode the actor will stop the reporter thread.
    reporterThread.join()
  }

  private def launchReporterThread(): Thread = {
    // The number of failures in a row until Reporter thread give up
    val reporterMaxFailures = sparkConf.get(MAX_REPORTER_THREAD_FAILURES)

    val t = new Thread {
      override def run() {
        var failureCount = 0
        while (!finished) {
          try {
            if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
              finish(FinalApplicationStatus.FAILED,
                ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES,
                s"Max number of executor failures ($maxNumExecutorFailures) reached")
            } else if (allocator.isAllNodeBlacklisted) {
              finish(FinalApplicationStatus.FAILED,
                ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES,
                "Due to executor failures all available nodes are blacklisted")
            } else {
              logDebug("Sending progress")
              allocator.allocateResources()
            }
            failureCount = 0
          } catch {
            case i: InterruptedException => // do nothing
            case e: ApplicationAttemptNotFoundException =>
              failureCount += 1
              logError("Exception from Reporter thread.", e)
              finish(FinalApplicationStatus.FAILED, ApplicationMaster.EXIT_REPORTER_FAILURE,
                e.getMessage)
            case e: Throwable =>
              failureCount += 1
              if (!NonFatal(e) || failureCount >= reporterMaxFailures) {
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_REPORTER_FAILURE, "Exception was thrown " +
                    s"$failureCount time(s) from Reporter thread.")
              } else {
                logWarning(s"Reporter thread fails $failureCount time(s) in a row.", e)
              }
          }
          try {
            val numPendingAllocate = allocator.getPendingAllocate.size
            var sleepStart = 0L
            var sleepInterval = 200L // ms
            allocatorLock.synchronized {
              sleepInterval =
                if (numPendingAllocate > 0 || allocator.getNumPendingLossReasonRequests > 0) {
                  val currentAllocationInterval =
                    math.min(heartbeatInterval, nextAllocationInterval)
                  nextAllocationInterval = currentAllocationInterval * 2 // avoid overflow
                  currentAllocationInterval
                } else {
                  nextAllocationInterval = initialAllocationInterval
                  heartbeatInterval
                }
              sleepStart = System.currentTimeMillis()
              allocatorLock.wait(sleepInterval)
            }
            val sleepDuration = System.currentTimeMillis() - sleepStart
            if (sleepDuration < sleepInterval) {
              // log when sleep is interrupted
              logDebug(s"Number of pending allocations is $numPendingAllocate. " +
                  s"Slept for $sleepDuration/$sleepInterval ms.")
              // if sleep was less than the minimum interval, sleep for the rest of it
              val toSleep = math.max(0, initialAllocationInterval - sleepDuration)
              if (toSleep > 0) {
                logDebug(s"Going back to sleep for $toSleep ms")
                // use Thread.sleep instead of allocatorLock.wait. there is no need to be woken up
                // by the methods that signal allocatorLock because this is just finishing the min
                // sleep interval, which should happen even if this is signalled again.
                Thread.sleep(toSleep)
              }
            } else {
              logDebug(s"Number of pending allocations is $numPendingAllocate. " +
                  s"Slept for $sleepDuration/$sleepInterval.")
            }
          } catch {
            case e: InterruptedException =>
          }
        }
      }
    }
    // setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.setName("Reporter")
    t.start()
    logInfo(s"Started progress reporter thread with (heartbeat : $heartbeatInterval, " +
            s"initial allocation : $initialAllocationInterval) intervals")
    t
  }

  /**
   * Clean up the staging directory.
   */
  private def cleanupStagingDir(): Unit = {
    var stagingDirPath: Path = null
    try {
      val preserveFiles = sparkConf.get(PRESERVE_STAGING_FILES)
      if (!preserveFiles) {
        stagingDirPath = new Path(System.getenv("SPARK_YARN_STAGING_DIR"))
        logInfo("Deleting staging directory " + stagingDirPath)
        val fs = stagingDirPath.getFileSystem(yarnConf)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logError("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  /** Add the Yarn IP filter that is required for properly securing the UI. */
  private def addAmIpFilter(driver: Option[RpcEndpointRef]) = {
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val params = client.getAmIpFilterParams(yarnConf, proxyBase)
    driver match {
      case Some(d) =>
        d.send(AddWebUIFilter(amFilter, params.toMap, proxyBase))

      case None =>
        System.setProperty("spark.ui.filters", amFilter)
        params.foreach { case (k, v) => System.setProperty(s"spark.$amFilter.param.$k", v) }
    }
  }

  /**
   * 在单独的线程中启动包含 Spark 驱动程序的用户类。
   * 如果主例程干净地退出或以 System.exit(N) 退出，对于任何 N 我们认为它是成功的，
   * 对于所有其他情况，我们假设失败。
   *
   * Returns the user thread that was started.
   */
  private def startUserApplication(): Thread = {
    logInfo("Starting the user application in a separate Thread")

    var userArgs = args.userArgs  // 用户参数
    if (args.primaryPyFile != null && args.primaryPyFile.endsWith(".py")) {
      // When running pyspark, the app is run using PythonRunner. The second argument is the list
      // of files to add to PYTHONPATH, which Client.scala already handles, so it's empty.
      userArgs = Seq(args.primaryPyFile, "") ++ userArgs
    }
    if (args.primaryRFile != null && args.primaryRFile.endsWith(".R")) {
      // TODO(davies): add R dependencies here
    }

    // 通过反射获取到用户的执行主类
    val mainMethod = userClassLoader.loadClass(args.userClass)
      .getMethod("main", classOf[Array[String]])

    val userThread = new Thread {
      override def run() {
        try {
          if (!Modifier.isStatic(mainMethod.getModifiers)) {
            logError(s"Could not find static main method in object ${args.userClass}")
            finish(FinalApplicationStatus.FAILED, ApplicationMaster.EXIT_EXCEPTION_USER_CLASS)
          } else {
            // 调用用户的执行主函数
            mainMethod.invoke(null, userArgs.toArray)
            finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
            logDebug("Done running user class")
          }
        } catch {
          case e: InvocationTargetException =>
            e.getCause match {
              case _: InterruptedException =>
                // Reporter thread can interrupt to stop user class
              case SparkUserAppException(exitCode) =>
                val msg = s"User application exited with status $exitCode"
                logError(msg)
                finish(FinalApplicationStatus.FAILED, exitCode, msg)
              case cause: Throwable =>
                logError("User class threw exception: " + cause, cause)
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_EXCEPTION_USER_CLASS,
                  "User class threw exception: " + StringUtils.stringifyException(cause))
            }
            sparkContextPromise.tryFailure(e.getCause())
        } finally {
          // Notify the thread waiting for the SparkContext, in case the application did not
          // instantiate one. This will do nothing when the user code instantiates a SparkContext
          // (with the correct master), or when the user code throws an exception (due to the
          // tryFailure above).
          sparkContextPromise.trySuccess(null)
        }
      }
    }
    userThread.setContextClassLoader(userClassLoader)
    userThread.setName("Driver")
    userThread.start()
    userThread
  }

  private def resetAllocatorInterval(): Unit = allocatorLock.synchronized {
    nextAllocationInterval = initialAllocationInterval
    allocatorLock.notifyAll()
  }

  /**
   * An [[RpcEndpoint]] that communicates with the driver's scheduler backend.
   */
  private class AMEndpoint(override val rpcEnv: RpcEnv, driver: RpcEndpointRef)
    extends RpcEndpoint with Logging {

    override def onStart(): Unit = {
      driver.send(RegisterClusterManager(self))
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case r: RequestExecutors =>
        Option(allocator) match {
          case Some(a) =>
            if (a.requestTotalExecutorsWithPreferredLocalities(r.requestedTotal,
              r.localityAwareTasks, r.hostToLocalTaskCount, r.nodeBlacklist)) {
              resetAllocatorInterval()
            }
            context.reply(true)

          case None =>
            logWarning("Container allocator is not ready to request executors yet.")
            context.reply(false)
        }

      case KillExecutors(executorIds) =>
        logInfo(s"Driver requested to kill executor(s) ${executorIds.mkString(", ")}.")
        Option(allocator) match {
          case Some(a) => executorIds.foreach(a.killExecutor)
          case None => logWarning("Container allocator is not ready to kill executors yet.")
        }
        context.reply(true)

      case GetExecutorLossReason(eid) =>
        Option(allocator) match {
          case Some(a) =>
            a.enqueueGetLossReasonRequest(eid, context)
            resetAllocatorInterval()
          case None =>
            logWarning("Container allocator is not ready to find executor loss reasons yet.")
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      // In cluster mode, do not rely on the disassociated event to exit
      // This avoids potentially reporting incorrect exit codes if the driver fails
      if (!isClusterMode) {
        logInfo(s"Driver terminated or disconnected! Shutting down. $remoteAddress")
        finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
      }
    }
  }

  private def doAsUser[T](fn: => T): T = {
    ugi.doAs(new PrivilegedExceptionAction[T]() {
      override def run: T = fn
    })
  }

}

object ApplicationMaster extends Logging {

  // ApplicaionMaster 应用退出状态
  private val EXIT_SUCCESS = 0
  private val EXIT_UNCAUGHT_EXCEPTION = 10
  private val EXIT_MAX_EXECUTOR_FAILURES = 11
  private val EXIT_REPORTER_FAILURE = 12
  private val EXIT_SC_NOT_INITED = 13
  private val EXIT_SECURITY = 14
  private val EXIT_EXCEPTION_USER_CLASS = 15
  private val EXIT_EARLY = 16

  private var master: ApplicationMaster = _

  def main(args: Array[String]): Unit = {
    SignalUtils.registerLogger(log)
    val amArgs = new ApplicationMasterArguments(args)
    master = new ApplicationMaster(amArgs)
    System.exit(master.run())
  }

  private[spark] def sparkContextInitialized(sc: SparkContext): Unit = {
    master.sparkContextInitialized(sc)
  }

  private[spark] def getAttemptId(): ApplicationAttemptId = {
    master.getAttemptId
  }

  private[spark] def getHistoryServerAddress(
      sparkConf: SparkConf,
      yarnConf: YarnConfiguration,
      appId: String,
      attemptId: String): String = {
    sparkConf.get(HISTORY_SERVER_ADDRESS)
      .map { text => SparkHadoopUtil.get.substituteHadoopVariables(text, yarnConf) }
      .map { address => s"${address}${HistoryServer.UI_PATH_PREFIX}/${appId}/${attemptId}" }
      .getOrElse("")
  }
}

/**
 * This object does not provide any special functionality. It exists so that it's easy to tell
 * apart the client-mode AM from the cluster-mode AM when using tools such as ps or jps.
 */
object ExecutorLauncher {

  def main(args: Array[String]): Unit = {
    ApplicationMaster.main(args)
  }

}
