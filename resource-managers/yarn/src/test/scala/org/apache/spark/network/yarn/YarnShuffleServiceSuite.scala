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
package org.apache.spark.network.yarn

import java.io.{DataOutputStream, File, FileOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission._
import java.util.EnumSet

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.fs.Path
import org.apache.hadoop.service.ServiceStateException
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.api.{ApplicationInitializationContext, ApplicationTerminationContext}
import org.scalatest.{BeforeAndAfterEach, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.SecurityManager
import org.apache.spark.SparkFunSuite
import org.apache.spark.network.shuffle.ShuffleTestAccessor
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.util.Utils

class YarnShuffleServiceSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {
  private[yarn] var yarnConfig: YarnConfiguration = null
  private[yarn] val SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager"

  private var recoveryLocalDir: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    yarnConfig = new YarnConfiguration()
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, "spark_shuffle")
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format("spark_shuffle"),
      classOf[YarnShuffleService].getCanonicalName)
    yarnConfig.setInt("spark.shuffle.service.port", 0)
    yarnConfig.setBoolean(YarnShuffleService.STOP_ON_FAILURE_KEY, true)
    val localDir = Utils.createTempDir()
    yarnConfig.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath)

    recoveryLocalDir = Utils.createTempDir()
  }

  var s1: YarnShuffleService = null
  var s2: YarnShuffleService = null
  var s3: YarnShuffleService = null

  override def afterEach(): Unit = {
    try {
      if (s1 != null) {
        s1.stop()
        s1 = null
      }
      if (s2 != null) {
        s2.stop()
        s2 = null
      }
      if (s3 != null) {
        s3.stop()
        s3 = null
      }
    } finally {
      super.afterEach()
    }
  }

  test("executor state kept across NM restart") {
    s1 = new YarnShuffleService
    s1.setRecoveryPath(new Path(recoveryLocalDir.toURI))
    // set auth to true to test the secrets recovery
    yarnConfig.setBoolean(SecurityManager.SPARK_AUTH_CONF, true)
    s1.init(yarnConfig)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data = makeAppInfo("user", app2Id)
    s1.initializeApplication(app2Data)

    val execStateFile = s1.registeredExecutorFile
    execStateFile should not be (null)
    val secretsFile = s1.secretsFile
    secretsFile should not be (null)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, SORT_MANAGER)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, SORT_MANAGER)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", blockResolver) should
      be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", blockResolver) should
      be (Some(shuffleInfo2))

    if (!execStateFile.exists()) {
      @tailrec def findExistingParent(file: File): File = {
        if (file == null) file
        else if (file.exists()) file
        else findExistingParent(file.getParentFile())
      }
      val existingParent = findExistingParent(execStateFile)
      assert(false, s"$execStateFile does not exist -- closest existing parent is $existingParent")
    }
    assert(execStateFile.exists(), s"$execStateFile did not exist")

    // now we pretend the shuffle service goes down, and comes back up
    s1.stop()
    s2 = new YarnShuffleService
    s2.setRecoveryPath(new Path(recoveryLocalDir.toURI))
    s2.init(yarnConfig)
    s2.secretsFile should be (secretsFile)
    s2.registeredExecutorFile should be (execStateFile)

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)

    // now we reinitialize only one of the apps, and expect yarn to tell us that app2 was stopped
    // during the restart
    s2.initializeApplication(app1Data)
    s2.stopApplication(new ApplicationTerminationContext(app2Id))
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (None)

    // Act like the NM restarts one more time
    s2.stop()
    s3 = new YarnShuffleService
    s3.setRecoveryPath(new Path(recoveryLocalDir.toURI))
    s3.init(yarnConfig)
    s3.registeredExecutorFile should be (execStateFile)
    s3.secretsFile should be (secretsFile)

    val handler3 = s3.blockHandler
    val resolver3 = ShuffleTestAccessor.getBlockResolver(handler3)

    // app1 is still running
    s3.initializeApplication(app1Data)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver3) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver3) should be (None)
    s3.stop()
  }

  test("removed applications should not be in registered executor file") {
    s1 = new YarnShuffleService
    s1.setRecoveryPath(new Path(recoveryLocalDir.toURI))
    yarnConfig.setBoolean(SecurityManager.SPARK_AUTH_CONF, false)
    s1.init(yarnConfig)
    val secretsFile = s1.secretsFile
    secretsFile should be (null)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data = makeAppInfo("user", app2Id)
    s1.initializeApplication(app2Data)

    val execStateFile = s1.registeredExecutorFile
    execStateFile should not be (null)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, SORT_MANAGER)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, SORT_MANAGER)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)

    val db = ShuffleTestAccessor.shuffleServiceLevelDB(blockResolver)
    ShuffleTestAccessor.reloadRegisteredExecutors(db) should not be empty

    s1.stopApplication(new ApplicationTerminationContext(app1Id))
    ShuffleTestAccessor.reloadRegisteredExecutors(db) should not be empty
    s1.stopApplication(new ApplicationTerminationContext(app2Id))
    ShuffleTestAccessor.reloadRegisteredExecutors(db) shouldBe empty
  }

  test("shuffle service should be robust to corrupt registered executor file") {
    s1 = new YarnShuffleService
    s1.setRecoveryPath(new Path(recoveryLocalDir.toURI))
    s1.init(yarnConfig)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)

    val execStateFile = s1.registeredExecutorFile
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, SORT_MANAGER)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)

    // now we pretend the shuffle service goes down, and comes back up.  But we'll also
    // make a corrupt registeredExecutor File
    s1.stop()

    execStateFile.listFiles().foreach{_.delete()}

    val out = new DataOutputStream(new FileOutputStream(execStateFile + "/CURRENT"))
    out.writeInt(42)
    out.close()

    s2 = new YarnShuffleService
    s2.setRecoveryPath(new Path(recoveryLocalDir.toURI))
    s2.init(yarnConfig)
    s2.registeredExecutorFile should be (execStateFile)

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)

    // we re-initialize app1, but since the file was corrupt there is nothing we can do about it ...
    s2.initializeApplication(app1Data)
    // however, when we initialize a totally new app2, everything is still happy
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data = makeAppInfo("user", app2Id)
    s2.initializeApplication(app2Data)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, SORT_MANAGER)
    resolver2.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (Some(shuffleInfo2))
    s2.stop()

    // another stop & restart should be fine though (eg., we recover from previous corruption)
    s3 = new YarnShuffleService
    s3.setRecoveryPath(new Path(recoveryLocalDir.toURI))
    s3.init(yarnConfig)
    s3.registeredExecutorFile should be (execStateFile)
    val handler3 = s3.blockHandler
    val resolver3 = ShuffleTestAccessor.getBlockResolver(handler3)

    s3.initializeApplication(app2Data)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver3) should be (Some(shuffleInfo2))
    s3.stop()
  }

  test("get correct recovery path") {
    // Test recovery path is set outside the shuffle service, this is to simulate NM recovery
    // enabled scenario, where recovery path will be set by yarn.
    s1 = new YarnShuffleService
    val recoveryPath = new Path(Utils.createTempDir().toURI)
    s1.setRecoveryPath(recoveryPath)

    s1.init(yarnConfig)
    s1._recoveryPath should be (recoveryPath)
    s1.stop()
  }

  test("moving recovery file from NM local dir to recovery path") {
    // This is to test when Hadoop is upgrade to 2.5+ and NM recovery is enabled, we should move
    // old recovery file to the new path to keep compatibility

    // Simulate s1 is running on old version of Hadoop in which recovery file is in the NM local
    // dir.
    s1 = new YarnShuffleService
    s1.setRecoveryPath(new Path(yarnConfig.getTrimmedStrings(YarnConfiguration.NM_LOCAL_DIRS)(0)))
    // set auth to true to test the secrets recovery
    yarnConfig.setBoolean(SecurityManager.SPARK_AUTH_CONF, true)
    s1.init(yarnConfig)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data = makeAppInfo("user", app2Id)
    s1.initializeApplication(app2Data)

    assert(s1.secretManager.getSecretKey(app1Id.toString()) != null)
    assert(s1.secretManager.getSecretKey(app2Id.toString()) != null)

    val execStateFile = s1.registeredExecutorFile
    execStateFile should not be (null)
    val secretsFile = s1.secretsFile
    secretsFile should not be (null)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, SORT_MANAGER)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, SORT_MANAGER)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", blockResolver) should
      be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", blockResolver) should
      be (Some(shuffleInfo2))

    assert(execStateFile.exists(), s"$execStateFile did not exist")

    s1.stop()

    // Simulate s2 is running on Hadoop 2.5+ with NM recovery is enabled.
    assert(execStateFile.exists())
    val recoveryPath = new Path(recoveryLocalDir.toURI)
    s2 = new YarnShuffleService
    s2.setRecoveryPath(recoveryPath)
    s2.init(yarnConfig)

    // Ensure that s2 has loaded known apps from the secrets db.
    assert(s2.secretManager.getSecretKey(app1Id.toString()) != null)
    assert(s2.secretManager.getSecretKey(app2Id.toString()) != null)

    val execStateFile2 = s2.registeredExecutorFile
    val secretsFile2 = s2.secretsFile

    recoveryPath.toString should be (new Path(execStateFile2.getParentFile.toURI).toString)
    recoveryPath.toString should be (new Path(secretsFile2.getParentFile.toURI).toString)
    eventually(timeout(10 seconds), interval(5 millis)) {
      assert(!execStateFile.exists())
    }
    eventually(timeout(10 seconds), interval(5 millis)) {
      assert(!secretsFile.exists())
    }

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)

    // now we reinitialize only one of the apps, and expect yarn to tell us that app2 was stopped
    // during the restart
    // Since recovery file is got from old path, so the previous state should be stored.
    s2.initializeApplication(app1Data)
    s2.stopApplication(new ApplicationTerminationContext(app2Id))
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (None)

    s2.stop()
  }

  test("service throws error if cannot start") {
    // Set up a read-only local dir.
    val roDir = Utils.createTempDir()
    Files.setPosixFilePermissions(roDir.toPath(), EnumSet.of(OWNER_READ, OWNER_EXECUTE))

    // Try to start the shuffle service, it should fail.
    val service = new YarnShuffleService()
    service.setRecoveryPath(new Path(roDir.toURI))

    try {
      val error = intercept[ServiceStateException] {
        service.init(yarnConfig)
      }
      assert(error.getCause().isInstanceOf[IOException])
    } finally {
      service.stop()
      Files.setPosixFilePermissions(roDir.toPath(),
        EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE))
    }
  }

  private def makeAppInfo(user: String, appId: ApplicationId): ApplicationInitializationContext = {
    val secret = ByteBuffer.wrap(new Array[Byte](0))
    new ApplicationInitializationContext(user, appId, secret)
  }

  test("recovery db should not be created if NM recovery is not enabled") {
    s1 = new YarnShuffleService
    s1.init(yarnConfig)
    s1._recoveryPath should be (null)
    s1.registeredExecutorFile should be (null)
    s1.secretsFile should be (null)
  }

}
