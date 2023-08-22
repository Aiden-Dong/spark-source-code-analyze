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

package org.apache.spark.deploy.history

import java.io.File

import org.mockito.AdditionalAnswers
import org.mockito.Matchers.{any, anyBoolean, anyLong, eq => meq}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.status.KVUtils
import org.apache.spark.util.{ManualClock, Utils}
import org.apache.spark.util.kvstore.KVStore

class HistoryServerDiskManagerSuite extends SparkFunSuite with BeforeAndAfter {

  import config._

  private val MAX_USAGE = 3L

  private var testDir: File = _
  private var store: KVStore = _

  before {
    testDir = Utils.createTempDir()
    store = KVUtils.open(new File(testDir, "listing"), "test")
  }

  after {
    store.close()
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
  }

  private def mockManager(): HistoryServerDiskManager = {
    val conf = new SparkConf().set(MAX_LOCAL_DISK_USAGE, MAX_USAGE)
    val manager = spy(new HistoryServerDiskManager(conf, testDir, store, new ManualClock()))
    doAnswer(AdditionalAnswers.returnsFirstArg[Long]()).when(manager)
      .approximateSize(anyLong(), anyBoolean())
    manager
  }

  test("leasing space") {
    val manager = mockManager()

    // Lease all available space.
    val leaseA = manager.lease(1)
    val leaseB = manager.lease(1)
    val leaseC = manager.lease(1)
    assert(manager.free() === 0)

    // Revert one lease, get another one.
    leaseA.rollback()
    assert(manager.free() > 0)
    assert(!leaseA.tmpPath.exists())

    val leaseD = manager.lease(1)
    assert(manager.free() === 0)

    // Committing B should bring the "used" space up to 4, so there shouldn't be space left yet.
    doReturn(2L).when(manager).sizeOf(meq(leaseB.tmpPath))
    val dstB = leaseB.commit("app2", None)
    assert(manager.free() === 0)
    assert(manager.committed() === 2)

    // Rollback C and D, now there should be 1 left.
    leaseC.rollback()
    leaseD.rollback()
    assert(manager.free() === 1)

    // Release app 2 to make it available for eviction.
    doReturn(2L).when(manager).sizeOf(meq(dstB))
    manager.release("app2", None)
    assert(manager.committed() === 2)

    // Emulate an updated event log by replacing the store for lease B. Lease 1, and commit with
    // size 3.
    val leaseE = manager.lease(1)
    doReturn(3L).when(manager).sizeOf(meq(leaseE.tmpPath))
    val dstE = leaseE.commit("app2", None)
    assert(dstE === dstB)
    assert(dstE.exists())
    doReturn(3L).when(manager).sizeOf(meq(dstE))
    assert(!leaseE.tmpPath.exists())
    assert(manager.free() === 0)
    manager.release("app2", None)
    assert(manager.committed() === 3)

    // Try a big lease that should cause the released app to be evicted.
    val leaseF = manager.lease(6)
    assert(!dstB.exists())
    assert(manager.free() === 0)
    assert(manager.committed() === 0)

    // Leasing when no free space is available should still be allowed.
    manager.lease(1)
    assert(manager.free() === 0)
  }

  test("tracking active stores") {
    val manager = mockManager()

    // Lease and commit space for app 1, making it active.
    val leaseA = manager.lease(2)
    assert(manager.free() === 1)
    doReturn(2L).when(manager).sizeOf(leaseA.tmpPath)
    assert(manager.openStore("appA", None).isEmpty)
    val dstA = leaseA.commit("appA", None)

    // Create a new lease. Leases are always granted, but this shouldn't cause app1's store
    // to be deleted.
    val leaseB = manager.lease(2)
    assert(dstA.exists())

    // Trying to commit on top of an active application should fail.
    intercept[IllegalArgumentException] {
      leaseB.commit("appA", None)
    }

    leaseB.rollback()

    // Close appA with an updated size, then create a new lease. Now the app's directory should be
    // deleted.
    doReturn(3L).when(manager).sizeOf(dstA)
    manager.release("appA", None)
    assert(manager.free() === 0)

    val leaseC = manager.lease(1)
    assert(!dstA.exists())
    leaseC.rollback()

    assert(manager.openStore("appA", None).isEmpty)
  }

  test("approximate size heuristic") {
    val manager = new HistoryServerDiskManager(new SparkConf(false), testDir, store,
      new ManualClock())
    assert(manager.approximateSize(50L, false) < 50L)
    assert(manager.approximateSize(50L, true) > 50L)
  }

}
