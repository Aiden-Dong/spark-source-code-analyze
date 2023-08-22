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

package org.apache.spark.memory;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;

public class TaskMemoryManagerSuite {

  @Test
  public void leakedPageMemoryIsDetected() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MAX_VALUE,
        Long.MAX_VALUE,
        1),
      0);
    final MemoryConsumer c = new TestMemoryConsumer(manager);
    manager.allocatePage(4096, c);  // leak memory
    Assert.assertEquals(4096, manager.getMemoryConsumptionForThisTask());
    Assert.assertEquals(4096, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void encodePageNumberAndOffsetOffHeap() {
    final SparkConf conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1000");
    final TaskMemoryManager manager = new TaskMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.OFF_HEAP);
    final MemoryBlock dataPage = manager.allocatePage(256, c);
    // In off-heap mode, an offset is an absolute address that may require more than 51 bits to
    // encode. This test exercises that corner-case:
    final long offset = ((1L << TaskMemoryManager.OFFSET_BITS) + 10);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, offset);
    Assert.assertEquals(null, manager.getPage(encodedAddress));
    Assert.assertEquals(offset, manager.getOffsetInPage(encodedAddress));
    manager.freePage(dataPage, c);
  }

  @Test
  public void encodePageNumberAndOffsetOnHeap() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(new SparkConf().set("spark.memory.offHeap.enabled", "false")), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    final MemoryBlock dataPage = manager.allocatePage(256, c);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, 64);
    Assert.assertEquals(dataPage.getBaseObject(), manager.getPage(encodedAddress));
    Assert.assertEquals(64, manager.getOffsetInPage(encodedAddress));
  }

  @Test
  public void freeingPageSetsPageNumberToSpecialConstant() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(new SparkConf().set("spark.memory.offHeap.enabled", "false")), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    final MemoryBlock dataPage = manager.allocatePage(256, c);
    c.freePage(dataPage);
    Assert.assertEquals(MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER, dataPage.pageNumber);
  }

  @Test(expected = AssertionError.class)
  public void freeingPageDirectlyInAllocatorTriggersAssertionError() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(new SparkConf().set("spark.memory.offHeap.enabled", "false")), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    final MemoryBlock dataPage = manager.allocatePage(256, c);
    MemoryAllocator.HEAP.free(dataPage);
  }

  @Test(expected = AssertionError.class)
  public void callingFreePageOnDirectlyAllocatedPageTriggersAssertionError() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(new SparkConf().set("spark.memory.offHeap.enabled", "false")), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    final MemoryBlock dataPage = MemoryAllocator.HEAP.allocate(256);
    manager.freePage(dataPage, c);
  }

  @Test
  public void cooperativeSpilling() {
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager);
    c1.use(100);
    Assert.assertEquals(100, c1.getUsed());
    c2.use(100);
    Assert.assertEquals(100, c2.getUsed());
    Assert.assertEquals(0, c1.getUsed());  // spilled
    c1.use(100);
    Assert.assertEquals(100, c1.getUsed());
    Assert.assertEquals(0, c2.getUsed());  // spilled

    c1.use(50);
    Assert.assertEquals(50, c1.getUsed());  // spilled
    Assert.assertEquals(0, c2.getUsed());
    c2.use(50);
    Assert.assertEquals(50, c1.getUsed());
    Assert.assertEquals(50, c2.getUsed());

    c1.use(100);
    Assert.assertEquals(100, c1.getUsed());
    Assert.assertEquals(0, c2.getUsed());  // spilled

    c1.free(20);
    Assert.assertEquals(80, c1.getUsed());
    c2.use(10);
    Assert.assertEquals(80, c1.getUsed());
    Assert.assertEquals(10, c2.getUsed());
    c2.use(100);
    Assert.assertEquals(100, c2.getUsed());
    Assert.assertEquals(0, c1.getUsed());  // spilled

    c1.free(0);
    c2.free(100);
    Assert.assertEquals(0, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void cooperativeSpilling2() {
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c3 = new TestMemoryConsumer(manager);

    c1.use(20);
    Assert.assertEquals(20, c1.getUsed());
    c2.use(80);
    Assert.assertEquals(80, c2.getUsed());
    c3.use(80);
    Assert.assertEquals(20, c1.getUsed());  // c1: not spilled
    Assert.assertEquals(0, c2.getUsed());   // c2: spilled as it has required size of memory
    Assert.assertEquals(80, c3.getUsed());

    c2.use(80);
    Assert.assertEquals(20, c1.getUsed());  // c1: not spilled
    Assert.assertEquals(0, c3.getUsed());   // c3: spilled as it has required size of memory
    Assert.assertEquals(80, c2.getUsed());

    c3.use(10);
    Assert.assertEquals(0, c1.getUsed());   // c1: spilled as it has required size of memory
    Assert.assertEquals(80, c2.getUsed());  // c2: not spilled as spilling c1 already satisfies c3
    Assert.assertEquals(10, c3.getUsed());

    c1.free(0);
    c2.free(80);
    c3.free(10);
    Assert.assertEquals(0, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void shouldNotForceSpillingInDifferentModes() {
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager, MemoryMode.OFF_HEAP);
    c1.use(80);
    Assert.assertEquals(80, c1.getUsed());
    c2.use(80);
    Assert.assertEquals(20, c2.getUsed());  // not enough memory
    Assert.assertEquals(80, c1.getUsed());  // not spilled

    c2.use(10);
    Assert.assertEquals(10, c2.getUsed());  // spilled
    Assert.assertEquals(80, c1.getUsed());  // not spilled
  }

  @Test
  public void offHeapConfigurationBackwardsCompatibility() {
    // Tests backwards-compatibility with the old `spark.unsafe.offHeap` configuration, which
    // was deprecated in Spark 1.6 and replaced by `spark.memory.offHeap.enabled` (see SPARK-12251).
    final SparkConf conf = new SparkConf()
      .set("spark.unsafe.offHeap", "true")
      .set("spark.memory.offHeap.size", "1000");
    final TaskMemoryManager manager = new TaskMemoryManager(new TestMemoryManager(conf), 0);
    Assert.assertSame(MemoryMode.OFF_HEAP, manager.tungstenMemoryMode);
  }

}
