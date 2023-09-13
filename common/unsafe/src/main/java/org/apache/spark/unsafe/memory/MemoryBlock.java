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

package org.apache.spark.unsafe.memory;

import javax.annotation.Nullable;

import org.apache.spark.unsafe.Platform;

/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 */
public class MemoryBlock extends MemoryLocation {

  /** 对于不是由TaskMemoryManagers分配的页面，有一个特殊的pageNumber值。 */
  public static final int NO_PAGE_NUMBER = -1;

  /**
   * 用于标记在TaskMemoryManager中已被释放的页面的特殊pageNumber值。
   * 我们在TaskMemoryManager.freePage()中将pageNumber设置为这个值，
   * 以便MemoryAllocator可以检测到TaskMemoryManager分配的页面是否在传递给MemoryAllocator.free()之前在TMM中被释放
   * （在TaskMemoryManager中分配页面然后直接在MemoryAllocator中释放它，而不经过TMM的freePage()调用，是一个错误）。
   */
  public static final int FREED_IN_TMM_PAGE_NUMBER = -2;

  /**
   * 用于标记已经被MemoryAllocator释放的页面的特殊pageNumber值。
   * 这允许我们检测到重复释放的情况。
   */
  public static final int FREED_IN_ALLOCATOR_PAGE_NUMBER = -3;

  private final long length;

  /**
   * 可选的页面号码；当这个MemoryBlock表示由TaskMemoryManager分配的页面时使用。
   * 这个字段是public的，以便TaskMemoryManager可以修改它，而TaskMemoryManager位于不同的包中。
   */
  public int pageNumber = NO_PAGE_NUMBER;

  /***
   * 内存块
   * @param obj      long[]
   * @param offset   当前偏移位置
   * @param length   空间大小
   */
  public MemoryBlock(@Nullable Object obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  /**
   * 返回当前块的空间大小.
   */
  public long size() {
    return length;
  }

  /**
   * 创建一个指向长整型数组使用的内存的内存块。.
   */
  public static MemoryBlock fromLongArray(final long[] array) {
    return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8L);
  }

  /**
   * 使用指定的字节值填充内存块.
   */
  public void fill(byte value) {
    Platform.setMemory(obj, offset, length, value);
  }
}
