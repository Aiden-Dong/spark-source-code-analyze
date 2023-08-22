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

package org.apache.spark.util.kvstore;

import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * A factory for array wrappers so that arrays can be used as keys in a map, sorted or not.
 *
 * The comparator implementation makes two assumptions:
 * - All elements are instances of Comparable
 * - When comparing two arrays, they both contain elements of the same type in corresponding
 *   indices.
 *
 * Otherwise, ClassCastExceptions may occur. The equality method can compare any two arrays.
 *
 * This class is not efficient and is mostly meant to compare really small arrays, like those
 * generally used as indices and keys in a KVStore.
 */
class ArrayWrappers {

  @SuppressWarnings("unchecked")
  public static Comparable<Object> forArray(Object a) {
    Preconditions.checkArgument(a.getClass().isArray());
    Comparable<?> ret;
    if (a instanceof int[]) {
      ret = new ComparableIntArray((int[]) a);
    } else if (a instanceof long[]) {
      ret = new ComparableLongArray((long[]) a);
    } else if (a instanceof byte[]) {
      ret = new ComparableByteArray((byte[]) a);
    } else {
      Preconditions.checkArgument(!a.getClass().getComponentType().isPrimitive());
      ret = new ComparableObjectArray((Object[]) a);
    }
    return (Comparable<Object>) ret;
  }

  private static class ComparableIntArray implements Comparable<ComparableIntArray> {

    private final int[] array;

    ComparableIntArray(int[] array) {
      this.array = array;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ComparableIntArray)) {
        return false;
      }
      return Arrays.equals(array, ((ComparableIntArray) other).array);
    }

    @Override
    public int hashCode() {
      int code = 0;
      for (int i = 0; i < array.length; i++) {
        code = (code * 31) + array[i];
      }
      return code;
    }

    @Override
    public int compareTo(ComparableIntArray other) {
      int len = Math.min(array.length, other.array.length);
      for (int i = 0; i < len; i++) {
        int diff = array[i] - other.array[i];
        if (diff != 0) {
          return diff;
        }
      }

      return array.length - other.array.length;
    }
  }

  private static class ComparableLongArray implements Comparable<ComparableLongArray> {

    private final long[] array;

    ComparableLongArray(long[] array) {
      this.array = array;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ComparableLongArray)) {
        return false;
      }
      return Arrays.equals(array, ((ComparableLongArray) other).array);
    }

    @Override
    public int hashCode() {
      int code = 0;
      for (int i = 0; i < array.length; i++) {
        code = (code * 31) + (int) array[i];
      }
      return code;
    }

    @Override
    public int compareTo(ComparableLongArray other) {
      int len = Math.min(array.length, other.array.length);
      for (int i = 0; i < len; i++) {
        long diff = array[i] - other.array[i];
        if (diff != 0) {
          return diff > 0 ? 1 : -1;
        }
      }

      return array.length - other.array.length;
    }
  }

  private static class ComparableByteArray implements Comparable<ComparableByteArray> {

    private final byte[] array;

    ComparableByteArray(byte[] array) {
      this.array = array;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ComparableByteArray)) {
        return false;
      }
      return Arrays.equals(array, ((ComparableByteArray) other).array);
    }

    @Override
    public int hashCode() {
      int code = 0;
      for (int i = 0; i < array.length; i++) {
        code = (code * 31) + array[i];
      }
      return code;
    }

    @Override
    public int compareTo(ComparableByteArray other) {
      int len = Math.min(array.length, other.array.length);
      for (int i = 0; i < len; i++) {
        int diff = array[i] - other.array[i];
        if (diff != 0) {
          return diff;
        }
      }

      return array.length - other.array.length;
    }
  }

  private static class ComparableObjectArray implements Comparable<ComparableObjectArray> {

    private final Object[] array;

    ComparableObjectArray(Object[] array) {
      this.array = array;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ComparableObjectArray)) {
        return false;
      }
      return Arrays.equals(array, ((ComparableObjectArray) other).array);
    }

    @Override
    public int hashCode() {
      int code = 0;
      for (int i = 0; i < array.length; i++) {
        code = (code * 31) + array[i].hashCode();
      }
      return code;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(ComparableObjectArray other) {
      int len = Math.min(array.length, other.array.length);
      for (int i = 0; i < len; i++) {
        int diff = ((Comparable<Object>) array[i]).compareTo((Comparable<Object>) other.array[i]);
        if (diff != 0) {
          return diff;
        }
      }

      return array.length - other.array.length;
    }
  }

}
