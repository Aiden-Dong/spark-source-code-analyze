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

package org.apache.spark.sql.execution.streaming

import scala.reflect.ClassTag

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType

package object state {

  implicit class StateStoreOps[T: ClassTag](dataRDD: RDD[T]) {

    /** Map each partition of an RDD along with data in a [[StateStore]]. */
    def mapPartitionsWithStateStore[U: ClassTag](
        sqlContext: SQLContext,
        stateInfo: StatefulOperatorStateInfo,
        keySchema: StructType,
        valueSchema: StructType,
        indexOrdinal: Option[Int])(
        storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U]): StateStoreRDD[T, U] = {

      mapPartitionsWithStateStore(
        stateInfo,
        keySchema,
        valueSchema,
        indexOrdinal,
        sqlContext.sessionState,
        Some(sqlContext.streams.stateStoreCoordinator))(
        storeUpdateFunction)
    }

    /** Map each partition of an RDD along with data in a [[StateStore]]. */
    private[streaming] def mapPartitionsWithStateStore[U: ClassTag](
        stateInfo: StatefulOperatorStateInfo,
        keySchema: StructType,
        valueSchema: StructType,
        indexOrdinal: Option[Int],
        sessionState: SessionState,
        storeCoordinator: Option[StateStoreCoordinatorRef])(
        storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U]): StateStoreRDD[T, U] = {

      val cleanedF = dataRDD.sparkContext.clean(storeUpdateFunction)
      val wrappedF = (store: StateStore, iter: Iterator[T]) => {
        // Abort the state store in case of error
        TaskContext.get().addTaskCompletionListener[Unit](_ => {
          if (!store.hasCommitted) store.abort()
        })
        cleanedF(store, iter)
      }

      new StateStoreRDD(
        dataRDD,
        wrappedF,
        stateInfo.checkpointLocation,
        stateInfo.queryRunId,
        stateInfo.operatorId,
        stateInfo.storeVersion,
        keySchema,
        valueSchema,
        indexOrdinal,
        sessionState,
        storeCoordinator)
    }
  }
}
