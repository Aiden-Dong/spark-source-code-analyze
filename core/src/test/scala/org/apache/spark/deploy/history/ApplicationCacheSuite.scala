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

import java.util.{Date, NoSuchElementException}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.mutable

import com.codahale.metrics.Counter
import org.eclipse.jetty.servlet.ServletContextHandler
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo => AttemptInfo, ApplicationInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.ManualClock

class ApplicationCacheSuite extends SparkFunSuite with Logging with MockitoSugar with Matchers {

  /**
   * Stub cache operations.
   * The state is kept in a map of [[CacheKey]] to [[CacheEntry]],
   * the `probeTime` field in the cache entry setting the timestamp of the entry
   */
  class StubCacheOperations extends ApplicationCacheOperations with Logging {

    /** map to UI instances, including timestamps, which are used in update probes */
    val instances = mutable.HashMap.empty[CacheKey, CacheEntry]

    /** Map of attached spark UIs */
    val attached = mutable.HashMap.empty[CacheKey, SparkUI]

    var getAppUICount = 0L
    var attachCount = 0L
    var detachCount = 0L
    var updateProbeCount = 0L

    override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
      logDebug(s"getAppUI($appId, $attemptId)")
      getAppUICount += 1
      instances.get(CacheKey(appId, attemptId)).map { e => e.loadedUI }
    }

    override def attachSparkUI(
        appId: String,
        attemptId: Option[String],
        ui: SparkUI,
        completed: Boolean): Unit = {
      logDebug(s"attachSparkUI($appId, $attemptId, $ui)")
      attachCount += 1
      attached += (CacheKey(appId, attemptId) -> ui)
    }

    def putAndAttach(
        appId: String,
        attemptId: Option[String],
        completed: Boolean,
        started: Long,
        ended: Long): LoadedAppUI = {
      val ui = putAppUI(appId, attemptId, completed, started, ended)
      attachSparkUI(appId, attemptId, ui.ui, completed)
      ui
    }

    def putAppUI(
        appId: String,
        attemptId: Option[String],
        completed: Boolean,
        started: Long,
        ended: Long): LoadedAppUI = {
      val ui = LoadedAppUI(newUI(appId, attemptId, completed, started, ended))
      instances(CacheKey(appId, attemptId)) = new CacheEntry(ui, completed)
      ui
    }

    /**
     * Detach a reconstructed UI
     *
     * @param ui Spark UI
     */
    override def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
      logDebug(s"detachSparkUI($appId, $attemptId, $ui)")
      detachCount += 1
      var name = ui.getAppName
      val key = CacheKey(appId, attemptId)
      attached.getOrElse(key, { throw new java.util.NoSuchElementException() })
      attached -= key
    }

    /**
     * Lookup from the internal cache of attached UIs
     */
    def getAttached(appId: String, attemptId: Option[String]): Option[SparkUI] = {
      attached.get(CacheKey(appId, attemptId))
    }

  }

  /**
   * Create a new UI. The info/attempt info classes here are from the package
   * `org.apache.spark.status.api.v1`, not the near-equivalents from the history package
   */
  def newUI(
      name: String,
      attemptId: Option[String],
      completed: Boolean,
      started: Long,
      ended: Long): SparkUI = {
    val info = new ApplicationInfo(name, name, Some(1), Some(1), Some(1), Some(64),
      Seq(new AttemptInfo(attemptId, new Date(started), new Date(ended),
        new Date(ended), ended - started, "user", completed, org.apache.spark.SPARK_VERSION)))
    val ui = mock[SparkUI]
    when(ui.getApplicationInfoList).thenReturn(List(info).iterator)
    when(ui.getAppName).thenReturn(name)
    when(ui.appName).thenReturn(name)
    val handler = new ServletContextHandler()
    when(ui.getHandlers).thenReturn(Seq(handler))
    ui
  }

  /**
   * Test operations on completed UIs: they are loaded on demand, entries
   * are removed on overload.
   *
   * This effectively tests the original behavior of the history server's cache.
   */
  test("Completed UI get") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(1)
    implicit val cache = new ApplicationCache(operations, 2, clock)
    val metrics = cache.metrics
    // cache misses
    val app1 = "app-1"
    assertNotFound(app1, None)
    assertMetric("lookupCount", metrics.lookupCount, 1)
    assertMetric("lookupFailureCount", metrics.lookupFailureCount, 1)
    assert(1 === operations.getAppUICount, "getAppUICount")
    assertNotFound(app1, None)
    assert(2 === operations.getAppUICount, "getAppUICount")
    assert(0 === operations.attachCount, "attachCount")

    val now = clock.getTimeMillis()
    // add the entry
    operations.putAppUI(app1, None, true, now, now)

    // make sure its local
    operations.getAppUI(app1, None).get
    operations.getAppUICount = 0
    // now expect it to be found
    cache.withSparkUI(app1, None) { _ => }
    // assert about queries made of the operations
    assert(1 === operations.getAppUICount, "getAppUICount")
    assert(1 === operations.attachCount, "attachCount")

    // and in the map of attached
    assert(operations.getAttached(app1, None).isDefined, s"attached entry '1' from $cache")

    // go forward in time
    clock.setTime(10)
    val time2 = clock.getTimeMillis()
    val cacheEntry2 = cache.get(app1)
    // no more refresh as this is a completed app
    assert(1 === operations.getAppUICount, "getAppUICount")
    assert(0 === operations.updateProbeCount, "updateProbeCount")
    assert(0 === operations.detachCount, "attachCount")

    // evict the entry
    operations.putAndAttach("2", None, true, time2, time2)
    operations.putAndAttach("3", None, true, time2, time2)
    cache.get("2")
    cache.get("3")

    // there should have been a detachment here
    assert(1 === operations.detachCount, s"detach count from $cache")
    // and entry app1 no longer attached
    assert(operations.getAttached(app1, None).isEmpty, s"get($app1) in $cache")
    val appId = "app1"
    val attemptId = Some("_01")
    val time3 = clock.getTimeMillis()
    operations.putAppUI(appId, attemptId, false, time3, 0)
    // expect an error here
    assertNotFound(appId, None)
  }

  test("Test that if an attempt ID is set, it must be used in lookups") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(1)
    implicit val cache = new ApplicationCache(operations, retainedApplications = 10,
      clock = clock)
    val appId = "app1"
    val attemptId = Some("_01")
    operations.putAppUI(appId, attemptId, false, clock.getTimeMillis(), 0)
    assertNotFound(appId, None)
  }

  /**
   * Test that incomplete apps are not probed for updates during the time window,
   * but that they are checked if that window has expired and they are not completed.
   * Then, if they have changed, the old entry is replaced by a new one.
   */
  test("Incomplete apps refreshed") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(50)
    implicit val cache = new ApplicationCache(operations, 5, clock)
    val metrics = cache.metrics
    // add the incomplete app
    // add the entry
    val started = clock.getTimeMillis()
    val appId = "app1"
    val attemptId = Some("001")
    val initialUI = operations.putAndAttach(appId, attemptId, false, started, 0)

    val firstUI = cache.withSparkUI(appId, attemptId) { ui => ui }
    assertMetric("lookupCount", metrics.lookupCount, 1)
    assert(0 === operations.updateProbeCount, "expected no update probe on that first get")

    // Invalidate the first entry to trigger a re-load.
    initialUI.invalidate()

    // Update the UI in the stub so that a new one is provided to the cache.
    operations.putAppUI(appId, attemptId, true, started, started + 10)

    val updatedUI = cache.withSparkUI(appId, attemptId) { ui => ui }
    assert(firstUI !== updatedUI, s"expected updated UI")
    assertMetric("lookupCount", metrics.lookupCount, 2)
    assert(1 === operations.detachCount, s"detach count")
  }

  /**
   * Assert that a metric counter has a specific value; failure raises an exception
   * including the cache's toString value
   * @param name counter name (for exceptions)
   * @param counter counter
   * @param expected expected value.
   * @param cache cache
   */
  def assertMetric(
      name: String,
      counter: Counter,
      expected: Long)
      (implicit cache: ApplicationCache): Unit = {
    val actual = counter.getCount
    if (actual != expected) {
      // this is here because Scalatest loses stack depth
      throw new Exception(s"Wrong $name value - expected $expected but got $actual in $cache")
    }
  }

  /**
   * Assert that a key wasn't found in cache or loaded.
   *
   * Looks for the specific nested exception raised by [[ApplicationCache]]
   * @param appId application ID
   * @param attemptId attempt ID
   * @param cache app cache
   */
  def assertNotFound(
      appId: String,
      attemptId: Option[String])
      (implicit cache: ApplicationCache): Unit = {
    val ex = intercept[NoSuchElementException] {
      cache.get(appId, attemptId)
    }
  }

  test("Large Scale Application Eviction") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(0)
    val size = 5
    // only two entries are retained, so we expect evictions to occur on lookups
    implicit val cache = new ApplicationCache(operations, retainedApplications = size,
      clock = clock)

    val attempt1 = Some("01")

    val ids = new mutable.ListBuffer[String]()
    // build a list of applications
    val count = 100
    for (i <- 1 to count ) {
      val appId = f"app-$i%04d"
      ids += appId
      clock.advance(10)
      val t = clock.getTimeMillis()
      operations.putAppUI(appId, attempt1, true, t, t)
    }
    // now go through them in sequence reading them, expect evictions
    ids.foreach { id =>
      cache.get(id, attempt1)
    }
    logInfo(cache.toString)
    val metrics = cache.metrics

    assertMetric("loadCount", metrics.loadCount, count)
    assertMetric("evictionCount", metrics.evictionCount, count - size)
}

  test("Attempts are Evicted") {
    val operations = new StubCacheOperations()
    implicit val cache = new ApplicationCache(operations, 4, new ManualClock())
    val metrics = cache.metrics
    val appId = "app1"
    val attempt1 = Some("01")
    val attempt2 = Some("02")
    val attempt3 = Some("03")
    operations.putAppUI(appId, attempt1, true, 100, 110)
    operations.putAppUI(appId, attempt2, true, 200, 210)
    operations.putAppUI(appId, attempt3, true, 300, 310)
    val attempt4 = Some("04")
    operations.putAppUI(appId, attempt4, true, 400, 410)
    val attempt5 = Some("05")
    operations.putAppUI(appId, attempt5, true, 500, 510)

    def expectLoadAndEvictionCounts(expectedLoad: Int, expectedEvictionCount: Int): Unit = {
      assertMetric("loadCount", metrics.loadCount, expectedLoad)
      assertMetric("evictionCount", metrics.evictionCount, expectedEvictionCount)
    }

    // first entry
    cache.get(appId, attempt1)
    expectLoadAndEvictionCounts(1, 0)

    // second
    cache.get(appId, attempt2)
    expectLoadAndEvictionCounts(2, 0)

    // no change
    cache.get(appId, attempt2)
    expectLoadAndEvictionCounts(2, 0)

    // eviction time
    cache.get(appId, attempt3)
    cache.size() should be(3)
    cache.get(appId, attempt4)
    expectLoadAndEvictionCounts(4, 0)
    cache.get(appId, attempt5)
    expectLoadAndEvictionCounts(5, 1)
    cache.get(appId, attempt5)
    expectLoadAndEvictionCounts(5, 1)

  }

  test("redirect includes query params") {
    val operations = new StubCacheOperations()
    val ui = operations.putAndAttach("foo", None, true, 0, 10)
    val cache = mock[ApplicationCache]
    when(cache.operations).thenReturn(operations)
    val filter = new ApplicationCacheCheckFilter(new CacheKey("foo", None), ui, cache)
    ui.invalidate()

    val request = mock[HttpServletRequest]
    when(request.getMethod()).thenReturn("GET")
    when(request.getRequestURI()).thenReturn("http://localhost:18080/history/local-123/jobs/job/")
    when(request.getQueryString()).thenReturn("id=2")
    val resp = mock[HttpServletResponse]
    when(resp.encodeRedirectURL(any())).thenAnswer(new Answer[String]() {
      override def answer(invocationOnMock: InvocationOnMock): String = {
        invocationOnMock.getArguments()(0).asInstanceOf[String]
      }
    })
    filter.doFilter(request, resp, null)
    verify(resp).sendRedirect("http://localhost:18080/history/local-123/jobs/job/?id=2")
  }

}
