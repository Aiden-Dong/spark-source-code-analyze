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

package org.apache.spark.ui

import scala.xml.{Node, Text}

import org.apache.spark.SparkFunSuite

class UIUtilsSuite extends SparkFunSuite {
  import UIUtils._

  test("makeDescription(plainText = false)") {
    verify(
      """test <a href="/link"> text </a>""",
      <span class="description-input">test <a href="/link"> text </a></span>,
      "Correctly formatted text with only anchors and relative links should generate HTML",
      plainText = false
    )

    verify(
      """test <a href="/link" text </a>""",
      <span class="description-input">{"""test <a href="/link" text </a>"""}</span>,
      "Badly formatted text should make the description be treated as a string instead of HTML",
      plainText = false
    )

    verify(
      """test <a href="link"> text </a>""",
      <span class="description-input">{"""test <a href="link"> text </a>"""}</span>,
      "Non-relative links should make the description be treated as a string instead of HTML",
      plainText = false
    )

    verify(
      """test<a><img></img></a>""",
      <span class="description-input">{"""test<a><img></img></a>"""}</span>,
      "Non-anchor elements should make the description be treated as a string instead of HTML",
      plainText = false
    )

    verify(
      """test <a href="/link"> text </a>""",
      <span class="description-input">test <a href="base/link"> text </a></span>,
      baseUrl = "base",
      errorMsg = "Base URL should be prepended to html links",
      plainText = false
    )
  }

  test("makeDescription(plainText = true)") {
    verify(
      """test <a href="/link"> text </a>""",
      Text("test  text "),
      "Correctly formatted text with only anchors and relative links should generate a string " +
      "without any html tags",
      plainText = true
    )

    verify(
      """test <a href="/link"> text1 </a> <a href="/link"> text2 </a>""",
      Text("test  text1   text2 "),
      "Correctly formatted text with multiple anchors and relative links should generate a " +
      "string without any html tags",
      plainText = true
    )

    verify(
      """test <a href="/link"><span> text </span></a>""",
      Text("test  text "),
      "Correctly formatted text with nested anchors and relative links and/or spans should " +
      "generate a string without any html tags",
      plainText = true
    )

    verify(
      """test <a href="/link" text </a>""",
      Text("""test <a href="/link" text </a>"""),
      "Badly formatted text should make the description be as the same as the original text",
      plainText = true
    )

    verify(
      """test <a href="link"> text </a>""",
      Text("""test <a href="link"> text </a>"""),
      "Non-relative links should make the description be as the same as the original text",
      plainText = true
    )

    verify(
      """test<a><img></img></a>""",
      Text("""test<a><img></img></a>"""),
      "Non-anchor elements should make the description be as the same as the original text",
      plainText = true
    )
  }

  test("SPARK-11906: Progress bar should not overflow because of speculative tasks") {
    val generated = makeProgressBar(2, 3, 0, 0, Map.empty, 4).head.child.filter(_.label == "div")
    val expected = Seq(
      <div class="bar bar-completed" style="width: 75.0%"></div>,
      <div class="bar bar-running" style="width: 25.0%"></div>
    )
    assert(generated.sameElements(expected),
      s"\nRunning progress bar should round down\n\nExpected:\n$expected\nGenerated:\n$generated")
  }

  test("decodeURLParameter (SPARK-12708: Sorting task error in Stages Page when yarn mode.)") {
    val encoded1 = "%252F"
    val decoded1 = "/"
    val encoded2 = "%253Cdriver%253E"
    val decoded2 = "<driver>"

    assert(decoded1 === decodeURLParameter(encoded1))
    assert(decoded2 === decodeURLParameter(encoded2))

    // verify that no affect to decoded URL.
    assert(decoded1 === decodeURLParameter(decoded1))
    assert(decoded2 === decodeURLParameter(decoded2))
  }

  test("SPARK-20393: Prevent newline characters in parameters.") {
    val encoding = "Encoding:base64%0d%0a%0d%0aPGh0bWw%2bjcmlwdD48L2h0bWw%2b"
    val stripEncoding = "Encoding:base64PGh0bWw%2bjcmlwdD48L2h0bWw%2b"

    assert(stripEncoding === stripXSS(encoding))
  }

  test("SPARK-20393: Prevent script from parameters running on page.") {
    val scriptAlert = """>"'><script>alert(401)<%2Fscript>"""
    val stripScriptAlert = "&gt;&quot;&gt;&lt;script&gt;alert(401)&lt;%2Fscript&gt;"

    assert(stripScriptAlert === stripXSS(scriptAlert))
  }

  test("SPARK-20393: Prevent javascript from parameters running on page.") {
    val javascriptAlert =
      """app-20161208133404-0002<iframe+src%3Djavascript%3Aalert(1705)>"""
    val stripJavascriptAlert =
      "app-20161208133404-0002&lt;iframe+src%3Djavascript%3Aalert(1705)&gt;"

    assert(stripJavascriptAlert === stripXSS(javascriptAlert))
  }

  test("SPARK-20393: Prevent links from parameters on page.") {
    val link =
      """stdout'"><iframe+id%3D1131+src%3Dhttp%3A%2F%2Fdemo.test.net%2Fphishing.html>"""
    val stripLink =
      "stdout&quot;&gt;&lt;iframe+id%3D1131+src%3Dhttp%3A%2F%2Fdemo.test.net%2Fphishing.html&gt;"

    assert(stripLink === stripXSS(link))
  }

  test("SPARK-20393: Prevent popups from parameters on page.") {
    val popup = """stdout'%2Balert(60)%2B'"""
    val stripPopup = "stdout%2Balert(60)%2B"

    assert(stripPopup === stripXSS(popup))
  }

  private def verify(
      desc: String,
      expected: Node,
      errorMsg: String = "",
      baseUrl: String = "",
      plainText: Boolean): Unit = {
    val generated = makeDescription(desc, baseUrl, plainText)
    assert(generated.sameElements(expected),
      s"\n$errorMsg\n\nExpected:\n$expected\nGenerated:\n$generated")
  }
}
