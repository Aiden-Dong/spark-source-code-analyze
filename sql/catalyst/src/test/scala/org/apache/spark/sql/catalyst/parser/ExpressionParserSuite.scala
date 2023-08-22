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
package org.apache.spark.sql.catalyst.parser

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, _}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Test basic expression parsing.
 * If the type of an expression is supported it should be tested here.
 *
 * Please note that some of the expressions test don't have to be sound expressions, only their
 * structure needs to be valid. Unsound expressions should be caught by the Analyzer or
 * CheckAnalysis classes.
 */
class ExpressionParserSuite extends PlanTest {
  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  val defaultParser = CatalystSqlParser

  def assertEqual(
      sqlCommand: String,
      e: Expression,
      parser: ParserInterface = defaultParser): Unit = {
    compareExpressions(parser.parseExpression(sqlCommand), e)
  }

  def intercept(sqlCommand: String, messages: String*): Unit = {
    val e = intercept[ParseException](defaultParser.parseExpression(sqlCommand))
    messages.foreach { message =>
      assert(e.message.contains(message))
    }
  }

  test("star expressions") {
    // Global Star
    assertEqual("*", UnresolvedStar(None))

    // Targeted Star
    assertEqual("a.b.*", UnresolvedStar(Option(Seq("a", "b"))))
  }

  // NamedExpression (Alias/Multialias)
  test("named expressions") {
    // No Alias
    val r0 = 'a
    assertEqual("a", r0)

    // Single Alias.
    val r1 = 'a as "b"
    assertEqual("a as b", r1)
    assertEqual("a b", r1)

    // Multi-Alias
    assertEqual("a as (b, c)", MultiAlias('a, Seq("b", "c")))
    assertEqual("a() (b, c)", MultiAlias('a.function(), Seq("b", "c")))

    // Numeric literals without a space between the literal qualifier and the alias, should not be
    // interpreted as such. An unresolved reference should be returned instead.
    // TODO add the JIRA-ticket number.
    assertEqual("1SL", Symbol("1SL"))

    // Aliased star is allowed.
    assertEqual("a.* b", UnresolvedStar(Option(Seq("a"))) as 'b)
  }

  test("binary logical expressions") {
    // And
    assertEqual("a and b", 'a && 'b)

    // Or
    assertEqual("a or b", 'a || 'b)

    // Combination And/Or check precedence
    assertEqual("a and b or c and d", ('a && 'b) || ('c && 'd))
    assertEqual("a or b or c and d", 'a || 'b || ('c && 'd))

    // Multiple AND/OR get converted into a balanced tree
    assertEqual("a or b or c or d or e or f", (('a || 'b) || 'c) || (('d || 'e) || 'f))
    assertEqual("a and b and c and d and e and f", (('a && 'b) && 'c) && (('d && 'e) && 'f))
  }

  test("long binary logical expressions") {
    def testVeryBinaryExpression(op: String, clazz: Class[_]): Unit = {
      val sql = (1 to 1000).map(x => s"$x == $x").mkString(op)
      val e = defaultParser.parseExpression(sql)
      assert(e.collect { case _: EqualTo => true }.size === 1000)
      assert(e.collect { case x if clazz.isInstance(x) => true }.size === 999)
    }
    testVeryBinaryExpression(" AND ", classOf[And])
    testVeryBinaryExpression(" OR ", classOf[Or])
  }

  test("not expressions") {
    assertEqual("not a", !'a)
    assertEqual("!a", !'a)
    assertEqual("not true > true", Not(GreaterThan(true, true)))
  }

  test("exists expression") {
    assertEqual(
      "exists (select 1 from b where b.x = a.x)",
      Exists(table("b").where(Symbol("b.x") === Symbol("a.x")).select(1)))
  }

  test("comparison expressions") {
    assertEqual("a = b", 'a === 'b)
    assertEqual("a == b", 'a === 'b)
    assertEqual("a <=> b", 'a <=> 'b)
    assertEqual("a <> b", 'a =!= 'b)
    assertEqual("a != b", 'a =!= 'b)
    assertEqual("a < b", 'a < 'b)
    assertEqual("a <= b", 'a <= 'b)
    assertEqual("a !> b", 'a <= 'b)
    assertEqual("a > b", 'a > 'b)
    assertEqual("a >= b", 'a >= 'b)
    assertEqual("a !< b", 'a >= 'b)
  }

  test("between expressions") {
    assertEqual("a between b and c", 'a >= 'b && 'a <= 'c)
    assertEqual("a not between b and c", !('a >= 'b && 'a <= 'c))
  }

  test("in expressions") {
    assertEqual("a in (b, c, d)", 'a in ('b, 'c, 'd))
    assertEqual("a not in (b, c, d)", !('a in ('b, 'c, 'd)))
  }

  test("in sub-query") {
    assertEqual(
      "a in (select b from c)",
      InSubquery(Seq('a), ListQuery(table("c").select('b))))

    assertEqual(
      "(a, b, c) in (select d, e, f from g)",
      InSubquery(Seq('a, 'b, 'c), ListQuery(table("g").select('d, 'e, 'f))))

    assertEqual(
      "(a, b) in (select c from d)",
      InSubquery(Seq('a, 'b), ListQuery(table("d").select('c))))

    assertEqual(
      "(a) in (select b from c)",
      InSubquery(Seq('a), ListQuery(table("c").select('b))))
  }

  test("like expressions") {
    assertEqual("a like 'pattern%'", 'a like "pattern%")
    assertEqual("a not like 'pattern%'", !('a like "pattern%"))
    assertEqual("a rlike 'pattern%'", 'a rlike "pattern%")
    assertEqual("a not rlike 'pattern%'", !('a rlike "pattern%"))
    assertEqual("a regexp 'pattern%'", 'a rlike "pattern%")
    assertEqual("a not regexp 'pattern%'", !('a rlike "pattern%"))
  }

  test("like expressions with ESCAPED_STRING_LITERALS = true") {
    val conf = new SQLConf()
    conf.setConfString(SQLConf.ESCAPED_STRING_LITERALS.key, "true")
    val parser = new CatalystSqlParser(conf)
    assertEqual("a rlike '^\\x20[\\x20-\\x23]+$'", 'a rlike "^\\x20[\\x20-\\x23]+$", parser)
    assertEqual("a rlike 'pattern\\\\'", 'a rlike "pattern\\\\", parser)
    assertEqual("a rlike 'pattern\\t\\n'", 'a rlike "pattern\\t\\n", parser)
  }

  test("is null expressions") {
    assertEqual("a is null", 'a.isNull)
    assertEqual("a is not null", 'a.isNotNull)
    assertEqual("a = b is null", ('a === 'b).isNull)
    assertEqual("a = b is not null", ('a === 'b).isNotNull)
  }

  test("is distinct expressions") {
    assertEqual("a is distinct from b", !('a <=> 'b))
    assertEqual("a is not distinct from b", 'a <=> 'b)
  }

  test("binary arithmetic expressions") {
    // Simple operations
    assertEqual("a * b", 'a * 'b)
    assertEqual("a / b", 'a / 'b)
    assertEqual("a DIV b", ('a / 'b).cast(LongType))
    assertEqual("a % b", 'a % 'b)
    assertEqual("a + b", 'a + 'b)
    assertEqual("a - b", 'a - 'b)
    assertEqual("a & b", 'a & 'b)
    assertEqual("a ^ b", 'a ^ 'b)
    assertEqual("a | b", 'a | 'b)

    // Check precedences
    assertEqual(
      "a * t | b ^ c & d - e + f % g DIV h / i * k",
      'a * 't | ('b ^ ('c & ('d - 'e + (('f % 'g / 'h).cast(LongType) / 'i * 'k)))))
  }

  test("unary arithmetic expressions") {
    assertEqual("+a", 'a)
    assertEqual("-a", -'a)
    assertEqual("~a", ~'a)
    assertEqual("-+~~a", -(~(~'a)))
  }

  test("cast expressions") {
    // Note that DataType parsing is tested elsewhere.
    assertEqual("cast(a as int)", 'a.cast(IntegerType))
    assertEqual("cast(a as timestamp)", 'a.cast(TimestampType))
    assertEqual("cast(a as array<int>)", 'a.cast(ArrayType(IntegerType)))
    assertEqual("cast(cast(a as int) as long)", 'a.cast(IntegerType).cast(LongType))
  }

  test("function expressions") {
    assertEqual("foo()", 'foo.function())
    assertEqual("foo.bar()",
      UnresolvedFunction(FunctionIdentifier("bar", Some("foo")), Seq.empty, isDistinct = false))
    assertEqual("foo(*)", 'foo.function(star()))
    assertEqual("count(*)", 'count.function(1))
    assertEqual("foo(a, b)", 'foo.function('a, 'b))
    assertEqual("foo(all a, b)", 'foo.function('a, 'b))
    assertEqual("foo(distinct a, b)", 'foo.distinctFunction('a, 'b))
    assertEqual("grouping(distinct a, b)", 'grouping.distinctFunction('a, 'b))
    assertEqual("`select`(all a, b)", 'select.function('a, 'b))
    intercept("foo(a x)", "extraneous input 'x'")
  }

  test("lambda functions") {
    assertEqual("x -> x + 1", LambdaFunction('x + 1, Seq('x.attr)))
    assertEqual("(x, y) -> x + y", LambdaFunction('x + 'y, Seq('x.attr, 'y.attr)))
  }

  test("window function expressions") {
    val func = 'foo.function(star())
    def windowed(
        partitioning: Seq[Expression] = Seq.empty,
        ordering: Seq[SortOrder] = Seq.empty,
        frame: WindowFrame = UnspecifiedFrame): Expression = {
      WindowExpression(func, WindowSpecDefinition(partitioning, ordering, frame))
    }

    // Basic window testing.
    assertEqual("foo(*) over w1", UnresolvedWindowExpression(func, WindowSpecReference("w1")))
    assertEqual("foo(*) over ()", windowed())
    assertEqual("foo(*) over (partition by a, b)", windowed(Seq('a, 'b)))
    assertEqual("foo(*) over (distribute by a, b)", windowed(Seq('a, 'b)))
    assertEqual("foo(*) over (cluster by a, b)", windowed(Seq('a, 'b)))
    assertEqual("foo(*) over (order by a desc, b asc)", windowed(Seq.empty, Seq('a.desc, 'b.asc)))
    assertEqual("foo(*) over (sort by a desc, b asc)", windowed(Seq.empty, Seq('a.desc, 'b.asc)))
    assertEqual("foo(*) over (partition by a, b order by c)", windowed(Seq('a, 'b), Seq('c.asc)))
    assertEqual("foo(*) over (distribute by a, b sort by c)", windowed(Seq('a, 'b), Seq('c.asc)))

    // Test use of expressions in window functions.
    assertEqual(
      "sum(product + 1) over (partition by ((product) + (1)) order by 2)",
      WindowExpression('sum.function('product + 1),
        WindowSpecDefinition(Seq('product + 1), Seq(Literal(2).asc), UnspecifiedFrame)))
    assertEqual(
      "sum(product + 1) over (partition by ((product / 2) + 1) order by 2)",
      WindowExpression('sum.function('product + 1),
        WindowSpecDefinition(Seq('product / 2 + 1), Seq(Literal(2).asc), UnspecifiedFrame)))
  }

  test("range/rows window function expressions") {
    val func = 'foo.function(star())
    def windowed(
        partitioning: Seq[Expression] = Seq.empty,
        ordering: Seq[SortOrder] = Seq.empty,
        frame: WindowFrame = UnspecifiedFrame): Expression = {
      WindowExpression(func, WindowSpecDefinition(partitioning, ordering, frame))
    }

    val frameTypes = Seq(("rows", RowFrame), ("range", RangeFrame))
    val boundaries = Seq(
      // No between combinations
      ("unbounded preceding", UnboundedPreceding, CurrentRow),
      ("2147483648 preceding", -Literal(2147483648L), CurrentRow),
      ("10 preceding", -Literal(10), CurrentRow),
      ("3 + 1 preceding", -Add(Literal(3), Literal(1)), CurrentRow),
      ("0 preceding", -Literal(0), CurrentRow),
      ("current row", CurrentRow, CurrentRow),
      ("0 following", Literal(0), CurrentRow),
      ("3 + 1 following", Add(Literal(3), Literal(1)), CurrentRow),
      ("10 following", Literal(10), CurrentRow),
      ("2147483649 following", Literal(2147483649L), CurrentRow),
      ("unbounded following", UnboundedFollowing, CurrentRow), // Will fail during analysis

      // Between combinations
      ("between unbounded preceding and 5 following",
        UnboundedPreceding, Literal(5)),
      ("between unbounded preceding and 3 + 1 following",
        UnboundedPreceding, Add(Literal(3), Literal(1))),
      ("between unbounded preceding and 2147483649 following",
        UnboundedPreceding, Literal(2147483649L)),
      ("between unbounded preceding and current row", UnboundedPreceding, CurrentRow),
      ("between 2147483648 preceding and current row", -Literal(2147483648L), CurrentRow),
      ("between 10 preceding and current row", -Literal(10), CurrentRow),
      ("between 3 + 1 preceding and current row", -Add(Literal(3), Literal(1)), CurrentRow),
      ("between 0 preceding and current row", -Literal(0), CurrentRow),
      ("between current row and current row", CurrentRow, CurrentRow),
      ("between current row and 0 following", CurrentRow, Literal(0)),
      ("between current row and 5 following", CurrentRow, Literal(5)),
      ("between current row and 3 + 1 following", CurrentRow, Add(Literal(3), Literal(1))),
      ("between current row and 2147483649 following", CurrentRow, Literal(2147483649L)),
      ("between current row and unbounded following", CurrentRow, UnboundedFollowing),
      ("between 2147483648 preceding and unbounded following",
        -Literal(2147483648L), UnboundedFollowing),
      ("between 10 preceding and unbounded following",
        -Literal(10), UnboundedFollowing),
      ("between 3 + 1 preceding and unbounded following",
        -Add(Literal(3), Literal(1)), UnboundedFollowing),
      ("between 0 preceding and unbounded following", -Literal(0), UnboundedFollowing),

      // Between partial and full range
      ("between 10 preceding and 5 following", -Literal(10), Literal(5)),
      ("between unbounded preceding and unbounded following",
        UnboundedPreceding, UnboundedFollowing)
    )
    frameTypes.foreach {
      case (frameTypeSql, frameType) =>
        boundaries.foreach {
          case (boundarySql, begin, end) =>
            val query = s"foo(*) over (partition by a order by b $frameTypeSql $boundarySql)"
            val expr = windowed(Seq('a), Seq('b.asc), SpecifiedWindowFrame(frameType, begin, end))
            assertEqual(query, expr)
        }
    }

    // We cannot use an arbitrary expression.
    intercept("foo(*) over (partition by a order by b rows exp(b) preceding)",
      "Frame bound value must be a literal.")
  }

  test("row constructor") {
    // Note that '(a)' will be interpreted as a nested expression.
    assertEqual("(a, b)", CreateStruct(Seq('a, 'b)))
    assertEqual("(a, b, c)", CreateStruct(Seq('a, 'b, 'c)))
    assertEqual("(a as b, b as c)", CreateStruct(Seq('a as 'b, 'b as 'c)))
  }

  test("scalar sub-query") {
    assertEqual(
      "(select max(val) from tbl) > current",
      ScalarSubquery(table("tbl").select('max.function('val))) > 'current)
    assertEqual(
      "a = (select b from s)",
      'a === ScalarSubquery(table("s").select('b)))
  }

  test("case when") {
    assertEqual("case a when 1 then b when 2 then c else d end",
      CaseKeyWhen('a, Seq(1, 'b, 2, 'c, 'd)))
    assertEqual("case (a or b) when true then c when false then d else e end",
      CaseKeyWhen('a || 'b, Seq(true, 'c, false, 'd, 'e)))
    assertEqual("case 'a'='a' when true then 1 end",
      CaseKeyWhen("a" ===  "a", Seq(true, 1)))
    assertEqual("case when a = 1 then b when a = 2 then c else d end",
      CaseWhen(Seq(('a === 1, 'b.expr), ('a === 2, 'c.expr)), 'd))
    assertEqual("case when (1) + case when a > b then c else d end then f else g end",
      CaseWhen(Seq((Literal(1) + CaseWhen(Seq(('a > 'b, 'c.expr)), 'd.expr), 'f.expr)), 'g))
  }

  test("dereference") {
    assertEqual("a.b", UnresolvedAttribute("a.b"))
    assertEqual("`select`.b", UnresolvedAttribute("select.b"))
    assertEqual("(a + b).b", ('a + 'b).getField("b")) // This will fail analysis.
    assertEqual(
      "struct(a, b).b",
      namedStruct(NamePlaceholder, 'a, NamePlaceholder, 'b).getField("b"))
  }

  test("reference") {
    // Regular
    assertEqual("a", 'a)

    // Starting with a digit.
    assertEqual("1a", Symbol("1a"))

    // Quoted using a keyword.
    assertEqual("`select`", 'select)

    // Unquoted using an unreserved keyword.
    assertEqual("columns", 'columns)
  }

  test("subscript") {
    assertEqual("a[b]", 'a.getItem('b))
    assertEqual("a[1 + 1]", 'a.getItem(Literal(1) + 1))
    assertEqual("`c`.a[b]", UnresolvedAttribute("c.a").getItem('b))
  }

  test("parenthesis") {
    assertEqual("(a)", 'a)
    assertEqual("r * (a + b)", 'r * ('a + 'b))
  }

  test("type constructors") {
    // Dates.
    assertEqual("dAte '2016-03-11'", Literal(Date.valueOf("2016-03-11")))
    intercept("DAtE 'mar 11 2016'")

    // Timestamps.
    assertEqual("tImEstAmp '2016-03-11 20:54:00.000'",
      Literal(Timestamp.valueOf("2016-03-11 20:54:00.000")))
    intercept("timestamP '2016-33-11 20:54:00.000'")

    // Binary.
    assertEqual("X'A'", Literal(Array(0x0a).map(_.toByte)))
    assertEqual("x'A10C'", Literal(Array(0xa1, 0x0c).map(_.toByte)))
    intercept("x'A1OC'")

    // Unsupported datatype.
    intercept("GEO '(10,-6)'", "Literals of type 'GEO' are currently not supported.")
  }

  test("literals") {
    def testDecimal(value: String): Unit = {
      assertEqual(value, Literal(BigDecimal(value).underlying))
    }

    // NULL
    assertEqual("null", Literal(null))

    // Boolean
    assertEqual("trUe", Literal(true))
    assertEqual("False", Literal(false))

    // Integral should have the narrowest possible type
    assertEqual("787324", Literal(787324))
    assertEqual("7873247234798249234", Literal(7873247234798249234L))
    testDecimal("78732472347982492793712334")

    // Decimal
    testDecimal("7873247234798249279371.2334")

    // Scientific Decimal
    testDecimal("9.0e1")
    testDecimal(".9e+2")
    testDecimal("0.9e+2")
    testDecimal("900e-1")
    testDecimal("900.0E-1")
    testDecimal("9.e+1")
    intercept(".e3")

    // Tiny Int Literal
    assertEqual("10Y", Literal(10.toByte))
    intercept("-1000Y", s"does not fit in range [${Byte.MinValue}, ${Byte.MaxValue}]")

    // Small Int Literal
    assertEqual("10S", Literal(10.toShort))
    intercept("40000S", s"does not fit in range [${Short.MinValue}, ${Short.MaxValue}]")

    // Long Int Literal
    assertEqual("10L", Literal(10L))
    intercept("78732472347982492793712334L",
        s"does not fit in range [${Long.MinValue}, ${Long.MaxValue}]")

    // Double Literal
    assertEqual("10.0D", Literal(10.0D))
    intercept("-1.8E308D", s"does not fit in range")
    intercept("1.8E308D", s"does not fit in range")

    // BigDecimal Literal
    assertEqual("90912830918230182310293801923652346786BD",
      Literal(BigDecimal("90912830918230182310293801923652346786").underlying()))
    assertEqual("123.0E-28BD", Literal(BigDecimal("123.0E-28").underlying()))
    assertEqual("123.08BD", Literal(BigDecimal("123.08").underlying()))
    intercept("1.20E-38BD", "decimal can only support precision up to 38")
  }

  test("strings") {
    Seq(true, false).foreach { escape =>
      val conf = new SQLConf()
      conf.setConfString(SQLConf.ESCAPED_STRING_LITERALS.key, escape.toString)
      val parser = new CatalystSqlParser(conf)

      // tests that have same result whatever the conf is
      // Single Strings.
      assertEqual("\"hello\"", "hello", parser)
      assertEqual("'hello'", "hello", parser)

      // Multi-Strings.
      assertEqual("\"hello\" 'world'", "helloworld", parser)
      assertEqual("'hello' \" \" 'world'", "hello world", parser)

      // 'LIKE' string literals. Notice that an escaped '%' is the same as an escaped '\' and a
      // regular '%'; to get the correct result you need to add another escaped '\'.
      // TODO figure out if we shouldn't change the ParseUtils.unescapeSQLString method?
      assertEqual("'pattern%'", "pattern%", parser)
      assertEqual("'no-pattern\\%'", "no-pattern\\%", parser)

      // tests that have different result regarding the conf
      if (escape) {
        // When SQLConf.ESCAPED_STRING_LITERALS is enabled, string literal parsing fallbacks to
        // Spark 1.6 behavior.

        // 'LIKE' string literals.
        assertEqual("'pattern\\\\%'", "pattern\\\\%", parser)
        assertEqual("'pattern\\\\\\%'", "pattern\\\\\\%", parser)

        // Escaped characters.
        // Unescape string literal "'\\0'" for ASCII NUL (X'00') doesn't work
        // when ESCAPED_STRING_LITERALS is enabled.
        // It is parsed literally.
        assertEqual("'\\0'", "\\0", parser)

        // Note: Single quote follows 1.6 parsing behavior when ESCAPED_STRING_LITERALS is enabled.
        val e = intercept[ParseException](parser.parseExpression("'\''"))
        assert(e.message.contains("extraneous input '''"))

        // The unescape special characters (e.g., "\\t") for 2.0+ don't work
        // when ESCAPED_STRING_LITERALS is enabled. They are parsed literally.
        assertEqual("'\\\"'", "\\\"", parser)   // Double quote
        assertEqual("'\\b'", "\\b", parser)     // Backspace
        assertEqual("'\\n'", "\\n", parser)     // Newline
        assertEqual("'\\r'", "\\r", parser)     // Carriage return
        assertEqual("'\\t'", "\\t", parser)     // Tab character

        // The unescape Octals for 2.0+ don't work when ESCAPED_STRING_LITERALS is enabled.
        // They are parsed literally.
        assertEqual("'\\110\\145\\154\\154\\157\\041'", "\\110\\145\\154\\154\\157\\041", parser)
        // The unescape Unicode for 2.0+ doesn't work when ESCAPED_STRING_LITERALS is enabled.
        // They are parsed literally.
        assertEqual("'\\u0057\\u006F\\u0072\\u006C\\u0064\\u0020\\u003A\\u0029'",
          "\\u0057\\u006F\\u0072\\u006C\\u0064\\u0020\\u003A\\u0029", parser)
      } else {
        // Default behavior

        // 'LIKE' string literals.
        assertEqual("'pattern\\\\%'", "pattern\\%", parser)
        assertEqual("'pattern\\\\\\%'", "pattern\\\\%", parser)

        // Escaped characters.
        // See: http://dev.mysql.com/doc/refman/5.7/en/string-literals.html
        assertEqual("'\\0'", "\u0000", parser) // ASCII NUL (X'00')
        assertEqual("'\\''", "\'", parser)     // Single quote
        assertEqual("'\\\"'", "\"", parser)    // Double quote
        assertEqual("'\\b'", "\b", parser)     // Backspace
        assertEqual("'\\n'", "\n", parser)     // Newline
        assertEqual("'\\r'", "\r", parser)     // Carriage return
        assertEqual("'\\t'", "\t", parser)     // Tab character
        assertEqual("'\\Z'", "\u001A", parser) // ASCII 26 - CTRL + Z (EOF on windows)

        // Octals
        assertEqual("'\\110\\145\\154\\154\\157\\041'", "Hello!", parser)

        // Unicode
        assertEqual("'\\u0057\\u006F\\u0072\\u006C\\u0064\\u0020\\u003A\\u0029'", "World :)",
          parser)
      }

    }
  }

  test("intervals") {
    def intervalLiteral(u: String, s: String): Literal = {
      Literal(CalendarInterval.fromSingleUnitString(u, s))
    }

    // Empty interval statement
    intercept("interval", "at least one time unit should be given for interval literal")

    // Single Intervals.
    val units = Seq(
      "year",
      "month",
      "week",
      "day",
      "hour",
      "minute",
      "second",
      "millisecond",
      "microsecond")
    val forms = Seq("", "s")
    val values = Seq("0", "10", "-7", "21")
    units.foreach { unit =>
      forms.foreach { form =>
         values.foreach { value =>
           val expected = intervalLiteral(unit, value)
           assertEqual(s"interval $value $unit$form", expected)
           assertEqual(s"interval '$value' $unit$form", expected)
         }
      }
    }

    // Hive nanosecond notation.
    assertEqual("interval 13.123456789 seconds", intervalLiteral("second", "13.123456789"))
    assertEqual("interval -13.123456789 second", intervalLiteral("second", "-13.123456789"))

    // Non Existing unit
    intercept("interval 10 nanoseconds", "No interval can be constructed")

    // Year-Month intervals.
    val yearMonthValues = Seq("123-10", "496-0", "-2-3", "-123-0")
    yearMonthValues.foreach { value =>
      val result = Literal(CalendarInterval.fromYearMonthString(value))
      assertEqual(s"interval '$value' year to month", result)
    }

    // Day-Time intervals.
    val datTimeValues = Seq(
      "99 11:22:33.123456789",
      "-99 11:22:33.123456789",
      "10 9:8:7.123456789",
      "1 0:0:0",
      "-1 0:0:0",
      "1 0:0:1")
    datTimeValues.foreach { value =>
      val result = Literal(CalendarInterval.fromDayTimeString(value))
      assertEqual(s"interval '$value' day to second", result)
    }

    // Unknown FROM TO intervals
    intercept("interval 10 month to second", "Intervals FROM month TO second are not supported.")

    // Composed intervals.
    assertEqual(
      "interval 3 months 22 seconds 1 millisecond",
      Literal(new CalendarInterval(3, 22001000L)))
    assertEqual(
      "interval 3 years '-1-10' year to month 3 weeks '1 0:0:2' day to second",
      Literal(new CalendarInterval(14,
        22 * CalendarInterval.MICROS_PER_DAY + 2 * CalendarInterval.MICROS_PER_SECOND)))
  }

  test("composed expressions") {
    assertEqual("1 + r.r As q", (Literal(1) + UnresolvedAttribute("r.r")).as("q"))
    assertEqual("1 - f('o', o(bar))", Literal(1) - 'f.function("o", 'o.function('bar)))
    intercept("1 - f('o', o(bar)) hello * world", "mismatched input '*'")
  }

  test("SPARK-17364, fully qualified column name which starts with number") {
    assertEqual("123_", UnresolvedAttribute("123_"))
    assertEqual("1a.123_", UnresolvedAttribute("1a.123_"))
    // ".123" should not be treated as token of type DECIMAL_VALUE
    assertEqual("a.123A", UnresolvedAttribute("a.123A"))
    // ".123E3" should not be treated as token of type SCIENTIFIC_DECIMAL_VALUE
    assertEqual("a.123E3_column", UnresolvedAttribute("a.123E3_column"))
    // ".123D" should not be treated as token of type DOUBLE_LITERAL
    assertEqual("a.123D_column", UnresolvedAttribute("a.123D_column"))
    // ".123BD" should not be treated as token of type BIGDECIMAL_LITERAL
    assertEqual("a.123BD_column", UnresolvedAttribute("a.123BD_column"))
  }

  test("SPARK-17832 function identifier contains backtick") {
    val complexName = FunctionIdentifier("`ba`r", Some("`fo`o"))
    assertEqual(complexName.quotedString, UnresolvedAttribute("`fo`o.`ba`r"))
    intercept(complexName.unquotedString, "mismatched input")
    // Function identifier contains countious backticks should be treated correctly.
    val complexName2 = FunctionIdentifier("ba``r", Some("fo``o"))
    assertEqual(complexName2.quotedString, UnresolvedAttribute("fo``o.ba``r"))
  }

  test("SPARK-19526 Support ignore nulls keywords for first and last") {
    assertEqual("first(a ignore nulls)", First('a, Literal(true)).toAggregateExpression())
    assertEqual("first(a)", First('a, Literal(false)).toAggregateExpression())
    assertEqual("last(a ignore nulls)", Last('a, Literal(true)).toAggregateExpression())
    assertEqual("last(a)", Last('a, Literal(false)).toAggregateExpression())
  }
}
