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

package org.apache.spark.repl

import java.io.BufferedReader

// scalastyle:off println
import scala.Predef.{println => _, _}
// scalastyle:on println
import scala.concurrent.Future
import scala.reflect.classTag
import scala.reflect.io.File
import scala.tools.nsc.{GenericRunnerSettings, Properties}
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{isReplDebug, isReplPower, replProps}
import scala.tools.nsc.interpreter.{AbstractOrMissingHandler, ILoop, IMain, JPrintWriter}
import scala.tools.nsc.interpreter.{NamedParam, SimpleReader, SplashLoop, SplashReader}
import scala.tools.nsc.interpreter.StdReplTags.tagOfIMain
import scala.tools.nsc.util.stringFromStream
import scala.util.Properties.{javaVersion, javaVmName, versionNumberString, versionString}

/**
 *  A Spark-specific interactive shell.
 */
class SparkILoop(in0: Option[BufferedReader], out: JPrintWriter)
    extends ILoop(in0, out) {
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

  /**
   * TODO: Remove the following `override` when the support of Scala 2.11 is ended
   * Scala 2.11 has a bug of finding imported types in class constructors, extends clause
   * which is fixed in Scala 2.12 but never be back-ported into Scala 2.11.x.
   * As a result, we copied the fixes into `SparkILoopInterpreter`. See SPARK-22393 for detail.
   */
  override def createInterpreter(): Unit = {
    if (isScala2_11) {
      if (addedClasspath != "") {
        settings.classpath append addedClasspath
      }
      // scalastyle:off classforname
      // Have to use the default classloader to match the one used in
      // `classOf[Settings]` and `classOf[JPrintWriter]`.
      intp = Class.forName("org.apache.spark.repl.SparkILoopInterpreter")
        .getDeclaredConstructor(Seq(classOf[Settings], classOf[JPrintWriter]): _*)
        .newInstance(Seq(settings, out): _*)
        .asInstanceOf[IMain]
      // scalastyle:on classforname
    } else {
      super.createInterpreter()
    }
  }

  private val isScala2_11 = versionNumberString.startsWith("2.11")

  val initializationCommands: Seq[String] = Seq(
    """
    @transient val spark = if (org.apache.spark.repl.Main.sparkSession != null) {
        org.apache.spark.repl.Main.sparkSession
      } else {
        org.apache.spark.repl.Main.createSparkSession()
      }
    @transient val sc = {
      val _sc = spark.sparkContext
      if (_sc.getConf.getBoolean("spark.ui.reverseProxy", false)) {
        val proxyUrl = _sc.getConf.get("spark.ui.reverseProxyUrl", null)
        if (proxyUrl != null) {
          println(
            s"Spark Context Web UI is available at ${proxyUrl}/proxy/${_sc.applicationId}")
        } else {
          println(s"Spark Context Web UI is available at Spark Master Public URL")
        }
      } else {
        _sc.uiWebUrl.foreach {
          webUrl => println(s"Spark context Web UI available at ${webUrl}")
        }
      }
      println("Spark context available as 'sc' " +
        s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
      println("Spark session available as 'spark'.")
      _sc
    }
    """,
    "import org.apache.spark.SparkContext._",
    "import spark.implicits._",
    "import spark.sql",
    "import org.apache.spark.sql.functions._"
  )

  def initializeSpark(): Unit = {
    if (!intp.reporter.hasErrors) {
      // `savingReplayStack` removes the commands from session history.
      savingReplayStack {
        initializationCommands.foreach(intp quietRun _)
      }
    } else {
      throw new RuntimeException(s"Scala $versionString interpreter encountered " +
        "errors during initialization")
    }
  }

  /** Print a welcome message */
  override def printWelcome() {
    import org.apache.spark.SPARK_VERSION
    echo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  /** Available commands */
  override def commands: List[LoopCommand] = standardCommands

  override def resetCommand(line: String): Unit = {
    super.resetCommand(line)
    initializeSpark()
    echo("Note that after :reset, state of SparkSession and SparkContext is unchanged.")
  }

  override def replay(): Unit = {
    initializeSpark()
    super.replay()
  }

  /**
   * TODO: Remove `runClosure` when the support of Scala 2.11 is ended
   */
  private def runClosure(body: => Boolean): Boolean = {
    if (isScala2_11) {
      // In Scala 2.11, there is a bug that interpret could set the current thread's
      // context classloader, but fails to reset it to its previous state when returning
      // from that method. This is fixed in SI-8521 https://github.com/scala/scala/pull/5657
      // which is never back-ported into Scala 2.11.x. The following is a workaround fix.
      val original = Thread.currentThread().getContextClassLoader
      try {
        body
      } finally {
        Thread.currentThread().setContextClassLoader(original)
      }
    } else {
      body
    }
  }

  /**
   * The following code is mostly a copy of `process` implementation in `ILoop.scala` in Scala
   *
   * In newer version of Scala, `printWelcome` is the first thing to be called. As a result,
   * SparkUI URL information would be always shown after the welcome message.
   *
   * However, this is inconsistent compared with the existing version of Spark which will always
   * show SparkUI URL first.
   *
   * The only way we can make it consistent will be duplicating the Scala code.
   *
   * We should remove this duplication once Scala provides a way to load our custom initialization
   * code, and also customize the ordering of printing welcome message.
   */
  override def process(settings: Settings): Boolean = runClosure {

    def newReader = in0.fold(chooseReader(settings))(r => SimpleReader(r, out, interactive = true))

    /** Reader to use before interpreter is online. */
    def preLoop = {
      val sr = SplashReader(newReader) { r =>
        in = r
        in.postInit()
      }
      in = sr
      SplashLoop(sr, prompt)
    }

    /* Actions to cram in parallel while collecting first user input at prompt.
     * Run with output muted both from ILoop and from the intp reporter.
     */
    def loopPostInit(): Unit = mumly {
      // Bind intp somewhere out of the regular namespace where
      // we can get at it in generated code.
      intp.quietBind(NamedParam[IMain]("$intp", intp)(tagOfIMain, classTag[IMain]))

      // Auto-run code via some setting.
      ( replProps.replAutorunCode.option
        flatMap (f => File(f).safeSlurp())
        foreach (intp quietRun _)
        )
      // power mode setup
      if (isReplPower) enablePowerMode(true)
      initializeSpark()
      loadInitFiles()
      // SI-7418 Now, and only now, can we enable TAB completion.
      in.postInit()
    }
    def loadInitFiles(): Unit = settings match {
      case settings: GenericRunnerSettings =>
        for (f <- settings.loadfiles.value) {
          loadCommand(f)
          addReplay(s":load $f")
        }
        for (f <- settings.pastefiles.value) {
          pasteCommand(f)
          addReplay(s":paste $f")
        }
      case _ =>
    }
    // wait until after startup to enable noisy settings
    def withSuppressedSettings[A](body: => A): A = {
      val ss = this.settings
      import ss._
      val noisy = List(Xprint, Ytyperdebug)
      val noisesome = noisy.exists(!_.isDefault)
      val current = (Xprint.value, Ytyperdebug.value)
      if (isReplDebug || !noisesome) body
      else {
        this.settings.Xprint.value = List.empty
        this.settings.Ytyperdebug.value = false
        try body
        finally {
          Xprint.value = current._1
          Ytyperdebug.value = current._2
          intp.global.printTypings = current._2
        }
      }
    }
    def startup(): String = withSuppressedSettings {
      // let them start typing
      val splash = preLoop

      // while we go fire up the REPL
      try {
        // don't allow ancient sbt to hijack the reader
        savingReader {
          createInterpreter()
        }
        intp.initializeSynchronous()

        val field = classOf[ILoop].getDeclaredFields.filter(_.getName.contains("globalFuture")).head
        field.setAccessible(true)
        field.set(this, Future successful true)

        if (intp.reporter.hasErrors) {
          echo("Interpreter encountered errors during initialization!")
          null
        } else {
          loopPostInit()
          printWelcome()
          splash.start()

          val line = splash.line           // what they typed in while they were waiting
          if (line == null) {              // they ^D
            try out print Properties.shellInterruptedString
            finally closeInterpreter()
          }
          line
        }
      } finally splash.stop()
    }

    this.settings = settings
    startup() match {
      case null => false
      case line =>
        try loop(line) match {
          case LineResults.EOF => out print Properties.shellInterruptedString
          case _ =>
        }
        catch AbstractOrMissingHandler()
        finally closeInterpreter()
        true
    }
  }
}

object SparkILoop {

  /**
   * Creates an interpreter loop with default settings and feeds
   * the given code to it as input.
   */
  def run(code: String, sets: Settings = new Settings): String = {
    import java.io.{ BufferedReader, StringReader, OutputStreamWriter }

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val input = new BufferedReader(new StringReader(code))
        val output = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl = new SparkILoop(input, output)

        if (sets.classpath.isDefault) {
          sets.classpath.value = sys.props("java.class.path")
        }
        repl process sets
      }
    }
  }
  def run(lines: List[String]): String = run(lines.map(_ + "\n").mkString)
}
