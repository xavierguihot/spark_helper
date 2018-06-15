package com.spark_helper

import com.spark_helper.monitoring.Test

import java.util.Calendar

import org.apache.commons.lang3.time.DurationFormatUtils

/** A logger dedicated to Spark jobs.
  *
  * It's a simple logger/report which contains a report that one can update from
  * the driver and a success state. The idea is to persist job executions logs
  * and errors (and forget about grepping unreadable yarn logs).
  *
  * It's designed for periodic spark jobs (handles storage and purge of logs)
  * and provides a way to handle kpis validation.
  *
  * Logs are stored on the go which means one can have a direct real time access
  * of the job logs/status and it's current state (which can otherwise be a pain
  * if it means going through yarn logs, or even for certain production
  * environments going through additional layers of software logs to get to yarn
  * logs).
  *
  * One of the issues this logger aims at tackling is the handling of exceptions
  * reported from executors. An exception within a Spark pipeline doesn't always
  * mean one want to make the job fail. Or if it's the case, one might still
  * want to perform a few actions before letting the job crash. The idea is thus
  * to surround (driver side) a Spark pipeline within a try catch and redirect
  * the exception to the logger for a clean logging.
  *
  * This is a "driver-only" logger and is not intended at logging concurrent
  * actions from executors.
  *
  * Produced reports can easily be inserted in a notification email whenever
  * the job fails, which saves a lot of time to maintainers operating on heavy
  * production environments.
  *
  * The produced persisted report is also a way for downstream jobs to know the
  * status of their input data.
  *
  * Let's go through a simple Spark job example monitored with this Monitor
  * facility:
  *
  * {{{
  * Monitor.setTitle("My job title")
  * Monitor.addDescription(
  *   "My job description (whatever you want); for instance:\n" +
  *   "Documentation: https://github.com/xavierguihot/spark_helper")
  * Monitor.setLogFolder("path/to/log/folder")
  *
  * try {
  *
  *   // Let's perform a spark pipeline which might go wrong:
  *   val processedData = sc.textFile("file.json").map(/**whatever*/)
  *
  *   // Let's say you want to get some KPIs on your output before storing it:
  *   val outputIsValid = Monitor.kpis(
  *     List(
  *       Test("Nbr of output records", processedData.count(), SUPERIOR_THAN, 10e6d, NBR),
  *       Test("Some pct of invalid output", your_complex_kpi, INFERIOR_THAN, 3, PCT)
  *     ),
  *     "My pipeline description"
  *   )
  *
  *   if (outputIsValid)
  *     processedData.saveAsTextFile("wherever.csv")
  *
  * } catch {
  *   case iie: InvalidInputException =>
  *     Monitor.error(iie, "My pipeline description", diagnostic = "No input data!")
  *   case e: Throwable =>
  *     Monitor.error(e, "My pipeline description") // whatever unexpected error
  * }
  *
  * if (Monitor.isSuccess()) {
  *   val doMore = "Let's do some more stuff!"
  *   Monitor.log("My second pipeline description: success")
  * }
  *
  * // At the end of the different steps of the job, we can store the report in
  * // HDFS (this saves the logs in the folder set with Monitor.setLogFolder):
  * Monitor.store()
  *
  * // At the end of the job, if the job isn't successful, you might want to
  * // crash it (for instance to get a notification from your scheduler):
  * if (!Monitor.isSuccess()) throw new Exception() // or send an email, or ...
  * }}}
  *
  * At any time during the job, logs can be accessed from file
  * path/to/log/folder/current.ongoing
  *
  * If we were to read the stored report after this simple pipeline, here are
  * some possible reports:
  *
  * First scenario, problem with the input of the job:
  * {{{
  *           My job title
  *
  * My job description (whatever you want); for instance:
  * Documentation: https://github.com/xavierguihot/spark_helper
  * [10:23] Beginning
  * [10:23-10:23] My pipeline description: failed
  *   Diagnostic: No input data!
  *     org.apache.hadoop.mapred.InvalidInputException: Input path does not exist: hdfs://my/hdfs/input/path
  *     at org.apache.hadoop.mapred.FileInputFormat.singleThreadedListStatus(FileInputFormat.java:285)
  *     at org.apache.hadoop.mapred.FileInputFormat.listStatus(FileInputFormat.java:228)
  *     ...
  * [10:23] Duration: 00:00:00
  * }}}
  *
  * Another scenario, unexpected problem:
  * {{{
  *           My job title
  *
  * My job description (whatever you want); for instance:
  * Documentation: https://github.com/xavierguihot/spark_helper
  * [10:23] Beginning
  * [10:23-10:36] My pipeline description: failed
  *     java.lang.NumberFormatException: For input string: "a"
  *     java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
  *     java.lang.Integer.parseInt(Integer.java:492)
  *     ...
  * [10:36] Duration: 00:13:47
  * }}}
  *
  * Another scenario, successful spark pipeline and KPIs are valid; all good!:
  * {{{
  *           My job title
  *
  * My job description (whatever you want); for instance:
  * Documentation: https://github.com/xavierguihot/spark_helper
  * [10:23] Beginning
  * [10:23-10:41] My pipeline description: success
  *   KPI: Nbr of output records
  *     Value: 14669071.0
  *     Must be superior than 10000000.0
  *     Validated: true
  *   KPI: Some pct of invalid output
  *     Value: 0.06%
  *     Must be inferior than 3.0%
  *     Validated: true
  * [10:41-10:42] My second pipeline description: success
  * [10:42] Duration: 00:19:23
  * }}}
  *
  * Source <a href="https://github.com/xavierguihot/spark_helper/blob/master/src
  * /main/scala/com/spark_helper/monitoring/Monitor.scala">Monitor</a>
  *
  * @todo would a State monad be appropriate?
  * @author Xavier Guihot
  * @since 2017-02
  */
object Monitor {

  private var reportTitle: Option[String] = None
  private var pointsOfContact: Option[List[String]] = None
  private var reportDescription: Option[String] = None
  private var logDirectory: Option[String] = None
  private var purgeWindow: Option[Int] = None

  private val jobStart = DateHelper.now("[HH:mm]") + " Beginning"

  // Join of reportTitle, pointsOfContact, reportDescription, logDirectory and
  // jobStart:
  private var reportHeader = buildReportHeader()

  private val beginning = Calendar.getInstance().getTimeInMillis
  private var lastReportUpdate = DateHelper.now("HH:mm")

  /** Sets the report's title.
    *
    * This will be the first line of the report:
    *
    * {{{
    * // Using:
    * Monitor.setReportTitle("My Simple Job")
    * // Produces this at the beginning of the report:
    * "          My Simple Job"
    * ""
    * }}}
    *
    * @param title the title of the report
    */
  def setTitle(title: String): Unit = {
    reportTitle = Some(title)
    reportHeader = buildReportHeader()
    storeCurrent()
  }

  /** Sets the report's contact list.
    *
    * This will appear within the first lines of the report:
    *
    * {{{
    * // Using:
    * Monitor.setReportTitle("My Simple Job")
    * Monitor.addContacts(List("x.guihot@gmail.com", "smbdy@gmail.com"))
    * // Produces this at the beginning of the report:
    * "          My Simple Job"
    * ""
    * "Point of contact: x.guihot@gmail.com, smbdy@gmail.com"
    * }}}
    *
    * @param contacts the list of points of contact
    */
  def addContacts(contacts: List[String]): Unit = {
    pointsOfContact = Some(contacts)
    reportHeader = buildReportHeader()
    storeCurrent()
  }

  /** Sets the report's description.
    *
    * This will appear within the first lines of the report:
    *
    * {{{
    * // Using:
    * Monitor.setReportTitle("My Simple Job")
    * Monitor.addDescription("Documentation: https://github.com/xavierguihot/spark_helper")
    * // Produces this at the beginning of the report:
    * "          My Simple Job"
    * ""
    * "Documentation: https://github.com/xavierguihot/spark_helper"
    * }}}
    *
    * @param description the description of the Spark job (or whatever)
    */
  def addDescription(description: String): Unit = {
    reportDescription = Some(description)
    reportHeader = buildReportHeader()
    storeCurrent()
  }

  /** Sets the folder in which logs are stored.
    *
    * Reports are persisted with this format:
    * {{{
    * src/test/resources/logs/20170326_110643.log.failed
    * src/test/resources/logs/20170327_105423.log.success
    * src/test/resources/logs/current.success
    * }}}
    *
    * where current.success actually contains same logs a
    * {{{ 20170327_105423.log.success }}}
    *
    * During the execution of the job, logs are persisted and can be accessed
    * from:
    * {{{ src/test/resources/logs/current.ongoing }}}
    *
    * @param logFolder the folder in which reports are stored
    */
  def setLogFolder(logFolder: String): Unit = {
    logDirectory = Some(logFolder)
    prepareLogFolder()
    storeCurrent()
  }

  /** Activates the purge of logs and sets the purge window.
    *
    * @param window the nbr of days of logs retention
    */
  def withPurge(window: Int): Unit = purgeWindow = Some(window)

  private var successful = true
  private var report = ""

  /** Returns if at that point of the job, all previous stages were successful.
    *
    * @return if your spark job is successful.
    */
  def isSuccess: Boolean = successful

  /** Returns the current state of the monitoring report.
    *
    * @return the report.
    */
  def logs(): String = s"$reportHeader\n$report"

  /** Updates the report with some text.
    *
    * Using this method like this:
    * {{{ monitor.log("Some text") }}}
    * will result in this to be appended to the report:
    * {{{ "[10:35-10:37] Some text\n" }}}
    *
    * @param text the text to append to the report
    */
  def log(text: String): Unit = log(text, withTimestamp = true)

  /** Updates the report with some text and a success.
    *
    * If the status of the monitoring was success, then it stays success. If it
    * was failure, then it stays a failure.
    *
    * Using this method like this:
    * {{{ monitor.success("Some text") }}}
    * will result in this to be appended to the report:
    * {{{ "[10:35-10:37] Some text: success\n" }}}
    *
    * @param taskDescription the text to append to the report
    * @return true since it's a success
    */
  def success(taskDescription: String): Boolean = {
    log(s"$taskDescription: success")
    true
  }

  /** Updates the report with some text and a failure.
    *
    * This sets the status of the monitoring to false. After that the status
    * will never be success again, even if you update the report with success().
    *
    * Using this method like this:
    * {{{ monitor.error("Some text") }}}
    * will result in this to be appended to the report:
    * {{{ "[10:35-10:37] Some text: failure\n" }}}
    *
    * Once the monitoring is a failure, then whatever following successful
    * action won't change the failed status of the monitoring.
    *
    * @param taskDescription the text to append to the report
    * @return false since it's a failure
    */
  def error(taskDescription: String): Boolean = {
    log(s"$taskDescription: failed")
    successful = false
    false
  }

  /** Updates the report with the stack trace of an error.
    *
    * This sets the status of the monitoring to false. After that the status
    * will never be success again, even if you update the report with success
    * tasks.
    *
    * The main idea is to surround a Spark pipeline on the driver in order to
    * catch whatever exception from executors and thus log the exact error while
    * still being able to keep on with the job or end it properly.
    *
    * Catching an error like this:
    * {{{
    * monitor.error(
    *   invalidInputException,
    *   "My pipeline description",
    *   diagnostic = "No input data!")
    * }}}
    * will result in this to be appended to the report:
    * {{{
    * [10:23-10:24] My pipeline description: failed
    *   Diagnostic: No input data!
    *     org.apache.hadoop.mapred.InvalidInputException: Input path does not exist: hdfs://my/hdfs/input/path
    *     at org.apache.hadoop.mapred.FileInputFormat.singleThreadedListStatus(FileInputFormat.java:285)
    *     at org.apache.hadoop.mapred.FileInputFormat.listStatus(FileInputFormat.java:228)
    *     ...
    * }}}
    *
    * @param exception/error the thrown exception/error
    * @param taskDescription the description of the step which failed
    * @param diagnostic (optional) the message one want to add to clarify the
    * source of the problem.
    * @return false since it's a failure
    */
  def error(
      exception: Throwable,
      taskDescription: String,
      diagnostic: String = ""
  ): Boolean = {

    successful = false

    val serializedException =
      "\t\t" + exception.toString + "\n" +
        exception.getStackTrace.map(line => s"\t\t$line").mkString("\n")

    val update = List(
      if (taskDescription.nonEmpty) s"$taskDescription: failed\n" else "",
      if (diagnostic.nonEmpty) s"\tDiagnostic: $diagnostic\n" else "",
      serializedException + "\n"
    ).mkString("")

    log(update)

    false
  }

  /** Updates the report with the validation of a list of kpis/tests.
    *
    * By providing a list of [[com.spark_helper.monitoring.Test]] objects to
    * validate against thresholds, the report is updated with a detailed result
    * of the validation and the success status of the monitoring is set to false
    * if at least one if the tested KPIs isn't valid.
    *
    * If the validation of tests is a failure then after that the status will
    * never be success again, even if you update the report with success().
    *
    * Using this method like this:
    * {{{
    * monitor.kpis(
    *   List(
    *     Test("pctOfWhatever", 0.06d, INFERIOR_THAN, 0.1d, PCT),
    *     Test("pctOfSomethingElse", 0.27d, SUPERIOR_THAN, 0.3d, PCT),
    *     Test("someNbr", 1235d, EQUAL_TO, 1235d, NBR)
    *   ),
    *   "Tests for whatever"
    * )
    * }}}
    * will result in this to be appended to the report:
    * {{{
    * [10:35-10:37] Tests for whatever: failed
    *   KPI: pctOfWhatever
    *     Value: 0.06%
    *     Must be inferior than 0.1%
    *     Validated: true
    *   KPI: pctOfSomethingElse
    *     Value: 0.27%
    *     Must be superior than 0.3%
    *     Validated: false
    *   KPI: someNbr
    *     Value: 1235.0
    *     Must be equal to 1235.0
    *     Validated: true
    * }}}
    *
    * @param tests the list of Test objects to validate
    * @param testSuitName the description of the task being tested
    * @return if all tests were successful
    */
  def kpis(tests: List[Test], testSuitName: String = ""): Boolean = {

    val testsAreValid = tests.forall(_.isSuccess)

    if (!testsAreValid)
      successful = false

    val serializedTests = tests.mkString("\n")

    val update = testSuitName match {
      case "" => serializedTests
      case _ =>
        val status = if (testsAreValid) "success" else "failed"
        s"$testSuitName: $status\n$serializedTests"
    }

    log(update)

    testsAreValid
  }

  /** Updates the report by the validation of a single kpi.
    *
    * By providing a [[com.spark_helper.monitoring.Test]] object to validate
    * against a threshold, the report is updated with a detailed result of the
    * validation and the success status of the monitoring is set to false if the
    * KPI isn't valid.
    *
    * If the validation is a failure then after that the status will never be
    * success again, even if you update the report with success tasks.
    *
    * Using this method like this:
    * {{{
    * monitor.kpi(
    *   Test("pctOfWhatever", 0.06d, INFERIOR_THAN, 0.1d, PCT),
    *   "Tests for whatever")
    * }}}
    * will result in this to be appended to the report:
    * {{{
    * [10:35-10:37] Tests for whatever: success
    *   KPI: pctOfWhatever
    *     Value: 0.06%
    *     Must be inferior than 0.1%
    *     Validated: true
    * }}}
    *
    * @param test the Test object to validate
    * @param testSuitName the description of the task being tested
    * @return if the test is successful
    */
  def kpi(test: Test, testSuitName: String = ""): Boolean =
    kpis(List(test), testSuitName)

  /** Persists the report as a text file.
    *
    * This report will be stored in the folder provided with
    * <span style="background-color: #eff0f1">Monitor.setLogFolder()</span> and
    * its name will be either <span style="background-color: #eff0f1">
    * yyyyMMdd_HHmmss.log.success</span> or
    * <span style="background-color: #eff0f1">yyyyMMdd_HHmmss.log.failed</span>
    * depending on the monitoring status.
    *
    * In addition to storing the report with a timestamp-based name, it is also
    * stored under the name <span style="background-color: #eff0f1">
    * current.success</span> or <span style="background-color: #eff0f1">
    * current.failed</span> in the same folder in order to give it a fixed name
    * for downstream projects to look for. Obviously if the new status is
    * success, and the previous was failed, the previous current.failed file is
    * deleted and vis et versa.
    *
    * For high frequency jobs, it might be good not to keep all logs
    * indefinitely. One activate a purge of logs with
    * <span style="background-color: #eff0f1">Monitor.withPurge(nbrOfDays: Int)
    * </span>.
    */
  def store(): Unit = {

    logDirectory match {

      case Some(logFolder) =>
        // We add the job duration to the report:
        val jobDuration = DurationFormatUtils.formatDuration(
          Calendar.getInstance().getTimeInMillis - beginning,
          "HH:mm:ss")

        var now = DateHelper.now("[HH:mm]")

        val finalReport = s"$reportHeader\n$report$now Duration: $jobDuration"

        // The extension of the report depending on the success:
        val reportExtension = if (isSuccess) "success" else "failed"

        now = DateHelper.now("yyyyMMdd_HHmmss")

        prepareLogFolder()
        // And we store the file as a simple text file with a name based on the
        // timestamp:
        HdfsHelper
          .writeToHdfsFile(finalReport, s"$logFolder/$now.log.$reportExtension")
        // And we also store it with a fixed name: current.success:
        HdfsHelper
          .writeToHdfsFile(finalReport, s"$logFolder/current.$reportExtension")

        purgeWindow.foreach(window => purgeOutdatedLogs(logFolder, window))

      case None =>
        require(
          logDirectory.nonEmpty,
          "to save the report, please specify the log folder using " +
            "Monitor.setLogFolder(\"hdfs/path/to/log/folder\")")
    }
  }

  /** Clean-up previous current.success, current.failed and current.ongoing
    * log files */
  private def prepareLogFolder(): Unit =
    logDirectory.foreach(logFolder => {
      HdfsHelper.deleteFile(s"$logFolder/current.success")
      HdfsHelper.deleteFile(s"$logFolder/current.failed")
      HdfsHelper.deleteFile(s"$logFolder/current.ongoing")
    })

  private def buildReportHeader(): String =
    List(
      reportTitle.map(title => s"\t\t\t\t\t$title\n"),
      pointsOfContact
        .map(contacts => "Point of contact: " + contacts.mkString(", ")),
      reportDescription,
      Some(jobStart)
    ).flatten
      .mkString("\n")

  private def log(text: String, withTimestamp: Boolean): Unit = {

    val before = lastReportUpdate
    val now = DateHelper.now("HH:mm")

    lastReportUpdate = now

    val update = if (withTimestamp) s"[$before-$now] $text" else text

    report += s"$update\n"

    // We print the update to also have it within yarn logs:
    println(s"MONITOR: $update")

    // And if the logFolder parameter has been set, we also update live the log
    // file:
    storeCurrent()
  }

  /** Updates the current stored version of logs in file
    * logFolder/current.ongoing */
  private def storeCurrent(): Unit =
    logDirectory.foreach { logFolder =>
      val warning =
        "WARNING: If this file exists it does not necessarily mean that " +
          "your job is still running. This file might persist if your job " +
          "has been killed and thus couldn't reach your call to the " +
          "Monitor.store()."

      val ongoingReport =
        s"$reportHeader\n$report\n$warning"

      HdfsHelper.writeToHdfsFile(ongoingReport, s"$logFolder/current.ongoing")
    }

  private def purgeOutdatedLogs(logFolder: String, window: Int): Unit = {

    val nDaysAgo = DateHelper.nDaysBefore(window, "yyyyMMdd")

    if (HdfsHelper.folderExists(logFolder))
      HdfsHelper
        .listFileNamesInFolder(logFolder) // 20170327_1545.log.success
        .filter(!_.startsWith("current"))
        .filter(_.substring(0, 8) < nDaysAgo)
        .foreach(logName => HdfsHelper.deleteFile(s"$logFolder/$logName"))
  }

  /** Only used during unit testing as we're working with a singleton */
  private[spark_helper] def reset(): Unit = {
    reportTitle = None
    pointsOfContact = None
    reportDescription = None
    logDirectory = None
    purgeWindow = None
    reportHeader = buildReportHeader()
    successful = true
    report = ""
  }
}
