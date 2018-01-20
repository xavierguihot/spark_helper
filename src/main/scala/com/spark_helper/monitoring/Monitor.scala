package com.spark_helper.monitoring

import com.spark_helper.{DateHelper, HdfsHelper}

import java.util.Calendar

import org.apache.commons.lang3.time.DurationFormatUtils

import java.lang.Throwable

/** A facility used to monitor a Spak job.
  *
  * It's a simple logger/report that you update during your job. It contains a
  * report (a simple string) that you can update and a success boolean which can
  * be updated to give a success status on your job. At the end of your job
  * you'll have the possibility to store the report in hdfs.
  *
  * Let's go through a simple Spark job example monitored with this Monitor
  * facility:
  *
  * {{{
  * val sparkContext = new SparkContext(new SparkConf())
  * val monitor = new Monitor("My Simple Job")
  *
  * try {
  *
  *   // Let's perform a spark pipeline which might goes wrong:
  *   val processedData = sparkContext
  *     .textFile("/my/hdfs/input/path").map(do whatever)
  *
  *   // Let's say you want to get some KPIs on your output before storing it:
  *   val outputIsValid = monitor.updateByKpisValidation(
  *     List(
  *       new Test("Nbr of output records", processedData.count(), "superior to", 10e6f, "nbr"),
  *       new Test("Some pct of invalid output", your_complex_kpi, "inferior to", 3, "pct")
  *     ),
  *     "My pipeline descirption"
  *   )
  *
  *   if (outputIsValid)
  *     processedData.saveAsTextFile("wherever/folder")
  *
  * } catch {
  *   case iie: InvalidInputException => {
  *     monitor.updateReportWithError(
  *       iie, "My pipeline descirption", diagnostic = "No input data!")
  *   }
  *   case e: Throwable => {
  *     monitor.updateReportWithError(e, "My pipeline descirption")
  *   }
  * }
  *
  * if (monitor.isSuccess()) {
  *   val doMore = "Let's do more stuff!"
  *   monitor.updateReport("My second pipeline description: success")
  * }
  *
  * // At the end of the different steps of the job, we can store the report in
  * // HDFS:
  * monitor.saveReport("/my/hdfs/functionnal/logs/folder")
  *
  * // At the end of your job, if you considered your job isn't successfull,
  * // then crash it!:
  * if (!monitor.isSuccess()) throw new Exception()
  * }}}
  *
  * If we were to read the stored report after this simple pipeline, here are
  * some possible scenarios:
  *
  * First scenario, problem with the input of the job:
  * {{{
  *           My Simple Job
  *
  * [10:23] Begining
  * [10:23-10:23] My pipeline descirption: failed
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
  *           My Simple Job
  *
  * [10:23] Begining
  * [10:23-10:36] My pipeline descirption: failed
  *     java.lang.NumberFormatException: For input string: "a"
  *     java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
  *     java.lang.Integer.parseInt(Integer.java:492)
  *     ...
  * [10:36] Duration: 00:13:47
  * }}}
  *
  * Another scenario, successfull spark pipeline and KPIs are valid; all good!:
  * {{{
  *           My Simple Job
  *
  * [10:23] Begining
  * [10:23-10:41] My pipeline descirption: success
  *   KPI: Nbr of output records
  *     Value: 14669071.0
  *     Must be superior to 10000000.0
  *     Validated: true
  *   KPI: Some pct of invalid output
  *     Value: 0.06%
  *     Must be inferior to 3.0%
  *     Validated: true
  * [10:41-10:42] My second pipeline description: success
  * [10:42] Duration: 00:19:23
  * }}}
  *
  * One of the good things of this facility is the catching of spark exceptions
  * and it's storage within a log file. This makes it a lot easier to quickly
  * find what went wrong than having to find back and scroll yarn logs.
  *
  * It comes in handy in production environments for which locating the yarn
  * logs of your spark job can be a pain. Or in order when the poduction job
  * fails to send the report of the error by email. Or simply to keep track of
  * historical kpis, processing times, ...
  *
  * This is not supposed to be updated from within a Spark pipeline
  * (actions/transformations) but rather from the orchestration of the
  * pipelines.
  *
  * When instantiating the Monitor object with the optional parameter
  * "logFolder", then you don't need to wait for the end of your job (your call
  * to saveReport()) to be able to look at what's going on. With this option
  * filled, any report update will directly be saved in the file
  * logFolder/current.ongoing. This way, you can have a live idea of what's
  * going on with your job and even if your job is forced-killed, you'll have
  * the possibility to easily have a look at what happened.
  *
  * Source <a href="https://github.com/xavierguihot/spark_helper/blob/master/src
  * /main/scala/com/spark_helper/monitoring/Monitor.scala">Monitor</a>
  *
  * @author Xavier Guihot
  * @since 2017-02
  *
  * @constructor Creates a Monitor object.
  *
  * Creating the Monitor object like this:
  * {{{ new Monitor("My Spark Job Title", "someone@box.com", "Whatever pretty descritpion.") }}}
  * will result in the report to start like this:
  * {{{
  *           My Spark Job Title
  *
  * Point of contact: someone@box.com
  * Whatever pretty descritpion.
  * [..:..] Begining
  * }}}
  *
  * @param reportTitle (optional) what's outputed as a first line of the report
  * @param pointOfContact (optional) the persons in charge of the job
  * @param additionalInfo (optional) anything you want written at the begining
  * of your report.
  * @param logFolder (optional) the folder in which this report is stored
  */
class Monitor(
    reportTitle: String = "",
    pointOfContact: String = "",
    additionalInfo: String = "",
    logFolder: String = ""
) {

  private var success = true
  private var report = ""

  // Let's initiate the report with parameters given while instantiating the
  // Monitor object:
  initiateReport()

  private val begining = Calendar.getInstance().getTimeInMillis()

  private var lastReportUpdate = DateHelper.now("HH:mm")

  /** Returns if at that point all previous stages were successfull.
    *
    * @return if your spark job is successfull.
    */
  def isSuccess(): Boolean = success

  /** Returns the current state of the monitoring report.
    *
    * @return the report.
    */
  def getReport(): String = report

  /** Updates the report with some text.
    *
    * Using this method like this:
    * {{{ monitor.updateReport("Some text") }}}
    * will result in this to be appended to the report:
    * {{{ "[10:35-10:37] Some text\n" }}}
    *
    * @param text the text to append to the report
    */
  def updateReport(text: String): Unit = updateReport(text, true)

  /** Updates the report with some text and a success.
    *
    * If the status of the monitoring was success, then it stays success. If it
    * was failure, then it stays a failure.
    *
    * Using this method like this:
    * {{{ monitor.updateReportWithSuccess("Some text") }}}
    * will result in this to be appended to the report:
    * {{{ "[10:35-10:37] Some text: success\n" }}}
    *
    * @param taskDescription the text to append to the report
    * @return true since it's a success
    */
  def updateReportWithSuccess(taskDescription: String): Boolean = {
    updateReport(taskDescription + ": success")
    true
  }

  /** Updates the report with some text and a failure.
    *
    * This sets the status of the monitoring to false. After that the status
    * will never be success again, even if you update the report with success
    * tasks.
    *
    * Using this method like this:
    * {{{ monitor.updateReportWithFailure("Some text") }}}
    * will result in this to be appended to the report:
    * {{{ "[10:35-10:37] Some text: failure\n" }}}
    *
    * Once the monitoring is a failure, then whatever following successfull
    * action won't change the failed status of the monitoring.
    *
    * @param taskDescription the text to append to the report
    * @return false since it's a failure
    */
  def updateReportWithFailure(taskDescription: String): Boolean = {
    updateReport(taskDescription + ": failed")
    success = false
    false
  }

  /** Updates the report with the stack trace of an error.
    *
    * This sets the status of the monitoring to false. After that the status
    * will never be success again, even if you update the report with success
    * tasks.
    *
    * Catching an error like this:
    * {{{
    * monitor.updateReportWithError(
    *   invalidInputException,
    *   "My pipeline descirption",
    *   diagnostic = "No input data!")
    * }}}
    * will result in this to be appended to the report:
    * {{{
    * [10:23-10:24] My pipeline descirption: failed
    *   Diagnostic: No input data!
    *     org.apache.hadoop.mapred.InvalidInputException: Input path does not exist: hdfs://my/hdfs/input/path
    *     at org.apache.hadoop.mapred.FileInputFormat.singleThreadedListStatus(FileInputFormat.java:285)
    *     at org.apache.hadoop.mapred.FileInputFormat.listStatus(FileInputFormat.java:228)
    *     ...
    * }}}
    *
    * @param error the trown error
    * @param taskDescription the description of the step which failed
    * @param diagnostic (optional) the message we want to add to clarify the
    * source of the problem. By default if this parameter is not used, then no
    * diagnostic is append to the report.
    * @return false since it's a failure
    */
  def updateReportWithError(
      error: Throwable,
      taskDescription: String,
      diagnostic: String = ""
  ): Boolean = {

    // In addition to updating the report with the stack trace and a possible
    // diagnostic, we set the monitoring as failed:
    success = false

    var update = ""

    if (!taskDescription.isEmpty)
      update += taskDescription + ": failed\n"

    if (!diagnostic.isEmpty)
      update += "\tDiagnostic: " + diagnostic + "\n"

    update += ("\t\t" + error.toString() + "\n" +
      error.getStackTrace.map(line => "\t\t" + line).mkString("\n") + "\n")

    updateReport(update)

    false
  }

  /** Updates the report by the validation of a list of kpis/tests.
    *
    * By providing a list of [[com.spark_helper.monitoring.Test]] objects to
    * validate against thresholds, the report is updated with a detailed result
    * of the validation and the success status of the monitoring is set to false
    * if at least one KPI isn't valid.
    *
    * If the validation of tests is a failure then after that the status will
    * never be success again, even if you update the report with success tasks.
    *
    * Using this method like this:
    * {{{
    * monitor.updateByKpisValidation(
    *   List(
    *     new Test("pctOfWhatever", 0.06f, "inferior to", 0.1f, "pct"),
    *     new Test("pctOfSomethingElse", 0.27f, "superior to", 0.3f, "pct"),
    *     new Test("someNbr", 1235f, "equal to", 1235f, "nbr")
    *   ),
    *   "Tests for whatever"
    * )
    * }}}
    * will result in this to be appended to the report:
    * {{{
    * [10:35-10:37] Tests for whatever: failed
    *   KPI: pctOfWhatever
    *     Value: 0.06%
    *     Must be inferior to 0.1%
    *     Validated: true
    *   KPI: pctOfSomethingElse
    *     Value: 0.27%
    *     Must be superior to 0.3%
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
  def updateByKpisValidation(
      tests: List[Test],
      testSuitName: String
  ): Boolean = {

    val testsAreValid = tests.forall(_.isSuccess())

    if (!testsAreValid)
      success = false

    var update = ""

    // A title in the report for the kpi validation:
    if (!testSuitName.isEmpty) {
      val validation = if (testsAreValid) "success" else "failed"
      update += testSuitName + ": " + validation + "\n"
    }

    // The kpi report is added to the report:
    update += tests.map(_.stringify).mkString("\n")

    updateReport(update)

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
    * monitor.updateByKpiValidation(
    *   new Test("pctOfWhatever", 0.06f, "inferior to", 0.1f, "pct"),
    *   "Tests for whatever")
    * }}}
    * will result in this to be appended to the report:
    * {{{
    * [10:35-10:37] Tests for whatever: success
    *   KPI: pctOfWhatever
    *     Value: 0.06%
    *     Must be inferior to 0.1%
    *     Validated: true
    * }}}
    *
    * @param test the Test object to validate
    * @param testSuitName the description of the task being tested
    * @return if the test is successful
    */
  def updateByKpiValidation(test: Test, testSuitName: String): Boolean = {
    updateByKpisValidation(List(test), testSuitName)
  }

  /** Saves the report in a single text file.
    *
    * This report will be stored in the folder provided by the parameter
    * logFolder and its name will be either yyyyMMdd_HHmmss.log.success or
    * yyyyMMdd_HHmmss.log.failed depending on the monitoring status.
    *
    * In addition to storing the report with a timestamp-based name, it is also
    * stored under the name "current.success" or "current.failed" in the same
    * folder in order to give it a fixed name for downstream projects to look
    * for. Obviously if the new status is success, and the previous was failed,
    * the previous current.failed file is deleted and vis et versa.
    *
    * For high frequency jobs, it might be good not to keep all logs
    * indefinitely. To avoid that, the parameter purgeLogs can be set to true
    * and by providing the parameter purgeWindow, the nbr of days after which a
    * log file is purged can be specified.
    *
    * @param logFolder the path of the folder in which this report is archived
    * @param purgeLogs (default = false) if logs are purged when too old
    * @param purgeWindow (default = 7 if purgeLogs = true) if purgeLogs is set
    * to true, after how many days a log file is considered outdated and is
    * purged.
    */
  def saveReport(
      logFolder: String,
      purgeLogs: Boolean = false,
      purgeWindow: Int = 7
  ): Unit = {

    // We add the job duration to the report:
    val jobDuration = DurationFormatUtils.formatDuration(
      Calendar.getInstance().getTimeInMillis() - begining,
      "HH:mm:ss")
    val finalReport =
      report + DateHelper.now("[HH:mm]") + " Duration: " + jobDuration

    // The extension of the report depending on the success:
    val reportExtension = if (isSuccess) ".success" else ".failed"

    // And we store the file as a simple text file with a name based on the
    // timestamp:
    HdfsHelper.writeToHdfsFile(
      finalReport,
      logFolder + "/" +
        DateHelper.now("yyyyMMdd_HHmmss") + ".log" + reportExtension)

    // But we store it as well with a fixed name such as current.success:
    HdfsHelper.deleteFile(logFolder + "/current.success")
    HdfsHelper.deleteFile(logFolder + "/current.failed")
    HdfsHelper.writeToHdfsFile(
      finalReport,
      logFolder + "/current" + reportExtension
    )

    // And if we "live loged", then we remove the "current.ongoing" file:
    HdfsHelper.deleteFile(logFolder + "/current.ongoing")

    if (purgeLogs)
      purgeOutdatedLogs(logFolder, purgeWindow)
  }

  private def initiateReport(): Unit = {

    // If there was a current.success or a current.failed in the log folder
    // remaining from the previous run, we delete it:

    if (!logFolder.isEmpty) {
      HdfsHelper.deleteFile(logFolder + "/current.success")
      HdfsHelper.deleteFile(logFolder + "/current.failed")
    }

    // And we initiate the report with it's title and basic infos:

    var initialReport = ""

    if (!reportTitle.isEmpty)
      initialReport += "\t\t\t\t\t" + reportTitle + "\n\n"
    if (!pointOfContact.isEmpty)
      initialReport += "Point of contact: " + pointOfContact + "\n"
    if (!additionalInfo.isEmpty)
      initialReport += additionalInfo + "\n"

    initialReport += DateHelper.now("[HH:mm]") + " Begining"

    updateReport(initialReport, false)
  }

  private def updateReport(text: String, withTimestamp: Boolean): Unit = {

    val before = lastReportUpdate
    val now = DateHelper.now("HH:mm")

    lastReportUpdate = now

    val update =
      if (withTimestamp) "[" + before + "-" + now + "]" + " " + text
      else text

    report += update + "\n"

    // We print the update to also have it within yarn logs:
    println("MONITOR: " + update)

    // And if the logFolder parameter was used to instantiate the Monitor
    // object, we also update live the log file:
    if (!logFolder.isEmpty) {

      val ongoingReport = (report + "\n" +
        "WARNING: Do not base yourself on this file to check if your job is " +
        "still running. This file might persist if your job has been killed " +
        "and thus couldn't reach your call to the saveReport() method.")

      HdfsHelper.writeToHdfsFile(ongoingReport, logFolder + "/current.ongoing")
    }
  }

  private def purgeOutdatedLogs(logFolder: String, purgeWindow: Int): Unit = {

    val nDaysAgo = DateHelper.nDaysBefore(purgeWindow, "yyyyMMdd")

    if (HdfsHelper.folderExists(logFolder))
      HdfsHelper
        .listFileNamesInFolder(logFolder)
        .filter(logName => !logName.startsWith("current"))
        .filter { logName => // 20170327_1545.log.success
          val logDate = logName.substring(0, 8) // 20170327
          logDate < nDaysAgo
        }
        .foreach(logName => HdfsHelper.deleteFile(logFolder + "/" + logName))
  }
}
