package com.spark_helper.monitoring

import com.spark_helper.{DateHelper, HdfsHelper}

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.FunSuite

/** Testing facility for the Monitor facility.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class MonitorTest extends FunSuite with SharedSparkContext {

  test("Basic Monitoring Testing") {

    var monitor = new Monitor()
    assert(monitor.isSuccess())
    var report = removeTimeStamps(monitor.getReport())
    assert(report === "[..:..] Begining\n")

    // Creation of the Monitor object with additional info:
    monitor = new Monitor(
      "Processing of whatever",
      "xguihot@gmail.com",
      "Documentation: https://github.com/xavierguihot/spark_helper")
    report = removeTimeStamps(monitor.getReport())
    var expectedReport = (
      "					Processing of whatever\n" +
        "\n" +
        "Point of contact: xguihot@gmail.com\n" +
        "Documentation: https://github.com/xavierguihot/spark_helper\n" +
        "[..:..] Begining\n"
    )
    assert(report === expectedReport)

    // Simple text update without success modification:
    monitor = new Monitor()
    monitor.updateReport("My First Stage")
    report = removeTimeStamps(monitor.getReport())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage\n"
    )
    assert(report === expectedReport)

    monitor.updateReport("My Second Stage")
    report = removeTimeStamps(monitor.getReport())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage\n" +
        "[..:..-..:..] My Second Stage\n"
    )
    assert(report === expectedReport)

    // Update report with success or failure:
    monitor = new Monitor()
    monitor.updateReportWithSuccess("My First Stage")
    report = removeTimeStamps(monitor.getReport())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage: success\n"
    )
    assert(report === expectedReport)
    assert(monitor.isSuccess())
    // Failure:
    monitor.updateReportWithFailure("My Second Stage")
    report = removeTimeStamps(monitor.getReport())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage: success\n" +
        "[..:..-..:..] My Second Stage: failed\n"
    )
    assert(report === expectedReport)
    assert(!monitor.isSuccess())
    // A success after a failure, which doesn't overwrite the failure:
    monitor.updateReportWithSuccess("My Third Stage")
    report = removeTimeStamps(monitor.getReport())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage: success\n" +
        "[..:..-..:..] My Second Stage: failed\n" +
        "[..:..-..:..] My Third Stage: success\n"
    )
    assert(report === expectedReport)
    assert(!monitor.isSuccess())
  }

  test("Check current.ongoing Live Monitoring") {

    // We remove previous data:
    HdfsHelper.deleteFolder("src/test/resources/logs")

    val monitor = new Monitor(
      "My Processing",
      "xguihot@gmail.com",
      "Documentation: https://github.com/xavierguihot/spark_helper",
      logFolder = "src/test/resources/logs")
    monitor.updateReport("Doing something: success")

    val reportStoredLines = sc
      .textFile("src/test/resources/logs/current.ongoing")
      .collect()
      .toList
      .mkString("\n")

    val extectedReport = (
      "					My Processing\n" +
        "\n" +
        "Point of contact: xguihot@gmail.com\n" +
        "Documentation: https://github.com/xavierguihot/spark_helper\n" +
        "[..:..] Begining\n" +
        "[..:..-..:..] Doing something: success\n" +
        "\n" +
        "WARNING: Do not base yourself on this file to check if your " +
        "job is still running. This file might persist if your job " +
        "has been killed and thus couldn't reach your call to the " +
        "saveReport() method."
    )
    assert(removeTimeStamps(reportStoredLines) === extectedReport)
  }

  test("Add Error Stack Trace to Report") {

    val monitor = new Monitor()

    // Explanation to someone running tests and seeing an error stack trace
    // even though tests are actually successfull:
    println(
      "README: The following stack trace is NOT a test failure. This " +
        "is the logging of the tested stack trace error as it would " +
        "appear in yarn logs."
    )

    try {
      "a".toInt
    } catch {
      case nfe: NumberFormatException =>
        monitor.updateReportWithError(nfe, "Parse to integer", "my diagnostic")
    }
    // Warning, here I remove the stack trace because it depends on the
    // java/scala version! And yes this test is thus quite not usefull.
    val report =
      removeTimeStamps(monitor.getReport()).split("\n").take(3).mkString("\n")
    val expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] Parse to integer: failed\n" +
        "	Diagnostic: my diagnostic"
    )
    assert(report === expectedReport)
  }

  test("Simple Tests") {

    // 1: List of tests:
    var monitor = new Monitor()
    var success = monitor.updateByKpisValidation(
      List(
        Test("pctOfWhatever", 0.06d, INFERIOR_THAN, 0.1d, PCT),
        Test("pctOfSomethingElse", 0.27d, SUPERIOR_THAN, 0.3d, PCT),
        Test("someNbr", 1235d, EQUAL_TO, 1235d, NBR)
      ),
      "Tests for whatever"
    )

    assert(!success)
    assert(!monitor.isSuccess())

    var report = removeTimeStamps(monitor.getReport())
    var expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] Tests for whatever: failed\n" +
        "	KPI: pctOfWhatever\n" +
        "		Value: 0.06%\n" +
        "		Must be inferior than 0.1%\n" +
        "		Validated: true\n" +
        "	KPI: pctOfSomethingElse\n" +
        "		Value: 0.27%\n" +
        "		Must be superior than 0.3%\n" +
        "		Validated: false\n" +
        "	KPI: someNbr\n" +
        "		Value: 1235.0\n" +
        "		Must be equal to 1235.0\n" +
        "		Validated: true\n"
    )
    assert(report === expectedReport)

    // 2: Single test:
    monitor = new Monitor()
    success = monitor.updateByKpiValidation(
      Test("someNbr", 55e6d, SUPERIOR_THAN, 50e6d, NBR),
      "Tests for whatever")

    assert(success)
    assert(monitor.isSuccess())

    report = removeTimeStamps(monitor.getReport())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] Tests for whatever: success\n" +
        "	KPI: someNbr\n" +
        "		Value: 5.5E7\n" +
        "		Must be superior than 5.0E7\n" +
        "		Validated: true\n"
    )
    assert(report === expectedReport)
  }

  test("Save Report") {

    // We remove previous data:
    HdfsHelper.deleteFolder("src/test/resources/logs")

    val monitor = new Monitor(
      "My Processing",
      "xguihot@gmail.com",
      "Documentation: https://github.com/xavierguihot/spark_helper")
    monitor.updateReport("Doing something: success")

    monitor.saveReport("src/test/resources/logs")

    val reportStoredLines = sc
      .textFile("src/test/resources/logs/*.log.success")
      .collect()
      .toList
      .mkString("\n")

    val extectedReport = (
      "					My Processing\n" +
        "\n" +
        "Point of contact: xguihot@gmail.com\n" +
        "Documentation: https://github.com/xavierguihot/spark_helper\n" +
        "[..:..] Begining\n" +
        "[..:..-..:..] Doing something: success\n" +
        "[..:..] Duration: 00:00:00"
    )
    assert(removeTimeStamps(reportStoredLines) === extectedReport)
  }

  test("Save Report with Purge") {

    HdfsHelper.deleteFolder("src/test/resources/logs")

    // Let's create an outdated log file (12 days before):
    val outdatedDate = DateHelper.nDaysBefore(12, "yyyyMMdd")
    val outdatedLogFile = outdatedDate + ".log.success"
    HdfsHelper.writeToHdfsFile("", "src/test/resources/logs/" + outdatedLogFile)
    // Let's create a log file not old enough to be purged (3 days before):
    val notOutdatedDate = DateHelper.nDaysBefore(3, "yyyyMMdd")
    val notOutdatedLogFile = notOutdatedDate + ".log.failed"
    HdfsHelper
      .writeToHdfsFile("", "src/test/resources/logs/" + notOutdatedLogFile)

    // Let's create the previous current.failed status log file:
    HdfsHelper.writeToHdfsFile("", "src/test/resources/logs/current.failed")

    // And we save the new report with the purge option:
    val monitor = new Monitor()
    monitor
      .saveReport("src/test/resources/logs", purgeLogs = true, purgeWindow = 7)

    assert(!HdfsHelper.fileExists("src/test/resources/logs/" + outdatedLogFile))
    assert(
      HdfsHelper.fileExists("src/test/resources/logs/" + notOutdatedLogFile))
    assert(!HdfsHelper.fileExists("src/test/resources/logs/current.failed"))
    assert(HdfsHelper.fileExists("src/test/resources/logs/current.success"))

    HdfsHelper.deleteFolder("src/test/resources/logs")
  }

  private def removeTimeStamps(logs: String): String = {

    var timeStampFreeLogs = logs
    var index = timeStampFreeLogs.indexOf("[")

    while (index >= 0) {

      if (timeStampFreeLogs(index + 6) == ']') // [12:15]
        timeStampFreeLogs =
          timeStampFreeLogs.substring(0, index) + "[..:..]" +
            timeStampFreeLogs.substring(index + 7)
      else if (timeStampFreeLogs(index + 12) == ']') // [12:15-12:23]
        timeStampFreeLogs =
          timeStampFreeLogs.substring(0, index) + "[..:..-..:..]" +
            timeStampFreeLogs.substring(index + 13)

      index = timeStampFreeLogs.indexOf("[", index + 1);
    }

    timeStampFreeLogs
  }
}
