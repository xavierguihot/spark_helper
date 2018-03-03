package com.spark_helper

import com.spark_helper.monitoring.Test
import com.spark_helper.monitoring.{INFERIOR_THAN, SUPERIOR_THAN, EQUAL_TO}
import com.spark_helper.monitoring.{PCT, NBR}

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.FunSuite

/** Testing facility for the Monitor facility.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class MonitorTest extends FunSuite with SharedSparkContext {

  test("Basic monitoring testing") {

    // Monitor is initialy successful:
    assert(Monitor.isSuccess())
    // Here is what a report generated without any additional settings should
    // look like:
    var report = removeTimeStamps(Monitor.logs())
    assert(report === "[..:..] Begining\n")

    // Include additional info which are placed in the report's header:
    Monitor.setTitle("Processing of whatever")
    Monitor.addContacts(List("x.guihot@gmail.com", "smbdy@gmail.com"))
    Monitor.addDescription(
      "Documentation: https://github.com/xavierguihot/spark_helper")
    report = removeTimeStamps(Monitor.logs())
    var expectedReport = (
      "					Processing of whatever\n" +
        "\n" +
        "Point of contact: x.guihot@gmail.com, smbdy@gmail.com\n" +
        "Documentation: https://github.com/xavierguihot/spark_helper\n" +
        "[..:..] Begining\n"
    )
    assert(report === expectedReport)

    // Simple text update without success modification:
    Monitor.reset()
    Monitor.log("My First Stage")
    report = removeTimeStamps(Monitor.logs())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage\n"
    )
    assert(report === expectedReport)

    // Let's call .log() another time:
    Monitor.log("My Second Stage")
    report = removeTimeStamps(Monitor.logs())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage\n" +
        "[..:..-..:..] My Second Stage\n"
    )
    assert(report === expectedReport)

    // Successive updates:
    // Update report with success:
    Monitor.reset()
    Monitor.success("My First Stage")
    report = removeTimeStamps(Monitor.logs())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage: success\n"
    )
    assert(report === expectedReport)
    assert(Monitor.isSuccess())
    // Update report with a failure:
    Monitor.error("My Second Stage")
    report = removeTimeStamps(Monitor.logs())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage: success\n" +
        "[..:..-..:..] My Second Stage: failed\n"
    )
    assert(report === expectedReport)
    assert(!Monitor.isSuccess())
    // A success after a failure, which must not overwrite the failure:
    Monitor.success("My Third Stage")
    report = removeTimeStamps(Monitor.logs())
    expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] My First Stage: success\n" +
        "[..:..-..:..] My Second Stage: failed\n" +
        "[..:..-..:..] My Third Stage: success\n"
    )
    assert(report === expectedReport)
    assert(!Monitor.isSuccess())
  }

  test("Check current.ongoing live monitoring") {

    // We remove previous data:
    HdfsHelper.deleteFolder("src/test/resources/logs")

    Monitor.reset()
    Monitor.setTitle("My Processing")
    Monitor.addContacts(List("x.guihot@gmail.com", "smbdy@gmail.com"))
    Monitor.addDescription(
      "Documentation: https://github.com/xavierguihot/spark_helper")
    Monitor.setLogFolder("src/test/resources/logs")
    Monitor.log("Doing something")

    val reportStoredLines = sc
      .textFile("src/test/resources/logs/current.ongoing")
      .collect()
      .toList
      .mkString("\n")

    val expectedReport = (
      "					My Processing\n" +
        "\n" +
        "Point of contact: x.guihot@gmail.com, smbdy@gmail.com\n" +
        "Documentation: https://github.com/xavierguihot/spark_helper\n" +
        "[..:..] Begining\n" +
        "[..:..-..:..] Doing something\n" +
        "\n" +
        "WARNING: If this file exists it does not necessarily mean that " +
        "your job is still running. This file might persist if your job has " +
        "been killed and thus couldn't reach your call to the Monitor.store()."
    )
    assert(removeTimeStamps(reportStoredLines) === expectedReport)
  }

  test("Add error stack trace to report") {

    Monitor.reset()

    // Explanation to someone running tests and seeing an error stack trace
    // even though tests are actually successfull:
    println(
      "README: The following stack trace is NOT a test failure. This " +
        "is the logging/print of the tested stack trace error as it would " +
        "appear in yarn logs."
    )

    try {
      "a".toInt
    } catch {
      case nfe: NumberFormatException =>
        Monitor.error(nfe, "Parse to integer", "my diagnostic")
    }
    // Warning, here I remove the stack trace because it depends on the
    // java/scala version! And yes this test is a bit less usefull.
    val report =
      removeTimeStamps(Monitor.logs()).split("\n").take(3).mkString("\n")
    val expectedReport = (
      "[..:..] Begining\n" +
        "[..:..-..:..] Parse to integer: failed\n" +
        "	Diagnostic: my diagnostic"
    )
    assert(report === expectedReport)
  }

  test("Simple tests") {

    // 1: List of tests:
    Monitor.reset()
    var success = Monitor.kpis(
      List(
        Test("pctOfWhatever", 0.06d, INFERIOR_THAN, 0.1d, PCT),
        Test("pctOfSomethingElse", 0.27d, SUPERIOR_THAN, 0.3d, PCT),
        Test("someNbr", 1235d, EQUAL_TO, 1235d, NBR)
      ),
      "Tests for whatever"
    )

    assert(!success)
    assert(!Monitor.isSuccess())

    var report = removeTimeStamps(Monitor.logs())
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
    Monitor.reset()
    success = Monitor.kpi(
      Test("someNbr", 55e6d, SUPERIOR_THAN, 50e6d, NBR),
      "Tests for whatever")

    assert(success)
    assert(Monitor.isSuccess())

    report = removeTimeStamps(Monitor.logs())
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

  test("Save report") {

    // We remove previous data:
    HdfsHelper.deleteFolder("src/test/resources/logs")

    Monitor.reset()
    Monitor.setTitle("My Processing")
    Monitor.addContacts(List("x.guihot@gmail.com"))
    Monitor.addDescription(
      "Documentation: https://github.com/xavierguihot/spark_helper")
    Monitor.setLogFolder("src/test/resources/logs")
    Monitor.success("Doing something")

    Monitor.store()

    val reportStoredLines = sc
      .textFile("src/test/resources/logs/*.log.success")
      .collect()
      .toList
      .mkString("\n")
      .dropRight(2) + "00" // removes the seconds of the job duration

    val expectedReport = (
      "					My Processing\n" +
        "\n" +
        "Point of contact: x.guihot@gmail.com\n" +
        "Documentation: https://github.com/xavierguihot/spark_helper\n" +
        "[..:..] Begining\n" +
        "[..:..-..:..] Doing something: success\n" +
        "[..:..] Duration: 00:00:00"
    )
    assert(removeTimeStamps(reportStoredLines) === expectedReport)
  }

  test("Save report with purge") {

    HdfsHelper.deleteFolder("src/test/resources/logs")

    // Let's create an outdated log file (12 days before):
    val outdatedDate = DateHelper.nDaysBefore(12, "yyyyMMdd")
    val outdatedLogFile = s"src/test/resources/logs/$outdatedDate.log.success"
    HdfsHelper.writeToHdfsFile("", outdatedLogFile)
    // Let's create a log file not old enough to be purged (3 days before):
    val notOutdatedDate = DateHelper.nDaysBefore(3, "yyyyMMdd")
    val notOutdatedLogFile =
      s"src/test/resources/logs/$notOutdatedDate.log.failed"
    HdfsHelper.writeToHdfsFile("", notOutdatedLogFile)

    // Let's create the previous current.failed status log file:
    HdfsHelper.writeToHdfsFile("", "src/test/resources/logs/current.failed")

    // And we save the new report with the purge option:
    Monitor.reset()
    Monitor.setLogFolder("src/test/resources/logs")
    Monitor.withPurge(7)
    Monitor.store()

    assert(!HdfsHelper.fileExists(outdatedLogFile))
    assert(HdfsHelper.fileExists(notOutdatedLogFile))
    assert(!HdfsHelper.fileExists("src/test/resources/logs/current.failed"))
    assert(HdfsHelper.fileExists("src/test/resources/logs/current.success"))

    HdfsHelper.deleteFolder("src/test/resources/logs")
  }

  // Removes date/time related information in order to assert equality between
  // reports:
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
