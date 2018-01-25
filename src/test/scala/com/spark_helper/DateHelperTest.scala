package com.spark_helper

import org.scalatest.FunSuite

/** Testing facility for date helpers.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class DateHelperTest extends FunSuite {

  test("Range of Dates") {

    // 1: With the default formatter:
    var dates = DateHelper.daysBetween("20161229", "20170103")
    var expectedDates = List(
      "20161229",
      "20161230",
      "20161231",
      "20170101",
      "20170102",
      "20170103"
    )
    assert(dates === expectedDates)

    // 2: With a custom formatter:
    dates = DateHelper.daysBetween("29Dec16", "03Jan17", "ddMMMyy")
    expectedDates = List(
      "29Dec16",
      "30Dec16",
      "31Dec16",
      "01Jan17",
      "02Jan17",
      "03Jan17"
    )
    assert(dates === expectedDates)
  }

  test("Reformat Date") {
    assert(
      DateHelper.reformatDate("20170327", "yyyyMMdd", "yyMMdd") === "170327")
    assert(
      DateHelper.reformatDate("20170327", "yyyyMMdd", "MMddyy") === "032717")
  }

  test("Next Day") {
    assert(DateHelper.nextDay("20170310") === "20170311")
    assert(DateHelper.nextDay("170310", "yyMMdd") === "170311")
    assert(
      DateHelper.nextDay("20170310_0000", "yyyyMMdd_HHmm") === "20170311_0000")
  }

  test("Previous Day") {
    assert(DateHelper.previousDay("20170310") === "20170309")
    assert(DateHelper.previousDay("170310", "yyMMdd") === "170309")
    assert(
      DateHelper
        .previousDay("20170310_0000", "yyyyMMdd_HHmm") === "20170309_0000")
  }

  test("Nbr of Days Between Two Dates") {

    assert(DateHelper.nbrOfDaysBetween("20170327", "20170327") === 0)
    assert(DateHelper.nbrOfDaysBetween("20170327", "20170401") === 5)

    val nbrOfDays = DateHelper
      .nbrOfDaysBetween("20170214_1129", "20170822_0000", "yyyyMMdd_HHmm")
    assert(nbrOfDays === 188)
  }

  test("Get Date from Timestamp") {
    assert(DateHelper.dateFromTimestamp(1496074819L) === "20170529")
    assert(DateHelper.dateFromTimestamp(1496074819L, "yyMMdd") === "170529")
  }

  test("Date it was N Days before Date") {
    assert(DateHelper.nDaysBeforeDate(3, "20170310") === "20170307")
    assert(DateHelper.nDaysBeforeDate(5, "170310", "yyMMdd") === "170305")
  }

  test("Date it will be N Days affter Date") {
    assert(DateHelper.nDaysAfterDate(3, "20170307") === "20170310")
    assert(DateHelper.nDaysAfterDate(5, "170305", "yyMMdd") === "170310")
  }

  test("Day of Week") {
    assert(DateHelper.dayOfWeek("20180102") === 2)
  }

  test("Date versus Provided Format") {
    assert(DateHelper.isDateCompliantWithFormat("20170302", "yyyyMMdd"))
    assert(!DateHelper.isDateCompliantWithFormat("20170333", "yyyyMMdd"))
    assert(DateHelper.isDateCompliantWithFormat("20170228", "yyyyMMdd"))
    assert(!DateHelper.isDateCompliantWithFormat("20170229", "yyyyMMdd"))
    assert(!DateHelper.isDateCompliantWithFormat("170228", "yyyyMMdd"))
    assert(!DateHelper.isDateCompliantWithFormat("", "yyyyMMdd"))
    assert(!DateHelper.isDateCompliantWithFormat("a", "yyyyMMdd"))
    assert(!DateHelper.isDateCompliantWithFormat("24JAN17", "yyyyMMdd"))
  }
}
