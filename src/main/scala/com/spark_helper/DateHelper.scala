package com.spark_helper

import org.joda.time.{DateTime, DateTimeZone, Days}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/** A facility which deals with usual date needs (wrapper around <a href="http://www.joda.org/joda-time/apidocs/">joda-time</a>).
  *
  * The goal is to remove the maximum of highly used low-level code from your
  * spark job and replace it with methods fully tested whose name is
  * self-explanatory/readable.
  *
  * A few exemples:
  *
  * {{{
  * assert(DateHelper.daysBetween("20161230", "20170101") == List("20161230", "20161231", "20170101"))
  * assert(DateHelper.today() == "20170310") // If today's "20170310"
  * assert(DateHelper.yesterday() == "20170309") // If today's "20170310"
  * assert(DateHelper.reformatDate("20170327", "yyyyMMdd", "yyMMdd") == "170327")
  * assert(DateHelper.now("HH:mm") == "10:24")
  * assert(DateHelper.currentTimestamp() == "1493105229736")
  * assert(DateHelper.nDaysBefore(3) == "20170307") // If today's "20170310"
  * assert(DateHelper.nDaysAfterDate(3, "20170307") == "20170310")
  * }}}
  *
  * Source <a href="https://github.com/xavierguihot/spark_helper/blob/master/src
  * /main/scala/com/spark_helper/DateHelper.scala">DateHelper</a>
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
object DateHelper extends Serializable {

	/** Finds the list of dates between the two given dates.
	  *
	  * {{{
	  * assert(DateHelper.daysBetween("20161230", "20170101") == List("20161230", "20161231", "20170101"))
	  * }}}
	  *
	  * @param firstDate the first date (in the given format)
	  * @param lastDate the last date (in the given format)
	  * @param format (default = "yyyyMMdd") the format to use for firstDate and
	  * lastDate and for the returned list of dates.
	  * @return the list of dates between firstDate and lastDate in the given
	  * format.
	  */
	def daysBetween(
		firstDate: String, lastDate: String, format: String = "yyyyMMdd"
	): List[String] = {

		val formatter = DateTimeFormat.forPattern(format).withZone(DateTimeZone.UTC)

		jodaDaysBetween(
			formatter.parseDateTime(firstDate),
			formatter.parseDateTime(lastDate)
		).map(
			formatter.print
		)
	}

	/** Finds the list of dates between the two given dates.
	  *
	  * Sometimes we might want to do additional filtering/operations on the
	  * returned list of dates and thus prefer getting a list of Joda DateTime
	  * objects instead of String dates.
	  *
	  * @param jodaFirstDate the joda DateTime first date
	  * @param jodaLastDate the joda DateTime last date
	  * @return the list of joda DateTime between jodaFirstDate and jodaLastDate
	  */
	def jodaDaysBetween(
		jodaFirstDate: DateTime, jodaLastDate: DateTime
	): List[DateTime] = {

		val nbrOfDaysWithinRange = Days.daysBetween(
			jodaFirstDate, jodaLastDate
		).getDays()

		(0 to nbrOfDaysWithinRange).toList.map(jodaFirstDate.plusDays)
	}

	/** Returns which date it was x days before today under the requested format.
	  *
	  * If we're "20170125" and we request for 3 days before, we'll return
	  * "20170122".
	  *
	  * {{{
	  * // If today's "20170310":
	  * assert(DateHelper.nDaysBefore(3) == "20170307")
	  * assert(DateHelper.nDaysBefore(5, "yyMMdd") == "170305")
	  * }}}
	  *
	  * @param nbrOfDaysBefore the nbr of days before today
	  * @param format (default = "yyyyMMdd") the format for the returned date
	  * @return today's date minus the nbrOfDaysBefore under the requested
	  * format.
	  */
	def nDaysBefore(nbrOfDaysBefore: Int, format: String = "yyyyMMdd"): String = {
		DateTimeFormat.forPattern(format).print(new DateTime().minusDays(nbrOfDaysBefore))
	}

	/** Returns which date it was x days before the given date.
	  *
	  * If the given date is "20170125" and we request the date it was 3 days
	  * before, we'll return "20170122".
	  *
	  * {{{
	  * assert(DateHelper.nDaysBeforeDate(3, "20170310") == "20170307")
	  * assert(DateHelper.nDaysBeforeDate(5, "170310", "yyMMdd") == "170305")
	  * }}}
	  *
	  * @param nbrOfDaysBefore the nbr of days before the given date
	  * @param date the date under the provided format for which we want the
	  * date for nbrOfDaysBefore days before.
	  * @param format (default = "yyyyMMdd") the format for the provided and
	  * returned dates.
	  * @return the date it was nbrOfDaysBefore before date under the
	  * requested format.
	  */
	def nDaysBeforeDate(
		nbrOfDaysBefore: Int, date: String, format: String = "yyyyMMdd"
	): String = {
		val currentDate = DateTimeFormat.forPattern(format).parseDateTime(date)
		DateTimeFormat.forPattern(format).print(currentDate.minusDays(nbrOfDaysBefore))
	}

	/** Returns which date it will be x days after the given date.
	  *
	  * If the given date is "20170122" and we request the date it will be 3
	  * days after, we'll return "20170125".
	  *
	  * {{{
	  * assert(DateHelper.nDaysAfterDate(3, "20170307") == "20170310")
	  * assert(DateHelper.nDaysAfterDate(5, "170305", "yyMMdd") == "170310")
	  * }}}
	  *
	  * @param nbrOfDaysAfter the nbr of days after the given date
	  * @param date the date under the provided format for which we want the
	  * date for nbrOfDaysAfter days after.
	  * @param format (default = "yyyyMMdd") the format for the provided and
	  * returned dates.
	  * @return the date it was nbrOfDaysAfter after date under the
	  * requested format.
	  */
	def nDaysAfterDate(
		nbrOfDaysAfter: Int, date: String, format: String = "yyyyMMdd"
	): String = {
		val currentDate = DateTimeFormat.forPattern(format).parseDateTime(date)
		DateTimeFormat.forPattern(format).print(currentDate.plusDays(nbrOfDaysAfter))
	}

	/** Returns today's date/time under the requested format.
	  *
	  * {{{
	  * // If today's "20170310":
	  * assert(DateHelper.now() == "20170310_1047")
	  * assert(DateHelper.now("yyyyMMdd") == "20170310")
	  * }}}
	  *
	  * @param format (default = "yyyyMMdd_HHmm") the format for the current date
	  * @param utc (default false) whether it's the "local now" or the "utc now"
	  * @return today's date under the requested format
	  */
	def now(format: String = "yyyyMMdd_HHmm", utc: Boolean = false): String = {
		DateTimeFormat.forPattern(format).print(
			if (utc) new DateTime().withZone(DateTimeZone.UTC) else new DateTime())
	}

	/** Returns today's date/time under the requested format.
	  *
	  * {{{
	  * // If today's "20170310":
	  * assert(DateHelper.today() == "20170310")
	  * assert(DateHelper.today("yyMMdd") == "170310")
	  * }}}
	  *
	  * @param format (default = "yyyyMMdd") the format for the current date
	  * @return today's date under the requested format
	  */
	def today(format: String = "yyyyMMdd"): String = nDaysBefore(0, format)

	/** Returns yesterday's date/time under the requested format.
	  *
	  * {{{
	  * // If today's "20170310":
	  * assert(DateHelper.yesterday() == "20170309")
	  * assert(DateHelper.yesterday("yyMMdd") == "170309")
	  * }}}
	  *
	  * @param format (default = "yyyyMMdd") the format in which to output the
	  * date of yesterday
	  * @return yesterday's date under the requested format
	  */
	def yesterday(format: String = "yyyyMMdd"): String = nDaysBefore(1, format)

	/** Returns which date it was 2 days before today under the requested format.
	  *
	  * {{{
	  * // If today's "20170310":
	  * assert(DateHelper.twoDaysAgo() == "20170308")
	  * assert(DateHelper.twoDaysAgo("yyMMdd") == "170308")
	  * }}}
	  *
	  * @param format (default = "yyyyMMdd") the format in which to output the
	  * date of two days ago
	  * @return the date of two days ago under the requested format
	  */
	def twoDaysAgo(format: String = "yyyyMMdd"): String = nDaysBefore(2, format)

	/** Reformats a date from one format to another.
	  *
	  * {{{
	  * assert(DateHelper.reformatDate("20170327", "yyyyMMdd", "yyMMdd") == "170327")
	  * }}}
	  *
	  * @param date the date to reformat
	  * @param inputFormat the format in which the date to reformat is provided
	  * @param outputFormat the format in which to format the provided date
	  * @return the date under the new format
	  */
	def reformatDate(
		date: String, inputFormat: String, outputFormat: String
	): String = {
		DateTimeFormat.forPattern(outputFormat).print(
			DateTimeFormat.forPattern(inputFormat).parseDateTime(date))
	}

	/** Returns the current local timestamp.
	  *
	  * {{{ assert(DateHelper.currentTimestamp() == "1493105229736") }}}
	  *
	  * @return the current timestamps (nbr of millis since 1970-01-01) in the
	  * local computer's zone.
	  */
	def currentTimestamp(): String = new DateTime().getMillis().toString

	/** Returns the current UTC timestamp.
	  *
	  * {{{ assert(DateHelper.currentUtcTimestamp() == "1493105229736") }}}
	  *
	  * @return the current UTC timestamps (nbr of millis since 1970-01-01).
	  */
	def currentUtcTimestamp(): String = {
		new DateTime().withZone(DateTimeZone.UTC).getMillis().toString
	}

	/** Returns for a date the date one day latter.
	  *
	  * {{{
	  * // If the given date is "20170310":
	  * assert(DateHelper.nextDay("20170310") == "20170311")
	  * assert(DateHelper.nextDay("170310", "yyMMdd") == "170311")
	  * }}}
	  *
	  * @param date the date for which to find the date of the day after
	  * @param format (default = "yyyyMMdd") the format of the provided and the
	  * returned dates.
	  * @return the date of the day after the given date
	  */
	def nextDay(date: String, format: String = "yyyyMMdd"): String = {
		val currentDate = DateTimeFormat.forPattern(format).parseDateTime(date)
		DateTimeFormat.forPattern(format).print(currentDate.plusDays(1))
	}

	/** Returns for a date the date one day before.
	  *
	  * {{{
	  * // If the given date is "20170310":
	  * assert(DateHelper.previousDay("20170310") == "20170309")
	  * assert(DateHelper.previousDay("170310", "yyMMdd") == "170309")
	  * }}}
	  *
	  * @param date the date for which to find the date of the day before
	  * @param format (default = "yyyyMMdd") the format of the provided and the
	  * returned dates.
	  * @return the date of the day before the given date
	  */
	def previousDay(date: String, format: String = "yyyyMMdd"): String = {
		val currentDate = DateTimeFormat.forPattern(format).parseDateTime(date)
		DateTimeFormat.forPattern(format).print(currentDate.minusDays(1))
	}

	/** Returns the nbr of days between today and the given date.
	  *
	  * {{{
	  * // If today is "20170327":
	  * assert(DateHelper.nbrOfDaysSince("20170310") == 17)
	  * assert(DateHelper.nbrOfDaysSince("170310", "yyMMdd") == 17)
	  * }}}
	  *
	  * @param date the date for which to find the nbr of days of diff with today
	  * @param format (default = "yyyyMMdd") the format of the provided date
	  * @return the nbr of days between today and the given date
	  */
	def nbrOfDaysSince(date: String, format: String = "yyyyMMdd"): Int = {
		Days.daysBetween(
			DateTimeFormat.forPattern(format).parseDateTime(date),
			new DateTime()
		).getDays()
	}

	/** Returns the nbr of days between the two given dates.
	  *
	  * {{{
	  * assert(DateHelper.nbrOfDaysBetween("20170327", "20170327") == 0)
	  * assert(DateHelper.nbrOfDaysBetween("20170327", "20170401") == 5)
	  * }}}
	  *
	  * This expects the first date to be before the last date.
	  *
	  * @param firstDate the first date of the range for which to egt the nbr of
	  * days.
	  * @param lastDate the last date of the range for which to egt the nbr of
	  * days.
	  * @param format (default = "yyyyMMdd") the format of the provided dates
	  * @return the nbr of days between the two given dates
	  */
	def nbrOfDaysBetween(
		firstDate: String, lastDate: String, format: String = "yyyyMMdd"
	): Int = {
		Days.daysBetween(
			DateTimeFormat.forPattern(format)
				.withZone(DateTimeZone.UTC).parseDateTime(firstDate),
			DateTimeFormat.forPattern(format)
				.withZone(DateTimeZone.UTC).parseDateTime(lastDate)
		).getDays()
	}

	/** Returns the date associated to the given UTC timestamp.
	  *
	  * {{{
	  * assert(DateHelper.dateFromTimestamp(1496074819L) == "20170529")
	  * assert(DateHelper.dateFromTimestamp(1496074819L, "yyMMdd") == "170529")
	  * }}}
	  *
	  * @param timestamp the UTC timestamps (nbr of millis since 1970-01-01) for
	  * which to get the associated date.
	  * @param format (default = "yyyyMMdd") the format of the provided dates
	  * @return the associated date under the requested format
	  */
	def dateFromTimestamp(
		timestamp: Long, format: String = "yyyyMMdd"
	): String = {
		DateTimeFormat.forPattern(format).print(
			new DateTime(timestamp * 1000L, DateTimeZone.UTC))
	}

	/** Returns the day of week for a date under the given format.
	  *
	  * A Monday is 1 and a Sunday is 7.
	  *
	  * {{{ assert(DateHelper.dayOfWeek("20160614") == 2) }}}
	  *
	  * @param date the date for which to get the day of week
	  * @param format (default = "yyyyMMdd") the format under which the date is
	  * provided.
	  * @return the associated day of week, such as 2 for Tuesday
	  */
	def dayOfWeek(date: String, format: String = "yyyyMMdd"): Int = {
		DateTimeFormat.forPattern(format).parseDateTime(date).getDayOfWeek()
	}
}
