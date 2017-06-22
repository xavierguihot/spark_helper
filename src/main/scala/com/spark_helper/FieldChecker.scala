package com.spark_helper

import Math.round

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.IllegalFieldValueException

/** A facility which validates a value for a specific type of field.
  *
  * For instance this allows to validate that a stringified integer is an
  * integer or that a date is under yyyyMMdd format or again that an airport is
  * a 3 upper case string:
  *
  * A few exemples:
  *
  * {{{
  * assert(FieldChecker.isInteger("15"))
  * assert(!FieldChecker.isInteger("1.5"))
  * assert(FieldChecker.isInteger("-1"))
  * assert(FieldChecker.isStrictlyPositiveInteger("123"))
  * assert(!FieldChecker.isYyyyMMddDate("20170333"))
  * assert(FieldChecker.isCurrencyCode("USD"))
  * }}}
  *
  * Source <a href="https://github.com/xavierguihot/spark_helper/blob/master/src
  * /main/scala/com/spark_helper/FieldChecker.scala">FieldChecker</a>
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
object FieldChecker extends Serializable {

	/** Validates a string is an integer.
	  *
	  * {{{
	  * assert(!FieldChecker.isInteger(""))
	  * assert(!FieldChecker.isInteger("sdc"))
	  * assert(FieldChecker.isInteger("0"))
	  * assert(FieldChecker.isInteger("15"))
	  * assert(FieldChecker.isInteger("-1"))
	  * assert(!FieldChecker.isInteger("-1.5"))
	  * assert(!FieldChecker.isInteger("1.5"))
	  * assert(FieldChecker.isInteger("0452"))
	  * }}}
	  *
	  * @param stringValue the stringified integer
	  * @return if the stringified integer is an integer
	  */
	def isInteger(stringValue: String): Boolean = {
		try {
			round(stringValue.toDouble).toFloat == stringValue.toFloat
		} catch {
			case nfe: NumberFormatException => false
		}
	}

	/** Validates a string is an positive integer.
	  *
	  * {{{
	  * assert(!FieldChecker.isPositiveInteger(""))
	  * assert(!FieldChecker.isPositiveInteger("sdc"))
	  * assert(FieldChecker.isPositiveInteger("0"))
	  * assert(FieldChecker.isPositiveInteger("15"))
	  * assert(!FieldChecker.isPositiveInteger("-1"))
	  * assert(!FieldChecker.isPositiveInteger("-1.5"))
	  * assert(!FieldChecker.isPositiveInteger("1.5"))
	  * assert(FieldChecker.isPositiveInteger("0452"))
	  * }}}
	  *
	  * @param stringValue the stringified positive integer
	  * @return if the stringified positive integer is a positive integer
	  */
	def isPositiveInteger(stringValue: String): Boolean = {
		isInteger(stringValue) && round(stringValue.toDouble).toInt >= 0
	}

	/** Validates a string is a strictly positive integer.
	  *
	  * {{{
	  * assert(!FieldChecker.isStrictlyPositiveInteger(""))
	  * assert(!FieldChecker.isStrictlyPositiveInteger("sdc"))
	  * assert(!FieldChecker.isStrictlyPositiveInteger("0"))
	  * assert(FieldChecker.isStrictlyPositiveInteger("1"))
	  * assert(FieldChecker.isStrictlyPositiveInteger("15"))
	  * assert(!FieldChecker.isStrictlyPositiveInteger("-1"))
	  * assert(!FieldChecker.isStrictlyPositiveInteger("-1.5"))
	  * assert(!FieldChecker.isStrictlyPositiveInteger("1.5"))
	  * assert(FieldChecker.isStrictlyPositiveInteger("0452"))
	  * }}}
	  *
	  * @param stringValue the stringified strictly positive integer
	  * @return if the stringified strictly positive integer is a strictly
	  * positive integer.
	  */
	def isStrictlyPositiveInteger(stringValue: String): Boolean = {
		isInteger(stringValue) && round(stringValue.toDouble).toInt > 0
	}

	/** Validates a string is a float.
	  *
	  * {{{
	  * assert(!FieldChecker.isFloat(""))
	  * assert(!FieldChecker.isFloat("sdc"))
	  * assert(FieldChecker.isFloat("0"))
	  * assert(FieldChecker.isFloat("15"))
	  * assert(FieldChecker.isFloat("-1"))
	  * assert(FieldChecker.isFloat("-1.5"))
	  * assert(FieldChecker.isFloat("1.5"))
	  * }}}
	  *
	  * @param stringValue the stringified float
	  * @return if the stringified float is an float
	  */
	def isFloat(stringValue: String): Boolean = {
		try {
			stringValue.toFloat
			true
		} catch {
			case nfe: NumberFormatException => false
		}
	}

	/** Validates a string is a positive float.
	  *
	  * {{{
	  * assert(!FieldChecker.isPositiveFloat(""))
	  * assert(!FieldChecker.isPositiveFloat("sdc"))
	  * assert(FieldChecker.isPositiveFloat("0"))
	  * assert(FieldChecker.isPositiveFloat("15"))
	  * assert(!FieldChecker.isPositiveFloat("-1"))
	  * assert(!FieldChecker.isPositiveFloat("-1.5"))
	  * assert(FieldChecker.isPositiveFloat("1.5"))
	  * }}}
	  *
	  * @param stringValue the stringified positive float
	  * @return if the stringified positive float is a positive float
	  */
	def isPositiveFloat(stringValue: String): Boolean = {
		isFloat(stringValue) && stringValue.toFloat >= 0
	}

	/** Validates a string is a strictly positive float.
	  *
	  * {{{
	  * assert(!FieldChecker.isStrictlyPositiveFloat(""))
	  * assert(!FieldChecker.isStrictlyPositiveFloat("sdc"))
	  * assert(!FieldChecker.isStrictlyPositiveFloat("0"))
	  * assert(FieldChecker.isStrictlyPositiveFloat("15"))
	  * assert(!FieldChecker.isStrictlyPositiveFloat("-1"))
	  * assert(!FieldChecker.isStrictlyPositiveFloat("-1.5"))
	  * assert(FieldChecker.isStrictlyPositiveFloat("1.5"))
	  * }}}
	  *
	  * @param stringValue the stringified strictly positive float
	  * @return if the stringified strictly positive float is a strictly
	  * positive float.
	  */
	def isStrictlyPositiveFloat(stringValue: String): Boolean = {
		isFloat(stringValue) && stringValue.toFloat > 0
	}

	/** Validates a string is a yyyyMMdd date.
	  *
	  * {{{
	  * assert(FieldChecker.isYyyyMMddDate("20170302"))
	  * assert(!FieldChecker.isYyyyMMddDate("20170333"))
	  * assert(FieldChecker.isYyyyMMddDate("20170228"))
	  * assert(!FieldChecker.isYyyyMMddDate("20170229"))
	  * assert(!FieldChecker.isYyyyMMddDate("170228"))
	  * assert(!FieldChecker.isYyyyMMddDate(""))
	  * assert(!FieldChecker.isYyyyMMddDate("a"))
	  * assert(!FieldChecker.isYyyyMMddDate("24JAN17"))
	  * }}}
	  *
	  * @param stringValue the stringified yyyyMMdd date
	  * @return if the stringified yyyyMMdd date is a yyyyMMdd date
	  */
	def isYyyyMMddDate(stringValue: String): Boolean = {
		isDateCompliantWithFormat(stringValue, "yyyyMMdd")
	}

	/** Validates a string is a yyMMdd date.
	  *
	  * {{{
	  * assert(FieldChecker.isYyMMddDate("170302"))
	  * assert(!FieldChecker.isYyMMddDate("170333"))
	  * assert(FieldChecker.isYyMMddDate("170228"))
	  * assert(!FieldChecker.isYyMMddDate("170229"))
	  * assert(!FieldChecker.isYyMMddDate("20170228"))
	  * assert(!FieldChecker.isYyMMddDate(""))
	  * assert(!FieldChecker.isYyMMddDate("a"))
	  * assert(!FieldChecker.isYyMMddDate("24JAN17"))
	  * }}}
	  *
	  * @param stringValue the stringified yyMMdd date
	  * @return if the stringified yyMMdd date is a yyMMdd date
	  */
	def isYyMMddDate(stringValue: String): Boolean = {
		isDateCompliantWithFormat(stringValue, "yyMMdd")
	}

	/** Validates a string is a HHmm time.
	  *
	  * {{{
	  * assert(FieldChecker.isHHmmTime("1224"))
	  * assert(FieldChecker.isHHmmTime("0023"))
	  * assert(!FieldChecker.isHHmmTime("2405"))
	  * assert(!FieldChecker.isHHmmTime("12:24"))
	  * assert(!FieldChecker.isHHmmTime("23"))
	  * assert(!FieldChecker.isHHmmTime(""))
	  * assert(!FieldChecker.isHHmmTime("a"))
	  * assert(!FieldChecker.isHHmmTime("24JAN17"))
	  * }}}
	  *
	  * @param stringValue the stringified HHmm time
	  * @return if the stringified HHmm time is a HHmm time
	  */
	def isHHmmTime(stringValue: String): Boolean = {
		isDateCompliantWithFormat(stringValue, "HHmm")
	}

	/** Validates a string date is under the provided format.
	  *
	  * {{{
	  * assert(FieldChecker.isDateCompliantWithFormat("20170302", "yyyyMMdd"))
	  * assert(!FieldChecker.isDateCompliantWithFormat("20170333", "yyyyMMdd"))
	  * assert(FieldChecker.isDateCompliantWithFormat("20170228", "yyyyMMdd"))
	  * assert(!FieldChecker.isDateCompliantWithFormat("20170229", "yyyyMMdd"))
	  * assert(!FieldChecker.isDateCompliantWithFormat("170228", "yyyyMMdd"))
	  * assert(!FieldChecker.isDateCompliantWithFormat("", "yyyyMMdd"))
	  * assert(!FieldChecker.isDateCompliantWithFormat("a", "yyyyMMdd"))
	  * assert(!FieldChecker.isDateCompliantWithFormat("24JAN17", "yyyyMMdd"))
	  * }}}
	  *
	  * @param stringValue the stringified date
	  * @return if the provided date is under the provided format
	  */
	def isDateCompliantWithFormat(stringValue: String, format: String): Boolean = {
		try {
			DateTimeFormat.forPattern(format).parseDateTime(stringValue)
			true
		} catch {
			case ife: IllegalFieldValueException => false
			case iae: IllegalArgumentException => false
		}
	}

	/** Validates a string is a 3-upper-chars (city/airport code).
	  *
	  * {{{
	  * assert(FieldChecker.isLocationCode("ORY"))
	  * assert(FieldChecker.isLocationCode("NYC "))
	  * assert(!FieldChecker.isLocationCode(""))
	  * assert(!FieldChecker.isLocationCode("ORd"))
	  * assert(!FieldChecker.isLocationCode("FR"))
	  * assert(!FieldChecker.isLocationCode("ORYD"))
	  * }}}
	  *
	  * @param stringValue the airport or city code
	  * @return if the airport or city code looks like a location code
	  */
	def isLocationCode(stringValue: String): Boolean = {
		val value = stringValue.trim
		value.toUpperCase() == value && value.length == 3
	}

	/** Validates a string is a 3-upper-chars airport code.
	  *
	  * {{{
	  * assert(FieldChecker.isAirportCode("ORY"))
	  * assert(FieldChecker.isAirportCode("NYC "))
	  * assert(!FieldChecker.isAirportCode(""))
	  * assert(!FieldChecker.isAirportCode("ORd"))
	  * assert(!FieldChecker.isAirportCode("FR"))
	  * assert(!FieldChecker.isAirportCode("ORYD"))
	  * }}}
	  *
	  * @param stringValue the airport code
	  * @return if the airport code looks like an airport code
	  */
	def isAirportCode(stringValue: String): Boolean = isLocationCode(stringValue)

	/** Validates a string is a 3-upper-chars city code.
	  *
	  * {{{
	  * assert(FieldChecker.isAirportCode("PAR"))
	  * assert(FieldChecker.isAirportCode("NCE "))
	  * assert(!FieldChecker.isAirportCode(""))
	  * assert(!FieldChecker.isAirportCode("ORd"))
	  * assert(!FieldChecker.isAirportCode("FR"))
	  * assert(!FieldChecker.isAirportCode("ORYD"))
	  * }}}
	  *
	  * @param stringValue the city code
	  * @return if the city code looks like a city code
	  */
	def isCityCode(stringValue: String): Boolean = isLocationCode(stringValue)

	/** Validates a string is a 3-upper-chars currency code.
	  *
	  * {{{
	  * assert(FieldChecker.isCurrencyCode("EUR"))
	  * assert(FieldChecker.isCurrencyCode("USD "))
	  * assert(!FieldChecker.isCurrencyCode("EUr"))
	  * assert(!FieldChecker.isCurrencyCode(""))
	  * assert(!FieldChecker.isCurrencyCode("EU"))
	  * assert(!FieldChecker.isCurrencyCode("EURD"))
	  * }}}
	  *
	  * @param stringValue the currency code
	  * @return if the currency code looks like a currency code
	  */
	def isCurrencyCode(stringValue: String): Boolean = {
		val value = stringValue.trim
		value.toUpperCase() == value && value.length == 3
	}

	/** Validates a string is a 2-upper-chars country code.
	  *
	  * {{{
	  * assert(FieldChecker.isCountryCode("FR"))
	  * assert(FieldChecker.isCountryCode("US "))
	  * assert(!FieldChecker.isCountryCode(""))
	  * assert(!FieldChecker.isCountryCode("Us"))
	  * assert(!FieldChecker.isCountryCode("USD"))
	  * }}}
	  *
	  * @param stringValue the country code
	  * @return if the country code looks like a country code
	  */
	def isCountryCode(stringValue: String): Boolean = {
		val value = stringValue.trim
		value.toUpperCase() == value && value.length == 2
	}

	/** Validates a string is a 2-upper-chars airline code.
	  *
	  * {{{
	  * assert(FieldChecker.isAirlineCode("AF"))
	  * assert(FieldChecker.isAirlineCode("BA"))
	  * assert(FieldChecker.isAirlineCode("AA "))
	  * assert(!FieldChecker.isAirlineCode(""))
	  * assert(!FieldChecker.isAirlineCode("Af"))
	  * assert(!FieldChecker.isAirlineCode("AFS"))
	  * }}}
	  *
	  * @param stringValue the airline code
	  * @return if the airline code looks like an airline code
	  */
	def isAirlineCode(stringValue: String): Boolean = {
		val value = stringValue.trim
		value.toUpperCase() == value && value.length == 2
	}

	/** Validates a string is a 1-upper-chars cabin/rbd code.
	  *
	  * {{{
	  * assert(FieldChecker.isClassCode("Y"))
	  * assert(FieldChecker.isClassCode("S "))
	  * assert(!FieldChecker.isClassCode(""))
	  * assert(!FieldChecker.isClassCode("s"))
	  * assert(!FieldChecker.isClassCode("SS"))
	  * }}}
	  *
	  * @param stringValue the cabin/rbd code
	  * @return if the cabin/rbd code looks like a cabin/rbd code
	  */
	def isClassCode(stringValue: String): Boolean = {
		val value = stringValue.trim
		value.toUpperCase() == value && value.length == 1
	}
}
