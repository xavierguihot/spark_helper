package com.spark_helper

import org.joda.time.format.DateTimeFormat

import org.scalatest.FunSuite

/** Testing facility for field validation helpers.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class FieldCheckerTest extends FunSuite {

	test("Integer") {
		assert(!FieldChecker.isInteger(""))
		assert(!FieldChecker.isInteger("sdc"))
		assert(FieldChecker.isInteger("0"))
		assert(FieldChecker.isInteger("15"))
		assert(FieldChecker.isInteger("-1"))
		assert(!FieldChecker.isInteger("-1.5"))
		assert(!FieldChecker.isInteger("1.5"))
		assert(FieldChecker.isInteger("0452"))
	}

	test("Positive Integer") {
		assert(!FieldChecker.isPositiveInteger(""))
		assert(!FieldChecker.isPositiveInteger("sdc"))
		assert(FieldChecker.isPositiveInteger("0"))
		assert(FieldChecker.isPositiveInteger("15"))
		assert(!FieldChecker.isPositiveInteger("-1"))
		assert(!FieldChecker.isPositiveInteger("-1.5"))
		assert(!FieldChecker.isPositiveInteger("1.5"))
		assert(FieldChecker.isPositiveInteger("0452"))
	}

	test("Strictly Positive Integer") {
		assert(!FieldChecker.isStrictlyPositiveInteger(""))
		assert(!FieldChecker.isStrictlyPositiveInteger("sdc"))
		assert(!FieldChecker.isStrictlyPositiveInteger("0"))
		assert(FieldChecker.isStrictlyPositiveInteger("1"))
		assert(FieldChecker.isStrictlyPositiveInteger("15"))
		assert(!FieldChecker.isStrictlyPositiveInteger("-1"))
		assert(!FieldChecker.isStrictlyPositiveInteger("-1.5"))
		assert(!FieldChecker.isStrictlyPositiveInteger("1.5"))
		assert(FieldChecker.isStrictlyPositiveInteger("0452"))
	}

	test("Float") {
		assert(!FieldChecker.isFloat(""))
		assert(!FieldChecker.isFloat("sdc"))
		assert(FieldChecker.isFloat("0"))
		assert(FieldChecker.isFloat("15"))
		assert(FieldChecker.isFloat("-1"))
		assert(FieldChecker.isFloat("-1.5"))
		assert(FieldChecker.isFloat("1.5"))
	}

	test("Positive Float") {
		assert(!FieldChecker.isPositiveFloat(""))
		assert(!FieldChecker.isPositiveFloat("sdc"))
		assert(FieldChecker.isPositiveFloat("0"))
		assert(FieldChecker.isPositiveFloat("15"))
		assert(!FieldChecker.isPositiveFloat("-1"))
		assert(!FieldChecker.isPositiveFloat("-1.5"))
		assert(FieldChecker.isPositiveFloat("1.5"))
	}

	test("Strictly Positive Float") {
		assert(!FieldChecker.isStrictlyPositiveFloat(""))
		assert(!FieldChecker.isStrictlyPositiveFloat("sdc"))
		assert(!FieldChecker.isStrictlyPositiveFloat("0"))
		assert(FieldChecker.isStrictlyPositiveFloat("15"))
		assert(!FieldChecker.isStrictlyPositiveFloat("-1"))
		assert(!FieldChecker.isStrictlyPositiveFloat("-1.5"))
		assert(FieldChecker.isStrictlyPositiveFloat("1.5"))
	}

	test("YyyyMMdd Date") {
		assert(FieldChecker.isYyyyMMddDate("20170302"))
		assert(!FieldChecker.isYyyyMMddDate("20170333"))
		assert(FieldChecker.isYyyyMMddDate("20170228"))
		assert(!FieldChecker.isYyyyMMddDate("20170229"))
		assert(!FieldChecker.isYyyyMMddDate("170228"))
		assert(!FieldChecker.isYyyyMMddDate(""))
		assert(!FieldChecker.isYyyyMMddDate("a"))
		assert(!FieldChecker.isYyyyMMddDate("24JAN17"))
	}

	test("YyMMdd Date") {
		assert(FieldChecker.isYyMMddDate("170302"))
		assert(!FieldChecker.isYyMMddDate("170333"))
		assert(FieldChecker.isYyMMddDate("170228"))
		assert(!FieldChecker.isYyMMddDate("170229"))
		assert(!FieldChecker.isYyMMddDate("20170228"))
		assert(!FieldChecker.isYyMMddDate(""))
		assert(!FieldChecker.isYyMMddDate("a"))
		assert(!FieldChecker.isYyMMddDate("24JAN17"))
	}

	test("HHmm Time") {
		assert(FieldChecker.isHHmmTime("1224"))
		assert(FieldChecker.isHHmmTime("0023"))
		assert(!FieldChecker.isHHmmTime("2405"))
		assert(!FieldChecker.isHHmmTime("12:24"))
		assert(!FieldChecker.isHHmmTime("23"))
		assert(!FieldChecker.isHHmmTime(""))
		assert(!FieldChecker.isHHmmTime("a"))
		assert(!FieldChecker.isHHmmTime("24JAN17"))
	}

	test("Date versus Provided Format") {
		assert(FieldChecker.isDateCompliantWithFormat("20170302", "yyyyMMdd"))
		assert(!FieldChecker.isDateCompliantWithFormat("20170333", "yyyyMMdd"))
		assert(FieldChecker.isDateCompliantWithFormat("20170228", "yyyyMMdd"))
		assert(!FieldChecker.isDateCompliantWithFormat("20170229", "yyyyMMdd"))
		assert(!FieldChecker.isDateCompliantWithFormat("170228", "yyyyMMdd"))
		assert(!FieldChecker.isDateCompliantWithFormat("", "yyyyMMdd"))
		assert(!FieldChecker.isDateCompliantWithFormat("a", "yyyyMMdd"))
		assert(!FieldChecker.isDateCompliantWithFormat("24JAN17", "yyyyMMdd"))
	}

	test("String is Airport or City Code") {

		// Location alias:

		assert(FieldChecker.isLocationCode("ORY"))
		assert(FieldChecker.isLocationCode("NCE"))
		assert(FieldChecker.isLocationCode("JFK"))
		assert(FieldChecker.isLocationCode("NYC"))
		assert(FieldChecker.isLocationCode("NYC "))

		assert(!FieldChecker.isLocationCode("ORd"))
		assert(!FieldChecker.isLocationCode("xxx"))

		assert(!FieldChecker.isLocationCode("FR"))
		assert(!FieldChecker.isLocationCode("fr"))
		assert(!FieldChecker.isLocationCode("ORYD"))

		assert(!FieldChecker.isLocationCode(""))

		// Airport alias:

		assert(FieldChecker.isAirportCode("ORY"))
		assert(FieldChecker.isAirportCode("NCE"))
		assert(FieldChecker.isAirportCode("JFK"))
		assert(FieldChecker.isAirportCode("NYC"))
		assert(FieldChecker.isAirportCode("NYC "))

		assert(!FieldChecker.isAirportCode("ORd"))
		assert(!FieldChecker.isAirportCode("xxx"))

		assert(!FieldChecker.isAirportCode("FR"))
		assert(!FieldChecker.isAirportCode("fr"))
		assert(!FieldChecker.isAirportCode("ORYD"))

		assert(!FieldChecker.isAirportCode(""))

		// City alias:

		assert(FieldChecker.isCityCode("ORY"))
		assert(FieldChecker.isCityCode("NCE"))
		assert(FieldChecker.isCityCode("JFK"))
		assert(FieldChecker.isCityCode("NYC"))
		assert(FieldChecker.isCityCode("NYC "))

		assert(!FieldChecker.isCityCode("ORd"))
		assert(!FieldChecker.isCityCode("xxx"))

		assert(!FieldChecker.isCityCode("FR"))
		assert(!FieldChecker.isCityCode("fr"))
		assert(!FieldChecker.isCityCode("ORYD"))

		assert(!FieldChecker.isCityCode(""))
	}

	test("String is Currency Code") {

		assert(FieldChecker.isCurrencyCode("EUR"))
		assert(FieldChecker.isCurrencyCode("USD"))
		assert(FieldChecker.isCurrencyCode("USD "))

		assert(!FieldChecker.isCurrencyCode("EUr"))
		assert(!FieldChecker.isCurrencyCode("xxx"))

		assert(!FieldChecker.isCurrencyCode("EU"))
		assert(!FieldChecker.isCurrencyCode("eu"))
		assert(!FieldChecker.isCurrencyCode("EURD"))

		assert(!FieldChecker.isCurrencyCode(""))
	}

	test("String is Country Code") {

		assert(FieldChecker.isCountryCode("FR"))
		assert(FieldChecker.isCountryCode("US"))
		assert(FieldChecker.isCountryCode("US "))

		assert(!FieldChecker.isCountryCode("Us"))
		assert(!FieldChecker.isCountryCode("us"))
		assert(!FieldChecker.isCountryCode("USD"))

		assert(!FieldChecker.isCountryCode(""))
	}

	test("String is Airline Code") {

		assert(FieldChecker.isAirlineCode("AF"))
		assert(FieldChecker.isAirlineCode("BA"))
		assert(FieldChecker.isAirlineCode("AA "))

		assert(!FieldChecker.isAirlineCode("Af"))
		assert(!FieldChecker.isAirlineCode("af"))
		assert(!FieldChecker.isAirlineCode("AFS"))

		assert(!FieldChecker.isAirlineCode(""))
	}

	test("String is Class Code") {

		assert(FieldChecker.isClassCode("Y"))
		assert(FieldChecker.isClassCode("H"))
		assert(FieldChecker.isClassCode("S "))

		assert(!FieldChecker.isClassCode("s"))
		assert(!FieldChecker.isClassCode("SS"))

		assert(!FieldChecker.isClassCode(""))
	}
}
