package com.spark_helper.monitoring

import java.lang.Math.abs

/** A class which represents a KPI to validate.
  *
  * This is intended to be used as parameter of Monitor.updateByKpiValidation
  * and Monitor.updateByKpisValidation methods.
  *
  * Some exemples of Test objects:
  * {{{
  * new Test("pctOfWhatever", 0.06d, "inferior to", 0.1d, "pct")
  * new Test("pctOfSomethingElse", 0.27d, "superior to", 0.3d, "pct")
  * new Test("someNbr", 1235d, "equal to", 1235d, "nbr")
  * }}}
  *
  * @author Xavier Guihot
  * @since 2016-12
  *
  * @constructor Creates a Test object.
  *
  * Some exemples of Test objects:
  * {{{
  * new Test("pctOfWhatever", 0.06d, "inferior to", 0.1d, "pct")
  * new Test("pctOfSomethingElse", 0.27d, "superior to", 0.3d, "pct")
  * new Test("someNbr", 1235d, "equal to", 1235d, "nbr")
  * }}}
  *
  * @param description the name/description of the KPI which will appear on the
  * validation report.
  * @param kpiValue the value for this KPI
  * @param thresholdType the type of threshold ("superior to", "inferior to"
  * or "equal to").
  * @param appliedThreshold the threshold to apply
  * @param kpiType the type of KPI ("pct" or "nbr")
  */
class Test(
	description: String, kpiValue: Double, thresholdType: String,
	appliedThreshold: Double, kpiType: String
) {

	require(
		List("superior to", "inferior to", "equal to").contains(thresholdType),
		"the threshold type can only be \"superior to\", \"inferior to\" or " +
		"\"equal to\", but you used: \"" + thresholdType + "\"."
	)

	require(
		List("pct", "nbr").contains(kpiType),
		"the kpi type can only be \"pct\" or \"nbr\", but you used: \"" +
		kpiType + "\"."
	)

	private[monitoring] def isSuccess(): Boolean = thresholdType match {
		case "superior to" => abs(kpiValue) >= appliedThreshold
		case "inferior to" => abs(kpiValue) <= appliedThreshold
		case "equal to"    => kpiValue == appliedThreshold
	}

	private[monitoring] def stringify(): String = {

		val suffix = kpiType match {
			case "pct" => "%"
			case "nbr" => ""
		}

		List(
			"\tKPI: " + description,
			"\t\tValue: " + kpiValue.toString + suffix,
			"\t\tMust be " + thresholdType + " " + appliedThreshold.toString + suffix,
			"\t\tValidated: " + isSuccess().toString
		).mkString("\n")
	}
}
