package com.spark_helper.monitoring

import java.lang.Math.abs

/** A class which represents a KPI to validate.
  *
  * This is intended to be used as parameter of Monitor.updateByKpiValidation
  * and Monitor.updateByKpisValidation methods.
  *
  * Some exemples of Test objects:
  * {{{
  * Test("pctOfWhatever", 0.06d, INFERIOR_THAN, 0.1d, PCT)
  * Test("pctOfSomethingElse", 0.27d, SUPERIOR_THAN, 0.3d, PCT)
  * Test("someNbr", 1235d, EQUAL_TO, 1235d, NBR)
  * }}}
  *
  * @author Xavier Guihot
  * @since 2016-12
  *
  * @constructor Creates a Test object.
  *
  * Some exemples of Test objects:
  * {{{
  * Test("pctOfWhatever", 0.06d, INFERIOR_THAN, 0.1d, PCT)
  * Test("pctOfSomethingElse", 0.27d, SUPERIOR_THAN, 0.3d, PCT)
  * Test("someNbr", 1235d, EQUAL_TO, 1235d, NBR)
  * }}}
  *
  * @param description the name/description of the KPI which will appear on the
  * validation report.
  * @param kpiValue the value for this KPI
  * @param thresholdType the type of threshold (SUPERIOR_THAN, INFERIOR_THAN or
  * EQUAL_TO).
  * @param appliedThreshold the threshold to apply
  * @param kpiType the type of KPI (PCT or NBR)
  */
final case class Test(
    description: String,
    kpiValue: Double,
    thresholdType: ThresholdType,
    appliedThreshold: Double,
    kpiType: KpiType
) {

  private[spark_helper] def isSuccess(): Boolean = thresholdType match {
    case EQUAL_TO      => kpiValue == appliedThreshold
    case SUPERIOR_THAN => abs(kpiValue) >= appliedThreshold
    case INFERIOR_THAN => abs(kpiValue) <= appliedThreshold
  }

  override def toString(): String =
    List(
      "\tKPI: " + description,
      "\t\tValue: " + kpiValue.toString + kpiType.name,
      "\t\tMust be " + thresholdType.name + " " + appliedThreshold.toString + kpiType.name,
      "\t\tValidated: " + isSuccess().toString
    ).mkString("\n")
}

/** An enumeration which represents the type of threshol to use (EQUAL_TO,
  * SUPERIOR_THAN or INFERIOR_THAN) */
sealed trait ThresholdType { def name: String }

case object EQUAL_TO extends ThresholdType { val name = "equal to" }
case object SUPERIOR_THAN extends ThresholdType { val name = "superior than" }
case object INFERIOR_THAN extends ThresholdType { val name = "inferior than" }

/** An enumeration which represents the type of kpi to use (PCT or NBR) */
sealed trait KpiType { def name: String }

case object PCT extends KpiType { val name = "%" }
case object NBR extends KpiType { val name = "" }
