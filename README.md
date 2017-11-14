
# SparkHelper [![Build Status](https://travis-ci.org/XavierGuihot/spark_helper.svg?branch=master)](https://travis-ci.org/XavierGuihot/spark_helper)


## Overview


Version: 1.0.6

API Scaladoc: [SparkHelper](http://xavierguihot.github.io/spark_helper/#com.spark_helper.SparkHelper$)

This library contains a bunch of low-level basic methods for data processing
with Scala Spark. This is a bunch of 5 modules:

* [HdfsHelper](http://xavierguihot.github.io/spark_helper/#com.spark_helper.HdfsHelper$): Wrapper around apache Hadoop FileSystem API ([org.apache.hadoop.fs.FileSystem](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html)) for file manipulations on hdfs.
* [SparkHelper](http://xavierguihot.github.io/spark_helper/#com.spark_helper.SparkHelper$): Hdfs file manipulations through the Spark API.
* [DateHelper](http://xavierguihot.github.io/spark_helper/#com.spark_helper.DateHelper$): Wrapper around [joda-time](http://www.joda.org/joda-time/apidocs/) for dates manipulations.
* [FieldChecker](http://xavierguihot.github.io/spark_helper/#com.spark_helper.FieldChecker$): Validation for stringified fields
* [Monitor](http://xavierguihot.github.io/spark_helper/#com.spark_helper.monitoring.Monitor$): Spark custom monitoring/logger and kpi validator

The goal is to remove the maximum of highly used and highly duplicated low-level
code from the spark job code and replace it with methods fully tested whose
names are self-explanatory and readable.


## Using spark_helper:

### HdfsHelper:

The full list of methods is available at [HdfsHelper](http://xavierguihot.github.io/spark_helper/#com.spark_helper.HdfsHelper$).

Contains basic file-related methods mostly based on hdfs apache Hadoop
FileSystem API [org.apache.hadoop.fs.FileSystem](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html).

A few exemples:

	import com.spark_helper.HdfsHelper

	// A bunch of methods wrapping the FileSystem API, such as:
	HdfsHelper.fileExists("my/hdfs/file/path.txt")
	assert(HdfsHelper.listFileNamesInFolder("my/folder/path") == List("file_name_1.txt", "file_name_2.csv"))
	assert(HdfsHelper.getFileModificationDate("my/hdfs/file/path.txt") == "20170306")
	assert(HdfsHelper.getNbrOfDaysSinceFileWasLastModified("my/hdfs/file/path.txt") == 3)

	// Some Xml helpers for hadoop as well:
	HdfsHelper.isHdfsXmlCompliantWithXsd("my/hdfs/file/path.xml", getClass.getResource("/some_xml.xsd"))

### SparkHelper:

The full list of methods is available at [SparkHelper](http://xavierguihot.github.io/spark_helper/#com.spark_helper.SparkHelper$).

Contains basic file/RRD-related methods based on the Spark APIs.

A few exemples:

	import com.spark_helper.SparkHelper

	// Same as SparkContext.saveAsTextFile, but the result is a single file:
	SparkHelper.saveAsSingleTextFile(myOutputRDD, "/my/output/file/path.txt")
	// Same as SparkContext.textFile, but instead of reading one record per line,
	// it reads records spread over several lines:
	SparkHelper.textFileWithDelimiter("/my/input/folder/path", sparkContext, "---\n")

### DateHelper:

The full list of methods is available at [DateHelper](http://xavierguihot.github.io/spark_helper/#com.spark_helper.DateHelper$).

Wrapper around [joda-time](http://www.joda.org/joda-time/apidocs/) for dates manipulations.

A few exemples:

	import com.spark_helper.DateHelper

	assert(DateHelper.daysBetween("20161230", "20170101") == List("20161230", "20161231", "20170101"))
	assert(DateHelper.today() == "20170310") // If today's "20170310"
	assert(DateHelper.yesterday() == "20170309") // If today's "20170310"
	assert(DateHelper.reformatDate("20170327", "yyyyMMdd", "yyMMdd") == "170327")
	assert(DateHelper.now("HH:mm") == "10:24")

### FieldChecker

The full list of methods is available at [FieldChecker](http://xavierguihot.github.io/spark_helper/#com.spark_helper.FieldChecker$).

Validation (before cast) for stringified fields:

A few exemples:

	import com.spark_helper.FieldChecker

	assert(FieldChecker.isInteger("15"))
	assert(!FieldChecker.isInteger("1.5"))
	assert(FieldChecker.isInteger("-1"))
	assert(FieldChecker.isStrictlyPositiveInteger("123"))
	assert(!FieldChecker.isYyyyMMddDate("20170333"))
	assert(FieldChecker.isCurrencyCode("USD"))

### Monitor:

The full list of methods is available at [Monitor](http://xavierguihot.github.io/spark_helper/#com.spark_helper.monitoring.Monitor).

It's a simple logger/report that you update during your job. It contains a
report (a simple string) that you can update and a success boolean which can
be updated to give a success status on your job. At the end of your job you'll
have the possibility to store the report in hdfs.

Have a look at the [scaladoc](http://xavierguihot.github.io/spark_helper/#com.spark_helper.monitoring.Monitor)
for a cool exemple.


## Including spark_helper to your dependencies:


With sbt, just add this one line to your build.sbt:

	libraryDependencies += "spark_helper" % "spark_helper" % "1.0.6" from "https://github.com/xavierguihot/spark_helper/releases/download/v1.0.6/spark_helper-1.0.6.jar"


## Building the project:


With sbt:

	sbt assembly
