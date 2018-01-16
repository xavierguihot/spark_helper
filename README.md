
# SparkHelper [![Build Status](https://travis-ci.org/xavierguihot/spark_helper.svg?branch=master)](https://travis-ci.org/xavierguihot/spark_helper) [![Coverage Status](https://coveralls.io/repos/github/xavierguihot/spark_helper/badge.svg?branch=master)](https://coveralls.io/github/xavierguihot/spark_helper?branch=master) [![Release](https://jitpack.io/v/xavierguihot/spark_helper.svg)](https://jitpack.io/#xavierguihot/spark_helper)


## Overview


Version: 1.0.16

API Scaladoc: [SparkHelper](http://xavierguihot.com/spark_helper/#com.spark_helper.SparkHelper$)

This library contains a bunch of low-level basic methods for data processing
with Scala Spark.

The goal is to remove the maximum of highly used and highly duplicated low-level
code from the spark job code and replace it with methods fully tested whose
names are self-explanatory and readable.

This also provides a monitoring/logger tool.

This is a bunch of 4 modules:

* [HdfsHelper](http://xavierguihot.com/spark_helper/#com.spark_helper.HdfsHelper$): Wrapper around apache Hadoop FileSystem API ([org.apache.hadoop.fs.FileSystem](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html)) for file manipulations on hdfs.
* [SparkHelper](http://xavierguihot.com/spark_helper/#com.spark_helper.SparkHelper$): Hdfs file manipulations through the Spark API.
* [Monitor](http://xavierguihot.com/spark_helper/#com.spark_helper.monitoring.Monitor$): Spark custom monitoring/logger and kpi validator.
* [DateHelper](http://xavierguihot.com/spark_helper/#com.spark_helper.DateHelper$): Wrapper around [joda-time](http://www.joda.org/joda-time/apidocs/) for usual data mining dates manipulations.

Compatible with Spark 2.


## Using spark_helper:

### HdfsHelper:

The full list of methods is available at [HdfsHelper](http://xavierguihot.com/spark_helper/#com.spark_helper.HdfsHelper$).

Contains basic file-related methods mostly based on hdfs apache Hadoop
FileSystem API [org.apache.hadoop.fs.FileSystem](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html).

For instance, one don't want to remove a file from hdfs using 3 lines of code
and thus could instead just use HdfsHelper.deleteFile("my/hdfs/file/path.csv").

A non-exhaustive list of exemples:

```scala
import com.spark_helper.HdfsHelper

// A bunch of methods wrapping the FileSystem API, such as:
HdfsHelper.fileExists("my/hdfs/file/path.txt")
assert(HdfsHelper.listFileNamesInFolder("my/folder/path") == List("file_name_1.txt", "file_name_2.csv"))
assert(HdfsHelper.getFileModificationDate("my/hdfs/file/path.txt") == "20170306")
assert(HdfsHelper.getNbrOfDaysSinceFileWasLastModified("my/hdfs/file/path.txt") == 3)
HdfsHelper.deleteFile("my/hdfs/file/path.csv")
HdfsHelper.moveFolder("my/hdfs/folder")
HdfsHelper.compressFile("hdfs/path/to/uncompressed_file.txt", classOf[GzipCodec])
HdfsHelper.appendHeader("my/hdfs/file/path.csv", "colum0,column1")

// Some Xml/Typesafe helpers for hadoop as well:
HdfsHelper.isHdfsXmlCompliantWithXsd("my/hdfs/file/path.xml", getClass.getResource("/some_xml.xsd"))
HdfsHelper.loadXmlFileFromHdfs("my/hdfs/file/path.xml")

// Very handy to load a config (typesafe format) stored on hdfs at the begining of a spark job:
HdfsHelper.loadTypesafeConfigFromHdfs("my/hdfs/file/path.conf"): Config

// In order to write small amount of data in a file on hdfs without the whole spark stack:
HdfsHelper.writeToHdfsFile(Array("some", "relatively small", "text"), "/some/hdfs/file/path.txt")

// Deletes all files/folders in "hdfs/path/to/folder" for which the timestamp is older than 10 days:
HdfsHelper.purgeFolder("hdfs/path/to/folder", 10)
```

### SparkHelper:

The full list of methods is available at [SparkHelper](http://xavierguihot.com/spark_helper/#com.spark_helper.SparkHelper$).

Contains basic file/RRD-related methods based on the Spark APIs.

A non-exhaustive list of exemples:

```scala
import com.spark_helper.SparkHelper

// Same as SparkContext.saveAsTextFile, but the result is a single file:
SparkHelper.saveAsSingleTextFile(myOutputRDD, "/my/output/file/path.txt")

// Same as SparkContext.textFile, but instead of reading one record per line,
// it reads records spread over several lines. This way, xml, json, yml or
// any multi-line record file format can be used with Spark:
SparkHelper.textFileWithDelimiter("/my/input/folder/path", sparkContext, "---\n")
```

### Monitor:

The full description is available at [Monitor](http://xavierguihot.com/spark_helper/#com.spark_helper.monitoring.Monitor).

It's a simple logger/report that you update during your job. It contains a
report that you can update and a success boolean which can be updated to give a
success status on your job. At the end of your job you'll have the possibility
to store the report in hdfs.

Have a look at the [scaladoc](http://xavierguihot.com/spark_helper/#com.spark_helper.monitoring.Monitor)
for a cool exemple.

### DateHelper:

The full list of methods is available at [DateHelper](http://xavierguihot.com/spark_helper/#com.spark_helper.DateHelper$).

Wrapper around [joda-time](http://www.joda.org/joda-time/apidocs/) for
data-mining classic dates manipulations.

A non-exhaustive list of exemples:

```scala
import com.spark_helper.DateHelper

assert(DateHelper.daysBetween("20161230", "20170101") == List("20161230", "20161231", "20170101"))
assert(DateHelper.today() == "20170310") // If today's "20170310"
assert(DateHelper.yesterday() == "20170309") // If today's "20170310"
assert(DateHelper.reformatDate("20170327", "yyyyMMdd", "yyMMdd") == "170327")
assert(DateHelper.now("HH:mm") == "10:24")
assert(DateHelper.currentTimestamp() == "1493105229736")
assert(DateHelper.nDaysBefore(3) == "20170307") // If today's "20170310"
assert(DateHelper.nDaysAfterDate(3, "20170307") == "20170310")
```

## Including spark_helper to your dependencies:


With sbt, add these lines to your build.sbt:

```scala
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.xavierguihot" % "spark_helper" % "v1.0.16"
```

With maven, add these lines to your pom.xml:

```xml
<repositories>
	<repository>
		<id>jitpack.io</id>
		<url>https://jitpack.io</url>
	</repository>
</repositories>

<dependency>
	<groupId>com.github.xavierguihot</groupId>
	<artifactId>spark_helper</artifactId>
	<version>v1.0.16</version>
</dependency>
```

With gradle, add these lines to your build.gradle:

```groovy
allprojects {
	repositories {
		maven { url 'https://jitpack.io' }
	}
}

dependencies {
	compile 'com.github.xavierguihot:spark_helper:v1.0.16'
}
```


## Building the project:


With sbt:

	sbt assembly
