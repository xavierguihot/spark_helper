package com.spark_helper

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.FunSuite

/** Testing facility for the SparkHelper.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class SparkHelperTest extends FunSuite with SharedSparkContext {

	test("Save as Single Text File") {

		// 1: Without an intermediate working dir:

		var repartitionedDataToStore = sc.parallelize(Array(
			"data_a", "data_b", "data_c"
		)).repartition(3)

		HdfsHelper.deleteFile("src/test/resources/single_text_file.txt")
		SparkHelper.saveAsSingleTextFile(
			repartitionedDataToStore, "src/test/resources/single_text_file.txt"
		)

		var singleFileStoredData = sc.textFile(
			"src/test/resources/single_text_file.txt"
		).collect().sorted
		assert(singleFileStoredData === Array("data_a", "data_b", "data_c"))

		HdfsHelper.deleteFile("src/test/resources/single_text_file.txt")

		// 2: With an intermediate working dir:
		// Notice as well that we test by moving the single file in a folder
		// which doesn't exists.

		repartitionedDataToStore = sc.parallelize(Array(
			"data_a", "data_b", "data_c"
		)).repartition(3)

		HdfsHelper.deleteFile("src/test/resources/folder/single_text_file.txt")
		HdfsHelper.deleteFolder("src/test/resources/folder")
		SparkHelper.saveAsSingleTextFile(
			repartitionedDataToStore, "src/test/resources/folder/single_text_file.txt",
			workingFolder = "src/test/resources/tmp"
		)
		assert(HdfsHelper.fileExists("src/test/resources/folder/single_text_file.txt"))

		singleFileStoredData = sc.textFile(
			"src/test/resources/folder/single_text_file.txt"
		).collect().sorted
		assert(singleFileStoredData === Array("data_a", "data_b", "data_c"))

		HdfsHelper.deleteFolder("src/test/resources/folder")
		HdfsHelper.deleteFolder("src/test/resources/tmp")
	}

	test("Read Text File with Specific Record Delimiter") {

		// 1: Let's read a file where a record begins with a line begining with
		// 3 and other lines begining by 4:

		HdfsHelper.deleteFile("src/test/resources/some_weird_format.txt")

		val textContent = (
			"3 first line of the first record\n" +
			"4 another line of the first record\n" +
			"4 and another one for the first record\n" +
			"3 first line of the second record\n" +
			"3 first line of the third record\n" +
			"4 another line for the third record"
		)

		HdfsHelper.writeToHdfsFile(
			textContent, "src/test/resources/some_weird_format.txt"
		)

		var computedRecords = SparkHelper.textFileWithDelimiter(
			"src/test/resources/some_weird_format.txt", sc, "\n3"
		).collect()

		var expectedRecords = Array(
			(
				"3 first line of the first record\n" +
				"4 another line of the first record\n" +
				"4 and another one for the first record"
			),
			" first line of the second record",
			(
				" first line of the third record\n" +
				"4 another line for the third record"
			)
		)

		assert(computedRecords === expectedRecords)

		HdfsHelper.deleteFile("src/test/resources/some_weird_format.txt")

		// 2: Let's read an xml file:

		HdfsHelper.deleteFile("src/test/resources/some_basic_xml.xml")

		val xmlTextContent = (
			"<Customers>\n" +
			"<Customer>\n" +
			"<Address>34 thingy street, someplace, sometown</Address>\n" +
			"</Customer>\n" +
			"<Customer>\n" +
			"<Address>12 thingy street, someplace, sometown</Address>\n" +
			"</Customer>\n" +
			"</Customers>"
		)

		HdfsHelper.writeToHdfsFile(
			xmlTextContent, "src/test/resources/some_basic_xml.xml"
		)

		computedRecords = SparkHelper.textFileWithDelimiter(
			"src/test/resources/some_basic_xml.xml", sc, "<Customer>\n"
		).collect()

		expectedRecords = Array(
			"<Customers>\n",
			(
				"<Address>34 thingy street, someplace, sometown</Address>\n" +
				"</Customer>\n"
			),
			(
				"<Address>12 thingy street, someplace, sometown</Address>\n" +
				"</Customer>\n" +
				"</Customers>"
			)
		)

		assert(computedRecords === expectedRecords)

		HdfsHelper.deleteFile("src/test/resources/some_basic_xml.xml")
	}

	test("Save as Text File by Key") {

		HdfsHelper.deleteFolder("src/test/resources/key_value_storage")

		val someKeyValueRdd = sc.parallelize[(String, String)](
			Array(
				("key_1", "value_a"),
				("key_1", "value_b"),
				("key_2", "value_c"),
				("key_2", "value_b"),
				("key_2", "value_d"),
				("key_3", "value_a"),
				("key_3", "value_b")
			)
		)

		SparkHelper.saveAsTextFileByKey(
			someKeyValueRdd, "src/test/resources/key_value_storage", 3
		)

		// The folder key_value_storage has been created:
		assert(HdfsHelper.folderExists("src/test/resources/key_value_storage"))

		// And it contains one file per key:
		val genratedKeyFiles = HdfsHelper.listFileNamesInFolder(
			"src/test/resources/key_value_storage"
		)
		val expectedKeyFiles = List("_SUCCESS", "key_1", "key_2", "key_3")
		assert(genratedKeyFiles === expectedKeyFiles)

		val valuesForKey1 = sc.textFile(
			"src/test/resources/key_value_storage/key_1"
		).collect().sorted
		assert(valuesForKey1 === Array("value_a", "value_b"))

		val valuesForKey2 = sc.textFile(
			"src/test/resources/key_value_storage/key_2"
		).collect().sorted
		assert(valuesForKey2 === Array("value_b", "value_c", "value_d"))

		val valuesForKey3 = sc.textFile(
			"src/test/resources/key_value_storage/key_3"
		).collect().sorted
		assert(valuesForKey3 === Array("value_a", "value_b"))

		HdfsHelper.deleteFolder("src/test/resources/key_value_storage")
	}

	test("Decrease Coalescence Level") {

		HdfsHelper.deleteFolder("src/test/resources/re_coalescence_test_input")
		HdfsHelper.deleteFolder("src/test/resources/re_coalescence_test_output")

		// Let's create the folder with high level of coalescence (3 files):
		SparkHelper.saveAsSingleTextFile(
			sc.parallelize[String](Array("data_1_a", "data_1_b", "data_1_c")),
			"src/test/resources/re_coalescence_test_input/input_file_1"
		)
		SparkHelper.saveAsSingleTextFile(
			sc.parallelize[String](Array("data_2_a", "data_2_b")),
			"src/test/resources/re_coalescence_test_input/input_file_2"
		)
		SparkHelper.saveAsSingleTextFile(
			sc.parallelize[String](Array("data_3_a", "data_3_b", "data_3_c")),
			"src/test/resources/re_coalescence_test_input/input_file_3"
		)

		// Let's decrease the coalescence level in order to only have 2 files:
		SparkHelper.decreaseCoalescence(
			"src/test/resources/re_coalescence_test_input",
			"src/test/resources/re_coalescence_test_output",
			2, sc
		)

		// And we check we have two files in output:
		val outputFileList = HdfsHelper.listFileNamesInFolder(
			"src/test/resources/re_coalescence_test_output"
		)
		val expectedFileList = List("_SUCCESS", "part-00000", "part-00001")
		assert(outputFileList === expectedFileList)

		// And that all input data is in the output:
		val outputData = sc.textFile(
			"src/test/resources/re_coalescence_test_output"
		).collect.sorted
		val expectedOutputData = Array(
			"data_1_a", "data_1_b", "data_1_c", "data_2_a", "data_2_b",
			"data_3_a", "data_3_b", "data_3_c"
		)
		assert(outputData === expectedOutputData)

		HdfsHelper.deleteFolder("src/test/resources/re_coalescence_test_output")
	}
}
