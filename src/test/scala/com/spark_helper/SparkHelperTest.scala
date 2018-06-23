package com.spark_helper

import com.spark_helper.SparkHelper._

import org.apache.hadoop.io.compress.GzipCodec

import com.holdenkarau.spark.testing.{SharedSparkContext, RDDComparisons}

import org.scalatest.FunSuite

/** Testing facility for the SparkHelper.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class SparkHelperTest
    extends FunSuite
    with SharedSparkContext
    with RDDComparisons {

  val resourceFolder = "src/test/resources"

  test("Save as single text file") {

    val testFolder = s"$resourceFolder/folder"
    val singleTextFilePath = s"$testFolder/single_text_file.txt"
    val tmpFolder = s"$resourceFolder/tmp"

    HdfsHelper.deleteFolder(testFolder)
    HdfsHelper.deleteFolder(tmpFolder)

    val rddToStore =
      sc.parallelize(Array("data_a", "data_b", "data_c")).repartition(3)

    // 1: Without an intermediate working dir:

    rddToStore.saveAsSingleTextFile(singleTextFilePath)

    var singleFileStoredData = sc.textFile(singleTextFilePath).collect().sorted

    assert(singleFileStoredData === Array("data_a", "data_b", "data_c"))

    HdfsHelper.deleteFolder(testFolder)

    // 2: With an intermediate working dir:
    // Notice as well that we test by moving the single file in a folder
    // which doesn't exists.

    rddToStore.saveAsSingleTextFile(
      singleTextFilePath,
      workingFolder = tmpFolder
    )

    singleFileStoredData = sc.textFile(singleTextFilePath).collect().sorted

    assert(singleFileStoredData === Array("data_a", "data_b", "data_c"))

    HdfsHelper.deleteFolder(testFolder)
    HdfsHelper.deleteFolder(tmpFolder)

    // 3: With a compression codec:

    rddToStore
      .saveAsSingleTextFile(s"$singleTextFilePath.gz", classOf[GzipCodec])

    singleFileStoredData =
      sc.textFile(s"$singleTextFilePath.gz").collect().sorted

    assert(singleFileStoredData === Array("data_a", "data_b", "data_c"))

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Read text file with specific record delimiter") {

    val weirdFormatFilePath = s"$resourceFolder/some_weird_format.txt"

    // 1: Let's read a file where a record begins with a line beginning with
    // 3 and other lines beginning by 4:

    HdfsHelper.deleteFile(weirdFormatFilePath)

    val textContent =
      "3 first line of the first record\n" +
        "4 another line of the first record\n" +
        "4 and another one for the first record\n" +
        "3 first line of the second record\n" +
        "3 first line of the third record\n" +
        "4 another line for the third record"

    HdfsHelper.writeToHdfsFile(textContent, weirdFormatFilePath)

    var computedRecords = sc.textFile(weirdFormatFilePath, "\n3").collect()

    var expectedRecords = Array(
      "3 first line of the first record\n" +
        "4 another line of the first record\n" +
        "4 and another one for the first record",
      " first line of the second record",
      " first line of the third record\n" +
        "4 another line for the third record"
    )

    assert(computedRecords === expectedRecords)

    HdfsHelper.deleteFile(weirdFormatFilePath)

    // 2: Let's read an xml file:

    val xmlFilePath = s"$resourceFolder/some_basic_xml.xml"

    HdfsHelper.deleteFile(xmlFilePath)

    val xmlTextContent =
      "<Customers>\n" +
        "<Customer>\n" +
        "<Address>34 thingy street, someplace, sometown</Address>\n" +
        "</Customer>\n" +
        "<Customer>\n" +
        "<Address>12 thingy street, someplace, sometown</Address>\n" +
        "</Customer>\n" +
        "</Customers>"

    HdfsHelper.writeToHdfsFile(xmlTextContent, xmlFilePath)

    computedRecords = sc.textFile(xmlFilePath, "<Customer>\n").collect()

    expectedRecords = Array(
      "<Customers>\n",
      "<Address>34 thingy street, someplace, sometown</Address>\n" +
        "</Customer>\n",
      "<Address>12 thingy street, someplace, sometown</Address>\n" +
        "</Customer>\n" +
        "</Customers>"
    )

    assert(computedRecords === expectedRecords)

    HdfsHelper.deleteFile(xmlFilePath)
  }

  test("Flatten RDD") {

    var in = sc.parallelize(Array(Seq(1, 2, 3), Seq(), Nil, Seq(4), Seq(5, 6)))
    var out = sc.parallelize(Array(1, 2, 3, 4, 5, 6))
    assertRDDEquals(in.flatten, out)

    in = sc.parallelize(Array(List(1, 2, 3), List(), Nil, List(4), List(5, 6)))
    out = sc.parallelize(Array(1, 2, 3, 4, 5, 6))
    assertRDDEquals(in.flatten, out)

    val in2 = sc.parallelize(Array(Option(1), None, Option(2)))
    val out2 = sc.parallelize(Array(1, 2))
    assertRDDEquals(in2.flatten, out2)
  }

  test("Save as text file by key") {

    val keyValueFolder = s"$resourceFolder/key_value_storage"

    // 1: Let's store key values per file:

    HdfsHelper.deleteFolder(keyValueFolder)

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

    someKeyValueRdd.saveAsTextFileByKey(keyValueFolder, 3)

    // The folder key_value_storage has been created:
    assert(HdfsHelper.folderExists(keyValueFolder))

    // And it contains one file per key:
    var generatedKeyFiles = HdfsHelper.listFileNamesInFolder(keyValueFolder)
    var expectedKeyFiles = List("_SUCCESS", "key_1", "key_2", "key_3")
    assert(generatedKeyFiles === expectedKeyFiles)

    var valuesForKey1 = sc.textFile(s"$keyValueFolder/key_1").collect().sorted
    assert(valuesForKey1 === Array("value_a", "value_b"))

    val valuesForKey2 = sc.textFile(s"$keyValueFolder/key_2").collect().sorted
    assert(valuesForKey2 === Array("value_b", "value_c", "value_d"))

    val valuesForKey3 = sc.textFile(s"$keyValueFolder/key_3").collect().sorted
    assert(valuesForKey3 === Array("value_a", "value_b"))

    // 2: Let's store key values per file; but without providing the nbr of
    // keys:

    HdfsHelper.deleteFolder(keyValueFolder)

    someKeyValueRdd.saveAsTextFileByKey(keyValueFolder)

    // The folder key_value_storage has been created:
    assert(HdfsHelper.folderExists(keyValueFolder))

    // And it contains one file per key:
    generatedKeyFiles = HdfsHelper.listFileNamesInFolder(keyValueFolder)
    expectedKeyFiles = List("_SUCCESS", "key_1", "key_2", "key_3")
    assert(generatedKeyFiles === expectedKeyFiles)

    valuesForKey1 = sc.textFile(s"$keyValueFolder/key_1").collect().sorted
    assert(valuesForKey1 === Array("value_a", "value_b"))

    // 3: Let's store key values per file and compress these files:

    HdfsHelper.deleteFolder(keyValueFolder)

    someKeyValueRdd.saveAsTextFileByKey(keyValueFolder, 3, classOf[GzipCodec])

    // The folder key_value_storage has been created:
    assert(HdfsHelper.folderExists(keyValueFolder))

    // And it contains one file per key:
    generatedKeyFiles = HdfsHelper.listFileNamesInFolder(keyValueFolder)
    expectedKeyFiles = List("_SUCCESS", "key_1.gz", "key_2.gz", "key_3.gz")
    assert(generatedKeyFiles === expectedKeyFiles)

    valuesForKey1 = sc.textFile(s"$keyValueFolder/key_1.gz").collect().sorted
    assert(valuesForKey1 === Array("value_a", "value_b"))

    HdfsHelper.deleteFolder(keyValueFolder)
  }

  test("Save as text file and reduce nbr of partitions") {

    val testFolder = s"$resourceFolder/folder"

    HdfsHelper.deleteFolder(testFolder)

    val rddToStore =
      sc.parallelize(Array("data_a", "data_b", "data_c")).repartition(3)

    // 1: Without compressing:

    rddToStore.saveAsTextFileAndCoalesce(testFolder, 2)

    // Let's check the nbr of partitions:
    var generatedKeyFiles = HdfsHelper.listFileNamesInFolder(testFolder)
    var expectedKeyFiles = List("_SUCCESS", "part-00000", "part-00001")
    assert(generatedKeyFiles === expectedKeyFiles)

    // And let's check the content:
    var singleFileStoredData = sc.textFile(testFolder).collect().sorted
    assert(singleFileStoredData === Array("data_a", "data_b", "data_c"))

    HdfsHelper.deleteFolder(testFolder)

    // 2: By compressing:

    rddToStore.saveAsTextFileAndCoalesce(testFolder, 2, classOf[GzipCodec])

    // Let's check the nbr of partitions:
    generatedKeyFiles = HdfsHelper.listFileNamesInFolder(testFolder)
    expectedKeyFiles = List("_SUCCESS", "part-00000.gz", "part-00001.gz")
    assert(generatedKeyFiles === expectedKeyFiles)

    // And let's check the content:
    singleFileStoredData = sc.textFile(testFolder).collect().sorted
    assert(singleFileStoredData === Array("data_a", "data_b", "data_c"))

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Decrease coalescence level") {

    val inputTestFolder = s"$resourceFolder/re_coalescence_test_input"
    val outputTestFolder = s"$resourceFolder/re_coalescence_test_output"

    HdfsHelper.deleteFolder(inputTestFolder)
    HdfsHelper.deleteFolder(outputTestFolder)

    // Let's create the folder with high level of coalescence (3 files):
    sc.parallelize(Array("data_1_a", "data_1_b", "data_1_c"))
      .saveAsSingleTextFile(s"$inputTestFolder/input_file_1")
    sc.parallelize(Array("data_2_a", "data_2_b"))
      .saveAsSingleTextFile(s"$inputTestFolder/input_file_2")
    sc.parallelize(Array("data_3_a", "data_3_b", "data_3_c"))
      .saveAsSingleTextFile(s"$inputTestFolder/input_file_3")

    // Let's decrease the coalescence level in order to only have 2 files:
    sc.decreaseCoalescence(inputTestFolder, outputTestFolder, 2)

    // And we check we have two files in output:
    val outputFileList = HdfsHelper.listFileNamesInFolder(outputTestFolder)
    val expectedFileList = List("_SUCCESS", "part-00000", "part-00001")
    assert(outputFileList === expectedFileList)

    // And that all input data is in the output:
    val outputData = sc.textFile(outputTestFolder).collect.sorted
    val expectedOutputData = Array(
      "data_1_a",
      "data_1_b",
      "data_1_c",
      "data_2_a",
      "data_2_b",
      "data_3_a",
      "data_3_b",
      "data_3_c"
    )
    assert(outputData === expectedOutputData)

    HdfsHelper.deleteFolder(inputTestFolder)
    HdfsHelper.deleteFolder(outputTestFolder)
  }

  test(
    "Extract lines of files to an RDD of tuple containing the line and file " +
      "the line comes from") {

    val testFolder = s"$resourceFolder/with_file_name"

    HdfsHelper.deleteFolder(testFolder)

    HdfsHelper.writeToHdfsFile(
      "data_1_a\ndata_1_b\ndata_1_c",
      s"$testFolder/file_1.txt"
    )
    HdfsHelper.writeToHdfsFile(
      "data_2_a\ndata_2_b",
      s"$testFolder/file_2.txt"
    )
    HdfsHelper.writeToHdfsFile(
      "data_3_a\ndata_3_b\ndata_3_c\ndata_3_d",
      s"$testFolder/folder_1/file_3.txt"
    )

    val computedRdd = sc
      .textFileWithFileName(testFolder)
      // We remove the part of the path which is specific to the local machine
      // on which the test run:
      .map {
        case (filePath, line) =>
          val nonLocalPath = filePath.split("src/test/") match {
            case Array(_, projectRelativePath) =>
              "file:/.../src/test/" + projectRelativePath
          }
          (nonLocalPath, line)
      }

    val expectedRdd = sc.parallelize(
      Array(
        ("file:/.../src/test/resources/with_file_name/file_1.txt", "data_1_a"),
        ("file:/.../src/test/resources/with_file_name/file_1.txt", "data_1_b"),
        ("file:/.../src/test/resources/with_file_name/file_1.txt", "data_1_c"),
        (
          "file:/.../src/test/resources/with_file_name/folder_1/file_3.txt",
          "data_3_a"),
        (
          "file:/.../src/test/resources/with_file_name/folder_1/file_3.txt",
          "data_3_b"),
        (
          "file:/.../src/test/resources/with_file_name/folder_1/file_3.txt",
          "data_3_c"),
        (
          "file:/.../src/test/resources/with_file_name/folder_1/file_3.txt",
          "data_3_d"),
        ("file:/.../src/test/resources/with_file_name/file_2.txt", "data_2_a"),
        ("file:/.../src/test/resources/with_file_name/file_2.txt", "data_2_b")
      ))

    assertRDDEquals(computedRdd, expectedRdd)

    HdfsHelper.deleteFolder(testFolder)
  }

  test("textFile with files containing commas in their path") {

    val testFolder = s"$resourceFolder/files_containing_commas"

    HdfsHelper.deleteFolder(testFolder)

    HdfsHelper.writeToHdfsFile(
      "data_1_a\ndata_1_b",
      s"$testFolder/file,1.txt"
    )
    HdfsHelper.writeToHdfsFile(
      "data_2_a\ndata_2_b",
      s"$testFolder/file_2.txt"
    )

    val computedRdd =
      sc.textFile(List(s"$testFolder/file,1.txt", s"$testFolder/file_2.txt"))
    val expectedRdd =
      sc.parallelize("data_1_a\ndata_1_b\ndata_2_a\ndata_2_b".split("\n"))

    assertRDDEquals(computedRdd, expectedRdd)

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Partial map") {
    val in = sc.parallelize(Array(1, 3, 2, 7, 8))
    val computedOut = in.update { case a if a % 2 == 0 => 2 * a }
    val expectedOut = sc.parallelize(Array(1, 3, 4, 7, 16))
    assertRDDEquals(computedOut, expectedOut)
  }

  test("With Key") {
    val in = sc.parallelize(Array(A(1, "a"), A(2, "b")))
    val out = sc.parallelize(Array((1, A(1, "a")), (2, A(2, "b"))))
    assertRDDEquals(in.withKey(_.x), out)
  }

  test("Filter not") {
    val in = sc.parallelize(Array(1, 3, 2, 7, 8))
    val out = sc.parallelize(Array(1, 3, 7))
    assertRDDEquals(in.filterNot(_ % 2 == 0), out)
  }

  test("Filter key/value") {
    val in = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val outK = sc.parallelize(Array((1, "a"), (3, "c")))
    assertRDDEquals(in.filterKey(_ % 2 == 1), outK)
    val outV = sc.parallelize(Array((2, "b"), (3, "c")))
    assertRDDEquals(in.filterValue(v => Set("b", "c").contains(v)), outV)
  }

  test("Rdd to List") {
    val list = Array(1, 3, 2, 7, 8)
    assert(sc.parallelize(list).toList === list)
  }
}

case class A(x: Int, y: String)
