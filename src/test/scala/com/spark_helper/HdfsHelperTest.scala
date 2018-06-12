package com.spark_helper

import org.apache.hadoop.io.compress.GzipCodec

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.FunSuite

/** Testing facility for the HdfsHelper.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class HdfsHelperTest extends FunSuite with SharedSparkContext {

  val resourceFolder = "src/test/resources"
  val testFolder = s"$resourceFolder/folder"

  test("Delete file/folder") {

    val filePath = s"$testFolder/file.txt"

    // Let's try to delete a file:

    HdfsHelper.createEmptyHdfsFile(filePath)

    // 1: Let's try to delete it with the deleteFolder method:
    var messageThrown = intercept[IllegalArgumentException] {
      HdfsHelper.deleteFolder(filePath)
    }
    var expectedMessage =
      "requirement failed: to delete a file, prefer using the " +
        "deleteFile() method."
    assert(messageThrown.getMessage === expectedMessage)
    assert(HdfsHelper.fileExists(filePath))

    // 2: Let's delete it with the deleteFile method:
    HdfsHelper.deleteFile(filePath)
    assert(!HdfsHelper.fileExists(filePath))

    // Let's try to delete a folder:

    HdfsHelper.createEmptyHdfsFile(s"$testFolder/file.txt")

    // 3: Let's try to delete it with the deleteFile method:
    messageThrown = intercept[IllegalArgumentException] {
      HdfsHelper.deleteFile(testFolder)
    }
    expectedMessage =
      "requirement failed: to delete a folder, prefer using the " +
        "deleteFolder() method."
    assert(messageThrown.getMessage === expectedMessage)
    assert(HdfsHelper.folderExists(testFolder))

    // 4: Let's delete it with the deleteFolder method:
    HdfsHelper.deleteFolder(testFolder)
    assert(!HdfsHelper.folderExists(testFolder))
  }

  test("File/folder exists") {

    val folderPath = s"$resourceFolder/folder"
    val filePath = s"$folderPath/file.txt"

    HdfsHelper.deleteFile(filePath)
    HdfsHelper.deleteFolder(folderPath)

    // Let's try to check if a file exists:

    assert(!HdfsHelper.fileExists(filePath))

    HdfsHelper.createEmptyHdfsFile(filePath)

    // 1: Let's try to check it exists with the folderExists method:
    var messageThrown = intercept[IllegalArgumentException] {
      HdfsHelper.folderExists(filePath)
    }
    var expectedMessage =
      "requirement failed: to check if a file exists, prefer using the " +
        "fileExists() method."
    assert(messageThrown.getMessage === expectedMessage)

    // 2: Let's try to check it exists with the fileExists method:
    assert(HdfsHelper.fileExists(filePath))

    // Let's try to check if a folder exists:

    HdfsHelper.deleteFolder(folderPath)
    assert(!HdfsHelper.folderExists(folderPath))

    HdfsHelper.createEmptyHdfsFile(filePath)

    // 3: Let's try to check it exists with the fileExists method:
    messageThrown = intercept[IllegalArgumentException] {
      HdfsHelper.fileExists(folderPath)
    }
    expectedMessage =
      "requirement failed: to check if a folder exists, prefer using " +
        "the folderExists() method."
    assert(messageThrown.getMessage === expectedMessage)

    // 2: Let's try to check it exists with the folderExists method:
    assert(HdfsHelper.folderExists(folderPath))

    HdfsHelper.deleteFile(filePath)
    HdfsHelper.deleteFolder(folderPath)
  }

  test("Create an empty file on hdfs") {

    val filePath = s"$testFolder/empty_file.token"

    HdfsHelper.deleteFile(filePath)

    HdfsHelper.createEmptyHdfsFile(filePath)

    assert(HdfsHelper.fileExists(filePath))

    val tokenContent = sc.textFile(filePath).collect().sorted.mkString("\n")
    assert(tokenContent === "")

    HdfsHelper.deleteFile(filePath)
  }

  test(
    "Save text in HDFS file with the fileSystem API instead of the Spark API") {

    val filePath = s"$testFolder/small_file.txt"

    // 1: Stores using a "\n"-joined string:

    HdfsHelper.deleteFile(filePath)

    val contentToStore = "Hello World\nWhatever"

    HdfsHelper.writeToHdfsFile(contentToStore, filePath)

    assert(HdfsHelper.fileExists(filePath))

    var storedContent = sc.textFile(filePath).collect().sorted.mkString("\n")
    assert(storedContent === contentToStore)

    HdfsHelper.deleteFolder(testFolder)

    // 2: Stores using a list of strings to be "\n"-joined:

    HdfsHelper.deleteFile(filePath)

    val listToStore = List("Hello World", "Whatever")
    HdfsHelper.writeToHdfsFile(listToStore, filePath)

    assert(HdfsHelper.fileExists(filePath))

    storedContent = sc.textFile(filePath).collect().sorted.mkString("\n")
    assert(storedContent === listToStore.mkString("\n"))

    HdfsHelper.deleteFolder(testFolder)
  }

  test("List file names in Hdfs folder") {

    val folder1 = s"$resourceFolder/folder_1"

    HdfsHelper.createEmptyHdfsFile(s"$folder1/file_1.txt")
    HdfsHelper.createEmptyHdfsFile(s"$folder1/file_2.csv")
    HdfsHelper.createEmptyHdfsFile(s"$folder1/folder_2/file_3.txt")

    // 1: Not recursive, names only:
    var fileNames = HdfsHelper.listFileNamesInFolder(folder1)
    var expectedFileNames = List("file_1.txt", "file_2.csv")
    assert(fileNames === expectedFileNames)

    // 2: Not recursive, full paths:
    fileNames = HdfsHelper.listFileNamesInFolder(folder1, onlyName = false)
    expectedFileNames = List(s"$folder1/file_1.txt", s"$folder1/file_2.csv")
    assert(fileNames === expectedFileNames)

    // 3: Recursive, names only:
    fileNames = HdfsHelper.listFileNamesInFolder(folder1, recursive = true)
    expectedFileNames = List("file_1.txt", "file_2.csv", "file_3.txt")
    assert(fileNames === expectedFileNames)

    // 4: Recursive, full paths:
    fileNames = HdfsHelper
      .listFileNamesInFolder(folder1, recursive = true, onlyName = false)
    expectedFileNames = List(
      s"$folder1/file_1.txt",
      s"$folder1/file_2.csv",
      s"$folder1/folder_2/file_3.txt"
    )
    assert(fileNames === expectedFileNames)

    HdfsHelper.deleteFolder(folder1)
  }

  test("List folder names in Hdfs folder") {

    val folder1 = s"$resourceFolder/folder_1"

    HdfsHelper.createEmptyHdfsFile(s"$folder1/file_1.txt")
    HdfsHelper.createEmptyHdfsFile(s"$folder1/folder_2/file_2.txt")
    HdfsHelper.createEmptyHdfsFile(s"$folder1/folder_3/file_3.txt")

    val folderNames = HdfsHelper.listFolderNamesInFolder(folder1)
    val expectedFolderNames = List("folder_2", "folder_3")

    assert(folderNames === expectedFolderNames)

    HdfsHelper.deleteFolder(folder1)
  }

  test("Move file") {

    val filePath = s"$testFolder/some_file.txt"
    val renamedPath = s"$testFolder/renamed_file.txt"

    // Let's remove possible previous stuff:
    HdfsHelper.deleteFolder(testFolder)

    // Let's create the file to rename:
    HdfsHelper.writeToHdfsFile("whatever", filePath)

    // 1: Let's try to move the file on a file which already exists without
    // the overwrite option:

    assert(HdfsHelper.fileExists(filePath))
    assert(!HdfsHelper.fileExists(renamedPath))

    // Let's create the existing file where we want to move our file:
    HdfsHelper.createEmptyHdfsFile(renamedPath)

    // Let's rename the file to the path where a file already exists:
    val ioExceptionThrown = intercept[IllegalArgumentException] {
      HdfsHelper.moveFile(filePath, renamedPath)
    }
    var expectedMessage =
      "requirement failed: overwrite option set to false, but a file " +
        "already exists at target location " +
        "src/test/resources/folder/renamed_file.txt"
    assert(ioExceptionThrown.getMessage === expectedMessage)

    assert(HdfsHelper.fileExists(filePath))
    assert(HdfsHelper.fileExists(renamedPath))

    HdfsHelper.deleteFile(renamedPath)

    // 2: Let's fail to move the file with the moveFolder() method:

    assert(HdfsHelper.fileExists(filePath))
    assert(!HdfsHelper.fileExists(renamedPath))

    // Let's rename the file:
    val illegalArgExceptionThrown = intercept[IllegalArgumentException] {
      HdfsHelper.moveFolder(filePath, renamedPath)
    }
    expectedMessage =
      "requirement failed: to move a file, prefer using the " +
        "moveFile() method."
    assert(illegalArgExceptionThrown.getMessage === expectedMessage)

    assert(HdfsHelper.fileExists(filePath))
    assert(!HdfsHelper.fileExists(renamedPath))

    // 3: Let's successfuly move the file with the moveFile() method:

    // Let's rename the file:
    HdfsHelper.moveFile(filePath, renamedPath)

    assert(!HdfsHelper.fileExists(filePath))
    assert(HdfsHelper.fileExists(renamedPath))

    val newContent = sc.textFile(renamedPath).collect
    assert(Array("whatever") === newContent)

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Move folder") {

    val folderToMove = s"$testFolder/folder_to_move"
    val renamedFolder = s"$testFolder/renamed_folder"

    // Let's remove possible previous stuff:
    HdfsHelper.deleteFolder(testFolder)

    // Let's create the folder to rename:
    HdfsHelper.writeToHdfsFile("whatever", s"$folderToMove/file_1.txt")
    HdfsHelper.writeToHdfsFile("something", s"$folderToMove/file_2.txt")

    // 1: Let's fail to move the folder with the moveFile() method:

    assert(HdfsHelper.fileExists(s"$folderToMove/file_1.txt"))
    assert(HdfsHelper.fileExists(s"$folderToMove/file_2.txt"))
    assert(!HdfsHelper.folderExists(renamedFolder))

    // Let's rename the folder:
    val messageThrown = intercept[IllegalArgumentException] {
      HdfsHelper.moveFile(folderToMove, renamedFolder)
    }
    val expectedMessage =
      "requirement failed: to move a folder, prefer using the " +
        "moveFolder() method."
    assert(messageThrown.getMessage === expectedMessage)

    assert(HdfsHelper.fileExists(s"$folderToMove/file_1.txt"))
    assert(HdfsHelper.fileExists(s"$folderToMove/file_2.txt"))
    assert(!HdfsHelper.folderExists(renamedFolder))

    // 2: Let's successfuly move the folder with the moveFolder() method:

    // Let's rename the folder:
    HdfsHelper.moveFolder(folderToMove, renamedFolder)

    assert(!HdfsHelper.folderExists(folderToMove))
    assert(HdfsHelper.fileExists(s"$renamedFolder/file_1.txt"))
    assert(HdfsHelper.fileExists(s"$renamedFolder/file_2.txt"))

    val newContent = sc.textFile(renamedFolder).collect().sorted
    assert(newContent === Array("something", "whatever"))

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Append header and footer to file") {

    val filePath = s"$testFolder/header_footer_file.txt"
    val tmpFolder = s"$testFolder/header_footer_tmp"

    // 1: Without the tmp/working folder:

    HdfsHelper.deleteFolder(testFolder)

    // Let's create the file for which to add header and footer:
    HdfsHelper.writeToHdfsFile("whatever\nsomething else\n", filePath)

    HdfsHelper.appendHeaderAndFooter(filePath, "my_header", "my_footer")

    var newContent = sc.textFile(filePath).collect.mkString("\n")

    var expectedNewContent =
      "my_header\n" +
        "whatever\n" +
        "something else\n" +
        "my_footer"

    assert(newContent === expectedNewContent)

    HdfsHelper.deleteFile(filePath)

    // 2: With the tmp/working folder:

    // Let's create the file for which to add header and footer:
    HdfsHelper.writeToHdfsFile("whatever\nsomething else\n", filePath)

    HdfsHelper
      .appendHeaderAndFooter(filePath, "my_header", "my_footer", tmpFolder)

    assert(HdfsHelper.folderExists(tmpFolder))
    assert(!HdfsHelper.fileExists(s"$tmpFolder/xml.tmp"))

    newContent = sc.textFile(filePath).collect.mkString("\n")

    expectedNewContent =
      "my_header\n" +
        "whatever\n" +
        "something else\n" +
        "my_footer"

    assert(newContent === expectedNewContent)

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Validate Xml Hdfs file with Xsd") {

    val xmlPath = s"$testFolder/file.xml"

    // 1: Valid xml:
    HdfsHelper.deleteFolder(testFolder)
    HdfsHelper.writeToHdfsFile(
      "<Customer>\n" +
        "	<Age>24</Age>\n" +
        "	<Address>34 thingy street, someplace, sometown</Address>\n" +
        "</Customer>",
      xmlPath
    )
    var xsdFile = getClass.getResource("/some_xml.xsd")
    assert(HdfsHelper.isHdfsXmlCompliantWithXsd(xmlPath, xsdFile))

    // 2: Invalid xml:
    HdfsHelper.deleteFolder(testFolder)
    HdfsHelper.writeToHdfsFile(
      "<Customer>\n" +
        "	<Age>trente</Age>\n" +
        "	<Address>34 thingy street, someplace, sometown</Address>\n" +
        "</Customer>",
      xmlPath
    )
    xsdFile = getClass.getResource("/some_xml.xsd")
    assert(!HdfsHelper.isHdfsXmlCompliantWithXsd(xmlPath, xsdFile))

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Load Typesafe Config from Hdfs") {

    HdfsHelper.deleteFile("src/test/resources/typesafe_config.conf")
    HdfsHelper.writeToHdfsFile(
      "config {\n" +
        "	something = something_else\n" +
        "}",
      "src/test/resources/typesafe_config.conf"
    )

    val config = HdfsHelper
      .loadTypesafeConfigFromHdfs("src/test/resources/typesafe_config.conf")

    assert(config.getString("config.something") === "something_else")

    HdfsHelper.deleteFile("src/test/resources/typesafe_config.conf")
  }

  test("Load Xml file from Hdfs") {

    val xmlPath = s"$testFolder/file.xml"

    HdfsHelper.deleteFolder(testFolder)

    HdfsHelper.writeToHdfsFile(
      "<toptag>\n" +
        "	<sometag value=\"something\">whatever</sometag>\n" +
        "</toptag>",
      xmlPath
    )

    val xmlContent = HdfsHelper.loadXmlFileFromHdfs(xmlPath)

    assert((xmlContent \ "sometag" \ "@value").text === "something")
    assert((xmlContent \ "sometag").text === "whatever")

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Purge folder from too old files/folders") {

    val folderToPurge = s"$testFolder/folder_to_purge"

    HdfsHelper.deleteFolder(testFolder)
    HdfsHelper.createEmptyHdfsFile(s"$folderToPurge/file.txt")
    HdfsHelper.createEmptyHdfsFile(s"$folderToPurge/folder/file.txt")
    assert(HdfsHelper.fileExists(s"$folderToPurge/file.txt"))
    assert(HdfsHelper.folderExists(s"$folderToPurge/folder"))

    HdfsHelper.purgeFolder(folderToPurge, 63)
    assert(HdfsHelper.fileExists(s"$folderToPurge/file.txt"))
    assert(HdfsHelper.folderExists(s"$folderToPurge/folder"))

    HdfsHelper.purgeFolder(folderToPurge, 1)
    assert(HdfsHelper.fileExists(s"$folderToPurge/file.txt"))
    assert(HdfsHelper.folderExists(s"$folderToPurge/folder"))

    val messageThrown = intercept[IllegalArgumentException] {
      HdfsHelper.purgeFolder(folderToPurge, -3)
    }
    val expectedMessage =
      "requirement failed: the purgeAge provided \"-3\" must be superior to 0."
    assert(messageThrown.getMessage === expectedMessage)

    HdfsHelper.purgeFolder(folderToPurge, 0)
    assert(!HdfsHelper.fileExists(s"$folderToPurge/file.txt"))
    assert(!HdfsHelper.folderExists(s"$folderToPurge/folder"))

    HdfsHelper.deleteFolder(testFolder)
  }

  test("Compress hdfs file") {

    val filePath = s"$testFolder/file.txt"

    HdfsHelper.deleteFile(filePath)

    HdfsHelper.writeToHdfsFile("hello\nworld", filePath)
    HdfsHelper.compressFile(filePath, classOf[GzipCodec], true)

    assert(HdfsHelper.fileExists(s"$filePath.gz"))

    // Easy to test with spark, as reading a file with the ".gz" extention
    // forces the read with the compression codec:
    val content = sc.textFile(s"$filePath.gz").collect.sorted
    assert(content === Array("hello", "world"))

    HdfsHelper.deleteFolder(testFolder)
  }
}
