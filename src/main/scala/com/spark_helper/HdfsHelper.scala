package com.spark_helper

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.io.compress.{GzipCodec, BZip2Codec}
import org.apache.hadoop.io.IOUtils

import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat

import scala.xml.Elem
import javax.xml.transform.stream.StreamSource
import javax.xml.validation._
import scala.xml.XML
import javax.xml.XMLConstants

import org.xml.sax.SAXException

import java.net.URL

import java.io.InputStreamReader

import com.typesafe.config.{Config, ConfigFactory}

/** A facility to deal with file manipulations (wrapper around hdfs apache
  * Hadoop FileSystem API <a href="https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html">org.apache.hadoop.fs.FileSystem</a>).
  *
  * The goal is to remove the maximum of highly used low-level code from your
  * spark job and replace it with methods fully tested whose name is
  * self-explanatory/readable.
  *
  * For instance, one don't want to remove a file from hdfs using 3 lines of
  * code and thus could instead just use
  * HdfsHelper.deleteFile("my/hdfs/file/path.csv").
  *
  * A few exemples:
  *
  * {{{
  * import com.spark_helper.HdfsHelper
  *
  * // A bunch of methods wrapping the FileSystem API, such as:
  * HdfsHelper.fileExists("my/hdfs/file/path.txt")
  * assert(HdfsHelper.listFileNamesInFolder("my/folder/path") == List("file_name_1.txt", "file_name_2.csv"))
  * assert(HdfsHelper.fileModificationDate("my/hdfs/file/path.txt") == "20170306")
  * assert(HdfsHelper.nbrOfDaysSinceFileWasLastModified("my/hdfs/file/path.txt") == 3)
  * HdfsHelper.deleteFile("my/hdfs/file/path.csv")
  * HdfsHelper.moveFolder("my/hdfs/folder")
  * HdfsHelper.compressFile("hdfs/path/to/uncompressed_file.txt", classOf[GzipCodec])
  * HdfsHelper.appendHeader("my/hdfs/file/path.csv", "colum0,column1")
  *
  * // Some Xml/Typesafe helpers for hadoop as well:
  * HdfsHelper.isHdfsXmlCompliantWithXsd(
  *   "my/hdfs/file/path.xml", getClass.getResource("/some_xml.xsd"))
  * HdfsHelper.loadXmlFileFromHdfs("my/hdfs/file/path.xml")
  *
  * // Very handy to load a config (typesafe format) stored on hdfs at the
  * // begining of a spark job:
  * HdfsHelper.loadTypesafeConfigFromHdfs("my/hdfs/file/path.conf"): Config
  *
  * // In order to write small amount of data in a file on hdfs without the
  * // whole spark stack:
  * HdfsHelper.writeToHdfsFile(
  *   Array("some", "relatively small", "text"),
  *   "/some/hdfs/file/path.txt")
  *
  * // Deletes all files/folders in "hdfs/path/to/folder" for which the
  * // timestamp is older than 10 days:
  * HdfsHelper.purgeFolder("hdfs/path/to/folder", 10)
  * }}}
  *
  * Source <a href="https://github.com/xavierguihot/spark_helper/blob/master/src
  * /main/scala/com/spark_helper/HdfsHelper.scala">HdfsHelper</a>
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
object HdfsHelper extends Serializable {

  /** Deletes a file on HDFS.
    *
    * Doesn't throw an exception if the file to delete doesn't exist.
    *
    * @param hdfsPath the path of the file to delete
    */
  def deleteFile(hdfsPath: String): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val fileToDelete = new Path(hdfsPath)

    if (fileSystem.exists(fileToDelete)) {

      require(
        fileSystem.isFile(fileToDelete),
        "to delete a folder, prefer using the deleteFolder() method.")

      fileSystem.delete(fileToDelete, true)
    }
  }

  /** Deletes a folder on HDFS.
    *
    * Doesn't throw an exception if the folder to delete doesn't exist.
    *
    * @param hdfsPath the path of the folder to delete
    */
  def deleteFolder(hdfsPath: String): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val folderToDelete = new Path(hdfsPath)

    if (fileSystem.exists(folderToDelete)) {

      require(
        !fileSystem.isFile(folderToDelete),
        "to delete a file, prefer using the deleteFile() method.")

      fileSystem.delete(folderToDelete, true)
    }
  }

  /** Creates a folder on HDFS.
    *
    * Doesn't throw an exception if the folder to create already exists.
    *
    * @param hdfsPath the path of the folder to create
    */
  def createFolder(hdfsPath: String): Unit =
    FileSystem.get(new Configuration()).mkdirs(new Path(hdfsPath))

  /** Checks if the file exists.
    *
    * @param hdfsPath the path of the file for which we check if it exists
    * @return if the file exists
    */
  def fileExists(hdfsPath: String): Boolean = {

    val fileSystem = FileSystem.get(new Configuration())

    val fileToCheck = new Path(hdfsPath)

    if (fileSystem.exists(fileToCheck))
      require(
        fileSystem.isFile(fileToCheck),
        "to check if a folder exists, prefer using the folderExists() method.")

    fileSystem.exists(fileToCheck)
  }

  /** Checks if the folder exists.
    *
    * @param hdfsPath the path of the folder for which we check if it exists
    * @return if the folder exists
    */
  def folderExists(hdfsPath: String): Boolean = {

    val fileSystem = FileSystem.get(new Configuration())

    val folderToCheck = new Path(hdfsPath)

    if (fileSystem.exists(folderToCheck))
      require(
        !fileSystem.isFile(folderToCheck),
        "to check if a file exists, prefer using the fileExists() method.")

    fileSystem.exists(folderToCheck)
  }

  /** Moves/renames a file.
    *
    * This method deals with performing the "mkdir -p" if the target path has
    * intermediate folders not yet created.
    *
    * @param oldPath the path of the file to rename
    * @param newPath the new path of the file to rename
    * @param overwrite (default = false) if true, enable the overwrite of the
    * destination.
    */
  def moveFile(
      oldPath: String,
      newPath: String,
      overwrite: Boolean = false
  ): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val fileToRename = new Path(oldPath)
    val renamedFile = new Path(newPath)

    if (fileSystem.exists(fileToRename))
      require(
        fileSystem.isFile(fileToRename),
        "to move a folder, prefer using the moveFolder() method.")

    if (overwrite)
      fileSystem.delete(renamedFile, true)
    else
      require(
        !fileSystem.exists(renamedFile),
        "overwrite option set to false, but a file already exists at target " +
          "location " + newPath)

    // Before moving the file to its final destination, we check if the folder
    // where to put the file exists, and if not we create it:
    val targetContainerFolder = newPath.split("/").init.mkString("/")
    createFolder(targetContainerFolder)

    fileSystem.rename(fileToRename, renamedFile)
  }

  /** Moves/renames a folder.
    *
    * This method deals with performing the "mkdir -p" if the target path has
    * intermediate folders not yet created.
    *
    * @param oldPath the path of the folder to rename
    * @param newPath the new path of the folder to rename
    * @param overwrite (default = false) if true, enable the overwrite of the
    * destination.
    */
  def moveFolder(
      oldPath: String,
      newPath: String,
      overwrite: Boolean = false
  ): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val folderToRename = new Path(oldPath)
    val renamedFolder = new Path(newPath)

    if (fileSystem.exists(folderToRename))
      require(
        !fileSystem.isFile(folderToRename),
        "to move a file, prefer using the moveFile() method.")

    if (overwrite)
      fileSystem.delete(renamedFolder, true)
    else
      require(
        !fileSystem.exists(renamedFolder),
        "overwrite option set to false, but a folder already exists at target " +
          "location " + newPath)

    // Before moving the folder to its final destination, we check if the folder
    // where to put the folder exists, and if not we create it:
    val targetContainerFolder = newPath.split("/").init.mkString("/")
    createFolder(targetContainerFolder)

    fileSystem.rename(folderToRename, new Path(newPath))
  }

  /** Creates an empty file on hdfs.
    *
    * Might be usefull for token files. For instance a file which is only used
    * as a timestamp token of the last update of a processus, or a file which
    * blocks the execution of an other instance of the same job, ...
    *
    * Overwrites the file if it already exists.
    *
    * {{{ HdfsHelper.createEmptyHdfsFile("/some/hdfs/file/path.token") }}}
    *
    * In case this is used as a timestamp container, you can then use the
    * following methods to retrieve its timestamp:
    * {{{
    * val fileAge = HdfsHelper.nbrOfDaysSinceFileWasLastModified("/some/hdfs/file/path.token")
    * val lastModificationDate = HdfsHelper.folderModificationDate("/some/hdfs/file/path.token")
    * }}}
    *
    * @param filePath the path of the empty file to create
    */
  def createEmptyHdfsFile(filePath: String): Unit =
    FileSystem.get(new Configuration()).create(new Path(filePath)).close()

  /** Saves text in a file when content is too small to really require an RDD.
    *
    * Please only consider this way of storing data when the data set is small
    * enough.
    *
    * Overwrites the file if it already exists.
    *
    * {{{ HdfsHelper.writeToHdfsFile(
    *   "some\nrelatively small\ntext", "/some/hdfs/file/path.txt") }}}
    *
    * @param content the string to write in the file (you can provide a string
    * with \n in order to write several lines).
    * @param filePath the path of the file in which to write the content
    */
  def writeToHdfsFile(content: String, filePath: String): Unit = {

    val outputFile =
      FileSystem.get(new Configuration()).create(new Path(filePath))

    outputFile.write(content.getBytes("UTF-8"))
    outputFile.close()
  }

  /** Saves text in a file when content is too small to really require an RDD.
    *
    * Please only consider this way of storing data when the data set is small
    * enough.
    *
    * Overwrites the file if it already exists.
    *
    * {{{
    * HdfsHelper.writeToHdfsFile(
    *   Array("some", "relatively small", "text"), "/some/hdfs/file/path.txt")
    * HdfsHelper.writeToHdfsFile(
    *   List("some", "relatively small", "text"), "/some/hdfs/file/path.txt")
    * }}}
    *
    * @param content the array of strings to write in the file as one line per
    * string (this takes care of joining strings with "\n"s).
    * @param filePath the path of the file in which to write the content
    */
  def writeToHdfsFile(content: Seq[String], filePath: String): Unit =
    writeToHdfsFile(content.mkString("\n"), filePath)

  /** Lists file names in the specified hdfs folder.
    *
    * {{{
    * assert(HdfsHelper.listFileNamesInFolder("my/folder/path") == List("file_name_1.txt", "file_name_2.csv"))
    * }}}
    *
    * @param hdfsPath the path of the folder for which to list file names
    * @param recursive (default = false) if true, list files in subfolders as
    * well.
    * @param onlyName (default = true) if false, list paths instead of only name
    * of files.
    * @return the list of file names in the specified folder
    */
  def listFileNamesInFolder(
      hdfsPath: String,
      recursive: Boolean = false,
      onlyName: Boolean = true
  ): List[String] = {

    FileSystem
      .get(new Configuration())
      .listStatus(new Path(hdfsPath))
      .flatMap(status => {

        // If it's a file:
        if (status.isFile) {
          if (onlyName) List(status.getPath.getName)
          else List(hdfsPath + "/" + status.getPath.getName)
        }
        // If it's a dir and we're in a recursive option:
        else if (recursive)
          listFileNamesInFolder(
            hdfsPath + "/" + status.getPath.getName,
            true,
            onlyName)
        // If it's a dir and we're not in a recursive option:
        else
          Nil
      })
      .toList
      .sorted
  }

  /** Lists folder names in the specified hdfs folder.
    *
    * {{{
    * assert(HdfsHelper.listFolderNamesInFolder("my/folder/path") == List("folder_1", "folder_2"))
    * }}}
    *
    * @param hdfsPath the path of the folder for which to list folder names
    * @return the list of folder names in the specified folder
    */
  def listFolderNamesInFolder(hdfsPath: String): List[String] =
    FileSystem
      .get(new Configuration())
      .listStatus(new Path(hdfsPath))
      .filter(!_.isFile)
      .map(_.getPath.getName)
      .toList
      .sorted

  /** Returns the joda DateTime of the last modification of the given file.
    *
    * @param hdfsPath the path of the file for which to get the last
    * modification date.
    * @return the joda DateTime of the last modification of the given file
    */
  def fileModificationDateTime(hdfsPath: String): DateTime =
    new DateTime(
      FileSystem
        .get(new Configuration())
        .getFileStatus(new Path(hdfsPath))
        .getModificationTime())

  /** Returns the stringified date of the last modification of the given file.
    *
    * {{{
    * assert(HdfsHelper.fileModificationDate("my/hdfs/file/path.txt") == "20170306")
    * }}}
    *
    * @param hdfsPath the path of the file for which to get the last
    * modification date.
    * @param format (default = "yyyyMMdd") the format under which to get the
    * modification date.
    * @return the stringified date of the last modification of the given file,
    * under the provided format.
    */
  def fileModificationDate(
      hdfsPath: String,
      format: String = "yyyyMMdd"
  ): String =
    DateTimeFormat.forPattern(format).print(fileModificationDateTime(hdfsPath))

  /** Returns the joda DateTime of the last modification of the given folder.
    *
    * @param hdfsPath the path of the folder for which to get the last
    * modification date.
    * @return the joda DateTime of the last modification of the given folder
    */
  def folderModificationDateTime(hdfsPath: String): DateTime =
    fileModificationDateTime(hdfsPath)

  /** Returns the stringified date of the last modification of the given folder.
    *
    * {{{
    * assert(HdfsHelper.folderModificationDate("my/hdfs/folder") == "20170306")
    * }}}
    *
    * @param hdfsPath the path of the folder for which to get the last
    * modification date.
    * @param format (default = "yyyyMMdd") the format under which to get the
    * modification date.
    * @return the stringified date of the last modification of the given folder,
    * under the provided format.
    */
  def folderModificationDate(
      hdfsPath: String,
      format: String = "yyyyMMdd"
  ): String =
    fileModificationDate(hdfsPath, format)

  /** Returns the nbr of days since the given file has been last modified.
    *
    * {{{
    * assert(HdfsHelper.nbrOfDaysSinceFileWasLastModified("my/hdfs/file/path.txt") == 3)
    * }}}
    *
    * @param hdfsPath the path of the file for which we want the nbr of days
    * since the last modification.
    * @return the nbr of days since the given file has been last modified
    */
  def nbrOfDaysSinceFileWasLastModified(hdfsPath: String): Int =
    Days
      .daysBetween(fileModificationDateTime(hdfsPath), new DateTime())
      .getDays()

  /** Appends a header and a footer to a file.
    *
    * Usefull when creating an xml file with spark and you need to add top level
    * tags.
    *
    * If the workingFolderPath parameter is provided, then the processing is
    * done in a working/tmp folder and then only, the final file is moved to its
    * final real location. This way, in case of cluster instability, i.e. in
    * case the Spark job is interupted, this avoids having a temporary or
    * corrupted file in output.
    *
    * @param filePath the path of the file for which to add the header and the
    * footer.
    * @param header the header to add
    * @param footer the footer to add
    * @param workingFolderPath the path where file manipulations will happen
    */
  def appendHeaderAndFooter(
      filePath: String,
      header: String,
      footer: String,
      workingFolderPath: String = ""
  ): Unit =
    appendHeaderAndFooterInternal(
      filePath,
      Some(header),
      Some(footer),
      workingFolderPath)

  /** Appends a header to a file.
    *
    * Usefull when creating a csv file with spark and you need to add a header
    * describing the different fields.
    *
    * If the workingFolderPath parameter is provided, then the processing is
    * done in a working/tmp folder and then only, the final file is moved to its
    * final real location. This way, in case of cluster instability, i.e. in
    * case the Spark job is interupted, this avoids having a temporary or
    * corrupted file in output.
    *
    * @param filePath the path of the file for which to add the header
    * @param header the header to add
    * @param workingFolderPath the path where file manipulations will happen
    */
  def appendHeader(
      filePath: String,
      header: String,
      workingFolderPath: String = ""
  ): Unit =
    appendHeaderAndFooterInternal(
      filePath,
      Some(header),
      None,
      workingFolderPath)

  /** Appends a footer to a file.
    *
    * If the workingFolderPath parameter is provided, then the processing is
    * done in a working/tmp folder and then only, the final file is moved to its
    * final real location. This way, in case of cluster instability, i.e. in
    * case the Spark job is interupted, this avoids having a temporary or
    * corrupted file in output.
    *
    * @param filePath the path of the file for which to add the footer
    * @param footer the footer to add
    * @param workingFolderPath the path where file manipulations will happen
    */
  def appendFooter(
      filePath: String,
      footer: String,
      workingFolderPath: String = ""
  ): Unit =
    appendHeaderAndFooterInternal(
      filePath,
      None,
      Some(footer),
      workingFolderPath)

  /** Validates an XML file on hdfs in regard to the given XSD.
    *
    * @param hdfsXmlPath the path of the file on hdfs for which to validate the
    * compliance with the given xsd.
    * @param xsdFile the xsd file. The easiest is to put your xsd file within
    * your resources folder (src/main/resources) and then get it as an URL with
    * getClass.getResource("/my_file.xsd").
    * @return if the xml is compliant with the xsd
    */
  def isHdfsXmlCompliantWithXsd(hdfsXmlPath: String, xsdFile: URL): Boolean = {
    try {
      validateHdfsXmlWithXsd(hdfsXmlPath, xsdFile)
      true
    } catch {
      case saxe: SAXException => false
    }
  }

  /** Validates an XML file on hdfs in regard to the given XSD.
    *
    * Returns nothing and don't catch the error if the xml is not valid. This
    * way you can retrieve the error and analyse it.
    *
    * @param hdfsXmlPath the path of the file on hdfs for which to validate the
    * compliance with the given xsd.
    * @param xsdFile the xsd file. The easiest is to put your xsd file within
    * your resources folder (src/main/resources) and then get it as an URL with
    * getClass.getResource("/my_file.xsd").
    */
  def validateHdfsXmlWithXsd(hdfsXmlPath: String, xsdFile: URL): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val xmlFile = new StreamSource(fileSystem.open(new Path(hdfsXmlPath)))

    val schemaFactory =
      SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)

    val validator = schemaFactory.newSchema(xsdFile).newValidator()

    validator.validate(xmlFile)
  }

  /** Loads a typesafe config from Hdfs.
    *
    * The best way to load the configuration of your job from hdfs.
    *
    * Typesafe is a config format which looks like this:
    * {{{
    * config {
    *   airlines = [
    *     {
    *       code = QF
    *       window_size_in_hour = 6
    *       kpis {
    *         search_count_threshold = 25000
    *         popularity_count_threshold = 400
    *       }
    *     }
    *     {
    *       code = AF
    *       window_size_in_hour = 6
    *       kpis {
    *         search_count_threshold = 100000
    *         popularity_count_threshold = 800
    *       }
    *     }
    *   ]
    * }
    * }}}
    *
    * @param hdfsConfigPath the absolute path of the typesafe config file on
    * hdfs we want to load as a typesafe Config object.
    * @return the com.typesafe.config.Config object which contains usable data
    */
  def loadTypesafeConfigFromHdfs(hdfsConfigPath: String): Config = {

    val reader = new InputStreamReader(
      FileSystem.get(new Configuration()).open(new Path(hdfsConfigPath)))

    try { ConfigFactory.parseReader(reader) } finally { reader.close() }
  }

  /** Loads an Xml file from Hdfs as a scala.xml.Elem object.
    *
    * For xml files too big to fit in memory, consider instead using the spark
    * API.
    *
    * @param hdfsXmlPath the path of the xml file on hdfs
    * @return the scala.xml.Elem object
    */
  def loadXmlFileFromHdfs(hdfsXmlPath: String): Elem = {

    val reader = new InputStreamReader(
      FileSystem.get(new Configuration()).open(new Path(hdfsXmlPath)))

    try { XML.load(reader) } finally { reader.close() }
  }

  /** Compresses an Hdfs file to the given codec, without changing the lines
    * order.
    *
    * For instance, after producing an xml for which the order matters, one
    * don't want to use sparkContext.saveAsTextFile with a compression codec due
    * to the resulting compressed file in which lines would be unordered.
    *
    * Here is an example, where hdfs/path/to/uncompressed_file.txt will be
    * compressed and renamed hdfs/path/to/uncompressed_file.txt.gz:
    *
    * {{{
    * HdfsHelper.compressFile("hdfs/path/to/uncompressed_file.txt", classOf[GzipCodec])
    * }}}
    *
    * @param inputPath the path of the file on hdfs to compress
    * @param compressionCodec the type of compression to use (for instance
    * classOf[BZip2Codec] or classOf[GzipCodec])).
    * @param deleteInputFile if the input file is deleted after its compression
    */
  def compressFile(
      inputPath: String,
      compressionCodec: Class[_ <: CompressionCodec],
      deleteInputFile: Boolean = true
  ): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val ClassOfGzip = classOf[GzipCodec]
    val ClassOfBZip2 = classOf[BZip2Codec]

    val outputPath = compressionCodec match {
      case ClassOfGzip  => inputPath + ".gz"
      case ClassOfBZip2 => inputPath + ".bz2"
    }

    val inputStream = fileSystem.open(new Path(inputPath))
    val outputStream = fileSystem.create(new Path(outputPath))

    // The compression code:
    val codec = new CompressionCodecFactory(new Configuration())
      .getCodec(new Path(outputPath))
    // We include the compression codec to the output stream:
    val compressedOutputStream = codec.createOutputStream(outputStream)

    try {
      IOUtils.copyBytes(
        inputStream,
        compressedOutputStream,
        new Configuration(),
        false
      )
    } finally {
      inputStream.close()
      compressedOutputStream.close()
    }

    if (deleteInputFile)
      deleteFile(inputPath)
  }

  /** Deletes in the given folder, the files/folders older than the given
    * threshold (in days).
    *
    * {{{
    * // Deletes all files/folders in "hdfs/path/to/folder" for which the
    * // timestamp is older than 10 days:
    * HdfsHelper.purgeFolder("hdfs/path/to/folder", 10)
    * }}}
    *
    * @param folderPath the path of the folder on hdfs to purge
    * @param purgeAge the threshold (in nbr of days) above which a file is
    * considered too old and thus deleted/purged.
    */
  def purgeFolder(folderPath: String, purgeAge: Int): Unit = {

    require(
      purgeAge >= 0,
      "the purgeAge provided \"" + purgeAge.toString + "\" must be superior to 0.")

    FileSystem
      .get(new Configuration())
      .listStatus(new Path(folderPath))
      .filter(path => {

        val fileAgeInDays = Days
          .daysBetween(new DateTime(path.getModificationTime()), new DateTime())
          .getDays()

        fileAgeInDays >= purgeAge

      })
      .foreach {
        case path if path.isFile =>
          deleteFile(folderPath + "/" + path.getPath.getName)
        case path =>
          deleteFolder(folderPath + "/" + path.getPath.getName)
      }
  }

  /** Internal implementation of the addition to a file of header and footer.
    *
    * @param filePath the path of the file for which to add the header and the
    * footer.
    * @param header the header to add
    * @param footer the footer to add
    * @param workingFolderPath the path where file manipulations will happen
    */
  private def appendHeaderAndFooterInternal(
      filePath: String,
      header: Option[String],
      footer: Option[String],
      workingFolderPath: String
  ): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val tmpOutputPath = workingFolderPath match {
      case "" => filePath + ".tmp"
      case _  => workingFolderPath + "/xml.tmp"
    }
    deleteFile(tmpOutputPath)

    val inputFile = fileSystem.open(new Path(filePath))
    val tmpOutputFile = fileSystem.create(new Path(tmpOutputPath))

    // If there is an header, we add it to the file:
    header.foreach(h => tmpOutputFile.write((h + "\n").getBytes("UTF-8")))

    try {
      IOUtils.copyBytes(inputFile, tmpOutputFile, new Configuration(), false)
    } finally {
      inputFile.close()
    }

    // If there is a footer, we append it to the file:
    footer.foreach(f => tmpOutputFile.write((f + "\n").getBytes("UTF-8")))

    deleteFile(filePath)
    moveFile(tmpOutputPath, filePath)

    tmpOutputFile.close()
  }
}
