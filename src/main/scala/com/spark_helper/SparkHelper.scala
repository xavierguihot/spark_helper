package com.spark_helper

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.{RDD, HadoopRDD}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat => TextInputFormat2}

import scala.util.Random

/** A facility to deal with RDD/file manipulations based on the Spark API.
  *
  * The goal is to remove the maximum of highly used low-level code from your
  * spark job and replace it with methods fully tested whose name is
  * self-explanatory/readable.
  *
  * A few exemples:
  *
  * {{{
  * // Same as SparkContext.saveAsTextFile, but the result is a single file:
  * SparkHelper.saveAsSingleTextFile(myOutputRDD, "/my/output/file/path.txt")
  * // Same as SparkContext.textFile, but instead of reading one record per
  * // line, it reads records spread over several lines.
  * // This way, xml, json, yml or any multi-line record file format can be used
  * // with Spark:
  * SparkHelper.textFileWithDelimiter("/my/input/folder/path", sparkContext, "---\n")
  * // Same as SparkContext.textFile, but instead of returning an RDD of
  * // records, it returns an RDD of tuples containing both the record and the
  * // path of the file it comes from:
  * SparkHelper.textFileWithFileName("folder", sparkContext)
  * }}}
  *
  * Source <a href="https://github.com/xavierguihot/spark_helper/blob/master/src
  * /main/scala/com/spark_helper/SparkHelper.scala">SparkHelper</a>
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
object SparkHelper extends Serializable {

  /** Saves an RDD in exactly one file.
    *
    * Allows one to save an RDD in one file, while keeping the processing
    * parallelized.
    *
    * {{{ SparkHelper.saveAsSingleTextFile(myRddToStore, "/my/file/path.txt") }}}
    *
    * @param outputRDD the RDD of strings to store in one file
    * @param outputFile the path of the produced file
    */
  def saveAsSingleTextFile(outputRDD: RDD[String], outputFile: String): Unit =
    saveAsSingleTextFileInternal(outputRDD, outputFile, None)

  /** Saves an RDD in exactly one file.
    *
    * Allows one to save an RDD in one file, while keeping the processing
    * parallelized.
    *
    * {{{
    * SparkHelper.saveAsSingleTextFile(
    *   myRddToStore, "/my/file/path.txt", classOf[BZip2Codec])
    * }}}
    *
    * @param outputRDD the RDD of strings to store in one file
    * @param outputFile the path of the produced file
    * @param compressionCodec the type of compression to use (for instance
    * classOf[BZip2Codec] or classOf[GzipCodec]))
    */
  def saveAsSingleTextFile(
      outputRDD: RDD[String],
      outputFile: String,
      compressionCodec: Class[_ <: CompressionCodec]
  ): Unit =
    saveAsSingleTextFileInternal(outputRDD, outputFile, Some(compressionCodec))

  /** Saves an RDD in exactly one file.
    *
    * Allows one to save an RDD in one file, while keeping the processing
    * parallelized.
    *
    * This variant of saveAsSingleTextFile performs the storage in a temporary
    * folder instead of directly in the final output folder. This way the
    * risks of having corrupted files in the real output folder due to cluster
    * interruptions is minimized.
    *
    * {{{
    * SparkHelper.saveAsSingleTextFile(
    *   myRddToStore, "/my/file/path.txt", "/my/working/folder/path")
    * }}}
    *
    * @param outputRDD the RDD of strings to store in one file
    * @param outputFile the path of the produced file
    * @param workingFolder the path where file manipulations will temporarily
    * happen.
    */
  def saveAsSingleTextFile(
      outputRDD: RDD[String],
      outputFile: String,
      workingFolder: String
  ): Unit = {

    saveAsSingleTextFileWithWorkingFolderInternal(
      outputRDD,
      outputFile,
      workingFolder,
      None)
  }

  /** Saves an RDD in exactly one file.
    *
    * Allows one to save an RDD in one file, while keeping the processing
    * parallelized.
    *
    * This variant of saveAsSingleTextFile performs the storage in a temporary
    * folder instead of directly in the final output folder. This way the risks
    * of having corrupted files in the real output folder due to cluster
    * interruptions is minimized.
    *
    * {{{
    * SparkHelper.saveAsSingleTextFile(
    *   myRddToStore,
    *   "/my/file/path.txt",
    *   "/my/working/folder/path",
    *   classOf[BZip2Codec])
    * }}}
    *
    * @param outputRDD the RDD of strings to store in one file
    * @param outputFile the path of the produced file
    * @param workingFolder the path where file manipulations will temporarily
    * happen.
    * @param compressionCodec the type of compression to use (for instance
    * classOf[BZip2Codec] or classOf[GzipCodec]))
    */
  def saveAsSingleTextFile(
      outputRDD: RDD[String],
      outputFile: String,
      workingFolder: String,
      compressionCodec: Class[_ <: CompressionCodec]
  ): Unit = {

    saveAsSingleTextFileWithWorkingFolderInternal(
      outputRDD,
      outputFile,
      workingFolder,
      Some(compressionCodec))
  }

  /** Equivalent to sparkContext.textFile(), but for a specific record delimiter.
    *
    * By default, sparkContext.textFile() will provide one record per line. But
    * what if the format you want to read considers that one record (one entity)
    * is stored in more than one line (yml, xml, ...)?
    *
    * For instance in order to read a yml file, which is a format for which a
    * record (a single entity) is spread other several lines, you can modify the
    * record delimiter with "---\n" instead of "\n". Same goes when reading an
    * xml file where a record might be spread over several lines or worse the
    * whole xml file is one line.
    *
    * {{{
    * // Let's say data we want to use with Spark looks like this (one record is
    * // a customer, but it's spread over several lines):
    * <Customers>\n
    * <Customer>\n
    * <Address>34 thingy street, someplace, sometown</Address>\n
    * </Customer>\n
    * <Customer>\n
    * <Address>12 thingy street, someplace, sometown</Address>\n
    * </Customer>\n
    * </Customers>
    * //Then you can use it this way:
    * val computedRecords = SparkHelper.textFileWithDelimiter(
    *   "my/path/to/customers.xml", sparkContext, <Customer>\n
    * ).collect()
    * val expectedRecords = Array(
    *   <Customers>\n,
    *   (
    *     <Address>34 thingy street, someplace, sometown</Address>\n +
    *     </Customer>\n
    *   ),
    *   (
    *     <Address>12 thingy street, someplace, sometown</Address>\n +
    *     </Customer>\n +
    *     </Customers>
    *   )
    * )
    * assert(computedRecords == expectedRecords)
    * }}}
    *
    * @param hdfsPath the path of the file to read (folder or file, '*' works as
    * well).
    * @param sparkContext the SparkContext
    * @param delimiter the specific record delimiter which replaces "\n"
    * @param maxRecordLength the max length (not sure which unit) of a record
    * before considering the record too long to fit into memory.
    * @return the RDD of records
    */
  def textFileWithDelimiter(
      hdfsPath: String,
      sparkContext: SparkContext,
      delimiter: String,
      maxRecordLength: String = "1000000"
  ): RDD[String] = {

    val conf = new Configuration(sparkContext.hadoopConfiguration)

    // This configuration sets the record delimiter:
    conf.set("textinputformat.record.delimiter", delimiter)

    // and this one limits the size of one record. This is necessary in order to
    // avoid reading from a corrupted file from which a record could be too long
    // to fit in memory. This way, when reading a corrupted file, this will
    // throw an exception (java.io.IOException - thus catchable) rather than
    // having a messy out of memory which will stop the sparkContext:
    conf.set("mapreduce.input.linerecordreader.line.maxlength", maxRecordLength)

    sparkContext
      .newAPIHadoopFile(
        hdfsPath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        conf
      )
      .map { case (_, text) => text.toString }
  }

  /** Saves and repartitions a key/value RDD on files whose name is the key.
    *
    * Within the provided outputFolder, will be one file per key in your
    * keyValueRDD. And within a file for a given key are only values for this
    * key.
    *
    * You need to know the nbr of keys beforehand (in general you use this to
    * split your dataset in subsets, or to output one file per client, so you
    * know how many keys you have). So you need to put as keyNbr the exact nbr
    * of keys you'll have.
    *
    * This is not scalable. This shouldn't be considered for any data flow with
    * normal or big volumes.
    *
    * {{{
    * SparkHelper.saveAsTextFileByKey(
    *   myKeyValueRddToStore, "/my/output/folder/path", 12)
    * }}}
    *
    * @param keyValueRDD the key/value RDD
    * @param outputFolder the foldder where will be storrred key files
    * @param keyNbr the nbr of expected keys (which is the nbr of outputed files)
    */
  def saveAsTextFileByKey(
      keyValueRDD: RDD[(String, String)],
      outputFolder: String,
      keyNbr: Int
  ): Unit = {

    HdfsHelper.deleteFolder(outputFolder)

    keyValueRDD
      .partitionBy(new HashPartitioner(keyNbr))
      .saveAsHadoopFile(
        outputFolder,
        classOf[String],
        classOf[String],
        classOf[KeyBasedOutput]
      )
  }

  /** Saves and repartitions a key/value RDD on files whose name is the key.
    *
    * Within the provided outputFolder, will be one file per key in your
    * keyValueRDD. And within a file for a given key are only values for this
    * key.
    *
    * You need to know the nbr of keys beforehand (in general you use this to
    * split your dataset in subsets, or to output one file per client, so you
    * know how many keys you have). So you need to put as keyNbr the exact nbr
    * of keys you'll have.
    *
    * This is not scalable. This shouldn't be considered for any data flow with
    * normal or big volumes.
    *
    * {{{
    * SparkHelper.saveAsTextFileByKey(
    *   myKeyValueRddToStore, "/my/output/folder/path", 12, classOf[BZip2Codec])
    * }}}
    *
    * @param keyValueRDD the key/value RDD
    * @param outputFolder the foldder where will be storrred key files
    * @param keyNbr the nbr of expected keys (which is the nbr of outputed files)
    * @param compressionCodec the type of compression to use (for instance
    * classOf[BZip2Codec] or classOf[GzipCodec]))
    */
  def saveAsTextFileByKey(
      keyValueRDD: RDD[(String, String)],
      outputFolder: String,
      keyNbr: Int,
      compressionCodec: Class[_ <: CompressionCodec]
  ): Unit = {

    HdfsHelper.deleteFolder(outputFolder)

    keyValueRDD
      .partitionBy(new HashPartitioner(keyNbr))
      .saveAsHadoopFile(
        outputFolder,
        classOf[String],
        classOf[String],
        classOf[KeyBasedOutput],
        compressionCodec
      )
  }

  /** Decreases the nbr of partitions of a folder.
    *
    * This is often handy when the last step of your job needs to run on
    * thousands of files, but you want to store your final output on let's say
    * only 300 files.
    *
    * It's like a FileUtil.copyMerge, but the merging produces more than one
    * file.
    *
    * Be aware that this methods deletes the provided input folder.
    *
    * {{{
    * SparkHelper.decreaseCoalescence(
    *   "/folder/path/with/2000/files",
    *   "/produced/folder/path/with/only/300/files",
    *   300,
    *   sparkContext)
    * }}}
    *
    * @param highCoalescenceLevelFolder the folder which contains 10000 files
    * @param lowerCoalescenceLevelFolder the folder which will contain the same
    * data as highCoalescenceLevelFolder but spread on only 300 files (where 300
    * is the finalCoalescenceLevel parameter).
    * @param finalCoalescenceLevel the nbr of files within the folder at the end
    * of this method.
    * @param sparkContext the SparkContext
    */
  def decreaseCoalescence(
      highCoalescenceLevelFolder: String,
      lowerCoalescenceLevelFolder: String,
      finalCoalescenceLevel: Int,
      sparkContext: SparkContext
  ): Unit = {

    decreaseCoalescenceInternal(
      highCoalescenceLevelFolder,
      lowerCoalescenceLevelFolder,
      finalCoalescenceLevel,
      sparkContext,
      None)
  }

  /** Decreases the nbr of partitions of a folder.
    *
    * This is often handy when the last step of your job needs to run on
    * thousands of files, but you want to store your final output on let's say
    * only 300 files.
    *
    * It's like a FileUtil.copyMerge, but the merging produces more than one
    * file.
    *
    * Be aware that this methods deletes the provided input folder.
    *
    * {{{
    * SparkHelper.decreaseCoalescence(
    *   "/folder/path/with/2000/files",
    *   "/produced/folder/path/with/only/300/files",
    *   300,
    *   sparkContext,
    *   classOf[BZip2Codec])
    * }}}
    *
    * @param highCoalescenceLevelFolder the folder which contains 10000 files
    * @param lowerCoalescenceLevelFolder the folder which will contain the same
    * data as highCoalescenceLevelFolder but spread on only 300 files (where 300
    * is the finalCoalescenceLevel parameter).
    * @param finalCoalescenceLevel the nbr of files within the folder at the end
    * of this method.
    * @param sparkContext the SparkContext
    * @param compressionCodec the type of compression to use (for instance
    * classOf[BZip2Codec] or classOf[GzipCodec]))
    */
  def decreaseCoalescence(
      highCoalescenceLevelFolder: String,
      lowerCoalescenceLevelFolder: String,
      finalCoalescenceLevel: Int,
      sparkContext: SparkContext,
      compressionCodec: Class[_ <: CompressionCodec]
  ): Unit = {

    decreaseCoalescenceInternal(
      highCoalescenceLevelFolder,
      lowerCoalescenceLevelFolder,
      finalCoalescenceLevel,
      sparkContext,
      Some(compressionCodec))
  }

  /** Saves as text file, but by decreasing the nbr of partitions of the output.
    *
    * Same as decreaseCoalescence, but the storage of the RDD in an intermediate
    * folder is included.
    *
    * This still makes the processing parallelized, but the output is coalesced.
    *
    * {{{
    * SparkHelper.saveAsTextFileAndCoalesce(
    *   myRddToStore, "/produced/folder/path/with/only/300/files", 300)
    * }}}
    *
    * @param outputRDD the RDD to store, processed for instance on 10000 tasks
    * (which would thus be stored as 10000 files).
    * @param outputFolder the folder where will finally be stored the RDD but
    * spread on only 300 files (where 300 is the value of the
    * finalCoalescenceLevel parameter).
    * @param finalCoalescenceLevel the nbr of files within the folder at the end
    * of this method.
    */
  def saveAsTextFileAndCoalesce(
      outputRDD: RDD[String],
      outputFolder: String,
      finalCoalescenceLevel: Int
  ): Unit = {

    val sparkContext = outputRDD.context

    // We remove folders where to store data in case they already exist:
    HdfsHelper.deleteFolder(outputFolder + "_tmp")
    HdfsHelper.deleteFolder(outputFolder)

    // We first save the rdd with the level of coalescence used during the
    // processing. This way the processing is done with the right level of
    // tasks:
    outputRDD.saveAsTextFile(outputFolder + "_tmp")

    // Then we read back this tmp folder, apply the coalesce and store it back:
    decreaseCoalescenceInternal(
      outputFolder + "_tmp",
      outputFolder,
      finalCoalescenceLevel,
      sparkContext,
      None)
  }

  /** Saves as text file, but by decreasing the nbr of partitions of the output.
    *
    * Same as decreaseCoalescence, but the storage of the RDD in an intermediate
    * folder is included.
    *
    * This still makes the processing parallelized, but the output is coalesced.
    *
    * {{{
    * SparkHelper.saveAsTextFileAndCoalesce(
    *   myRddToStore,
    *   "/produced/folder/path/with/only/300/files",
    *   300,
    *   classOf[BZip2Codec])
    * }}}
    *
    * @param outputRDD the RDD to store, processed for instance on 10000 tasks
    * (which would thus be stored as 10000 files).
    * @param outputFolder the folder where will finally be stored the RDD but
    * spread on only 300 files (where 300 is the value of the
    * finalCoalescenceLevel parameter).
    * @param finalCoalescenceLevel the nbr of files within the folder at the end
    * of this method.
    * @param compressionCodec the type of compression to use (for instance
    * classOf[BZip2Codec] or classOf[GzipCodec]))
    */
  def saveAsTextFileAndCoalesce(
      outputRDD: RDD[String],
      outputFolder: String,
      finalCoalescenceLevel: Int,
      compressionCodec: Class[_ <: CompressionCodec]
  ): Unit = {

    val sparkContext = outputRDD.context

    // We remove folders where to store data in case they already exist:
    HdfsHelper.deleteFolder(outputFolder + "_tmp")
    HdfsHelper.deleteFolder(outputFolder)

    // We first save the rdd with the level of coalescence used during the
    // processing. This way the processing is done with the right level of
    // tasks:
    outputRDD.saveAsTextFile(outputFolder + "_tmp")

    // Then we read back this tmp folder, apply the coalesce and store it back:
    decreaseCoalescenceInternal(
      outputFolder + "_tmp",
      outputFolder,
      finalCoalescenceLevel,
      sparkContext,
      Some(compressionCodec))
  }

  /** Equivalent to sparkContext.textFile(), but for each line is associated
    * with its file path.
    *
    * Produces a RDD[(file_name, line)] which provides a way to know from which
    * file a given line comes from.
    *
    * {{{
    * // Considering this folder:
    * // folder/file_1.txt whose content is data1\ndata2\ndata3
    * // folder/file_2.txt whose content is data4\ndata4
    * // folder/folder_1/file_3.txt whose content is data6\ndata7
    * // then:
    * SparkHelper.textFileWithFileName("folder", sparkContext)
    * // will return:
    * RDD(
    *   ("file:/path/on/machine/folder/file_1.txt", "data1"),
    *   ("file:/path/on/machine/folder/file_1.txt", "data2"),
    *   ("file:/path/on/machine/folder/file_1.txt", "data3"),
    *   ("file:/path/on/machine/folder/file_2.txt", "data4"),
    *   ("file:/path/on/machine/folder/file_2.txt", "data5"),
    *   ("file:/path/on/machine/folder/folder_1/file_3.txt", "data6"),
    *   ("file:/path/on/machine/folder/folder_1/file_3.txt", "data7")
    * )
    * }}}
    *
    * @param hdfsPath the path of the folder (or structure of folders) to read
    * @param sparkContext the SparkContext
    * @return the RDD of records where a record is a tuple containing the path
    * of the file the record comes from and the record itself.
    */
  def textFileWithFileName(
      hdfsPath: String,
      sparkContext: SparkContext
  ): RDD[(String, String)] = {

    // In order to go through the folder structure recursively:
    sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sparkContext
      .hadoopFile(
        hdfsPath,
        classOf[TextInputFormat2],
        classOf[LongWritable],
        classOf[Text],
        sparkContext.defaultMinPartitions
      )
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit {
        case (inputSplit, iterator) =>
          val file = inputSplit.asInstanceOf[FileSplit]
          iterator.map(tpl => (file.getPath.toString, tpl._2.toString))
      }
  }

  //////
  // Internal core:
  //////

  private def saveAsSingleTextFileWithWorkingFolderInternal(
      outputRDD: RDD[String],
      outputFile: String,
      workingFolder: String,
      compressionCodec: Option[Class[_ <: CompressionCodec]]
  ): Unit = {

    // We chose a random name for the temporary file:
    val temporaryName = Random.alphanumeric.take(10).mkString("")
    val temporaryFile = workingFolder + "/" + temporaryName

    // We perform the merge into a temporary single text file:
    saveAsSingleTextFileInternal(outputRDD, temporaryFile, compressionCodec)

    // And then only we put the resulting file in its final real location:
    HdfsHelper.moveFile(temporaryFile, outputFile, overwrite = true)
  }

  /** Saves RDD in exactly one file.
    *
    * Allows one to save an RDD as one text file, but at the same time to keep
    * the processing parallelized.
    *
    * @param outputRDD the RDD of strings to save as text file
    * @param outputFile the path where to save the file
    * @param compression the compression codec to use (can be left to None)
    */
  private def saveAsSingleTextFileInternal(
      outputRDD: RDD[String],
      outputFile: String,
      compressionCodec: Option[Class[_ <: CompressionCodec]]
  ): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    // Classic saveAsTextFile in a temporary folder:
    HdfsHelper.deleteFolder(outputFile + ".tmp")
    compressionCodec match {
      case Some(compressionCodec) =>
        outputRDD.saveAsTextFile(outputFile + ".tmp", compressionCodec)
      case None =>
        outputRDD.saveAsTextFile(outputFile + ".tmp")
    }

    // Merge the folder into a single file:
    HdfsHelper.deleteFile(outputFile)
    FileUtil.copyMerge(
      fileSystem,
      new Path(outputFile + ".tmp"),
      fileSystem,
      new Path(outputFile),
      true,
      new Configuration(),
      null)
    HdfsHelper.deleteFolder(outputFile + ".tmp")
  }

  private def decreaseCoalescenceInternal(
      highCoalescenceLevelFolder: String,
      lowerCoalescenceLevelFolder: String,
      finalCoalescenceLevel: Int,
      sparkContext: SparkContext,
      compressionCodec: Option[Class[_ <: CompressionCodec]]
  ): Unit = {

    val intermediateRDD = sparkContext
      .textFile(highCoalescenceLevelFolder)
      .coalesce(finalCoalescenceLevel)

    compressionCodec match {
      case Some(compressionCodec) =>
        intermediateRDD
          .saveAsTextFile(lowerCoalescenceLevelFolder, compressionCodec)
      case None =>
        intermediateRDD.saveAsTextFile(lowerCoalescenceLevelFolder)
    }

    HdfsHelper.deleteFolder(highCoalescenceLevelFolder)
  }
}

private class KeyBasedOutput extends MultipleTextOutputFormat[Any, Any] {

  override def generateActualKey(key: Any, value: Any): Any = NullWritable.get()

  override def generateFileNameForKeyValue(
      key: Any,
      value: Any,
      name: String
  ): String = {
    key.asInstanceOf[String]
  }
}
