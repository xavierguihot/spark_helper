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

  implicit class RDDExtensions(val rdd: RDD[String]) extends AnyVal {

    /** Saves an RDD in exactly one file.
      *
      * Allows one to save an RDD in one file, while keeping the processing
      * parallelized.
      *
      * {{{ rdd.saveAsSingleTextFile("/my/file/path.txt") }}}
      *
      * @param path the path of the produced file
      */
    def saveAsSingleTextFile(path: String): Unit =
      SparkHelper.saveAsSingleTextFileInternal(rdd, path, None)

    /** Saves an RDD in exactly one file.
      *
      * Allows one to save an RDD in one file, while keeping the processing
      * parallelized.
      *
      * {{{ rdd.saveAsSingleTextFile("/my/file/path.txt", classOf[BZip2Codec]) }}}
      *
      * @param path the path of the produced file
      * @param codec the type of compression to use (for instance
      * classOf[BZip2Codec] or classOf[GzipCodec]))
      */
    def saveAsSingleTextFile(
        path: String,
        codec: Class[_ <: CompressionCodec]
    ): Unit =
      SparkHelper.saveAsSingleTextFileInternal(rdd, path, Some(codec))

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
      * {{{ rdd.saveAsSingleTextFile("/my/file/path.txt", "/my/working/folder/path") }}}
      *
      * @param path the path of the produced file
      * @param workingFolder the path where file manipulations will temporarily
      * happen.
      */
    def saveAsSingleTextFile(path: String, workingFolder: String): Unit =
      SparkHelper.saveAsSingleTextFileWithWorkingFolderInternal(
        rdd,
        path,
        workingFolder,
        None
      )

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
      * rdd.saveAsSingleTextFile("/my/file/path.txt", "/my/working/folder/path", classOf[BZip2Codec])
      * }}}
      *
      * @param path the path of the produced file
      * @param workingFolder the path where file manipulations will temporarily
      * happen.
      * @param codec the type of compression to use (for instance
      * classOf[BZip2Codec] or classOf[GzipCodec]))
      */
    def saveAsSingleTextFile(
        path: String,
        workingFolder: String,
        codec: Class[_ <: CompressionCodec]
    ): Unit =
      SparkHelper.saveAsSingleTextFileWithWorkingFolderInternal(
        rdd,
        path,
        workingFolder,
        Some(codec)
      )

    /** Saves as text file, but by decreasing the nbr of partitions of the output.
      *
      * Same as <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">rdd.saveAsTextFile()</code>
      * , but decreases the nbr of partitions in the output folder before doing
      * so.
      *
      * The result is equivalent to <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">rdd.coalesce(x).saveAsTextFile()</code>
      * , but if <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">x</code>
      * is very low, <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">coalesce</code>
      * would make the processing time explode, wherease this methods keeps the
      * processing parallelized, save as text file and then only merges the
      * result in a lower nbr of partitions.
      *
      * {{{ rdd.saveAsTextFileAndCoalesce("/produced/folder/path/with/only/30/files", 30) }}}
      *
      * @param path the folder where will finally be stored the RDD but spread
      * on only 30 files (where 30 is the value of the finalCoalesceLevel
      * parameter).
      * @param finalCoalesceLevel the nbr of files within the folder at the end
      * of this method.
      */
    def saveAsTextFileAndCoalesce(
        path: String,
        finalCoalesceLevel: Int
    ): Unit = {

      // We remove folders where to store data in case they already exist:
      HdfsHelper.deleteFolder(s"${path}_tmp")
      HdfsHelper.deleteFolder(path)

      // We first save the rdd with the level of coalescence used during the
      // processing. This way the processing is done with the right level of
      // tasks:
      rdd.saveAsTextFile(s"${path}_tmp")

      // Then we read back this tmp folder, apply the coalesce and store it back:
      SparkHelper.decreaseCoalescenceInternal(
        s"${path}_tmp",
        path,
        finalCoalesceLevel,
        rdd.context,
        None
      )
    }

    /** Saves as text file, but by decreasing the nbr of partitions of the output.
      *
      * Same as <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">rdd.saveAsTextFile()</code>
      * , but decreases the nbr of partitions in the output folder before doing
      * so.
      *
      * The result is equivalent to <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">rdd.coalesce(x).saveAsTextFile()</code>
      * , but if <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">x</code>
      * is very low, <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">coalesce</code>
      * would make the processing time explode, wherease this methods keeps the
      * processing parallelized, save as text file and then only merges the
      * result in a lower nbr of partitions.
      *
      * {{{ rdd.saveAsTextFileAndCoalesce("/produced/folder/path/with/only/30/files", 30, classOf[BZip2Codec]) }}}
      *
      * @param path the folder where will finally be stored the RDD but spread
      * on only 30 files (where 30 is the value of the finalCoalesceLevel
      * parameter).
      * @param finalCoalesceLevel the nbr of files within the folder at the end
      * of this method.
      * @param codec the type of compression to use (for instance
      * classOf[BZip2Codec] or classOf[GzipCodec]))
      */
    def saveAsTextFileAndCoalesce(
        path: String,
        finalCoalesceLevel: Int,
        codec: Class[_ <: CompressionCodec]
    ): Unit = {

      // We remove folders where to store data in case they already exist:
      HdfsHelper.deleteFolder(s"${path}_tmp")
      HdfsHelper.deleteFolder(path)

      // We first save the rdd with the level of coalescence used during the
      // processing. This way the processing is done with the right level of
      // tasks:
      rdd.saveAsTextFile(s"${path}_tmp")

      // Then we read back this tmp folder, apply the coalesce and store it back:
      decreaseCoalescenceInternal(
        s"${path}_tmp",
        path,
        finalCoalesceLevel,
        rdd.context,
        Some(codec)
      )
    }
  }

  implicit class PairRDDExtensions(val rdd: RDD[(String, String)])
      extends AnyVal {

    /** Saves and repartitions a key/value RDD on files whose name is the key.
      *
      * Within the provided path, there will be one file per key in the given
      * keyValueRDD. And within a file for a given key are only stored values
      * for this key.
      *
      * As this internally needs to know the nbr of keys, this will have to
      * compute it. If this nbr of keys is known beforehand, it would spare
      * resources to use saveAsTextFileByKey(path: String, keyNbr: Int)
      * instead.
      *
      * This is not scalable. This shouldn't be considered for any data flow
      * with normal or big volumes.
      *
      * {{{ rdd.saveAsTextFileByKey("/my/output/folder/path") }}}
      *
      * @param path the folder where will be storrred key files
      */
    def saveAsTextFileByKey(path: String): Unit =
      SparkHelper.saveAsTextFileByKeyInternal(rdd, path, None, None)

    /** Saves and repartitions a key/value RDD on files whose name is the key.
      *
      * Within the provided path, there will be one file per key in the given
      * keyValueRDD. And within a file for a given key are only stored values
      * for this key.
      *
      * This is not scalable. This shouldn't be considered for any data flow
      * with normal or big volumes.
      *
      * {{{ rdd.saveAsTextFileByKey("/my/output/folder/path", 12) }}}
      *
      * @param path the folder where will be storrred key files
      * @param keyNbr the nbr of expected keys (which is the nbr of outputed
      * files)
      */
    def saveAsTextFileByKey(path: String, keyNbr: Int): Unit =
      SparkHelper.saveAsTextFileByKeyInternal(rdd, path, Some(keyNbr), None)

    /** Saves and repartitions a key/value RDD on files whose name is the key.
      *
      * Within the provided path, there will be one file per key in the given
      * keyValueRDD. And within a file for a given key are only stored values
      * for this key.
      *
      * As this internally needs to know the nbr of keys, this will have to
      * compute it. If this nbr of keys is known beforehand, it would spare
      * resources to use
      * saveAsTextFileByKey(path: String, keyNbr: Int, codec: Class[_ <: CompressionCodec])
      * instead.
      *
      * This is not scalable. This shouldn't be considered for any data flow
      * with normal or big volumes.
      *
      * {{{ rdd.saveAsTextFileByKey("/my/output/folder/path", classOf[BZip2Codec]) }}}
      *
      * @param path the folder where will be storrred key files
      * @param codec the type of compression to use (for instance
      * classOf[BZip2Codec] or classOf[GzipCodec]))
      */
    def saveAsTextFileByKey(
        path: String,
        codec: Class[_ <: CompressionCodec]
    ): Unit =
      SparkHelper.saveAsTextFileByKeyInternal(rdd, path, None, Some(codec))

    /** Saves and repartitions a key/value RDD on files whose name is the key.
      *
      * Within the provided path, there will be one file per key in the given
      * keyValueRDD. And within a file for a given key are only stored values
      * for this key.
      *
      * This is not scalable. This shouldn't be considered for any data flow
      * with normal or big volumes.
      *
      * {{{ rdd.saveAsTextFileByKey("/my/output/folder/path", 12, classOf[BZip2Codec]) }}}
      *
      * @param path the folder where will be storrred key files
      * @param keyNbr the nbr of expected keys (which is the nbr of outputed
      * files)
      * @param codec the type of compression to use (for instance
      * classOf[BZip2Codec] or classOf[GzipCodec]))
      */
    def saveAsTextFileByKey(
        path: String,
        keyNbr: Int,
        codec: Class[_ <: CompressionCodec]
    ): Unit =
      SparkHelper
        .saveAsTextFileByKeyInternal(rdd, path, Some(keyNbr), Some(codec))
  }

  implicit class SparkContextExtensions(val sc: SparkContext) extends AnyVal {

    /** Equivalent to sparkContext.textFile(), but for a specific record delimiter.
      *
      * By default, sparkContext.textFile() will provide one record per line
      * (per '\n'). But what if the format to read considers that one record
      * is stored in more than one line (yml, custom format, ...)?
      *
      * For instance in order to read a yml file, which is a format for which a
      * record (a single entity) is spread other several lines, you can modify
      * the record delimiter with "---\n" instead of "\n". Same goes when
      * reading an xml file where a record might be spread over several lines or
      * worse the whole xml file is one line.
      *
      * {{{
      * // Let's say data we want to use with Spark looks like this (one record
      * // is a customer, but it's spread over several lines):
      * <Customers>\n
      * <Customer>\n
      * <Address>34 thingy street, someplace, sometown</Address>\n
      * </Customer>\n
      * <Customer>\n
      * <Address>12 thingy street, someplace, sometown</Address>\n
      * </Customer>\n
      * </Customers>
      * //Then you can use it this way:
      * val computedRecords = sc.textFile("my/path/to/customers.xml", "<Customer>\n")
      * val expectedRecords = RDD(
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
      * @param path the path of the file to read (folder or file, '*' works
      * as well).
      * @param delimiter the specific record delimiter which replaces "\n"
      * @param maxRecordLength the max length (not sure which unit) of a record
      * before considering the record too long to fit into memory.
      * @return the RDD of records
      */
    def textFile(
        path: String,
        delimiter: String,
        maxRecordLength: String = "1000000"
    ): RDD[String] = {

      val conf = new Configuration(sc.hadoopConfiguration)

      // This configuration sets the record delimiter:
      conf.set("textinputformat.record.delimiter", delimiter)

      // and this one limits the size of one record. This is necessary in order
      // to avoid reading from a corrupted file from which a record could be too
      // long to fit in memory. This way, when reading a corrupted file, this
      // will throw an exception (java.io.IOException - thus catchable) rather
      // than having a messy out of memory which will stop the sparkContext:
      conf
        .set("mapreduce.input.linerecordreader.line.maxlength", maxRecordLength)

      sc.newAPIHadoopFile(
          path,
          classOf[TextInputFormat],
          classOf[LongWritable],
          classOf[Text],
          conf
        )
        .map { case (_, text) => text.toString }
    }
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
    * is the finalCoalesceLevel parameter).
    * @param finalCoalesceLevel the nbr of files within the folder at the end
    * of this method.
    * @param sparkContext the SparkContext
    */
  def decreaseCoalescence(
      highCoalescenceLevelFolder: String,
      lowerCoalescenceLevelFolder: String,
      finalCoalesceLevel: Int,
      sparkContext: SparkContext
  ): Unit =
    decreaseCoalescenceInternal(
      highCoalescenceLevelFolder,
      lowerCoalescenceLevelFolder,
      finalCoalesceLevel,
      sparkContext,
      None)

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
    * is the finalCoalesceLevel parameter).
    * @param finalCoalesceLevel the nbr of files within the folder at the end
    * of this method.
    * @param sparkContext the SparkContext
    * @param codec the type of compression to use (for instance
    * classOf[BZip2Codec] or classOf[GzipCodec]))
    */
  def decreaseCoalescence(
      highCoalescenceLevelFolder: String,
      lowerCoalescenceLevelFolder: String,
      finalCoalesceLevel: Int,
      sparkContext: SparkContext,
      codec: Class[_ <: CompressionCodec]
  ): Unit =
    decreaseCoalescenceInternal(
      highCoalescenceLevelFolder,
      lowerCoalescenceLevelFolder,
      finalCoalesceLevel,
      sparkContext,
      Some(codec)
    )

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
    * @param path the path of the folder (or structure of folders) to read
    * @param sparkContext the SparkContext
    * @return the RDD of records where a record is a tuple containing the path
    * of the file the record comes from and the record itself.
    */
  def textFileWithFileName(
      path: String,
      sparkContext: SparkContext
  ): RDD[(String, String)] = {

    // In order to go through the folder structure recursively:
    sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sparkContext
      .hadoopFile(
        path,
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

  // Internal core:

  private def saveAsSingleTextFileWithWorkingFolderInternal(
      outputRDD: RDD[String],
      path: String,
      workingFolder: String,
      codec: Option[Class[_ <: CompressionCodec]]
  ): Unit = {

    // We chose a random name for the temporary file:
    val temporaryName = Random.alphanumeric.take(10).mkString("")
    val temporaryFile = s"$workingFolder/$temporaryName"

    // We perform the merge into a temporary single text file:
    saveAsSingleTextFileInternal(outputRDD, temporaryFile, codec)

    // And then only we put the resulting file in its final real location:
    HdfsHelper.moveFile(temporaryFile, path, overwrite = true)
  }

  /** Saves RDD in exactly one file.
    *
    * Allows one to save an RDD as one text file, but at the same time to keep
    * the processing parallelized.
    *
    * @param outputRDD the RDD of strings to save as text file
    * @param path the path where to save the file
    * @param compression the compression codec to use (can be left to None)
    */
  private def saveAsSingleTextFileInternal(
      outputRDD: RDD[String],
      path: String,
      codec: Option[Class[_ <: CompressionCodec]]
  ): Unit = {

    val hadoopConfiguration = outputRDD.sparkContext.hadoopConfiguration
    val fileSystem = FileSystem.get(hadoopConfiguration)

    // Classic saveAsTextFile in a temporary folder:
    HdfsHelper.deleteFolder(s"$path.tmp")
    codec match {
      case Some(codec) =>
        outputRDD.saveAsTextFile(s"$path.tmp", codec)
      case None =>
        outputRDD.saveAsTextFile(s"$path.tmp")
    }

    // Merge the folder into a single file:
    HdfsHelper.deleteFile(path)
    FileUtil.copyMerge(
      fileSystem,
      new Path(s"$path.tmp"),
      fileSystem,
      new Path(path),
      true,
      hadoopConfiguration,
      null)
    HdfsHelper.deleteFolder(s"$path.tmp")
  }

  private def saveAsTextFileByKeyInternal(
      rdd: RDD[(String, String)],
      path: String,
      optKeyNbr: Option[Int],
      codec: Option[Class[_ <: CompressionCodec]]
  ): Unit = {

    HdfsHelper.deleteFolder(path)

    // Whether the rdd was already cached or not (used to unpersist it if we
    // have to get the nbr of keys):
    val isCached = rdd.getStorageLevel.useMemory

    // If the nbr of keys isn't provided, we have to get it ourselves:
    val keyNbr = optKeyNbr match {
      case Some(keyNbr) =>
        keyNbr
      case None =>
        if (!isCached)
          rdd.cache()
        rdd.keys.distinct.count.toInt
    }

    val prdd = rdd.partitionBy(new HashPartitioner(keyNbr))

    codec match {
      case Some(codec) =>
        prdd.saveAsHadoopFile(
          path,
          classOf[String],
          classOf[String],
          classOf[KeyBasedOutput],
          codec
        )
      case None =>
        prdd.saveAsHadoopFile(
          path,
          classOf[String],
          classOf[String],
          classOf[KeyBasedOutput]
        )
    }

    if (optKeyNbr.isEmpty && !isCached)
      rdd.unpersist()
  }

  private def decreaseCoalescenceInternal(
      highCoalescenceLevelFolder: String,
      lowerCoalescenceLevelFolder: String,
      finalCoalesceLevel: Int,
      sparkContext: SparkContext,
      codec: Option[Class[_ <: CompressionCodec]]
  ): Unit = {

    val intermediateRDD = sparkContext
      .textFile(highCoalescenceLevelFolder)
      .coalesce(finalCoalesceLevel)

    codec match {
      case Some(codec) =>
        intermediateRDD.saveAsTextFile(lowerCoalescenceLevelFolder, codec)
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
  ): String =
    key.asInstanceOf[String]
}
