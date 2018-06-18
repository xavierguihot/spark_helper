package com.spark_helper

import org.apache.spark.TextFileOverwrite
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.{RDD, HadoopRDD}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat => TextInputFormat2}

import scala.reflect.ClassTag

import scala.util.Random

/** A facility to deal with RDD/file manipulations based on the Spark API.
  *
  * The goal is to remove the maximum of highly used low-level code from your
  * spark job and replace it with methods fully tested whose name is
  * self-explanatory/readable.
  *
  * A few examples:
  *
  * {{{
  * import com.spark_helper.SparkHelper._
  *
  * // Same as rdd.saveAsTextFile("path"), but the result is a single file (while
  * // keeping the processing distributed):
  * rdd.saveAsSingleTextFile("/my/output/file/path.txt")
  * rdd.saveAsSingleTextFile("/my/output/file/path.txt", classOf[BZip2Codec])
  *
  * // Same as sc.textFile("path"), but instead of reading one record per line (by
  * // splitting the input with \n), it splits the file in records based on a custom
  * // delimiter. This way, xml, json, yml or any multi-line record file format can
  * // be used with Spark:
  * sc.textFile("/my/input/folder/path", "---\n") // for a yml file for instance
  *
  * // Equivalent to rdd.flatMap(identity) for RDDs of Seqs or Options:
  * rdd.flatten
  *
  * // Equivalent to sc.textFile(), but for each line is tupled with its file path:
  * sc.textFileWithFileName("/my/input/folder/path")
  * // which produces:
  * // RDD(("folder/file_1.txt", "record1fromfile1"), ("folder/file_1.txt", "record2fromfile1"),
  * //    ("folder/file_2.txt", "record1fromfile2"), ...)
  *
  * // In the given folder, this generates one file per key in the given key/value
  * // RDD. Within each file (named from the key) are all values for this key:
  * rdd.saveAsTextFileByKey("/my/output/folder/path")
  *
  * // Concept mapper (the following example transforms RDD(1, 3, 2, 7, 8) into RDD(1, 3, 4, 7, 16)):
  * rdd.partialMap { case a if a % 2 == 0 => 2 * a }
  *
  * // For when input files contain commas and textFile can't handle it:
  * sc.textFile(Seq("path/hello,world.txt", "path/hello_world.txt"))
  * }}}
  *
  * Source <a href="https://github.com/xavierguihot/spark_helper/blob/master/src
  * /main/scala/com/spark_helper/SparkHelper.scala">SparkHelper</a>
  *
  * @todo sc.parallelize[T](elmts: T*) instead of sc.parallelize[T](elmts: Array[T])
  * @author Xavier Guihot
  * @since 2017-02
  */
object SparkHelper extends Serializable {

  implicit class RDDExtensions[T: ClassTag](val rdd: RDD[T]) {

    /** Map an RDD to the same type, by applying a partial function and the
      * identity otherwise.
      *
      * Avoids having <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">case x => x</code>.
      *
      * Similar idea to <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">.collect</code>,
      * but instead of skipping non-matching items, it keeps them as-is.
      *
      * {{{
      * sc.parallelize(Array(1, 3, 2, 7, 8)).partialMap { case a if a % 2 == 0 => 2 * a }
      * // is equivalent to:
      * sc.parallelize(Array(1, 3, 2, 7, 8)).map {
      *   case a if a % 2 == 0 => 2 * a
      *   case a               => a
      * }
      * // in order to map to:
      * sc.parallelize(Array(1, 3, 4, 7, 16))
      * }}}
      *
      * @param pf the partial function to apply
      * @return an rdd of the same type, for which each element is either the
      * application of the partial function where defined or the identity.
      */
    def partialMap(pf: PartialFunction[T, T]): RDD[T] =
      rdd.map {
        case x if pf.isDefinedAt(x) => pf(x)
        case x                      => x
      }

    /** Map an RDD of A to an RDD of (B, A) where B is determined from A.
      *
      * Replaces for example <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">rdd.map(x => (x.smthg, x))</code>
      * with <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">rdd.withKey(_.smthg)</code>.
      *
      * {{{ RDD((1, "a"), (2, "b")).withKey(_._1) // RDD(1, (1, "a")), (2, (2, "b")) }}}
      *
      * @param toKey the function to apply to extract the key
      * @return an rdd of (K, V) from an RDD of V where K is determined by
      * applying toKey on V.
      */
    def withKey[K](toKey: T => K): RDD[(K, T)] = rdd.map(x => (toKey(x), x))
  }

  implicit class StringRDDExtensions(val rdd: RDD[String]) extends AnyVal {

    /** Saves an RDD in exactly one file.
      *
      * Allows one to save an RDD in one file, while keeping the processing
      * distributed.
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
      * distributed.
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
      * distributed.
      *
      * This variant of <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">saveAsSingleTextFile</code>
      * performs the storage in a temporary folder instead of directly in the
      * final output folder. This way the risks of having corrupted files in the
      * real output folder due to cluster interruptions is minimized.
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
      * distributed.
      *
      * This variant of <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">saveAsSingleTextFile</code>
      * performs the storage in a temporary folder instead of directly in the
      * final output folder. This way the risks of having corrupted files in the
      * real output folder due to cluster interruptions is minimized.
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
      * would make the processing time explode, whereas this methods keeps the
      * processing distributed, save as text file and then only merges the
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

    /** Saves as text file, and decreases the nbr of output partitions.
      *
      * Same as <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">rdd.saveAsTextFile()</code>
      * , but decreases the nbr of partitions in the output folder before doing
      * so.
      *
      * The result is equivalent to <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">rdd.coalesce(x).saveAsTextFile()</code>
      * , but if <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">x</code>
      * is very low, <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">coalesce</code>
      * would make the processing time explode, whereas this methods keeps the
      * processing distributed, save as text file and then only merges the
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

  implicit class SeqRDDExtensions[T: ClassTag](val rdd: RDD[Seq[T]]) {

    /** Flattens an RDD of <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">Seq[T]</code>
      * to <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">RDD[T]</code>.
      *
      * {{{ sc.parallelize(Array(Seq(1, 2, 3), Nil, Seq(4))).flatten == sc.parallelize(Array(Seq(1, 2, 3, 4))) }}}
      *
      * @return the flat RDD as <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">RDD.flatMap(identity)</code>
      * or <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">List.flatten</code>
      * would have.
      */
    def flatten: RDD[T] = rdd.flatMap(identity)
  }

  implicit class OptionRDDExtensions[T: ClassTag](val rdd: RDD[Option[T]]) {

    /** Flattens an RDD of <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">Option[T]</code>
      * to <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">RDD[T]</code>.
      *
      * {{{ sc.parallelize(Array(Some(1), None, Some(2))).flatten == sc.parallelize(Array(Seq(1, 2))) }}}
      *
      * @return the flat RDD as <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">RDD.flatMap(x => x)</code>
      * or <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">List.flatten</code>
      * would have.
      */
    def flatten: RDD[T] = rdd.flatMap(o => o)
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
      * resources to use <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">saveAsTextFileByKey(path: String, keyNbr: Int)</code>
      * instead.
      *
      * This is not scalable. This shouldn't be considered for any data flow
      * with normal or big volumes.
      *
      * {{{ rdd.saveAsTextFileByKey("/my/output/folder/path") }}}
      *
      * @param path the folder where will be stored key files
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
      * @param path the folder where will be stored key files
      * @param keyNbr the nbr of expected keys (which is the nbr of output
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
      * <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">saveAsTextFileByKey(path: String, keyNbr: Int, codec: Class[_ <: CompressionCodec])</code>
      * instead.
      *
      * This is not scalable. This shouldn't be considered for any data flow
      * with normal or big volumes.
      *
      * {{{ rdd.saveAsTextFileByKey("/my/output/folder/path", classOf[BZip2Codec]) }}}
      *
      * @param path the folder where will be stored key files
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
      * @param path the folder where will be stored key files
      * @param keyNbr the nbr of expected keys (which is the nbr of output
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

    /** Equivalent to <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">sparkContext.textFile()</code>
      * , but for a specific record delimiter.
      *
      * By default, <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">sparkContext.textFile()</code>
      * will provide one record per line (per <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">'\n'</code>).
      * But what if the format to read considers that one record is stored in
      * more than one line (yml, custom format, ...)?
      *
      * For instance in order to read a yml file, which is a format for which a
      * record (a single entity) is spread other several lines, you can modify
      * the record delimiter with <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">"---\n"</code>
      * instead of <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">"\n"</code>.
      * Same goes when reading an xml file where a record might be spread over
      * several lines or worse the whole xml file is one line.
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

    /** Equivalent to <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">sparkContext.textFile()</code>
      * , but each record is associated with the file path it comes from.
      *
      * Produces an <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">RDD[(file_name, line)]</code>
      * which provides a way to know from which file a given line comes from.
      *
      * {{{
      * // Considering this folder:
      * // folder/file_1.txt whose content is data1\ndata2\ndata3
      * // folder/file_2.txt whose content is data4\ndata4
      * // folder/folder_1/file_3.txt whose content is data6\ndata7
      * // then:
      * sc.textFileWithFileName("folder")
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
      * @return the RDD of records where a record is a tuple containing the path
      * of the file the record comes from and the record itself.
      */
    def textFileWithFileName(path: String): RDD[(String, String)] = {

      // In order to go through the folder structure recursively:
      sc.hadoopConfiguration
        .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

      sc.hadoopFile(
          path,
          classOf[TextInputFormat2],
          classOf[LongWritable],
          classOf[Text],
          sc.defaultMinPartitions
        )
        .asInstanceOf[HadoopRDD[LongWritable, Text]]
        .mapPartitionsWithInputSplit {
          case (inputSplit, iterator) =>
            val file = inputSplit.asInstanceOf[FileSplit]
            iterator.map(tpl => (file.getPath.toString, tpl._2.toString))
        }

      // An other way of doing would be:
      //
      // import org.apache.spark.sql.functions.input_file_name
      // import spark.implicits._
      //
      // spark.read
      //   .text(testFolder)
      //   .select(input_file_name, $"value")
      //   .as[(String, String)]
      //   .rdd
    }

    /** A replacement for <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">sc.textFile()</code>
      * when files contains commas in their name.
      *
      * As <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">sc.textFile()</code>
      * allows to provide several files at once by giving them as a string which
      * is a list of strings joined with <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">,</code>,
      * we can't give it files containing commas in their name.
      *
      * This method aims at bypassing this limitation by passing paths as a
      * sequence of strings.
      *
      * {{{ sc.textFile(Seq("path/hello,world.txt", "path/hello_world.txt")) }}}
      *
      * @param paths the paths of the file(s)/folder(s) to read
      */
    def textFile(paths: Seq[String]): RDD[String] =
      TextFileOverwrite.textFile(paths, sc.defaultMinPartitions, sc)

    /** A replacement for <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">sc.textFile()</code>
      * when files contains commas in their name.
      *
      * As <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">sc.textFile()</code>
      * allows to provide several files at once by giving them as a string which
      * is a list of strings joined with <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">,</code>,
      * we can't give it files containing commas in their name.
      *
      * This method aims at bypassing this limitation by passing paths as a
      * sequence of strings.
      *
      * {{{ sc.textFile(Seq("path/hello,world.txt", "path/hello_world.txt")) }}}
      *
      * @param paths the paths of the file(s)/folder(s) to read
      * @param minPartitions the nbr of partitions in which to split the input
      */
    def textFile(paths: Seq[String], minPartitions: Int): RDD[String] =
      TextFileOverwrite.textFile(paths, minPartitions, sc)

    /** Decreases the nbr of partitions of a folder.
      *
      * This comes in handy when the last step of your job needs to run on
      * thousands of files, but you want to store your final output on let's say
      * only 30 files.
      *
      * It's like a <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">FileUtil.copyMerge()</code>
      * , but the merging produces more than one file.
      *
      * Be aware that this methods deletes the provided input folder.
      *
      * {{{
      * sc.decreaseCoalescence(
      *   "/folder/path/with/2000/files",
      *   "/produced/folder/path/with/only/30/files",
      *   30
      * )
      * }}}
      *
      * @param highCoalescenceLevelFolder the folder which contains 10000 files
      * @param lowerCoalescenceLevelFolder the folder which will contain the same
      * data as highCoalescenceLevelFolder but spread on only 30 files (where 30
      * is the finalCoalesceLevel parameter).
      * @param finalCoalesceLevel the nbr of files within the folder at the end
      * of this method.
      */
    def decreaseCoalescence(
        highCoalescenceLevelFolder: String,
        lowerCoalescenceLevelFolder: String,
        finalCoalesceLevel: Int
    ): Unit =
      SparkHelper.decreaseCoalescenceInternal(
        highCoalescenceLevelFolder,
        lowerCoalescenceLevelFolder,
        finalCoalesceLevel,
        sc,
        None
      )

    /** Decreases the nbr of partitions of a folder.
      *
      * This comes in handy when the last step of your job needs to run on
      * thousands of files, but you want to store your final output on let's say
      * only 30 files.
      *
      * It's like a <code style="background-color:#eff0f1;padding:1px 5px;font-size:12px">FileUtil.copyMerge()</code>
      * , but the merging produces more than one file.
      *
      * Be aware that this methods deletes the provided input folder.
      *
      * {{{
      * sc.decreaseCoalescence(
      *   "/folder/path/with/2000/files",
      *   "/produced/folder/path/with/only/30/files",
      *   30,
      *   classOf[BZip2Codec]
      * )
      * }}}
      *
      * @param highCoalescenceLevelFolder the folder which contains 10000 files
      * @param lowerCoalescenceLevelFolder the folder which will contain the same
      * data as highCoalescenceLevelFolder but spread on only 30 files (where 30
      * is the finalCoalesceLevel parameter).
      * @param finalCoalesceLevel the nbr of files within the folder at the end
      * of this method.
      * @param codec the type of compression to use (for instance
      * classOf[BZip2Codec] or classOf[GzipCodec]))
      */
    def decreaseCoalescence(
        highCoalescenceLevelFolder: String,
        lowerCoalescenceLevelFolder: String,
        finalCoalesceLevel: Int,
        codec: Class[_ <: CompressionCodec]
    ): Unit =
      SparkHelper.decreaseCoalescenceInternal(
        highCoalescenceLevelFolder,
        lowerCoalescenceLevelFolder,
        finalCoalesceLevel,
        sc,
        Some(codec)
      )
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
    * the processing distributed.
    *
    * @param outputRDD the RDD of strings to save as text file
    * @param path the path where to save the file
    * @param codec the compression codec to use (can be left to None)
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
      case Some(compression) =>
        outputRDD.saveAsTextFile(s"$path.tmp", compression)
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
    val keyNbr = optKeyNbr.getOrElse {
      if (!isCached)
        rdd.cache()
      rdd.keys.distinct.count.toInt
    }

    val prdd = rdd.partitionBy(new HashPartitioner(keyNbr))

    codec match {
      case Some(compression) =>
        prdd.saveAsHadoopFile(
          path,
          classOf[String],
          classOf[String],
          classOf[KeyBasedOutput],
          compression
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
      sc: SparkContext,
      codec: Option[Class[_ <: CompressionCodec]]
  ): Unit = {

    val intermediateRDD = sc
      .textFile(highCoalescenceLevelFolder)
      .coalesce(finalCoalesceLevel)

    codec match {
      case Some(compression) =>
        intermediateRDD.saveAsTextFile(lowerCoalescenceLevelFolder, compression)
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
