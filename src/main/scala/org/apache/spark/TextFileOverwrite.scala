package org.apache.spark

import org.apache.spark.rdd.{RDD, HadoopRDD}
import org.apache.spark.util.SerializableConfiguration
import org.apache.hadoop.mapred.{FileInputFormat, JobConf, TextInputFormat}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.fs.Path

object TextFileOverwrite {

  def textFile(
      paths: Seq[String],
      minPartitions: Int,
      sc: SparkContext
  ): RDD[String] = {

    /* Private notes:
     *
     * * Compared to sc.textFile(), the only difference in the implementation is
     * the call to FileInputFormat.setInputPaths which takes Paths in input
     * instead of a comma-separated String.
     *
     * * I use the package org.apache.spark to store this function, because
     * SerializableConfiguration has the visibility private[spark] in spark's
     * code base.
     *
     * * I would have preferred giving Seq[Path] instead of Seq[String] as an
     * input of this method, but Path is not yet Serializable in the current
     * version of hadoop-common used by Spark (it will become Serializable
     * starting version 3 of hadoop-common).
     *
     * * I don't String* (instead of Seq[String]) as for 1 String only it would
     * confuse the compiler as to which sc.textFile to use (the default one or
     * this one).
     */

    val confBroadcast =
      sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))

    val setInputPathsFunc =
      (jobConf: JobConf) =>
        FileInputFormat.setInputPaths(jobConf, paths.map(p => new Path(p)): _*)

    new HadoopRDD(
      sc,
      confBroadcast,
      Some(setInputPathsFunc),
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      minPartitions
    ).map(pair => pair._2.toString)
  }
}
