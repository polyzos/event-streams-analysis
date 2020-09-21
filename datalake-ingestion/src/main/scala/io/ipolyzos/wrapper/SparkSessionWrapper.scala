package io.ipolyzos.wrapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val mbDivider: Long = 1000000

  private lazy val totalProcessors = Runtime.getRuntime.availableProcessors()
  private lazy val freeMemory = Runtime.getRuntime.freeMemory() / mbDivider
  private lazy val totalMemory = Runtime.getRuntime.totalMemory() / mbDivider
  private lazy val maxMemory = Runtime.getRuntime.maxMemory() / mbDivider

  println(s"Total Available Processors: $totalProcessors")
  println(s"Free Memory: $freeMemory MB")
  println(s"Total Memory: $totalMemory MB")
  println(s"Max Memory: $maxMemory MB")

  def initSparkSession(appName: String,
                       configs: List[(String, String)] = List.empty,
                       master: String = "local[*]",
                       sparkConf: SparkConf = new SparkConf()): SparkSession = {
    val conf = if (configs.isEmpty) sparkConf else new SparkConf().setAll(configs)

    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config(conf)
      .getOrCreate()
    spark
  }
}
