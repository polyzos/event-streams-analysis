package io.ipolyzos.utils

import io.ipolyzos.wrapper.SparkSessionWrapper
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait PipelineUtils extends SparkSessionWrapper {

  def readFromKafka(topicName: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", topicName)
      .option("maxOffsetsPerTrigger", "1000")
      .load()
      .select("value")
      .selectExpr("CAST(value AS STRING)")
  }

  def writeToDatalake[T](outputDF: DataFrame, outputPath: String, partitions: List[String]): StreamingQuery = {
    outputDF.writeStream
      .format("delta")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "checkpoint_dir")
      .partitionBy(partitions:_*)
      .start(outputPath)
  }
}
