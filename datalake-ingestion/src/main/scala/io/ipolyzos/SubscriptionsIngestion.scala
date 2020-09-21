package io.ipolyzos

import io.ipolyzos.models.UserDomain.Subscription
import io.ipolyzos.utils.PipelineUtils
import org.apache.spark.sql.SparkSession

object SubscriptionsIngestion extends PipelineUtils {

  import io.circe.generic.auto._
  import org.apache.spark.sql.functions._
  import io.ipolyzos.formatters.CustomFormatters._

  private lazy val lakehouseDir = "lakehouse"
  def main(args: Array[String]): Unit = {
    val configs = List(
      ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
      ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    implicit val spark: SparkSession = initSparkSession("subscriptionsIngestion", configs)
    import spark.implicits._

    val inputStream = readFromKafka("subscriptions")
    val parsedStream = inputStream
      .as[String]
      .map { value =>
        val s = value
        io.circe.parser.decode[Subscription](s).right.get
      }
      .withColumn("year", year($"startDate"))
      .withColumn("month", month($"startDate"))

    val query = writeToDatalake[Subscription](parsedStream, lakehouseDir + "/subscriptions", List("year", "month"))
    query.awaitTermination()
  }
}
