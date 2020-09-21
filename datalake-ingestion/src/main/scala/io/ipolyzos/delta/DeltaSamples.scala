package io.ipolyzos.delta

import io.ipolyzos.wrapper.SparkSessionWrapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DeltaSamples extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val configs = List(
      ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
      ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    implicit val spark: SparkSession = initSparkSession("accountIngestion", configs)
    import spark.implicits._

    val accountsDF = spark.read
      .format("delta")
      .load("lakehouse/accounts")
      .drop("year")

    accountsDF.show(truncate = false)
    println(accountsDF.count()) // 14641

    val subscriptionsDF = spark.read
      .format("delta")
      .load("lakehouse/subscriptions")
      .drop("year", "month")

    subscriptionsDF.show(truncate = false)
    println(subscriptionsDF.count())  // 55560

    val condition = accountsDF("id") === subscriptionsDF("accountID")
    val accountsWithSubscriptions = accountsDF.join(subscriptionsDF, condition)
    accountsWithSubscriptions.show(truncate = false)
    println(accountsWithSubscriptions.count())  // 55560

  }
}
