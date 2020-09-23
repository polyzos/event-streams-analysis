package io.ipolyzos.generators

import java.sql.{Date, Timestamp}

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.models.UserDomain.{Account, Event, EventType, Subscription}
import io.ipolyzos.serdes.UserDomainSerdes.AccountSerdes.AccountJsonSerializer
import io.ipolyzos.serdes.UserDomainSerdes.EventSerdes.EventJsonSerializer
import io.ipolyzos.serdes.UserDomainSerdes.EventTypeSerdes.EventTypeJsonSerializer
import io.ipolyzos.serdes.UserDomainSerdes.SubscriptionSerdes.SubscriptionJsonSerializer
import io.ipolyzos.utils.AdminUtils
import io.ipolyzos.wrappers.ProducerConfigWrapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source
import scala.util.Try

object DataGenerator extends ProducerConfigWrapper {

  private lazy val dataDir = "commons/src/main/resources/data"

  def populateAccountsData(): Unit = {
    val source = Source.fromFile(dataDir + "/accounts.csv")
    println("Sending account data ...")
    val producer: KafkaProducer[String, Account] = initProducer[String, Account]("accounts-producer", classOf[StringSerializer], classOf[AccountJsonSerializer])

    source.getLines().map { line =>
      val values = line.split(",")
      val country = if (Try(values(3)).isSuccess) Some(values(3)) else Some("")
      Account(values(0).toInt, values(1), Date.valueOf(values(2)), country)
    }.foreach { account =>
      val record = new ProducerRecord[String, Account](KafkaConfig.ACCOUNTS_TOPIC, account.id.toString, account)
      producer.send(record, callback)
    }
    println("Done Sending acount data ...")
    shutdownProducer(producer)
    source.close()
  }

  def populateSubscriptions(): Unit = {
    val source = Source.fromFile(dataDir + "/subscriptions.csv")
    println("Sending subscription data ...")
    val producer: KafkaProducer[String, Subscription] = initProducer[String, Subscription]("subscriptions-producer", classOf[StringSerializer], classOf[SubscriptionJsonSerializer])

    source.getLines().map { line =>
      val values = line.split(",")
      val mrr = if (Try(values(5).toInt).isSuccess) Some(values(3).toInt) else None
      val quantity = if (Try(values(6).toInt).isSuccess) Some(values(3).toInt) else None
      val units = if (Try(values(7).toInt).isSuccess) Some(values(3).toInt) else None
      Subscription(values(0).toInt,
        values(1).toInt,
        values(2),
        Date.valueOf(values(3)),
        Date.valueOf(values(4)),
        mrr,
        quantity,
        units,
        values(8).toInt
      )

    }.foreach { subscription =>
      val record = new ProducerRecord[String, Subscription](KafkaConfig.SUBSCRIPTIONS_TOPIC, subscription.id.toString, subscription)
      producer.send(record, callback)
    }
    println("Done Sending subscription data ...")
    shutdownProducer(producer)
    source.close()
  }

  def populateEventTypesData(): Unit = {
    val source = Source.fromFile(dataDir + "/event_types.csv")
    println("Sending event types data ...")
    val producer: KafkaProducer[String, EventType] = initProducer[String, EventType]("event-types-producer", classOf[StringSerializer], classOf[EventTypeJsonSerializer])

    source.getLines().map { line =>
      val values = line.split(",")
      EventType(values(0).toInt, values(1))
    }.foreach { et =>
      val record = new ProducerRecord[String, EventType](KafkaConfig.EVENT_TYPES_TOPIC, et.eventTypeID.toString, et)
      producer.send(record, callback)
    }
    println("Done event types data ...")
    shutdownProducer(producer)
    source.close()
  }

  def populateEventsData(): Unit = {
    val source = Source.fromFile(dataDir + "/events.csv")
    println("Sending events data ...")
    val producer: KafkaProducer[String, Event] = initProducer[String, Event]("events-producer", classOf[StringSerializer], classOf[EventJsonSerializer])

    source.getLines().foreach { line =>
      val values = line.split(",")
      val event = Event(values(0).toInt, Timestamp.valueOf(values(1)), values(2).toInt)
      val record = new ProducerRecord[String, Event](KafkaConfig.EVENTS_TOPIC, event.accountID.toString, event)
      producer.send(record, callback)
    }
    println("Done Sending events data ...")
    shutdownProducer(producer)
    source.close()
  }

  def main(args: Array[String]): Unit = {
    KafkaConfig.topics.foreach { topic =>
      AdminUtils.createTopic(topic)
    }

    populateAccountsData()
    populateSubscriptions()
    populateEventTypesData()
//    Thread.sleep(10000)
    println("-" * 100)
    populateEventsData()
  }
}
