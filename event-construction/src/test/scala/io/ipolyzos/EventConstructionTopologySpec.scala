package io.ipolyzos

import java.sql.{Date, Timestamp}

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.models.UserDomain
import io.ipolyzos.models.UserDomain.{Account, Event, EventType, EventWithType, EventWithTypeAndAccount, EnrichedEvent, Subscription}
import io.ipolyzos.serdes.UserDomainSerdes.AccountSerdes.AccountJsonSerializer
import io.ipolyzos.serdes.UserDomainSerdes.EventSerdes.EventJsonSerializer
import io.ipolyzos.serdes.UserDomainSerdes.EventTypeSerdes.EventTypeJsonSerializer
import io.ipolyzos.serdes.UserDomainSerdes.EventWithTypeAndAccountAndSubscriptionSerdes.EventWithTypeAndAccountAndSubscriptionJsonDeSerializer
import io.ipolyzos.serdes.UserDomainSerdes.SubscriptionSerdes.SubscriptionJsonSerializer
import io.ipolyzos.topology.EventConstructionTopology
import io.ipolyzos.utils.TestDataUtils
import io.ipolyzos.wrappers.KafkaTestConfigWrapper
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{TestInputTopic, TopologyTestDriver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventConstructionTopologySpec extends AnyFlatSpec
  with Matchers
  with TestDataUtils
  with KafkaTestConfigWrapper {
  import collection.JavaConverters._

  "UserActivityTopology" should "should join all topics" in {
    val driver: TopologyTestDriver = new TopologyTestDriver(EventConstructionTopology.build(), getConfig())

    val accountsTopic: TestInputTopic[String, UserDomain.Account] = driver.createInputTopic(KafkaConfig.ACCOUNTS_TOPIC, new StringSerializer(), new AccountJsonSerializer())
    val subscriptionsTopic: TestInputTopic[String, UserDomain.Subscription] = driver.createInputTopic(KafkaConfig.SUBSCRIPTIONS_TOPIC,  new StringSerializer(), new SubscriptionJsonSerializer())
    val eventsTopic: TestInputTopic[String, UserDomain.Event] = driver.createInputTopic(KafkaConfig.EVENTS_TOPIC,  new StringSerializer(), new EventJsonSerializer())
    val eventTypesTopic: TestInputTopic[String, UserDomain.EventType] = driver.createInputTopic(KafkaConfig.EVENT_TYPES_TOPIC,  new StringSerializer(), new EventTypeJsonSerializer())

    val account = Account(2,"appstore1", Date.valueOf("1940-10-05"), Some("AU"))
    val subscription = Subscription(6, 2, "socialnet7", Date.valueOf("2020-02-10"), Date.valueOf("2020-03-10"), Some(9), None,None, 1)
    val eventType = EventType(6, "message")
    val event =  Event(2, Timestamp.valueOf("2020-01-01 00:00:58"), 6)

    accountsTopic.pipeInput(account.id.toString, account)
    subscriptionsTopic.pipeInput(subscription.id.toString, subscription)
    eventTypesTopic.pipeInput(eventType.eventTypeID.toString, eventType)
    eventsTopic.pipeInput(event.accountID.toString, event)

    val output = driver.createOutputTopic("enriched_events", new StringDeserializer(), new EventWithTypeAndAccountAndSubscriptionJsonDeSerializer())

    val eventWithType = EventWithType(event.accountID, event.eventTime, eventType.eventTypeName)
    val eventWithTypeAndAccount = EventWithTypeAndAccount(2,"appstore1", Date.valueOf("1940-10-05"), Some("AU"), eventWithType.eventTime, eventWithType.eventTypeName)
    val eventWithTypeAndAccountAndSubscription = EnrichedEvent(
      eventWithTypeAndAccount.channel,
      eventWithTypeAndAccount.dateOfBirth,
      eventWithTypeAndAccount.country,
      eventWithTypeAndAccount.eventTime,
      eventWithTypeAndAccount.eventTypeName,
      subscription.id,
      subscription.accountID,
      subscription.product,
      subscription.startDate,
      subscription.endDate,
      subscription.mrr,
      subscription.quantity,
      subscription.units,
      subscription.billPeriodMonths
    )

    val outputValues = output.readKeyValuesToList().asScala

    outputValues.size shouldEqual 1
    outputValues.head.value shouldEqual eventWithTypeAndAccountAndSubscription

    driver.close()
  }

  "UserActivityTopology" should "should process all events, perform joins and construct correct output" in {
    val driver: TopologyTestDriver = new TopologyTestDriver(EventConstructionTopology.build(), getConfig())

    val accountsTopic: TestInputTopic[String, UserDomain.Account] = driver.createInputTopic(KafkaConfig.ACCOUNTS_TOPIC, new StringSerializer(), new AccountJsonSerializer())
    val subscriptionsTopic: TestInputTopic[String, UserDomain.Subscription] = driver.createInputTopic(KafkaConfig.SUBSCRIPTIONS_TOPIC,  new StringSerializer(), new SubscriptionJsonSerializer())
    val eventsTopic: TestInputTopic[String, UserDomain.Event] = driver.createInputTopic(KafkaConfig.EVENTS_TOPIC,  new StringSerializer(), new EventJsonSerializer())
    val eventTypesTopic: TestInputTopic[String, UserDomain.EventType] = driver.createInputTopic(KafkaConfig.EVENT_TYPES_TOPIC,  new StringSerializer(), new EventTypeJsonSerializer())

    accountsTopic.pipeKeyValueList(accounts.asJava)
    subscriptionsTopic.pipeKeyValueList(subscriptions.asJava)
    eventTypesTopic.pipeKeyValueList(eventTypes.asJava)
    Thread.sleep(500)

    eventsTopic.pipeKeyValueList(events.asJava)

    val output = driver.createOutputTopic("enriched_events", new StringDeserializer(), new EventWithTypeAndAccountAndSubscriptionJsonDeSerializer())

    val outputValues = output.readKeyValuesToList().asScala.map(_.value)
    val expectedOutput = createConstructedEvents()

    outputValues shouldEqual expectedOutput
    outputValues.size shouldEqual expectedOutput.size
    driver.close()
  }

  "accounts state store" should "update correctly" in {
    val driver: TopologyTestDriver = new TopologyTestDriver(EventConstructionTopology.build(), getConfig())
    val accountsTopic: TestInputTopic[String, UserDomain.Account] = driver.createInputTopic(KafkaConfig.ACCOUNTS_TOPIC, new StringSerializer(), new AccountJsonSerializer())
    accountsTopic.pipeKeyValueList(accounts.asJava)

    val store: KeyValueStore[String, Account] = driver.getKeyValueStore[String, Account]("accountStore")
    val result = List(store.get("3"), store.get("4"), store.get("5"), store.get("6"))

    store.approximateNumEntries() shouldEqual 6
    result shouldEqual accounts.takeRight(4).map(_.value)
    driver.close()
  }


  "subscriptions state store" should "update correctly" in {
    val driver: TopologyTestDriver = new TopologyTestDriver(EventConstructionTopology.build(), getConfig())
    val subscriptionsTopic: TestInputTopic[String, UserDomain.Subscription] = driver.createInputTopic(KafkaConfig.SUBSCRIPTIONS_TOPIC,  new StringSerializer(), new SubscriptionJsonSerializer())

    subscriptionsTopic.pipeKeyValueList(subscriptions.asJava)
    val store: KeyValueStore[String, Subscription] = driver.getKeyValueStore[String, Subscription]("subscriptionStore")
    val result = List(store.get("3"), store.get("4"), store.get("5"), store.get("6"))

    store.approximateNumEntries() shouldEqual 6
    result shouldEqual subscriptions.takeRight(4).map(_.value)
    driver.close()
  }
}
