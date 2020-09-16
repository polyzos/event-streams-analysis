package io.ipolyzos.topology

import java.nio.charset.StandardCharsets

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.models.UserDomain.{Account, Event, EventType, EventWithType, EventWithTypeAndAccount, EventWithTypeAndAccountAndSubscription, Subscription}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{GlobalKTable, Joined, Printed, Produced}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable}

object UserActivityTopology {
  import io.circe.parser._
  import io.circe.syntax._
  import io.circe.generic.auto._
  import io.ipolyzos.formatters.CustomFormatters._

  def build(): Topology = {
    val streamsBuilder = new StreamsBuilder()

    import io.ipolyzos.serdes.UserDomainSerdes._

    implicit val stringSerdes: Serde[String] = Serdes.String
    implicit val accountSerdes = AccountSerdes
    implicit val subscriptionSerdes = SubscriptionSerdes
    implicit val eventTypesSerdes = EventTypeSerdes
    implicit val eventSerdes = EventSerdes

    implicit val eventWithTypeSerdes = Serdes.fromFn(
      (_: String, data: EventWithType) =>  data.asJson.noSpaces.getBytes(),
      (_: String, data: Array[Byte]) => decode[EventWithType](new String(data, StandardCharsets.UTF_8)).right.toOption
    )

    implicit val eventWithTypeAndAccountSerdes = Serdes.fromFn(
      (_: String, data: EventWithTypeAndAccount) =>  data.asJson.noSpaces.getBytes(),
      (_: String, data: Array[Byte]) => decode[EventWithTypeAndAccount](new String(data, StandardCharsets.UTF_8)).right.toOption
    )

    implicit val eventWithTypeAndAccountAndSubscriptionSerdes = Serdes.fromFn(
      (_: String, data: EventWithTypeAndAccountAndSubscription) =>  data.asJson.noSpaces.getBytes(),
      (_: String, data: Array[Byte]) => decode[EventWithTypeAndAccountAndSubscription](new String(data, StandardCharsets.UTF_8)).right.toOption
    )


    val accountConsumed = Consumed.`with`[String, Account](stringSerdes, accountSerdes)
    val subscriptionConsumed = Consumed.`with`[String, Subscription](stringSerdes, subscriptionSerdes)
    val eventTypesConsumed = Consumed.`with`[String, EventType](stringSerdes, eventTypesSerdes)
    val eventsConsumed = Consumed.`with`[String, Event](stringSerdes, eventSerdes)


    val eventStream: KStream[String, Event] = streamsBuilder.stream(KafkaConfig.EVENTS_TOPIC)(eventsConsumed)
    val accountsTable: KTable[String, Account] = streamsBuilder.table(KafkaConfig.ACCOUNTS_TOPIC)(accountConsumed)
    val subscriptionsTable: KTable[String, Subscription] = streamsBuilder.table(KafkaConfig.SUBSCRIPTIONS_TOPIC)(subscriptionConsumed)

    val eventTypesGlobalTable: GlobalKTable[String, EventType] = streamsBuilder.globalTable(KafkaConfig.EVENT_TYPES_TOPIC)(eventTypesConsumed)

    // Join Events with Event Types
    val eventWithTypeStream: KStream[String, EventWithType] = eventStream.join(eventTypesGlobalTable)(
      (_, value) => value.eventTypeID.toString,
      (event, eventType) => EventWithType(event.accountID, event.eventTime, eventType.eventTypeName)
    )

    // Join the above output with Accounts
    val eventWithTypeAndAccountStream: KStream[String, EventWithTypeAndAccount] = eventWithTypeStream.join(accountsTable){ (eventWithType, account) =>
      EventWithTypeAndAccount(
        account.id,
        account.channel,
        account.dateOfBirth,
        account.country,
        eventWithType.eventTime,
        eventWithType.eventTypeName
      )
    }(Joined.`with`(stringSerdes, eventWithTypeSerdes, accountSerdes))

    // Change the key of subscriptions to use accountID as Key
    subscriptionsTable.toStream
      .selectKey((_, value) => value.accountID.toString)
      .to(KafkaConfig.SUBSCRIPTIONS_REPARTITIONED_TOPIC)(Produced.`with`(stringSerdes, subscriptionSerdes))

    val subscriptionsRepartitionedTable: KTable[String, Subscription] = streamsBuilder.table(KafkaConfig.SUBSCRIPTIONS_REPARTITIONED_TOPIC)(subscriptionConsumed)


    // Join Events, Event Types and Accounts with Subscriptions - Final Event
    val eventWithTypeAndAccountAndSubscriptionStream: KStream[String, EventWithTypeAndAccountAndSubscription] = eventWithTypeAndAccountStream.join(subscriptionsRepartitionedTable){ (eventWithTypeAndAccount, subscription) =>
      EventWithTypeAndAccountAndSubscription(
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
    }(Joined.`with`(stringSerdes, eventWithTypeAndAccountSerdes, subscriptionSerdes))

    eventWithTypeAndAccountAndSubscriptionStream.print(
      Printed.toSysOut[String, EventWithTypeAndAccountAndSubscription].withLabel("enriched-event"))

    eventWithTypeAndAccountAndSubscriptionStream.to("enriched_events")(Produced.`with`(stringSerdes, eventWithTypeAndAccountAndSubscriptionSerdes))
    streamsBuilder.build()
  }
}