package io.ipolyzos.serdes

import java.nio.charset.StandardCharsets

import io.ipolyzos.models.UserDomain.{Account, Event, EventType, EventWithTypeAndAccountAndSubscription, Subscription}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object UserDomainSerdes {

  import io.circe.parser._
  import io.circe.syntax._
  import io.circe.generic.auto._
  import io.ipolyzos.formatters.CustomFormatters._

  // Event Serdes
  object EventSerdes extends Serde[Event] {
    override def serializer(): Serializer[Event] = new EventJsonSerializer()

    override def deserializer(): Deserializer[Event] = new EventJsonDeSerializer()

    class EventJsonSerializer() extends Serializer[Event] {
      override def serialize(topic: String, data: Event): Array[Byte] = {
        data.asJson.noSpaces.getBytes()
      }
    }

    class EventJsonDeSerializer() extends Deserializer[Event] {
      override def deserialize(topic: String, data: Array[Byte]): Event = {
        decode[Event](new String(data, StandardCharsets.UTF_8)).right.get
      }
    }
  }


  // Account Serdes
  object AccountSerdes extends Serde[Account] {
    override def serializer(): Serializer[Account] = new AccountJsonSerializer()

    override def deserializer(): Deserializer[Account] = new AccountJsonDeSerializer()

    class AccountJsonSerializer() extends Serializer[Account] {
      override def serialize(topic: String, data: Account): Array[Byte] = {
        data.asJson.noSpaces.getBytes()
      }
    }

    class AccountJsonDeSerializer() extends Deserializer[Account] {
      override def deserialize(topic: String, data: Array[Byte]): Account = {
        decode[Account](new String(data, StandardCharsets.UTF_8)).right.get
      }
    }
  }

  // Subscription Serdes
  object SubscriptionSerdes extends Serde[Subscription] {
    override def serializer(): Serializer[Subscription] = new SubscriptionJsonSerializer()

    override def deserializer(): Deserializer[Subscription] = new SubscriptionJsonDeSerializer()

    class SubscriptionJsonSerializer() extends Serializer[Subscription] {
      override def serialize(topic: String, data: Subscription): Array[Byte] = {
        data.asJson.noSpaces.getBytes()
      }
    }

    class SubscriptionJsonDeSerializer() extends Deserializer[Subscription] {
      override def deserialize(topic: String, data: Array[Byte]): Subscription = {
        decode[Subscription](new String(data, StandardCharsets.UTF_8)).right.get
      }
    }
  }


  // EventType Serdes
  object EventTypeSerdes extends Serde[EventType] {
    override def serializer(): Serializer[EventType] = new EventTypeJsonSerializer()

    override def deserializer(): Deserializer[EventType] = new EventTypeJsonDeSerializer()

    class EventTypeJsonSerializer() extends Serializer[EventType] {
      override def serialize(topic: String, data: EventType): Array[Byte] = {
        data.asJson.noSpaces.getBytes()
      }
    }

    class EventTypeJsonDeSerializer() extends Deserializer[EventType] {
      override def deserialize(topic: String, data: Array[Byte]): EventType = {
        decode[EventType](new String(data, StandardCharsets.UTF_8)).right.get
      }
    }
  }

  object EventWithTypeAndAccountAndSubscriptionSerdes extends Serde[EventWithTypeAndAccountAndSubscription] {
    override def serializer(): Serializer[EventWithTypeAndAccountAndSubscription] = new EventWithTypeAndAccountAndSubscriptionJsonSerializer()

    override def deserializer(): Deserializer[EventWithTypeAndAccountAndSubscription] = new EventWithTypeAndAccountAndSubscriptionJsonDeSerializer()

    class EventWithTypeAndAccountAndSubscriptionJsonSerializer() extends Serializer[EventWithTypeAndAccountAndSubscription] {
      override def serialize(topic: String, data: EventWithTypeAndAccountAndSubscription): Array[Byte] = {
        data.asJson.noSpaces.getBytes()
      }
    }

    class EventWithTypeAndAccountAndSubscriptionJsonDeSerializer() extends Deserializer[EventWithTypeAndAccountAndSubscription] {
      override def deserialize(topic: String, data: Array[Byte]): EventWithTypeAndAccountAndSubscription = {
        decode[EventWithTypeAndAccountAndSubscription](new String(data, StandardCharsets.UTF_8)).right.get
      }
    }
  }

}
