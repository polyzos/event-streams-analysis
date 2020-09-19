package io.ipolyzos.config

object KafkaConfig {

  lazy val bootstrapServers = "localhost:9092"
  lazy val enableIdempotence = "true"
  lazy val partitionsNumber = 3
  lazy val replicationFactor = 3

  // Available Topics
  lazy val EVENTS_TOPIC = "events"
  lazy val ENRICHED_EVENTS_TOPIC = "enriched_events"
  lazy val EVENT_TYPES_TOPIC = "event_types"
  lazy val ACCOUNTS_TOPIC = "accounts"
  lazy val SUBSCRIPTIONS_TOPIC = "subscriptions"
  lazy val SUBSCRIPTIONS_REPARTITIONED_TOPIC = "subscriptions_rep"

  lazy val ACCOUNT_STORE_NAME       = "accountStore"
  lazy val SUBSCRIPTION_STORE_NAME  = "subscriptionStore"
  lazy val EVENT_TYPE_STORE_NAME    = "eventTypeStore"


  lazy val topics = List(
    EVENTS_TOPIC,
    EVENT_TYPES_TOPIC,
    ACCOUNTS_TOPIC,
    SUBSCRIPTIONS_TOPIC,
    SUBSCRIPTIONS_REPARTITIONED_TOPIC
  )
}
