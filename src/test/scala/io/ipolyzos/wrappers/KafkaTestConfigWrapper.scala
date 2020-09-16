package io.ipolyzos.wrappers

import java.util.Properties

import org.apache.kafka.streams.StreamsConfig

trait KafkaTestConfigWrapper {
  def getConfig(): Properties = {
    val config = new Properties()
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testing")
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config
  }
}
