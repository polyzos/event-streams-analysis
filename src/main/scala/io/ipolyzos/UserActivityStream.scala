package io.ipolyzos

import java.util.{Properties, UUID}

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.topology.UserActivityTopology
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object UserActivityStream {
  private val applicationID = "user-activity-stream"

  //private val stateStoreName = "top3"
  private val stateStoreLocation = "tmp/state-store"
  private val queryServerHost = "localhost"
  private val queryServerPort = 7010

  def main(args: Array[String]): Unit = {
    val topology = UserActivityTopology.build()

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID + UUID.randomUUID().toString)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers)
    props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreLocation)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0")
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, s"$queryServerHost:$queryServerPort")

    val streams = new KafkaStreams(topology, props)
    streams.cleanUp()
    streams.start()
  }
}
