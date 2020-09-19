package io.ipolyzos

import java.util.{Properties, UUID}

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.topology.EventAnalysisTopology
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.util.Try

object EventAnalysisStream {
  def main(args: Array[String]): Unit = {
    val appID = Try(System.getenv("APP_ID")).getOrElse("0")
    val bootstrapServers = sys.env.getOrElse("BOOTSTRAP_SERVERS", KafkaConfig.bootstrapServers)

    val topology = EventAnalysisTopology.build()

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + UUID.randomUUID())
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "cgroup_lvl2" + UUID.randomUUID())
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
//    props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreLocation + s"$appID")
//    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, rpcEndpoint)

    val streams = new KafkaStreams(topology, props)
    streams.cleanUp()
    streams.start()
  }
}
