package io.ipolyzos

import java.util.Properties

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.rpc.RPCServer
import io.ipolyzos.topology.UserActivityTopology
import io.ipolyzos.wrappers.AsyncWrapper
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object UserActivityStream extends AsyncWrapper {
  private val applicationID = "user-activity-stream"

  private val stateStoreLocation = "tmp/state-store"

  def main(args: Array[String]): Unit = {
    val (appID, queryServerHost, queryServerPort) = {
      if (args.length != 3) (0, "localhost", 7010)
      else (args(0), args(1), args(2).toInt)
    }


    val rpcEndpoint = s"$queryServerHost:$queryServerPort"

    val topology = UserActivityTopology.build()

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID)
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "cgroup")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers)
    props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreLocation + s"$appID")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, rpcEndpoint)

    val streams = new KafkaStreams(topology, props)
    streams.cleanUp()
    val rpcServer = RPCServer(streams, new HostInfo(queryServerHost, queryServerPort))
    streams.setStateListener { (currentState, previousState) =>
      val isActive = currentState == KafkaStreams.State.RUNNING && previousState == KafkaStreams.State.REBALANCING
      rpcServer.setActive(isActive)
    }

    streams.start()
    rpcServer.start()
  }
}
