package io.ipolyzos

import java.util.Properties

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.rpc.RPCServer
import io.ipolyzos.topology.UserActivityTopology
import io.ipolyzos.wrappers.AsyncWrapper
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.concurrent.Future

object UserActivityStream extends AsyncWrapper {
  private val applicationID = "user-activity-stream"

  //private val stateStoreName = "top3"
  private val stateStoreLocation = "tmp/state-store"
  private val queryServerHost = "localhost"
  private val queryServerPort = 701


  def main(args: Array[String]): Unit = {
    (0 until 3).foreach { i =>
      Future {
        println(s"Starting UserActivityStream $i")
        val topology = UserActivityTopology.build()

        val port = s"$queryServerPort$i".toInt
        val rpcEndpoint = s"$queryServerHost:$port"

        println(s"[$i] Starting RPC server at '$rpcEndpoint'")
        val props = new Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID)
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "cgroup")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers)
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreLocation + s"$i")
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0")
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, rpcEndpoint)

        val streams = new KafkaStreams(topology, props)
        streams.cleanUp()
        val rpcServer = RPCServer(streams, new HostInfo(queryServerHost, port))
        streams.setStateListener { (currentState, previousState) =>
          val isActive = currentState == KafkaStreams.State.RUNNING &&  previousState == KafkaStreams.State.REBALANCING
          rpcServer.setActive(isActive)
        }

        streams.start()
        rpcServer.start()
      }
    }
  }
}
