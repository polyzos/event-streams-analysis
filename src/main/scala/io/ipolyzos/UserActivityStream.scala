package io.ipolyzos

import java.util.concurrent.{ExecutorService, Executors}
import java.util.Properties

import akka.actor.ActorSystem
import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.models.UserDomain.Account
import io.ipolyzos.rpc.RPCServer
import io.ipolyzos.topology.UserActivityTopology
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.{HostInfo, QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters, StreamsConfig}

import scala.concurrent.{ExecutionContext, Future}

object UserActivityStream {
  private val applicationID = "user-activity-stream"

  //private val stateStoreName = "top3"
  private val stateStoreLocation = "tmp/state-store"
  private val queryServerHost = "localhost"
  private val queryServerPort = 701

  private val executorService: ExecutorService = Executors.newFixedThreadPool(3)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

  implicit val system: ActorSystem = ActorSystem("SampleServer")

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
