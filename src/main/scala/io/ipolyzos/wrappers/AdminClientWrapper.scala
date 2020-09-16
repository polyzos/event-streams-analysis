package io.ipolyzos.wrappers

import java.util.Properties

import io.ipolyzos.config.KafkaConfig
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

trait AdminClientWrapper {
  private val props = new Properties()
  props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers)

  val adminClient: AdminClient = AdminClient.create(props)
}
