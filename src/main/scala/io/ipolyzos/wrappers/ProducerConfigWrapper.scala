package io.ipolyzos.wrappers

import java.util.Properties

import io.ipolyzos.config.KafkaConfig
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, RecordMetadata}

trait ProducerConfigWrapper {
  private def getProducerProperties(clientID: String,
                                    keySerializer: Class[_],
                                    valueSerializer: Class[_]): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, KafkaConfig.enableIdempotence)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)

    // Kafka Producer Optional Configs
    //    props.put(ProducerConfig.ACKS_CONFIG, "") // 0, 1 OR ALL
    //    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "") // amount of memory to buffer messages before sending to brokers
    //    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "") // snappy, gzip, lz4
    //    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "") // amount of memory in bytes foreach batch
    //    props.put(ProducerConfig.LINGER_MS_CONFIG, "") // time to wait before sending the batch, even if batch-size not filled
    //    props.put(ProducerConfig.RETRIES_CONFIG, "")
    //    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "") // how many messages will be sent, without receiving responses
    //    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "") // how long the producer will wait for a response from the server
    //    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "") // how long the producer will block, when calling send()
    //    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "") // controls the size of a produce request sent by the producer
    //    props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "") // sizes of TCP send/receive buffers used by the sockets when writing/reading
    //    props.put(ProducerConfig.SEND_BUFFER_CONFIG, "") // sizes of TCP send/receive buffers used by the sockets when writing/reading
    //    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "") // how long the producer will wait for a response from the server, when requesting metadata
    props
  }

  def initProducer[K, V](clientID: String,
                         keySerializer: Class[_],
                         valueSerializer: Class[_]): KafkaProducer[K, V] = {
    val producerProperties = getProducerProperties(clientID, keySerializer, valueSerializer)
    new KafkaProducer[K, V](producerProperties)
  }

  def shutdownProducer[K, V](producer: KafkaProducer[K, V]): Unit = {
    producer.close()
  }

  val callback: Callback = (_: RecordMetadata, exception: Exception) => {
    if (exception != null) {
      exception.printStackTrace()
    }
  }
}
