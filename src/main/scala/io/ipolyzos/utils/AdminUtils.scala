package io.ipolyzos.utils

import java.util.Collections

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.wrappers.AdminClientWrapper
import org.apache.kafka.clients.admin.NewTopic

object AdminUtils extends AdminClientWrapper{
  import collection.JavaConverters._
  def createTopic(topicName: String,
                  partitions: Int = KafkaConfig.partitionsNumber,
                  replicationFactor: Short = KafkaConfig.replicationFactor.toShort): Unit = {
    if (!listTopics().contains(topicName)) {
      println(s"Creating Topic '$topicName':")
      adminClient.createTopics(
        Collections.singletonList(
          new NewTopic(topicName, partitions, replicationFactor))
      ).all().get()
      while(!adminClient.listTopics().names().get().contains(topicName)) {
        println(s"Waiting for topic '$topicName' to be created")
        Thread.sleep(2000)
      }
      println(s"Topic '$topicName' created successfully.")
      println("\nAvailable Topics:")
      listTopics().foreach { topic =>
        println(s"\t- $topic")
      }
    }
  }

  def listTopics(): List[String] = {
    adminClient.listTopics().names().get().asScala.toList
  }
}