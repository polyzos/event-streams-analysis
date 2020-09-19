package io.ipolyzos.topology

import java.nio.charset.StandardCharsets

import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.models.UserDomain.EnrichedEvent
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{Grouped, Materialized, Printed}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream}

object EventAnalysisTopology {
  import io.circe.parser._
  import io.circe.syntax._
  import io.circe.generic.auto._
  import io.ipolyzos.formatters.CustomFormatters._

  def build(): Topology = {
    val streamsBuilder = new StreamsBuilder()

    implicit val stringSerdes = Serdes.String
    implicit val enrichedEventSerdes = Serdes.fromFn(
      (_: String, data: EnrichedEvent) =>  data.asJson.noSpaces.getBytes(),
      (_: String, data: Array[Byte]) => decode[EnrichedEvent](new String(data, StandardCharsets.UTF_8)).right.toOption
    )

    val enrichedEventsConsumed = Consumed.`with`[String, EnrichedEvent](stringSerdes, enrichedEventSerdes)

    val enrichedStream = streamsBuilder.stream(KafkaConfig.ENRICHED_EVENTS_TOPIC)(enrichedEventsConsumed)
      .groupByKey(Grouped.`with`[String, EnrichedEvent](stringSerdes, enrichedEventSerdes))
      .count()(Materialized.as("transactionCounts"))

    enrichedStream.toStream.print(Printed.toSysOut[String, Long].withLabel("c"))

    streamsBuilder.build()
  }
}
