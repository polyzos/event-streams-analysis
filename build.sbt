name := "user-activity-analysis"

version := "0.1"

scalaVersion := "2.12.12"

val kafkaVersion    = "2.5.0"
val circeVersion    = "0.12.3"
val scalaTestVersion = "3.2.0"

lazy val kafkaClients         = "org.apache.kafka"       %    "kafka-clients"         % kafkaVersion
lazy val kafkaStreams         = "org.apache.kafka"      %%    "kafka-streams-scala"   % kafkaVersion
lazy val circeCore            = "io.circe"              %%    "circe-core"            % circeVersion
lazy val circeParser          = "io.circe"              %%    "circe-parser"          % circeVersion
lazy val circeGeneric         = "io.circe"              %%    "circe-generic"         % circeVersion

lazy val scalaTest            = "org.scalatest"         %%    "scalatest"             % scalaTestVersion    % Test


libraryDependencies ++= Seq(
  scalaTest,
  kafkaClients,
  kafkaStreams,
  circeCore,
  circeParser,
  circeGeneric
)