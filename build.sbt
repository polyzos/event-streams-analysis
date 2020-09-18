name := "user-activity-analysis"

version := "0.1"

scalaVersion := "2.12.12"

val kafkaVersion      = "2.5.0"
val circeVersion      = "0.12.3"
val AkkaVersion       = "2.6.8"
val AkkaHttpVersion   = "10.2.0"
val scalaTestVersion  = "3.2.0"

lazy val kafkaClients     = "org.apache.kafka"   %  "kafka-clients"             % kafkaVersion
lazy val kafkaStreams     = "org.apache.kafka"  %%  "kafka-streams-scala"       % kafkaVersion
lazy val circeCore        = "io.circe"          %%  "circe-core"                % circeVersion
lazy val circeParser      = "io.circe"          %%  "circe-parser"              % circeVersion
lazy val circeGeneric     = "io.circe"          %%  "circe-generic"             % circeVersion

lazy val scalaTest        = "org.scalatest"     %%  "scalatest"                 % scalaTestVersion    % Test
lazy val kafkaStreamsTest = "org.apache.kafka"   %  "kafka-streams-test-utils"  % kafkaVersion        % Test
lazy val akkaHttp         = "com.typesafe.akka" %%  "akka-http"                 % AkkaHttpVersion
lazy val akkaStreams      = "com.typesafe.akka" %%  "akka-stream"               % AkkaVersion

libraryDependencies ++= Seq(
  scalaTest,
  kafkaClients,
  kafkaStreams,
  kafkaStreamsTest,
  circeCore,
  circeParser,
  circeGeneric,
  akkaHttp,
  akkaStreams
)

assemblyMergeStrategy in assembly := {
  case "module-info.class"                           => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}