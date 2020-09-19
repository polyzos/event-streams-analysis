name := "user-activity-analysis"

val common = Seq(
  version := "1.0.0",
  scalaVersion := "2.12.12",
  assemblyMergeStrategy in assembly := {
    case "module-info.class"    => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

val kafkaVersion      = "2.5.0"
val circeVersion      = "0.12.3"
val sparkVersion      = "3.0.0"
val AkkaVersion       = "2.6.8"
val AkkaHttpVersion   = "10.2.0"
val scalaTestVersion  = "3.2.0"
val deltaLakeVersion  = "0.7.0"


lazy val kafkaClients     = "org.apache.kafka"   %  "kafka-clients"             % kafkaVersion
lazy val kafkaStreams     = "org.apache.kafka"  %%  "kafka-streams-scala"       % kafkaVersion
lazy val circeCore        = "io.circe"          %%  "circe-core"                % circeVersion
lazy val circeParser      = "io.circe"          %%  "circe-parser"              % circeVersion
lazy val circeGeneric     = "io.circe"          %%  "circe-generic"             % circeVersion

lazy val scalaTest        = "org.scalatest"     %%  "scalatest"                 % scalaTestVersion    % Test
lazy val kafkaStreamsTest = "org.apache.kafka"   %  "kafka-streams-test-utils"  % kafkaVersion        % Test
lazy val akkaHttp         = "com.typesafe.akka" %%  "akka-http"                 % AkkaHttpVersion
lazy val akkaStreams      = "com.typesafe.akka" %%  "akka-stream"               % AkkaVersion

lazy val sparkSQL         = "org.apache.spark"  %%  "spark-sql"                 % sparkVersion        % "provided"
lazy val sparkSQLKafka    = "org.apache.spark"  %%  "spark-sql-kafka-0-10"      % sparkVersion        % "provided"
lazy val deltaLake        = "io.delta"          %%  "delta-core"                % deltaLakeVersion

val commonsDependencies   = Seq(scalaTest)

val akkaDepedencies       = Seq(akkaHttp, akkaStreams)
val sparkDependencies     = Seq(sparkSQL, sparkSQLKafka, deltaLake)
val circeDepedencies      = Seq(circeCore, circeParser, circeGeneric)
val kafkaDepedencies      = Seq(kafkaClients, kafkaStreams, kafkaStreamsTest)

lazy val root = (project in file("."))
  .settings(common)
//  .settings(
//    publishArtifact := false
//  )
  .aggregate(eventConstruction, eventAnalysis, datalakeIngestion)

lazy val commons = (project in file("commons"))
  .settings(common)
  .settings(
    name := "commons",
    libraryDependencies ++= (commonsDependencies ++ akkaDepedencies ++ kafkaDepedencies ++ circeDepedencies)
  )

lazy val eventConstruction = (project in file("event-construction"))
  .settings(common)
  .settings(
    name := "event-construction",
    libraryDependencies ++= (commonsDependencies ++ akkaDepedencies ++ kafkaDepedencies ++ circeDepedencies)
  )
  .dependsOn(commons)


lazy val eventAnalysis = (project in file("event-analysis"))
  .settings(common)
  .settings(
    name := "event-analysis",
    libraryDependencies ++= (commonsDependencies ++ akkaDepedencies ++ kafkaDepedencies ++ circeDepedencies)
  )
  .dependsOn(commons)

lazy val datalakeIngestion = (project in file("datalake-ingestion"))
  .settings(common)
  .settings(
    name := "datalake-ingestion",
    libraryDependencies ++= sparkDependencies
  )