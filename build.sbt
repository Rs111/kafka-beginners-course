
name := "kafkabeginnerscourse"

version := "1.0.0"

scalaVersion := Version.Scala

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % Version.KafkaClients,
  "org.clapper" %% "grizzled-slf4j" % Version.GrizSLF4J,
  "ch.qos.logback" % "logback-classic" % Version.LogbackClassic,
  "com.github.scopt" %% "scopt" % Version.Scopt,
  "com.twitter" % "hbc-core" % "2.2.0",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.6.2",
  "org.apache.kafka" % "kafka-streams" % "2.5.0"
)
