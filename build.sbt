
name := "kafkabeginnerscourse"

version := "1.0.0"

scalaVersion := Version.Scala

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % Version.KafkaClients,
  "org.clapper" %% "grizzled-slf4j" % Version.GrizSLF4J,
  "ch.qos.logback" % "logback-classic" % Version.LogbackClassic,
)
