package kafkastreams

import java.util.Properties

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

object StreamaFilterTweets extends App {

  // create properties
  val properties = new Properties()
  properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo_kafka_streams") // similar to a consumer group
  properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringSerde].getName)
  properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[StringSerde].getName)

  // create a topology
  val streamsBuilder: StreamsBuilder = new StreamsBuilder()

  // input topic
  val inputTopic: KStream[String,String] = streamsBuilder.stream("twitter_tweets")
  val filteredStream =
    inputTopic
      .filter((k,v) => v.contains("s")) // write whatever operations

  filteredStream.to("important_tweets")

  // build the topology and application
  val topology: Topology = streamsBuilder.build()
  val kafkaStreams: KafkaStreams = new KafkaStreams(topology, properties)

  // start our streams application
  kafkaStreams.start()
}
