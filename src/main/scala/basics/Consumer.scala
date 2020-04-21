package basics

import java.time.Duration
import java.util
import java.util.Properties

import grizzled.slf4j.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._

class Consumer(topic: String) extends Logging {
  /** create properties */
  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  // all data inside kafka is bytes; kafka sends bytes to consumer; need to tell consumer client how to deserialize it
  // i.e. consumer needs to take bytes and create a string from it
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-forth-application3") // if start one consumer instance in group, then start another in sae group, a rebalance would occur
  /* what to do when there is no initial offset in kafka of if the current offset does not exist anymore (e.g. because data has been deleted)
    - earliest:
      - automatically reset the offset to the earliest offset
      - you want to read from the very beginning of your topic
      - earliest is equivalent to the from-beginning option in the CLI
    - latest:
      - automatically reset the offset to the latest offset
      - you read from only the new messages onwards
    - none:
      - throw exception to the consumer if no previous offset found for the consumer's group
      - will throw an error if there are no offsets saved
   */
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // earliest, latest, none

  /** create consumer **/
  val consumer: KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)

  /** subscribe to topic(s) **/
  consumer.subscribe(util.Arrays.asList(topic)) // could subscribe to multiple topics

  /** poll for new data **/
  def consume(): Unit = {
    // consumer does not get data until it asks for it
    while (true) {
      /*
   val recordsJavaWay: ConsumerRecords[String,String] = consumer.poll(Duration.ofMillis(100))

   */
      val records: Iterable[ConsumerRecord[String, String]] = consumer.poll(Duration.ofMillis(100)).asScala // kafka 2.0.0 deprecates poll(l: Long)

      for (record <- records.iterator) {
        logger.info( "\n" +
          "Key: " + record.key() + "\n" +
          "Partition: " + record.partition() + "\n" +
          "Offset: " + record.offset()
        )
      }
    }
  }
}
