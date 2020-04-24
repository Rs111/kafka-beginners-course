package elasticsearch

import java.time.Duration
import java.util.concurrent.CountDownLatch
import scala.jdk.CollectionConverters._
import grizzled.slf4j.Logger

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

trait AugKafkaConsumer {
  val topic: String
  val countDownLatch: CountDownLatch
  val kafkaConsumer: KafkaConsumer[String,String]

  def consume(): Unit = {
    val logger: Logger = Logger("callback logger")
    while (true) {
      val records: Iterable[ConsumerRecord[String, String]] = kafkaConsumer.poll(Duration.ofMillis(100)).asScala
      for (record <- records.iterator) {
        logger.info( "\n" +
          "Key: " + record.key() + "\n" +
          "Value: " + record.value() + "\n" +
          "Partition: " + record.partition() + "\n" +
          "Offset: " + record.offset()
        )

      }
    }
  }

}

object AugKafkaConsumer extends ((String,CountDownLatch,KafkaConsumer[String,String]) => AugKafkaConsumer) {
  def apply(
             _topic: String,
             _countDownLatch: CountDownLatch,
             _kafkaConsumer: KafkaConsumer[String, String]): AugKafkaConsumer = new AugKafkaConsumer {
    override val topic: String = _topic
    override val countDownLatch: CountDownLatch = _countDownLatch
    override val kafkaConsumer: KafkaConsumer[String,String] = _kafkaConsumer
  }
}
