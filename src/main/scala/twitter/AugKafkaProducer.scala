package twitter

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import java.util.concurrent.Future

import grizzled.slf4j.Logger

trait AugKafkaProducer {
  val topic: String
  val producer: KafkaProducer[String,String]

  def send(value: String): Future[RecordMetadata] = {
    producer.send(createProducerRecord(topic, value), createCallBack())
  }

  /** create producer record **/
  private def createProducerRecord(topic: String, value: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, null, value)
  }

  /** callback; function: (RecordMetadata, Exception) => SomeExpression **/
  private def createCallBack(): Callback = (metadata: RecordMetadata, e: Exception) => {
    val logger: Logger = Logger("callback logger")
    Option(e) match {
      case None =>
        // record was successfully sent
        logger.info("Received new metadata: \n" +
          "Topic: " + metadata.topic() + "\n" +
          "Partition: " + metadata.partition() + "\n" +
          "Offset: " + metadata.offset() + "\n" +
          "Timestamp: " + metadata.timestamp() + "\n"
        )
        metadata
      case Some(e: Throwable) =>
        logger.error("Error while producing", e)
        e.printStackTrace()
    }
  }
}

object AugKafkaProducer extends ((String, KafkaProducer[String,String]) => AugKafkaProducer){
  def apply(_topic: String, _producer: KafkaProducer[String,String]): AugKafkaProducer = new AugKafkaProducer {
    override val topic: String = _topic
    override val producer: KafkaProducer[String, String] = _producer
  }
}
