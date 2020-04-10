import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch

import grizzled.slf4j.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._

/*
  assign and seek is another way of writing a kafka app
  - create consumer without groupId and topic; delete groupId and topic
  - assign and seek are mostly used to replay data or fetch a specific message
 */
class ConsumerAssignAndSeek(topic: String) extends Logging {
  /** create properties */
  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  // all data inside kafka is bytes; kafka sends bytes to consumer; need to tell consumer client how to deserialize it
  // i.e. consumer needs to take bytes and create a string from it
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
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

  /** assign **/
  // pass in a collection of topic partitions
  val partitionToReadFrom: TopicPartition = new TopicPartition(topic, 0) // topic and partition
  consumer.assign(util.Arrays.asList(partitionToReadFrom))

  /** seek **/
  // pass in offset position from which to read
  val offsetToReadFrom: Long = 15L
  consumer.seek(partitionToReadFrom, offsetToReadFrom)

  /** poll for new data **/
  def consumeNew(): Unit = {
    // create latch for dealing with multiple threads
    val latch = new CountDownLatch(1)
    //create consumer runnable
    val runnable = new ConsumerRunnable(latch)
    // start the thread
    val thread = new Thread(runnable)
    thread.start()
    // add shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Caught shutdown hook")
      runnable.shutdown()
      try {
        latch.await()
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
      logger.info("application has exited")
    }))

    try {
      latch.await()
    } catch {
      case e: InterruptedException => {
        logger.error("error")
        e.printStackTrace()
      }
    } finally {
      logger.info("app is closing")
    }
  }


  // latch is something to deal with concurrency; it is going to be able to shut down our application correctly
  class ConsumerRunnable(latch: CountDownLatch) extends Runnable {

    override def run(): Unit = {
      try {
        while (true) {
          val records: Iterable[ConsumerRecord[String, String]] = consumer.poll(Duration.ofMillis(100)).asScala // kafka 2.0.0 deprecates poll(l: Long)

          for (record <- records.iterator) {
            logger.info("\n" +
              "Key: " + record.key() + "\n" +
              "Partition: " + record.partition() + "\n" +
              "Offset: " + record.offset()
            )
          }
        }
      } catch {
        case _: WakeupException => logger.info("Received shutdown signal")
      } finally {
        consumer.close()
        // allow our main code to understand that we are able to exit
        // tell our main code we are done with consumer
        latch.countDown()
      }
    }

    // wakeup is a special method to interupt consumer.poll; makes it throw exception: WakeUpException
    def shutdown(): Unit = {
      consumer.wakeup()
    }
  }
}
