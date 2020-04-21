package basics

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch

import grizzled.slf4j.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._

class ConsumerWithThreads(topic: String) extends Logging {

  /** poll for new data **/
  def consumeNew(): Unit = {
    // create latch for dealing with multiple threads
    val latch = new CountDownLatch(1)
    //create consumer runnable
    val runnable = new ConsumerRunnable(latch)
    // start the thread
    val thread = new Thread(runnable)
    thread.start()

    // add shutdown hook; this properly shuts down application
    // i.e. this is the thing that runs when you stop the application
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

    //don't want to our application to exit right away, so we do await
    // this basically makes wait until application is over
    try {
      latch.await()
    } catch {
      case e: InterruptedException => {
        logger.error("application got interrupted", e)
        e.printStackTrace()
      }
    } finally {
      logger.info("app is closing")
    }
  }


  // latch is something to deal with concurrency; it is going to be able to shut down our application correctly
  class ConsumerRunnable(latch: CountDownLatch) extends Runnable {

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-forth-application5") // if start one consumer instance in group, then start another in sae group, a rebalance would occur
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // earliest, latest, none

    val consumer: KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Arrays.asList(topic))

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
