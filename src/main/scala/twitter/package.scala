import java.util
import java.util.Properties
import java.util.concurrent.BlockingQueue

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Try

package object twitter {

  /** creating client **/
  def createHosebirdClient(hosebirdAuth: Authentication, msgQueue: BlockingQueue[String]): Try[Client] = Try {
    val hosebirdHosts: Hosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint
    // track terms
    val terms: util.List[String] = Lists.newArrayList("bitcoin")
    hosebirdEndpoint.trackTerms(terms)

    new ClientBuilder()
      .name("Hosebird-Client-01")                              // optional: mainly for the logs
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))
      .build()
  }

  /** create producer **/
  def createKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // safety properties
    props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    // all the below would be set automatically by idempotent config above; setting manually for visibility
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString)
    props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

    // high-throughput (at expense of a bit of latency and CPU usage)
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString) // 32 KB batch size

    new KafkaProducer[String, String](props)
  }
}
